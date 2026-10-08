defmodule TripleStore.Benchmark.LDBC.SPB.Template do
  @moduledoc """
  Strict renderer for the pinned SPB Mustache query templates.

  SPB parameters are complete SPARQL lexical forms rather than prepared-query
  variables. This renderer therefore accepts only declared parameter names and
  validates each value by type. Grammar fragments are selected from fixed enums;
  callers cannot inject arbitrary clauses.
  """

  @placeholder ~r/\{\{\{([A-Za-z][A-Za-z0-9]*)\}\}\}/
  @datetime "http://www.w3.org/2001/XMLSchema#dateTime"

  @iri_parameters ~w(
    cwAboutEntityType cwAboutOrMentionsUri cwAudience cwAudienceType cwCategoryType
    cwLiveCoverage cwPrimaryFormat cwType cwUri cwWebDocumentType entityA entityB
  )
  @datetime_parameters ~w(cwStartDateTime cwEndDateTime)
  @decimal_parameters ~w(refDeviation refLatitude refLongtitude)

  @fragment_values %{
    "cwAboutOrMentions" => ["cwork:about", "cwork:mentions"],
    "cwFilterDateModifiedCondition" => [
      "FILTER(?dateModif >= \"1970-01-01T00:00:00Z\"^^xsd:dateTime)"
    ],
    "filter1" => ["FILTER(?year >= 1970)"],
    "filter2" => ["FILTER(?month >= 1)"],
    "filter3" => ["FILTER(?month <= 12)"],
    "filterCondition" => ["FILTER(?pcCount >= 0)"],
    "groupBy" => ["GROUP BY ?year ?month"],
    "orderBy" => ["", "ORDER BY DESC(?count)"],
    "projection" => ["?year ?month (COUNT(*) AS ?count)"],
    "timeFilter" => ["FILTER(?year >= 1970)"]
  }

  @doc "Returns the unique placeholders in source order."
  @spec placeholders(String.t()) :: [String.t()]
  def placeholders(template) when is_binary(template) do
    @placeholder
    |> Regex.scan(template, capture: :all_but_first)
    |> List.flatten()
    |> Enum.uniq()
  end

  @doc "Returns the typed schema for a template, rejecting unknown placeholders."
  @spec schema(String.t()) :: {:ok, [map()]} | {:error, term()}
  def schema(template) do
    template
    |> placeholders()
    |> Enum.reduce_while({:ok, []}, fn name, {:ok, fields} ->
      case type(name) do
        {:ok, type} -> {:cont, {:ok, [%{name: name, type: type, required: true} | fields]}}
        :error -> {:halt, {:error, {:unknown_placeholder, name}}}
      end
    end)
    |> case do
      {:ok, fields} -> {:ok, Enum.reverse(fields)}
      error -> error
    end
  end

  @doc "Renders a template after exact-name and lexical validation."
  @spec render(String.t(), map()) :: {:ok, String.t()} | {:error, term()}
  def render(template, parameters) when is_binary(template) and is_map(parameters) do
    names = placeholders(template)
    provided = parameters |> Map.keys() |> Enum.map(&to_string/1)
    unknown = Enum.sort(provided -- names)
    missing = Enum.sort(names -- provided)

    cond do
      unknown != [] ->
        {:error, {:unknown_parameters, unknown}}

      missing != [] ->
        {:error, {:missing_parameters, missing}}

      true ->
        with {:ok, encoded} <- encode_all(names, parameters) do
          rendered =
            Regex.replace(@placeholder, template, fn _match, name -> Map.fetch!(encoded, name) end)

          {:ok, rendered}
        end
    end
  end

  @doc "Returns deterministic, parser-valid smoke values for every declared type."
  @spec defaults(String.t(), [String.t()]) :: {:ok, map()} | {:error, term()}
  def defaults(template, dataset_values \\ []) do
    iri = Enum.find(dataset_values, &valid_iri?/1) || "urn:ldbc:spb:entity:smoke"

    with {:ok, fields} <- schema(template) do
      {:ok, Map.new(fields, &{&1.name, default(&1.name, &1.type, iri)})}
    end
  end

  defp type(name) when name in @iri_parameters, do: {:ok, :iri}
  defp type(name) when name in @datetime_parameters, do: {:ok, :datetime}
  defp type(name) when name in @decimal_parameters, do: {:ok, :decimal}
  defp type("randomLimit"), do: {:ok, {:integer, 1, 10_000}}
  defp type("word"), do: {:ok, :string}

  defp type(name) do
    case Map.fetch(@fragment_values, name) do
      {:ok, values} -> {:ok, {:enum, values}}
      :error -> :error
    end
  end

  defp encode_all(names, parameters) do
    Enum.reduce_while(names, {:ok, %{}}, fn name, {:ok, encoded} ->
      value = Map.get(parameters, name, Map.get(parameters, String.to_atom(name)))

      with {:ok, type} <- normalize_type(type(name), name),
           {:ok, lexical} <- encode(type, value) do
        {:cont, {:ok, Map.put(encoded, name, lexical)}}
      else
        {:error, reason} -> {:halt, {:error, {:invalid_parameter, name, reason}}}
      end
    end)
  end

  defp normalize_type({:ok, type}, _name), do: {:ok, type}
  defp normalize_type(:error, name), do: {:error, {:unknown_placeholder, name}}

  defp encode(:iri, %RDF.IRI{} = iri), do: encode(:iri, to_string(iri))

  defp encode(:iri, iri) when is_binary(iri) do
    if valid_iri?(iri) and not String.contains?(iri, [">", "<", "\"", "{"]) do
      {:ok, "<#{iri}>"}
    else
      {:error, :invalid_iri}
    end
  end

  defp encode(:datetime, %DateTime{} = value),
    do: {:ok, "\"#{DateTime.to_iso8601(value)}\"^^<#{@datetime}>"}

  defp encode(:datetime, value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, 0} -> encode(:datetime, datetime)
      _ -> {:error, :invalid_utc_datetime}
    end
  end

  defp encode(:decimal, value) when is_number(value), do: {:ok, to_string(value)}

  defp encode({:integer, minimum, maximum}, value)
       when is_integer(value) and value >= minimum and value <= maximum,
       do: {:ok, Integer.to_string(value)}

  defp encode(:string, value) when is_binary(value) do
    if String.valid?(value) do
      escaped = value |> String.replace("\\", "\\\\") |> String.replace("\"", "\\\"")
      {:ok, "\"#{escaped}\""}
    else
      {:error, :invalid_utf8}
    end
  end

  defp encode({:enum, values}, value) do
    if value in values, do: {:ok, value}, else: {:error, :unknown_enum_value}
  end

  defp encode(_type, _value), do: {:error, :wrong_type_or_out_of_range}

  defp default(_name, :iri, iri), do: iri
  defp default("cwStartDateTime", :datetime, _iri), do: "1970-01-01T00:00:00Z"
  defp default("cwEndDateTime", :datetime, _iri), do: "2100-01-01T00:00:00Z"
  defp default("refLatitude", :decimal, _iri), do: 51.5074
  defp default("refLongtitude", :decimal, _iri), do: -0.1278
  defp default("refDeviation", :decimal, _iri), do: 1.0
  defp default("randomLimit", {:integer, _, _}, _iri), do: 10
  defp default("word", :string, _iri), do: "news"
  defp default(name, {:enum, [value | _]}, _iri), do: preferred_fragment(name, value)

  defp preferred_fragment("orderBy", _value), do: ""
  defp preferred_fragment(_name, value), do: value

  defp valid_iri?(value) when is_binary(value) do
    case URI.parse(value) do
      %URI{scheme: scheme} when is_binary(scheme) and scheme != "" -> true
      _ -> false
    end
  end

  defp valid_iri?(_value), do: false
end
