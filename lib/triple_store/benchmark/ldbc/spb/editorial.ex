defmodule TripleStore.Benchmark.LDBC.SPB.Editorial do
  @moduledoc """
  Typed SPB editorial create, alter, delete, and validation transitions.

  Rendered updates are parsed before entering one store-owned transaction
  coordinator. Successful commits invalidate normal query state through the
  coordinator and trigger deterministic SPB inference rederivation.
  """

  alias TripleStore.Benchmark.LDBC.SPB.{Semantics, Workload}
  alias TripleStore.SPARQL.{Parser, Query}
  alias TripleStore.Transaction

  @actions [:insert, :update, :delete]
  @iri_fields ~w(cwGraphUri cwUri cwType cwCategory cwAudienceType cwLiveCoverage cwThumbnailUri)
  @string_fields ~w(cwTitle cwShortTitle cwDescription)
  @datetime_fields ~w(cwDateCreated cwDateModified)
  @list_sections %{
    "cwAboutsList" => {"cwAboutUri", :iri},
    "cwMentionsList" => {"cwMentionsUri", :iri},
    "cwPrimaryFormatList" => {"cwPrimaryFormat", :iri},
    "cwPrimaryContentList" => {:primary_content, :map}
  }

  @doc "Starts the single coordinator owned by an SPB benchmark runtime."
  @spec start_coordinator(TripleStore.store(), keyword()) :: GenServer.on_start()
  def start_coordinator(store, opts \\ []) do
    Transaction.start_link(
      db: store.db,
      dict_manager: store.dict_manager,
      plan_cache: Keyword.get(opts, :plan_cache, TripleStore.SPARQL.PlanCache),
      stats_callback: Keyword.get(opts, :stats_callback)
    )
  end

  @doc "Returns deterministic typed parameters with stable generated resource IDs."
  @spec parameters(non_neg_integer(), keyword()) :: map()
  def parameters(sequence, opts \\ []) when is_integer(sequence) and sequence >= 0 do
    graph = "urn:ldbc:spb:editorial:graph:#{sequence}"
    work = "urn:ldbc:spb:editorial:work:#{sequence}"
    modified = Keyword.get(opts, :modified, "2026-01-02T00:00:00Z")

    %{
      "cwGraphUri" => graph,
      "cwUri" => work,
      "cwType" => "http://www.bbc.co.uk/ontologies/creativework/CreativeWork",
      "cwTitle" => Keyword.get(opts, :title, "SPB editorial work #{sequence}"),
      "cwShortTitle" => "Work #{sequence}",
      "cwCategory" => "urn:ldbc:spb:category:news",
      "cwDescription" => "Deterministic editorial transition #{sequence}",
      "cwAboutsList" => ["urn:ldbc:spb:entity:#{sequence}"],
      "cwMentionsList" => ["urn:ldbc:spb:entity:mentioned:#{sequence}"],
      "cwAudienceType" => "http://www.bbc.co.uk/ontologies/creativework/NationalAudience",
      "cwLiveCoverage" => "http://www.bbc.co.uk/ontologies/creativework/LiveCoverage",
      "cwPrimaryFormatList" => ["http://www.bbc.co.uk/ontologies/creativework/TextualFormat"],
      "cwDateCreated" => "2026-01-01T00:00:00Z",
      "cwDateModified" => modified,
      "cwThumbnailUri" => "urn:ldbc:spb:thumbnail:#{sequence}",
      "cwPrimaryContentList" => [
        %{
          "cwPrimaryContentUri" => "urn:ldbc:spb:content:#{sequence}",
          "cwWebDocumentType" => "http://www.bbc.co.uk/ontologies/bbc/WebDocument"
        }
      ]
    }
  end

  @doc "Renders and native-parser validates one canonical editorial action."
  @spec render(atom(), map()) :: {:ok, String.t()} | {:error, term()}
  def render(action, parameters) when action in @actions and is_map(parameters) do
    with {:ok, package} <- Workload.load(),
         {:ok, entry} <- editorial_entry(package, action),
         :ok <- validate_parameter_names(action, parameters),
         {:ok, expanded} <- expand_sections(entry.template, parameters),
         {:ok, query} <- replace_fields(expanded, parameters),
         {:ok, _ast} <- Parser.parse_update(query) do
      {:ok, query}
    end
  end

  @doc "Executes one transition and validates graph state after commit."
  @spec execute(GenServer.server(), TripleStore.store(), atom(), map()) ::
          {:ok, map()} | {:error, term()}
  def execute(coordinator, store, action, parameters) when action in @actions do
    with {:ok, update} <- render(action, parameters),
         {:ok, affected} <- Transaction.update(coordinator, update),
         {:ok, reasoning} <- Semantics.after_commit(store, {:ok, affected}),
         {:ok, validation} <- validate_state(store, action, parameters["cwGraphUri"]) do
      {:ok,
       %{
         action: action,
         affected: affected,
         graph: parameters["cwGraphUri"],
         reasoning: reasoning,
         validation: validation
       }}
    end
  end

  @doc "Queries the canonical validation probe for one editorial graph."
  @spec validate_state(TripleStore.store(), atom(), String.t()) :: {:ok, map()} | {:error, term()}
  def validate_state(store, action, graph) when action in @actions and is_binary(graph) do
    with {:ok, package} <- Workload.load(),
         {:ok, entry} <- validation_entry(package, action),
         query <- String.replace(entry.template, "{{{contextURI}}}", "<#{graph}>"),
         {:ok, rows} <- Query.query(Semantics.execution_context(store), query) do
      count = if is_list(rows), do: length(rows), else: 0
      valid? = if action == :delete, do: count == 0, else: count > 0

      if valid?, do: {:ok, %{row_count: count, valid?: true}}, else: {:error, :state_probe_failed}
    end
  end

  defp expand_sections(template, parameters) do
    Enum.reduce_while(@list_sections, {:ok, template}, fn {section, spec}, {:ok, source} ->
      expand_section(source, section, spec, Map.get(parameters, section, []))
    end)
  end

  defp expand_section(source, section, spec, values) when is_list(values) do
    pattern = ~r/\{\{##{section}\}\}([\s\S]*?)\{\{\/#{section}\}\}/

    replacement =
      Regex.replace(pattern, source, fn _match, body ->
        Enum.map_join(values, "", &render_section(body, spec, &1))
      end)

    {:cont, {:ok, replacement}}
  end

  defp expand_section(_source, section, _spec, _values),
    do: {:halt, {:error, {:invalid_list_parameter, section}}}

  defp render_section(body, {:primary_content, :map}, value) when is_map(value) do
    body
    |> String.replace(
      "{{{cwPrimaryContentUri}}}",
      encode!(:iri, value["cwPrimaryContentUri"])
    )
    |> String.replace("{{{cwWebDocumentType}}}", encode!(:iri, value["cwWebDocumentType"]))
  end

  defp render_section(body, {field, type}, value) do
    String.replace(body, "{{{#{field}}}}", encode!(type, value))
  end

  defp replace_fields(template, parameters) do
    types =
      Map.new(@iri_fields, &{&1, :iri})
      |> Map.merge(Map.new(@string_fields, &{&1, :string}))
      |> Map.merge(Map.new(@datetime_fields, &{&1, :datetime}))

    rendered =
      Enum.reduce(types, template, fn {field, type}, source ->
        case Map.fetch(parameters, field) do
          {:ok, value} -> String.replace(source, "{{{#{field}}}}", encode!(type, value))
          :error -> source
        end
      end)

    case Regex.run(~r/\{\{[#\/]?\{?[^}]+\}\}?\}/, rendered) do
      nil -> {:ok, rendered}
      [placeholder | _] -> {:error, {:unresolved_placeholder, placeholder}}
    end
  rescue
    error in ArgumentError -> {:error, {:invalid_parameter, error.message}}
  end

  defp encode!(:iri, value) when is_binary(value) do
    case URI.parse(value) do
      %URI{scheme: scheme} when is_binary(scheme) and scheme != "" ->
        if String.contains?(value, ["<", ">", "\"", "{"]), do: raise(ArgumentError, "unsafe IRI")
        "<#{value}>"

      _ ->
        raise ArgumentError, "invalid IRI"
    end
  end

  defp encode!(:string, value) when is_binary(value) and byte_size(value) <= 16_384 do
    escaped = value |> String.replace("\\", "\\\\") |> String.replace("\"", "\\\"")
    "\"#{escaped}\""
  end

  defp encode!(:datetime, value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, 0} ->
        "\"#{DateTime.to_iso8601(datetime)}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"

      _ ->
        raise ArgumentError, "invalid UTC datetime"
    end
  end

  defp encode!(_type, _value), do: raise(ArgumentError, "wrong parameter type")

  defp validate_parameter_names(:delete, parameters) do
    exact_names(parameters, ["cwGraphUri"])
  end

  defp validate_parameter_names(_action, parameters) do
    exact_names(
      parameters,
      @iri_fields ++ @string_fields ++ @datetime_fields ++ Map.keys(@list_sections)
    )
  end

  defp exact_names(parameters, expected) do
    actual = Map.keys(parameters) |> Enum.sort()
    expected = Enum.sort(expected)
    if actual == expected, do: :ok, else: {:error, {:parameter_names, expected, actual}}
  end

  defp editorial_entry(package, action) do
    fetch_entry(package, :editorial, String.upcase(to_string(action)))
  end

  defp validation_entry(package, action) do
    fetch_entry(package, :validation, "validate#{action |> to_string() |> String.capitalize()}")
  end

  defp fetch_entry(package, family, upstream_id) do
    case Enum.find(
           package.operations,
           &(&1.family == family and &1.operation.upstream_id == upstream_id)
         ) do
      nil -> {:error, {:operation_not_packaged, upstream_id}}
      entry -> {:ok, entry}
    end
  end
end
