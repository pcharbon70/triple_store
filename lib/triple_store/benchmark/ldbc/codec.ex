defmodule TripleStore.Benchmark.LDBC.Codec do
  @moduledoc """
  Strict codecs for canonical LDBC parameters and result values.

  Conversion failures are data errors. The codec does not silently narrow
  numbers, normalize non-UTC timestamps, replace invalid UTF-8, or conflate the
  explicit `:null` and `:unbound` markers.
  """

  @type canonical_type ::
          :id
          | {:integer, 8 | 16 | 32 | 64}
          | :float32
          | :float64
          | :date
          | :timestamp
          | :boolean
          | :string
          | {:list, canonical_type()}
          | {:optional, canonical_type()}
          | :rdf_term

  @doc "Decodes an external value into its strict canonical representation."
  @spec decode(canonical_type(), term()) :: {:ok, term()} | {:error, term()}
  def decode(:id, value), do: decode_integer(value, 64)

  def decode({:integer, bits}, value) when bits in [8, 16, 32, 64],
    do: decode_integer(value, bits)

  def decode(:float32, value), do: decode_float(value, :float32)
  def decode(:float64, value), do: decode_float(value, :float64)
  def decode(:boolean, value) when is_boolean(value), do: {:ok, value}
  def decode(:boolean, value) when value in ["true", "false"], do: {:ok, value == "true"}

  def decode(:string, value) when is_binary(value) do
    if String.valid?(value), do: {:ok, value}, else: {:error, :invalid_utf8}
  end

  def decode(:date, %Date{} = value), do: {:ok, value}
  def decode(:date, value) when is_binary(value), do: Date.from_iso8601(value)
  def decode(:timestamp, %DateTime{utc_offset: 0, std_offset: 0} = value), do: {:ok, value}

  def decode(:timestamp, %DateTime{}), do: {:error, :timezone_drift}

  def decode(:timestamp, value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, timestamp, 0} -> {:ok, timestamp}
      {:ok, _timestamp, _offset} -> {:error, :timezone_drift}
      {:error, reason} -> {:error, reason}
    end
  end

  def decode({:list, type}, values) when is_list(values), do: traverse(values, &decode(type, &1))
  def decode({:optional, _type}, :null), do: {:ok, :null}
  def decode({:optional, _type}, :unbound), do: {:ok, :unbound}
  def decode({:optional, type}, value), do: decode(type, value)
  def decode(:rdf_term, %RDF.IRI{} = value), do: {:ok, value}
  def decode(:rdf_term, %RDF.BlankNode{} = value), do: {:ok, value}
  def decode(:rdf_term, %RDF.Literal{} = value), do: {:ok, value}
  def decode(_type, _value), do: {:error, :type_mismatch}

  @doc "Decodes a parameter map and rejects missing or undeclared names."
  @spec decode_parameters([map()], map()) :: {:ok, map()} | {:error, term()}
  def decode_parameters(schema, parameters) when is_list(schema) and is_map(parameters) do
    expected = MapSet.new(Enum.map(schema, & &1.name))
    supplied = MapSet.new(Map.keys(parameters))
    unknown = MapSet.difference(supplied, expected) |> MapSet.to_list() |> Enum.sort()

    if unknown == [] do
      decode_schema(schema, parameters)
    else
      {:error, {:unknown_parameters, unknown}}
    end
  end

  def decode_parameters(_schema, _parameters), do: {:error, :invalid_parameters}

  defp decode_integer(value, bits) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> decode_integer(integer, bits)
      _ -> {:error, :type_mismatch}
    end
  end

  defp decode_integer(value, bits) when is_integer(value) do
    min = -Integer.pow(2, bits - 1)
    max = Integer.pow(2, bits - 1) - 1
    if value in min..max, do: {:ok, value}, else: {:error, {:integer_overflow, bits}}
  end

  defp decode_integer(_value, _bits), do: {:error, :type_mismatch}

  defp decode_float(value, width) when is_integer(value), do: decode_float(value * 1.0, width)

  defp decode_float(value, :float64) when is_float(value) do
    if finite?(value), do: {:ok, value}, else: {:error, :non_finite_float}
  end

  defp decode_float(value, :float32) when is_float(value) do
    if finite?(value) do
      <<narrowed::float-32>> = <<value::float-32>>
      if narrowed == value, do: {:ok, narrowed}, else: {:error, :precision_loss}
    else
      {:error, :non_finite_float}
    end
  end

  defp decode_float(_value, _width), do: {:error, :type_mismatch}

  defp finite?(value) do
    <<_sign::1, exponent::11, _fraction::52>> = <<value::float-64>>
    exponent != 0x7FF
  end

  defp decode_schema(schema, parameters) do
    Enum.reduce_while(schema, {:ok, %{}}, fn field, {:ok, decoded} ->
      case decode_parameter(field, parameters) do
        {:ok, canonical} -> {:cont, {:ok, Map.put(decoded, field.name, canonical)}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp decode_parameter(field, parameters) do
    case Map.fetch(parameters, field.name) do
      {:ok, value} -> decode_parameter_value(field, value)
      :error -> missing_parameter(field)
    end
  end

  defp decode_parameter_value(field, value) do
    case decode(field.type, value) do
      {:ok, canonical} -> {:ok, canonical}
      {:error, reason} -> {:error, {:invalid_parameter, field.name, reason}}
    end
  end

  defp missing_parameter(%{required: false}), do: {:ok, :unbound}

  defp missing_parameter(%{name: name}) when is_binary(name),
    do: {:error, {:missing_parameter, name}}

  defp traverse(values, fun) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case fun.(value) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end
end
