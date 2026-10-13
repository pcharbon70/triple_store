defmodule TripleStore.Benchmark.LDBC.SNB.BI.Handlers.StandardQuery do
  @moduledoc """
  Prepared-query adapter for standard SNB BI analytical operations.

  Translation placeholders are converted to prepared-query placeholders; no
  parameter value is interpolated into SPARQL text. Values are validated and
  converted to RDF terms before AST-level binding by `SPARQL.Query`.
  """

  alias TripleStore.SPARQL.Query

  @xsd "http://www.w3.org/2001/XMLSchema#"
  @placeholder ~r/\{\{([A-Za-z][A-Za-z0-9_]*)\}\}/

  @doc "Executes one standard BI translation and returns rows for the BI result contract."
  @spec execute(map(), map(), map(), keyword()) :: {:ok, [map()]} | {:error, term()}
  def execute(definition, context, parameters, opts) do
    with :ok <- validate_context(context),
         {:ok, bindings} <- typed_bindings(definition.parameters, parameters),
         {:ok, prepared} <- Query.prepare(prepared_source(definition.translation.sparql)),
         {:ok, rows} <-
           Query.execute(
             context,
             prepared,
             bindings,
             Keyword.take(opts, [:timeout])
           ) do
      normalize_rows(definition.result, rows)
    end
  end

  defp prepared_source(sparql), do: Regex.replace(@placeholder, sparql, "$\\1")

  defp validate_context(%{db: db, dict_manager: manager})
       when is_pid(db) and is_pid(manager),
       do: :ok

  defp validate_context(_context), do: {:error, :bi_query_context_required}

  defp typed_bindings(schema, parameters) when is_list(schema) and is_map(parameters) do
    expected = schema |> Enum.map(& &1.name) |> MapSet.new()
    actual = parameters |> Map.keys() |> MapSet.new()

    bind_parameters(expected == actual, schema, parameters, expected, actual)
  end

  defp typed_bindings(_schema, _parameters), do: {:error, :invalid_parameters}

  defp bind_parameters(true, schema, parameters, _expected, _actual) do
    Enum.reduce_while(schema, {:ok, %{}}, fn field, {:ok, acc} ->
      bind_parameter(field, parameters, acc)
    end)
  end

  defp bind_parameters(false, _schema, _parameters, expected, actual) do
    {:error,
     {:parameter_names, expected |> MapSet.to_list() |> Enum.sort(),
      actual |> MapSet.to_list() |> Enum.sort()}}
  end

  defp bind_parameter(field, parameters, acc) do
    case typed_parameter(field.type, Map.fetch!(parameters, field.name)) do
      {:ok, value} -> {:cont, {:ok, Map.put(acc, field.name, value)}}
      {:error, reason} -> {:halt, {:error, {:invalid_parameter, field.name, reason}}}
    end
  end

  defp typed_parameter(type, value) when type in ["String", "Long String"] and is_binary(value),
    do: {:ok, {:literal, :simple, value}}

  defp typed_parameter("\\{String\\}", values) when is_list(values) do
    if Enum.all?(values, &is_binary/1) do
      {:ok, Enum.map(values, &{:literal, :simple, &1})}
    else
      {:error, :invalid_string_set}
    end
  end

  defp typed_parameter("Date", %Date{} = date),
    do: {:ok, typed_literal(Date.to_iso8601(date), "date")}

  defp typed_parameter("Date", value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> typed_parameter("Date", date)
      {:error, _reason} -> {:error, :invalid_date}
    end
  end

  defp typed_parameter("DateTime", %DateTime{} = timestamp),
    do: {:ok, typed_literal(DateTime.to_iso8601(timestamp), "dateTime")}

  defp typed_parameter("DateTime", value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, timestamp, 0} -> typed_parameter("DateTime", timestamp)
      _other -> {:error, :invalid_datetime}
    end
  end

  defp typed_parameter(type, value)
       when type in ["ID", "32-bit Integer", "64-bit Integer"] and is_integer(value),
       do: {:ok, typed_literal(Integer.to_string(value), "integer")}

  defp typed_parameter("32-bit Float", value) when is_integer(value) or is_float(value),
    do: {:ok, typed_literal(to_string(value), "double")}

  defp typed_parameter("Boolean", value) when is_boolean(value),
    do: {:ok, typed_literal(to_string(value), "boolean")}

  defp typed_parameter(_type, _value), do: {:error, :invalid_type}

  defp typed_literal(value, datatype),
    do: {:literal, :typed, value, @xsd <> datatype}

  defp normalize_rows(schema, rows) when is_list(rows) do
    rows
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {row, index}, {:ok, acc} ->
      case normalize_row(schema, row) do
        {:ok, normalized} -> {:cont, {:ok, [normalized | acc]}}
        {:error, reason} -> {:halt, {:error, {:invalid_query_result, index, reason}}}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      {:error, _} = error -> error
    end
  end

  defp normalize_rows(_schema, _rows), do: {:error, :invalid_query_rows}

  defp normalize_row(schema, row) when is_map(row) do
    Enum.reduce_while(schema, {:ok, %{}}, fn field, {:ok, acc} ->
      normalize_field(field, row, acc)
    end)
  end

  defp normalize_row(_schema, _row), do: {:error, :row_must_be_map}

  defp normalize_field(field, row, acc) do
    with {:ok, value} <- Map.fetch(row, field.name),
         {:ok, decoded} <- external_value(field.type, value) do
      {:cont, {:ok, Map.put(acc, field.name, decoded)}}
    else
      :error -> {:halt, {:error, {:missing_column, field.name}}}
      {:error, reason} -> {:halt, {:error, {field.name, reason}}}
    end
  end

  defp external_value(type, {:literal, :simple, value})
       when type in ["String", "Long String"],
       do: {:ok, value}

  defp external_value(type, {:literal, :lang, value, _language})
       when type in ["String", "Long String"],
       do: {:ok, value}

  defp external_value("32-bit Float", {:literal, :typed, value, _datatype}),
    do: parse_float(value)

  defp external_value(_type, {:literal, :typed, value, _datatype}), do: {:ok, value}
  defp external_value(_type, value), do: {:ok, value}

  defp parse_float(value) when is_float(value), do: {:ok, value}
  defp parse_float(value) when is_integer(value), do: {:ok, value * 1.0}

  defp parse_float(value) when is_binary(value) do
    case Float.parse(value) do
      {float, ""} -> {:ok, float}
      _other -> {:error, :invalid_float}
    end
  end

  defp parse_float(_value), do: {:error, :invalid_float}
end
