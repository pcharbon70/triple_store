defmodule TripleStore.Benchmark.LDBC.SNB.BI.ResultContract do
  @moduledoc """
  Strict result typing, stable BI ordering, limits, and bag semantics.

  Duplicate rows are retained. Values are decoded against the upstream BI
  widths before ordering so lexical RDF representation cannot affect answers.
  """

  alias TripleStore.Benchmark.LDBC.Codec

  @doc "Converts result maps to typed rows and applies canonical ordering and limit."
  @spec materialize(map(), [map()]) :: {:ok, map()} | {:error, term()}
  def materialize(definition, rows) when is_list(rows) do
    schema = Enum.map(definition.result, &Map.put(&1, :type, canonical_type(&1.type)))

    with {:ok, typed} <- decode_rows(rows, schema) do
      ordered = order(typed, definition.ordering)
      limited = if is_integer(definition.limit), do: Enum.take(ordered, definition.limit), else: ordered

      {:ok,
       %{
         columns: Enum.map(schema, & &1.name),
         types: Enum.map(schema, & &1.type),
         rows: Enum.map(limited, fn row -> Enum.map(schema, &Map.fetch!(row, &1.name)) end),
         ordering: definition.ordering,
         duplicate_semantics: :bag
       }}
    end
  end

  defp decode_rows(rows, schema) do
    rows
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {row, index}, {:ok, acc} ->
      case decode_row(row, schema) do
        {:ok, typed} -> {:cont, {:ok, [typed | acc]}}
        {:error, reason} -> {:halt, {:error, {:invalid_bi_result, index, reason}}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      error -> error
    end
  end

  defp decode_row(row, schema) when is_map(row) do
    expected = schema |> Enum.map(& &1.name) |> MapSet.new()
    actual = row |> Map.keys() |> MapSet.new()

    if expected == actual do
      Enum.reduce_while(schema, {:ok, %{}}, fn field, {:ok, typed} ->
        case Codec.decode(field.type, Map.fetch!(row, field.name)) do
          {:ok, value} -> {:cont, {:ok, Map.put(typed, field.name, value)}}
          {:error, reason} -> {:halt, {:error, {field.name, reason}}}
        end
      end)
    else
      {:error, {:column_mismatch, MapSet.to_list(expected), MapSet.to_list(actual)}}
    end
  end

  defp decode_row(_row, _schema), do: {:error, :row_must_be_map}

  defp order(rows, ordering) do
    Enum.sort(rows, fn left, right -> compare(left, right, ordering) end)
  end

  defp compare(left, right, ordering) do
    Enum.reduce_while(ordering, false, fn %{name: name, direction: direction}, _acc ->
      case term_compare(Map.fetch!(left, name), Map.fetch!(right, name)) do
        :eq -> {:cont, false}
        :lt -> {:halt, direction == "asc"}
        :gt -> {:halt, direction == "desc"}
      end
    end)
  end

  defp term_compare(left, right) when left == right, do: :eq
  defp term_compare(left, right) when left < right, do: :lt
  defp term_compare(_left, _right), do: :gt

  defp canonical_type("ID"), do: :id
  defp canonical_type("32-bit Integer"), do: {:integer, 32}
  defp canonical_type("64-bit Integer"), do: {:integer, 64}
  defp canonical_type("32-bit Float"), do: :float32
  defp canonical_type("64-bit Float"), do: :float64
  defp canonical_type("Boolean"), do: :boolean
  defp canonical_type("Date"), do: :date
  defp canonical_type("DateTime"), do: :timestamp
  defp canonical_type(type) when type in ["String", "Long String"], do: :string
  defp canonical_type("\\{String\\}"), do: {:list, :string}
end
