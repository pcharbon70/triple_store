defmodule TripleStore.Benchmark.LDBC.Result do
  @moduledoc """
  Canonical typed rows for LDBC correctness and artifact processing.

  Rows retain declared column order and duplicate multiplicity. Unordered
  answers may be canonicalized explicitly; ordered answers are never reordered.
  """

  alias TripleStore.Benchmark.LDBC.{Codec, Operation}

  @enforce_keys [:columns, :types, :rows, :ordering]
  defstruct [:columns, :types, :rows, :ordering]

  @type t :: %__MODULE__{
          columns: [String.t()],
          types: [Codec.canonical_type()],
          rows: [[term()]],
          ordering: map()
        }

  @doc "Converts result maps into canonical typed rows and checks every declared column."
  @spec from_maps(Operation.t(), [map()]) :: {:ok, t()} | {:error, term()}
  def from_maps(%Operation{} = operation, rows) when is_list(rows) do
    columns = Operation.result_columns(operation)
    types = Enum.map(operation.result_schema, & &1.type)

    with {:ok, decoded_rows} <- decode_rows(rows, operation.result_schema) do
      {:ok,
       %__MODULE__{
         columns: columns,
         types: types,
         rows: decoded_rows,
         ordering: operation.ordering
       }}
    end
  end

  @doc "Sorts rows only for an explicitly unordered result contract."
  @spec canonicalize(t()) :: {:ok, t()} | {:error, :sorting_not_permitted}
  def canonicalize(%__MODULE__{ordering: %{mode: :unordered}} = result) do
    {:ok,
     %{result | rows: Enum.sort_by(result.rows, &:erlang.term_to_binary(&1, [:deterministic]))}}
  end

  def canonicalize(%__MODULE__{ordering: %{mode: :not_applicable}} = result), do: {:ok, result}
  def canonicalize(%__MODULE__{}), do: {:error, :sorting_not_permitted}

  defp decode_rows(rows, schema) do
    rows
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {row, index}, {:ok, acc} ->
      case decode_row(row, schema) do
        {:ok, values} -> {:cont, {:ok, [values | acc]}}
        {:error, reason} -> {:halt, {:error, {:invalid_result_row, index, reason}}}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end
  end

  defp decode_row(row, schema) when is_map(row) do
    expected = MapSet.new(Enum.map(schema, & &1.name))
    actual = MapSet.new(Map.keys(row))
    missing = MapSet.difference(expected, actual)
    unexpected = MapSet.difference(actual, expected)

    cond do
      MapSet.size(missing) > 0 ->
        {:error, {:missing_columns, missing |> MapSet.to_list() |> Enum.sort()}}

      MapSet.size(unexpected) > 0 ->
        {:error, {:unexpected_columns, unexpected |> MapSet.to_list() |> Enum.sort()}}

      true ->
        schema
        |> Enum.map(fn field -> Codec.decode(field.type, Map.fetch!(row, field.name)) end)
        |> collect()
    end
  end

  defp decode_row(_row, _schema), do: {:error, :row_must_be_a_map}

  defp collect(values) do
    Enum.reduce_while(values, {:ok, []}, fn
      {:ok, value}, {:ok, acc} -> {:cont, {:ok, [value | acc]}}
      {:error, reason}, _acc -> {:halt, {:error, reason}}
    end)
    |> case do
      {:ok, value} -> {:ok, Enum.reverse(value)}
      error -> error
    end
  end
end
