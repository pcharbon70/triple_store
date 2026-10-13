defmodule TripleStore.Benchmark.LDBC.SNB.BI.ResultContract do
  @moduledoc """
  Strict result typing, stable BI ordering, limits, and bag semantics.

  Duplicate rows are retained. Values are decoded against the upstream BI
  widths before ordering so lexical RDF representation cannot affect answers.
  """

  alias TripleStore.Benchmark.LDBC.{Codec, Result}

  @doc "Converts result maps to typed rows and applies canonical ordering and limit."
  @spec materialize(map(), [map()]) :: {:ok, Result.t()} | {:error, term()}
  def materialize(definition, rows) when is_list(rows) do
    schema = Enum.map(definition.result, &Map.put(&1, :type, canonical_type(&1.type)))

    with {:ok, typed} <- decode_rows(rows, schema) do
      ordered = order(typed, definition.ordering)

      limited =
        if is_integer(definition.limit), do: Enum.take(ordered, definition.limit), else: ordered

      {:ok,
       %Result{
         columns: Enum.map(schema, & &1.name),
         types: Enum.map(schema, & &1.type),
         rows: Enum.map(limited, fn row -> Enum.map(schema, &Map.fetch!(row, &1.name)) end),
         ordering: ordering_contract(definition.ordering),
         duplicate_semantics: :bag
       }}
    end
  end

  @doc "Decodes an accepted answer without repairing its order or applying its limit."
  @spec decode_reference(map(), [map()]) :: {:ok, Result.t()} | {:error, term()}
  def decode_reference(definition, rows) when is_list(rows) do
    schema = Enum.map(definition.result, &Map.put(&1, :type, canonical_type(&1.type)))

    with :ok <- validate_reference_limit(rows, definition.limit),
         {:ok, typed} <- decode_rows(rows, schema),
         :ok <- validate_reference_order(typed, definition.ordering) do
      {:ok, build_result(schema, typed, definition.ordering)}
    end
  end

  @doc "Adapts a materialized BI result to the shared LDBC result contract."
  @spec to_result(map(), Result.t() | map()) :: {:ok, Result.t()} | {:error, term()}
  def to_result(_definition, %Result{} = result), do: {:ok, result}

  def to_result(definition, %{
        columns: columns,
        types: types,
        rows: rows,
        ordering: ordering
      }) do
    expected_ordering = ordering_contract(definition.ordering)
    ordering = if is_list(ordering), do: ordering_contract(ordering), else: ordering

    {:ok,
     %Result{
       columns: columns,
       types: types,
       rows: rows,
       ordering: ordering,
       duplicate_semantics: :bag
     }}
    |> ensure_contract(definition, expected_ordering)
  end

  def to_result(_definition, _result), do: {:error, :invalid_bi_result_contract}

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

  defp validate_reference_limit(rows, limit) when is_integer(limit) and length(rows) > limit,
    do: {:error, {:result_limit_exceeded, limit, length(rows)}}

  defp validate_reference_limit(_rows, _limit), do: :ok

  defp validate_reference_order(_rows, []), do: :ok

  defp validate_reference_order(rows, ordering) do
    if rows == order(rows, ordering), do: :ok, else: {:error, :noncanonical_result_order}
  end

  defp build_result(schema, rows, ordering) do
    %Result{
      columns: Enum.map(schema, & &1.name),
      types: Enum.map(schema, & &1.type),
      rows: Enum.map(rows, fn row -> Enum.map(schema, &Map.fetch!(row, &1.name)) end),
      ordering: ordering_contract(ordering),
      duplicate_semantics: :bag
    }
  end

  defp decode_row(row, schema) when is_map(row) do
    expected = schema |> Enum.map(& &1.name) |> MapSet.new()
    actual = row |> Map.keys() |> MapSet.new()

    decode_matching_row(expected == actual, row, schema, expected, actual)
  end

  defp decode_row(_row, _schema), do: {:error, :row_must_be_map}

  defp decode_matching_row(true, row, schema, _expected, _actual) do
    Enum.reduce_while(schema, {:ok, %{}}, fn field, {:ok, typed} ->
      case Codec.decode(field.type, Map.fetch!(row, field.name)) do
        {:ok, value} -> {:cont, {:ok, Map.put(typed, field.name, value)}}
        {:error, reason} -> {:halt, {:error, {field.name, reason}}}
      end
    end)
  end

  defp decode_matching_row(false, _row, _schema, expected, actual),
    do: {:error, {:column_mismatch, MapSet.to_list(expected), MapSet.to_list(actual)}}

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

  defp ordering_contract([]), do: %{mode: :unordered, keys: []}

  defp ordering_contract(ordering) do
    %{
      mode: :ordered,
      keys:
        Enum.map(ordering, fn %{name: name, direction: direction} ->
          %{column: name, direction: ordering_direction(direction)}
        end)
    }
  end

  defp ordering_direction("asc"), do: :asc
  defp ordering_direction("desc"), do: :desc

  defp ensure_contract({:ok, result}, definition, expected_ordering) do
    expected_columns = Enum.map(definition.result, & &1.name)
    expected_types = Enum.map(definition.result, &canonical_type(&1.type))

    if result.columns == expected_columns and result.types == expected_types and
         result.ordering == expected_ordering do
      {:ok, result}
    else
      {:error, :bi_result_contract_mismatch}
    end
  end
end
