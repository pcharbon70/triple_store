defmodule TripleStore.Benchmark.LDBC.Correctness do
  @moduledoc """
  Compares canonical LDBC answers without hiding order, type, or duplicates.

  Small answers retain full difference evidence. Large answers use row counts
  plus SHA-256 ordered or multiset digests, selected from the operation contract.
  """

  alias TripleStore.Benchmark.LDBC.Result

  @doc "Compares expected and actual canonical results."
  @spec compare(Result.t(), Result.t(), keyword()) :: map()
  def compare(%Result{} = expected, %Result{} = actual, opts \\ []) do
    threshold = Keyword.get(opts, :full_comparison_limit, 10_000)

    cond do
      expected.columns != actual.columns or expected.types != actual.types ->
        failure(:mistyped, expected, actual, %{
          expected_columns: expected.columns,
          actual_columns: actual.columns,
          expected_types: expected.types,
          actual_types: actual.types
        })

      expected.ordering != actual.ordering ->
        failure(:contract_mismatch, expected, actual, %{})

      max(length(expected.rows), length(actual.rows)) <= threshold ->
        compare_full(expected, actual, opts)

      true ->
        compare_hashed(expected, actual)
    end
    |> apply_accepted_divergence(Keyword.get(opts, :accepted_divergence))
  end

  @doc "Validates an accepted divergence record against a pinned source version."
  @spec validate_divergence(map(), String.t()) :: :ok | {:error, term()}
  def validate_divergence(
        %{permitted?: true, reason: reason, source_version: version},
        expected_version
      )
      when is_binary(reason) and reason != "" and version == expected_version,
      do: :ok

  def validate_divergence(_divergence, _expected_version),
    do: {:error, :invalid_accepted_divergence}

  defp compare_full(expected, actual, opts) do
    ordered? = expected.ordering.mode == :ordered
    equal? = rows_equal?(expected.rows, actual.rows, opts)

    cond do
      equal? ->
        success(expected, actual, :full)

      not ordered? and frequencies(expected.rows) == frequencies(actual.rows) ->
        success(expected, actual, :full)

      ordered? and frequencies(expected.rows) == frequencies(actual.rows) ->
        failure(:misordered, expected, actual, %{
          first_difference: first_difference(expected.rows, actual.rows)
        })

      true ->
        expected_counts = frequencies(expected.rows)
        actual_counts = frequencies(actual.rows)

        missing = multiset_difference(expected_counts, actual_counts)
        unexpected = multiset_difference(actual_counts, expected_counts)

        category =
          if numeric_only?(missing, unexpected), do: :numerically_divergent, else: :row_mismatch

        failure(category, expected, actual, %{
          missing: missing,
          unexpected: unexpected,
          comparison: :full
        })
    end
  end

  defp compare_hashed(expected, actual) do
    mode = if expected.ordering.mode == :ordered, do: :ordered, else: :multiset
    expected_hash = digest(expected.rows, mode)
    actual_hash = digest(actual.rows, mode)

    if length(expected.rows) == length(actual.rows) and expected_hash == actual_hash do
      success(expected, actual, {:hash, mode, expected_hash})
    else
      failure(:hash_mismatch, expected, actual, %{
        comparison: :hash,
        mode: mode,
        expected_hash: expected_hash,
        actual_hash: actual_hash
      })
    end
  end

  defp rows_equal?(expected, actual, opts) do
    if Keyword.get(opts, :numeric_tolerance) do
      pairwise_equal?(expected, actual, Keyword.fetch!(opts, :numeric_tolerance))
    else
      expected == actual
    end
  end

  defp pairwise_equal?(expected, actual, tolerance) when length(expected) == length(actual) do
    Enum.zip(expected, actual)
    |> Enum.all?(fn {expected_row, actual_row} ->
      length(expected_row) == length(actual_row) and
        Enum.zip(expected_row, actual_row)
        |> Enum.all?(fn {left, right} -> value_equal?(left, right, tolerance) end)
    end)
  end

  defp pairwise_equal?(_expected, _actual, _tolerance), do: false

  defp value_equal?(left, right, tolerance) when is_number(left) and is_number(right),
    do: abs(left - right) <= tolerance

  defp value_equal?(left, right, _tolerance), do: left == right

  defp frequencies(rows), do: Enum.frequencies(rows)

  defp multiset_difference(left, right) do
    Enum.flat_map(left, fn {row, count} ->
      case max(count - Map.get(right, row, 0), 0) do
        0 -> []
        difference -> [%{row: row, count: difference}]
      end
    end)
  end

  defp numeric_only?(missing, unexpected) do
    missing != [] and unexpected != [] and
      Enum.all?(missing ++ unexpected, fn %{row: row} -> Enum.any?(row, &is_number/1) end)
  end

  defp first_difference(expected, actual) do
    Enum.zip(expected, actual)
    |> Enum.with_index()
    |> Enum.find_value(fn {{left, right}, index} ->
      if left != right, do: %{index: index, expected: left, actual: right}
    end)
  end

  defp digest(rows, :ordered), do: hash(rows)

  defp digest(rows, :multiset) do
    rows
    |> Enum.map(&hash/1)
    |> Enum.sort()
    |> hash()
  end

  defp hash(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp success(expected, actual, comparison) do
    %{
      status: :correct,
      category: nil,
      expected_count: length(expected.rows),
      actual_count: length(actual.rows),
      comparison: comparison,
      details: %{}
    }
  end

  defp failure(category, expected, actual, details) do
    %{
      status: :incorrect,
      category: category,
      expected_count: length(expected.rows),
      actual_count: length(actual.rows),
      comparison: Map.get(details, :comparison, :full),
      details: details
    }
  end

  defp apply_accepted_divergence(
         %{status: :incorrect} = result,
         %{permitted?: true, reason: reason, source_version: version} = divergence
       )
       when is_binary(reason) and reason != "" and is_binary(version) do
    %{
      result
      | status: :accepted_divergence,
        details: Map.put(result.details, :accepted_divergence, divergence)
    }
  end

  defp apply_accepted_divergence(result, _divergence), do: result
end
