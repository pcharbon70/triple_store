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

      Keyword.has_key?(opts, :numeric_tolerance) ->
        compare_tolerant_hashed(expected, actual, Keyword.fetch!(opts, :numeric_tolerance))

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

    equal? =
      if ordered? do
        rows_equal?(expected.rows, actual.rows, opts)
      else
        unordered_rows_equal?(expected.rows, actual.rows, opts)
      end

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

  defp compare_tolerant_hashed(expected, actual, tolerance) do
    mode = if expected.ordering.mode == :ordered, do: :ordered, else: :multiset
    expected_hash = digest(expected.rows, mode)
    actual_hash = digest(actual.rows, mode)

    cond do
      mode == :multiset ->
        failure(:tolerant_comparison_too_large, expected, actual, %{
          comparison: :hash,
          mode: mode,
          numeric_tolerance: tolerance,
          expected_hash: expected_hash,
          actual_hash: actual_hash
        })

      pairwise_equal?(expected.rows, actual.rows, tolerance) ->
        success(
          expected,
          actual,
          {:tolerant_hash, mode, tolerance, expected_hash, actual_hash}
        )

      true ->
        failure(:hash_mismatch, expected, actual, %{
          comparison: :hash,
          mode: mode,
          numeric_tolerance: tolerance,
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

  defp unordered_rows_equal?(expected, actual, opts) do
    case Keyword.fetch(opts, :numeric_tolerance) do
      {:ok, tolerance} -> unordered_pairwise_equal?(expected, actual, tolerance)
      :error -> frequencies(expected) == frequencies(actual)
    end
  end

  defp unordered_pairwise_equal?(expected, actual, tolerance)
       when length(expected) == length(actual) do
    expected
    |> Enum.with_index()
    |> Enum.reduce_while(%{}, fn {_row, expected_index}, matches ->
      case augment_match(expected_index, %{}, matches, expected, actual, tolerance) do
        {:ok, matches} -> {:cont, matches}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      :error -> false
      matches -> map_size(matches) == length(expected)
    end
  end

  defp unordered_pairwise_equal?(_expected, _actual, _tolerance), do: false

  defp row_equal?(left, right, tolerance) when length(left) == length(right) do
    Enum.zip(left, right)
    |> Enum.all?(fn {left_value, right_value} ->
      value_equal?(left_value, right_value, tolerance)
    end)
  end

  defp row_equal?(_left, _right, _tolerance), do: false

  defp augment_match(expected_index, seen, matches, expected, actual, tolerance) do
    expected_row = Enum.at(expected, expected_index)
    context = %{expected: expected, actual: actual, tolerance: tolerance}

    actual
    |> Enum.with_index()
    |> Enum.reduce_while(:error, fn {actual_row, actual_index}, _result ->
      attempt_match(
        expected_row,
        expected_index,
        actual_row,
        actual_index,
        seen,
        matches,
        context
      )
    end)
  end

  defp attempt_match(
         expected_row,
         expected_index,
         actual_row,
         actual_index,
         seen,
         matches,
         context
       ) do
    if not Map.has_key?(seen, actual_index) and
         row_equal?(expected_row, actual_row, context.tolerance) do
      assign_match(
        expected_index,
        actual_index,
        Map.put(seen, actual_index, true),
        matches,
        context
      )
    else
      {:cont, :error}
    end
  end

  defp assign_match(
         expected_index,
         actual_index,
         seen,
         matches,
         context
       ) do
    case Map.fetch(matches, actual_index) do
      :error ->
        {:halt, {:ok, Map.put(matches, actual_index, expected_index)}}

      {:ok, displaced_index} ->
        reassign_match(
          displaced_index,
          expected_index,
          actual_index,
          seen,
          matches,
          context
        )
    end
  end

  defp reassign_match(
         displaced_index,
         expected_index,
         actual_index,
         seen,
         matches,
         context
       ) do
    case augment_match(
           displaced_index,
           seen,
           matches,
           context.expected,
           context.actual,
           context.tolerance
         ) do
      {:ok, reassigned} ->
        {:halt, {:ok, Map.put(reassigned, actual_index, expected_index)}}

      :error ->
        {:cont, :error}
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
