defmodule TripleStore.Benchmark.Wikidata.Correctness do
  @moduledoc """
  Divergence analysis for Wikidata benchmark runs.

  This module compares captured benchmark answers against reference baselines and
  attaches per-query correctness summaries that later reporting layers can
  aggregate.
  """

  alias TripleStore.Benchmark.Wikidata.{AcceptedDivergence, Baseline}

  @type correctness_status ::
          :match | :divergent | :accepted_divergence | :missing_reference | :not_comparable

  @type divergence_classification ::
          :parser
          | :optimizer
          | :paths
          | :duplicates
          | :datatype_handling
          | :limit_or_distinct_semantics
          | :blank_node_handling
          | :ordering_variation
          | :execution_failure
          | nil

  @type correctness_summary :: %{
          benchmark_id: String.t(),
          execution_variant: atom(),
          status: correctness_status(),
          classification: divergence_classification(),
          accepted: boolean(),
          answer_fingerprint: String.t() | nil,
          reference_fingerprint: String.t() | nil,
          actual_row_count: non_neg_integer() | nil,
          reference_row_count: non_neg_integer() | nil,
          divergence_count: non_neg_integer(),
          exemplars: [map()]
        }

  @doc """
  Attaches correctness summaries to each query run in a benchmark result.
  """
  @spec attach(map(), keyword()) :: {:ok, map()} | {:error, term()}
  def attach(run_result, opts \\ [])

  def attach(%{query_runs: query_runs} = run_result, opts) when is_list(query_runs) do
    baseline = Keyword.get(opts, :baseline)
    accepted_divergences = Keyword.get(opts, :accepted_divergences, [])

    with :ok <- validate_baseline(baseline),
         :ok <- validate_accepted_divergences(accepted_divergences) do
      {:ok,
       %{
         run_result
         | query_runs:
             Enum.map(query_runs, fn query_run ->
               correctness =
                 compare_query_run(
                   query_run,
                   baseline_entry(baseline, query_run.benchmark_id, query_run.execution_variant),
                   accepted_divergences
                 )

               Map.put(query_run, :correctness, correctness)
             end)
       }}
    end
  end

  def attach(_run_result, _opts), do: {:error, :invalid_run_result}

  @doc """
  Compares a query run against an optional baseline entry.
  """
  @spec compare_query_run(map(), map() | nil, [AcceptedDivergence.t()]) :: correctness_summary()
  def compare_query_run(query_run, baseline_entry, accepted_divergences \\ [])

  def compare_query_run(query_run, nil, _accepted_divergences) do
    %{
      benchmark_id: query_run.benchmark_id,
      execution_variant: query_run.execution_variant,
      status: :missing_reference,
      classification: nil,
      accepted: false,
      answer_fingerprint: query_run[:answer_record] && query_run.answer_record.fingerprint,
      reference_fingerprint: nil,
      actual_row_count: query_run[:answer_record] && query_run.answer_record.row_count,
      reference_row_count: nil,
      divergence_count: 0,
      exemplars: []
    }
  end

  def compare_query_run(query_run, baseline_entry, accepted_divergences) when is_map(query_run) do
    actual = Map.get(query_run, :answer_record)
    reference = Map.get(baseline_entry, :answer_record)

    comparison =
      cond do
        is_nil(actual) ->
          classify_failure_without_answer(query_run, reference)

        actual.fingerprint == reference.fingerprint ->
          match_summary(query_run, actual, reference)

        true ->
          divergent_summary(query_run, actual, reference)
      end

    if comparison.status == :divergent and accepted?(comparison, accepted_divergences) do
      %{comparison | status: :accepted_divergence, accepted: true}
    else
      comparison
    end
  end

  defp match_summary(query_run, actual, reference) do
    %{
      benchmark_id: query_run.benchmark_id,
      execution_variant: query_run.execution_variant,
      status: :match,
      classification: nil,
      accepted: false,
      answer_fingerprint: actual.fingerprint,
      reference_fingerprint: reference.fingerprint,
      actual_row_count: actual.row_count,
      reference_row_count: reference.row_count,
      divergence_count: 0,
      exemplars: []
    }
  end

  defp divergent_summary(query_run, actual, reference) do
    %{
      benchmark_id: query_run.benchmark_id,
      execution_variant: query_run.execution_variant,
      status: :divergent,
      classification: classify_divergence(query_run, actual, reference),
      accepted: false,
      answer_fingerprint: actual.fingerprint,
      reference_fingerprint: reference.fingerprint,
      actual_row_count: actual.row_count,
      reference_row_count: reference.row_count,
      divergence_count: divergence_count(actual, reference),
      exemplars: divergence_exemplars(actual, reference)
    }
  end

  defp classify_failure_without_answer(query_run, reference) do
    classification =
      cond do
        query_run.parser_error_count > 0 -> :parser
        query_run.execution_error_count > 0 -> :execution_failure
        query_run.timeout_count > 0 -> :optimizer
        query_run.failure_count > 0 -> :execution_failure
        true -> nil
      end

    %{
      benchmark_id: query_run.benchmark_id,
      execution_variant: query_run.execution_variant,
      status: :not_comparable,
      classification: classification,
      accepted: false,
      answer_fingerprint: nil,
      reference_fingerprint: reference && reference.fingerprint,
      actual_row_count: nil,
      reference_row_count: reference && reference.row_count,
      divergence_count: if(classification, do: 1, else: 0),
      exemplars: failure_exemplars(query_run)
    }
  end

  defp classify_divergence(query_run, actual, reference) do
    cond do
      actual.unordered_fingerprint == reference.unordered_fingerprint and
          actual.fingerprint != reference.fingerprint ->
        :ordering_variation

      actual.datatype_relaxed_fingerprint == reference.datatype_relaxed_fingerprint ->
        :datatype_handling

      actual.anonymous_blank_node_fingerprint == reference.anonymous_blank_node_fingerprint ->
        :blank_node_handling

      actual.distinct_fingerprint == reference.distinct_fingerprint and
          actual.row_count != reference.row_count ->
        :duplicates

      limit_or_distinct_sensitive?(query_run) ->
        :limit_or_distinct_semantics

      path_sensitive?(query_run) ->
        :paths

      query_run.parser_error_count > 0 ->
        :parser

      true ->
        :optimizer
    end
  end

  defp divergence_exemplars(actual, reference) do
    missing = list_diff(reference.normalized_rows, actual.normalized_rows, :missing)
    unexpected = list_diff(actual.normalized_rows, reference.normalized_rows, :unexpected)

    (missing ++ unexpected)
    |> Enum.take(3)
  end

  defp failure_exemplars(query_run) do
    query_run
    |> Map.get(:failures, [])
    |> Enum.take(3)
    |> Enum.map(fn failure ->
      %{
        type: :failure,
        iteration: failure.iteration,
        class: failure.class,
        message: failure.message
      }
    end)
  end

  defp divergence_count(actual, reference) do
    max(
      abs(actual.row_count - reference.row_count),
      missing_and_unexpected_count(actual, reference)
    )
  end

  defp missing_and_unexpected_count(actual, reference) do
    length(list_diff(reference.normalized_rows, actual.normalized_rows, :missing)) +
      length(list_diff(actual.normalized_rows, reference.normalized_rows, :unexpected))
  end

  defp list_diff(left, right, type) do
    right_frequencies = frequency_map(right)

    {rows, _remaining} =
      Enum.reduce(left, {[], right_frequencies}, fn row, {acc, frequencies} ->
        count = Map.get(frequencies, row, 0)

        if count > 0 do
          {acc, Map.put(frequencies, row, count - 1)}
        else
          {acc ++ [%{type: type, row: row}], frequencies}
        end
      end)

    rows
  end

  defp frequency_map(rows) do
    Enum.reduce(rows, %{}, fn row, acc -> Map.update(acc, row, 1, &(&1 + 1)) end)
  end

  defp accepted?(comparison, accepted_divergences) do
    Enum.any?(accepted_divergences, &AcceptedDivergence.matches?(&1, comparison))
  end

  defp limit_or_distinct_sensitive?(query_run) do
    query_run.execution_variant in [:count_only, :distinct_only] or
      String.contains?(query_run.normalized_query_text, " DISTINCT ") or
      String.contains?(String.upcase(query_run.normalized_query_text), " LIMIT ")
  end

  defp path_sensitive?(query_run) do
    query_run.shape in [:path_bgp, :property_path] or
      Enum.any?(query_run.feature_tags, &(&1 in [:property_path, "property_path"]))
  end

  defp baseline_entry(nil, _benchmark_id, _execution_variant), do: nil

  defp baseline_entry(baseline, benchmark_id, execution_variant) do
    case Baseline.lookup(baseline, benchmark_id, execution_variant) do
      {:ok, entry} -> entry
      {:error, :not_found} -> nil
    end
  end

  defp validate_baseline(nil), do: :ok
  defp validate_baseline(%{entries: entries}) when is_list(entries), do: :ok
  defp validate_baseline(_baseline), do: {:error, :invalid_baseline}

  defp validate_accepted_divergences(records) when is_list(records) do
    if Enum.all?(records, &match?(%AcceptedDivergence{}, &1)) do
      :ok
    else
      {:error, :invalid_accepted_divergences}
    end
  end
end
