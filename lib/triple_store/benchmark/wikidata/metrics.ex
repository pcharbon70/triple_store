defmodule TripleStore.Benchmark.Wikidata.Metrics do
  @moduledoc """
  Summary-statistics layer for Wikidata benchmark runs.

  This module turns the low-level runner output into stable report-friendly
  summaries at four levels:

  - per query
  - per workload family
  - per query-shape category
  - overall run totals
  """

  alias TripleStore.Benchmark.Wikidata.Runner

  @type timing_summary :: %{
          count: non_neg_integer(),
          total_us: non_neg_integer(),
          min_us: non_neg_integer(),
          q1_us: float(),
          median_us: float(),
          q3_us: float(),
          max_us: non_neg_integer(),
          mean_us: float(),
          p95_us: float(),
          p99_us: float()
        }

  @type error_totals :: %{
          parse_error: non_neg_integer(),
          execution_error: non_neg_integer(),
          timeout: non_neg_integer(),
          cancellation: non_neg_integer(),
          out_of_memory: non_neg_integer()
        }

  @type throughput_summary :: %{
          raw_queries_per_sec: float(),
          adjusted_iterations_per_sec: float()
        }

  @type query_summary :: %{
          benchmark_id: String.t(),
          query_name: String.t(),
          suite: atom(),
          category: atom() | String.t(),
          group: atom() | String.t() | nil,
          shape: atom(),
          execution_variant: atom(),
          source_id: String.t() | nil,
          feature_tags: [atom() | String.t()],
          answer_size_class: atom(),
          stress_points: [atom() | String.t()],
          measurement_iterations: pos_integer(),
          warmup_iterations: non_neg_integer(),
          timeout_ms: pos_integer(),
          completion_rate: float(),
          success_count: non_neg_integer(),
          failure_count: non_neg_integer(),
          penalty_count: non_neg_integer(),
          result_count: non_neg_integer() | nil,
          partial_failure_class: atom(),
          error_totals: error_totals(),
          raw_timing_summary: timing_summary(),
          adjusted_timing_summary: timing_summary(),
          throughput: throughput_summary(),
          divergence_count: non_neg_integer(),
          divergence_status: atom()
        }

  @type group_summary :: %{
          group_type: atom(),
          group_key: atom() | String.t(),
          query_count: non_neg_integer(),
          total_measurement_iterations: non_neg_integer(),
          total_successes: non_neg_integer(),
          total_failures: non_neg_integer(),
          completion_rate: float(),
          known_result_count_total: non_neg_integer(),
          penalty_count: non_neg_integer(),
          partial_failure_breakdown: map(),
          error_totals: error_totals(),
          raw_timing_summary: timing_summary(),
          adjusted_timing_summary: timing_summary(),
          throughput: throughput_summary(),
          divergence_count: non_neg_integer(),
          divergence_status: atom()
        }

  @type summary_report :: %{
          schema_version: pos_integer(),
          report_version: pos_integer(),
          report_id: String.t(),
          generated_at: DateTime.t(),
          run: %{
            schema_version: pos_integer(),
            run_kind: Runner.run_kind(),
            started_at: DateTime.t(),
            completed_at: DateTime.t(),
            duration_ms: non_neg_integer(),
            git_sha: String.t() | nil,
            target: map()
          },
          dataset_manifest: map() | nil,
          runtime_config: map(),
          runtime_metadata: map(),
          overall_summary: group_summary(),
          query_summaries: [query_summary()],
          suite_summaries: [group_summary()],
          aggregates: %{
            by_workload_family: [group_summary()],
            by_query_shape: [group_summary()]
          }
        }

  @doc """
  Builds a stable summary report from a Wikidata benchmark run result.
  """
  @spec summarize(Runner.run_result(), keyword()) :: {:ok, summary_report()} | {:error, term()}
  def summarize(run_result, opts \\ [])

  def summarize(%{query_runs: query_runs} = run_result, opts) when is_list(query_runs) do
    generated_at = normalize_generated_at(Keyword.get(opts, :generated_at, DateTime.utc_now()))
    report_id = Keyword.get(opts, :report_id, default_report_id(run_result, generated_at))

    suite_summaries =
      query_runs
      |> Enum.group_by(& &1.suite)
      |> summarize_groups(:suite)

    shape_summaries =
      query_runs
      |> Enum.group_by(&normalize_shape(&1.shape))
      |> summarize_groups(:shape)

    {:ok,
     %{
       schema_version: 1,
       report_version: 1,
       report_id: report_id,
       generated_at: generated_at,
       run: %{
         schema_version: Map.get(run_result, :schema_version, 1),
         run_kind: Map.fetch!(run_result, :run_kind),
         started_at: Map.fetch!(run_result, :started_at),
         completed_at: Map.fetch!(run_result, :completed_at),
         duration_ms: Map.fetch!(run_result, :duration_ms),
         git_sha: Map.get(run_result, :git_sha),
         target: Map.get(run_result, :target, %{})
       },
       dataset_manifest: Map.get(run_result, :dataset_manifest),
       runtime_config: Map.get(run_result, :runtime_config, %{}),
       runtime_metadata: Map.get(run_result, :runtime_metadata, %{}),
       overall_summary:
         summarize_group(:overall, Map.get(run_result, :run_kind, :unknown), query_runs),
       query_summaries: Enum.map(query_runs, &summarize_query_run/1),
       suite_summaries: suite_summaries,
       aggregates: %{
         by_workload_family: suite_summaries,
         by_query_shape: shape_summaries
       }
     }}
  rescue
    KeyError -> {:error, :invalid_run_result}
  end

  def summarize(_run_result, _opts), do: {:error, :invalid_run_result}

  @doc """
  Calculates a percentile using linear interpolation.
  """
  @spec percentile([number()], number()) :: float()
  def percentile([], _p), do: 0.0

  def percentile(values, p) when is_list(values) and is_number(p) and p >= 0 and p <= 100 do
    sorted = Enum.sort(values)
    n = length(sorted)
    rank = p / 100 * (n - 1)
    lower = floor(rank)
    upper = ceil(rank)
    fraction = rank - lower

    lower_val = Enum.at(sorted, lower) * 1.0
    upper_val = Enum.at(sorted, upper) * 1.0

    lower_val + fraction * (upper_val - lower_val)
  end

  defp summarize_groups(grouped_query_runs, group_type) do
    grouped_query_runs
    |> Enum.map(fn {group_key, query_runs} ->
      summarize_group(group_type, group_key, query_runs)
    end)
    |> Enum.sort_by(fn summary -> sort_key(summary.group_key) end)
  end

  defp summarize_group(group_type, group_key, query_runs) do
    raw_timings =
      query_runs
      |> Enum.flat_map(&Map.get(&1, :raw_timings_us, []))
      |> Enum.filter(&is_number/1)

    adjusted_timings =
      query_runs
      |> Enum.flat_map(&Map.get(&1, :adjusted_timings_us, []))
      |> Enum.filter(&is_number/1)

    total_measurement_iterations = Enum.sum(Enum.map(query_runs, & &1.measurement_iterations))
    total_successes = Enum.sum(Enum.map(query_runs, & &1.success_count))
    total_failures = Enum.sum(Enum.map(query_runs, & &1.failure_count))
    raw_timing_summary = timing_summary(raw_timings)
    adjusted_timing_summary = timing_summary(adjusted_timings)
    error_totals = group_error_totals(query_runs)

    %{
      group_type: group_type,
      group_key: group_key,
      query_count: length(query_runs),
      total_measurement_iterations: total_measurement_iterations,
      total_successes: total_successes,
      total_failures: total_failures,
      completion_rate: ratio(total_successes, total_measurement_iterations),
      known_result_count_total: known_result_count_total(query_runs),
      penalty_count: Enum.sum(Enum.map(query_runs, & &1.penalty_count)),
      partial_failure_breakdown: partial_failure_breakdown(query_runs),
      error_totals: error_totals,
      raw_timing_summary: raw_timing_summary,
      adjusted_timing_summary: adjusted_timing_summary,
      throughput: %{
        raw_queries_per_sec: queries_per_sec(total_successes, raw_timing_summary.total_us),
        adjusted_iterations_per_sec:
          queries_per_sec(total_measurement_iterations, adjusted_timing_summary.total_us)
      },
      divergence_count: 0,
      divergence_status: :not_evaluated
    }
  end

  defp summarize_query_run(query_run) do
    raw_timing_summary = timing_summary(query_run.raw_timings_us)
    adjusted_timing_summary = timing_summary(query_run.adjusted_timings_us)

    %{
      benchmark_id: query_run.benchmark_id,
      query_name: query_run.query_name,
      suite: query_run.suite,
      category: query_run.category,
      group: query_run.group,
      shape: normalize_shape(query_run.shape),
      execution_variant: query_run.execution_variant,
      source_id: query_run.source_id,
      feature_tags: query_run.feature_tags,
      answer_size_class: query_run.answer_size_class,
      stress_points: query_run.stress_points,
      measurement_iterations: query_run.measurement_iterations,
      warmup_iterations: query_run.warmup_iterations,
      timeout_ms: query_run.timeout_ms,
      completion_rate: query_run.completion_rate,
      success_count: query_run.success_count,
      failure_count: query_run.failure_count,
      penalty_count: query_run.penalty_count,
      result_count: query_run.result_count,
      partial_failure_class: query_run.partial_failure_class,
      error_totals: %{
        parse_error: query_run.parser_error_count,
        execution_error: query_run.execution_error_count,
        timeout: query_run.timeout_count,
        cancellation: query_run.cancellation_count,
        out_of_memory: query_run.out_of_memory_count
      },
      raw_timing_summary: raw_timing_summary,
      adjusted_timing_summary: adjusted_timing_summary,
      throughput: %{
        raw_queries_per_sec:
          queries_per_sec(query_run.success_count, raw_timing_summary.total_us),
        adjusted_iterations_per_sec:
          queries_per_sec(query_run.measurement_iterations, adjusted_timing_summary.total_us)
      },
      divergence_count: 0,
      divergence_status: :not_evaluated
    }
  end

  defp timing_summary(values) do
    numeric_values =
      values
      |> Enum.filter(&is_number/1)
      |> Enum.map(&Kernel.round/1)

    case numeric_values do
      [] ->
        %{
          count: 0,
          total_us: 0,
          min_us: 0,
          q1_us: 0.0,
          median_us: 0.0,
          q3_us: 0.0,
          max_us: 0,
          mean_us: 0.0,
          p95_us: 0.0,
          p99_us: 0.0
        }

      _ ->
        %{
          count: length(numeric_values),
          total_us: Enum.sum(numeric_values),
          min_us: Enum.min(numeric_values),
          q1_us: percentile(numeric_values, 25),
          median_us: percentile(numeric_values, 50),
          q3_us: percentile(numeric_values, 75),
          max_us: Enum.max(numeric_values),
          mean_us: Enum.sum(numeric_values) / length(numeric_values),
          p95_us: percentile(numeric_values, 95),
          p99_us: percentile(numeric_values, 99)
        }
    end
  end

  defp group_error_totals(query_runs) do
    %{
      parse_error: Enum.sum(Enum.map(query_runs, & &1.parser_error_count)),
      execution_error: Enum.sum(Enum.map(query_runs, & &1.execution_error_count)),
      timeout: Enum.sum(Enum.map(query_runs, & &1.timeout_count)),
      cancellation: Enum.sum(Enum.map(query_runs, & &1.cancellation_count)),
      out_of_memory: Enum.sum(Enum.map(query_runs, & &1.out_of_memory_count))
    }
  end

  defp partial_failure_breakdown(query_runs) do
    Enum.reduce(query_runs, %{}, fn query_run, acc ->
      Map.update(acc, query_run.partial_failure_class, 1, &(&1 + 1))
    end)
  end

  defp known_result_count_total(query_runs) do
    query_runs
    |> Enum.map(& &1.result_count)
    |> Enum.reject(&is_nil/1)
    |> Enum.sum()
  end

  defp queries_per_sec(_count, 0), do: 0.0
  defp queries_per_sec(count, total_us), do: count * 1_000_000 / total_us

  defp ratio(_numerator, 0), do: 0.0
  defp ratio(numerator, denominator), do: numerator / denominator

  defp normalize_shape(nil), do: :unknown
  defp normalize_shape(shape), do: shape

  defp default_report_id(run_result, generated_at) do
    tier =
      run_result
      |> Map.get(:runtime_config, %{})
      |> Map.get(:dataset_tier, :unknown)

    timestamp = Calendar.strftime(generated_at, "%Y%m%dT%H%M%SZ")
    "wikidata-#{tier}-#{run_result.run_kind}-#{timestamp}"
  end

  defp normalize_generated_at(%DateTime{} = generated_at), do: generated_at

  defp normalize_generated_at(generated_at) when is_binary(generated_at) do
    case DateTime.from_iso8601(generated_at) do
      {:ok, parsed, _offset} -> parsed
      _ -> DateTime.utc_now()
    end
  end

  defp normalize_generated_at(_generated_at), do: DateTime.utc_now()

  defp sort_key(value) when is_atom(value), do: Atom.to_string(value)
  defp sort_key(value) when is_binary(value), do: value
  defp sort_key(value), do: inspect(value)
end
