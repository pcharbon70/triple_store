defmodule TripleStore.Benchmark.Targets do
  @moduledoc """
  Performance targets for the triple store benchmark suite.

  Defines measurable performance goals and provides validation functions
  to check whether benchmark results meet the defined targets.

  ## Performance Targets

  | Target | Metric | Threshold | Dataset |
  |--------|--------|-----------|---------|
  | Simple BGP | p95 latency | <10ms | 1M triples |
  | Complex Join | p95 latency | <100ms | 1M triples |
  | Bulk Load | throughput | >100K triples/sec | any |
  | BSBM Mix | p95 latency | <50ms | 1M triples |

  ## Usage

      # Get all targets
      targets = Targets.all()

      # Validate benchmark results
      {:ok, validation} = Targets.validate(benchmark_results)

      # Check specific target
      Targets.check_simple_bgp(p95_us: 5000)
      # => :pass

      Targets.check_simple_bgp(p95_us: 15000)
      # => {:fail, "p95 latency 15.0ms exceeds target <10ms"}

  """

  @typedoc "Target identifier"
  @type target_id :: :simple_bgp | :complex_join | :bulk_load | :bsbm_mix

  @typedoc "Target definition"
  @type target :: %{
          id: target_id(),
          name: String.t(),
          description: String.t(),
          metric: :p95_latency | :throughput,
          threshold: number(),
          unit: :microseconds | :milliseconds | :triples_per_sec,
          operator: :lt | :gt,
          dataset_size: pos_integer() | :any
        }

  @typedoc "Validation result for a single target"
  @type check_result :: :pass | {:fail, String.t()}

  @typedoc "Full validation report"
  @type validation_report :: %{
          passed: boolean(),
          targets_checked: pos_integer(),
          targets_passed: pos_integer(),
          targets_failed: pos_integer(),
          results: [%{target: target_id(), result: check_result(), value: number()}]
        }

  # Performance target thresholds
  # All latency values in microseconds for consistency

  @simple_bgp_p95_us 10_000
  @complex_join_p95_us 100_000
  @bulk_load_tps 100_000
  @bsbm_mix_p95_us 50_000

  # Reference dataset size (1 million triples)
  @reference_dataset_size 1_000_000

  @doc """
  Returns all defined performance targets.
  """
  @spec all() :: [target()]
  def all do
    [
      simple_bgp_target(),
      complex_join_target(),
      bulk_load_target(),
      bsbm_mix_target()
    ]
  end

  @doc """
  Returns a specific target by ID.
  """
  @spec get(target_id()) :: {:ok, target()} | {:error, :not_found}
  def get(id) when is_atom(id) do
    case Enum.find(all(), fn t -> t.id == id end) do
      nil -> {:error, :not_found}
      target -> {:ok, target}
    end
  end

  @doc """
  Returns the reference dataset size for performance targets (1M triples).
  """
  @spec reference_dataset_size() :: pos_integer()
  def reference_dataset_size, do: @reference_dataset_size

  # ===========================================================================
  # Target Definitions
  # ===========================================================================

  @doc """
  Returns the simple BGP query target.

  Target: p95 latency <10ms on 1M triples
  """
  @spec simple_bgp_target() :: target()
  def simple_bgp_target do
    %{
      id: :simple_bgp,
      name: "Simple BGP Query",
      description: "Single triple pattern query with one bound term",
      metric: :p95_latency,
      threshold: @simple_bgp_p95_us,
      unit: :microseconds,
      operator: :lt,
      dataset_size: @reference_dataset_size
    }
  end

  @doc """
  Returns the complex join query target.

  Target: p95 latency <100ms on 1M triples
  """
  @spec complex_join_target() :: target()
  def complex_join_target do
    %{
      id: :complex_join,
      name: "Complex Join Query",
      description: "Multi-pattern query with 3+ triple patterns and joins",
      metric: :p95_latency,
      threshold: @complex_join_p95_us,
      unit: :microseconds,
      operator: :lt,
      dataset_size: @reference_dataset_size
    }
  end

  @doc """
  Returns the bulk load throughput target.

  Target: >100K triples/second
  """
  @spec bulk_load_target() :: target()
  def bulk_load_target do
    %{
      id: :bulk_load,
      name: "Bulk Load Throughput",
      description: "Rate of triple insertion during bulk loading",
      metric: :throughput,
      threshold: @bulk_load_tps,
      unit: :triples_per_sec,
      operator: :gt,
      dataset_size: :any
    }
  end

  @doc """
  Returns the BSBM query mix target.

  Target: p95 latency <50ms for overall query mix
  """
  @spec bsbm_mix_target() :: target()
  def bsbm_mix_target do
    %{
      id: :bsbm_mix,
      name: "BSBM Query Mix",
      description: "Overall p95 latency for BSBM benchmark query mix",
      metric: :p95_latency,
      threshold: @bsbm_mix_p95_us,
      unit: :microseconds,
      operator: :lt,
      dataset_size: @reference_dataset_size
    }
  end

  # ===========================================================================
  # Target Checking
  # ===========================================================================

  @doc """
  Checks if simple BGP query performance meets the target.

  ## Options

  - `:p95_us` - The p95 latency in microseconds (required)

  ## Examples

      Targets.check_simple_bgp(p95_us: 5000)
      # => :pass

      Targets.check_simple_bgp(p95_us: 15000)
      # => {:fail, "p95 latency 15.0ms exceeds target <10ms"}

  """
  @spec check_simple_bgp(keyword()) :: check_result()
  def check_simple_bgp(opts) do
    p95_us = Keyword.fetch!(opts, :p95_us)
    check_latency(p95_us, @simple_bgp_p95_us)
  end

  @doc """
  Checks if complex join query performance meets the target.

  ## Options

  - `:p95_us` - The p95 latency in microseconds (required)

  """
  @spec check_complex_join(keyword()) :: check_result()
  def check_complex_join(opts) do
    p95_us = Keyword.fetch!(opts, :p95_us)
    check_latency(p95_us, @complex_join_p95_us)
  end

  @doc """
  Checks if bulk load throughput meets the target.

  ## Options

  - `:triples_per_sec` - The loading throughput (required)

  """
  @spec check_bulk_load(keyword()) :: check_result()
  def check_bulk_load(opts) do
    tps = Keyword.fetch!(opts, :triples_per_sec)
    check_throughput(tps, @bulk_load_tps)
  end

  @doc """
  Checks if BSBM query mix performance meets the target.

  ## Options

  - `:p95_us` - The aggregate p95 latency in microseconds (required)

  """
  @spec check_bsbm_mix(keyword()) :: check_result()
  def check_bsbm_mix(opts) do
    p95_us = Keyword.fetch!(opts, :p95_us)
    check_latency(p95_us, @bsbm_mix_p95_us)
  end

  @doc """
  Validates benchmark results against all applicable targets.

  Takes a benchmark result from `Runner.run/3` and checks each target
  that can be evaluated from the results.

  ## Returns

  A validation report with pass/fail status for each checked target.

  """
  @spec validate(map()) :: {:ok, validation_report()}
  def validate(benchmark_result) do
    results =
      case benchmark_result.benchmark do
        :lubm -> validate_lubm(benchmark_result)
        :bsbm -> validate_bsbm(benchmark_result)
        _ -> []
      end

    passed_count = Enum.count(results, fn r -> r.result == :pass end)
    failed_count = Enum.count(results, fn r -> r.result != :pass end)

    report = %{
      passed: failed_count == 0,
      targets_checked: length(results),
      targets_passed: passed_count,
      targets_failed: failed_count,
      results: results
    }

    {:ok, report}
  end

  @doc """
  Validates bulk load performance.

  ## Arguments

  - `triple_count` - Number of triples loaded
  - `duration_ms` - Time taken in milliseconds

  ## Returns

  A validation report for the bulk load target.

  """
  @spec validate_bulk_load(pos_integer(), pos_integer()) :: {:ok, validation_report()}
  def validate_bulk_load(triple_count, duration_ms) do
    tps = triple_count / (duration_ms / 1000)
    result = check_bulk_load(triples_per_sec: tps)

    report = %{
      passed: result == :pass,
      targets_checked: 1,
      targets_passed: if(result == :pass, do: 1, else: 0),
      targets_failed: if(result == :pass, do: 0, else: 1),
      results: [%{target: :bulk_load, result: result, value: tps}]
    }

    {:ok, report}
  end

  @doc """
  Formats a validation report as a human-readable string.
  """
  @spec format_report(validation_report()) :: String.t()
  def format_report(report) do
    status = if report.passed, do: "PASSED", else: "FAILED"

    header = """
    === Performance Target Validation ===
    Status: #{status}
    Targets: #{report.targets_passed}/#{report.targets_checked} passed
    """

    details =
      Enum.map_join(report.results, "\n", fn r ->
        {:ok, target} = get(r.target)
        status_str = format_result(r.result)
        value_str = format_value(r.value, target.unit)
        threshold_str = format_threshold(target)

        "  #{target.name}: #{status_str} (#{value_str}, target: #{threshold_str})"
      end)

    header <> "\n" <> details
  end

  @doc """
  Prints a validation report to stdout.
  """
  @spec print_report(validation_report()) :: :ok
  def print_report(report) do
    IO.puts(format_report(report))
    :ok
  end

  # ===========================================================================
  # Private: Validation Helpers
  # ===========================================================================

  defp validate_lubm(result) do
    # For LUBM, we check simple and complex queries based on query characteristics
    simple_queries = [:q3, :q14]
    complex_queries = [:q2, :q7, :q8, :q9]

    simple_p95 = aggregate_p95(result.query_results, simple_queries)
    complex_p95 = aggregate_p95(result.query_results, complex_queries)

    results = []

    results =
      if simple_p95 do
        check = check_simple_bgp(p95_us: simple_p95)
        [%{target: :simple_bgp, result: check, value: simple_p95} | results]
      else
        results
      end

    results =
      if complex_p95 do
        check = check_complex_join(p95_us: complex_p95)
        [%{target: :complex_join, result: check, value: complex_p95} | results]
      else
        results
      end

    Enum.reverse(results)
  end

  defp validate_bsbm(result) do
    # For BSBM, we check overall query mix performance
    p95 = result.aggregate.p95_us

    check = check_bsbm_mix(p95_us: p95)
    [%{target: :bsbm_mix, result: check, value: p95}]
  end

  defp aggregate_p95(query_results, query_ids) do
    relevant =
      query_results
      |> Enum.filter(fn qr -> qr.query_id in query_ids end)
      |> Enum.flat_map(& &1.latencies_us)

    if Enum.empty?(relevant) do
      nil
    else
      TripleStore.Benchmark.Runner.percentile(relevant, 95)
    end
  end

  defp check_latency(actual_us, threshold_us) do
    if actual_us < threshold_us do
      :pass
    else
      actual_ms = Float.round(actual_us / 1000, 1)
      threshold_ms = Float.round(threshold_us / 1000, 1)
      {:fail, "p95 latency #{actual_ms}ms exceeds target <#{threshold_ms}ms"}
    end
  end

  defp check_throughput(actual_tps, threshold_tps) do
    if actual_tps > threshold_tps do
      :pass
    else
      {:fail,
       "throughput #{format_number(actual_tps)} triples/sec below target >#{format_number(threshold_tps)}"}
    end
  end

  defp format_result(:pass), do: "✓ PASS"
  defp format_result({:fail, _}), do: "✗ FAIL"

  defp format_value(value, :microseconds), do: "#{Float.round(value / 1000, 2)}ms"
  defp format_value(value, :milliseconds), do: "#{Float.round(value, 2)}ms"
  defp format_value(value, :triples_per_sec), do: "#{format_number(value)} triples/sec"

  defp format_threshold(target) do
    op = if target.operator == :lt, do: "<", else: ">"

    case target.unit do
      :microseconds -> "#{op}#{Float.round(target.threshold / 1000, 0)}ms"
      :milliseconds -> "#{op}#{target.threshold}ms"
      :triples_per_sec -> "#{op}#{format_number(target.threshold)} triples/sec"
    end
  end

  defp format_number(num) when is_float(num), do: format_number(trunc(num))

  defp format_number(num) when num >= 1_000_000 do
    "#{Float.round(num / 1_000_000, 1)}M"
  end

  defp format_number(num) when num >= 1000 do
    "#{Float.round(num / 1000, 1)}K"
  end

  defp format_number(num), do: to_string(num)
end
