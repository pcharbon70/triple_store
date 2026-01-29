defmodule TripleStore.Benchmark.Targets do
  @moduledoc """
  Performance targets for the TripleStore benchmark suite.

  Defines measurable performance goals and provides validation functions
  to check whether benchmark results meet the defined targets.

  ## Performance Targets

  | Target | Metric | Threshold | Dataset |
  |--------|--------|-----------|---------|
  | Simple Query | p95 latency | <10ms | WatDiv (any scale) |
  | Complex Query | p95 latency | <100ms | WatDiv (any scale) |
  | Bulk Load | throughput | >100K triples/sec | any |
  | Query Mix | p95 latency | <50ms | WatDiv (any scale) |

  ## Usage

      # Check individual targets
      Targets.check_simple_query(p95_us: 5000)
      # => :pass

      Targets.check_bulk_load(triples_per_sec: 50000)
      # => {:fail, "throughput 50K triples/sec below target >100K"}

      # Validate bulk load performance
      {:ok, report} = Targets.validate_bulk_load(100_000, 1000)
      Targets.print_report(report)

  """

  @typedoc "Target identifier"
  @type target_id :: :simple_query | :complex_query | :bulk_load | :query_mix

  @typedoc "Target definition"
  @type target :: %{
          id: target_id(),
          name: String.t(),
          description: String.t(),
          metric: :p95_latency | :throughput,
          threshold: number(),
          unit: :microseconds | :milliseconds | :triples_per_sec,
          operator: :lt | :gt
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

  @simple_query_p95_us 10_000
  @complex_query_p95_us 100_000
  @bulk_load_tps 100_000
  @query_mix_p95_us 50_000

  @doc """
  Returns all defined performance targets.
  """
  @spec all() :: [target()]
  def all do
    [
      simple_query_target(),
      complex_query_target(),
      bulk_load_target(),
      query_mix_target()
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

  # ===========================================================================
  # Target Definitions
  # ===========================================================================

  @doc """
  Returns the simple query target.

  Target: p95 latency <10ms

  Applies to WatDiv linear queries (L1-L5).
  """
  @spec simple_query_target() :: target()
  def simple_query_target do
    %{
      id: :simple_query,
      name: "Simple Query",
      description: "Linear WatDiv queries (L1-L5) - single pattern queries",
      metric: :p95_latency,
      threshold: @simple_query_p95_us,
      unit: :microseconds,
      operator: :lt
    }
  end

  @doc """
  Returns the complex query target.

  Target: p95 latency <100ms

  Applies to WatDiv snowflake (F1-F5) and complex (C1-C3) queries.
  """
  @spec complex_query_target() :: target()
  def complex_query_target do
    %{
      id: :complex_query,
      name: "Complex Query",
      description: "WatDiv snowflake and complex queries with 3+ patterns",
      metric: :p95_latency,
      threshold: @complex_query_p95_us,
      unit: :microseconds,
      operator: :lt
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
      operator: :gt
    }
  end

  @doc """
  Returns the query mix target.

  Target: p95 latency <50ms for overall query mix

  Applies to aggregate WatDiv query performance.
  """
  @spec query_mix_target() :: target()
  def query_mix_target do
    %{
      id: :query_mix,
      name: "Query Mix",
      description: "Overall p95 latency for WatDiv query mix (all 20 queries)",
      metric: :p95_latency,
      threshold: @query_mix_p95_us,
      unit: :microseconds,
      operator: :lt
    }
  end

  # ===========================================================================
  # Target Checking
  # ===========================================================================

  @doc """
  Checks if simple query performance meets the target.

  ## Options

  - `:p95_us` - The p95 latency in microseconds (required)

  ## Examples

      Targets.check_simple_query(p95_us: 5000)
      # => :pass

      Targets.check_simple_query(p95_us: 15000)
      # => {:fail, "p95 latency 15.0ms exceeds target <10ms"}

  """
  @spec check_simple_query(keyword()) :: check_result()
  def check_simple_query(opts) do
    p95_us = Keyword.fetch!(opts, :p95_us)
    check_latency(p95_us, @simple_query_p95_us)
  end

  @doc """
  Checks if complex query performance meets the target.

  ## Options

  - `:p95_us` - The p95 latency in microseconds (required)

  """
  @spec check_complex_query(keyword()) :: check_result()
  def check_complex_query(opts) do
    p95_us = Keyword.fetch!(opts, :p95_us)
    check_latency(p95_us, @complex_query_p95_us)
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
  Checks if query mix performance meets the target.

  ## Options

  - `:p95_us` - The aggregate p95 latency in microseconds (required)

  """
  @spec check_query_mix(keyword()) :: check_result()
  def check_query_mix(opts) do
    p95_us = Keyword.fetch!(opts, :p95_us)
    check_latency(p95_us, @query_mix_p95_us)
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
  # Private Helpers
  # ===========================================================================

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

  defp format_result(:pass), do: "PASS"
  defp format_result({:fail, _}), do: "FAIL"

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
