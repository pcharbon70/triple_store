defmodule TripleStore.SPARQL.Benchmark do
  @moduledoc """
  Performance benchmarking suite for SPARQL queries (S12).

  Provides utilities for:
  - Measuring query execution time
  - Comparing query performance across multiple runs
  - Memory profiling
  - Generating benchmark reports

  ## Examples

      # Benchmark a single query
      Benchmark.measure(fn ->
        TripleStore.query(db, "SELECT * WHERE { ?s ?p ?o }")
      end)

      # Benchmark with multiple iterations
      Benchmark.run(query_fn, iterations: 100)

      # Compare two implementations
      Benchmark.compare(fn -> old_implementation() end, fn -> new_implementation() end)
  """

  @type measurement :: %{
          iterations: pos_integer(),
          total_us: integer(),
          avg_us: float(),
          min_us: integer(),
          max_us: integer(),
          median_us: float(),
          p95_us: float(),
          p99_us: float(),
          memory_mb: float()
        }

  @type comparison :: %{
          a: measurement(),
          b: measurement(),
          speedup: float(),
          winner: :a | :b | :tie
        }

  @type report :: %{
          name: String.t(),
          measurement: measurement(),
          timestamp: integer()
        }

  @doc """
  Measures the execution time of a function once.
  """
  @spec measure((-> any())) :: {any(), integer()}
  def measure(fun) when is_function(fun, 0) do
    {time, result} = :timer.tc(fun)
    {result, time}
  end

  @doc """
  Measures a function with memory profiling.
  """
  @spec measure_with_memory((-> any())) :: {any(), integer(), float()}
  def measure_with_memory(fun) when is_function(fun, 0) do
    :erlang.garbage_collect()

    memory_before = :erlang.memory(:total)
    {time, result} = :timer.tc(fun)
    memory_after = :erlang.memory(:total)

    memory_used_mb = (memory_after - memory_before) / 1_048_576

    {result, time, memory_used_mb}
  end

  @doc """
  Runs a benchmark with multiple iterations.

  ## Options
  - `:iterations` - Number of iterations (default: 10)
  - `:warmup` - Number of warmup iterations (default: 0)
  - `:memory` - Whether to track memory usage (default: false)
  """
  @spec run((-> any()), keyword()) :: measurement()
  def run(fun, opts \\ []) when is_function(fun, 0) do
    iterations = Keyword.get(opts, :iterations, 10)
    warmup = Keyword.get(opts, :warmup, 0)
    track_memory = Keyword.get(opts, :memory, false)

    # Warmup runs (not measured)
    for _ <- 1..warmup do
      fun.()
    end

    # Actual measurements
    measurements =
      if track_memory do
        for _ <- 1..iterations do
          {_result, time, memory} = measure_with_memory(fun)
          {time, memory}
        end
      else
        for _ <- 1..iterations do
          {_result, time} = measure(fun)
          {time, nil}
        end
      end

    times = Enum.map(measurements, fn {time, _mem} -> time end)
    memory_values = Enum.map(measurements, fn {_time, mem} -> mem end) |> Enum.reject(&is_nil/1)

    %{
      iterations: iterations,
      total_us: Enum.sum(times),
      avg_us: average(times),
      min_us: Enum.min(times),
      max_us: Enum.max(times),
      median_us: median(times),
      p95_us: percentile(times, 95),
      p99_us: percentile(times, 99),
      memory_mb: if(length(memory_values) > 0, do: average(memory_values), else: 0.0)
    }
  end

  @doc """
  Compares two functions and returns comparison statistics.
  """
  @spec compare((-> any()), (-> any()), keyword()) :: comparison()
  def compare(fun_a, fun_b, opts \\ []) do
    iterations = Keyword.get(opts, :iterations, 10)

    measure_a = run(fun_a, iterations: iterations)
    measure_b = run(fun_b, iterations: iterations)

    avg_a = measure_a.avg_us
    avg_b = measure_b.avg_us

    speedup = avg_a / avg_b

    winner =
      cond do
        speedup > 1.05 -> :b
        speedup < 0.95 -> :a
        true -> :tie
      end

    %{
      a: measure_a,
      b: measure_b,
      speedup: speedup,
      winner: winner
    }
  end

  @doc """
  Runs a benchmark suite with multiple named functions.

  Returns a list of reports with names and measurements.
  """
  @spec suite([{String.t(), (-> any())}], keyword()) :: [report()]
  def suite(functions, opts \\ []) when is_list(functions) do
    iterations = Keyword.get(opts, :iterations, 10)

    Enum.map(functions, fn {name, fun} ->
      measurement = run(fun, iterations: iterations)

      %{
        name: name,
        measurement: measurement,
        timestamp: System.system_time(:millisecond)
      }
    end)
  end

  @doc """
  Formats a measurement as a human-readable string.
  """
  @spec format_measurement(measurement()) :: String.t()
  def format_measurement(measurement) do
    [
      "Iterations: #{measurement.iterations}",
      "Total: #{format_duration(measurement.total_us)}",
      "Average: #{format_duration(measurement.avg_us)}",
      "Min: #{format_duration(measurement.min_us)}",
      "Max: #{format_duration(measurement.max_us)}",
      "Median: #{format_duration(measurement.median_us)}",
      "P95: #{format_duration(measurement.p95_us)}",
      "P99: #{format_duration(measurement.p99_us)}",
      "Memory: #{:erlang.float_to_binary(measurement.memory_mb, decimals: 2)} MB"
    ]
    |> Enum.join("\n")
  end

  @doc """
  Formats a comparison as a human-readable string.
  """
  @spec format_comparison(comparison()) :: String.t()
  def format_comparison(comparison) do
    speedup_str =
      if comparison.speedup >= 1 do
        "#{:erlang.float_to_binary(comparison.speedup, decimals: 2)}x faster"
      else
        "#{:erlang.float_to_binary(1 / comparison.speedup, decimals: 2)}x slower"
      end

    winner_str =
      case comparison.winner do
        :a -> "A is faster"
        :b -> "B is faster"
        :tie -> "No significant difference"
      end

    """
    A (#{format_duration(comparison.a.avg_us)} avg)
    vs
    B (#{format_duration(comparison.b.avg_us)} avg)

    #{winner_str} (#{speedup_str})
    """
    |> String.trim()
  end

  @doc """
  Formats a suite report as a human-readable string.
  """
  @spec format_suite([report()]) :: String.t()
  def format_suite(reports) do
    reports
    |> Enum.map(fn report ->
      """
      ## #{report.name}

      #{format_measurement(report.measurement)}
      """
    end)
    |> Enum.join("\n\n")
  end

  @doc """
  Exports benchmark results as CSV.
  """
  @spec export_csv([report()], String.t()) :: :ok
  def export_csv(reports, path) when is_list(reports) and is_binary(path) do
    headers = [
      "Name",
      "Iterations",
      "Avg (μs)",
      "Min (μs)",
      "Max (μs)",
      "Median (μs)",
      "P95 (μs)",
      "P99 (μs)",
      "Memory (MB)"
    ]

    rows =
      Enum.map(reports, fn report ->
        m = report.measurement

        [
          report.name,
          to_string(m.iterations),
          format_float(m.avg_us),
          to_string(m.min_us),
          to_string(m.max_us),
          format_float(m.median_us),
          format_float(m.p95_us),
          format_float(m.p99_us),
          :erlang.float_to_binary(m.memory_mb, decimals: 2)
        ]
      end)

    csv_content =
      [headers | rows]
      |> Enum.map(&encode_csv_row/1)
      |> Enum.join("\n")

    File.write!(path, csv_content)
    :ok
  end

  defp format_float(val) when is_float(val) do
    :erlang.float_to_binary(val, decimals: 2)
  end

  defp format_float(val), do: to_string(val)

  defp encode_csv_row(row) do
    row
    |> Enum.map(fn
      val when is_binary(val) ->
        if String.contains?(val, [",", "\"", "\n"]) do
          "\"#{String.replace(val, "\"", "\"\"")}\""
        else
          val
        end

      val ->
        to_string(val)
    end)
    |> Enum.join(",")
  end

  @doc """
  Runs a stress test with increasing iterations.
  """
  @spec stress_test((-> any()), keyword()) :: [{pos_integer(), measurement()}]
  def stress_test(fun, opts \\ []) when is_function(fun, 0) do
    max_iterations = Keyword.get(opts, :max_iterations, 1000)
    step = Keyword.get(opts, :step, 10)

    Stream.iterate(step, &(&1 + step))
    |> Stream.take_while(&(&1 <= max_iterations))
    |> Enum.map(fn iterations ->
      measurement = run(fun, iterations: iterations, warmup: 1)
      {iterations, measurement}
    end)
  end

  @doc """
  Profiles memory usage over multiple runs.
  """
  @spec memory_profile((-> any()), keyword()) :: %{
          samples: [float()],
          avg_mb: float(),
          max_mb: float()
        }
  def memory_profile(fun, opts \\ []) when is_function(fun, 0) do
    samples = Keyword.get(opts, :samples, 10)

    memory_samples =
      for _ <- 1..samples do
        {_result, _time, memory} = measure_with_memory(fun)
        memory
      end

    %{
      samples: memory_samples,
      avg_mb: average(memory_samples),
      max_mb: Enum.max(memory_samples)
    }
  end

  @doc """
  Formats a duration in microseconds to a human-readable string.
  """
  @spec format_duration(integer() | float()) :: String.t()
  def format_duration(us) when us < 1000 do
    "#{round(us)}μs"
  end

  def format_duration(us) when us < 1_000_000 do
    ms = us / 1000
    "#{:erlang.float_to_binary(ms, decimals: 2)}ms"
  end

  def format_duration(us) do
    s = us / 1_000_000
    "#{:erlang.float_to_binary(s, decimals: 2)}s"
  end

  # Private helpers

  defp average(list) when is_list(list) do
    Enum.sum(list) / length(list)
  end

  defp median(list) when is_list(list) do
    sorted = Enum.sort(list)
    len = length(sorted)

    if rem(len, 2) == 0 do
      mid = div(len, 2)
      (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
    else
      Enum.at(sorted, div(len, 2))
    end
  end

  defp percentile(list, p) when is_list(list) and is_integer(p) do
    sorted = Enum.sort(list)
    len = length(sorted)
    index = trunc(p / 100 * len)

    Enum.at(sorted, max(0, min(index, len - 1)))
  end
end
