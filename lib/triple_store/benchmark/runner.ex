defmodule TripleStore.Benchmark.Runner do
  @moduledoc """
  Benchmark execution infrastructure for measuring triple store performance.

  Provides a unified interface for running LUBM and BSBM benchmarks with
  configurable warmup iterations, metric collection, and output formats.

  ## Features

  - Warmup iterations before measurement
  - Latency percentiles (p50, p95, p99)
  - Throughput metrics (queries/sec, triples/sec)
  - Structured output (JSON, CSV)

  ## Usage

      # Run LUBM benchmark
      {:ok, results} = Runner.run(db, :lubm, scale: 1, iterations: 100)

      # Run BSBM benchmark with warmup
      {:ok, results} = Runner.run(db, :bsbm, scale: 100, warmup: 10, iterations: 100)

      # Export results
      Runner.to_json(results)
      Runner.to_csv(results)

  ## Result Structure

  Results include:
  - Per-query latency statistics
  - Aggregate throughput metrics
  - Percentile distributions
  - Query metadata

  """

  alias TripleStore.Benchmark.{LUBMQueries, BSBMQueries}

  @type benchmark :: :lubm | :bsbm
  @type query_result :: %{
          query_id: atom(),
          query_name: String.t(),
          iterations: pos_integer(),
          latencies_us: [non_neg_integer()],
          p50_us: non_neg_integer(),
          p95_us: non_neg_integer(),
          p99_us: non_neg_integer(),
          min_us: non_neg_integer(),
          max_us: non_neg_integer(),
          mean_us: float(),
          std_dev_us: float(),
          queries_per_sec: float(),
          result_count: non_neg_integer() | :error
        }

  @type benchmark_result :: %{
          benchmark: benchmark(),
          started_at: DateTime.t(),
          completed_at: DateTime.t(),
          duration_ms: non_neg_integer(),
          scale: pos_integer(),
          warmup_iterations: non_neg_integer(),
          measurement_iterations: pos_integer(),
          query_results: [query_result()],
          aggregate: %{
            total_queries: pos_integer(),
            total_time_us: non_neg_integer(),
            queries_per_sec: float(),
            p50_us: non_neg_integer(),
            p95_us: non_neg_integer(),
            p99_us: non_neg_integer()
          }
        }

  @type run_opts :: [
          scale: pos_integer(),
          warmup: non_neg_integer(),
          iterations: pos_integer(),
          queries: [atom()],
          params: keyword()
        ]

  @doc """
  Runs a benchmark suite against the triple store.

  ## Arguments

  - `db` - The triple store database handle
  - `benchmark` - The benchmark to run (`:lubm` or `:bsbm`)

  ## Options

  - `:scale` - Scale factor for data generation (default: 1)
  - `:warmup` - Number of warmup iterations (default: 5)
  - `:iterations` - Number of measurement iterations per query (default: 10)
  - `:queries` - List of query IDs to run (default: all queries)
  - `:params` - Additional parameters for query substitution

  ## Returns

  `{:ok, benchmark_result()}` on success, `{:error, reason}` on failure.

  ## Examples

      {:ok, results} = Runner.run(db, :lubm, scale: 1, iterations: 100)
      {:ok, results} = Runner.run(db, :bsbm, scale: 100, queries: [:q1, :q2, :q7])

  """
  @spec run(term(), benchmark(), run_opts()) :: {:ok, benchmark_result()} | {:error, term()}
  def run(db, benchmark, opts \\ []) when benchmark in [:lubm, :bsbm] do
    scale = Keyword.get(opts, :scale, 1)
    warmup = Keyword.get(opts, :warmup, 5)
    iterations = Keyword.get(opts, :iterations, 10)
    query_ids = Keyword.get(opts, :queries, nil)
    params = Keyword.get(opts, :params, [])

    started_at = DateTime.utc_now()

    # Get query templates
    queries = get_queries(benchmark, query_ids, params)

    # Run warmup phase
    if warmup > 0 do
      run_warmup(db, queries, warmup)
    end

    # Run measurement phase
    query_results = run_measurements(db, queries, iterations)

    completed_at = DateTime.utc_now()
    duration_ms = DateTime.diff(completed_at, started_at, :millisecond)

    # Calculate aggregate statistics
    aggregate = calculate_aggregate(query_results)

    result = %{
      benchmark: benchmark,
      started_at: started_at,
      completed_at: completed_at,
      duration_ms: duration_ms,
      scale: scale,
      warmup_iterations: warmup,
      measurement_iterations: iterations,
      query_results: query_results,
      aggregate: aggregate
    }

    {:ok, result}
  end

  @doc """
  Converts benchmark results to JSON format.

  ## Examples

      json = Runner.to_json(results)

  """
  @spec to_json(benchmark_result()) :: String.t()
  def to_json(result) do
    result
    |> prepare_for_serialization()
    |> Jason.encode!(pretty: true)
  end

  @doc """
  Converts benchmark results to CSV format.

  Returns a CSV string with one row per query containing:
  - query_id, query_name, iterations, p50_us, p95_us, p99_us, min_us, max_us, mean_us, qps

  ## Examples

      csv = Runner.to_csv(results)

  """
  @spec to_csv(benchmark_result()) :: String.t()
  def to_csv(result) do
    header = "query_id,query_name,iterations,p50_us,p95_us,p99_us,min_us,max_us,mean_us,std_dev_us,queries_per_sec,result_count\n"

    rows =
      Enum.map_join(result.query_results, "\n", fn qr ->
        [
          qr.query_id,
          escape_csv(qr.query_name),
          qr.iterations,
          qr.p50_us,
          qr.p95_us,
          qr.p99_us,
          qr.min_us,
          qr.max_us,
          Float.round(qr.mean_us, 2),
          Float.round(qr.std_dev_us, 2),
          Float.round(qr.queries_per_sec, 2),
          format_result_count(qr.result_count)
        ]
        |> Enum.join(",")
      end)

    header <> rows
  end

  @doc """
  Calculates percentile from a sorted list of values.

  ## Examples

      Runner.percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 50)
      # => 5

      Runner.percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 95)
      # => 10

  """
  @spec percentile([number()], number()) :: number()
  def percentile([], _p), do: 0

  def percentile(values, p) when p >= 0 and p <= 100 do
    sorted = Enum.sort(values)
    n = length(sorted)
    rank = p / 100 * (n - 1)
    lower = trunc(rank)
    upper = min(lower + 1, n - 1)
    fraction = rank - lower

    lower_val = Enum.at(sorted, lower)
    upper_val = Enum.at(sorted, upper)

    trunc(lower_val + fraction * (upper_val - lower_val))
  end

  @doc """
  Formats a duration in microseconds to a human-readable string.

  ## Examples

      Runner.format_duration(1234)
      # => "1.23ms"

      Runner.format_duration(1234567)
      # => "1.23s"

  """
  @spec format_duration(non_neg_integer()) :: String.t()
  def format_duration(us) when us < 1000, do: "#{us}µs"
  def format_duration(us) when us < 1_000_000, do: "#{Float.round(us / 1000, 2)}ms"
  def format_duration(us), do: "#{Float.round(us / 1_000_000, 2)}s"

  @doc """
  Prints a summary of benchmark results to stdout.
  """
  @spec print_summary(benchmark_result()) :: :ok
  def print_summary(result) do
    IO.puts("\n=== #{String.upcase(to_string(result.benchmark))} Benchmark Results ===")
    IO.puts("Scale: #{result.scale}")
    IO.puts("Warmup: #{result.warmup_iterations} iterations")
    IO.puts("Measurement: #{result.measurement_iterations} iterations per query")
    IO.puts("Duration: #{result.duration_ms}ms")
    IO.puts("")

    IO.puts("Query Results:")
    IO.puts(String.duplicate("-", 80))

    for qr <- result.query_results do
      IO.puts("#{qr.query_id}: #{qr.query_name}")
      IO.puts("  p50: #{format_duration(qr.p50_us)}, p95: #{format_duration(qr.p95_us)}, p99: #{format_duration(qr.p99_us)}")
      IO.puts("  min: #{format_duration(qr.min_us)}, max: #{format_duration(qr.max_us)}, mean: #{format_duration(trunc(qr.mean_us))}")
      IO.puts("  QPS: #{Float.round(qr.queries_per_sec, 1)}, results: #{format_result_count(qr.result_count)}")
      IO.puts("")
    end

    IO.puts(String.duplicate("-", 80))
    IO.puts("Aggregate:")
    IO.puts("  Total queries: #{result.aggregate.total_queries}")
    IO.puts("  Total time: #{format_duration(result.aggregate.total_time_us)}")
    IO.puts("  Overall QPS: #{Float.round(result.aggregate.queries_per_sec, 1)}")
    IO.puts("  p50: #{format_duration(result.aggregate.p50_us)}, p95: #{format_duration(result.aggregate.p95_us)}, p99: #{format_duration(result.aggregate.p99_us)}")

    :ok
  end

  # ===========================================================================
  # Private: Query Retrieval
  # ===========================================================================

  defp get_queries(:lubm, nil, params) do
    LUBMQueries.all()
    |> Enum.map(fn q ->
      {:ok, substituted} = LUBMQueries.get(q.id, params)
      substituted
    end)
  end

  defp get_queries(:lubm, query_ids, params) when is_list(query_ids) do
    query_ids
    |> Enum.map(fn id ->
      {:ok, q} = LUBMQueries.get(id, params)
      q
    end)
  end

  defp get_queries(:bsbm, nil, params) do
    BSBMQueries.all()
    |> Enum.map(fn q ->
      {:ok, substituted} = BSBMQueries.get(q.id, params)
      substituted
    end)
  end

  defp get_queries(:bsbm, query_ids, params) when is_list(query_ids) do
    query_ids
    |> Enum.map(fn id ->
      {:ok, q} = BSBMQueries.get(id, params)
      q
    end)
  end

  # ===========================================================================
  # Private: Warmup Phase
  # ===========================================================================

  defp run_warmup(db, queries, iterations) do
    for _i <- 1..iterations, query <- queries do
      execute_query(db, query.sparql)
    end

    :ok
  end

  # ===========================================================================
  # Private: Measurement Phase
  # ===========================================================================

  defp run_measurements(db, queries, iterations) do
    Enum.map(queries, fn query ->
      {latencies, result_count} = measure_query(db, query.sparql, iterations)

      stats = calculate_stats(latencies)

      %{
        query_id: query.id,
        query_name: query.name,
        iterations: iterations,
        latencies_us: latencies,
        p50_us: stats.p50,
        p95_us: stats.p95,
        p99_us: stats.p99,
        min_us: stats.min,
        max_us: stats.max,
        mean_us: stats.mean,
        std_dev_us: stats.std_dev,
        queries_per_sec: stats.qps,
        result_count: result_count
      }
    end)
  end

  defp measure_query(db, sparql, iterations) do
    {latencies, result_counts} =
      1..iterations
      |> Enum.map(fn _i ->
        {time_us, result} = :timer.tc(fn -> execute_query(db, sparql) end)

        result_count =
          case result do
            {:ok, results} when is_list(results) -> length(results)
            {:ok, %{results: results}} when is_list(results) -> length(results)
            _ -> :error
          end

        {time_us, result_count}
      end)
      |> Enum.unzip()

    # Use first successful result count
    result_count = Enum.find(result_counts, :error, fn c -> c != :error end)

    {latencies, result_count}
  end

  defp execute_query(db, sparql) do
    # Try to execute the query using the SPARQL query interface
    # This is a placeholder - the actual implementation depends on the store's API
    try do
      case TripleStore.SPARQL.Query.execute(db, sparql) do
        {:ok, results} -> {:ok, results}
        {:error, reason} -> {:error, reason}
      end
    rescue
      e -> {:error, e}
    end
  end

  # ===========================================================================
  # Private: Statistics Calculation
  # ===========================================================================

  defp calculate_stats([]), do: %{p50: 0, p95: 0, p99: 0, min: 0, max: 0, mean: 0.0, std_dev: 0.0, qps: 0.0}

  defp calculate_stats(latencies) do
    sorted = Enum.sort(latencies)
    n = length(latencies)
    sum = Enum.sum(latencies)
    mean = sum / n

    variance =
      latencies
      |> Enum.map(fn x -> (x - mean) * (x - mean) end)
      |> Enum.sum()
      |> Kernel./(n)

    std_dev = :math.sqrt(variance)

    # QPS based on mean latency
    qps = if mean > 0, do: 1_000_000 / mean, else: 0.0

    %{
      p50: percentile(sorted, 50),
      p95: percentile(sorted, 95),
      p99: percentile(sorted, 99),
      min: Enum.min(sorted),
      max: Enum.max(sorted),
      mean: mean,
      std_dev: std_dev,
      qps: qps
    }
  end

  defp calculate_aggregate(query_results) do
    all_latencies =
      query_results
      |> Enum.flat_map(& &1.latencies_us)

    total_queries = length(all_latencies)
    total_time_us = Enum.sum(all_latencies)

    qps = if total_time_us > 0, do: total_queries * 1_000_000 / total_time_us, else: 0.0

    %{
      total_queries: total_queries,
      total_time_us: total_time_us,
      queries_per_sec: qps,
      p50_us: percentile(all_latencies, 50),
      p95_us: percentile(all_latencies, 95),
      p99_us: percentile(all_latencies, 99)
    }
  end

  # ===========================================================================
  # Private: Serialization Helpers
  # ===========================================================================

  defp prepare_for_serialization(result) do
    %{
      benchmark: result.benchmark,
      started_at: DateTime.to_iso8601(result.started_at),
      completed_at: DateTime.to_iso8601(result.completed_at),
      duration_ms: result.duration_ms,
      scale: result.scale,
      warmup_iterations: result.warmup_iterations,
      measurement_iterations: result.measurement_iterations,
      query_results:
        Enum.map(result.query_results, fn qr ->
          %{
            query_id: qr.query_id,
            query_name: qr.query_name,
            iterations: qr.iterations,
            p50_us: qr.p50_us,
            p95_us: qr.p95_us,
            p99_us: qr.p99_us,
            min_us: qr.min_us,
            max_us: qr.max_us,
            mean_us: Float.round(qr.mean_us, 2),
            std_dev_us: Float.round(qr.std_dev_us, 2),
            queries_per_sec: Float.round(qr.queries_per_sec, 2),
            result_count: format_result_count(qr.result_count)
          }
        end),
      aggregate: %{
        total_queries: result.aggregate.total_queries,
        total_time_us: result.aggregate.total_time_us,
        queries_per_sec: Float.round(result.aggregate.queries_per_sec, 2),
        p50_us: result.aggregate.p50_us,
        p95_us: result.aggregate.p95_us,
        p99_us: result.aggregate.p99_us
      }
    }
  end

  defp escape_csv(str) do
    if String.contains?(str, [",", "\"", "\n"]) do
      "\"#{String.replace(str, "\"", "\"\"")}\""
    else
      str
    end
  end

  defp format_result_count(:error), do: "error"
  defp format_result_count(count) when is_integer(count), do: count
end
