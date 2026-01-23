# Run BSBM, LUBM, and WatDiv benchmarks after Phase 4 optimizations
# Usage: mix run scripts/run_benchmarks.exs

alias TripleStore.Benchmark.{BSBM, LUBM, WatDiv, BSBMQueries, LUBMQueries, WatDivQueries}

IO.puts("\n" <> String.duplicate("=", 60))
IO.puts("Running Benchmarks - Post Phase 4 Optimizations")
IO.puts("Date: #{Date.utc_today()}")
IO.puts(String.duplicate("=", 60))

# Ensure tmp directory exists
File.mkdir_p!("./tmp")

# ===========================================================================
# Helper Functions
# ===========================================================================

defmodule BenchHelper do
  def percentile(values, p) when p >= 0 and p <= 100 do
    sorted = Enum.sort(values)
    n = length(sorted)
    if n == 0, do: 0, else: Enum.at(sorted, trunc(p / 100 * (n - 1)))
  end

  def format_duration(us) when us < 1000, do: "#{us}us"
  def format_duration(us) when us < 1_000_000, do: "#{Float.round(us / 1000, 2)}ms"
  def format_duration(us), do: "#{Float.round(us / 1_000_000, 2)}s"

  def pad_number(num, width) when is_number(num) do
    num_str = if is_float(num), do: "#{Float.round(num, 2)}", else: "#{num}"
    String.pad_leading(num_str, width)
  end

  def pad_number(num, width, decimals) when is_number(num) do
    num_str = "#{Float.round(num, decimals)}"
    String.pad_leading(num_str, width)
  end

  def pad_right(str, width) when is_binary(str) do
    String.pad_trailing(str, width)
  end

  def run_query(store, sparql, warmup, iterations) do
    # Warmup
    for _ <- 1..warmup do
      TripleStore.query(store, sparql)
    end

    # Measure
    {latencies, result_counts} =
      1..iterations
      |> Enum.map(fn _ ->
        {time_us, result} = :timer.tc(fn -> TripleStore.query(store, sparql) end)
        count = case result do
          {:ok, results} when is_list(results) -> length(results)
          _ -> :error
        end
        {time_us, count}
      end)
      |> Enum.unzip()

    result_count = Enum.find(result_counts, :error, &(&1 != :error))

    %{
      latencies: latencies,
      p50: percentile(latencies, 50),
      p95: percentile(latencies, 95),
      p99: percentile(latencies, 99),
      mean: if(length(latencies) > 0, do: Enum.sum(latencies) / length(latencies), else: 0),
      result_count: result_count
    }
  end
end

# ===========================================================================
# BSBM Benchmark
# ===========================================================================
IO.puts("\n\n>>> BSBM Benchmark (1000 products, ~141K triples) <<<\n")

bsbm_path = "./tmp/bsbm_bench_#{System.unique_integer([:positive])}"

IO.puts("Generating BSBM data (1000 products)...")
{time_us, graph} = :timer.tc(fn -> BSBM.generate(1000) end)
triple_count = length(RDF.Graph.triples(graph))
IO.puts("  Generated #{triple_count} triples in #{Float.round(time_us / 1000, 2)}ms")

IO.puts("Opening database at #{bsbm_path}...")
{:ok, store} = TripleStore.open(bsbm_path)

IO.puts("Loading data...")
{bsbm_load_time_us, _} = :timer.tc(fn -> TripleStore.load_graph(store, graph) end)
bsbm_load_throughput = triple_count / (bsbm_load_time_us / 1_000_000)
IO.puts("  Loaded in #{Float.round(bsbm_load_time_us / 1000, 2)}ms (#{trunc(bsbm_load_throughput)} triples/sec)")

warmup = 2
iterations = 5

IO.puts("\nRunning BSBM queries (warmup: #{warmup}, iterations: #{iterations})...\n")

bsbm_queries = BSBMQueries.all()
bsbm_results = for q <- bsbm_queries do
  {:ok, query} = BSBMQueries.get(q.id, [])
  stats = BenchHelper.run_query(store, query.sparql, warmup, iterations)

  p50_str = BenchHelper.format_duration(stats.p50)
  p95_str = BenchHelper.format_duration(stats.p95)
  result_str = if stats.result_count == :error, do: "error", else: "#{stats.result_count}"

  IO.puts("  #{q.id}: p50=#{p50_str}, p95=#{p95_str}, results=#{result_str}")

  Map.merge(stats, %{id: q.id, name: q.name})
end

# Calculate BSBM summary
bsbm_latencies = Enum.flat_map(bsbm_results, & &1.latencies)
bsbm_avg_p50 = Enum.sum(Enum.map(bsbm_results, & &1.p50)) / length(bsbm_results)
bsbm_avg_p95 = Enum.sum(Enum.map(bsbm_results, & &1.p95)) / length(bsbm_results)
bsbm_max_p95 = Enum.max(Enum.map(bsbm_results, & &1.p95))

IO.puts("\nBSBM Summary:")
IO.puts("  Load throughput: #{trunc(bsbm_load_throughput)} triples/sec")
IO.puts("  Average p50: #{BenchHelper.format_duration(trunc(bsbm_avg_p50))}")
IO.puts("  Average p95: #{BenchHelper.format_duration(trunc(bsbm_avg_p95))}")
IO.puts("  Max p95: #{BenchHelper.format_duration(bsbm_max_p95)}")

TripleStore.close(store)
File.rm_rf!(bsbm_path)

# ===========================================================================
# LUBM Benchmark
# ===========================================================================
IO.puts("\n\n>>> LUBM Benchmark (Scale 1, ~23K triples) <<<\n")

lubm_path = "./tmp/lubm_bench_#{System.unique_integer([:positive])}"

IO.puts("Generating LUBM data (scale 1)...")
{time_us, graph} = :timer.tc(fn -> LUBM.generate(1) end)
triple_count = length(RDF.Graph.triples(graph))
IO.puts("  Generated #{triple_count} triples in #{Float.round(time_us / 1000, 2)}ms")

IO.puts("Opening database at #{lubm_path}...")
{:ok, store} = TripleStore.open(lubm_path)

IO.puts("Loading data...")
{lubm_load_time_us, _} = :timer.tc(fn -> TripleStore.load_graph(store, graph) end)
lubm_load_throughput = triple_count / (lubm_load_time_us / 1_000_000)
IO.puts("  Loaded in #{Float.round(lubm_load_time_us / 1000, 2)}ms (#{trunc(lubm_load_throughput)} triples/sec)")

IO.puts("\nRunning LUBM queries (warmup: #{warmup}, iterations: #{iterations})...\n")

lubm_queries = LUBMQueries.all()
lubm_results = for q <- lubm_queries do
  {:ok, query} = LUBMQueries.get(q.id, [])
  stats = BenchHelper.run_query(store, query.sparql, warmup, iterations)

  p50_str = BenchHelper.format_duration(stats.p50)
  p95_str = BenchHelper.format_duration(stats.p95)
  result_str = if stats.result_count == :error, do: "error", else: "#{stats.result_count}"

  IO.puts("  #{q.id}: p50=#{p50_str}, p95=#{p95_str}, results=#{result_str}")

  Map.merge(stats, %{id: q.id, name: q.name})
end

# Calculate LUBM summary
lubm_latencies = Enum.flat_map(lubm_results, & &1.latencies)
lubm_avg_p50 = Enum.sum(Enum.map(lubm_results, & &1.p50)) / length(lubm_results)
lubm_avg_p95 = Enum.sum(Enum.map(lubm_results, & &1.p95)) / length(lubm_results)
lubm_max_p95 = Enum.max(Enum.map(lubm_results, & &1.p95))

IO.puts("\nLUBM Summary:")
IO.puts("  Load throughput: #{trunc(lubm_load_throughput)} triples/sec")
IO.puts("  Average p50: #{BenchHelper.format_duration(trunc(lubm_avg_p50))}")
IO.puts("  Average p95: #{BenchHelper.format_duration(trunc(lubm_avg_p95))}")
IO.puts("  Max p95: #{BenchHelper.format_duration(lubm_max_p95)}")

TripleStore.close(store)
File.rm_rf!(lubm_path)

# ===========================================================================
# WatDiv Benchmark
# ===========================================================================
IO.puts("\n\n>>> WatDiv Benchmark (Scale 1, ~100K triples) <<<\n")

watdiv_path = "./tmp/watdiv_bench_#{System.unique_integer([:positive])}"

IO.puts("Generating WatDiv data (scale 1)...")
{time_us, graph} = :timer.tc(fn -> WatDiv.generate(1) end)
triple_count = length(RDF.Graph.triples(graph))
IO.puts("  Generated #{triple_count} triples in #{Float.round(time_us / 1000, 2)}ms")

IO.puts("Opening database at #{watdiv_path}...")
{:ok, store} = TripleStore.open(watdiv_path)

IO.puts("Loading data...")
{watdiv_load_time_us, _} = :timer.tc(fn -> TripleStore.load_graph(store, graph) end)
watdiv_load_throughput = triple_count / (watdiv_load_time_us / 1_000_000)
IO.puts("  Loaded in #{Float.round(watdiv_load_time_us / 1000, 2)}ms (#{trunc(watdiv_load_throughput)} triples/sec)")

IO.puts("\nRunning WatDiv queries (warmup: #{warmup}, iterations: #{iterations})...\n")

watdiv_queries = WatDivQueries.all()
watdiv_results = for q <- watdiv_queries do
  {:ok, query} = WatDivQueries.get(q.id, [])
  stats = BenchHelper.run_query(store, query.sparql, warmup, iterations)

  p50_str = BenchHelper.format_duration(stats.p50)
  p95_str = BenchHelper.format_duration(stats.p95)
  result_str = if stats.result_count == :error, do: "error", else: "#{stats.result_count}"

  IO.puts("  #{q.id}: p50=#{p50_str}, p95=#{p95_str}, results=#{result_str}")

  Map.merge(stats, %{id: q.id, name: q.name, category: q.category})
end

# Calculate WatDiv summary
watdiv_latencies = Enum.flat_map(watdiv_results, & &1.latencies)
watdiv_avg_p50 = Enum.sum(Enum.map(watdiv_results, & &1.p50)) / length(watdiv_results)
watdiv_avg_p95 = Enum.sum(Enum.map(watdiv_results, & &1.p95)) / length(watdiv_results)
watdiv_max_p95 = Enum.max(Enum.map(watdiv_results, & &1.p95))

IO.puts("\nWatDiv Summary:")
IO.puts("  Load throughput: #{trunc(watdiv_load_throughput)} triples/sec")
IO.puts("  Average p50: #{BenchHelper.format_duration(trunc(watdiv_avg_p50))}")
IO.puts("  Average p95: #{BenchHelper.format_duration(trunc(watdiv_avg_p95))}")
IO.puts("  Max p95: #{BenchHelper.format_duration(watdiv_max_p95)}")

# Category breakdown
categories = [:linear, :star, :snowflake, :complex]
IO.puts("\n  By Category:")
for cat <- categories do
  cat_results = Enum.filter(watdiv_results, fn r -> r.category == cat end)
  if length(cat_results) > 0 do
    cat_avg_p50 = Enum.sum(Enum.map(cat_results, & &1.p50)) / length(cat_results)
    cat_avg_p95 = Enum.sum(Enum.map(cat_results, & &1.p95)) / length(cat_results)
    cat_name = String.upcase(to_string(cat))
    IO.puts("    #{cat_name}: avg p50=#{BenchHelper.format_duration(trunc(cat_avg_p50))}, avg p95=#{BenchHelper.format_duration(trunc(cat_avg_p95))}")
  end
end

TripleStore.close(store)
File.rm_rf!(watdiv_path)

# ===========================================================================
# Print Tables for Documentation
# ===========================================================================
IO.puts("\n\n" <> String.duplicate("=", 60))
IO.puts("Results for Documentation")
IO.puts(String.duplicate("=", 60))

IO.puts("\n### BSBM Query Performance\n")
IO.puts("| Query    |   p50 |   p95 |   p99 |  Mean | Results | Status   |")
IO.puts("|----------|-------|-------|-------|-------|---------|----------|")
for r <- bsbm_results do
  p50 = Float.round(r.p50 / 1000, 1)
  p95 = Float.round(r.p95 / 1000, 1)
  p99 = Float.round(r.p99 / 1000, 1)
  mean = Float.round(r.mean / 1000, 1)
  results = if r.result_count == :error, do: "error", else: "#{r.result_count}"
  status = cond do
    r.result_count == :error -> "Fail"
    r.p95 > 200_000 -> "Very Slow"
    r.p95 > 50_000 -> "Slow"
    true -> "Pass"
  end
  IO.puts("| #{String.pad_trailing(String.upcase(to_string(r.id)), 8)} | #{BenchHelper.pad_number(p50, 5)}ms | #{BenchHelper.pad_number(p95, 5)}ms | #{BenchHelper.pad_number(p99, 5)}ms | #{BenchHelper.pad_number(mean, 5)}ms | #{BenchHelper.pad_right(results, 7)} | #{BenchHelper.pad_right(status, 8)} |")
end

IO.puts("\n### LUBM Query Performance\n")
IO.puts("| Query    |    p50 |    p95 |    p99 |   Mean | Results |")
IO.puts("|----------|--------|--------|--------|--------|---------|")
for r <- lubm_results do
  p50 = Float.round(r.p50 / 1000, 2)
  p95 = Float.round(r.p95 / 1000, 2)
  p99 = Float.round(r.p99 / 1000, 2)
  mean = Float.round(r.mean / 1000, 2)
  results = if r.result_count == :error, do: "error", else: "#{r.result_count}"
  IO.puts("| #{String.pad_trailing(String.upcase(to_string(r.id)), 8)} | #{BenchHelper.pad_number(p50, 6)}ms | #{BenchHelper.pad_number(p95, 6)}ms | #{BenchHelper.pad_number(p99, 6)}ms | #{BenchHelper.pad_number(mean, 6)}ms | #{BenchHelper.pad_right(results, 7)} |")
end

IO.puts("\n### WatDiv Query Performance\n")
IO.puts("| Query | Category   |   p50 |   p95 |   p99 |  Mean | Results |")
IO.puts("|-------|------------|-------|-------|-------|-------|---------|")
for r <- watdiv_results do
  p50 = Float.round(r.p50 / 1000, 1)
  p95 = Float.round(r.p95 / 1000, 1)
  p99 = Float.round(r.p99 / 1000, 1)
  mean = Float.round(r.mean / 1000, 1)
  results = if r.result_count == :error, do: "error", else: "#{r.result_count}"
  cat_str = String.slice(to_string(r.category), 0, 10)
  IO.puts("| #{String.pad_trailing(String.upcase(to_string(r.id)), 5)} | #{BenchHelper.pad_right(cat_str, 10)} | #{BenchHelper.pad_number(p50, 5)}ms | #{BenchHelper.pad_number(p95, 5)}ms | #{BenchHelper.pad_number(p99, 5)}ms | #{BenchHelper.pad_number(mean, 5)}ms | #{BenchHelper.pad_right(results, 7)} |")
end

IO.puts("\n### Bulk Load Throughput\n")
IO.puts("| Dataset    |  Triples | Throughput  |")
IO.puts("|------------|----------|-------------|")
# Capture BSBM triple count for the table
{_, bsbm_graph} = :timer.tc(fn -> BSBM.generate(1000) end)
bsbm_triple_count = length(RDF.Graph.triples(bsbm_graph))
IO.puts("| BSBM 1000  | #{BenchHelper.pad_right("#{bsbm_triple_count}", 8)} | #{BenchHelper.pad_number(trunc(bsbm_load_throughput), 11)} tps |")
# Capture LUBM triple count for the table
{_, lubm_graph} = :timer.tc(fn -> LUBM.generate(1) end)
lubm_triple_count = length(RDF.Graph.triples(lubm_graph))
IO.puts("| LUBM 1     | #{BenchHelper.pad_right("#{lubm_triple_count}", 8)} | #{BenchHelper.pad_number(trunc(lubm_load_throughput), 11)} tps |")
# Capture WatDiv triple count for the table
{_, watdiv_graph} = :timer.tc(fn -> WatDiv.generate(1) end)
watdiv_triple_count = length(RDF.Graph.triples(watdiv_graph))
IO.puts("| WatDiv 1   | #{BenchHelper.pad_right("#{watdiv_triple_count}", 8)} | #{BenchHelper.pad_number(trunc(watdiv_load_throughput), 11)} tps |")

IO.puts("\n\nBenchmark complete!")
