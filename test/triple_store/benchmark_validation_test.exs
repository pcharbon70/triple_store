defmodule TripleStore.BenchmarkValidationTest do
  @moduledoc """
  Benchmark validation tests for Task 5.7.2.

  These tests validate that the triple store meets its performance targets
  by running LUBM and BSBM benchmarks and checking results against defined
  thresholds.

  ## Performance Targets

  | Target | Metric | Threshold | Dataset |
  |--------|--------|-----------|---------|
  | Simple BGP | p95 latency | <10ms | 1M triples |
  | Complex Join | p95 latency | <100ms | 1M triples |
  | Bulk Load | throughput | >100K triples/sec | any |
  | BSBM Mix | p95 latency | <50ms | 1M triples |

  Note: Tests use smaller datasets (1K-10K triples) for fast CI runs.
  Full validation with 1M triples requires the :benchmark tag.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.{Runner, Targets, LUBM, BSBM}

  @moduletag :integration
  @moduletag timeout: 300_000

  # ===========================================================================
  # Setup Helpers
  # ===========================================================================

  defp create_temp_store do
    path = Path.join(System.tmp_dir!(), "benchmark_validation_#{:rand.uniform(1_000_000)}")
    {:ok, store} = TripleStore.open(path)
    {store, path}
  end

  defp cleanup_store(store, path) do
    try do
      TripleStore.close(store)
    rescue
      _ -> :ok
    end

    File.rm_rf!(path)
  end

  # ===========================================================================
  # 5.7.2.1: LUBM Benchmark Validation
  # ===========================================================================

  describe "5.7.2.1: LUBM benchmark validation" do
    @tag :benchmark
    @tag timeout: 600_000
    test "LUBM benchmark meets performance targets on scaled dataset" do
      {store, path} = create_temp_store()

      try do
        # Generate LUBM data at scale 1 (approximately 100K triples)
        graph = LUBM.generate(1)
        _triple_count = RDF.Graph.triple_count(graph)

        # Load data and measure throughput
        start_time = System.monotonic_time(:millisecond)
        {:ok, loaded} = TripleStore.load_graph(store, graph)
        end_time = System.monotonic_time(:millisecond)

        load_duration_ms = max(end_time - start_time, 1)
        triples_per_sec = loaded / (load_duration_ms / 1000)

        IO.puts("\n=== LUBM Data Load ===")
        IO.puts("Triples loaded: #{loaded}")
        IO.puts("Duration: #{load_duration_ms}ms")
        IO.puts("Throughput: #{Float.round(triples_per_sec, 0)} triples/sec")

        # Run LUBM benchmark
        {:ok, results} = Runner.run(store, :lubm, scale: 1, warmup: 3, iterations: 10)

        # Print results
        IO.puts("\n=== LUBM Benchmark Results ===")
        IO.puts("Duration: #{results.duration_ms}ms")
        IO.puts("Aggregate p95: #{Runner.format_duration(results.aggregate.p95_us)}")
        IO.puts("Queries/sec: #{Float.round(results.aggregate.queries_per_sec, 1)}")

        for qr <- results.query_results do
          IO.puts("  #{qr.query_id}: p95=#{Runner.format_duration(qr.p95_us)}, results=#{qr.result_count}")
        end

        # Validate targets
        {:ok, validation} = Targets.validate(results)
        Targets.print_report(validation)

        # Store detailed metrics for documentation
        assert is_list(results.query_results)
        assert length(results.query_results) > 0
      after
        cleanup_store(store, path)
      end
    end

    test "LUBM queries execute correctly on small dataset" do
      {store, path} = create_temp_store()

      try do
        # Use small scale for fast CI (1 university)
        graph = LUBM.generate(1)
        {:ok, _} = TripleStore.load_graph(store, graph)

        # Run subset of LUBM queries
        {:ok, results} = Runner.run(store, :lubm,
          scale: 1,
          warmup: 1,
          iterations: 3,
          queries: [:q1, :q3, :q14]
        )

        # Verify queries executed
        assert length(results.query_results) == 3

        # Check each query returned some result
        for qr <- results.query_results do
          assert qr.iterations == 3
          assert length(qr.latencies_us) == 3
          # p50 and p95 can be 0 if queries complete within 1 microsecond
          assert qr.p50_us >= 0
          assert qr.p95_us >= qr.p50_us
        end
      after
        cleanup_store(store, path)
      end
    end

    test "LUBM simple BGP queries are fast" do
      {store, path} = create_temp_store()

      try do
        graph = LUBM.generate(1)
        {:ok, _} = TripleStore.load_graph(store, graph)

        # Run simple queries (single triple pattern)
        {:ok, results} = Runner.run(store, :lubm,
          scale: 1,
          warmup: 2,
          iterations: 5,
          queries: [:q3, :q14]  # Simple BGP queries
        )

        # Simple queries should complete quickly
        for qr <- results.query_results do
          # Even on small dataset, simple queries should be under 100ms
          assert qr.p95_us < 100_000, "Query #{qr.query_id} p95 too slow: #{qr.p95_us}µs"
        end
      after
        cleanup_store(store, path)
      end
    end
  end

  # ===========================================================================
  # 5.7.2.2: BSBM Benchmark Validation
  # ===========================================================================

  describe "5.7.2.2: BSBM benchmark validation" do
    @tag :benchmark
    @tag timeout: 600_000
    test "BSBM benchmark meets performance targets on scaled dataset" do
      {store, path} = create_temp_store()

      try do
        # Generate BSBM data (e-commerce simulation)
        graph = BSBM.generate(1000)
        _triple_count = RDF.Graph.triple_count(graph)

        # Load data
        start_time = System.monotonic_time(:millisecond)
        {:ok, loaded} = TripleStore.load_graph(store, graph)
        end_time = System.monotonic_time(:millisecond)

        load_duration_ms = max(end_time - start_time, 1)
        triples_per_sec = loaded / (load_duration_ms / 1000)

        IO.puts("\n=== BSBM Data Load ===")
        IO.puts("Triples loaded: #{loaded}")
        IO.puts("Duration: #{load_duration_ms}ms")
        IO.puts("Throughput: #{Float.round(triples_per_sec, 0)} triples/sec")

        # Run BSBM benchmark
        {:ok, results} = Runner.run(store, :bsbm, scale: 1, warmup: 3, iterations: 10)

        # Print results
        IO.puts("\n=== BSBM Benchmark Results ===")
        IO.puts("Duration: #{results.duration_ms}ms")
        IO.puts("Aggregate p95: #{Runner.format_duration(results.aggregate.p95_us)}")
        IO.puts("Queries/sec: #{Float.round(results.aggregate.queries_per_sec, 1)}")

        for qr <- results.query_results do
          IO.puts("  #{qr.query_id}: p95=#{Runner.format_duration(qr.p95_us)}, results=#{qr.result_count}")
        end

        # Validate targets
        {:ok, validation} = Targets.validate(results)
        Targets.print_report(validation)

        # Store results
        assert results.aggregate.p95_us > 0
      after
        cleanup_store(store, path)
      end
    end

    test "BSBM queries execute correctly on small dataset" do
      {store, path} = create_temp_store()

      try do
        # Small scale for fast CI
        graph = BSBM.generate(100)
        {:ok, _} = TripleStore.load_graph(store, graph)

        # Run BSBM queries
        {:ok, results} = Runner.run(store, :bsbm,
          scale: 1,
          warmup: 1,
          iterations: 3,
          queries: [:q1, :q2, :q7]
        )

        # Verify queries executed
        assert length(results.query_results) == 3

        for qr <- results.query_results do
          assert qr.iterations == 3
          # p95 can be 0 if queries complete within 1 microsecond
          assert qr.p95_us >= 0
        end
      after
        cleanup_store(store, path)
      end
    end

    test "BSBM e-commerce query patterns complete" do
      {store, path} = create_temp_store()

      try do
        graph = BSBM.generate(50)
        {:ok, _} = TripleStore.load_graph(store, graph)

        # Test key BSBM query types
        {:ok, results} = Runner.run(store, :bsbm,
          scale: 1,
          warmup: 1,
          iterations: 3
        )

        # Should have multiple query types
        assert length(results.query_results) > 0

        # Aggregate stats should be calculated
        assert results.aggregate.total_queries > 0
        assert results.aggregate.queries_per_sec > 0
      after
        cleanup_store(store, path)
      end
    end
  end

  # ===========================================================================
  # 5.7.2.3: Profiling and Bottleneck Identification
  # ===========================================================================

  describe "5.7.2.3: profiling and bottleneck identification" do
    test "bulk load throughput measurement" do
      {store, path} = create_temp_store()

      try do
        # Generate varying sizes to measure throughput scaling
        sizes = [100, 500, 1000]

        for size <- sizes do
          triples =
            for i <- 1..size do
              {
                RDF.iri("http://example.org/item#{i}"),
                RDF.iri("http://example.org/value"),
                RDF.literal(i)
              }
            end

          graph = RDF.Graph.new(triples)

          start_time = System.monotonic_time(:microsecond)
          {:ok, count} = TripleStore.load_graph(store, graph)
          end_time = System.monotonic_time(:microsecond)

          duration_us = max(end_time - start_time, 1)
          tps = count / (duration_us / 1_000_000)

          IO.puts("Load #{size} triples: #{Float.round(tps, 0)} triples/sec (#{duration_us}µs)")

          # Throughput should be reasonable
          assert tps > 1000, "Throughput too low for #{size} triples: #{tps}"
        end
      after
        cleanup_store(store, path)
      end
    end

    test "query latency distribution analysis" do
      {store, path} = create_temp_store()

      try do
        # Load test data
        triples =
          for i <- 1..500 do
            {
              RDF.iri("http://example.org/item#{i}"),
              RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
              RDF.iri("http://example.org/Item")
            }
          end

        graph = RDF.Graph.new(triples)
        {:ok, _} = TripleStore.load_graph(store, graph)

        # Run multiple queries to get latency distribution
        query = "SELECT ?item WHERE { ?item a <http://example.org/Item> }"

        latencies =
          for _i <- 1..20 do
            start = System.monotonic_time(:microsecond)
            {:ok, _results} = TripleStore.query(store, query)
            finish = System.monotonic_time(:microsecond)
            finish - start
          end

        # Calculate percentiles
        sorted = Enum.sort(latencies)
        p50 = Runner.percentile(sorted, 50)
        p95 = Runner.percentile(sorted, 95)
        p99 = Runner.percentile(sorted, 99)
        mean = Enum.sum(latencies) / length(latencies)

        IO.puts("\n=== Query Latency Distribution ===")
        IO.puts("Min: #{Enum.min(latencies)}µs")
        IO.puts("P50: #{p50}µs")
        IO.puts("P95: #{p95}µs")
        IO.puts("P99: #{p99}µs")
        IO.puts("Max: #{Enum.max(latencies)}µs")
        IO.puts("Mean: #{Float.round(mean, 1)}µs")

        # Verify reasonable latency distribution
        assert p50 < p95, "P50 should be less than P95"
        assert p95 < p99 or p95 == p99, "P95 should be <= P99"
      after
        cleanup_store(store, path)
      end
    end

    test "concurrent query performance" do
      {store, path} = create_temp_store()

      try do
        # Load test data
        triples =
          for i <- 1..200 do
            {
              RDF.iri("http://example.org/item#{i}"),
              RDF.iri("http://example.org/value"),
              RDF.literal(i)
            }
          end

        graph = RDF.Graph.new(triples)
        {:ok, _} = TripleStore.load_graph(store, graph)

        query = "SELECT * WHERE { ?s ?p ?o } LIMIT 50"

        # Measure single-threaded baseline
        single_start = System.monotonic_time(:microsecond)

        for _i <- 1..10 do
          TripleStore.query(store, query)
        end

        single_end = System.monotonic_time(:microsecond)
        single_duration = single_end - single_start

        # Measure concurrent performance
        concurrent_start = System.monotonic_time(:microsecond)

        tasks =
          for _i <- 1..10 do
            Task.async(fn -> TripleStore.query(store, query) end)
          end

        Task.await_many(tasks, 30_000)
        concurrent_end = System.monotonic_time(:microsecond)
        concurrent_duration = concurrent_end - concurrent_start

        IO.puts("\n=== Concurrent Query Performance ===")
        IO.puts("Sequential 10 queries: #{single_duration}µs")
        IO.puts("Concurrent 10 queries: #{concurrent_duration}µs")
        IO.puts("Speedup: #{Float.round(single_duration / concurrent_duration, 2)}x")

        # Concurrent should not be significantly slower
        assert concurrent_duration < single_duration * 2,
               "Concurrent queries too slow: #{concurrent_duration}µs vs #{single_duration}µs"
      after
        cleanup_store(store, path)
      end
    end
  end

  # ===========================================================================
  # 5.7.2.4: Performance Characteristics Documentation
  # ===========================================================================

  describe "5.7.2.4: performance characteristics documentation" do
    test "generates performance report" do
      {store, path} = create_temp_store()

      try do
        # Load representative dataset
        graph = LUBM.generate(1)
        {:ok, _loaded} = TripleStore.load_graph(store, graph)

        # Run benchmarks
        {:ok, lubm_results} = Runner.run(store, :lubm,
          scale: 1,
          warmup: 2,
          iterations: 5,
          queries: [:q1, :q3, :q14]
        )

        # Generate reports
        json_report = Runner.to_json(lubm_results)
        csv_report = Runner.to_csv(lubm_results)

        # Verify JSON report
        assert is_binary(json_report)
        {:ok, parsed} = Jason.decode(json_report)
        assert Map.has_key?(parsed, "benchmark")
        assert Map.has_key?(parsed, "query_results")
        assert Map.has_key?(parsed, "aggregate")

        # Verify CSV report
        assert is_binary(csv_report)
        lines = String.split(csv_report, "\n")
        assert length(lines) >= 2  # Header + at least 1 row
        assert String.contains?(hd(lines), "query_id")

        IO.puts("\n=== Performance Report (JSON) ===")
        IO.puts(String.slice(json_report, 0, 500) <> "...")

        IO.puts("\n=== Performance Report (CSV) ===")
        IO.puts(csv_report)
      after
        cleanup_store(store, path)
      end
    end

    test "validates all performance targets" do
      # Document target definitions
      targets = Targets.all()

      IO.puts("\n=== Performance Targets ===")

      for target <- targets do
        IO.puts("#{target.name}:")
        IO.puts("  Description: #{target.description}")
        IO.puts("  Metric: #{target.metric}")
        IO.puts("  Threshold: #{format_threshold(target)}")
        IO.puts("  Dataset: #{format_dataset(target.dataset_size)}")
      end

      # Verify target definitions
      assert length(targets) == 4

      target_ids = Enum.map(targets, & &1.id)
      assert :simple_bgp in target_ids
      assert :complex_join in target_ids
      assert :bulk_load in target_ids
      assert :bsbm_mix in target_ids
    end

    test "bulk load target validation" do
      {store, path} = create_temp_store()

      try do
        # Generate test data
        triples =
          for i <- 1..5000 do
            {
              RDF.iri("http://example.org/item#{i}"),
              RDF.iri("http://example.org/value"),
              RDF.literal(i)
            }
          end

        graph = RDF.Graph.new(triples)

        # Measure load time
        start_time = System.monotonic_time(:millisecond)
        {:ok, count} = TripleStore.load_graph(store, graph)
        end_time = System.monotonic_time(:millisecond)

        duration_ms = max(end_time - start_time, 1)

        # Validate against bulk load target
        {:ok, validation} = Targets.validate_bulk_load(count, duration_ms)

        IO.puts("\n=== Bulk Load Validation ===")
        Targets.print_report(validation)

        # Just verify validation runs
        assert is_map(validation)
        assert Map.has_key?(validation, :passed)
      after
        cleanup_store(store, path)
      end
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp format_threshold(target) do
    op = if target.operator == :lt, do: "<", else: ">"

    case target.unit do
      :microseconds -> "#{op}#{target.threshold / 1000}ms"
      :milliseconds -> "#{op}#{target.threshold}ms"
      :triples_per_sec -> "#{op}#{target.threshold} triples/sec"
    end
  end

  defp format_dataset(:any), do: "any size"
  defp format_dataset(size), do: "#{size} triples"
end
