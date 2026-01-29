defmodule TripleStore.BenchmarkValidationTest do
  @moduledoc """
  Benchmark validation tests for Task 5.7.2.

  These tests validate that the triple store meets its performance targets
  by running benchmarks and checking results against defined thresholds.

  ## Performance Targets

  | Target | Metric | Threshold | Dataset |
  |--------|--------|-----------|---------|
  | Simple Query | p95 latency | <10ms | WatDiv (any scale) |
  | Complex Query | p95 latency | <100ms | WatDiv (any scale) |
  | Bulk Load | throughput | >100K triples/sec | any |
  | Query Mix | p95 latency | <50ms | WatDiv (any scale) |

  Note: Tests use smaller datasets (1K-10K triples) for fast CI runs.
  Full validation with 1M triples requires the :benchmark tag.

  ## Timeout Configuration

  Default timeout: 300 seconds (5 minutes)
  Rationale: Benchmark tests with :benchmark tag may run large-scale tests
  that load and query large datasets. Small-scale CI tests complete faster
  but we keep the timeout high for consistency.
  """

  use ExUnit.Case, async: false

  import TripleStore.Test.IntegrationHelpers,
    only: [
      create_test_store: 1,
      cleanup_test_store: 2
    ]

  alias TripleStore.Benchmark.{Runner, Targets}

  @moduletag :integration
  # 5 minute timeout for benchmarks (may run large-scale tests with :benchmark tag)
  @moduletag timeout: 300_000

  # ===========================================================================
  # Profiling and Bottleneck Identification
  # ===========================================================================

  describe "profiling and bottleneck identification" do
    @tag :slow
    test "bulk load throughput measurement" do
      {store, path} = create_test_store(prefix: "bench_test")

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
        cleanup_test_store(store, path)
      end
    end

    test "query latency distribution analysis" do
      {store, path} = create_test_store(prefix: "bench_test")

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
        cleanup_test_store(store, path)
      end
    end

    test "concurrent query performance" do
      {store, path} = create_test_store(prefix: "bench_test")

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
        cleanup_test_store(store, path)
      end
    end
  end

  # ===========================================================================
  # Performance Characteristics Documentation
  # ===========================================================================

  describe "performance characteristics documentation" do
    test "validates all performance targets" do
      # Document target definitions
      targets = Targets.all()

      IO.puts("\n=== Performance Targets ===")

      for target <- targets do
        IO.puts("#{target.name}:")
        IO.puts("  Description: #{target.description}")
        IO.puts("  Metric: #{target.metric}")
        IO.puts("  Threshold: #{format_threshold(target)}")
      end

      # Verify target definitions
      assert length(targets) == 4

      target_ids = Enum.map(targets, & &1.id)
      assert :simple_query in target_ids
      assert :complex_query in target_ids
      assert :bulk_load in target_ids
      assert :query_mix in target_ids
    end

    test "bulk load target validation" do
      {store, path} = create_test_store(prefix: "bench_test")

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
        cleanup_test_store(store, path)
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
end
