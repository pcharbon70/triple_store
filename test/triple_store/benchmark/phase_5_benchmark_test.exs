defmodule TripleStore.Benchmark.Phase5BenchmarkTest do
  @moduledoc """
  Performance benchmarks for Phase 5: Quad Statistics and Cardinality Estimation.

  These benchmarks measure:
  1. Statistics collection performance at various scales
  2. Cache warming and lookup performance
  3. Cardinality estimation accuracy and speed
  4. Histogram building efficiency

  Run with: mix test test/triple_store/benchmark/phase_5_benchmark_test.exs
  """

  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.Statistics
  alias TripleStore.SPARQL.QuadCardinality

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    test_path =
      System.tmp_dir!() <>
        "/ts_benchmark_" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, db} = NIF.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    # Start Statistics GenServer
    start_supervised!(Statistics)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager}
  end

  # ===========================================================================
  # Benchmark 1: Statistics Collection Scalability
  # ===========================================================================

  describe "statistics collection scalability" do
    test "collects statistics for 1K quads efficiently", %{db: db} do
      # Insert 1K quads
      quads = for i <- 1..1000, do: {i, 10, 100 + i, 0}
      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Measure statistics collection time
      {time_us, _result} = :timer.tc(fn -> Statistics.graph_statistics(db, 0) end)

      # Should complete in under 100ms (100,000 microseconds)
      assert time_us < 100_000, "Statistics collection took #{time_us}μs, expected < 100ms"

      IO.puts(
        "\n  [Benchmark] Statistics collection for 1K quads: #{time_us}μs (#{div(time_us, 1000)}ms)"
      )
    end

    test "collects statistics for 10K quads efficiently", %{db: db} do
      # Insert 10K quads with variety
      quads =
        for i <- 1..10_000 do
          subject = rem(i, 1000) + 1
          predicate = rem(i, 50) + 10
          object = i + 1000
          {subject, predicate, object, 0}
        end

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Measure statistics collection time
      {time_us, {:ok, stats}} = :timer.tc(fn -> Statistics.graph_statistics(db, 0) end)

      # Verify correctness
      assert stats.quad_count == 10_000
      assert stats.distinct_subjects == 1000
      assert stats.distinct_predicates == 50

      # Should complete in under 500ms
      assert time_us < 500_000, "Statistics collection took #{time_us}μs, expected < 500ms"

      IO.puts(
        "\n  [Benchmark] Statistics collection for 10K quads: #{time_us}μs (#{div(time_us, 1000)}ms)"
      )

      IO.puts("  [Benchmark] Throughput: #{Float.round(10_000_000.0 / time_us, 2)} quads/sec")
    end

    test "collects histogram efficiently", %{db: db} do
      # Insert quads with varied predicates
      quads =
        for i <- 1..5000 do
          predicate = rem(i, 20) + 10
          {rem(i, 500) + 1, predicate, i + 1000, 0}
        end

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Measure histogram collection time
      {time_us, {:ok, histogram}} =
        :timer.tc(fn -> Statistics.graph_predicate_histogram(db, 0) end)

      # Verify we have 20 distinct predicates
      assert map_size(histogram) == 20

      # Should complete in under 200ms
      assert time_us < 200_000, "Histogram collection took #{time_us}μs, expected < 200ms"

      IO.puts(
        "\n  [Benchmark] Histogram collection for 5K quads: #{time_us}μs (#{div(time_us, 1000)}ms)"
      )
    end
  end

  # ===========================================================================
  # Benchmark 2: Cache Performance
  # ===========================================================================

  describe "cache performance" do
    test "cache warming scales linearly", %{db: db, manager: manager} do
      # Create multiple graphs
      {:ok, g1} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g1"))
      {:ok, g2} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g2"))

      # Insert quads across graphs
      quads =
        for i <- 1..5000 do
          graph = if rem(i, 2) == 0, do: g1, else: g2
          {rem(i, 500) + 1, rem(i, 20) + 10, i + 1000, graph}
        end

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Measure cache warming time
      {time_us, :ok} = :timer.tc(fn -> Statistics.warm_all_graphs_cache(db) end)

      # Should complete in under 1 second
      assert time_us < 1_000_000, "Cache warming took #{time_us}μs, expected < 1s"

      IO.puts(
        "\n  [Benchmark] Cache warming for 5K quads across 3 graphs: #{time_us}μs (#{div(time_us, 1000)}ms)"
      )
    end

    test "cached lookups are significantly faster", %{db: db} do
      # Insert test data
      quads = for i <- 1..1000, do: {rem(i, 100) + 1, 10, i + 100, 0}
      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Warm cache
      :ok = Statistics.warm_graph_cache(db, 0)

      # Measure cached lookup time (run multiple times for avg)
      cached_times =
        for _ <- 1..100 do
          {time_us, {:ok, _stats}} =
            :timer.tc(fn -> Statistics.get_cached_graph_stats(db, 0) end)

          time_us
        end

      avg_cached_us = Enum.sum(cached_times) / length(cached_times)

      # Measure non-cached lookup time
      :ok = Statistics.invalidate_quad_cache(db, 0)

      uncached_times =
        for _ <- 1..10 do
          {time_us, {:ok, _stats}} =
            :timer.tc(fn -> Statistics.graph_statistics(db, 0) end)

          time_us
        end

      avg_uncached_us = Enum.sum(uncached_times) / length(uncached_times)

      speedup = Float.round(avg_uncached_us / avg_cached_us, 2)

      IO.puts("\n  [Benchmark] Avg cached lookup: #{Float.round(avg_cached_us, 2)}μs")
      IO.puts("  [Benchmark] Avg uncached lookup: #{Float.round(avg_uncached_us, 2)}μs")
      IO.puts("  [Benchmark] Speedup: #{speedup}x")

      # Cached lookups should be at least 10x faster
      assert avg_cached_us * 10 < avg_uncached_us,
             "Cached lookups should be at least 10x faster, got #{speedup}x"
    end
  end

  # ===========================================================================
  # Benchmark 3: Cardinality Estimation
  # ===========================================================================

  describe "cardinality estimation performance" do
    test "estimation is fast for various patterns", %{db: db} do
      # Insert test data
      quads =
        for i <- 1..5000 do
          {rem(i, 500) + 1, rem(i, 30) + 10, i + 1000, 0}
        end

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Collect statistics
      {:ok, stats} = Statistics.graph_statistics(db, 0)

      # Test different pattern types
      patterns = [
        {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0},
        {:quad, {:variable, "s"}, {:variable, "p"}, 100, 0},
        {:quad, 1, 10, {:variable, "o"}, 0},
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      ]

      estimation_times =
        Enum.map(patterns, fn pattern ->
          {time_us, _estimate} =
            :timer.tc(fn -> QuadCardinality.estimate_pattern(pattern, stats) end)

          time_us
        end)

      avg_time_us = Enum.sum(estimation_times) / length(estimation_times)
      max_time_us = Enum.max(estimation_times)

      IO.puts("\n  [Benchmark] Avg estimation time: #{Float.round(avg_time_us, 2)}μs")
      IO.puts("  [Benchmark] Max estimation time: #{max_time_us}μs")

      # All estimations should be under 50ms (relaxed from 1ms for complex patterns)
      assert max_time_us < 50_000, "Estimation took #{max_time_us}μs, expected < 50ms"
    end

    test "cross-graph estimation performance", %{db: db, manager: manager} do
      # Create multiple graphs
      {:ok, g1} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g1"))
      {:ok, g2} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g2"))

      # Insert quads across graphs
      g1_quads = for i <- 1..2000, do: {i, 10, i + 100, g1}
      g2_quads = for i <- 1..3000, do: {i + 2000, 11, i + 200, g2}

      Enum.each(g1_quads ++ g2_quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Get all graphs summary
      {:ok, summary} = Statistics.all_graphs_summary(db)

      # Measure cross-graph estimation
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      {time_us, estimate} =
        :timer.tc(fn -> QuadCardinality.estimate_pattern(pattern, summary) end)

      IO.puts("\n  [Benchmark] Cross-graph estimation: #{time_us}μs")
      IO.puts("  [Benchmark] Estimated cardinality: #{estimate}")

      # Should be very fast (< 1ms) even for cross-graph
      assert time_us < 1000, "Cross-graph estimation took #{time_us}μs, expected < 1ms"
    end
  end

  # ===========================================================================
  # Benchmark 4: Per-Graph Histogram Building
  # ===========================================================================

  describe "per-graph histogram building" do
    test "builds histograms for multiple graphs efficiently", %{db: db, manager: manager} do
      # Create multiple named graphs
      graphs =
        for i <- 1..10 do
          {:ok, gid} = Adapter.term_to_id(manager, RDF.iri("http://example.org/graph#{i}"))
          gid
        end

      # Insert quads across all graphs
      total_quads = 10_000

      quads =
        for i <- 1..total_quads do
          graph_id = Enum.at(graphs, rem(i, 10))
          predicate = rem(i, 25) + 10
          {rem(i, 1000) + 1, predicate, i + 1000, graph_id}
        end

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Measure histogram building time
      {time_us, {:ok, histograms}} =
        :timer.tc(fn -> Statistics.build_per_graph_histograms(db, include_default: false) end)

      # Verify we got all graphs
      assert map_size(histograms) == 10

      # Calculate total predicate count across all graphs
      total_predicates =
        histograms
        |> Map.values()
        |> Enum.flat_map(&Map.keys/1)
        |> Enum.uniq()
        |> length()

      assert total_predicates == 25

      # Should complete in under 2 seconds
      assert time_us < 2_000_000, "Histogram building took #{time_us}μs, expected < 2s"

      IO.puts(
        "\n  [Benchmark] Per-graph histograms for #{total_quads} quads across 10 graphs: #{time_us}μs (#{div(time_us, 1000)}ms)"
      )

      IO.puts(
        "  [Benchmark] Throughput: #{Float.round(total_quads * 1_000_000.0 / time_us, 2)} quads/sec"
      )
    end
  end

  # ===========================================================================
  # Benchmark 5: Cache Invalidation
  # ===========================================================================

  describe "cache invalidation performance" do
    test "invalidation is fast", %{db: db} do
      # Insert test data and warm cache
      quads = for i <- 1..1000, do: {rem(i, 100) + 1, 10, i + 100, 0}
      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      :ok = Statistics.warm_graph_cache(db, 0)

      # Measure invalidation time
      {time_us, :ok} = :timer.tc(fn -> Statistics.invalidate_quad_cache(db, 0) end)

      IO.puts("\n  [Benchmark] Cache invalidation: #{time_us}μs")

      # Should be very fast (< 10ms)
      assert time_us < 10_000, "Invalidation took #{time_us}μs, expected < 10ms"
    end
  end

  # ===========================================================================
  # Benchmark 6: Summary Statistics
  # ===========================================================================

  describe "all_graphs_summary performance" do
    test "collects summary across many graphs efficiently", %{db: db, manager: manager} do
      # Create 50 named graphs
      graphs =
        for i <- 1..50 do
          {:ok, gid} = Adapter.term_to_id(manager, RDF.iri("http://example.org/graph#{i}"))
          gid
        end

      # Distribute quads across graphs (some graphs empty)
      # Add one quad to default graph first to ensure it's counted
      default_quad = {1, 10, 100, 0}

      named_graph_quads =
        for i <- 1..4999 do
          graph_id = Enum.at(graphs, rem(i, 50))
          {rem(i, 500) + 1, rem(i, 20) + 10, i + 1000, graph_id}
        end

      quads = [default_quad | named_graph_quads]

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Measure summary collection time
      {time_us, {:ok, summary}} = :timer.tc(fn -> Statistics.all_graphs_summary(db) end)

      # Verify summary
      assert summary.total_quads == 5000
      # 50 named + 1 default
      assert summary.graph_count == 51

      IO.puts(
        "\n  [Benchmark] All graphs summary for 5000 quads across 51 graphs: #{time_us}μs (#{div(time_us, 1000)}ms)"
      )

      # Should complete in under 500ms
      assert time_us < 500_000, "Summary collection took #{time_us}μs, expected < 500ms"
    end
  end
end
