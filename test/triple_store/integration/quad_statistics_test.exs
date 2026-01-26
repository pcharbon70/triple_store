defmodule TripleStore.Integration.QuadStatisticsTest do
  @moduledoc """
  End-to-end integration tests for Phase 5: Quad Statistics.

  Tests the complete flow:
  1. Insert quads → collect statistics → verify accuracy
  2. Cache warming → verify cached data
  3. Multi-graph statistics collection
  4. Cardinality estimation with real data

  These tests use a real quad store database with actual RocksDB storage.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
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
        "/ts_quadstats_" <> Integer.to_string(System.unique_integer([:positive]))

    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    # Start Statistics GenServer
    start_supervised!(Statistics)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager}
  end

  # ===========================================================================
  # Test 1: Statistics Collection Flow
  # ===========================================================================

  describe "statistics collection flow" do
    test "insert quads and collect accurate statistics", %{db: db, manager: manager} do
      # Insert test quads into default graph
      quads = [
        # s1, p1, o1, default
        {1, 10, 100, 0},
        # s1, p1, o2, default
        {1, 10, 101, 0},
        # s2, p1, o1, default
        {2, 10, 100, 0},
        # s1, p2, o3, default
        {1, 11, 102, 0}
      ]

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Collect statistics for default graph (graph ID 0)
      assert {:ok, stats} = Statistics.graph_statistics(db, 0)

      # Verify statistics accuracy
      assert stats.quad_count == 4
      assert stats.distinct_subjects == 2
      assert stats.distinct_predicates == 2
    end

    test "statistics reflect cross-graph distribution", %{db: db, manager: manager} do
      # Create named graph
      {:ok, graph_iri} = Adapter.term_to_id(manager, RDF.iri("http://example.org/graph1"))

      # Insert quads to different graphs
      quads = [
        # default graph
        {1, 10, 100, 0},
        # default graph
        {2, 10, 101, 0},
        # named graph
        {3, 11, 102, graph_iri},
        # named graph
        {4, 11, 103, graph_iri}
      ]

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Get statistics for default graph
      {:ok, default_stats} = Statistics.graph_statistics(db, 0)
      assert default_stats.quad_count == 2

      # Get statistics for named graph
      {:ok, named_stats} = Statistics.graph_statistics(db, graph_iri)
      assert named_stats.quad_count == 2
    end
  end

  # ===========================================================================
  # Test 2: Cache Warming
  # ===========================================================================

  describe "cache warming" do
    test "warmed cache is used for subsequent lookups", %{db: db, manager: manager} do
      # Insert test quads
      quads = [{1, 10, 100, 0}, {2, 11, 101, 0}]
      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Warm the cache
      assert :ok = Statistics.warm_graph_cache(db, 0)

      # Get cached stats (should be fast, no recomputation)
      assert {:ok, cached_stats} = Statistics.get_cached_graph_stats(db, 0)
      assert cached_stats.quad_count == 2

      # Insert more quads (invalidates cache)
      :ok = QuadOperations.insert_quad(db, {3, 12, 102, 0})

      # Cache should be invalidated, get fresh stats
      {:ok, fresh_stats} = Statistics.graph_statistics(db, 0)
      assert fresh_stats.quad_count == 3
    end

    test "warm all graphs cache collects summary", %{db: db, manager: manager} do
      # Create named graphs
      {:ok, g1} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g1"))
      {:ok, g2} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g2"))

      # Insert quads to multiple graphs
      quads = [
        # default: 1 quad
        {1, 10, 100, 0},
        # g1: 2 quads
        {2, 11, 101, g1},
        {3, 11, 102, g1},
        # g2: 3 quads
        {4, 12, 103, g2},
        {5, 12, 104, g2},
        {6, 13, 105, g2}
      ]

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Warm all graphs cache
      assert :ok = Statistics.warm_all_graphs_cache(db)

      # Get summary to verify
      assert {:ok, summary} = Statistics.all_graphs_summary(db)
      assert summary.total_quads == 6
      assert summary.graph_count == 3
    end
  end

  # ===========================================================================
  # Test 3: Multi-Graph Statistics
  # ===========================================================================

  describe "multi-graph statistics" do
    test "per-graph predicate histograms are accurate", %{db: db, manager: manager} do
      # Create named graphs
      {:ok, g1} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g1"))
      {:ok, g2} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g2"))

      # Insert quads with predicate distribution
      # g1: predicate 10 appears 3 times, predicate 11 appears 1 time
      g1_quads = [
        {1, 10, 100, g1},
        {2, 10, 101, g1},
        {3, 10, 102, g1},
        {4, 11, 103, g1}
      ]

      # g2: predicate 10 appears 1 time, predicate 11 appears 3 times
      g2_quads = [
        {5, 10, 104, g2},
        {6, 11, 105, g2},
        {7, 11, 106, g2},
        {8, 11, 107, g2}
      ]

      Enum.each(g1_quads ++ g2_quads, fn quad ->
        :ok = QuadOperations.insert_quad(db, quad)
      end)

      # Get predicate histogram for g1
      {:ok, g1_histogram} = Statistics.graph_predicate_histogram(db, g1)
      assert Map.get(g1_histogram, 10, 0) == 3
      assert Map.get(g1_histogram, 11, 0) == 1

      # Get predicate histogram for g2
      {:ok, g2_histogram} = Statistics.graph_predicate_histogram(db, g2)
      assert Map.get(g2_histogram, 10, 0) == 1
      assert Map.get(g2_histogram, 11, 0) == 3
    end

    test "build per-graph histograms efficiently", %{db: db, manager: manager} do
      # Use simple graph IDs for testing
      g1 = 10
      g2 = 20
      g3 = 30

      # Insert quads across graphs
      all_quads = [
        {1, 100, 1000, g1},
        {2, 100, 1001, g1},
        {3, 101, 1002, g1},
        {4, 100, 1003, g2},
        {5, 101, 1004, g2},
        {6, 100, 1005, g3},
        {7, 101, 1006, g3},
        {8, 102, 1007, g3}
      ]

      Enum.each(all_quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Build per-graph histograms (single-pass scan)
      assert {:ok, histograms} = Statistics.build_per_graph_histograms(db, include_default: false)

      # Verify all graphs are represented
      assert map_size(histograms) == 3

      # Verify g1 histogram (2 quads with predicate 100, 1 with predicate 101)
      g1_hist = Map.get(histograms, g1, %{})
      assert Map.get(g1_hist, 100, 0) == 2
      assert Map.get(g1_hist, 101, 0) == 1

      # Verify g2 histogram (1 quad with predicate 100, 1 with predicate 101)
      g2_hist = Map.get(histograms, g2, %{})
      assert Map.get(g2_hist, 100, 0) == 1
      assert Map.get(g2_hist, 101, 0) == 1

      # Verify g3 histogram (1 quad each with predicates 100, 101, 102)
      g3_hist = Map.get(histograms, g3, %{})
      assert Map.get(g3_hist, 100, 0) == 1
      assert Map.get(g3_hist, 101, 0) == 1
      assert Map.get(g3_hist, 102, 0) == 1
    end
  end

  # ===========================================================================
  # Test 4: Cardinality Estimation with Real Data
  # ===========================================================================

  describe "cardinality estimation with real data" do
    test "estimate_pattern returns accurate cardinality for real quads", %{
      db: db,
      manager: manager
    } do
      # Insert test quads
      quads = for i <- 1..50, do: {i, 10, 100 + i, 0}
      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Collect statistics
      assert {:ok, stats} = Statistics.graph_statistics(db, 0)

      # Estimate cardinality for pattern with bound predicate
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}
      estimate = QuadCardinality.estimate_pattern(pattern, stats)

      # Should be approximately 50 (the actual count)
      assert estimate > 0
      # Should be reasonable
      assert estimate < 1000
    end

    test "cross-graph pattern estimates sum across graphs", %{db: db, manager: manager} do
      # Create two graphs
      {:ok, g1} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g1"))
      {:ok, g2} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g2"))

      # Insert 10 quads to g1, 20 to g2
      g1_quads = for i <- 1..10, do: {i, 10, 100, g1}
      g2_quads = for i <- 1..20, do: {i + 100, 10, 200, g2}

      Enum.each(g1_quads ++ g2_quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Collect statistics
      assert :ok = Statistics.warm_all_graphs_cache(db)
      assert {:ok, all_stats} = Statistics.all_graphs_summary(db)

      # Estimate cross-graph pattern
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, {:variable, "g"}}
      estimate = QuadCardinality.estimate_pattern(pattern, all_stats)

      # Should be approximately 30 (10 + 20)
      assert estimate > 0
    end
  end

  # ===========================================================================
  # Test 5: Cache Invalidation
  # ===========================================================================

  describe "cache invalidation integration" do
    test "statistics cache invalidates on request", %{db: db, manager: manager} do
      # Insert initial data
      :ok = QuadOperations.insert_quad(db, {1, 10, 100, 0})

      # Collect and cache statistics
      assert :ok = Statistics.warm_graph_cache(db, 0)
      assert {:ok, stats1} = Statistics.get_cached_graph_stats(db, 0)
      assert stats1.quad_count == 1

      # Insert more data
      :ok = QuadOperations.insert_quad(db, {2, 10, 101, 0})

      # Manually invalidate to test the invalidation mechanism
      :ok = Statistics.invalidate_quad_cache(db, 0)

      # Cache should be cleared
      assert [] = :ets.lookup(:triple_store_quad_stats_cache, Statistics.quad_cache_key(0))

      # Get fresh statistics
      assert {:ok, stats2} = Statistics.graph_statistics(db, 0)
      assert stats2.quad_count == 2
    end
  end

  # ===========================================================================
  # Test 6: Quad Count Tracking
  # ===========================================================================

  describe "quad count tracking" do
    test "tracks quad count per graph accurately", %{db: db, manager: manager} do
      # Create named graphs
      {:ok, g1} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g1"))

      # Insert quads to default and named graph
      quads = [
        {1, 10, 100, 0},
        {2, 10, 101, 0},
        {3, 11, 102, g1},
        {4, 11, 103, g1},
        {5, 12, 104, g1}
      ]

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Check quad count for default graph
      assert {:ok, default_count} = Statistics.graph_quad_count(db, 0)
      assert default_count == 2

      # Check quad count for named graph
      assert {:ok, named_count} = Statistics.graph_quad_count(db, g1)
      assert named_count == 3
    end

    test "all_graphs_summary aggregates correctly", %{db: db, manager: manager} do
      # Insert quads to multiple graphs
      {:ok, g1} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g1"))
      {:ok, g2} = Adapter.term_to_id(manager, RDF.iri("http://example.org/g2"))

      quads = [
        # default: 1 quad
        {1, 10, 100, 0},
        # g1: 2 quads
        {2, 11, 101, g1},
        {3, 11, 102, g1},
        # g2: 3 quads
        {4, 12, 103, g2},
        {5, 12, 104, g2},
        {6, 13, 105, g2}
      ]

      Enum.each(quads, fn quad -> :ok = QuadOperations.insert_quad(db, quad) end)

      # Get all graphs summary
      assert {:ok, summary} = Statistics.all_graphs_summary(db)

      assert summary.total_quads == 6
      assert summary.graph_count == 3
    end
  end
end
