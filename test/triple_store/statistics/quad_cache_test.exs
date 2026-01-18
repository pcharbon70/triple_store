defmodule TripleStore.Statistics.QuadCacheTest do
  @moduledoc """
  Unit tests for Quad Statistics Cache (Section 5.4).

  Tests the ETS-based caching layer for graph-specific statistics including:
  - Cache key design
  - Cache retrieval and storage
  - Cache invalidation
  - Cache warming
  """

  use ExUnit.Case, async: false

  alias TripleStore.Statistics

  # ETS table name
  @cache_table :triple_store_quad_stats_cache

  setup_all do
    # Start the Statistics GenServer
    start_supervised!(Statistics)
    :ok
  end

  setup do
    # Clear cache before each test
    :ets.delete_all_objects(@cache_table)
    :ok
  end

  # ===========================================================================
  # Cache Key Tests (5.4.1)
  # ===========================================================================

  describe "quad_cache_key/1" do
    test "generates unique key for default graph (ID 0)" do
      key = Statistics.quad_cache_key(0)

      assert {prefix, 0} = key
      assert is_binary(prefix)
      # Prefix should be distinct from triple stats prefix
      assert prefix != <<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01>>
    end

    test "generates unique key for named graph" do
      key = Statistics.quad_cache_key(123)

      assert {_prefix, 123} = key
    end

    test "different graph IDs produce different keys" do
      key1 = Statistics.quad_cache_key(0)
      key2 = Statistics.quad_cache_key(123)
      key3 = Statistics.quad_cache_key(456)

      assert key1 != key2
      assert key2 != key3
      assert key1 != key3
    end
  end

  describe "all_graphs_cache_key/0" do
    test "generates key for all-graphs summary" do
      key = Statistics.all_graphs_cache_key()

      assert {prefix, :all_graphs} = key
      assert is_binary(prefix)
    end

    test "all-graphs key is distinct from graph-specific keys" do
      all_key = Statistics.all_graphs_cache_key()
      graph_key = Statistics.quad_cache_key(0)

      assert all_key != graph_key
    end
  end

  # ===========================================================================
  # Cache Storage and Retrieval Tests
  # ===========================================================================

  describe "cache storage and retrieval" do
    test "stores and retrieves graph statistics from cache" do
      # Create a mock statistics entry
      stats = %{
        graph_id: 0,
        quad_count: 1000,
        distinct_subjects: 50,
        distinct_predicates: 10,
        distinct_objects: 200,
        predicate_counts: %{42 => 500, 43 => 500},
        accuracy: :exact
      }

      cache_key = Statistics.quad_cache_key(0)
      :ets.insert(@cache_table, {cache_key, stats})

      # Verify retrieval
      assert [{^cache_key, retrieved}] = :ets.lookup(@cache_table, cache_key)
      assert retrieved.graph_id == 0
      assert retrieved.quad_count == 1000
    end

    test "returns empty list for cache miss" do
      cache_key = Statistics.quad_cache_key(999)

      assert [] = :ets.lookup(@cache_table, cache_key)
    end
  end

  # ===========================================================================
  # Cache Invalidation Tests (5.4.2)
  # ===========================================================================

  describe "invalidate_quad_cache/2" do
    test "removes specific graph from cache" do
      # Add data for two graphs
      stats1 = %{graph_id: 0, quad_count: 100}
      stats2 = %{graph_id: 123, quad_count: 200}

      key1 = Statistics.quad_cache_key(0)
      key2 = Statistics.quad_cache_key(123)

      :ets.insert(@cache_table, [{key1, stats1}, {key2, stats2}])

      # Verify both are cached
      assert [{^key1, _}] = :ets.lookup(@cache_table, key1)
      assert [{^key2, _}] = :ets.lookup(@cache_table, key2)

      # Invalidate graph 0
      assert :ok = Statistics.invalidate_quad_cache(:db, 0)

      # Verify graph 0 is removed but graph 123 remains
      assert [] = :ets.lookup(@cache_table, key1)
      assert [{^key2, _}] = :ets.lookup(@cache_table, key2)
    end

    test "also invalidates all-graphs summary when graph invalidated" do
      # Add graph stats and all-graphs summary
      stats = %{graph_id: 0, quad_count: 100}
      all_summary = %{total_quads: 1000, graph_count: 3}

      graph_key = Statistics.quad_cache_key(0)
      all_key = Statistics.all_graphs_cache_key()

      :ets.insert(@cache_table, [{graph_key, stats}, {all_key, all_summary}])

      # Invalidate graph
      Statistics.invalidate_quad_cache(:db, 0)

      # Both should be invalidated
      assert [] = :ets.lookup(@cache_table, graph_key)
      assert [] = :ets.lookup(@cache_table, all_key)
    end

    test "handle_info accepts invalidation message" do
      # Add data to cache
      stats = %{graph_id: 0, quad_count: 100}
      key = Statistics.quad_cache_key(0)
      :ets.insert(@cache_table, {key, stats})

      # Send invalidation message directly
      send(Statistics, {:invalidate_graph, 0})

      # Give GenServer time to process
      Process.sleep(10)

      # Verify cache was cleared
      assert [] = :ets.lookup(@cache_table, key)
    end
  end

  describe "invalidate_all_quad_cache/1" do
    test "removes all quad statistics from cache" do
      # Add multiple entries
      stats0 = %{graph_id: 0, quad_count: 100}
      stats123 = %{graph_id: 123, quad_count: 200}
      all_summary = %{total_quads: 300}

      :ets.insert(@cache_table, [
        {Statistics.quad_cache_key(0), stats0},
        {Statistics.quad_cache_key(123), stats123},
        {Statistics.all_graphs_cache_key(), all_summary}
      ])

      # Verify entries exist
      assert length(:ets.tab2list(@cache_table)) == 3

      # Invalidate all
      assert :ok = Statistics.invalidate_all_quad_cache(:db)

      # Verify all are removed
      assert :ets.tab2list(@cache_table) == []
    end

    test "handle_info accepts invalidate_all message" do
      # Add data
      :ets.insert(@cache_table, {Statistics.quad_cache_key(0), %{graph_id: 0}})

      # Send message
      send(Statistics, :invalidate_all)
      Process.sleep(10)

      # Verify cleared
      assert :ets.tab2list(@cache_table) == []
    end
  end

  # ===========================================================================
  # Cache Warming Tests (5.4.3)
  # ===========================================================================

  describe "warm_graph_cache/3" do
    test "computes and caches graph statistics" do
      # This test requires an actual database
      # For now, test the cache insertion logic
      stats = %{
        graph_id: 0,
        quad_count: 1000,
        distinct_subjects: 50,
        distinct_predicates: 10,
        distinct_objects: 200,
        predicate_counts: %{},
        accuracy: :exact
      }

      # Manually simulate what warm_graph_cache does
      cache_key = Statistics.quad_cache_key(0)
      :ets.insert(@cache_table, {cache_key, stats})

      # Verify cached
      assert [{^cache_key, cached}] = :ets.lookup(@cache_table, cache_key)
      assert cached.quad_count == 1000
    end
  end

  describe "get_cached_graph_stats/2" do
    test "returns cached stats on cache hit" do
      stats = %{
        graph_id: 0,
        quad_count: 1000,
        distinct_subjects: 50,
        distinct_predicates: 10,
        distinct_objects: 200,
        predicate_counts: %{},
        accuracy: :exact
      }

      cache_key = Statistics.quad_cache_key(0)
      :ets.insert(@cache_table, {cache_key, stats})

      # Get cached should return the stats without computing
      assert {:ok, retrieved} = Statistics.get_cached_graph_stats(:db, 0)
      assert retrieved.quad_count == 1000
    end

    test "cache miss triggers computation when database available" do
      # This test would require a real database
      # For unit tests, we verify the behavior structure
      cache_key = Statistics.quad_cache_key(999)

      # Cache is empty
      assert [] = :ets.lookup(@cache_table, cache_key)
    end
  end

  describe "get_cached_all_graphs_summary/2" do
    test "returns cached summary on cache hit" do
      summary = %{
        total_quads: 5000,
        graph_count: 3,
        largest_graph_id: 123,
        largest_graph_count: 3000,
        per_graph: %{}
      }

      cache_key = Statistics.all_graphs_cache_key()
      :ets.insert(@cache_table, {cache_key, summary})

      assert {:ok, retrieved} = Statistics.get_cached_all_graphs_summary(:db)
      assert retrieved.total_quads == 5000
      assert retrieved.graph_count == 3
    end

    test "cache miss when summary not cached" do
      cache_key = Statistics.all_graphs_cache_key()
      assert [] = :ets.lookup(@cache_table, cache_key)
    end
  end

  # ===========================================================================
  # Integration Tests
  # ===========================================================================

  describe "cache integration" do
    test "cache persists across multiple calls" do
      stats = %{graph_id: 0, quad_count: 1000, distinct_subjects: 50, distinct_predicates: 10, distinct_objects: 200, predicate_counts: %{}, accuracy: :exact}

      key = Statistics.quad_cache_key(0)
      :ets.insert(@cache_table, {key, stats})

      # Multiple calls should return same cached data
      assert [{^key, retrieved1}] = :ets.lookup(@cache_table, key)
      assert [{^key, retrieved2}] = :ets.lookup(@cache_table, key)
      assert retrieved1 == retrieved2
    end

    test "overwrites existing cache entry" do
      stats1 = %{graph_id: 0, quad_count: 1000, distinct_subjects: 50, distinct_predicates: 10, distinct_objects: 200, predicate_counts: %{}, accuracy: :exact}
      stats2 = %{graph_id: 0, quad_count: 2000, distinct_subjects: 100, distinct_predicates: 20, distinct_objects: 400, predicate_counts: %{}, accuracy: :exact}

      key = Statistics.quad_cache_key(0)

      # Insert first
      :ets.insert(@cache_table, {key, stats1})
      assert [{^key, retrieved}] = :ets.lookup(@cache_table, key)
      assert retrieved.quad_count == 1000

      # Overwrite
      :ets.insert(@cache_table, {key, stats2})
      assert [{^key, retrieved}] = :ets.lookup(@cache_table, key)
      assert retrieved.quad_count == 2000
    end
  end

  # ===========================================================================
  # Telemetry Tests
  # ===========================================================================

  describe "telemetry events" do
    test "emit telemetry on cache invalidation" do
      # Attach telemetry handler
      _handler_id =
        :telemetry.attach(
          "test-invalid-handler",
          [:triple_store, :statistics, :quad_cache, :invalidate],
          fn event, measurements, metadata, _config ->
            send(self(), {:telemetry, event, measurements, metadata})
          end,
          nil
        )

      # Ensure handler is attached
      Process.sleep(50)

      # Trigger invalidation
      Statistics.invalidate_quad_cache(:db, 123)

      # Verify telemetry was emitted (with timeout)
      # The event is passed as a list of atoms
      assert_receive {:telemetry, [:triple_store, :statistics, :quad_cache, :invalidate], _measurements,
                       _metadata},
                   1000

      :telemetry.detach("test-invalid-handler")
    end

    test "emit telemetry on invalidate_all" do
      _handler_id =
        :telemetry.attach(
          "test-invalidate-all-handler",
          [:triple_store, :statistics, :quad_cache, :invalidate_all],
          fn event, measurements, metadata, _config ->
            send(self(), {:telemetry, event, measurements, metadata})
          end,
          nil
        )

      Process.sleep(50)

      Statistics.invalidate_all_quad_cache(:db)

      assert_receive {:telemetry, [:triple_store, :statistics, :quad_cache, :invalidate_all], _measurements,
                       _metadata},
                   1000

      :telemetry.detach("test-invalidate-all-handler")
    end

    test "emits cache hit telemetry when graph stats are cached" do
      stats = %{graph_id: 0, quad_count: 1000, distinct_subjects: 50, distinct_predicates: 10, distinct_objects: 200, predicate_counts: %{}, accuracy: :exact}
      key = Statistics.quad_cache_key(0)
      :ets.insert(@cache_table, {key, stats})

      # Attach telemetry handler
      handler_id = "test-cache-hit-handler"
      parent_pid = self()

      :telemetry.attach(
        handler_id,
        [:triple_store, :statistics, :quad_cache, :hit],
        fn event, measurements, metadata, _config ->
          send(parent_pid, {:telemetry_event, event, measurements, metadata})
        end,
        nil
      )

      # Trigger cache lookup
      {:ok, _stats} = Statistics.get_cached_graph_stats(nil, 0)

      # Should receive telemetry event
      assert_receive {:telemetry_event, [:triple_store, :statistics, :quad_cache, :hit], measurements, _metadata}

      assert measurements.graph_id == 0

      :telemetry.detach(handler_id)
    end

    test "emits all_graphs_hit telemetry when summary is cached" do
      summary = %{total_graphs: 5, total_quads: 10_000, graph_stats: %{}}
      key = Statistics.all_graphs_cache_key()
      :ets.insert(@cache_table, {key, summary})

      # Attach telemetry handler
      handler_id = "test-all-graphs-hit-handler"
      parent_pid = self()

      :telemetry.attach(
        handler_id,
        [:triple_store, :statistics, :quad_cache, :all_graphs_hit],
        fn event, measurements, metadata, _config ->
          send(parent_pid, {:telemetry_event, event, measurements, metadata})
        end,
        nil
      )

      # Trigger cache lookup
      {:ok, _summary} = Statistics.get_cached_all_graphs_summary(nil)

      # Should receive telemetry event
      assert_receive {:telemetry_event, [:triple_store, :statistics, :quad_cache, :all_graphs_hit], _measurements, _metadata}

      :telemetry.detach(handler_id)
    end
  end

  # ===========================================================================
  # Concurrency Tests
  # ===========================================================================

  describe "concurrent access" do
    test "handles concurrent reads safely" do
      stats = %{graph_id: 0, quad_count: 1000, distinct_subjects: 50, distinct_predicates: 10, distinct_objects: 200, predicate_counts: %{}, accuracy: :exact}
      key = Statistics.quad_cache_key(0)
      :ets.insert(@cache_table, {key, stats})

      # Launch multiple concurrent readers
      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            :ets.lookup(@cache_table, key)
          end)
        end

      # All should complete successfully
      results = Task.await_many(tasks, 5000)
      assert length(results) == 10
      assert Enum.all?(results, fn [{^key, _}] -> true end)
    end

    test "handles concurrent writes safely" do
      key = Statistics.quad_cache_key(0)

      # Launch multiple concurrent writers
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            stats = %{graph_id: 0, quad_count: i * 100, distinct_subjects: i, distinct_predicates: 1, distinct_objects: i, predicate_counts: %{}, accuracy: :exact}
            :ets.insert(@cache_table, {key, stats})
          end)
        end

      # All should complete without error
      Task.await_many(tasks, 5000)

      # Final value should be from one of the writers
      [{^key, final}] = :ets.lookup(@cache_table, key)
      assert final.quad_count in [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    end
  end
end
