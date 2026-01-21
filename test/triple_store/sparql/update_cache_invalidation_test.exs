defmodule TripleStore.SPARQL.UpdateCacheInvalidationTest do
  @moduledoc """
  Tests for statistics cache invalidation after SPARQL UPDATE operations.

  This test suite verifies that INSERT DATA and DELETE DATA operations
  properly invalidate the quad statistics cache for affected graphs.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Statistics
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.UpdateExecutor

  import RDF

  @cache_table :triple_store_quad_stats_cache

  setup_all do
    # Start the Statistics GenServer
    start_supervised!(Statistics)
    :ok
  end

  setup do
    # Create a test database
    db_name = "test_db_#{:rand.uniform(1_000_000)}"
    test_path = Path.join([System.tmp_dir!(), "triple_store_test", db_name])
    File.mkdir_p!(test_path)

    {:ok, db} = NIF.open(test_path, schema: :quad)

    # Start a dictionary manager
    {:ok, manager} = Manager.start_link(db: db)

    # Clear cache before each test
    :ets.delete_all_objects(@cache_table)

    on_exit(fn ->
      try do
        if Process.alive?(manager) do
          Manager.stop(manager)
        end
      catch
        :exit, _ -> :ok
      end

      NIF.close(db)
      File.rm_rf!(test_path)
    end)

    %{db: db, manager: manager}
  end

  # ===========================================================================
  # INSERT DATA Cache Invalidation Tests
  # ===========================================================================

  describe "INSERT DATA invalidates statistics cache" do
    test "inserting to default graph invalidates cache", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Warm the cache for graph 0 (default)
      cache_key = Statistics.quad_cache_key(0)
      :ets.insert(@cache_table, {cache_key, %{graph_id: 0, quad_count: 100}})

      # Verify cache is populated
      assert [{^cache_key, _}] = :ets.lookup(@cache_table, cache_key)

      # Execute INSERT DATA
      update =
        "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> . }"

      assert {:ok, _ast} = Parser.parse_update(update)
      {:ok, ast} = Parser.parse_update(update)

      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast)

      # Verify cache was invalidated for graph 0
      # Give time for invalidation
      Process.sleep(10)
      assert [] = :ets.lookup(@cache_table, cache_key)
    end

    test "inserting to named graph invalidates that graph's cache", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # First, create a named graph by inserting to it
      update1 =
        "INSERT DATA { GRAPH <http://example.org/graph1> { <http://example.org/s> <http://example.org/p> <http://example.org/o> . } }"

      {:ok, ast1} = Parser.parse_update(update1)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast1)

      # Get the graph ID for our named graph
      {:ok, graph_id} = Adapter.term_to_id(manager, iri("http://example.org/graph1"))

      # Warm the cache for this graph
      cache_key = Statistics.quad_cache_key(graph_id)
      :ets.insert(@cache_table, {cache_key, %{graph_id: graph_id, quad_count: 100}})

      # Verify cache is populated
      assert [{^cache_key, _}] = :ets.lookup(@cache_table, cache_key)

      # Execute another INSERT DATA to the same graph
      update2 =
        "INSERT DATA { GRAPH <http://example.org/graph1> { <http://example.org/s2> <http://example.org/p2> <http://example.org/o2> . } }"

      {:ok, ast2} = Parser.parse_update(update2)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast2)

      # Verify cache was invalidated for this graph
      Process.sleep(10)
      assert [] = :ets.lookup(@cache_table, cache_key)
    end

    test "inserting to multiple graphs invalidates all affected caches", %{
      db: db,
      manager: manager
    } do
      ctx = %{db: db, dict_manager: manager}

      # Create two named graphs
      update1 = """
      INSERT DATA {
        GRAPH <http://example.org/graph1> { <http://example.org/s> <http://example.org/p> <http://example.org/o> . }
        GRAPH <http://example.org/graph2> { <http://example.org/s2> <http://example.org/p2> <http://example.org/o2> . }
      }
      """

      {:ok, ast1} = Parser.parse_update(update1)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast1)

      # Get the graph IDs
      {:ok, graph1_id} = Adapter.term_to_id(manager, iri("http://example.org/graph1"))
      {:ok, graph2_id} = Adapter.term_to_id(manager, iri("http://example.org/graph2"))

      # Warm caches for both graphs
      cache_key1 = Statistics.quad_cache_key(graph1_id)
      cache_key2 = Statistics.quad_cache_key(graph2_id)

      :ets.insert(@cache_table, [
        {cache_key1, %{graph_id: graph1_id}},
        {cache_key2, %{graph_id: graph2_id}}
      ])

      # Verify caches are populated
      assert [{^cache_key1, _}] = :ets.lookup(@cache_table, cache_key1)
      assert [{^cache_key2, _}] = :ets.lookup(@cache_table, cache_key2)

      # Insert more data to both graphs
      update2 = """
      INSERT DATA {
        GRAPH <http://example.org/graph1> { <http://example.org/s3> <http://example.org/p3> <http://example.org/o3> . }
        GRAPH <http://example.org/graph2> { <http://example.org/s4> <http://example.org/p4> <http://example.org/o4> . }
      }
      """

      {:ok, ast2} = Parser.parse_update(update2)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast2)

      # Verify both caches were invalidated
      Process.sleep(10)
      assert [] = :ets.lookup(@cache_table, cache_key1)
      assert [] = :ets.lookup(@cache_table, cache_key2)
    end
  end

  # ===========================================================================
  # DELETE DATA Cache Invalidation Tests
  # ===========================================================================

  describe "DELETE DATA invalidates statistics cache" do
    test "deleting from default graph invalidates cache", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # First insert data
      update1 =
        "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> . }"

      {:ok, ast1} = Parser.parse_update(update1)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast1)

      # Warm the cache for graph 0
      cache_key = Statistics.quad_cache_key(0)
      :ets.insert(@cache_table, {cache_key, %{graph_id: 0, quad_count: 100}})

      # Verify cache is populated
      assert [{^cache_key, _}] = :ets.lookup(@cache_table, cache_key)

      # Execute DELETE DATA
      update2 =
        "DELETE DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> . }"

      {:ok, ast2} = Parser.parse_update(update2)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast2)

      # Verify cache was invalidated for graph 0
      Process.sleep(10)
      assert [] = :ets.lookup(@cache_table, cache_key)
    end

    test "deleting from named graph invalidates that graph's cache", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # First insert data to named graph
      update1 =
        "INSERT DATA { GRAPH <http://example.org/graph1> { <http://example.org/s> <http://example.org/p> <http://example.org/o> . } }"

      {:ok, ast1} = Parser.parse_update(update1)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast1)

      # Get the graph ID
      {:ok, graph_id} = Adapter.term_to_id(manager, iri("http://example.org/graph1"))

      # Warm the cache
      cache_key = Statistics.quad_cache_key(graph_id)
      :ets.insert(@cache_table, {cache_key, %{graph_id: graph_id, quad_count: 100}})

      # Verify cache is populated
      assert [{^cache_key, _}] = :ets.lookup(@cache_table, cache_key)

      # Execute DELETE DATA
      update2 =
        "DELETE DATA { GRAPH <http://example.org/graph1> { <http://example.org/s> <http://example.org/p> <http://example.org/o> . } }"

      {:ok, ast2} = Parser.parse_update(update2)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast2)

      # Verify cache was invalidated
      Process.sleep(10)
      assert [] = :ets.lookup(@cache_table, cache_key)
    end

    test "deleting from multiple graphs invalidates all affected caches", %{
      db: db,
      manager: manager
    } do
      ctx = %{db: db, dict_manager: manager}

      # First insert data
      update1 = """
      INSERT DATA {
        GRAPH <http://example.org/graph1> { <http://example.org/s> <http://example.org/p> <http://example.org/o> . }
        GRAPH <http://example.org/graph2> { <http://example.org/s2> <http://example.org/p2> <http://example.org/o2> . }
      }
      """

      {:ok, ast1} = Parser.parse_update(update1)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast1)

      # Get graph IDs
      {:ok, graph1_id} = Adapter.term_to_id(manager, iri("http://example.org/graph1"))
      {:ok, graph2_id} = Adapter.term_to_id(manager, iri("http://example.org/graph2"))

      # Warm caches
      cache_key1 = Statistics.quad_cache_key(graph1_id)
      cache_key2 = Statistics.quad_cache_key(graph2_id)

      :ets.insert(@cache_table, [
        {cache_key1, %{graph_id: graph1_id}},
        {cache_key2, %{graph_id: graph2_id}}
      ])

      # Verify caches are populated
      assert [{^cache_key1, _}] = :ets.lookup(@cache_table, cache_key1)
      assert [{^cache_key2, _}] = :ets.lookup(@cache_table, cache_key2)

      # Delete data
      update2 = """
      DELETE DATA {
        GRAPH <http://example.org/graph1> { <http://example.org/s> <http://example.org/p> <http://example.org/o> . }
        GRAPH <http://example.org/graph2> { <http://example.org/s2> <http://example.org/p2> <http://example.org/o2> . }
      }
      """

      {:ok, ast2} = Parser.parse_update(update2)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast2)

      # Verify both caches were invalidated
      Process.sleep(10)
      assert [] = :ets.lookup(@cache_table, cache_key1)
      assert [] = :ets.lookup(@cache_table, cache_key2)
    end
  end

  # ===========================================================================
  # Cache Isolation Tests
  # ===========================================================================

  describe "cache invalidation is isolated to affected graphs" do
    test "modifying one graph does not invalidate other graph caches", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert data to two graphs
      update1 = """
      INSERT DATA {
        GRAPH <http://example.org/graph1> { <http://example.org/s> <http://example.org/p> <http://example.org/o> . }
        GRAPH <http://example.org/graph2> { <http://example.org/s2> <http://example.org/p2> <http://example.org/o2> . }
      }
      """

      {:ok, ast1} = Parser.parse_update(update1)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast1)

      # Get graph IDs
      {:ok, graph1_id} = Adapter.term_to_id(manager, iri("http://example.org/graph1"))
      {:ok, graph2_id} = Adapter.term_to_id(manager, iri("http://example.org/graph2"))

      # Warm caches for both graphs
      cache_key1 = Statistics.quad_cache_key(graph1_id)
      cache_key2 = Statistics.quad_cache_key(graph2_id)

      :ets.insert(@cache_table, [
        {cache_key1, %{graph_id: graph1_id, quad_count: 100}},
        {cache_key2, %{graph_id: graph2_id, quad_count: 100}}
      ])

      # Modify only graph1
      update2 =
        "INSERT DATA { GRAPH <http://example.org/graph1> { <http://example.org/s3> <http://example.org/p3> <http://example.org/o3> . } }"

      {:ok, ast2} = Parser.parse_update(update2)
      assert {:ok, _count} = UpdateExecutor.execute(ctx, ast2)

      # Verify only graph1 cache was invalidated
      Process.sleep(10)
      assert [] = :ets.lookup(@cache_table, cache_key1)
      # graph2 cache still valid
      assert [{^cache_key2, _}] = :ets.lookup(@cache_table, cache_key2)
    end
  end

  # ===========================================================================
  # Telemetry Tests
  # ===========================================================================

  describe "telemetry events for cache invalidation" do
    test "INSERT DATA emits telemetry on cache invalidation", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Attach telemetry handler
      handler_id = "insert-invalid-handler"
      parent_pid = self()

      :telemetry.attach(
        handler_id,
        [:triple_store, :statistics, :quad_cache, :invalidate],
        fn event, measurements, metadata, _config ->
          send(parent_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      # Execute INSERT DATA
      update =
        "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> . }"

      {:ok, ast} = Parser.parse_update(update)
      {:ok, _count} = UpdateExecutor.execute(ctx, ast)

      # Verify telemetry was emitted
      assert_receive {:telemetry, [:triple_store, :statistics, :quad_cache, :invalidate],
                      _measurements, _metadata},
                     1000

      :telemetry.detach(handler_id)
    end

    test "DELETE DATA emits telemetry on cache invalidation", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # First insert data
      update1 =
        "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> . }"

      {:ok, ast1} = Parser.parse_update(update1)
      {:ok, _count} = UpdateExecutor.execute(ctx, ast1)

      # Attach telemetry handler
      handler_id = "delete-invalid-handler"
      parent_pid = self()

      :telemetry.attach(
        handler_id,
        [:triple_store, :statistics, :quad_cache, :invalidate],
        fn event, measurements, metadata, _config ->
          send(parent_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      # Execute DELETE DATA
      update2 =
        "DELETE DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> . }"

      {:ok, ast2} = Parser.parse_update(update2)
      {:ok, _count} = UpdateExecutor.execute(ctx, ast2)

      # Verify telemetry was emitted
      assert_receive {:telemetry, [:triple_store, :statistics, :quad_cache, :invalidate],
                      _measurements, _metadata},
                     1000

      :telemetry.detach(handler_id)
    end
  end
end
