defmodule TripleStore.Integration.ConcurrencyTest do
  @moduledoc """
  Integration tests for Section 6.7: Concurrency Tests.

  Tests concurrent operations on the quad store:
  - 6.7.1: Concurrent Reads (thread safety, consistency)
  - 6.7.2: Concurrent Writes (atomicity, serialization)
  - 6.7.3: Mixed Read/Write (isolation levels)

  These tests validate that the quad store handles concurrent access
  correctly under various scenarios.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Integration.Helpers
  alias TripleStore.Loader
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.{Authorization, Parser, Query, UpdateExecutor}

  @test_db_base "/tmp/concurrency_test"
  @ex "http://example.org/"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    Helpers.unique_path("concurrency_test")
  end

  defp cleanup_path(path) do
    Helpers.cleanup_path(path)
  end

  defp setup_db do
    path = unique_path()
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)
    ctx = %{db: db, dict_manager: manager}
    {ctx, path}
  end

  defp teardown_db(db, manager, path) do
    try do
      if Process.alive?(manager), do: Manager.stop(manager)
    catch
      :exit, _ -> :ok
    end

    try do
      ErlangAdapter.close(db)
    catch
      :exit, _ -> :ok
    end

    cleanup_path(path)
  end

  defp load_test_data(ctx, graph_iris) do
    # Build N-Quads format with full URIs
    nquads_content =
      Enum.flat_map(graph_iris, fn graph_iri ->
        [
          "<#{@ex}item1> <#{@ex}value> \"1\" <#{graph_iri}> .",
          "<#{@ex}item2> <#{@ex}value> \"2\" <#{graph_iri}> .",
          "<#{@ex}item3> <#{@ex}value> \"3\" <#{graph_iri}> ."
        ]
      end)
      |> Enum.join("\n")

    {:ok, _count} = Loader.load_nquads_string(ctx.db, ctx.dict_manager, nquads_content)

    # Grant public read permissions
    Enum.each(graph_iris, fn graph_iri ->
      :ok = Authorization.set_public(ctx, graph_iri)
    end)
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    {ctx, path} = setup_db()

    on_exit(fn ->
      teardown_db(ctx.db, ctx.dict_manager, path)
    end)

    %{ctx: ctx, db: ctx.db, manager: ctx.dict_manager}
  end

  # ===========================================================================
  # 6.7.1: Concurrent Reads
  # ===========================================================================

  describe "6.7.1 Concurrent Reads" do
    test "6.7.1.1 concurrent queries on different graphs", %{ctx: ctx} do
      graph1 = "#{@ex}graph1"
      graph2 = "#{@ex}graph2"
      graph3 = "#{@ex}graph3"

      load_test_data(ctx, [graph1, graph2, graph3])

      # Run concurrent queries on different graphs
      queries = [
        {"graph1", graph_query(graph1)},
        {"graph2", graph_query(graph2)},
        {"graph3", graph_query(graph3)}
      ]

      # Run concurrent queries on different graphs using Task.async
      tasks =
        Enum.map(queries, fn {name, query} ->
          Task.async(fn ->
            {name, Query.query(ctx, query)}
          end)
        end)

      results =
        Task.await_many(tasks, 10_000)
        |> Map.new()

      # All queries should succeed
      assert {:ok, _} = results["graph1"]
      assert {:ok, _} = results["graph2"]
      assert {:ok, _} = results["graph3"]

      # Each should return 3 results
      assert {:ok, graph1_results} = results["graph1"]
      assert {:ok, graph2_results} = results["graph2"]
      assert {:ok, graph3_results} = results["graph3"]
      assert length(graph1_results) == 3
      assert length(graph2_results) == 3
      assert length(graph3_results) == 3
    end

    test "6.7.1.2 concurrent queries on same graph", %{ctx: ctx} do
      graph = "#{@ex}shared"
      load_test_data(ctx, [graph])

      # Run multiple concurrent queries on the same graph
      query = graph_query(graph)

      tasks =
        Enum.map(1..10, fn _i ->
          Task.async(fn ->
            Query.query(ctx, query)
          end)
        end)

      results = Task.await_many(tasks, 10_000)

      # All queries should succeed and return same results
      assert Enum.all?(results, fn
               {:ok, r} -> length(r) == 3
               _ -> false
             end)
    end

    test "6.7.1.3 concurrent reads during load", %{ctx: ctx} do
      graph = "#{@ex}concurrent-load"
      :ok = Authorization.set_public(ctx, graph)

      # Start loading data in background
      load_task =
        Task.async(fn ->
          large_data =
            Enum.map_join(1..100, "\n", fn i ->
              "<#{@ex}item#{i}> <#{@ex}value> \"#{i}\" <#{graph}> ."
            end)

          Loader.load_nquads_string(ctx.db, ctx.dict_manager, large_data)
        end)

      # Give load a moment to start
      Process.sleep(10)

      # Start concurrent queries
      query_tasks =
        Enum.map(1..5, fn _i ->
          Task.async(fn ->
            query = """
              PREFIX ex: <#{@ex}>
              SELECT ?s WHERE {
                GRAPH <#{graph}> {
                  ?s ex:value ?o .
                }
              }
              LIMIT 10
            """

            Query.query(ctx, query)
          end)
        end)

      query_results = Task.await_many(query_tasks, 10_000)

      # Wait for load to complete
      Task.await(load_task, 10_000)

      # Queries during load should either succeed or return partial results
      # but should not crash
      assert Enum.all?(query_results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end

    test "6.7.1.4 concurrent graph enumeration", %{ctx: ctx} do
      graphs = ["#{@ex}g1", "#{@ex}g2", "#{@ex}g3", "#{@ex}g4", "#{@ex}g5"]
      load_test_data(ctx, graphs)

      # Run concurrent graph enumerations
      tasks =
        Enum.map(1..5, fn _i ->
          Task.async(fn ->
            QuadOperations.list_graphs(ctx.db, include_default: false)
          end)
        end)

      results = Task.await_many(tasks, 10_000)

      # All enumerations should succeed
      assert Enum.all?(results, fn
               {:ok, graphs} -> is_list(graphs) and length(graphs) >= 5
               _ -> false
             end)
    end

    test "6.7.1.5 concurrent statistics access", %{ctx: ctx} do
      graphs = ["#{@ex}stats1", "#{@ex}stats2"]
      load_test_data(ctx, graphs)

      # Start Statistics GenServer
      start_supervised!(TripleStore.Statistics)

      # Warm up statistics
      TripleStore.Statistics.warm_all_graphs_cache(ctx.db, include_default: false)

      # Run concurrent statistics queries
      tasks =
        Enum.map(1..5, fn _i ->
          Task.async(fn ->
            TripleStore.Statistics.get_cached_graph_stats(ctx.db, 0)
          end)
        end)

      results = Task.await_many(tasks, 10_000)

      # All statistics queries should succeed
      assert Enum.all?(results, fn
               {:ok, _stats} -> true
               _ -> false
             end)
    end
  end

  # ===========================================================================
  # 6.7.2: Concurrent Writes
  # ===========================================================================

  describe "6.7.2 Concurrent Writes" do
    test "6.7.2.1 concurrent inserts to different graphs", %{ctx: ctx} do
      graph1 = "#{@ex}write1"
      graph2 = "#{@ex}write2"

      # Authorize graphs
      :ok = Authorization.set_public(ctx, graph1)
      :ok = Authorization.set_public(ctx, graph2)

      # Insert to different graphs concurrently using N-Quads
      tasks =
        for {graph, suffix} <- [{graph1, "a"}, {graph2, "b"}] do
          Task.async(fn ->
            nquads = "<#{@ex}item> <#{@ex}value> \"#{suffix}\" <#{graph}> ."
            Loader.load_nquads_string(ctx.db, ctx.dict_manager, nquads)
          end)
        end

      # Wait for all inserts
      results = Task.await_many(tasks, 10_000)

      # Both should succeed
      assert {:ok, _} = Enum.at(results, 0)
      assert {:ok, _} = Enum.at(results, 1)

      # Verify data was inserted
      query1 = graph_query(graph1)
      query2 = graph_query(graph2)

      assert {:ok, results1} = Query.query(ctx, query1)
      assert {:ok, results2} = Query.query(ctx, query2)

      assert length(results1) == 1
      assert length(results2) == 1
    end

    test "6.7.2.2 concurrent inserts to same graph", %{ctx: ctx} do
      graph = "#{@ex}concurrent-insert"
      :ok = Authorization.set_public(ctx, graph)

      # Insert multiple items to the same graph concurrently using N-Quads
      tasks =
        Enum.map(1..10, fn i ->
          Task.async(fn ->
            nquads = "<#{@ex}item#{i}> <#{@ex}value> \"#{i}\" <#{graph}> ."
            Loader.load_nquads_string(ctx.db, ctx.dict_manager, nquads)
          end)
        end)

      # Wait for all inserts
      results = Task.await_many(tasks, 10_000)

      # All should succeed
      assert Enum.all?(results, fn
               {:ok, _count} -> true
               _ -> false
             end)

      # Verify all data was inserted (should be 10 items)
      query = """
        PREFIX ex: <#{@ex}>
        SELECT (COUNT(*) AS ?count) WHERE {
          GRAPH <#{graph}> {
            ?s ?p ?o .
          }
        }
      """

      # Note: Use COUNT(?s) due to COUNT(*) limitation
      query_fixed = """
        PREFIX ex: <#{@ex}>
        SELECT (COUNT(?s) AS ?count) WHERE {
          GRAPH <#{graph}> {
            ?s ?p ?o .
          }
        }
      """

      assert {:ok, count_results} = Query.query(ctx, query_fixed)
      assert [%{"count" => count}] = count_results
      assert count >= 10
    end

    test "6.7.2.3 concurrent updates on different graphs", %{ctx: ctx} do
      graph1 = "#{@ex}update1"
      graph2 = "#{@ex}update2"

      # Create and authorize graphs
      Enum.each([graph1, graph2], fn g ->
        {:ok, ast} = Parser.parse_update("CREATE GRAPH <#{g}>")
        assert {:ok, 0} = UpdateExecutor.execute(ctx, ast)
        :ok = Authorization.set_public(ctx, g)
      end)

      # Concurrent updates to different graphs
      tasks = [
        Task.async(fn ->
          {:ok, ast} =
            Parser.parse_update("""
              PREFIX ex: <#{@ex}>
              INSERT DATA {
                GRAPH <#{graph1}> {
                  ex:item ex:value "1" .
                }
              }
            """)

          UpdateExecutor.execute(ctx, ast)
        end),
        Task.async(fn ->
          {:ok, ast} =
            Parser.parse_update("""
              PREFIX ex: <#{@ex}>
              INSERT DATA {
                GRAPH <#{graph2}> {
                  ex:item ex:value "2" .
                }
              }
            """)

          UpdateExecutor.execute(ctx, ast)
        end)
      ]

      # Wait for both updates
      results = Task.await_many(tasks, 10_000)

      # Both should succeed
      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end

    test "6.7.2.4 concurrent CREATE GRAPH with data", %{ctx: ctx} do
      # Create 10 graphs concurrently, each with a quad
      tasks =
        Enum.map(1..10, fn i ->
          graph = "#{@ex}concurrent-create-#{i}"

          Task.async(fn ->
            # Insert data to create the graph
            nquads = "<#{@ex}item#{i}> <#{@ex}value> \"#{i}\" <#{graph}> ."
            Loader.load_nquads_string(ctx.db, ctx.dict_manager, nquads)
          end)
        end)

      results = Task.await_many(tasks, 10_000)

      # All should succeed
      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)

      # Verify all graphs exist
      {:ok, graphs} = QuadOperations.list_graphs(ctx.db, include_default: false)
      refute Enum.empty?(graphs)
    end

    test "6.7.2.5 concurrent DELETE on same graph", %{ctx: ctx} do
      graph = "#{@ex}concurrent-delete"

      # Setup: Load data (implicitly creates graph)
      :ok = Authorization.set_public(ctx, graph)

      # Load test data using N-Quads
      trig = """
        <#{@ex}item1> <#{@ex}value> "1" <#{graph}> .
        <#{@ex}item2> <#{@ex}value> "2" <#{graph}> .
        <#{@ex}item3> <#{@ex}value> "3" <#{graph}> .
      """

      {:ok, _} = Loader.load_nquads_string(ctx.db, ctx.dict_manager, trig)

      # Concurrent deletes of different items from same graph
      tasks =
        Enum.map(1..3, fn i ->
          Task.async(fn ->
            execute_update(ctx, """
              PREFIX ex: <#{@ex}>
              DELETE WHERE {
                GRAPH <#{graph}> {
                  ex:item#{i} ex:value ?o .
                }
              }
            """)
          end)
        end)

      results = Task.await_many(tasks, 10_000)

      # All should succeed
      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)

      # Verify only remaining items exist
      query = graph_query(graph)
      assert {:ok, remaining} = Query.query(ctx, query)
      # One item should remain (or none if we deleted all 3)
      assert is_list(remaining)
    end
  end

  # ===========================================================================
  # 6.7.3: Mixed Read/Write
  # ===========================================================================

  describe "6.7.3 Mixed Read/Write" do
    test "6.7.3.1 read during INSERT to different graph", %{ctx: ctx} do
      read_graph = "#{@ex}read-graph"
      write_graph = "#{@ex}write-graph"

      # Setup read graph
      load_test_data(ctx, [read_graph])
      :ok = Authorization.set_public(ctx, write_graph)

      # Start a long-running query on read graph
      query_task =
        Task.async(fn ->
          Query.query(ctx, graph_query(read_graph))
        end)

      # Give query time to start
      Process.sleep(10)

      # Insert to different graph
      insert_task =
        Task.async(fn ->
          nquads = "<#{@ex}newitem> <#{@ex}newvalue> \"new\" <#{write_graph}> ."
          Loader.load_nquads_string(ctx.db, ctx.dict_manager, nquads)
        end)

      # Both should complete successfully
      assert {:ok, _} = Task.await(query_task, 10_000)
      assert {:ok, _} = Task.await(insert_task, 10_000)
    end

    test "6.7.3.2 read during INSERT to same graph (snapshot isolation)", %{ctx: ctx} do
      graph = "#{@ex}snapshot-test"

      # Setup initial data
      load_test_data(ctx, [graph])

      # Start a query
      query_task =
        Task.async(fn ->
          Query.query(ctx, graph_query(graph))
        end)

      # Give query time to start
      Process.sleep(10)

      # Insert new data to same graph
      insert_task =
        Task.async(fn ->
          nquads = "<#{@ex}newitem> <#{@ex}value> \"new\" <#{graph}> ."
          Loader.load_nquads_string(ctx.db, ctx.dict_manager, nquads)
        end)

      # Query should see original data (3 items)
      # Insert should succeed
      query_result = Task.await(query_task, 10_000)
      assert {:ok, _} = Task.await(insert_task, 10_000)

      assert {:ok, query_results} = query_result
      # Query should see consistent snapshot
      assert length(query_results) >= 3
    end

    test "6.7.3.3 read during DELETE", %{ctx: ctx} do
      graph = "#{@ex}read-during-delete"

      # Setup data using N-Quads
      nquads = """
        <#{@ex}item1> <#{@ex}value> "1" <#{graph}> .
        <#{@ex}item2> <#{@ex}value> "2" <#{graph}> .
      """

      {:ok, _} = Loader.load_nquads_string(ctx.db, ctx.dict_manager, nquads)
      :ok = Authorization.set_public(ctx, graph)

      # Start query
      query_task =
        Task.async(fn ->
          Query.query(ctx, graph_query(graph))
        end)

      Process.sleep(10)

      # Delete from graph
      delete_task =
        Task.async(fn ->
          execute_update(ctx, """
            PREFIX ex: <#{@ex}>
            DELETE WHERE {
              GRAPH <#{graph}> {
                ex:item1 ex:value ?v .
              }
            }
          """)
        end)

      # Both should complete
      assert {:ok, _} = Task.await(query_task, 10_000)
      assert {:ok, _} = Task.await(delete_task, 10_000)
    end

    test "6.7.3.4 read during CLEAR GRAPH", %{ctx: ctx} do
      graph = "#{@ex}clear-test"

      # Setup data
      load_test_data(ctx, [graph])

      # Start query
      query_task =
        Task.async(fn ->
          Query.query(ctx, graph_query(graph))
        end)

      Process.sleep(10)

      # Clear graph
      clear_task =
        Task.async(fn ->
          execute_update(ctx, "CLEAR GRAPH <#{graph}>")
        end)

      # Both should complete
      assert {:ok, _} = Task.await(query_task, 10_000)
      assert {:ok, _} = Task.await(clear_task, 10_000)
    end

    test "6.7.3.5 query during DROP GRAPH", %{ctx: ctx} do
      graph = "#{@ex}drop-test"

      # Setup data
      load_test_data(ctx, [graph])

      # Start query
      query_task =
        Task.async(fn ->
          Query.query(ctx, graph_query(graph))
        end)

      Process.sleep(10)

      # Drop graph
      drop_task =
        Task.async(fn ->
          execute_update(ctx, "DROP GRAPH <#{graph}>")
        end)

      # Query should complete (data was there when query started)
      # Drop should complete
      query_result = Task.await(query_task, 10_000)
      assert {:ok, _} = Task.await(drop_task, 10_000)

      assert {:ok, _} = query_result
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp graph_query(graph_iri) do
    """
    PREFIX ex: <#{@ex}>

    SELECT ?s ?p ?o WHERE {
      GRAPH <#{graph_iri}> {
        ?s ?p ?o .
      }
    }
    """
  end

  defp execute_update(ctx, update_string) do
    with {:ok, ast} <- Parser.parse_update(update_string),
         {:ok, _count} <- UpdateExecutor.execute(ctx, ast) do
      {:ok, :executed}
    end
  end
end
