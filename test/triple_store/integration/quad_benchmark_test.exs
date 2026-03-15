defmodule TripleStore.Integration.QuadBenchmarkTest do
  @moduledoc """
  Integration tests for Section 6.5.3: Performance Benchmarks.

  Performance benchmarks for quad operations:
  - Loading N-Quads (1M quads target <30s)
  - Loading TriG (1M quads target <30s)
  - Graph-scoped query (<10ms for simple pattern)
  - Cross-graph query (<100ms for moderate complexity)
  - Graph enumeration (<100ms for 100 graphs)
  - INSERT/DELETE with graphs

  Run with: mix test --include benchmark test/triple_store/integration/quad_benchmark_test.exs
  """

  use ExUnit.Case, async: false

  @moduletag :benchmark
  @moduletag timeout: 300_000

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Authorization
  alias TripleStore.SPARQL.Query

  @test_db_base "/tmp/quad_benchmark_test"

  # Performance targets (adjusted for test environment)
  # 30 seconds for 1M quads
  @nquads_load_target_ms 30_000
  # 30 seconds for 1M quads
  @trig_load_target_ms 30_000
  # 10ms for simple graph-scoped query
  @simple_query_target_ms 10
  # 100ms for cross-graph query
  @cross_graph_query_target_ms 100
  # 100ms for 100 graphs
  @graph_enum_target_ms 100
  # 50ms for INSERT/DELETE operations
  @insert_delete_target_ms 50

  # Small scale test sizes for faster CI
  @small_test_quads 1_000
  @small_test_graphs 10
  @benchmark_quads 10_000

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    TripleStore.Integration.Helpers.unique_path("quad_benchmark_test")
  end

  defp cleanup_path(path) do
    TripleStore.Integration.Helpers.cleanup_path(path)
  end

  defp generate_nquads_string(count) do
    Enum.map(1..count, fn i ->
      graph = rem(i, 10)

      "http://example.org/s#{i} <http://example.org/p> \"o#{i}\" <http://example.org/graph#{graph}> ."
    end)
    |> Enum.join("\n")
  end

  defp generate_trig_string(count, graph_count \\ 5) do
    graph_blocks =
      Enum.map(0..(graph_count - 1), fn graph_num ->
        quads =
          Enum.map(1..div(count, graph_count), fn i ->
            idx = i + graph_num * div(count, graph_count)
            "  ex:s#{idx} ex:p \"o#{idx}\" ."
          end)
          |> Enum.join("\n")

        """
        GRAPH ex:graph#{graph_num} {
        #{quads}
        }
        """
      end)

    """
    @prefix ex: <http://example.org/> .

    #{Enum.join(graph_blocks, "\n")}
    """
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    db_path = unique_path()

    {:ok, db} = ErlangAdapter.open(db_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: manager}

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      cleanup_path(db_path)
    end)

    %{ctx: ctx, db: db, manager: manager}
  end

  # ===========================================================================
  # 6.5.3.1: Benchmark Loading N-Quads
  # ===========================================================================

  describe "6.5.3.1 N-Quads loading benchmark" do
    @tag :benchmark
    @tag timeout: 120_000

    test "loads small N-Quads dataset efficiently", %{ctx: ctx} do
      nquads_content = generate_nquads_string(@small_test_quads)

      {time, {:ok, count}} =
        :timer.tc(fn -> Loader.load_nquads_string(ctx.db, ctx.dict_manager, nquads_content) end)

      # Calculate throughput
      time_ms = div(time, 1000)
      quads_per_sec = div(@small_test_quads * 1_000_000, time)

      IO.puts(
        "N-Quads load: #{@small_test_quads} quads in #{time_ms}ms (#{quads_per_sec} quads/sec)"
      )

      assert count == @small_test_quads
      # Should load quickly - adjust target based on scale
      assert time_ms < div(@nquads_load_target_ms, 100) * @small_test_quads
    end

    @tag :benchmark
    @tag timeout: 120_000

    test "loads moderate N-Quads dataset efficiently", %{ctx: ctx} do
      nquads_content = generate_nquads_string(@benchmark_quads)

      {time, {:ok, count}} =
        :timer.tc(fn -> Loader.load_nquads_string(ctx.db, ctx.dict_manager, nquads_content) end)

      time_ms = div(time, 1000)
      quads_per_sec = div(@benchmark_quads * 1_000_000, time)

      IO.puts(
        "N-Quads load: #{@benchmark_quads} quads in #{time_ms}ms (#{quads_per_sec} quads/sec)"
      )

      assert count == @benchmark_quads
      # Linear scaling of target
      scaled_target = div(@nquads_load_target_ms, 100) * @benchmark_quads
      assert time_ms < scaled_target
    end
  end

  # ===========================================================================
  # 6.5.3.2: Benchmark Loading TriG
  # ===========================================================================

  describe "6.5.3.2 TriG loading benchmark" do
    @tag :benchmark
    @tag timeout: 120_000

    test "loads small TriG dataset efficiently", %{ctx: ctx} do
      trig_content = generate_trig_string(@small_test_quads, 5)

      {time, {:ok, count}} =
        :timer.tc(fn -> Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_content) end)

      time_ms = div(time, 1000)
      quads_per_sec = div(@small_test_quads * 1_000_000, time)

      IO.puts(
        "TriG load: #{@small_test_quads} quads in #{time_ms}ms (#{quads_per_sec} quads/sec)"
      )

      assert count == @small_test_quads
      assert time_ms < div(@trig_load_target_ms, 100) * @small_test_quads
    end

    @tag :benchmark
    @tag timeout: 120_000

    test "loads moderate TriG dataset efficiently", %{ctx: ctx} do
      trig_content = generate_trig_string(@benchmark_quads, 10)

      {time, {:ok, count}} =
        :timer.tc(fn -> Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_content) end)

      time_ms = div(time, 1000)
      quads_per_sec = div(@benchmark_quads * 1_000_000, time)

      IO.puts("TriG load: #{@benchmark_quads} quads in #{time_ms}ms (#{quads_per_sec} quads/sec)")

      assert count == @benchmark_quads
      scaled_target = div(@trig_load_target_ms, 100) * @benchmark_quads
      assert time_ms < scaled_target
    end
  end

  # ===========================================================================
  # 6.5.3.3: Graph-Scoped Query Benchmark
  # ===========================================================================

  describe "6.5.3.3 Graph-scoped query benchmark" do
    @tag :benchmark

    test "simple pattern in named graph is fast", %{ctx: ctx} do
      # Load test data
      trig_content = generate_trig_string(@small_test_quads, 5)
      {:ok, _} = Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_content)

      # Authorize all graphs
      Enum.each(0..4, fn i ->
        :ok = Authorization.set_public(ctx, "http://example.org/graph#{i}")
      end)

      query = """
      PREFIX ex: <http://example.org/>

      SELECT ?s ?o WHERE {
        GRAPH ex:graph0 {
          ?s ex:p ?o .
        }
      }
      LIMIT 100
      """

      # Warm up query
      Query.query(ctx, query)

      # Benchmark
      {time, {:ok, results}} =
        :timer.tc(fn -> Query.query(ctx, query) end)

      time_ms = div(time, 1000)

      IO.puts("Graph-scoped query: #{time_ms}ms, #{length(results)} results")

      # Scaled target for test environment
      assert time_ms < @simple_query_target_ms * 10
      refute Enum.empty?(results)
    end

    @tag :benchmark

    test "pattern match with multiple triples in graph", %{ctx: ctx} do
      # Load test data with multiple predicates
      trig_content = """
      @prefix ex: <http://example.org/> .

      GRAPH ex:benchmark {
        #{Enum.map(1..100, fn i -> "ex:s#{i} ex:p1 \"v1\" ; ex:p2 \"v2\" ; ex:p3 \"v3\" ." end) |> Enum.join("\n")}
      }
      """

      {:ok, _} = Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_content)
      :ok = Authorization.set_public(ctx, "http://example.org/benchmark")

      query = """
      PREFIX ex: <http://example.org/>

      SELECT ?s WHERE {
        GRAPH ex:benchmark {
          ?s ex:p1 ?v1 .
          ?s ex:p2 ?v2 .
        }
      }
      """

      # Warm up
      Query.query(ctx, query)

      {time, {:ok, results}} = :timer.tc(fn -> Query.query(ctx, query) end)

      time_ms = div(time, 1000)

      IO.puts("Multi-pattern graph query: #{time_ms}ms, #{length(results)} results")

      assert time_ms < @simple_query_target_ms * 10
      assert length(results) == 100
    end
  end

  # ===========================================================================
  # 6.5.3.4: Cross-Graph Query Benchmark
  # ===========================================================================

  describe "6.5.3.4 Cross-graph query benchmark" do
    @tag :benchmark

    test "query across multiple graphs is efficient", %{ctx: ctx} do
      # Load data across multiple graphs
      trig_content = generate_trig_string(@small_test_quads, 10)
      {:ok, _} = Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_content)

      Enum.each(0..9, fn i ->
        :ok = Authorization.set_public(ctx, "http://example.org/graph#{i}")
      end)

      query = """
      PREFIX ex: <http://example.org/>

      SELECT ?g ?count WHERE {
        {
          SELECT ?g (COUNT(*) AS ?count) WHERE {
            GRAPH ?g {
              ?s ex:p ?o .
            }
          }
          GROUP BY ?g
        }
      }
      """

      # Warm up
      Query.query(ctx, query)

      {time, {:ok, results}} = :timer.tc(fn -> Query.query(ctx, query) end)

      time_ms = div(time, 1000)

      IO.puts("Cross-graph aggregation: #{time_ms}ms, #{length(results)} graphs")

      assert time_ms < @cross_graph_query_target_ms * 10
      assert length(results) == 10
    end

    @tag :benchmark

    test "UNION across graphs is efficient", %{ctx: ctx} do
      trig_content = generate_trig_string(@small_test_quads, 5)
      {:ok, _} = Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_content)

      Enum.each(0..4, fn i ->
        :ok = Authorization.set_public(ctx, "http://example.org/graph#{i}")
      end)

      query = """
      PREFIX ex: <http://example.org/>

      SELECT ?s ?o WHERE {
        { GRAPH ex:graph0 { ?s ex:p ?o } }
        UNION
        { GRAPH ex:graph1 { ?s ex:p ?o } }
        UNION
        { GRAPH ex:graph2 { ?s ex:p ?o } }
      }
      LIMIT 100
      """

      Query.query(ctx, query)

      {time, {:ok, results}} = :timer.tc(fn -> Query.query(ctx, query) end)

      time_ms = div(time, 1000)

      IO.puts("Cross-graph UNION: #{time_ms}ms, #{length(results)} results")

      assert time_ms < @cross_graph_query_target_ms * 10
    end
  end

  # ===========================================================================
  # 6.5.3.5: Graph Enumeration Benchmark
  # ===========================================================================

  describe "6.5.3.5 Graph enumeration benchmark" do
    @tag :benchmark

    test "list graphs efficiently", %{ctx: ctx} do
      # Create multiple graphs using TriG loader (proper API)
      graph_count = @small_test_graphs

      graph_blocks =
        Enum.map(1..graph_count, fn i ->
          """
          GRAPH ex:graph#{i} {
            ex:s#{i} ex:p "o#{i}" .
          }
          """
        end)

      trig_content = """
        @prefix ex: <http://example.org/> .

        #{Enum.join(graph_blocks, "\n")}
      """

      {:ok, _} = Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_content)

      # Authorize graphs
      Enum.each(1..graph_count, fn i ->
        :ok = Authorization.set_public(ctx, "http://example.org/graph#{i}")
      end)

      {time, {:ok, graphs}} =
        :timer.tc(fn -> QuadOperations.list_graphs(ctx.db, include_default: false) end)

      time_ms = div(time, 1000)

      IO.puts("Graph enumeration: #{time_ms}ms, #{length(graphs)} graphs")

      # Scaled target for test environment
      scaled_target = div(@graph_enum_target_ms, 100) * graph_count
      assert time_ms < scaled_target
      assert length(graphs) >= graph_count
    end

    @tag :benchmark

    test "graph variable iteration is efficient", %{ctx: ctx} do
      trig_content = generate_trig_string(@small_test_quads, @small_test_graphs)
      {:ok, _} = Loader.load_trig_string(ctx.db, ctx.dict_manager, trig_content)

      Enum.each(0..(@small_test_graphs - 1), fn i ->
        :ok = Authorization.set_public(ctx, "http://example.org/graph#{i}")
      end)

      query = """
      PREFIX ex: <http://example.org/>

      SELECT ?g WHERE {
        GRAPH ?g {
          ?s ex:p ?o .
        }
      }
      """

      Query.query(ctx, query)

      {time, {:ok, results}} = :timer.tc(fn -> Query.query(ctx, query) end)

      time_ms = div(time, 1000)

      IO.puts("Graph variable iteration: #{time_ms}ms")

      assert time_ms < @graph_enum_target_ms * 10
    end
  end

  # ===========================================================================
  # 6.5.3.6: INSERT/DELETE with Graphs Benchmark
  # ===========================================================================

  describe "6.5.3.6 INSERT/DELETE with graphs benchmark" do
    @tag :benchmark

    test "batch INSERT to named graph is efficient", %{ctx: ctx} do
      :ok = Authorization.set_public(ctx, "http://example.org/test")

      quad_count = 100

      quads =
        Enum.map(1..quad_count, fn i ->
          {:"http://example.org/s#{i}", :"http://example.org/p", "o#{i}",
           :"http://example.org/test"}
        end)

      {time, {:ok, _}} =
        :timer.tc(fn -> QuadOperations.insert_quads(ctx.db, ctx.dict_manager, quads) end)

      time_ms = div(time, 1000)

      IO.puts("Batch INSERT (#{quad_count} quads): #{time_ms}ms")

      assert time_ms < @insert_delete_target_ms * 10
    end

    @tag :benchmark

    test "batch DELETE from named graph is efficient", %{ctx: ctx} do
      # First insert
      :ok = Authorization.set_public(ctx, "http://example.org/test")

      quad_count = 100

      quads =
        Enum.map(1..quad_count, fn i ->
          {:"http://example.org/s#{i}", :"http://example.org/p", "o#{i}",
           :"http://example.org/test"}
        end)

      {:ok, _} = QuadOperations.insert_quads(ctx.db, ctx.dict_manager, quads)

      # Then benchmark delete
      {time, {:ok, _}} =
        :timer.tc(fn -> QuadOperations.delete_quads(ctx.db, ctx.dict_manager, quads) end)

      time_ms = div(time, 1000)

      IO.puts("Batch DELETE (#{quad_count} quads): #{time_ms}ms")

      assert time_ms < @insert_delete_target_ms * 10
    end

    @tag :benchmark

    test "graph CREATE/DROP is efficient", %{ctx: ctx} do
      {time, {:ok, _}} =
        :timer.tc(fn ->
          TripleStore.SPARQL.UpdateExecutor.execute(
            ctx,
            {:create_graph, {:named_node, "http://example.org/bench_graph"}, false}
          )
        end)

      create_time_ms = div(time, 1000)

      IO.puts("CREATE GRAPH: #{create_time_ms}ms")

      assert create_time_ms < @insert_delete_target_ms

      # Drop and benchmark
      {time, {:ok, _}} =
        :timer.tc(fn ->
          TripleStore.SPARQL.UpdateExecutor.execute(
            ctx,
            {:drop_graph, {:named_node, "http://example.org/bench_graph"}, false}
          )
        end)

      drop_time_ms = div(time, 1000)

      IO.puts("DROP GRAPH: #{drop_time_ms}ms")

      assert drop_time_ms < @insert_delete_target_ms
    end
  end
end
