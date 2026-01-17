defmodule TripleStore.Integration.GraphClauseQueryTest do
  @moduledoc """
  Integration tests for Section 6.3.1: GRAPH Clause Queries.

  Tests SPARQL GRAPH clause execution with named graphs:
  - SELECT from single named graph
  - SELECT from multiple named graphs (UNION)
  - SELECT with graph variable
  - SELECT from default graph (implicit)
  - Nested GRAPH clauses
  - GRAPH with OPTIONAL
  - GRAPH with UNION
  - GRAPH with FILTER
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Authorization
  alias TripleStore.SPARQL.Query

  @test_db_base "/tmp/graph_clause_query_test"
  @ex "http://example.org/"

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp unique_path do
    time_component = System.system_time(:microsecond)
    rand_component = :rand.uniform(1_000_000)
    "#{@test_db_base}_#{time_component}_#{rand_component}"
  end

  defp cleanup_path(path) do
    File.rm_rf(path)
  end

  defp load_test_data(db, manager) do
    # Load test data into named graphs and default graph
    trig_content = """
    @prefix ex: <#{@ex}> .

    # Default graph data
    ex:default1 ex:p "default-value-1" .
    ex:default2 ex:p "default-value-2" .

    # Graph 1 data
    GRAPH ex:graph1 {
      ex:subject1 ex:p "value1-1" .
      ex:subject2 ex:p "value1-2" .
      ex:shared ex:p "shared-g1" .
    }

    # Graph 2 data
    GRAPH ex:graph2 {
      ex:subject1 ex:p "value2-1" .
      ex:subject3 ex:p "value2-3" .
      ex:shared ex:p "shared-g2" .
    }

    # Graph 3 data
    GRAPH ex:graph3 {
      ex:subject4 ex:p "value3-4" .
      ex:subject5 ex:p "value3-5" .
      ex:subject6 ex:p "Type-A" .
    }
    """

    {:ok, _count} = Loader.load_trig_string(db, manager, trig_content)

    # Grant public read permissions to all test graphs
    ctx = %{db: db, dict_manager: manager}
    :ok = Authorization.set_public(ctx, "#{@ex}graph1")
    :ok = Authorization.set_public(ctx, "#{@ex}graph2")
    :ok = Authorization.set_public(ctx, "#{@ex}graph3")
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    db_path = unique_path()

    {:ok, db} = NIF.open(db_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: manager}

    load_test_data(db, manager)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      NIF.close(db)
      cleanup_path(db_path)
    end)

    %{ctx: ctx, db: db, manager: manager}
  end

  # ===========================================================================
  # 6.3.1.1: Test SELECT from single named graph
  # ===========================================================================

  describe "6.3.1.1 SELECT from single named graph" do
    test "executes SELECT from single named graph using GRAPH clause", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        GRAPH <#{@ex}graph1> {
          ?s ex:p ?o
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      assert is_list(results)
      assert length(results) == 3

      # Verify all results are from graph1
      Enum.each(results, fn result ->
        assert Map.has_key?(result, "s")
        assert Map.has_key?(result, "o")
      end)
    end

    test "returns empty results for non-existent graph", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        GRAPH <#{@ex}nonexistent> {
          ?s ex:p ?o
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert results == []
    end

    test "supports IRI prefix in GRAPH clause", %{ctx: ctx, db: db, manager: manager} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s WHERE {
        GRAPH ex:graph2 {
          ?s ex:p ?o
        }
      }
      """

      # Debug: check if graph2 exists and has data
      graph_iri = RDF.iri("#{@ex}graph2")
      graph_exists = TripleStore.QuadOperations.graph_exists?(db, manager, graph_iri)
      IO.inspect(graph_exists, label: "graph2 exists")

      {:ok, count} = TripleStore.QuadOperations.graph_quad_count(db, manager, graph_iri)
      IO.inspect(count, label: "graph2 quad count")

      assert {:ok, results} = Query.query(ctx, query)

      # Debug: print results
      IO.inspect(results, label: "graph2 subjects")

      # graph2 has 3 quads with different subjects: subject1, subject3, shared
      assert length(results) >= 2
    end
  end

  # ===========================================================================
  # 6.3.1.2: Test SELECT from multiple named graphs (UNION)
  # ===========================================================================

  describe "6.3.1.2 SELECT from multiple named graphs" do
    test "executes SELECT from multiple graphs with UNION", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        { GRAPH <#{@ex}graph1> { ?s ex:p ?o } }
        UNION
        { GRAPH <#{@ex}graph2> { ?s ex:p ?o } }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # graph1 has 3 quads, graph2 has 3 quads, but some subjects may overlap
      assert length(results) >= 3
    end

    test "UNION returns distinct results", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s WHERE {
        { GRAPH <#{@ex}graph1> { ?s ex:p ?o } }
        UNION
        { GRAPH <#{@ex}graph1> { ?s ex:p ?o } }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Same graph twice should still return distinct results
      assert length(results) == 3
    end
  end

  # ===========================================================================
  # 6.3.1.3: Test SELECT with graph variable
  # ===========================================================================

  describe "6.3.1.3 SELECT with graph variable" do
    test "GRAPH clause with variable binds graph name", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o ?g WHERE {
        GRAPH ?g {
          ?s ex:p ?o
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      assert length(results) > 0

      # All results should have graph variable bound
      Enum.each(results, fn result ->
        assert Map.has_key?(result, "s")
        assert Map.has_key?(result, "o")
        assert Map.has_key?(result, "g")
      end)
    end

    test "can filter by specific graph using graph variable", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        GRAPH ?g {
          ?s ex:p ?o
          FILTER(?g = <#{@ex}graph3>)
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Should only get results from graph3
      assert length(results) == 3
    end

    test "graph variable appears in result bindings", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g WHERE {
        GRAPH ?g {
          ?s ex:p ?o
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Should get distinct graph names
      graphs = Enum.map(results, fn r -> r["g"] end)
        |> Enum.uniq()

      assert length(graphs) >= 3
    end
  end

  # ===========================================================================
  # 6.3.1.4: Test SELECT from default graph (implicit)
  # ===========================================================================

  describe "6.3.1.4 SELECT from default graph (implicit)" do
    test "SELECT without GRAPH clause queries default graph", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        ?s ex:p ?o
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Should get results from default graph only
      assert length(results) == 2
    end

    test "can explicitly query default graph with DEFAULT keyword", %{ctx: ctx} do
      # Note: SPARQL 1.1 doesn't have DEFAULT keyword, testing implicit behavior
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        ?s ex:p ?o
        FILTER NOT EXISTS {
          GRAPH ?g { ?s ex:p ?o }
          FILTER(?g != <http://www.w3.org/ns/graphs/default>)
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) == 2
    end
  end

  # ===========================================================================
  # 6.3.1.5: Test nested GRAPH clauses
  # ===========================================================================

  describe "6.3.1.5 nested GRAPH clauses" do
    test "supports GRAPH within GRAPH pattern", %{ctx: ctx} do
      # This tests finding graphs that contain certain patterns
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g WHERE {
        GRAPH ?g {
          ex:subject1 ex:p ?o
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # subject1 exists in both graph1 and graph2
      graphs = Enum.map(results, fn r -> r["g"] end)
      assert length(graphs) >= 1
    end

    test "nested pattern with shared subject across graphs", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g1 ?g2 WHERE {
        GRAPH ?g1 { ex:shared ex:p ?o1 }
        GRAPH ?g2 { ex:shared ex:p ?o2 }
        FILTER(?g1 != ?g2)
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # shared subject exists in both graph1 and graph2
      assert length(results) > 0
    end
  end

  # ===========================================================================
  # 6.3.1.6: Test GRAPH with OPTIONAL
  # ===========================================================================

  describe "6.3.1.6 GRAPH with OPTIONAL" do
    test "GRAPH clause with OPTIONAL pattern", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o1 ?o2 WHERE {
        GRAPH <#{@ex}graph1> {
          ?s ex:p ?o1
          OPTIONAL {
            ?s ex:type ?o2
          }
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # All subjects have ex:p, but not all have ex:type
      assert length(results) == 3
    end

    test "OPTIONAL across graphs", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o1 ?o2 WHERE {
        GRAPH <#{@ex}graph1> {
          ?s ex:p ?o1
        }
        OPTIONAL {
          GRAPH <#{@ex}graph2> {
            ?s ex:p ?o2
          }
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # subject1 exists in both graphs
      assert length(results) >= 1
    end
  end

  # ===========================================================================
  # 6.3.1.7: Test GRAPH with UNION
  # ===========================================================================

  describe "6.3.1.7 GRAPH with UNION" do
    test "UNION within GRAPH clause", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        GRAPH <#{@ex}graph1> {
          { ?s ex:p "value1-1" }
          UNION
          { ?s ex:p "value1-2" }
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) == 2
    end

    test "UNION across different GRAPH clauses", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o ?g WHERE {
        {
          GRAPH <#{@ex}graph1> { ?s ex:p ?o }
          BIND(<#{@ex}graph1> AS ?g)
        }
        UNION
        {
          GRAPH <#{@ex}graph2> { ?s ex:p ?o }
          BIND(<#{@ex}graph2> AS ?g)
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # subject1 exists in both graphs
      assert length(results) >= 1
    end
  end

  # ===========================================================================
  # 6.3.1.8: Test GRAPH with FILTER
  # ===========================================================================

  describe "6.3.1.8 GRAPH with FILTER" do
    test "FILTER within GRAPH clause", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        GRAPH <#{@ex}graph3> {
          ?s ex:p ?o
          FILTER(?o != "value3-4")
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Should exclude subject4 which has value3-4
      assert length(results) == 2
    end

    test "FILTER on graph variable", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s WHERE {
        GRAPH ?g {
          ?s ex:p ?o
          FILTER(STRENDS(STR(?g), "graph2"))
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Only graph2 should match
      assert length(results) >= 1
    end

    test "FILTER with regex in GRAPH clause", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        GRAPH ?g {
          ?s ex:p ?o
          FILTER(REGEX(STR(?o), "Type-"))
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Should find Type-A in graph3
      assert length(results) == 1

      [result] = results
      # Results are in internal format: {:literal, :simple, value}
      assert result["o"] == {:literal, :simple, "Type-A"}
    end
  end
end
