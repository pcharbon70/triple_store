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

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.SPARQL.Authorization
  alias TripleStore.SPARQL.Query

  @test_db_base "/tmp/graph_clause_query_test"
  @ex "http://example.org/"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    TripleStore.Integration.Helpers.unique_path("graph_clause_query_test")
  end

  defp cleanup_path(path) do
    TripleStore.Integration.Helpers.cleanup_path(path)
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

    {:ok, db} = ErlangAdapter.open(db_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: manager}

    load_test_data(db, manager)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
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

    test "supports IRI prefix in GRAPH clause", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s WHERE {
        GRAPH ex:graph2 {
          ?s ex:p ?o
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

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
      graphs =
        Enum.map(results, fn r -> r["g"] end)
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

    test "can distinguish default graph from named graphs", %{ctx: ctx} do
      # Query for data that's only in the default graph
      # The default graph has ex:default1 and ex:default2
      # Named graphs have ex:subject1, ex:subject2, ex:subject3, ex:shared
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?o WHERE {
        # Query default graph
        ?s ex:p ?o
        # Filter to only include results from subjects unique to default graph
        FILTER (?s = ex:default1 || ?s = ex:default2)
      }
      """

      assert {:ok, results} = Query.query(ctx, query)
      assert length(results) == 2

      # Verify the subjects are from default graph
      subjects = Enum.map(results, fn r -> r["s"] end)
      assert {:named_node, "http://example.org/default1"} in subjects
      assert {:named_node, "http://example.org/default2"} in subjects
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

  # ===========================================================================
  # 6.3.2: Cross-Graph Queries
  # ===========================================================================

  describe "6.3.2 Cross-Graph Queries" do
    test "6.3.2.1 query patterns across two graphs", %{ctx: ctx} do
      # Query that finds triples matching a pattern across two different graphs
      # The 'shared' subject exists in both graph1 and graph2
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g ?o WHERE {
        GRAPH ?g {
          ex:shared ex:p ?o
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Should find 2 results (one from each graph)
      assert length(results) == 2

      # Verify we get results from both graphs
      graph_names = Enum.map(results, fn r -> r["g"] end)

      graph_iris =
        Enum.map(graph_names, fn
          {:named_node, iri} -> iri
          %RDF.IRI{} = iri -> RDF.IRI.to_string(iri)
          iri when is_binary(iri) -> iri
        end)

      assert "http://example.org/graph1" in graph_iris
      assert "http://example.org/graph2" in graph_iris
    end

    test "6.3.2.2 query with graph variable in join", %{ctx: ctx} do
      # Test that we can query across multiple graphs
      # This query finds subjects from both graphs
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g1 ?g2 WHERE {
        GRAPH ex:graph1 { ex:shared ex:p ?o1 }
        GRAPH ex:graph2 { ex:shared ex:p ?o2 }
        BIND(ex:graph1 AS ?g1)
        BIND(ex:graph2 AS ?g2)
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Should find results since 'shared' exists in both graphs
      assert length(results) >= 1
    end

    test "6.3.2.3 query comparing graphs via FILTER", %{ctx: ctx} do
      # Use FILTER to compare results from different graphs
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g1 ?g2 WHERE {
        GRAPH ex:graph1 { ex:shared ex:p ?o1 }
        GRAPH ex:graph2 { ex:shared ex:p ?o2 }
        BIND(ex:graph1 AS ?g1)
        BIND(ex:graph2 AS ?g2)
        FILTER(?o1 != ?o2)
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Should find that the values are different
      assert length(results) >= 1
    end

    test "6.3.2.4 query aggregating across graphs", %{ctx: ctx} do
      # Count triples across all graphs
      query = """
      PREFIX ex: <#{@ex}>
      SELECT (COUNT(?s) AS ?count) WHERE {
        GRAPH ?g {
          ?s ex:p ?o
        }
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Should have at least one result with count
      assert length(results) >= 1

      [result | _] = results
      # The count should be a number (we have at least 6 quads in test data)
      assert {:literal, :typed, count, _} = result["count"]
      assert String.to_integer(count) >= 6
    end

    test "6.3.2.5 subquery across graphs", %{ctx: ctx} do
      # Use a subquery to find graphs with specific patterns
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g ?count WHERE {
        {
          SELECT ?g (COUNT(?s) AS ?count) WHERE {
            GRAPH ?g {
              ?s ex:p ?o
            }
          }
          GROUP BY ?g
        }
        FILTER(?count >= 2)
      }
      """

      assert {:ok, results} = Query.query(ctx, query)

      # graph1 and graph2 each have 3 quads, graph3 has 3 quads
      # Default graph has 2 quads (which won't be counted with GRAPH ?g)
      assert length(results) >= 3
    end
  end

  # ===========================================================================
  # 6.3.3: Result Serialization
  # ===========================================================================

  describe "6.3.3 Result Serialization" do
    test "6.3.3.1 SELECT returns graph variable binding", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g ?s WHERE {
        GRAPH ?g {
          ?s ex:p ?o
        }
      }
      LIMIT 1
      """

      assert {:ok, results} = Query.query(ctx, query)

      assert length(results) >= 1

      [result | _] = results
      # Should have graph variable in result
      assert Map.has_key?(result, "g")
      assert Map.has_key?(result, "s")
    end

    test "6.3.3.2 SELECT star includes graph", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT * WHERE {
        GRAPH ex:graph1 {
          ?s ex:p ?o
        }
      }
      LIMIT 1
      """

      assert {:ok, results} = Query.query(ctx, query)

      assert length(results) >= 1

      [result | _] = results
      # Should have all variables including s
      assert Map.has_key?(result, "s")
    end

    test "6.3.3.3 CONSTRUCT returns triples with graph", %{ctx: ctx} do
      # Note: CONSTRUCT returns an RDF.Graph or RDF.Dataset
      query = """
      PREFIX ex: <#{@ex}>
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

      CONSTRUCT {
        ?s a ex:QueriedResource .
      }
      WHERE {
        GRAPH ex:graph1 {
          ?s ex:p ?o
        }
      }
      LIMIT 1
      """

      assert {:ok, results} = Query.query(ctx, query)

      # CONSTRUCT returns an RDF.Graph struct (or Dataset)
      # Check that we got some kind of result
      assert results != nil
    end

    test "6.3.3.4 ASK with graph context", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      ASK {
        GRAPH ex:graph1 {
          ex:shared ex:p ?o
        }
      }
      """

      assert {:ok, result} = Query.query(ctx, query)

      # ASK returns a boolean directly
      assert is_boolean(result)
      assert result == true
    end

    test "6.3.3.5 ORDER BY with graph variable", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g WHERE {
        GRAPH ?g {
          ?s ex:p ?o
        }
      }
      ORDER BY ?g
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Results should be ordered by graph name
      assert length(results) >= 2

      graph_names = Enum.map(results, fn r -> r["g"] end)
      # First result should have a lexicographically smaller IRI than last
      first_graph = graph_names |> List.first()
      last_graph = graph_names |> List.last()

      assert first_graph <= last_graph
    end

    test "6.3.3.6 GROUP BY with graph variable", %{ctx: ctx} do
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?g (COUNT(?s) AS ?count) WHERE {
        GRAPH ?g {
          ?s ex:p ?o
        }
      }
      GROUP BY ?g
      ORDER BY ?g
      """

      assert {:ok, results} = Query.query(ctx, query)

      # Should have grouped results by graph
      assert length(results) >= 3

      # Each result should have graph and count
      Enum.each(results, fn result ->
        assert Map.has_key?(result, "g")
        assert Map.has_key?(result, "count")
      end)
    end
  end
end
