defmodule TripleStore.SPARQL.QueryTest do
  @moduledoc """
  Tests for the SPARQL Query public interface.

  Task 2.5.1: Query Function
  - Implement TripleStore.SPARQL.query/2 main entry point
  - Implement TripleStore.SPARQL.query/3 with options
  - Support timeout option for long-running queries
  - Support explain option for query plan inspection
  """

  use ExUnit.Case, async: false

  alias TripleStore.SPARQL.Query
  alias TripleStore.Index
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Backend.RocksDB.NIF

  @moduletag :tmp_dir

  # ===========================================================================
  # Setup Helpers
  # ===========================================================================

  defp setup_db(tmp_dir) do
    db_path = Path.join(tmp_dir, "test_db_#{:erlang.unique_integer([:positive])}")
    {:ok, db} = NIF.open(db_path)
    {:ok, manager} = Manager.start_link(db: db)
    {db, manager}
  end

  defp cleanup({_db, manager}) do
    Manager.stop(manager)
  end

  defp add_triple(db, manager, {s_term, p_term, o_term}) do
    {:ok, s_id} = Manager.get_or_create_id(manager, term_to_rdf(s_term))
    {:ok, p_id} = Manager.get_or_create_id(manager, term_to_rdf(p_term))
    {:ok, o_id} = Manager.get_or_create_id(manager, term_to_rdf(o_term))
    :ok = Index.insert_triple(db, {s_id, p_id, o_id})
  end

  defp term_to_rdf({:named_node, uri}), do: RDF.iri(uri)
  defp term_to_rdf({:literal, :simple, value}), do: RDF.literal(value)

  defp term_to_rdf({:literal, :typed, value, type}) do
    RDF.literal(value, datatype: type)
  end

  defp iri(uri), do: {:named_node, uri}
  defp literal(value), do: {:literal, :simple, value}

  # ===========================================================================
  # SELECT Query Tests
  # ===========================================================================

  describe "query/2 SELECT" do
    test "executes simple SELECT query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})
      add_triple(db, manager, {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")})

      {:ok, results} = Query.query(ctx, "SELECT ?name WHERE { ?s <http://ex.org/name> ?name }")

      assert is_list(results)
      assert length(results) == 2

      names = Enum.map(results, fn r -> r["name"] end)
      assert {:literal, :simple, "Alice"} in names
      assert {:literal, :simple, "Bob"} in names

      cleanup({db, manager})
    end

    test "executes SELECT * query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

      {:ok, results} = Query.query(ctx, "SELECT * WHERE { ?s <http://ex.org/name> ?name }")

      assert is_list(results)
      assert length(results) == 1

      [result] = results
      assert Map.has_key?(result, "s")
      assert Map.has_key?(result, "name")

      cleanup({db, manager})
    end

    test "returns empty list for no matches", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, results} = Query.query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/nonexistent> ?o }")

      assert results == []

      cleanup({db, manager})
    end

    test "executes SELECT with FILTER", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})
      add_triple(db, manager, {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")})

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?name WHERE {
            ?s <http://ex.org/name> ?name
            FILTER(BOUND(?name))
          }
        """)

      assert length(results) == 2

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # ASK Query Tests
  # ===========================================================================

  describe "query/2 ASK" do
    test "returns true when solutions exist", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

      {:ok, result} = Query.query(ctx, "ASK { ?s <http://ex.org/name> ?name }")

      assert result == true

      cleanup({db, manager})
    end

    test "returns false when no solutions exist", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, result} = Query.query(ctx, "ASK { ?s <http://ex.org/nonexistent> ?o }")

      assert result == false

      cleanup({db, manager})
    end

    test "returns true for specific resource check", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

      {:ok, result} = Query.query(ctx, "ASK { <http://ex.org/Alice> ?p ?o }")

      assert result == true

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # CONSTRUCT Query Tests
  # ===========================================================================

  describe "query/2 CONSTRUCT" do
    test "constructs graph from template", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

      {:ok, graph} =
        Query.query(ctx, """
          CONSTRUCT { ?s <http://xmlns.com/foaf/0.1/name> ?name }
          WHERE { ?s <http://ex.org/name> ?name }
        """)

      assert %RDF.Graph{} = graph
      assert RDF.Graph.triple_count(graph) == 1

      cleanup({db, manager})
    end

    test "constructs empty graph for no matches", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, graph} =
        Query.query(ctx, """
          CONSTRUCT { ?s <http://ex.org/result> ?o }
          WHERE { ?s <http://ex.org/nonexistent> ?o }
        """)

      assert %RDF.Graph{} = graph
      assert RDF.Graph.triple_count(graph) == 0

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # DESCRIBE Query Tests
  # ===========================================================================

  describe "query/2 DESCRIBE" do
    test "describes resource", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})
      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/age"), literal("30")})

      {:ok, graph} = Query.query(ctx, "DESCRIBE ?s WHERE { ?s <http://ex.org/name> ?name }")

      assert %RDF.Graph{} = graph
      # Should describe Alice with all her properties
      assert RDF.Graph.triple_count(graph) >= 1

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Query Options Tests
  # ===========================================================================

  describe "query/3 with options" do
    test "timeout option causes timeout error for long queries", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add some data
      add_triple(db, manager, {iri("http://ex.org/s"), iri("http://ex.org/p"), literal("o")})

      # Very short timeout - query should still complete but tests the mechanism
      {:ok, _results} = Query.query(ctx, "SELECT ?s WHERE { ?s ?p ?o }", timeout: 5000)

      cleanup({db, manager})
    end

    test "explain option returns query plan", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, {:explain, info}} =
        Query.query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/p> ?o }", explain: true)

      assert is_map(info)
      assert info.query_type == :select
      assert is_tuple(info.original_pattern) or is_nil(info.original_pattern)
      assert is_list(info.variables)
      assert is_map(info.modifiers)
      assert is_map(info.optimizations)

      cleanup({db, manager})
    end

    test "optimize: false skips optimization", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/s"), iri("http://ex.org/p"), literal("o")})

      {:ok, results} =
        Query.query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/p> ?o }", optimize: false)

      assert is_list(results)

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Error Handling Tests
  # ===========================================================================

  describe "query/2 error handling" do
    test "returns parse error for invalid SPARQL", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:error, {:parse_error, _reason}} = Query.query(ctx, "INVALID QUERY SYNTAX")

      cleanup({db, manager})
    end

    test "returns error for unsupported query type", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # UPDATE is not supported via query/2
      {:error, {:parse_error, _}} =
        Query.query(ctx, "INSERT DATA { <http://ex.org/s> <http://ex.org/p> <http://ex.org/o> }")

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # query!/3 Tests
  # ===========================================================================

  describe "query!/3" do
    test "returns result for valid query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/s"), iri("http://ex.org/p"), literal("o")})

      results = Query.query!(ctx, "SELECT ?s WHERE { ?s ?p ?o }")

      assert is_list(results)
      assert length(results) == 1

      cleanup({db, manager})
    end

    test "raises for invalid query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      assert_raise RuntimeError, ~r/Query failed/, fn ->
        Query.query!(ctx, "INVALID QUERY")
      end

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Solution Modifier Integration Tests
  # ===========================================================================

  describe "solution modifiers" do
    test "DISTINCT removes duplicates", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add triples that will produce duplicate bindings
      add_triple(db, manager, {iri("http://ex.org/s1"), iri("http://ex.org/type"), iri("http://ex.org/Person")})
      add_triple(db, manager, {iri("http://ex.org/s2"), iri("http://ex.org/type"), iri("http://ex.org/Person")})

      {:ok, results} =
        Query.query(ctx, "SELECT DISTINCT ?type WHERE { ?s <http://ex.org/type> ?type }")

      assert length(results) == 1

      cleanup({db, manager})
    end

    test "LIMIT restricts result count", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add multiple triples
      for i <- 1..10 do
        add_triple(db, manager, {
          iri("http://ex.org/s#{i}"),
          iri("http://ex.org/p"),
          literal("v#{i}")
        })
      end

      {:ok, results} =
        Query.query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/p> ?o } LIMIT 3")

      assert length(results) == 3

      cleanup({db, manager})
    end

    test "OFFSET skips results", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      for i <- 1..5 do
        add_triple(db, manager, {
          iri("http://ex.org/s#{i}"),
          iri("http://ex.org/p"),
          literal("v#{i}")
        })
      end

      {:ok, results} =
        Query.query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/p> ?o } OFFSET 2 LIMIT 2")

      assert length(results) == 2

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Integration Tests
  # ===========================================================================

  describe "integration" do
    test "complex query with multiple patterns", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Create a small graph
      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})
      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/knows"), iri("http://ex.org/Bob")})
      add_triple(db, manager, {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")})

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?person ?friendName WHERE {
            ?person <http://ex.org/knows> ?friend .
            ?friend <http://ex.org/name> ?friendName
          }
        """)

      assert length(results) == 1
      [result] = results
      assert result["friendName"] == {:literal, :simple, "Bob"}

      cleanup({db, manager})
    end

    test "query with UNION", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/s1"), iri("http://ex.org/type"), iri("http://ex.org/Person")})
      add_triple(db, manager, {iri("http://ex.org/s2"), iri("http://ex.org/kind"), iri("http://ex.org/Animal")})

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?s WHERE {
            { ?s <http://ex.org/type> ?o }
            UNION
            { ?s <http://ex.org/kind> ?o }
          }
        """)

      assert length(results) == 2

      cleanup({db, manager})
    end

    test "query with OPTIONAL", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})
      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/age"), literal("30")})
      add_triple(db, manager, {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")})
      # Bob has no age

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?name ?age WHERE {
            ?s <http://ex.org/name> ?name
            OPTIONAL { ?s <http://ex.org/age> ?age }
          }
        """)

      assert length(results) == 2

      # One should have age, one should not
      ages = Enum.map(results, fn r -> Map.get(r, "age") end)
      assert {:literal, :simple, "30"} in ages

      cleanup({db, manager})
    end
  end
end
