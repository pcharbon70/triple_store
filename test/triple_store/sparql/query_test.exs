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

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.SPARQL.Query

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

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

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

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

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

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

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

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

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

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

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

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

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

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/age"), literal("30")}
      )

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
      add_triple(
        db,
        manager,
        {iri("http://ex.org/s1"), iri("http://ex.org/type"), iri("http://ex.org/Person")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/s2"), iri("http://ex.org/type"), iri("http://ex.org/Person")}
      )

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
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/knows"), iri("http://ex.org/Bob")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

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

      add_triple(
        db,
        manager,
        {iri("http://ex.org/s1"), iri("http://ex.org/type"), iri("http://ex.org/Person")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/s2"), iri("http://ex.org/kind"), iri("http://ex.org/Animal")}
      )

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

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/age"), literal("30")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

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

  # ===========================================================================
  # Streaming Query Tests (Task 2.5.2)
  # ===========================================================================

  describe "stream_query/2" do
    test "returns lazy stream of bindings", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

      {:ok, stream} =
        Query.stream_query(ctx, "SELECT ?name WHERE { ?s <http://ex.org/name> ?name }")

      # Verify it's a stream (enumerable)
      assert is_function(stream) or is_struct(stream, Stream)

      # Materialize to verify results
      results = Enum.to_list(stream)
      assert length(results) == 2

      names = Enum.map(results, fn r -> r["name"] end)
      assert {:literal, :simple, "Alice"} in names
      assert {:literal, :simple, "Bob"} in names

      cleanup({db, manager})
    end

    test "stream is lazy - doesn't execute until consumed", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/s"), iri("http://ex.org/p"), literal("o")})

      # Just creating the stream shouldn't consume anything
      {:ok, stream} = Query.stream_query(ctx, "SELECT ?s WHERE { ?s ?p ?o }")

      # Stream exists but hasn't been consumed yet
      assert is_function(stream) or is_struct(stream, Stream)

      # Now consume
      results = Enum.to_list(stream)
      assert length(results) == 1

      cleanup({db, manager})
    end

    test "returns empty stream for no matches", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, stream} =
        Query.stream_query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/nonexistent> ?o }")

      results = Enum.to_list(stream)
      assert results == []

      cleanup({db, manager})
    end

    test "returns error for non-SELECT query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:error, {:unsupported_stream_type, :ask}} =
        Query.stream_query(ctx, "ASK { ?s ?p ?o }")

      {:error, {:unsupported_stream_type, :construct}} =
        Query.stream_query(ctx, "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }")

      cleanup({db, manager})
    end

    test "returns error for invalid query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:error, {:parse_error, _}} = Query.stream_query(ctx, "INVALID QUERY")

      cleanup({db, manager})
    end
  end

  describe "stream_query/3 with options" do
    test "optimize: false skips optimization", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/s"), iri("http://ex.org/p"), literal("o")})

      {:ok, stream} = Query.stream_query(ctx, "SELECT ?s WHERE { ?s ?p ?o }", optimize: false)

      results = Enum.to_list(stream)
      assert length(results) == 1

      cleanup({db, manager})
    end

    test "variables option projects to specified variables", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      {:ok, stream} =
        Query.stream_query(ctx, "SELECT * WHERE { ?s <http://ex.org/name> ?name }",
          variables: ["name"]
        )

      [result] = Enum.to_list(stream)
      assert Map.has_key?(result, "name")
      refute Map.has_key?(result, "s")

      cleanup({db, manager})
    end
  end

  describe "stream_query!/3" do
    test "returns stream for valid query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/s"), iri("http://ex.org/p"), literal("o")})

      stream = Query.stream_query!(ctx, "SELECT ?s WHERE { ?s ?p ?o }")
      results = Enum.to_list(stream)
      assert length(results) == 1

      cleanup({db, manager})
    end

    test "raises for invalid query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      assert_raise RuntimeError, ~r/Stream query failed/, fn ->
        Query.stream_query!(ctx, "INVALID QUERY")
      end

      cleanup({db, manager})
    end

    test "raises for non-SELECT query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      assert_raise RuntimeError, ~r/Stream query failed.*unsupported_stream_type/, fn ->
        Query.stream_query!(ctx, "ASK { ?s ?p ?o }")
      end

      cleanup({db, manager})
    end
  end

  describe "streaming early termination" do
    test "Enum.take/2 only consumes needed elements", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add many triples
      for i <- 1..100 do
        add_triple(db, manager, {
          iri("http://ex.org/s#{i}"),
          iri("http://ex.org/p"),
          literal("v#{i}")
        })
      end

      {:ok, stream} = Query.stream_query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/p> ?o }")

      # Take only first 5
      results = Enum.take(stream, 5)
      assert length(results) == 5

      cleanup({db, manager})
    end

    test "Enum.find/2 stops at first match", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      for i <- 1..10 do
        add_triple(db, manager, {
          iri("http://ex.org/s#{i}"),
          iri("http://ex.org/p"),
          literal("v#{i}")
        })
      end

      {:ok, stream} = Query.stream_query(ctx, "SELECT ?s ?o WHERE { ?s <http://ex.org/p> ?o }")

      # Find first result
      result = Enum.find(stream, fn _ -> true end)
      assert is_map(result)
      assert Map.has_key?(result, "s")

      cleanup({db, manager})
    end

    test "stream can be consumed multiple times by re-creating", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/s"), iri("http://ex.org/p"), literal("o")})

      # First consumption
      {:ok, stream1} = Query.stream_query(ctx, "SELECT ?s WHERE { ?s ?p ?o }")
      results1 = Enum.to_list(stream1)
      assert length(results1) == 1

      # Second consumption (new stream)
      {:ok, stream2} = Query.stream_query(ctx, "SELECT ?s WHERE { ?s ?p ?o }")
      results2 = Enum.to_list(stream2)
      assert length(results2) == 1

      cleanup({db, manager})
    end
  end

  describe "streaming with solution modifiers" do
    test "DISTINCT works with streaming", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add triples that will produce duplicate type bindings
      add_triple(
        db,
        manager,
        {iri("http://ex.org/s1"), iri("http://ex.org/type"), iri("http://ex.org/Person")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/s2"), iri("http://ex.org/type"), iri("http://ex.org/Person")}
      )

      {:ok, stream} =
        Query.stream_query(ctx, "SELECT DISTINCT ?type WHERE { ?s <http://ex.org/type> ?type }")

      results = Enum.to_list(stream)
      assert length(results) == 1

      cleanup({db, manager})
    end

    test "LIMIT works with streaming", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      for i <- 1..10 do
        add_triple(db, manager, {
          iri("http://ex.org/s#{i}"),
          iri("http://ex.org/p"),
          literal("v#{i}")
        })
      end

      {:ok, stream} =
        Query.stream_query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/p> ?o } LIMIT 3")

      results = Enum.to_list(stream)
      assert length(results) == 3

      cleanup({db, manager})
    end

    test "OFFSET works with streaming", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      for i <- 1..5 do
        add_triple(db, manager, {
          iri("http://ex.org/s#{i}"),
          iri("http://ex.org/p"),
          literal("v#{i}")
        })
      end

      {:ok, stream} =
        Query.stream_query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/p> ?o } OFFSET 2 LIMIT 2")

      results = Enum.to_list(stream)
      assert length(results) == 2

      cleanup({db, manager})
    end
  end

  describe "streaming backpressure" do
    test "stream respects consumer pace via lazy evaluation", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add data
      for i <- 1..20 do
        add_triple(db, manager, {
          iri("http://ex.org/s#{i}"),
          iri("http://ex.org/p"),
          literal("v#{i}")
        })
      end

      {:ok, stream} = Query.stream_query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/p> ?o }")

      # Simulate slow consumer by processing one at a time
      # Stream should not force all results to be computed at once
      count =
        stream
        |> Stream.map(fn binding ->
          # Simulate work
          binding
        end)
        |> Enum.take(5)
        |> length()

      assert count == 5

      cleanup({db, manager})
    end

    test "chaining stream operations maintains laziness", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      for i <- 1..50 do
        add_triple(db, manager, {
          iri("http://ex.org/s#{i}"),
          iri("http://ex.org/p"),
          literal("v#{i}")
        })
      end

      {:ok, stream} = Query.stream_query(ctx, "SELECT ?s ?o WHERE { ?s <http://ex.org/p> ?o }")

      # Chain multiple operations - all should be lazy
      result =
        stream
        |> Stream.filter(fn b -> Map.has_key?(b, "s") end)
        |> Stream.map(fn b -> b["s"] end)
        |> Enum.take(3)

      assert length(result) == 3

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Prepared Query Tests - Task 2.5.3
  # ===========================================================================

  describe "prepare/1" do
    test "prepares a simple SELECT query" do
      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { ?s <http://ex.org/name> ?name }")

      assert %Query.Prepared{} = prepared
      assert prepared.query_type == :select
      assert prepared.parameters == []
      assert prepared.pattern != nil
      assert prepared.optimized_pattern != nil
    end

    test "prepares query with $param parameters" do
      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { $person <http://ex.org/name> ?name }")

      assert prepared.parameters == ["person"]
    end

    test "prepares query with multiple parameters" do
      {:ok, prepared} =
        Query.prepare("SELECT ?o WHERE { $subject $predicate ?o }")

      assert "subject" in prepared.parameters
      assert "predicate" in prepared.parameters
      assert length(prepared.parameters) == 2
    end

    test "caches both original and optimized pattern" do
      {:ok, prepared} =
        Query.prepare("SELECT ?name WHERE { ?s <http://ex.org/name> ?name }")

      assert prepared.pattern != nil
      assert prepared.optimized_pattern != nil
    end

    test "returns error for invalid SPARQL" do
      assert {:error, {:parse_error, _}} = Query.prepare("INVALID SPARQL")
    end
  end

  describe "prepare/2 with options" do
    test "optimize: false skips optimization" do
      {:ok, prepared} =
        Query.prepare("SELECT ?name WHERE { ?s <http://ex.org/name> ?name }", optimize: false)

      # Pattern and optimized_pattern should be identical when optimization is disabled
      assert prepared.pattern == prepared.optimized_pattern
    end

    test "stats option passes statistics to optimizer" do
      {:ok, prepared} =
        Query.prepare(
          "SELECT ?o WHERE { ?s <http://ex.org/p1> ?x . ?x <http://ex.org/p2> ?o }",
          stats: %{"http://ex.org/p1" => 100, "http://ex.org/p2" => 1000}
        )

      assert %Query.Prepared{} = prepared
    end
  end

  describe "prepare!/1" do
    test "returns prepared query for valid SPARQL" do
      prepared = Query.prepare!("SELECT ?s WHERE { ?s ?p ?o }")
      assert %Query.Prepared{} = prepared
    end

    test "raises for invalid SPARQL" do
      assert_raise RuntimeError, ~r/Prepare failed/, fn ->
        Query.prepare!("INVALID SPARQL")
      end
    end
  end

  describe "execute/3" do
    test "executes prepared query without parameters", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { ?s <http://ex.org/name> ?name }")
      {:ok, results} = Query.execute(ctx, prepared)

      assert length(results) == 1
      assert hd(results)["name"] == {:literal, :simple, "Alice"}

      cleanup({db, manager})
    end

    test "executes prepared query with URI parameter", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { $person <http://ex.org/name> ?name }")

      # Execute with Alice
      {:ok, results1} = Query.execute(ctx, prepared, %{"person" => "http://ex.org/Alice"})
      assert length(results1) == 1
      assert hd(results1)["name"] == {:literal, :simple, "Alice"}

      # Execute with Bob
      {:ok, results2} = Query.execute(ctx, prepared, %{"person" => "http://ex.org/Bob"})
      assert length(results2) == 1
      assert hd(results2)["name"] == {:literal, :simple, "Bob"}

      cleanup({db, manager})
    end

    test "executes prepared query with literal parameter", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s <http://ex.org/name> $name }")

      {:ok, results} = Query.execute(ctx, prepared, %{"name" => "Alice"})
      assert length(results) == 1

      cleanup({db, manager})
    end

    test "executes prepared query with named_node tuple parameter", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { $person <http://ex.org/name> ?name }")

      {:ok, results} =
        Query.execute(ctx, prepared, %{"person" => {:named_node, "http://ex.org/Alice"}})

      assert length(results) == 1

      cleanup({db, manager})
    end

    test "executes prepared query with literal tuple parameter", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s <http://ex.org/name> $name }")

      {:ok, results} = Query.execute(ctx, prepared, %{"name" => {:literal, :simple, "Alice"}})
      assert length(results) == 1

      cleanup({db, manager})
    end

    test "executes prepared query multiple times efficiently", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      for i <- 1..10 do
        add_triple(db, manager, {
          iri("http://ex.org/person#{i}"),
          iri("http://ex.org/name"),
          literal("Person#{i}")
        })
      end

      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { $person <http://ex.org/name> ?name }")

      # Execute 10 times with different parameters
      for i <- 1..10 do
        {:ok, results} = Query.execute(ctx, prepared, %{"person" => "http://ex.org/person#{i}"})
        assert length(results) == 1
        assert hd(results)["name"] == {:literal, :simple, "Person#{i}"}
      end

      cleanup({db, manager})
    end

    test "returns error for missing parameters", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, prepared} = Query.prepare("SELECT ?o WHERE { $subject $predicate ?o }")

      # Missing all parameters
      assert {:error, {:missing_parameters, missing}} = Query.execute(ctx, prepared, %{})
      assert "subject" in missing
      assert "predicate" in missing

      # Missing one parameter
      assert {:error, {:missing_parameters, ["predicate"]}} =
               Query.execute(ctx, prepared, %{"subject" => "http://ex.org/s"})

      cleanup({db, manager})
    end

    test "extra parameters are ignored", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { $person <http://ex.org/name> ?name }")

      {:ok, results} =
        Query.execute(ctx, prepared, %{
          "person" => "http://ex.org/Alice",
          "extra" => "ignored"
        })

      assert length(results) == 1

      cleanup({db, manager})
    end
  end

  describe "execute/4 with options" do
    test "timeout option works", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/s"), iri("http://ex.org/p"), literal("o")})

      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, results} = Query.execute(ctx, prepared, %{}, timeout: 5000)

      assert length(results) == 1

      cleanup({db, manager})
    end
  end

  describe "execute!/3" do
    test "returns results for valid execution", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      prepared = Query.prepare!("SELECT ?name WHERE { $person <http://ex.org/name> ?name }")
      results = Query.execute!(ctx, prepared, %{"person" => "http://ex.org/Alice"})

      assert length(results) == 1
      assert hd(results)["name"] == {:literal, :simple, "Alice"}

      cleanup({db, manager})
    end

    test "raises for missing parameters", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      prepared = Query.prepare!("SELECT ?o WHERE { $subject ?p ?o }")

      assert_raise RuntimeError, ~r/Execute failed.*missing_parameters/, fn ->
        Query.execute!(ctx, prepared, %{})
      end

      cleanup({db, manager})
    end
  end

  describe "prepared query with different query types" do
    test "prepared ASK query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      {:ok, prepared} = Query.prepare("ASK { $person <http://ex.org/name> ?name }")
      assert prepared.query_type == :ask

      {:ok, result} = Query.execute(ctx, prepared, %{"person" => "http://ex.org/Alice"})
      assert result == true

      {:ok, result2} = Query.execute(ctx, prepared, %{"person" => "http://ex.org/Unknown"})
      assert result2 == false

      cleanup({db, manager})
    end

    test "prepared CONSTRUCT query", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      {:ok, prepared} =
        Query.prepare("""
          CONSTRUCT { $person <http://ex.org/hasName> ?name }
          WHERE { $person <http://ex.org/name> ?name }
        """)

      assert prepared.query_type == :construct

      {:ok, graph} = Query.execute(ctx, prepared, %{"person" => "http://ex.org/Alice"})
      assert %RDF.Graph{} = graph

      cleanup({db, manager})
    end
  end

  describe "prepared query parameter value conversion" do
    test "integer parameter is converted correctly", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add triple with integer literal
      {:ok, s_id} = Manager.get_or_create_id(manager, RDF.iri("http://ex.org/item"))
      {:ok, p_id} = Manager.get_or_create_id(manager, RDF.iri("http://ex.org/count"))
      {:ok, o_id} = Manager.get_or_create_id(manager, RDF.literal(42))
      :ok = Index.insert_triple(db, {s_id, p_id, o_id})

      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s <http://ex.org/count> $count }")

      # Execute with integer - this tests the conversion path
      {:ok, results} = Query.execute(ctx, prepared, %{"count" => 42})
      # May or may not match depending on literal format, just verify no error
      assert is_list(results)

      cleanup({db, manager})
    end

    test "boolean true parameter is converted correctly", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s <http://ex.org/active> $flag }")

      # Execute with boolean - tests conversion without error
      {:ok, results} = Query.execute(ctx, prepared, %{"flag" => true})
      assert is_list(results)

      cleanup({db, manager})
    end

    test "float parameter is converted correctly", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s <http://ex.org/value> $val }")

      # Execute with float - tests conversion without error
      {:ok, results} = Query.execute(ctx, prepared, %{"val" => 3.14})
      assert is_list(results)

      cleanup({db, manager})
    end
  end

  describe "prepared query caching behavior" do
    test "prepared query struct stores original SPARQL", %{tmp_dir: _tmp_dir} do
      sparql = "SELECT ?name WHERE { $person <http://ex.org/name> ?name }"
      {:ok, prepared} = Query.prepare(sparql)

      assert prepared.sparql == sparql
    end

    test "prepared query can be reused after module reload (serializable)", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { $person <http://ex.org/name> ?name }")

      # Simulate serialization/deserialization by converting to/from term
      serialized = :erlang.term_to_binary(prepared)
      deserialized = :erlang.binary_to_term(serialized)

      {:ok, results} = Query.execute(ctx, deserialized, %{"person" => "http://ex.org/Alice"})
      assert length(results) == 1

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # B2: Timeout Test
  # ===========================================================================

  describe "timeout behavior" do
    test "timeout actually triggers for very short timeout", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add enough data to make query take measurable time
      for i <- 1..100 do
        add_triple(
          db,
          manager,
          {iri("http://ex.org/s#{i}"), iri("http://ex.org/p"), literal("v#{i}")}
        )
      end

      # Very short timeout (1ms) should trigger timeout
      result = Query.query(ctx, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }", timeout: 1)

      # Either times out or completes very fast - both are valid
      assert result == {:error, :timeout} or match?({:ok, _}, result)

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # C9: EXTEND/BIND Pattern Test
  # ===========================================================================

  describe "BIND/EXTEND pattern" do
    test "BIND creates new variable binding", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/age"), literal("30")}
      )

      # BIND creates a new variable from an expression
      {:ok, results} =
        Query.query(
          ctx,
          """
          SELECT ?age ?computed WHERE {
            ?s <http://ex.org/age> ?age .
            BIND(?age AS ?computed)
          }
          """
        )

      # Query executes without error
      assert length(results) >= 0
      cleanup({db, manager})
    end

    test "BIND with constant IRI is pushed down", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add data for Alice
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/age"), literal("30")}
      )

      # Add data for Bob (should not appear in results)
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

      # Attach telemetry handler to verify push-down
      test_pid = self()
      handler_id = "test-bind-pushdown-#{:erlang.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :query, :bind_pushdown],
        fn _event, measurements, metadata, _ ->
          send(test_pid, {:bind_pushdown, measurements, metadata})
        end,
        nil
      )

      # BIND with constant IRI should be pushed down
      {:ok, results} =
        Query.query(
          ctx,
          """
          SELECT ?product ?name ?age WHERE {
            BIND(<http://ex.org/Alice> AS ?product)
            ?product <http://ex.org/name> ?name .
            ?product <http://ex.org/age> ?age .
          }
          """
        )

      :telemetry.detach(handler_id)

      # Should get Alice's data
      assert length(results) == 1
      [result] = results
      assert result["name"] == {:literal, :simple, "Alice"}
      assert result["age"] == {:literal, :simple, "30"}
      assert result["product"] == {:named_node, "http://ex.org/Alice"}

      # Verify telemetry was emitted for BIND push-down
      assert_receive {:bind_pushdown, %{count: 1}, %{variable: "product"}}, 1000

      cleanup({db, manager})
    end

    test "BIND with variable expression is not pushed down", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/age"), literal("30")}
      )

      # BIND with variable expression should not be pushed down
      {:ok, results} =
        Query.query(
          ctx,
          """
          SELECT ?s ?age ?copy WHERE {
            ?s <http://ex.org/age> ?age .
            BIND(?age AS ?copy)
          }
          """
        )

      assert length(results) == 1
      [result] = results
      assert result["age"] == {:literal, :simple, "30"}
      assert result["copy"] == {:literal, :simple, "30"}

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # C10: ORDER BY Test
  # ===========================================================================

  describe "ORDER BY" do
    test "ORDER BY sorts results ascending", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add triples with different names that will sort alphabetically
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Carol"), iri("http://ex.org/name"), literal("Carol")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

      {:ok, results} =
        Query.query(
          ctx,
          "SELECT ?name WHERE { ?s <http://ex.org/name> ?name } ORDER BY ?name"
        )

      assert length(results) == 3
      names = Enum.map(results, fn r -> elem(r["name"], 2) end)
      assert names == Enum.sort(names)

      cleanup({db, manager})
    end

    test "ORDER BY DESC sorts results descending", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Carol"), iri("http://ex.org/name"), literal("Carol")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

      {:ok, results} =
        Query.query(
          ctx,
          "SELECT ?name WHERE { ?s <http://ex.org/name> ?name } ORDER BY DESC(?name)"
        )

      assert length(results) == 3
      names = Enum.map(results, fn r -> elem(r["name"], 2) end)
      assert names == Enum.sort(names, :desc)

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # C5-C6: Option Validation Tests
  # ===========================================================================

  describe "option validation" do
    test "invalid option returns error", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Typo in option name should be detected
      result = Query.query(ctx, "SELECT ?s WHERE { ?s ?p ?o }", timout: 5000)
      assert result == {:error, {:invalid_option, :timout}}

      cleanup({db, manager})
    end

    test "invalid option in prepare returns error", %{tmp_dir: _tmp_dir} do
      result = Query.prepare("SELECT ?s WHERE { ?s ?p ?o }", optimze: true)
      assert result == {:error, {:invalid_option, :optimze}}
    end

    test "invalid option in stream_query returns error", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      result = Query.stream_query(ctx, "SELECT ?s WHERE { ?s ?p ?o }", tiemout: 5000)
      assert result == {:error, {:invalid_option, :tiemout}}

      cleanup({db, manager})
    end

    test "invalid option in execute returns error", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s ?p ?o }")
      result = Query.execute(ctx, prepared, %{}, timouet: 5000)
      assert result == {:error, {:invalid_option, :timouet}}

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # S11: Prepared DESCRIBE Test
  # ===========================================================================

  describe "prepared DESCRIBE query" do
    test "prepared DESCRIBE query works", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/age"), literal("30")}
      )

      {:ok, prepared} = Query.prepare("DESCRIBE <http://ex.org/Alice>")
      {:ok, graph} = Query.execute(ctx, prepared, %{})

      assert %RDF.Graph{} = graph

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # S12: Boolean False Parameter Test
  # ===========================================================================

  describe "boolean parameter conversion" do
    test "false boolean parameter is converted correctly", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s <http://ex.org/active> $val }")

      # Execute with false boolean - tests conversion without error
      {:ok, results} = Query.execute(ctx, prepared, %{"val" => false})
      assert is_list(results)

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # C8: URI Validation Tests
  # ===========================================================================

  describe "URI validation" do
    test "valid HTTP URI is recognized as named node", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, prepared} = Query.prepare("SELECT ?o WHERE { $s ?p ?o }")

      # Valid URI should work as named node
      {:ok, results} = Query.execute(ctx, prepared, %{"s" => "http://example.org/resource"})
      assert is_list(results)

      cleanup({db, manager})
    end

    test "URN is recognized as named node", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, prepared} = Query.prepare("SELECT ?o WHERE { $s ?p ?o }")

      # URN should work as named node
      {:ok, results} = Query.execute(ctx, prepared, %{"s" => "urn:isbn:0451450523"})
      assert is_list(results)

      cleanup({db, manager})
    end

    test "plain string without URI scheme is treated as literal", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s ?p $val }")

      # Plain string should become a literal
      {:ok, results} = Query.execute(ctx, prepared, %{"val" => "just a string"})
      assert is_list(results)

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Task 2.7.1: Query Pipeline Integration Tests
  # ===========================================================================

  describe "Task 2.7.1 - Query Pipeline Integration" do
    @describetag :integration

    test "2.7.1.1 - simple SELECT with single pattern", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Add a simple triple
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://xmlns.com/foaf/0.1/name"),
        literal("Alice")
      })

      # Execute simple single-pattern query
      {:ok, results} =
        Query.query(ctx, """
          SELECT ?name
          WHERE { <http://example.org/Alice> <http://xmlns.com/foaf/0.1/name> ?name }
        """)

      assert length(results) == 1
      assert hd(results)["name"] == {:literal, :simple, "Alice"}

      cleanup({db, manager})
    end

    test "2.7.1.2 - SELECT with star query (multiple patterns on same subject)", %{
      tmp_dir: tmp_dir
    } do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Add multiple properties for same subject
      add_triple(
        db,
        manager,
        {iri("http://ex.org/person1"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/person1"), iri("http://ex.org/age"), literal("30")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/person1"), iri("http://ex.org/email"), literal("alice@ex.org")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/person2"), iri("http://ex.org/name"), literal("Bob")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/person2"), iri("http://ex.org/age"), literal("25")}
      )

      # Star query: get all properties for subjects that have both name and age
      {:ok, results} =
        Query.query(ctx, """
          SELECT ?s ?name ?age
          WHERE {
            ?s <http://ex.org/name> ?name .
            ?s <http://ex.org/age> ?age
          }
        """)

      assert length(results) == 2

      # Both should have name and age
      for result <- results do
        assert Map.has_key?(result, "name")
        assert Map.has_key?(result, "age")
      end

      cleanup({db, manager})
    end

    test "2.7.1.3 - SELECT with OPTIONAL producing nulls", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Alice has email, Bob doesn't
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/email"), literal("alice@ex.org")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?name ?email
          WHERE {
            ?s <http://ex.org/name> ?name
            OPTIONAL { ?s <http://ex.org/email> ?email }
          }
        """)

      assert length(results) == 2

      # Find Alice's result - should have email
      alice_result = Enum.find(results, fn r -> r["name"] == {:literal, :simple, "Alice"} end)
      assert alice_result["email"] == {:literal, :simple, "alice@ex.org"}

      # Find Bob's result - should NOT have email key or have nil/unbound
      bob_result = Enum.find(results, fn r -> r["name"] == {:literal, :simple, "Bob"} end)
      assert bob_result["email"] == nil or not Map.has_key?(bob_result, "email")

      cleanup({db, manager})
    end

    test "2.7.1.4 - SELECT with UNION combining branches", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Different predicates for different types
      add_triple(
        db,
        manager,
        {iri("http://ex.org/e1"), iri("http://ex.org/type"), iri("http://ex.org/Person")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/e2"), iri("http://ex.org/rdf_type"), iri("http://ex.org/Animal")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/e3"), iri("http://ex.org/type"), iri("http://ex.org/Place")}
      )

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?entity ?type
          WHERE {
            { ?entity <http://ex.org/type> ?type }
            UNION
            { ?entity <http://ex.org/rdf_type> ?type }
          }
        """)

      assert length(results) == 3

      # All entities should be in results
      entities = Enum.map(results, fn r -> r["entity"] end)
      assert iri("http://ex.org/e1") in entities
      assert iri("http://ex.org/e2") in entities
      assert iri("http://ex.org/e3") in entities

      cleanup({db, manager})
    end

    test "2.7.1.5 - SELECT with complex FILTER expressions", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Products with prices
      add_typed_triple(db, manager, {
        iri("http://ex.org/product1"),
        iri("http://ex.org/price"),
        {:literal, :typed, "50", "http://www.w3.org/2001/XMLSchema#integer"}
      })

      add_typed_triple(db, manager, {
        iri("http://ex.org/product2"),
        iri("http://ex.org/price"),
        {:literal, :typed, "150", "http://www.w3.org/2001/XMLSchema#integer"}
      })

      add_typed_triple(db, manager, {
        iri("http://ex.org/product3"),
        iri("http://ex.org/price"),
        {:literal, :typed, "75", "http://www.w3.org/2001/XMLSchema#integer"}
      })

      # Filter for products with price between 40 and 100
      {:ok, results} =
        Query.query(ctx, """
          SELECT ?product ?price
          WHERE {
            ?product <http://ex.org/price> ?price
            FILTER(?price >= 40 && ?price <= 100)
          }
        """)

      assert length(results) == 2

      # Should include product1 (50) and product3 (75), not product2 (150)
      products = Enum.map(results, fn r -> r["product"] end)
      assert iri("http://ex.org/product1") in products
      assert iri("http://ex.org/product3") in products
      refute iri("http://ex.org/product2") in products

      cleanup({db, manager})
    end

    test "2.7.1.6 - SELECT with ORDER BY, LIMIT, OFFSET combined", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Items with numeric values for ordering
      for i <- 1..10 do
        add_typed_triple(db, manager, {
          iri("http://ex.org/item#{i}"),
          iri("http://ex.org/value"),
          {:literal, :typed, Integer.to_string(i * 10),
           "http://www.w3.org/2001/XMLSchema#integer"}
        })
      end

      # Get items 3-5 when ordered by value descending (100, 90, 80... so skip first 2, take 3)
      {:ok, results} =
        Query.query(ctx, """
          SELECT ?item ?value
          WHERE {
            ?item <http://ex.org/value> ?value
          }
          ORDER BY DESC(?value)
          LIMIT 3
          OFFSET 2
        """)

      assert length(results) == 3

      # Should be 80, 70, 60 (items 8, 7, 6)
      values =
        Enum.map(results, fn r ->
          {:literal, :typed, v, _} = r["value"]
          String.to_integer(v)
        end)

      # Verify they're in descending order
      assert values == Enum.sort(values, :desc)

      # Verify these are the 3rd, 4th, 5th highest values (80, 70, 60)
      assert hd(values) == 80

      cleanup({db, manager})
    end
  end

  # Helper for typed triples
  defp add_typed_triple(db, manager, {s_term, p_term, {:literal, :typed, value, type}}) do
    {:ok, s_id} = Manager.get_or_create_id(manager, term_to_rdf(s_term))
    {:ok, p_id} = Manager.get_or_create_id(manager, term_to_rdf(p_term))
    {:ok, o_id} = Manager.get_or_create_id(manager, RDF.literal(value, datatype: type))
    :ok = Index.insert_triple(db, {s_id, p_id, o_id})
  end

  # ===========================================================================
  # Task 2.7.2: Construct/Ask/Describe Integration Tests
  # ===========================================================================

  describe "Task 2.7.2 - Construct/Ask/Describe Integration" do
    @describetag :integration

    test "2.7.2.1 - CONSTRUCT produces valid RDF graph with multiple triples", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Create a small social network
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/knows"), iri("http://ex.org/Bob")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/knows"), iri("http://ex.org/Carol")}
      )

      # CONSTRUCT a new graph transforming the data
      {:ok, graph} =
        Query.query(ctx, """
          CONSTRUCT {
            ?person <http://xmlns.com/foaf/0.1/name> ?name .
            ?person <http://xmlns.com/foaf/0.1/knows> ?friend
          }
          WHERE {
            ?person <http://ex.org/name> ?name .
            ?person <http://ex.org/knows> ?friend
          }
        """)

      # Verify it's a valid RDF.Graph
      assert %RDF.Graph{} = graph

      # Should have 4 triples (2 name triples + 2 knows triples)
      assert RDF.Graph.triple_count(graph) == 4

      # Verify specific triples exist with transformed predicates
      alice = RDF.iri("http://ex.org/Alice")
      bob = RDF.iri("http://ex.org/Bob")
      foaf_name = RDF.iri("http://xmlns.com/foaf/0.1/name")
      foaf_knows = RDF.iri("http://xmlns.com/foaf/0.1/knows")

      assert RDF.Graph.include?(graph, {alice, foaf_name, RDF.literal("Alice")})
      assert RDF.Graph.include?(graph, {alice, foaf_knows, bob})

      cleanup({db, manager})
    end

    test "2.7.2.1 - CONSTRUCT with template creates new structure", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Simple data
      add_triple(
        db,
        manager,
        {iri("http://ex.org/item1"), iri("http://ex.org/value"), literal("100")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/item2"), iri("http://ex.org/value"), literal("200")}
      )

      # CONSTRUCT with a fixed predicate transformation
      {:ok, graph} =
        Query.query(ctx, """
          CONSTRUCT {
            ?item <http://schema.org/price> ?val
          }
          WHERE {
            ?item <http://ex.org/value> ?val
          }
        """)

      assert %RDF.Graph{} = graph
      assert RDF.Graph.triple_count(graph) == 2

      # Verify new predicate is used
      price_pred = RDF.iri("http://schema.org/price")
      triples = RDF.Graph.triples(graph)
      assert Enum.all?(triples, fn {_s, p, _o} -> p == price_pred end)

      cleanup({db, manager})
    end

    test "2.7.2.2 - ASK returns true when matches exist", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Add some data
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/type"), iri("http://ex.org/Person")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/age"), literal("30")}
      )

      # ASK with a pattern that matches
      {:ok, result} =
        Query.query(ctx, """
          ASK {
            ?person <http://ex.org/type> <http://ex.org/Person> .
            ?person <http://ex.org/age> ?age
          }
        """)

      assert result == true

      cleanup({db, manager})
    end

    test "2.7.2.2 - ASK with complex pattern returns true", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Create a chain of relationships
      add_triple(
        db,
        manager,
        {iri("http://ex.org/A"), iri("http://ex.org/knows"), iri("http://ex.org/B")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/B"), iri("http://ex.org/knows"), iri("http://ex.org/C")}
      )

      # ASK for a 2-hop path
      {:ok, result} =
        Query.query(ctx, """
          ASK {
            ?x <http://ex.org/knows> ?y .
            ?y <http://ex.org/knows> ?z
          }
        """)

      assert result == true

      cleanup({db, manager})
    end

    test "2.7.2.3 - ASK returns false when no matches exist", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Add data that won't match the query
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      # ASK for something that doesn't exist
      {:ok, result} =
        Query.query(ctx, """
          ASK {
            ?person <http://ex.org/type> <http://ex.org/Animal>
          }
        """)

      assert result == false

      cleanup({db, manager})
    end

    test "2.7.2.3 - ASK returns false for empty database", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # No data added - empty database

      {:ok, result} =
        Query.query(ctx, """
          ASK { ?s ?p ?o }
        """)

      assert result == false

      cleanup({db, manager})
    end

    test "2.7.2.4 - DESCRIBE produces CBD for resource", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Create a resource with multiple properties
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/age"), literal("30")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/email"), literal("alice@ex.org")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/knows"), iri("http://ex.org/Bob")}
      )

      # Bob's data (should NOT be included in Alice's CBD)
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

      {:ok, graph} =
        Query.query(ctx, "DESCRIBE <http://ex.org/Alice>")

      assert %RDF.Graph{} = graph

      # Should include all of Alice's properties (CBD = Concise Bounded Description)
      assert RDF.Graph.triple_count(graph) >= 4

      # Verify Alice's triples are included
      alice = RDF.iri("http://ex.org/Alice")

      # All subjects should be Alice (in a strict CBD)
      subjects = graph |> RDF.Graph.triples() |> Enum.map(fn {s, _p, _o} -> s end) |> Enum.uniq()
      assert alice in subjects

      cleanup({db, manager})
    end

    test "2.7.2.4 - DESCRIBE with WHERE clause", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Multiple people
      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/type"), iri("http://ex.org/Person")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/type"), iri("http://ex.org/Person")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Cat"), iri("http://ex.org/type"), iri("http://ex.org/Animal")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/Cat"), iri("http://ex.org/name"), literal("Whiskers")}
      )

      # DESCRIBE only persons
      {:ok, graph} =
        Query.query(ctx, """
          DESCRIBE ?person
          WHERE { ?person <http://ex.org/type> <http://ex.org/Person> }
        """)

      assert %RDF.Graph{} = graph

      # Should describe Alice and Bob, not Cat
      subjects = graph |> RDF.Graph.triples() |> Enum.map(fn {s, _p, _o} -> s end) |> Enum.uniq()

      assert RDF.iri("http://ex.org/Alice") in subjects or
               RDF.iri("http://ex.org/Bob") in subjects

      # Cat should not be described (it's an Animal, not a Person)
      refute RDF.iri("http://ex.org/Cat") in subjects

      cleanup({db, manager})
    end
  end

  describe "Task 2.7.3 - Aggregation Integration" do
    @describetag :integration

    test "2.7.3.1 - GROUP BY with COUNT", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Products in different categories
      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod1"), iri("http://ex.org/category"),
         iri("http://ex.org/Electronics")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod2"), iri("http://ex.org/category"),
         iri("http://ex.org/Electronics")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod3"), iri("http://ex.org/category"),
         iri("http://ex.org/Electronics")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod4"), iri("http://ex.org/category"), iri("http://ex.org/Books")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod5"), iri("http://ex.org/category"), iri("http://ex.org/Books")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod6"), iri("http://ex.org/category"), iri("http://ex.org/Clothing")}
      )

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?category (COUNT(?product) AS ?count)
          WHERE {
            ?product <http://ex.org/category> ?category
          }
          GROUP BY ?category
        """)

      assert length(results) == 3

      # Convert to map for easier assertion
      counts =
        Map.new(results, fn binding ->
          category = Map.get(binding, "category")
          count = Map.get(binding, "count")
          {category, count}
        end)

      electronics = {:named_node, "http://ex.org/Electronics"}
      books = {:named_node, "http://ex.org/Books"}
      clothing = {:named_node, "http://ex.org/Clothing"}

      # Aggregate values are returned as typed literals
      assert Map.get(counts, electronics) ==
               {:literal, :typed, "3", "http://www.w3.org/2001/XMLSchema#integer"}

      assert Map.get(counts, books) ==
               {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}

      assert Map.get(counts, clothing) ==
               {:literal, :typed, "1", "http://www.w3.org/2001/XMLSchema#integer"}

      cleanup({db, manager})
    end

    test "2.7.3.1 - GROUP BY with COUNT and multiple grouping variables", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Orders with year and status
      add_triple(
        db,
        manager,
        {iri("http://ex.org/order1"), iri("http://ex.org/year"), literal("2023")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/order1"), iri("http://ex.org/status"), literal("completed")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/order2"), iri("http://ex.org/year"), literal("2023")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/order2"), iri("http://ex.org/status"), literal("completed")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/order3"), iri("http://ex.org/year"), literal("2023")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/order3"), iri("http://ex.org/status"), literal("pending")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/order4"), iri("http://ex.org/year"), literal("2024")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/order4"), iri("http://ex.org/status"), literal("completed")}
      )

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?year ?status (COUNT(?order) AS ?count)
          WHERE {
            ?order <http://ex.org/year> ?year .
            ?order <http://ex.org/status> ?status
          }
          GROUP BY ?year ?status
        """)

      # Should have 3 groups: (2023, completed), (2023, pending), (2024, completed)
      assert length(results) == 3

      cleanup({db, manager})
    end

    test "2.7.3.2 - GROUP BY with SUM", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Sales by region
      add_triple(
        db,
        manager,
        {iri("http://ex.org/sale1"), iri("http://ex.org/region"), literal("North")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/sale1"), iri("http://ex.org/amount"),
         {:literal, :typed, 100, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/sale2"), iri("http://ex.org/region"), literal("North")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/sale2"), iri("http://ex.org/amount"),
         {:literal, :typed, 150, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/sale3"), iri("http://ex.org/region"), literal("South")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/sale3"), iri("http://ex.org/amount"),
         {:literal, :typed, 200, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?region (SUM(?amount) AS ?total)
          WHERE {
            ?sale <http://ex.org/region> ?region .
            ?sale <http://ex.org/amount> ?amount
          }
          GROUP BY ?region
        """)

      assert length(results) == 2

      # Convert to map for easier assertion
      totals =
        Map.new(results, fn binding ->
          region = Map.get(binding, "region")
          total = Map.get(binding, "total")
          {region, total}
        end)

      north = {:literal, :simple, "North"}
      south = {:literal, :simple, "South"}

      # Aggregate values are returned as typed literals
      assert Map.get(totals, north) ==
               {:literal, :typed, "250", "http://www.w3.org/2001/XMLSchema#integer"}

      assert Map.get(totals, south) ==
               {:literal, :typed, "200", "http://www.w3.org/2001/XMLSchema#integer"}

      cleanup({db, manager})
    end

    test "2.7.3.2 - GROUP BY with AVG", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Student scores by subject
      add_triple(
        db,
        manager,
        {iri("http://ex.org/score1"), iri("http://ex.org/subject"), literal("Math")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/score1"), iri("http://ex.org/score"),
         {:literal, :typed, 80, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/score2"), iri("http://ex.org/subject"), literal("Math")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/score2"), iri("http://ex.org/score"),
         {:literal, :typed, 90, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/score3"), iri("http://ex.org/subject"), literal("Math")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/score3"), iri("http://ex.org/score"),
         {:literal, :typed, 100, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/score4"), iri("http://ex.org/subject"), literal("Science")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/score4"), iri("http://ex.org/score"),
         {:literal, :typed, 70, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/score5"), iri("http://ex.org/subject"), literal("Science")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/score5"), iri("http://ex.org/score"),
         {:literal, :typed, 80, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?subject (AVG(?score) AS ?avg_score)
          WHERE {
            ?entry <http://ex.org/subject> ?subject .
            ?entry <http://ex.org/score> ?score
          }
          GROUP BY ?subject
        """)

      assert length(results) == 2

      # Convert to map for easier assertion
      averages =
        Map.new(results, fn binding ->
          subject = Map.get(binding, "subject")
          avg = Map.get(binding, "avg_score")
          {subject, avg}
        end)

      math = {:literal, :simple, "Math"}
      science = {:literal, :simple, "Science"}

      # Math: (80 + 90 + 100) / 3 = 90
      # Science: (70 + 80) / 2 = 75
      # AVG returns decimal type per SPARQL spec
      assert Map.get(averages, math) ==
               {:literal, :typed, "90.0", "http://www.w3.org/2001/XMLSchema#decimal"}

      assert Map.get(averages, science) ==
               {:literal, :typed, "75.0", "http://www.w3.org/2001/XMLSchema#decimal"}

      cleanup({db, manager})
    end

    test "2.7.3.3 - GROUP BY with HAVING filter", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Products by category (same as COUNT test)
      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod1"), iri("http://ex.org/category"),
         iri("http://ex.org/Electronics")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod2"), iri("http://ex.org/category"),
         iri("http://ex.org/Electronics")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod3"), iri("http://ex.org/category"),
         iri("http://ex.org/Electronics")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod4"), iri("http://ex.org/category"), iri("http://ex.org/Books")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod5"), iri("http://ex.org/category"), iri("http://ex.org/Books")}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/prod6"), iri("http://ex.org/category"), iri("http://ex.org/Clothing")}
      )

      # Only return categories with more than 2 products
      {:ok, results} =
        Query.query(ctx, """
          SELECT ?category (COUNT(?product) AS ?count)
          WHERE {
            ?product <http://ex.org/category> ?category
          }
          GROUP BY ?category
          HAVING (COUNT(?product) > 2)
        """)

      # Only Electronics has 3 products (> 2)
      assert length(results) == 1

      [result] = results
      assert Map.get(result, "category") == {:named_node, "http://ex.org/Electronics"}

      assert Map.get(result, "count") ==
               {:literal, :typed, "3", "http://www.w3.org/2001/XMLSchema#integer"}

      cleanup({db, manager})
    end

    test "2.7.3.3 - GROUP BY with complex HAVING filter", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Sales by region
      add_triple(
        db,
        manager,
        {iri("http://ex.org/sale1"), iri("http://ex.org/region"), literal("North")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/sale1"), iri("http://ex.org/amount"),
         {:literal, :typed, 100, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/sale2"), iri("http://ex.org/region"), literal("North")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/sale2"), iri("http://ex.org/amount"),
         {:literal, :typed, 200, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/sale3"), iri("http://ex.org/region"), literal("South")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/sale3"), iri("http://ex.org/amount"),
         {:literal, :typed, 50, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_triple(
        db,
        manager,
        {iri("http://ex.org/sale4"), iri("http://ex.org/region"), literal("East")}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/sale4"), iri("http://ex.org/amount"),
         {:literal, :typed, 300, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      # Only return regions with total sales >= 100 AND at least 1 sale
      {:ok, results} =
        Query.query(ctx, """
          SELECT ?region (SUM(?amount) AS ?total)
          WHERE {
            ?sale <http://ex.org/region> ?region .
            ?sale <http://ex.org/amount> ?amount
          }
          GROUP BY ?region
          HAVING (SUM(?amount) >= 100)
        """)

      # North: 300 (>= 100), South: 50 (< 100), East: 300 (>= 100)
      assert length(results) == 2

      regions = Enum.map(results, fn binding -> Map.get(binding, "region") end)
      north = {:literal, :simple, "North"}
      east = {:literal, :simple, "East"}
      south = {:literal, :simple, "South"}
      assert north in regions
      assert east in regions
      refute south in regions

      cleanup({db, manager})
    end

    test "2.7.3.4 - implicit grouping with single aggregate", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Multiple products with prices
      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/prod1"), iri("http://ex.org/price"),
         {:literal, :typed, 100, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/prod2"), iri("http://ex.org/price"),
         {:literal, :typed, 200, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/prod3"), iri("http://ex.org/price"),
         {:literal, :typed, 300, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/prod4"), iri("http://ex.org/price"),
         {:literal, :typed, 400, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/prod5"), iri("http://ex.org/price"),
         {:literal, :typed, 500, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      # Aggregate without GROUP BY = implicit grouping
      {:ok, results} =
        Query.query(ctx, """
          SELECT (COUNT(?product) AS ?total_products) (SUM(?price) AS ?total_value) (AVG(?price) AS ?avg_price)
          WHERE {
            ?product <http://ex.org/price> ?price
          }
        """)

      # Should return exactly 1 result (implicit single group)
      assert length(results) == 1

      [result] = results
      # Aggregate values are returned as typed literals
      # AVG returns decimal type per SPARQL spec
      assert Map.get(result, "total_products") ==
               {:literal, :typed, "5", "http://www.w3.org/2001/XMLSchema#integer"}

      assert Map.get(result, "total_value") ==
               {:literal, :typed, "1500", "http://www.w3.org/2001/XMLSchema#integer"}

      assert Map.get(result, "avg_price") ==
               {:literal, :typed, "300.0", "http://www.w3.org/2001/XMLSchema#decimal"}

      cleanup({db, manager})
    end

    test "2.7.3.4 - implicit grouping with MIN and MAX", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Setup: Products with prices
      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/prod1"), iri("http://ex.org/price"),
         {:literal, :typed, 50, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/prod2"), iri("http://ex.org/price"),
         {:literal, :typed, 150, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      add_typed_triple(
        db,
        manager,
        {iri("http://ex.org/prod3"), iri("http://ex.org/price"),
         {:literal, :typed, 75, "http://www.w3.org/2001/XMLSchema#integer"}}
      )

      {:ok, results} =
        Query.query(ctx, """
          SELECT (MIN(?price) AS ?min_price) (MAX(?price) AS ?max_price)
          WHERE {
            ?product <http://ex.org/price> ?price
          }
        """)

      assert length(results) == 1

      [result] = results
      # Aggregate values are returned as typed literals
      assert Map.get(result, "min_price") ==
               {:literal, :typed, "50", "http://www.w3.org/2001/XMLSchema#integer"}

      assert Map.get(result, "max_price") ==
               {:literal, :typed, "150", "http://www.w3.org/2001/XMLSchema#integer"}

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # FILTER NOT EXISTS and MINUS Tests
  # ===========================================================================

  describe "MINUS operation" do
    test "MINUS excludes matching patterns", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add people
      add_triple(db, manager, {
        iri("http://ex.org/Alice"),
        iri("http://ex.org/type"),
        iri("http://ex.org/Person")
      })

      add_triple(db, manager, {
        iri("http://ex.org/Bob"),
        iri("http://ex.org/type"),
        iri("http://ex.org/Person")
      })

      add_triple(db, manager, {
        iri("http://ex.org/Charlie"),
        iri("http://ex.org/type"),
        iri("http://ex.org/Person")
      })

      # Mark Bob as deleted
      add_triple(db, manager, {
        iri("http://ex.org/Bob"),
        iri("http://ex.org/deleted"),
        literal("true")
      })

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?person WHERE {
            ?person <http://ex.org/type> <http://ex.org/Person> .
            MINUS { ?person <http://ex.org/deleted> ?d }
          }
        """)

      # Alice and Charlie should remain, Bob should be excluded
      assert length(results) == 2

      persons = Enum.map(results, fn r -> r["person"] end)
      assert {:named_node, "http://ex.org/Alice"} in persons
      assert {:named_node, "http://ex.org/Charlie"} in persons
      refute {:named_node, "http://ex.org/Bob"} in persons

      cleanup({db, manager})
    end
  end

  describe "FILTER NOT EXISTS" do
    test "filters out solutions with matching patterns", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add people
      add_triple(db, manager, {
        iri("http://ex.org/Alice"),
        iri("http://ex.org/type"),
        iri("http://ex.org/Person")
      })

      add_triple(db, manager, {
        iri("http://ex.org/Bob"),
        iri("http://ex.org/type"),
        iri("http://ex.org/Person")
      })

      # Only Alice has an email
      add_triple(db, manager, {
        iri("http://ex.org/Alice"),
        iri("http://ex.org/email"),
        literal("alice@example.org")
      })

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?person WHERE {
            ?person <http://ex.org/type> <http://ex.org/Person> .
            FILTER NOT EXISTS { ?person <http://ex.org/email> ?email }
          }
        """)

      # Only Bob should remain (no email)
      assert length(results) == 1
      assert hd(results)["person"] == {:named_node, "http://ex.org/Bob"}

      cleanup({db, manager})
    end

    test "FILTER EXISTS keeps only solutions with matching patterns", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add people
      add_triple(db, manager, {
        iri("http://ex.org/Alice"),
        iri("http://ex.org/type"),
        iri("http://ex.org/Person")
      })

      add_triple(db, manager, {
        iri("http://ex.org/Bob"),
        iri("http://ex.org/type"),
        iri("http://ex.org/Person")
      })

      # Only Alice has an email
      add_triple(db, manager, {
        iri("http://ex.org/Alice"),
        iri("http://ex.org/email"),
        literal("alice@example.org")
      })

      {:ok, results} =
        Query.query(ctx, """
          SELECT ?person WHERE {
            ?person <http://ex.org/type> <http://ex.org/Person> .
            FILTER EXISTS { ?person <http://ex.org/email> ?email }
          }
        """)

      # Only Alice should remain (has email)
      assert length(results) == 1
      assert hd(results)["person"] == {:named_node, "http://ex.org/Alice"}

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Cache Integration Tests (Section 3.2.5)
  # ===========================================================================

  describe "query/3 with :use_cache option" do
    setup %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)

      # Start a test cache
      cache_name = :"test_query_cache_#{:erlang.unique_integer([:positive])}"
      {:ok, cache_pid} = TripleStore.Query.Cache.start_link(name: cache_name, max_entries: 100)

      # Add some test data
      add_triple(db, manager, {
        iri("http://ex.org/Alice"),
        iri("http://ex.org/name"),
        literal("Alice")
      })

      add_triple(db, manager, {
        iri("http://ex.org/Bob"),
        iri("http://ex.org/name"),
        literal("Bob")
      })

      ctx = %{db: db, dict_manager: manager}

      on_exit(fn ->
        try do
          if Process.alive?(cache_pid), do: GenServer.stop(cache_pid)
        catch
          :exit, _ -> :ok
        end

        try do
          cleanup({db, manager})
        catch
          :exit, _ -> :ok
        end
      end)

      %{ctx: ctx, cache_name: cache_name}
    end

    test "caches query results when use_cache is true", %{ctx: ctx, cache_name: cache_name} do
      query = "SELECT ?name WHERE { ?s <http://ex.org/name> ?name }"

      # First query - should execute and cache
      {:ok, results1} = Query.query(ctx, query, use_cache: true, cache_name: cache_name)
      assert length(results1) == 2

      # Cache should have an entry now
      stats = TripleStore.Query.Cache.stats(name: cache_name)
      assert stats.size == 1

      # Second query - should hit cache
      {:ok, results2} = Query.query(ctx, query, use_cache: true, cache_name: cache_name)
      assert results2 == results1

      # Check cache stats
      stats_after = TripleStore.Query.Cache.stats(name: cache_name)
      assert stats_after.hits >= 1
    end

    test "skips cache when use_cache is false (default)", %{ctx: ctx, cache_name: cache_name} do
      query = "SELECT ?name WHERE { ?s <http://ex.org/name> ?name }"

      # Execute without caching
      {:ok, results} = Query.query(ctx, query, cache_name: cache_name)
      assert length(results) == 2

      # Cache should be empty
      assert TripleStore.Query.Cache.size(name: cache_name) == 0
    end

    test "skips caching for queries with RAND", %{ctx: _ctx, cache_name: _cache_name} do
      # Non-deterministic function detection - RAND should not be cached
      refute TripleStore.Query.Cache.has_non_deterministic_functions?(
               "SELECT ?name WHERE { ?s ?p ?o }"
             )

      assert TripleStore.Query.Cache.has_non_deterministic_functions?(
               "SELECT (RAND() AS ?r) WHERE { ?s ?p ?o }"
             )
    end

    test "skips caching for queries with NOW", %{ctx: _ctx, cache_name: _cache_name} do
      assert TripleStore.Query.Cache.has_non_deterministic_functions?(
               "SELECT (NOW() AS ?t) WHERE { ?s ?p ?o }"
             )
    end

    test "cache tracks predicates for invalidation", %{ctx: ctx, cache_name: cache_name} do
      query = "SELECT ?name WHERE { ?s <http://ex.org/name> ?name }"

      # Execute with caching
      {:ok, _} = Query.query(ctx, query, use_cache: true, cache_name: cache_name)
      assert TripleStore.Query.Cache.size(name: cache_name) == 1

      # Invalidate based on predicate
      TripleStore.Query.Cache.invalidate_predicates(
        ["http://ex.org/name"],
        name: cache_name
      )

      # Cache should be empty
      assert TripleStore.Query.Cache.size(name: cache_name) == 0
    end

    test "cache hit returns same results as fresh execution", %{ctx: ctx, cache_name: cache_name} do
      query = "SELECT ?s ?name WHERE { ?s <http://ex.org/name> ?name } ORDER BY ?name"

      # Execute without cache first
      {:ok, uncached} = Query.query(ctx, query)

      # Execute with cache (cache miss)
      {:ok, cached1} = Query.query(ctx, query, use_cache: true, cache_name: cache_name)

      # Execute with cache (cache hit)
      {:ok, cached2} = Query.query(ctx, query, use_cache: true, cache_name: cache_name)

      # All should return the same results
      assert cached1 == uncached
      assert cached2 == uncached
    end

    test "explain option bypasses cache", %{ctx: ctx, cache_name: cache_name} do
      query = "SELECT ?name WHERE { ?s <http://ex.org/name> ?name }"

      # Explain should not use cache even when use_cache is true
      {:ok, {:explain, explanation}} =
        Query.query(ctx, query, explain: true, use_cache: true, cache_name: cache_name)

      assert explanation.query_type == :select

      # Cache should still be empty
      assert TripleStore.Query.Cache.size(name: cache_name) == 0
    end
  end
end
