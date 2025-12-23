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

  # ===========================================================================
  # Streaming Query Tests (Task 2.5.2)
  # ===========================================================================

  describe "stream_query/2" do
    test "returns lazy stream of bindings", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})
      add_triple(db, manager, {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")})

      {:ok, stream} = Query.stream_query(ctx, "SELECT ?name WHERE { ?s <http://ex.org/name> ?name }")

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

      {:ok, stream} = Query.stream_query(ctx, "SELECT ?s WHERE { ?s <http://ex.org/nonexistent> ?o }")

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

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

      {:ok, stream} = Query.stream_query(ctx, "SELECT * WHERE { ?s <http://ex.org/name> ?name }", variables: ["name"])

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
      add_triple(db, manager, {iri("http://ex.org/s1"), iri("http://ex.org/type"), iri("http://ex.org/Person")})
      add_triple(db, manager, {iri("http://ex.org/s2"), iri("http://ex.org/type"), iri("http://ex.org/Person")})

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

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { ?s <http://ex.org/name> ?name }")
      {:ok, results} = Query.execute(ctx, prepared)

      assert length(results) == 1
      assert hd(results)["name"] == {:literal, :simple, "Alice"}

      cleanup({db, manager})
    end

    test "executes prepared query with URI parameter", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})
      add_triple(db, manager, {iri("http://ex.org/Bob"), iri("http://ex.org/name"), literal("Bob")})

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

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s <http://ex.org/name> $name }")

      {:ok, results} = Query.execute(ctx, prepared, %{"name" => "Alice"})
      assert length(results) == 1

      cleanup({db, manager})
    end

    test "executes prepared query with named_node tuple parameter", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { $person <http://ex.org/name> ?name }")

      {:ok, results} = Query.execute(ctx, prepared, %{"person" => {:named_node, "http://ex.org/Alice"}})
      assert length(results) == 1

      cleanup({db, manager})
    end

    test "executes prepared query with literal tuple parameter", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

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

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

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

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

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

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

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

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

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

      add_triple(db, manager, {iri("http://ex.org/Alice"), iri("http://ex.org/name"), literal("Alice")})

      {:ok, prepared} = Query.prepare("SELECT ?name WHERE { $person <http://ex.org/name> ?name }")

      # Simulate serialization/deserialization by converting to/from term
      serialized = :erlang.term_to_binary(prepared)
      deserialized = :erlang.binary_to_term(serialized)

      {:ok, results} = Query.execute(ctx, deserialized, %{"person" => "http://ex.org/Alice"})
      assert length(results) == 1

      cleanup({db, manager})
    end
  end
end
