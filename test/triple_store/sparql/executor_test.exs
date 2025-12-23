defmodule TripleStore.SPARQL.ExecutorTest do
  use ExUnit.Case, async: false

  alias TripleStore.SPARQL.Executor
  alias TripleStore.Index
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Backend.RocksDB.NIF

  @moduletag :tmp_dir

  # Helper to create a temporary database
  defp setup_db(tmp_dir) do
    db_path = Path.join(tmp_dir, "test_db")
    {:ok, db} = NIF.open(db_path)
    {:ok, manager} = Manager.start_link(db: db)
    {db, manager}
  end

  # Helper to clean up
  defp cleanup({_db, manager}) do
    Manager.stop(manager)
  end

  # Helper to create algebra terms
  defp var(name), do: {:variable, name}
  defp iri(uri), do: {:named_node, uri}
  defp literal(value), do: {:literal, :simple, value}
  defp typed_literal(value, type), do: {:literal, :typed, value, type}
  defp triple(s, p, o), do: {:triple, s, p, o}

  # Helper to add a triple to the database
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

  describe "execute_bgp/3" do
    test "empty pattern returns single empty binding", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, stream} = Executor.execute_bgp(ctx, [])
      results = Enum.to_list(stream)

      assert results == [%{}]

      cleanup({db, manager})
    end

    test "single pattern with no matches returns empty stream", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      patterns = [triple(var("s"), iri("http://example.org/name"), var("o"))]
      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      results = Enum.to_list(stream)

      assert results == []

      cleanup({db, manager})
    end

    test "single pattern returns matching bindings", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/name"),
        literal("Alice")
      })

      patterns = [triple(var("s"), iri("http://example.org/name"), var("name"))]
      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      results = Enum.to_list(stream)

      assert length(results) == 1
      [binding] = results
      assert binding["s"] == {:named_node, "http://example.org/Alice"}
      assert binding["name"] == {:literal, :simple, "Alice"}

      cleanup({db, manager})
    end

    test "multiple matching triples return multiple bindings", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/name"),
        literal("Alice")
      })
      add_triple(db, manager, {
        iri("http://example.org/Bob"),
        iri("http://example.org/name"),
        literal("Bob")
      })

      patterns = [triple(var("s"), iri("http://example.org/name"), var("name"))]
      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      results = Enum.to_list(stream)

      assert length(results) == 2
      names = Enum.map(results, & &1["name"])
      assert {:literal, :simple, "Alice"} in names
      assert {:literal, :simple, "Bob"} in names

      cleanup({db, manager})
    end

    test "pattern with bound subject filters results", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/name"),
        literal("Alice")
      })
      add_triple(db, manager, {
        iri("http://example.org/Bob"),
        iri("http://example.org/name"),
        literal("Bob")
      })

      # Query with bound subject
      patterns = [
        triple(iri("http://example.org/Alice"), iri("http://example.org/name"), var("name"))
      ]
      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      results = Enum.to_list(stream)

      assert length(results) == 1
      [binding] = results
      assert binding["name"] == {:literal, :simple, "Alice"}

      cleanup({db, manager})
    end

    test "two patterns join on shared variable", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data: Alice knows Bob, Bob knows Carol
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/knows"),
        iri("http://example.org/Bob")
      })
      add_triple(db, manager, {
        iri("http://example.org/Bob"),
        iri("http://example.org/knows"),
        iri("http://example.org/Carol")
      })
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/knows"),
        iri("http://example.org/Dave")
      })

      # Find all x where Alice knows x
      patterns = [
        triple(iri("http://example.org/Alice"), iri("http://example.org/knows"), var("x"))
      ]
      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      results = Enum.to_list(stream)

      assert length(results) == 2

      cleanup({db, manager})
    end

    test "join with shared variable produces correct bindings", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/knows"),
        iri("http://example.org/Bob")
      })
      add_triple(db, manager, {
        iri("http://example.org/Bob"),
        iri("http://example.org/age"),
        typed_literal("30", "http://www.w3.org/2001/XMLSchema#integer")
      })

      # Find who Alice knows and their age
      patterns = [
        triple(iri("http://example.org/Alice"), iri("http://example.org/knows"), var("person")),
        triple(var("person"), iri("http://example.org/age"), var("age"))
      ]
      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      results = Enum.to_list(stream)

      assert length(results) == 1
      [binding] = results
      assert binding["person"] == {:named_node, "http://example.org/Bob"}
      assert binding["age"] == {:literal, :typed, "30", "http://www.w3.org/2001/XMLSchema#integer"}

      cleanup({db, manager})
    end

    test "initial binding constrains results", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/name"),
        literal("Alice")
      })
      add_triple(db, manager, {
        iri("http://example.org/Bob"),
        iri("http://example.org/name"),
        literal("Bob")
      })

      # Query with initial binding
      initial_binding = %{"s" => {:named_node, "http://example.org/Alice"}}
      patterns = [triple(var("s"), iri("http://example.org/name"), var("name"))]
      {:ok, stream} = Executor.execute_bgp(ctx, patterns, initial_binding)
      results = Enum.to_list(stream)

      assert length(results) == 1
      [binding] = results
      assert binding["s"] == {:named_node, "http://example.org/Alice"}
      assert binding["name"] == {:literal, :simple, "Alice"}

      cleanup({db, manager})
    end

    test "pattern with all variables returns all triples", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data
      add_triple(db, manager, {
        iri("http://example.org/s1"),
        iri("http://example.org/p1"),
        literal("o1")
      })
      add_triple(db, manager, {
        iri("http://example.org/s2"),
        iri("http://example.org/p2"),
        literal("o2")
      })

      patterns = [triple(var("s"), var("p"), var("o"))]
      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      results = Enum.to_list(stream)

      assert length(results) == 2

      cleanup({db, manager})
    end

    test "same variable in multiple positions requires consistent binding", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add reflexive and non-reflexive relations
      add_triple(db, manager, {
        iri("http://example.org/A"),
        iri("http://example.org/sameAs"),
        iri("http://example.org/A")
      })
      add_triple(db, manager, {
        iri("http://example.org/B"),
        iri("http://example.org/sameAs"),
        iri("http://example.org/C")
      })

      # Find reflexive relations: ?x sameAs ?x
      patterns = [triple(var("x"), iri("http://example.org/sameAs"), var("x"))]
      {:ok, stream} = Executor.execute_bgp(ctx, patterns)
      results = Enum.to_list(stream)

      # Only the reflexive A-A should match
      assert length(results) == 1
      [binding] = results
      assert binding["x"] == {:named_node, "http://example.org/A"}

      cleanup({db, manager})
    end
  end

  describe "empty_stream/0" do
    test "returns an empty stream" do
      stream = Executor.empty_stream()
      assert Enum.to_list(stream) == []
    end
  end

  describe "unit_stream/0" do
    test "returns a stream with single empty binding" do
      stream = Executor.unit_stream()
      assert Enum.to_list(stream) == [%{}]
    end
  end

  describe "execute_pattern/3" do
    test "executes a single pattern against bindings", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/name"),
        literal("Alice")
      })

      # Start with empty binding stream
      input_stream = Stream.iterate(%{}, & &1) |> Stream.take(1)
      pattern = triple(var("s"), iri("http://example.org/name"), var("name"))

      {:ok, stream} = Executor.execute_pattern(ctx, input_stream, pattern)
      results = Enum.to_list(stream)

      assert length(results) == 1
      [binding] = results
      assert binding["s"] == {:named_node, "http://example.org/Alice"}

      cleanup({db, manager})
    end
  end
end
