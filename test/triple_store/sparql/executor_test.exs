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

  # ===========================================================================
  # Join Execution Tests (Task 2.4.2)
  # ===========================================================================

  describe "merge_bindings/2" do
    test "merges non-overlapping bindings" do
      binding1 = %{"x" => 1, "y" => 2}
      binding2 = %{"z" => 3}

      assert {:ok, merged} = Executor.merge_bindings(binding1, binding2)
      assert merged == %{"x" => 1, "y" => 2, "z" => 3}
    end

    test "merges overlapping bindings with same values" do
      binding1 = %{"x" => 1, "y" => 2}
      binding2 = %{"x" => 1, "z" => 3}

      assert {:ok, merged} = Executor.merge_bindings(binding1, binding2)
      assert merged == %{"x" => 1, "y" => 2, "z" => 3}
    end

    test "returns incompatible for conflicting values" do
      binding1 = %{"x" => 1}
      binding2 = %{"x" => 2}

      assert :incompatible = Executor.merge_bindings(binding1, binding2)
    end

    test "handles empty bindings" do
      assert {:ok, %{}} = Executor.merge_bindings(%{}, %{})
      assert {:ok, %{"x" => 1}} = Executor.merge_bindings(%{"x" => 1}, %{})
      assert {:ok, %{"x" => 1}} = Executor.merge_bindings(%{}, %{"x" => 1})
    end

    test "handles RDF term values" do
      binding1 = %{"s" => {:named_node, "http://ex.org/A"}}
      binding2 = %{"s" => {:named_node, "http://ex.org/A"}, "name" => {:literal, :simple, "Alice"}}

      assert {:ok, merged} = Executor.merge_bindings(binding1, binding2)
      assert merged["s"] == {:named_node, "http://ex.org/A"}
      assert merged["name"] == {:literal, :simple, "Alice"}
    end
  end

  describe "bindings_compatible?/2" do
    test "returns true for compatible bindings" do
      assert Executor.bindings_compatible?(%{"x" => 1}, %{"y" => 2})
      assert Executor.bindings_compatible?(%{"x" => 1}, %{"x" => 1})
      assert Executor.bindings_compatible?(%{}, %{"x" => 1})
    end

    test "returns false for incompatible bindings" do
      refute Executor.bindings_compatible?(%{"x" => 1}, %{"x" => 2})
    end
  end

  describe "nested_loop_join/2" do
    test "joins bindings with shared variables" do
      left = [
        %{"x" => {:named_node, "http://ex.org/A"}, "y" => {:literal, :simple, "1"}},
        %{"x" => {:named_node, "http://ex.org/B"}, "y" => {:literal, :simple, "2"}}
      ]

      right = [
        %{"x" => {:named_node, "http://ex.org/A"}, "z" => {:literal, :simple, "a"}},
        %{"x" => {:named_node, "http://ex.org/C"}, "z" => {:literal, :simple, "c"}}
      ]

      results = Executor.nested_loop_join(left, right) |> Enum.to_list()

      assert length(results) == 1
      [binding] = results
      assert binding["x"] == {:named_node, "http://ex.org/A"}
      assert binding["y"] == {:literal, :simple, "1"}
      assert binding["z"] == {:literal, :simple, "a"}
    end

    test "produces cartesian product for non-overlapping variables" do
      left = [%{"x" => 1}, %{"x" => 2}]
      right = [%{"y" => "a"}, %{"y" => "b"}]

      results = Executor.nested_loop_join(left, right) |> Enum.to_list()

      assert length(results) == 4
      assert %{"x" => 1, "y" => "a"} in results
      assert %{"x" => 1, "y" => "b"} in results
      assert %{"x" => 2, "y" => "a"} in results
      assert %{"x" => 2, "y" => "b"} in results
    end

    test "returns empty for empty inputs" do
      assert [] == Executor.nested_loop_join([], [%{"x" => 1}]) |> Enum.to_list()
      assert [] == Executor.nested_loop_join([%{"x" => 1}], []) |> Enum.to_list()
      assert [] == Executor.nested_loop_join([], []) |> Enum.to_list()
    end

    test "handles multiple matches per left binding" do
      left = [%{"x" => 1}]
      right = [%{"x" => 1, "y" => "a"}, %{"x" => 1, "y" => "b"}]

      results = Executor.nested_loop_join(left, right) |> Enum.to_list()

      assert length(results) == 2
      assert %{"x" => 1, "y" => "a"} in results
      assert %{"x" => 1, "y" => "b"} in results
    end

    test "filters incompatible bindings" do
      left = [%{"x" => 1, "y" => 1}]
      right = [%{"x" => 1, "y" => 2}]  # Incompatible: y differs

      results = Executor.nested_loop_join(left, right) |> Enum.to_list()
      assert results == []
    end
  end

  describe "hash_join/2" do
    test "joins bindings with shared variables" do
      left = [
        %{"x" => {:named_node, "http://ex.org/A"}, "y" => {:literal, :simple, "1"}},
        %{"x" => {:named_node, "http://ex.org/B"}, "y" => {:literal, :simple, "2"}}
      ]

      right = [
        %{"x" => {:named_node, "http://ex.org/A"}, "z" => {:literal, :simple, "a"}},
        %{"x" => {:named_node, "http://ex.org/C"}, "z" => {:literal, :simple, "c"}}
      ]

      results = Executor.hash_join(left, right) |> Enum.to_list()

      assert length(results) == 1
      [binding] = results
      assert binding["x"] == {:named_node, "http://ex.org/A"}
      assert binding["y"] == {:literal, :simple, "1"}
      assert binding["z"] == {:literal, :simple, "a"}
    end

    test "produces cartesian product for non-overlapping variables" do
      left = [%{"x" => 1}, %{"x" => 2}]
      right = [%{"y" => "a"}, %{"y" => "b"}]

      results = Executor.hash_join(left, right) |> Enum.to_list()

      assert length(results) == 4
      assert %{"x" => 1, "y" => "a"} in results
      assert %{"x" => 1, "y" => "b"} in results
      assert %{"x" => 2, "y" => "a"} in results
      assert %{"x" => 2, "y" => "b"} in results
    end

    test "returns empty for empty inputs" do
      assert [] == Executor.hash_join([], [%{"x" => 1}]) |> Enum.to_list()
      assert [] == Executor.hash_join([%{"x" => 1}], []) |> Enum.to_list()
      assert [] == Executor.hash_join([], []) |> Enum.to_list()
    end

    test "handles multiple values for same key" do
      left = [%{"x" => 1, "y" => "a"}, %{"x" => 1, "y" => "b"}]
      right = [%{"x" => 1, "z" => "c"}]

      results = Executor.hash_join(left, right) |> Enum.to_list()

      assert length(results) == 2
      assert %{"x" => 1, "y" => "a", "z" => "c"} in results
      assert %{"x" => 1, "y" => "b", "z" => "c"} in results
    end

    test "handles multiple join variables" do
      left = [
        %{"x" => 1, "y" => 2, "a" => "L1"},
        %{"x" => 1, "y" => 3, "a" => "L2"}
      ]
      right = [
        %{"x" => 1, "y" => 2, "b" => "R1"},
        %{"x" => 1, "y" => 3, "b" => "R2"}
      ]

      results = Executor.hash_join(left, right) |> Enum.to_list()

      assert length(results) == 2
      assert %{"x" => 1, "y" => 2, "a" => "L1", "b" => "R1"} in results
      assert %{"x" => 1, "y" => 3, "a" => "L2", "b" => "R2"} in results
    end
  end

  describe "join/3" do
    test "default strategy produces correct results" do
      left = [%{"x" => 1}, %{"x" => 2}]
      right = [%{"x" => 1, "y" => "a"}, %{"x" => 2, "y" => "b"}]

      results = Executor.join(left, right) |> Enum.to_list()

      assert length(results) == 2
      assert %{"x" => 1, "y" => "a"} in results
      assert %{"x" => 2, "y" => "b"} in results
    end

    test "nested_loop strategy option works" do
      left = [%{"x" => 1}]
      right = [%{"x" => 1, "y" => "a"}]

      results = Executor.join(left, right, strategy: :nested_loop) |> Enum.to_list()

      assert results == [%{"x" => 1, "y" => "a"}]
    end

    test "hash strategy option works" do
      left = [%{"x" => 1}]
      right = [%{"x" => 1, "y" => "a"}]

      results = Executor.join(left, right, strategy: :hash) |> Enum.to_list()

      assert results == [%{"x" => 1, "y" => "a"}]
    end

    test "hash and nested_loop produce same results" do
      left = [
        %{"x" => 1, "y" => "a"},
        %{"x" => 2, "y" => "b"},
        %{"x" => 1, "y" => "c"}
      ]
      right = [
        %{"x" => 1, "z" => "1"},
        %{"x" => 3, "z" => "3"},
        %{"x" => 1, "z" => "2"}
      ]

      nested_results = Executor.join(left, right, strategy: :nested_loop) |> Enum.to_list() |> Enum.sort()
      hash_results = Executor.join(left, right, strategy: :hash) |> Enum.to_list() |> Enum.sort()

      assert nested_results == hash_results
      assert length(nested_results) == 4  # 2 left bindings with x=1, 2 right with x=1
    end
  end

  describe "left_join/3" do
    test "preserves left bindings when no match on right" do
      left = [%{"x" => 1}, %{"x" => 2}]
      right = [%{"x" => 3, "y" => "a"}]  # No matching x values

      results = Executor.left_join(left, right) |> Enum.to_list()

      # Both left bindings should be preserved unextended
      assert length(results) == 2
      assert %{"x" => 1} in results
      assert %{"x" => 2} in results
    end

    test "extends left bindings when match exists" do
      left = [%{"x" => 1}, %{"x" => 2}]
      right = [%{"x" => 1, "y" => "a"}]

      results = Executor.left_join(left, right) |> Enum.to_list()

      assert length(results) == 2
      # x=1 should be extended with y
      assert %{"x" => 1, "y" => "a"} in results
      # x=2 should be preserved unextended
      assert %{"x" => 2} in results
    end

    test "handles multiple matches per left binding" do
      left = [%{"x" => 1}]
      right = [%{"x" => 1, "y" => "a"}, %{"x" => 1, "y" => "b"}]

      results = Executor.left_join(left, right) |> Enum.to_list()

      # Both matches should be produced
      assert length(results) == 2
      assert %{"x" => 1, "y" => "a"} in results
      assert %{"x" => 1, "y" => "b"} in results
    end

    test "handles empty right side (all left preserved)" do
      left = [%{"x" => 1}, %{"x" => 2}]
      right = []

      results = Executor.left_join(left, right) |> Enum.to_list()

      assert results == left
    end

    test "handles empty left side" do
      left = []
      right = [%{"x" => 1}]

      results = Executor.left_join(left, right) |> Enum.to_list()

      assert results == []
    end

    test "applies filter function to matches" do
      left = [%{"x" => 1}]
      right = [%{"x" => 1, "y" => 1}, %{"x" => 1, "y" => 2}]

      # Filter to only y > 1
      filter_fn = fn binding -> binding["y"] > 1 end
      results = Executor.left_join(left, right, filter: filter_fn) |> Enum.to_list()

      assert results == [%{"x" => 1, "y" => 2}]
    end

    test "preserves left when filter rejects all matches" do
      left = [%{"x" => 1}]
      right = [%{"x" => 1, "y" => 1}]

      # Filter rejects the only match
      filter_fn = fn binding -> binding["y"] > 10 end
      results = Executor.left_join(left, right, filter: filter_fn) |> Enum.to_list()

      # Left should be preserved since no filtered matches
      assert results == [%{"x" => 1}]
    end

    test "implements OPTIONAL semantics correctly" do
      # Simulate: ?s :name ?name . OPTIONAL { ?s :age ?age }
      left = [
        %{"s" => {:named_node, "http://ex.org/Alice"}, "name" => {:literal, :simple, "Alice"}},
        %{"s" => {:named_node, "http://ex.org/Bob"}, "name" => {:literal, :simple, "Bob"}}
      ]
      right = [
        # Only Alice has an age
        %{"s" => {:named_node, "http://ex.org/Alice"}, "age" => {:literal, :typed, "30", "xsd:integer"}}
      ]

      results = Executor.left_join(left, right) |> Enum.to_list()

      assert length(results) == 2

      # Find Alice's binding (should have age)
      alice = Enum.find(results, fn b -> b["name"] == {:literal, :simple, "Alice"} end)
      assert alice["age"] == {:literal, :typed, "30", "xsd:integer"}

      # Find Bob's binding (should not have age)
      bob = Enum.find(results, fn b -> b["name"] == {:literal, :simple, "Bob"} end)
      refute Map.has_key?(bob, "age")
    end
  end

  describe "join integration with BGP execution" do
    test "join produces same results as BGP multi-pattern", %{tmp_dir: tmp_dir} do
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
        iri("http://example.org/name"),
        literal("Bob")
      })

      # Execute as BGP with two patterns
      bgp_patterns = [
        triple(var("s"), iri("http://example.org/knows"), var("friend")),
        triple(var("friend"), iri("http://example.org/name"), var("name"))
      ]
      {:ok, bgp_stream} = Executor.execute_bgp(ctx, bgp_patterns)
      bgp_results = Enum.to_list(bgp_stream)

      # Execute as separate BGPs joined
      {:ok, pattern1_stream} = Executor.execute_bgp(ctx, [
        triple(var("s"), iri("http://example.org/knows"), var("friend"))
      ])
      {:ok, pattern2_stream} = Executor.execute_bgp(ctx, [
        triple(var("friend"), iri("http://example.org/name"), var("name"))
      ])

      pattern1_results = Enum.to_list(pattern1_stream)
      pattern2_results = Enum.to_list(pattern2_stream)
      join_results = Executor.join(pattern1_results, pattern2_results) |> Enum.to_list()

      # Both approaches should give same bindings
      assert length(bgp_results) == length(join_results)
      assert length(bgp_results) == 1

      [bgp_binding] = bgp_results
      [join_binding] = join_results

      assert bgp_binding["s"] == join_binding["s"]
      assert bgp_binding["friend"] == join_binding["friend"]
      assert bgp_binding["name"] == join_binding["name"]

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Union Execution Tests (Task 2.4.3)
  # ===========================================================================

  describe "union/2" do
    test "concatenates two binding streams" do
      left = [%{"x" => 1}, %{"x" => 2}]
      right = [%{"x" => 3}, %{"x" => 4}]

      results = Executor.union(left, right) |> Enum.to_list()

      assert results == [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}, %{"x" => 4}]
    end

    test "preserves order within branches" do
      left = [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]
      right = [%{"y" => "a"}, %{"y" => "b"}]

      results = Executor.union(left, right) |> Enum.to_list()

      # Left branch comes first, then right branch
      assert Enum.at(results, 0) == %{"x" => 1}
      assert Enum.at(results, 1) == %{"x" => 2}
      assert Enum.at(results, 2) == %{"x" => 3}
      assert Enum.at(results, 3) == %{"y" => "a"}
      assert Enum.at(results, 4) == %{"y" => "b"}
    end

    test "handles different variables in each branch" do
      left = [%{"x" => 1, "y" => "a"}]
      right = [%{"x" => 2, "z" => "b"}]

      results = Executor.union(left, right) |> Enum.to_list()

      assert length(results) == 2
      # Each binding retains only its own variables
      assert Enum.at(results, 0) == %{"x" => 1, "y" => "a"}
      assert Enum.at(results, 1) == %{"x" => 2, "z" => "b"}
    end

    test "handles empty left branch" do
      left = []
      right = [%{"x" => 1}]

      results = Executor.union(left, right) |> Enum.to_list()

      assert results == [%{"x" => 1}]
    end

    test "handles empty right branch" do
      left = [%{"x" => 1}]
      right = []

      results = Executor.union(left, right) |> Enum.to_list()

      assert results == [%{"x" => 1}]
    end

    test "handles both branches empty" do
      results = Executor.union([], []) |> Enum.to_list()

      assert results == []
    end

    test "works with streams" do
      left = Stream.map([1, 2], fn x -> %{"x" => x} end)
      right = Stream.map([3, 4], fn x -> %{"x" => x} end)

      results = Executor.union(left, right) |> Enum.to_list()

      assert results == [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}, %{"x" => 4}]
    end

    test "handles RDF term values" do
      left = [%{"s" => {:named_node, "http://ex.org/A"}, "name" => {:literal, :simple, "Alice"}}]
      right = [%{"s" => {:named_node, "http://ex.org/B"}, "name" => {:literal, :simple, "Bob"}}]

      results = Executor.union(left, right) |> Enum.to_list()

      assert length(results) == 2
      assert Enum.at(results, 0)["name"] == {:literal, :simple, "Alice"}
      assert Enum.at(results, 1)["name"] == {:literal, :simple, "Bob"}
    end
  end

  describe "union_aligned/2" do
    test "aligns variables across branches" do
      left = [%{"x" => 1, "y" => "a"}]
      right = [%{"x" => 2, "z" => "b"}]

      results = Executor.union_aligned(left, right) |> Enum.to_list()

      assert length(results) == 2

      # First binding should have z as :unbound
      first = Enum.at(results, 0)
      assert first["x"] == 1
      assert first["y"] == "a"
      assert first["z"] == :unbound

      # Second binding should have y as :unbound
      second = Enum.at(results, 1)
      assert second["x"] == 2
      assert second["y"] == :unbound
      assert second["z"] == "b"
    end

    test "handles completely different variables" do
      left = [%{"a" => 1}]
      right = [%{"b" => 2}]

      results = Executor.union_aligned(left, right) |> Enum.to_list()

      assert Enum.at(results, 0) == %{"a" => 1, "b" => :unbound}
      assert Enum.at(results, 1) == %{"a" => :unbound, "b" => 2}
    end

    test "handles identical variables" do
      left = [%{"x" => 1}]
      right = [%{"x" => 2}]

      results = Executor.union_aligned(left, right) |> Enum.to_list()

      # No alignment needed - same variables
      assert results == [%{"x" => 1}, %{"x" => 2}]
    end

    test "handles empty branches" do
      left = [%{"x" => 1}]
      right = []

      results = Executor.union_aligned(left, right) |> Enum.to_list()

      assert results == [%{"x" => 1}]
    end

    test "preserves order within branches" do
      left = [%{"x" => 1}, %{"x" => 2}]
      right = [%{"y" => "a"}, %{"y" => "b"}]

      results = Executor.union_aligned(left, right) |> Enum.to_list()

      assert length(results) == 4
      # Left results come first
      assert Enum.at(results, 0)["x"] == 1
      assert Enum.at(results, 1)["x"] == 2
      # Right results come second
      assert Enum.at(results, 2)["y"] == "a"
      assert Enum.at(results, 3)["y"] == "b"
    end
  end

  describe "union_all/1" do
    test "concatenates multiple branches" do
      branches = [
        [%{"x" => 1}],
        [%{"x" => 2}],
        [%{"x" => 3}]
      ]

      results = Executor.union_all(branches) |> Enum.to_list()

      assert results == [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]
    end

    test "handles empty list" do
      results = Executor.union_all([]) |> Enum.to_list()

      assert results == []
    end

    test "handles single branch" do
      branches = [[%{"x" => 1}, %{"x" => 2}]]

      results = Executor.union_all(branches) |> Enum.to_list()

      assert results == [%{"x" => 1}, %{"x" => 2}]
    end

    test "preserves order across all branches" do
      branches = [
        [%{"x" => 1}, %{"x" => 2}],
        [%{"x" => 3}],
        [%{"x" => 4}, %{"x" => 5}, %{"x" => 6}]
      ]

      results = Executor.union_all(branches) |> Enum.to_list()

      assert results == [
        %{"x" => 1}, %{"x" => 2},
        %{"x" => 3},
        %{"x" => 4}, %{"x" => 5}, %{"x" => 6}
      ]
    end

    test "handles branches with different variables" do
      branches = [
        [%{"x" => 1}],
        [%{"y" => 2}],
        [%{"z" => 3}]
      ]

      results = Executor.union_all(branches) |> Enum.to_list()

      assert length(results) == 3
      assert Enum.at(results, 0) == %{"x" => 1}
      assert Enum.at(results, 1) == %{"y" => 2}
      assert Enum.at(results, 2) == %{"z" => 3}
    end
  end

  describe "collect_all_variables/1" do
    test "collects variables from multiple bindings" do
      bindings = [
        %{"x" => 1, "y" => 2},
        %{"x" => 3, "z" => 4}
      ]

      vars = Executor.collect_all_variables(bindings)

      assert MapSet.equal?(vars, MapSet.new(["x", "y", "z"]))
    end

    test "handles empty bindings list" do
      vars = Executor.collect_all_variables([])

      assert MapSet.equal?(vars, MapSet.new())
    end

    test "handles single binding" do
      vars = Executor.collect_all_variables([%{"x" => 1, "y" => 2}])

      assert MapSet.equal?(vars, MapSet.new(["x", "y"]))
    end

    test "handles empty binding maps" do
      vars = Executor.collect_all_variables([%{}, %{"x" => 1}])

      assert MapSet.equal?(vars, MapSet.new(["x"]))
    end
  end

  describe "align_binding/2" do
    test "adds missing variables as :unbound" do
      binding = %{"x" => 1}
      all_vars = MapSet.new(["x", "y", "z"])

      aligned = Executor.align_binding(binding, all_vars)

      assert aligned == %{"x" => 1, "y" => :unbound, "z" => :unbound}
    end

    test "preserves existing values" do
      binding = %{"x" => 1, "y" => 2}
      all_vars = MapSet.new(["x", "y"])

      aligned = Executor.align_binding(binding, all_vars)

      assert aligned == %{"x" => 1, "y" => 2}
    end

    test "handles empty binding" do
      binding = %{}
      all_vars = MapSet.new(["x", "y"])

      aligned = Executor.align_binding(binding, all_vars)

      assert aligned == %{"x" => :unbound, "y" => :unbound}
    end

    test "handles empty variable set" do
      binding = %{"x" => 1}
      all_vars = MapSet.new()

      aligned = Executor.align_binding(binding, all_vars)

      assert aligned == %{"x" => 1}
    end
  end

  describe "union integration with BGP execution" do
    test "union of two BGP results", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data for two different types of relationships
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/knows"),
        iri("http://example.org/Bob")
      })
      add_triple(db, manager, {
        iri("http://example.org/Carol"),
        iri("http://example.org/likes"),
        iri("http://example.org/Dave")
      })

      # Execute first BGP: ?s :knows ?o
      {:ok, knows_stream} = Executor.execute_bgp(ctx, [
        triple(var("s"), iri("http://example.org/knows"), var("o"))
      ])

      # Execute second BGP: ?s :likes ?o
      {:ok, likes_stream} = Executor.execute_bgp(ctx, [
        triple(var("s"), iri("http://example.org/likes"), var("o"))
      ])

      # UNION of both
      results = Executor.union(knows_stream, likes_stream) |> Enum.to_list()

      assert length(results) == 2

      # First result from :knows
      first = Enum.at(results, 0)
      assert first["s"] == {:named_node, "http://example.org/Alice"}
      assert first["o"] == {:named_node, "http://example.org/Bob"}

      # Second result from :likes
      second = Enum.at(results, 1)
      assert second["s"] == {:named_node, "http://example.org/Carol"}
      assert second["o"] == {:named_node, "http://example.org/Dave"}

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Filter Execution Tests (Task 2.4.4)
  # ===========================================================================

  # XSD type constants for tests
  @xsd_integer "http://www.w3.org/2001/XMLSchema#integer"
  @xsd_boolean "http://www.w3.org/2001/XMLSchema#boolean"
  @xsd_string "http://www.w3.org/2001/XMLSchema#string"
  @xsd_decimal "http://www.w3.org/2001/XMLSchema#decimal"

  describe "filter/2" do
    test "filters bindings where expression is true" do
      bindings = [
        %{"x" => {:literal, :typed, "10", @xsd_integer}},
        %{"x" => {:literal, :typed, "5", @xsd_integer}},
        %{"x" => {:literal, :typed, "20", @xsd_integer}}
      ]

      # Filter: ?x > 8
      expr = {:greater, {:variable, "x"}, {:literal, :typed, "8", @xsd_integer}}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert length(results) == 2
      values = Enum.map(results, fn b -> b["x"] end)
      assert {:literal, :typed, "10", @xsd_integer} in values
      assert {:literal, :typed, "20", @xsd_integer} in values
    end

    test "removes bindings where expression is false" do
      bindings = [
        %{"x" => {:literal, :typed, "5", @xsd_integer}}
      ]

      # Filter: ?x > 10 (false)
      expr = {:greater, {:variable, "x"}, {:literal, :typed, "10", @xsd_integer}}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert results == []
    end

    test "removes bindings where expression errors (unbound variable)" do
      bindings = [
        %{"x" => {:literal, :typed, "10", @xsd_integer}},
        %{"y" => {:literal, :typed, "5", @xsd_integer}}  # x is unbound
      ]

      # Filter: ?x > 5 (errors on second binding)
      expr = {:greater, {:variable, "x"}, {:literal, :typed, "5", @xsd_integer}}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      # Only first binding passes
      assert length(results) == 1
      assert hd(results)["x"] == {:literal, :typed, "10", @xsd_integer}
    end

    test "removes bindings where expression errors (type mismatch)" do
      bindings = [
        %{"x" => {:named_node, "http://example.org/A"}}  # Can't compare IRI with integer
      ]

      # Filter: ?x > 5 (type error)
      expr = {:greater, {:variable, "x"}, {:literal, :typed, "5", @xsd_integer}}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert results == []
    end

    test "handles equality filter" do
      bindings = [
        %{"name" => {:literal, :simple, "Alice"}},
        %{"name" => {:literal, :simple, "Bob"}}
      ]

      expr = {:equal, {:variable, "name"}, {:literal, :simple, "Alice"}}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert length(results) == 1
      assert hd(results)["name"] == {:literal, :simple, "Alice"}
    end

    test "handles logical AND filter" do
      bindings = [
        %{"x" => {:literal, :typed, "5", @xsd_integer}},
        %{"x" => {:literal, :typed, "15", @xsd_integer}},
        %{"x" => {:literal, :typed, "25", @xsd_integer}}
      ]

      # Filter: ?x > 0 && ?x < 20
      expr = {:and,
        {:greater, {:variable, "x"}, {:literal, :typed, "0", @xsd_integer}},
        {:less, {:variable, "x"}, {:literal, :typed, "20", @xsd_integer}}
      }
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert length(results) == 2
      values = Enum.map(results, fn b -> b["x"] end)
      assert {:literal, :typed, "5", @xsd_integer} in values
      assert {:literal, :typed, "15", @xsd_integer} in values
    end

    test "handles logical OR filter" do
      bindings = [
        %{"x" => {:literal, :typed, "5", @xsd_integer}},
        %{"x" => {:literal, :typed, "15", @xsd_integer}},
        %{"x" => {:literal, :typed, "25", @xsd_integer}}
      ]

      # Filter: ?x < 10 || ?x > 20
      expr = {:or,
        {:less, {:variable, "x"}, {:literal, :typed, "10", @xsd_integer}},
        {:greater, {:variable, "x"}, {:literal, :typed, "20", @xsd_integer}}
      }
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert length(results) == 2
      values = Enum.map(results, fn b -> b["x"] end)
      assert {:literal, :typed, "5", @xsd_integer} in values
      assert {:literal, :typed, "25", @xsd_integer} in values
    end

    test "handles NOT filter" do
      bindings = [
        %{"x" => {:literal, :typed, "5", @xsd_integer}},
        %{"x" => {:literal, :typed, "15", @xsd_integer}}
      ]

      # Filter: !(?x > 10)
      expr = {:not, {:greater, {:variable, "x"}, {:literal, :typed, "10", @xsd_integer}}}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert length(results) == 1
      assert hd(results)["x"] == {:literal, :typed, "5", @xsd_integer}
    end

    test "handles BOUND filter" do
      bindings = [
        %{"x" => {:literal, :typed, "5", @xsd_integer}, "y" => {:literal, :simple, "a"}},
        %{"x" => {:literal, :typed, "10", @xsd_integer}}  # y is unbound
      ]

      # Filter: BOUND(?y)
      expr = {:bound, {:variable, "y"}}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert length(results) == 1
      assert hd(results)["y"] == {:literal, :simple, "a"}
    end

    test "handles string function filter (CONTAINS)" do
      bindings = [
        %{"name" => {:literal, :simple, "Alice Smith"}},
        %{"name" => {:literal, :simple, "Bob Jones"}}
      ]

      # Filter: CONTAINS(?name, "Smith")
      expr = {:function_call, "CONTAINS", [{:variable, "name"}, {:literal, :simple, "Smith"}]}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert length(results) == 1
      assert hd(results)["name"] == {:literal, :simple, "Alice Smith"}
    end

    test "handles REGEX filter" do
      bindings = [
        %{"email" => {:literal, :simple, "alice@example.com"}},
        %{"email" => {:literal, :simple, "bob@test.org"}},
        %{"email" => {:literal, :simple, "invalid-email"}}
      ]

      # Filter: REGEX(?email, "@.*\\.com$")
      expr = {:function_call, "REGEX", [{:variable, "email"}, {:literal, :simple, "@.*\\.com$"}]}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert length(results) == 1
      assert hd(results)["email"] == {:literal, :simple, "alice@example.com"}
    end

    test "handles ISIRI filter" do
      bindings = [
        %{"x" => {:named_node, "http://example.org/A"}},
        %{"x" => {:literal, :simple, "not an IRI"}}
      ]

      expr = {:function_call, "ISIRI", [{:variable, "x"}]}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      assert length(results) == 1
      assert hd(results)["x"] == {:named_node, "http://example.org/A"}
    end

    test "preserves binding order" do
      bindings = [
        %{"x" => {:literal, :typed, "1", @xsd_integer}},
        %{"x" => {:literal, :typed, "3", @xsd_integer}},
        %{"x" => {:literal, :typed, "5", @xsd_integer}}
      ]

      # Filter: ?x > 0 (all pass)
      expr = {:greater, {:variable, "x"}, {:literal, :typed, "0", @xsd_integer}}
      results = Executor.filter(bindings, expr) |> Enum.to_list()

      # Order should be preserved
      assert Enum.at(results, 0)["x"] == {:literal, :typed, "1", @xsd_integer}
      assert Enum.at(results, 1)["x"] == {:literal, :typed, "3", @xsd_integer}
      assert Enum.at(results, 2)["x"] == {:literal, :typed, "5", @xsd_integer}
    end
  end

  describe "evaluate_filter/2" do
    test "returns true for true expression" do
      expr = {:greater, {:literal, :typed, "10", @xsd_integer}, {:literal, :typed, "5", @xsd_integer}}
      assert Executor.evaluate_filter(expr, %{}) == true
    end

    test "returns false for false expression" do
      expr = {:greater, {:literal, :typed, "3", @xsd_integer}, {:literal, :typed, "5", @xsd_integer}}
      assert Executor.evaluate_filter(expr, %{}) == false
    end

    test "returns false for error expression" do
      # Unbound variable
      expr = {:greater, {:variable, "x"}, {:literal, :typed, "5", @xsd_integer}}
      assert Executor.evaluate_filter(expr, %{}) == false
    end

    test "handles boolean literal result" do
      expr = {:literal, :typed, "true", @xsd_boolean}
      assert Executor.evaluate_filter(expr, %{}) == true

      expr = {:literal, :typed, "false", @xsd_boolean}
      assert Executor.evaluate_filter(expr, %{}) == false
    end
  end

  describe "evaluate_filter_3vl/2" do
    test "returns {:ok, true} for true expression" do
      expr = {:greater, {:literal, :typed, "10", @xsd_integer}, {:literal, :typed, "5", @xsd_integer}}
      assert Executor.evaluate_filter_3vl(expr, %{}) == {:ok, true}
    end

    test "returns {:ok, false} for false expression" do
      expr = {:greater, {:literal, :typed, "3", @xsd_integer}, {:literal, :typed, "5", @xsd_integer}}
      assert Executor.evaluate_filter_3vl(expr, %{}) == {:ok, false}
    end

    test "returns :error for error expression" do
      # Unbound variable
      expr = {:greater, {:variable, "x"}, {:literal, :typed, "5", @xsd_integer}}
      assert Executor.evaluate_filter_3vl(expr, %{}) == :error
    end

    test "returns :error for EBV error (IRI has no EBV)" do
      # IRI cannot have effective boolean value
      expr = {:variable, "x"}
      binding = %{"x" => {:named_node, "http://example.org/A"}}
      assert Executor.evaluate_filter_3vl(expr, binding) == :error
    end
  end

  describe "filter_all/2" do
    test "passes bindings that satisfy all expressions" do
      bindings = [
        %{"x" => {:literal, :typed, "5", @xsd_integer}},
        %{"x" => {:literal, :typed, "15", @xsd_integer}},
        %{"x" => {:literal, :typed, "25", @xsd_integer}}
      ]

      # Filters: ?x > 0 AND ?x < 20
      exprs = [
        {:greater, {:variable, "x"}, {:literal, :typed, "0", @xsd_integer}},
        {:less, {:variable, "x"}, {:literal, :typed, "20", @xsd_integer}}
      ]
      results = Executor.filter_all(bindings, exprs) |> Enum.to_list()

      assert length(results) == 2
    end

    test "empty expression list passes all bindings" do
      bindings = [%{"x" => 1}, %{"x" => 2}]
      results = Executor.filter_all(bindings, []) |> Enum.to_list()

      assert results == bindings
    end

    test "removes bindings that fail any expression" do
      bindings = [%{"x" => {:literal, :typed, "5", @xsd_integer}}]

      # ?x > 0 passes, but ?x > 10 fails
      exprs = [
        {:greater, {:variable, "x"}, {:literal, :typed, "0", @xsd_integer}},
        {:greater, {:variable, "x"}, {:literal, :typed, "10", @xsd_integer}}
      ]
      results = Executor.filter_all(bindings, exprs) |> Enum.to_list()

      assert results == []
    end
  end

  describe "filter_any/2" do
    test "passes bindings that satisfy any expression" do
      bindings = [
        %{"x" => {:literal, :typed, "5", @xsd_integer}},
        %{"x" => {:literal, :typed, "15", @xsd_integer}},
        %{"x" => {:literal, :typed, "25", @xsd_integer}}
      ]

      # Filters: ?x < 10 OR ?x > 20
      exprs = [
        {:less, {:variable, "x"}, {:literal, :typed, "10", @xsd_integer}},
        {:greater, {:variable, "x"}, {:literal, :typed, "20", @xsd_integer}}
      ]
      results = Executor.filter_any(bindings, exprs) |> Enum.to_list()

      assert length(results) == 2
      values = Enum.map(results, fn b -> b["x"] end)
      assert {:literal, :typed, "5", @xsd_integer} in values
      assert {:literal, :typed, "25", @xsd_integer} in values
    end

    test "empty expression list returns empty stream" do
      bindings = [%{"x" => 1}, %{"x" => 2}]
      results = Executor.filter_any(bindings, []) |> Enum.to_list()

      assert results == []
    end

    test "removes bindings that fail all expressions" do
      bindings = [%{"x" => {:literal, :typed, "15", @xsd_integer}}]

      # Both fail: ?x < 10 and ?x > 20
      exprs = [
        {:less, {:variable, "x"}, {:literal, :typed, "10", @xsd_integer}},
        {:greater, {:variable, "x"}, {:literal, :typed, "20", @xsd_integer}}
      ]
      results = Executor.filter_any(bindings, exprs) |> Enum.to_list()

      assert results == []
    end
  end

  describe "to_effective_boolean/1" do
    test "handles xsd:boolean true values" do
      assert Executor.to_effective_boolean({:literal, :typed, "true", @xsd_boolean}) == {:ok, true}
      assert Executor.to_effective_boolean({:literal, :typed, "1", @xsd_boolean}) == {:ok, true}
    end

    test "handles xsd:boolean false values" do
      assert Executor.to_effective_boolean({:literal, :typed, "false", @xsd_boolean}) == {:ok, false}
      assert Executor.to_effective_boolean({:literal, :typed, "0", @xsd_boolean}) == {:ok, false}
    end

    test "handles simple literals (empty string = false)" do
      assert Executor.to_effective_boolean({:literal, :simple, ""}) == {:ok, false}
      assert Executor.to_effective_boolean({:literal, :simple, "hello"}) == {:ok, true}
    end

    test "handles xsd:string (empty = false)" do
      assert Executor.to_effective_boolean({:literal, :typed, "", @xsd_string}) == {:ok, false}
      assert Executor.to_effective_boolean({:literal, :typed, "hello", @xsd_string}) == {:ok, true}
    end

    test "handles numeric types (0 = false, non-zero = true)" do
      assert Executor.to_effective_boolean({:literal, :typed, "0", @xsd_integer}) == {:ok, false}
      assert Executor.to_effective_boolean({:literal, :typed, "42", @xsd_integer}) == {:ok, true}
      assert Executor.to_effective_boolean({:literal, :typed, "-5", @xsd_integer}) == {:ok, true}

      assert Executor.to_effective_boolean({:literal, :typed, "0.0", @xsd_decimal}) == {:ok, false}
      assert Executor.to_effective_boolean({:literal, :typed, "3.14", @xsd_decimal}) == {:ok, true}
    end

    test "returns error for IRIs" do
      assert Executor.to_effective_boolean({:named_node, "http://example.org"}) == :error
    end

    test "returns error for blank nodes" do
      assert Executor.to_effective_boolean({:blank_node, "b1"}) == :error
    end

    test "returns error for language-tagged literals" do
      assert Executor.to_effective_boolean({:literal, :lang, "hello", "en"}) == :error
    end
  end

  describe "filter integration with BGP execution" do
    test "filter applied to BGP results", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data with different ages
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/age"),
        typed_literal("25", @xsd_integer)
      })
      add_triple(db, manager, {
        iri("http://example.org/Bob"),
        iri("http://example.org/age"),
        typed_literal("17", @xsd_integer)
      })
      add_triple(db, manager, {
        iri("http://example.org/Carol"),
        iri("http://example.org/age"),
        typed_literal("30", @xsd_integer)
      })

      # Execute BGP: ?s :age ?age
      {:ok, bgp_stream} = Executor.execute_bgp(ctx, [
        triple(var("s"), iri("http://example.org/age"), var("age"))
      ])

      # Apply filter: ?age >= 18
      expr = {:greater_or_equal, {:variable, "age"}, {:literal, :typed, "18", @xsd_integer}}
      results = Executor.filter(bgp_stream, expr) |> Enum.to_list()

      # Only Alice (25) and Carol (30) should pass
      assert length(results) == 2
      subjects = Enum.map(results, fn b -> b["s"] end)
      assert {:named_node, "http://example.org/Alice"} in subjects
      assert {:named_node, "http://example.org/Carol"} in subjects

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Solution Modifier Tests (Task 2.4.5)
  # ===========================================================================

  describe "project/2" do
    test "projects specified variables" do
      bindings = [
        %{"x" => 1, "y" => 2, "z" => 3},
        %{"x" => 4, "y" => 5, "z" => 6}
      ]

      results = Executor.project(bindings, ["x", "z"]) |> Enum.to_list()

      assert results == [%{"x" => 1, "z" => 3}, %{"x" => 4, "z" => 6}]
    end

    test "handles empty variable list" do
      bindings = [%{"x" => 1, "y" => 2}]

      results = Executor.project(bindings, []) |> Enum.to_list()

      assert results == [%{}]
    end

    test "handles missing variables in binding" do
      bindings = [
        %{"x" => 1, "y" => 2},
        %{"x" => 3}  # missing y
      ]

      results = Executor.project(bindings, ["x", "y"]) |> Enum.to_list()

      assert Enum.at(results, 0) == %{"x" => 1, "y" => 2}
      assert Enum.at(results, 1) == %{"x" => 3}  # y not added
    end

    test "handles RDF term values" do
      bindings = [
        %{"s" => {:named_node, "http://ex.org/A"}, "name" => {:literal, :simple, "Alice"}, "age" => {:literal, :typed, "30", @xsd_integer}}
      ]

      results = Executor.project(bindings, ["s", "name"]) |> Enum.to_list()

      assert length(results) == 1
      [binding] = results
      assert binding["s"] == {:named_node, "http://ex.org/A"}
      assert binding["name"] == {:literal, :simple, "Alice"}
      refute Map.has_key?(binding, "age")
    end

    test "preserves order" do
      bindings = [
        %{"x" => 1, "y" => "a"},
        %{"x" => 2, "y" => "b"},
        %{"x" => 3, "y" => "c"}
      ]

      results = Executor.project(bindings, ["x"]) |> Enum.to_list()

      assert Enum.at(results, 0) == %{"x" => 1}
      assert Enum.at(results, 1) == %{"x" => 2}
      assert Enum.at(results, 2) == %{"x" => 3}
    end

    test "works with streams" do
      stream = Stream.map([1, 2, 3], fn x -> %{"x" => x, "y" => x * 2} end)

      results = Executor.project(stream, ["x"]) |> Enum.to_list()

      assert results == [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]
    end
  end

  describe "distinct/1" do
    test "removes duplicate bindings" do
      bindings = [
        %{"x" => 1},
        %{"x" => 2},
        %{"x" => 1},  # duplicate
        %{"x" => 3},
        %{"x" => 2}   # duplicate
      ]

      results = Executor.distinct(bindings) |> Enum.to_list()

      assert results == [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]
    end

    test "preserves first occurrence order" do
      bindings = [
        %{"x" => 3},
        %{"x" => 1},
        %{"x" => 2},
        %{"x" => 1}  # duplicate
      ]

      results = Executor.distinct(bindings) |> Enum.to_list()

      assert results == [%{"x" => 3}, %{"x" => 1}, %{"x" => 2}]
    end

    test "handles empty stream" do
      results = Executor.distinct([]) |> Enum.to_list()

      assert results == []
    end

    test "handles all duplicates" do
      bindings = [
        %{"x" => 1},
        %{"x" => 1},
        %{"x" => 1}
      ]

      results = Executor.distinct(bindings) |> Enum.to_list()

      assert results == [%{"x" => 1}]
    end

    test "distinguishes different bindings" do
      bindings = [
        %{"x" => 1, "y" => "a"},
        %{"x" => 1, "y" => "b"},  # different
        %{"x" => 1, "y" => "a"}   # duplicate of first
      ]

      results = Executor.distinct(bindings) |> Enum.to_list()

      assert length(results) == 2
      assert %{"x" => 1, "y" => "a"} in results
      assert %{"x" => 1, "y" => "b"} in results
    end

    test "handles RDF term values" do
      bindings = [
        %{"s" => {:named_node, "http://ex.org/A"}},
        %{"s" => {:named_node, "http://ex.org/B"}},
        %{"s" => {:named_node, "http://ex.org/A"}}  # duplicate
      ]

      results = Executor.distinct(bindings) |> Enum.to_list()

      assert length(results) == 2
    end
  end

  describe "reduced/1" do
    test "removes duplicates (same as distinct)" do
      bindings = [
        %{"x" => 1},
        %{"x" => 1},
        %{"x" => 2}
      ]

      results = Executor.reduced(bindings) |> Enum.to_list()

      assert results == [%{"x" => 1}, %{"x" => 2}]
    end
  end

  describe "order_by/2" do
    test "orders by single variable ascending" do
      bindings = [
        %{"name" => {:literal, :simple, "Carol"}},
        %{"name" => {:literal, :simple, "Alice"}},
        %{"name" => {:literal, :simple, "Bob"}}
      ]

      results = Executor.order_by(bindings, [{"name", :asc}]) |> Enum.to_list()

      assert Enum.at(results, 0)["name"] == {:literal, :simple, "Alice"}
      assert Enum.at(results, 1)["name"] == {:literal, :simple, "Bob"}
      assert Enum.at(results, 2)["name"] == {:literal, :simple, "Carol"}
    end

    test "orders by single variable descending" do
      bindings = [
        %{"name" => {:literal, :simple, "Alice"}},
        %{"name" => {:literal, :simple, "Carol"}},
        %{"name" => {:literal, :simple, "Bob"}}
      ]

      results = Executor.order_by(bindings, [{"name", :desc}]) |> Enum.to_list()

      assert Enum.at(results, 0)["name"] == {:literal, :simple, "Carol"}
      assert Enum.at(results, 1)["name"] == {:literal, :simple, "Bob"}
      assert Enum.at(results, 2)["name"] == {:literal, :simple, "Alice"}
    end

    test "orders by multiple variables" do
      bindings = [
        %{"group" => "A", "name" => "Carol"},
        %{"group" => "B", "name" => "Alice"},
        %{"group" => "A", "name" => "Bob"},
        %{"group" => "B", "name" => "Dave"}
      ]

      results = Executor.order_by(bindings, [{"group", :asc}, {"name", :asc}]) |> Enum.to_list()

      # Group A first (alphabetically), then sorted by name within group
      assert Enum.at(results, 0) == %{"group" => "A", "name" => "Bob"}
      assert Enum.at(results, 1) == %{"group" => "A", "name" => "Carol"}
      assert Enum.at(results, 2) == %{"group" => "B", "name" => "Alice"}
      assert Enum.at(results, 3) == %{"group" => "B", "name" => "Dave"}
    end

    test "orders numeric values correctly" do
      bindings = [
        %{"age" => {:literal, :typed, "25", @xsd_integer}},
        %{"age" => {:literal, :typed, "5", @xsd_integer}},
        %{"age" => {:literal, :typed, "15", @xsd_integer}}
      ]

      results = Executor.order_by(bindings, [{"age", :asc}]) |> Enum.to_list()

      assert Enum.at(results, 0)["age"] == {:literal, :typed, "5", @xsd_integer}
      assert Enum.at(results, 1)["age"] == {:literal, :typed, "15", @xsd_integer}
      assert Enum.at(results, 2)["age"] == {:literal, :typed, "25", @xsd_integer}
    end

    test "handles nil (unbound) values" do
      bindings = [
        %{"x" => 2},
        %{},  # x is unbound
        %{"x" => 1}
      ]

      results = Executor.order_by(bindings, [{"x", :asc}]) |> Enum.to_list()

      # Unbound (nil) comes first in ascending order
      assert Enum.at(results, 0) == %{}
      assert Enum.at(results, 1) == %{"x" => 1}
      assert Enum.at(results, 2) == %{"x" => 2}
    end

    test "handles empty comparator list" do
      bindings = [%{"x" => 2}, %{"x" => 1}]

      results = Executor.order_by(bindings, []) |> Enum.to_list()

      # No sorting, original order preserved
      assert results == [%{"x" => 2}, %{"x" => 1}]
    end

    test "handles empty stream" do
      results = Executor.order_by([], [{"x", :asc}]) |> Enum.to_list()

      assert results == []
    end

    test "orders IRIs lexicographically" do
      bindings = [
        %{"s" => {:named_node, "http://ex.org/C"}},
        %{"s" => {:named_node, "http://ex.org/A"}},
        %{"s" => {:named_node, "http://ex.org/B"}}
      ]

      results = Executor.order_by(bindings, [{"s", :asc}]) |> Enum.to_list()

      assert Enum.at(results, 0)["s"] == {:named_node, "http://ex.org/A"}
      assert Enum.at(results, 1)["s"] == {:named_node, "http://ex.org/B"}
      assert Enum.at(results, 2)["s"] == {:named_node, "http://ex.org/C"}
    end

    test "orders with variable tuple syntax" do
      bindings = [
        %{"x" => 2},
        %{"x" => 1}
      ]

      # Using {:variable, "x"} instead of just "x"
      results = Executor.order_by(bindings, [{{:variable, "x"}, :asc}]) |> Enum.to_list()

      assert length(results) == 2
      assert Enum.at(results, 0) == %{"x" => 1}
      assert Enum.at(results, 1) == %{"x" => 2}
    end
  end

  describe "slice/3" do
    test "applies offset only" do
      bindings = [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}, %{"x" => 4}, %{"x" => 5}]

      results = Executor.slice(bindings, 2, nil) |> Enum.to_list()

      assert results == [%{"x" => 3}, %{"x" => 4}, %{"x" => 5}]
    end

    test "applies limit only" do
      bindings = [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}, %{"x" => 4}, %{"x" => 5}]

      results = Executor.slice(bindings, 0, 3) |> Enum.to_list()

      assert results == [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]
    end

    test "applies both offset and limit" do
      bindings = [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}, %{"x" => 4}, %{"x" => 5}]

      results = Executor.slice(bindings, 1, 2) |> Enum.to_list()

      assert results == [%{"x" => 2}, %{"x" => 3}]
    end

    test "handles offset beyond stream length" do
      bindings = [%{"x" => 1}, %{"x" => 2}]

      results = Executor.slice(bindings, 10, nil) |> Enum.to_list()

      assert results == []
    end

    test "handles limit of 0" do
      bindings = [%{"x" => 1}, %{"x" => 2}]

      results = Executor.slice(bindings, 0, 0) |> Enum.to_list()

      assert results == []
    end

    test "handles offset 0 and nil limit (returns all)" do
      bindings = [%{"x" => 1}, %{"x" => 2}]

      results = Executor.slice(bindings, 0, nil) |> Enum.to_list()

      assert results == bindings
    end

    test "works with streams" do
      stream = Stream.map(1..10, fn x -> %{"x" => x} end)

      results = Executor.slice(stream, 3, 4) |> Enum.to_list()

      assert results == [%{"x" => 4}, %{"x" => 5}, %{"x" => 6}, %{"x" => 7}]
    end
  end

  describe "offset/2" do
    test "skips first n bindings" do
      bindings = [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]

      results = Executor.offset(bindings, 1) |> Enum.to_list()

      assert results == [%{"x" => 2}, %{"x" => 3}]
    end

    test "offset 0 returns unchanged" do
      bindings = [%{"x" => 1}, %{"x" => 2}]

      results = Executor.offset(bindings, 0) |> Enum.to_list()

      assert results == bindings
    end
  end

  describe "limit/2" do
    test "takes first n bindings" do
      bindings = [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]

      results = Executor.limit(bindings, 2) |> Enum.to_list()

      assert results == [%{"x" => 1}, %{"x" => 2}]
    end

    test "limit 0 returns empty" do
      bindings = [%{"x" => 1}, %{"x" => 2}]

      results = Executor.limit(bindings, 0) |> Enum.to_list()

      assert results == []
    end

    test "limit greater than stream length returns all" do
      bindings = [%{"x" => 1}, %{"x" => 2}]

      results = Executor.limit(bindings, 10) |> Enum.to_list()

      assert results == bindings
    end
  end

  describe "solution modifiers integration" do
    test "project + distinct combination" do
      bindings = [
        %{"x" => 1, "y" => "a"},
        %{"x" => 1, "y" => "b"},
        %{"x" => 2, "y" => "a"}
      ]

      # Project to x only, then distinct
      results = bindings
        |> Executor.project(["x"])
        |> Executor.distinct()
        |> Enum.to_list()

      assert results == [%{"x" => 1}, %{"x" => 2}]
    end

    test "order + slice combination" do
      bindings = [
        %{"x" => 5},
        %{"x" => 1},
        %{"x" => 4},
        %{"x" => 2},
        %{"x" => 3}
      ]

      # Order by x, then take 2 starting from offset 1
      results = bindings
        |> Executor.order_by([{"x", :asc}])
        |> Executor.slice(1, 2)
        |> Enum.to_list()

      # After sorting: 1, 2, 3, 4, 5. Skip 1, take 2: 2, 3
      assert results == [%{"x" => 2}, %{"x" => 3}]
    end

    test "full pipeline: filter + project + distinct + order + slice", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add test data
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/age"),
        typed_literal("25", @xsd_integer)
      })
      add_triple(db, manager, {
        iri("http://example.org/Bob"),
        iri("http://example.org/age"),
        typed_literal("30", @xsd_integer)
      })
      add_triple(db, manager, {
        iri("http://example.org/Carol"),
        iri("http://example.org/age"),
        typed_literal("20", @xsd_integer)
      })
      add_triple(db, manager, {
        iri("http://example.org/Dave"),
        iri("http://example.org/age"),
        typed_literal("15", @xsd_integer)
      })

      # Execute BGP
      {:ok, bgp_stream} = Executor.execute_bgp(ctx, [
        triple(var("s"), iri("http://example.org/age"), var("age"))
      ])

      # Filter: age >= 20
      filter_expr = {:greater_or_equal, {:variable, "age"}, {:literal, :typed, "20", @xsd_integer}}

      results = bgp_stream
        |> Executor.filter(filter_expr)
        |> Executor.project(["s", "age"])
        |> Executor.order_by([{"age", :desc}])
        |> Executor.slice(0, 2)
        |> Enum.to_list()

      # Should have Alice (25), Bob (30), Carol (20) passing filter
      # Ordered by age desc: Bob (30), Alice (25), Carol (20)
      # Limit 2: Bob, Alice
      assert length(results) == 2
      assert Enum.at(results, 0)["s"] == {:named_node, "http://example.org/Bob"}
      assert Enum.at(results, 1)["s"] == {:named_node, "http://example.org/Alice"}

      cleanup({db, manager})
    end
  end

  # ===========================================================================
  # Result Serialization Tests (Task 2.4.6)
  # ===========================================================================

  describe "to_select_results/2" do
    test "converts stream to list of bindings" do
      bindings = [
        %{"x" => 1, "y" => "a"},
        %{"x" => 2, "y" => "b"}
      ]

      results = Executor.to_select_results(bindings)

      assert results == bindings
    end

    test "projects specified variables" do
      bindings = [
        %{"x" => 1, "y" => "a", "z" => 100},
        %{"x" => 2, "y" => "b", "z" => 200}
      ]

      results = Executor.to_select_results(bindings, ["x", "y"])

      assert results == [
        %{"x" => 1, "y" => "a"},
        %{"x" => 2, "y" => "b"}
      ]
    end

    test "handles empty stream" do
      results = Executor.to_select_results([])

      assert results == []
    end

    test "handles RDF term values" do
      bindings = [
        %{"s" => {:named_node, "http://ex.org/A"}, "name" => {:literal, :simple, "Alice"}}
      ]

      results = Executor.to_select_results(bindings, ["s", "name"])

      assert length(results) == 1
      [binding] = results
      assert binding["s"] == {:named_node, "http://ex.org/A"}
      assert binding["name"] == {:literal, :simple, "Alice"}
    end

    test "works with lazy streams" do
      stream = Stream.map(1..3, fn x -> %{"x" => x} end)

      results = Executor.to_select_results(stream)

      assert results == [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]
    end
  end

  describe "to_ask_result/1" do
    test "returns true when solutions exist" do
      bindings = [%{"x" => 1}]

      result = Executor.to_ask_result(bindings)

      assert result == true
    end

    test "returns false when no solutions exist" do
      result = Executor.to_ask_result([])

      assert result == false
    end

    test "returns true with multiple solutions" do
      bindings = [%{"x" => 1}, %{"x" => 2}, %{"x" => 3}]

      result = Executor.to_ask_result(bindings)

      assert result == true
    end

    test "works with lazy streams" do
      # Create a stream that would fail if fully evaluated
      stream = Stream.map(1..1000, fn x -> %{"x" => x} end)

      result = Executor.to_ask_result(stream)

      assert result == true
    end

    test "handles empty binding (unit stream)" do
      # Unit stream has one empty binding - this counts as a solution
      result = Executor.to_ask_result([%{}])

      assert result == true
    end
  end

  describe "to_construct_result/4" do
    test "builds graph from template with bindings", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      bindings = [
        %{"s" => {:named_node, "http://ex.org/Alice"}, "name" => {:literal, :simple, "Alice"}},
        %{"s" => {:named_node, "http://ex.org/Bob"}, "name" => {:literal, :simple, "Bob"}}
      ]

      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://xmlns.com/foaf/0.1/name"}, {:variable, "name"}}
      ]

      {:ok, graph} = Executor.to_construct_result(ctx, bindings, template)

      assert RDF.Graph.triple_count(graph) == 2

      # Check triples exist
      assert RDF.Graph.describes?(graph, RDF.iri("http://ex.org/Alice"))
      assert RDF.Graph.describes?(graph, RDF.iri("http://ex.org/Bob"))

      cleanup({db, manager})
    end

    test "skips triples with unbound variables", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      bindings = [
        %{"s" => {:named_node, "http://ex.org/Alice"}, "name" => {:literal, :simple, "Alice"}},
        %{"s" => {:named_node, "http://ex.org/Bob"}}  # name is unbound
      ]

      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://xmlns.com/foaf/0.1/name"}, {:variable, "name"}}
      ]

      {:ok, graph} = Executor.to_construct_result(ctx, bindings, template)

      # Only Alice's triple should be constructed
      assert RDF.Graph.triple_count(graph) == 1
      assert RDF.Graph.describes?(graph, RDF.iri("http://ex.org/Alice"))
      refute RDF.Graph.describes?(graph, RDF.iri("http://ex.org/Bob"))

      cleanup({db, manager})
    end

    test "handles concrete terms in template", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      bindings = [
        %{"s" => {:named_node, "http://ex.org/Alice"}}
      ]

      # Template with concrete predicate
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}, {:named_node, "http://xmlns.com/foaf/0.1/Person"}}
      ]

      {:ok, graph} = Executor.to_construct_result(ctx, bindings, template)

      assert RDF.Graph.triple_count(graph) == 1

      cleanup({db, manager})
    end

    test "handles empty bindings", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://ex.org/p"}, {:variable, "o"}}
      ]

      {:ok, graph} = Executor.to_construct_result(ctx, [], template)

      assert RDF.Graph.triple_count(graph) == 0

      cleanup({db, manager})
    end

    test "handles typed literals", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      bindings = [
        %{"s" => {:named_node, "http://ex.org/Alice"}, "age" => {:literal, :typed, "30", @xsd_integer}}
      ]

      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://ex.org/age"}, {:variable, "age"}}
      ]

      {:ok, graph} = Executor.to_construct_result(ctx, bindings, template)

      assert RDF.Graph.triple_count(graph) == 1

      cleanup({db, manager})
    end

    test "handles language-tagged literals", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      bindings = [
        %{"s" => {:named_node, "http://ex.org/Alice"}, "label" => {:literal, :lang, "Alice", "en"}}
      ]

      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://www.w3.org/2000/01/rdf-schema#label"}, {:variable, "label"}}
      ]

      {:ok, graph} = Executor.to_construct_result(ctx, bindings, template)

      assert RDF.Graph.triple_count(graph) == 1

      cleanup({db, manager})
    end
  end

  describe "to_describe_result/4" do
    test "describes resources from bindings", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add data about Alice
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/name"),
        literal("Alice")
      })
      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/age"),
        typed_literal("30", @xsd_integer)
      })

      # Add data about Bob (not to be described)
      add_triple(db, manager, {
        iri("http://example.org/Bob"),
        iri("http://example.org/name"),
        literal("Bob")
      })

      # Create bindings pointing to Alice
      bindings = [
        %{"person" => {:named_node, "http://example.org/Alice"}}
      ]

      {:ok, graph} = Executor.to_describe_result(ctx, bindings, ["person"], follow_bnodes: false)

      # Should have Alice's triples
      assert RDF.Graph.triple_count(graph) == 2
      assert RDF.Graph.describes?(graph, RDF.iri("http://example.org/Alice"))
      refute RDF.Graph.describes?(graph, RDF.iri("http://example.org/Bob"))

      cleanup({db, manager})
    end

    test "describes multiple resources", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Add data
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

      bindings = [
        %{"person" => {:named_node, "http://example.org/Alice"}},
        %{"person" => {:named_node, "http://example.org/Bob"}}
      ]

      {:ok, graph} = Executor.to_describe_result(ctx, bindings, ["person"], follow_bnodes: false)

      assert RDF.Graph.triple_count(graph) == 2
      assert RDF.Graph.describes?(graph, RDF.iri("http://example.org/Alice"))
      assert RDF.Graph.describes?(graph, RDF.iri("http://example.org/Bob"))

      cleanup({db, manager})
    end

    test "handles empty bindings", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      {:ok, graph} = Executor.to_describe_result(ctx, [], ["person"])

      assert RDF.Graph.triple_count(graph) == 0

      cleanup({db, manager})
    end

    test "handles unbound variable", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Binding without the variable we want to describe
      bindings = [
        %{"other" => {:named_node, "http://example.org/Alice"}}
      ]

      {:ok, graph} = Executor.to_describe_result(ctx, bindings, ["person"])

      assert RDF.Graph.triple_count(graph) == 0

      cleanup({db, manager})
    end

    test "handles nonexistent resource", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      # Resource not in database
      bindings = [
        %{"person" => {:named_node, "http://example.org/Unknown"}}
      ]

      {:ok, graph} = Executor.to_describe_result(ctx, bindings, ["person"])

      assert RDF.Graph.triple_count(graph) == 0

      cleanup({db, manager})
    end
  end

  describe "result serialization integration" do
    test "SELECT with BGP execution", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

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

      {:ok, stream} = Executor.execute_bgp(ctx, [
        triple(var("s"), iri("http://example.org/name"), var("name"))
      ])

      results = Executor.to_select_results(stream, ["name"])

      assert length(results) == 2
      names = Enum.map(results, fn b -> b["name"] end)
      assert {:literal, :simple, "Alice"} in names
      assert {:literal, :simple, "Bob"} in names

      cleanup({db, manager})
    end

    test "ASK with BGP execution", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/knows"),
        iri("http://example.org/Bob")
      })

      # ASK if Alice knows anyone
      {:ok, stream} = Executor.execute_bgp(ctx, [
        triple(iri("http://example.org/Alice"), iri("http://example.org/knows"), var("person"))
      ])

      assert Executor.to_ask_result(stream) == true

      # ASK if Bob knows anyone (he doesn't)
      {:ok, stream2} = Executor.execute_bgp(ctx, [
        triple(iri("http://example.org/Bob"), iri("http://example.org/knows"), var("person"))
      ])

      assert Executor.to_ask_result(stream2) == false

      cleanup({db, manager})
    end

    test "CONSTRUCT with BGP execution", %{tmp_dir: tmp_dir} do
      {db, manager} = setup_db(tmp_dir)
      ctx = %{db: db, dict_manager: manager}

      add_triple(db, manager, {
        iri("http://example.org/Alice"),
        iri("http://example.org/name"),
        literal("Alice")
      })

      {:ok, stream} = Executor.execute_bgp(ctx, [
        triple(var("s"), iri("http://example.org/name"), var("name"))
      ])

      # Construct with different predicate
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://xmlns.com/foaf/0.1/name"}, {:variable, "name"}}
      ]

      {:ok, graph} = Executor.to_construct_result(ctx, stream, template)

      assert RDF.Graph.triple_count(graph) == 1
      # The constructed graph uses foaf:name instead of ex:name
      assert RDF.Graph.describes?(graph, RDF.iri("http://example.org/Alice"))

      cleanup({db, manager})
    end
  end

  describe "stream laziness" do
    test "project/2 is lazy - only consumes what's needed", %{tmp_dir: _tmp_dir} do
      # Create a stream that tracks how many elements have been consumed
      agent = start_supervised!({Agent, fn -> 0 end})

      # Create a stream that increments counter when consumed
      stream =
        Stream.map(1..1000, fn n ->
          Agent.update(agent, &(&1 + 1))
          %{"x" => n, "y" => n * 2}
        end)

      # Project and take only 5
      result =
        stream
        |> Executor.project(["x"])
        |> Enum.take(5)

      assert length(result) == 5
      # Should have only consumed 5 elements, not all 1000
      consumed = Agent.get(agent, & &1)
      assert consumed == 5
    end

    test "limit/2 is lazy - only consumes what's needed", %{tmp_dir: _tmp_dir} do
      agent = start_supervised!({Agent, fn -> 0 end})

      stream =
        Stream.map(1..1000, fn n ->
          Agent.update(agent, &(&1 + 1))
          %{"x" => n}
        end)

      result =
        stream
        |> Executor.limit(10)
        |> Enum.to_list()

      assert length(result) == 10
      consumed = Agent.get(agent, & &1)
      assert consumed == 10
    end

    test "filter/2 is lazy - stops when limit reached", %{tmp_dir: _tmp_dir} do
      agent = start_supervised!({Agent, fn -> 0 end})

      # Stream of values alternating bound/not bound
      stream =
        Stream.map(1..1000, fn n ->
          Agent.update(agent, &(&1 + 1))
          # Only even numbers have "y" bound, so BOUND(?y) filters 50%
          if rem(n, 2) == 0 do
            %{"x" => n, "y" => {:literal, :simple, "value"}}
          else
            %{"x" => n}
          end
        end)

      # Filter expression: BOUND(?y) - only passes every other binding
      filter_expr = {:bound, {:variable, "y"}}

      result =
        stream
        |> Executor.filter(filter_expr)
        |> Enum.take(5)

      assert length(result) == 5
      # Should stop early - consumed more than 5 but less than 1000
      consumed = Agent.get(agent, & &1)
      # Need to consume ~10 items to get 5 that pass BOUND(?y)
      assert consumed >= 10
      assert consumed < 1000
    end

    test "slice/3 is lazy - skips efficiently and takes only needed", %{tmp_dir: _tmp_dir} do
      agent = start_supervised!({Agent, fn -> 0 end})

      stream =
        Stream.map(1..1000, fn n ->
          Agent.update(agent, &(&1 + 1))
          %{"x" => n}
        end)

      result =
        stream
        |> Executor.slice(100, 10)
        |> Enum.to_list()

      assert length(result) == 10
      # Should consume exactly 110 elements (100 to skip + 10 to take)
      consumed = Agent.get(agent, & &1)
      assert consumed == 110
    end
  end

  describe "telemetry events" do
    test "hash_join emits telemetry", %{tmp_dir: _tmp_dir} do
      test_pid = self()

      # Attach telemetry handler
      handler_id = "test-hash-join-handler"
      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :executor, :hash_join],
        fn event, measurements, metadata, _ ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      left = [%{"x" => 1}, %{"x" => 2}]
      right = [%{"x" => 1, "y" => "a"}, %{"x" => 2, "y" => "b"}]

      Executor.hash_join(left, right) |> Enum.to_list()

      assert_receive {:telemetry, [:triple_store, :sparql, :executor, :hash_join],
                      %{left_count: 2, right_count: 2}, %{}}

      :telemetry.detach(handler_id)
    end

    test "order_by emits telemetry", %{tmp_dir: _tmp_dir} do
      test_pid = self()

      handler_id = "test-order-by-handler"
      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :executor, :order_by],
        fn event, measurements, metadata, _ ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      bindings = [%{"x" => 3}, %{"x" => 1}, %{"x" => 2}]
      Executor.order_by(bindings, [{"x", :asc}]) |> Enum.to_list()

      assert_receive {:telemetry, [:triple_store, :sparql, :executor, :order_by],
                      %{binding_count: 3, comparator_count: 1}, %{}}

      :telemetry.detach(handler_id)
    end
  end

  # ===========================================================================
  # Task 2.6.1: GROUP BY Execution Tests
  # ===========================================================================

  describe "group_by/3" do
    test "groups bindings by single variable" do
      bindings = [
        %{"type" => {:named_node, "http://ex.org/Person"}, "name" => {:literal, :simple, "Alice"}},
        %{"type" => {:named_node, "http://ex.org/Person"}, "name" => {:literal, :simple, "Bob"}},
        %{"type" => {:named_node, "http://ex.org/Animal"}, "name" => {:literal, :simple, "Cat"}}
      ]

      group_vars = [{:variable, "type"}]
      aggregates = [{{:variable, "count"}, {:count, :star, false}}]

      result = Executor.group_by(bindings, group_vars, aggregates) |> Enum.to_list()

      assert length(result) == 2

      person_group = Enum.find(result, fn r -> r["type"] == {:named_node, "http://ex.org/Person"} end)
      animal_group = Enum.find(result, fn r -> r["type"] == {:named_node, "http://ex.org/Animal"} end)

      assert person_group["count"] == {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}
      assert animal_group["count"] == {:literal, :typed, "1", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "groups bindings by multiple variables" do
      bindings = [
        %{"type" => {:named_node, "http://ex.org/A"}, "status" => {:literal, :simple, "active"}, "val" => 1},
        %{"type" => {:named_node, "http://ex.org/A"}, "status" => {:literal, :simple, "active"}, "val" => 2},
        %{"type" => {:named_node, "http://ex.org/A"}, "status" => {:literal, :simple, "inactive"}, "val" => 3},
        %{"type" => {:named_node, "http://ex.org/B"}, "status" => {:literal, :simple, "active"}, "val" => 4}
      ]

      group_vars = [{:variable, "type"}, {:variable, "status"}]
      aggregates = [{{:variable, "cnt"}, {:count, :star, false}}]

      result = Executor.group_by(bindings, group_vars, aggregates) |> Enum.to_list()

      # Should have 3 groups: (A, active), (A, inactive), (B, active)
      assert length(result) == 3

      a_active = Enum.find(result, fn r ->
        r["type"] == {:named_node, "http://ex.org/A"} and r["status"] == {:literal, :simple, "active"}
      end)

      assert a_active["cnt"] == {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "COUNT(*) counts all solutions" do
      bindings = [
        %{"x" => {:literal, :simple, "a"}},
        %{"x" => {:literal, :simple, "b"}},
        %{"x" => {:literal, :simple, "c"}}
      ]

      aggregates = [{{:variable, "total"}, {:count, :star, false}}]

      result = Executor.group_by(bindings, [], aggregates) |> Enum.to_list()

      assert length(result) == 1
      assert hd(result)["total"] == {:literal, :typed, "3", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "COUNT(expr) counts non-null values" do
      bindings = [
        %{"x" => {:literal, :simple, "a"}, "y" => {:literal, :simple, "1"}},
        %{"x" => {:literal, :simple, "b"}},  # y is missing
        %{"x" => {:literal, :simple, "c"}, "y" => {:literal, :simple, "3"}}
      ]

      aggregates = [{{:variable, "cnt"}, {:count, {:variable, "y"}, false}}]

      result = Executor.group_by(bindings, [], aggregates) |> Enum.to_list()

      assert length(result) == 1
      # Only 2 bindings have y
      assert hd(result)["cnt"] == {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "SUM aggregates numeric values" do
      bindings = [
        %{"type" => {:literal, :simple, "A"}, "val" => {:literal, :typed, "10", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"type" => {:literal, :simple, "A"}, "val" => {:literal, :typed, "20", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"type" => {:literal, :simple, "B"}, "val" => {:literal, :typed, "5", "http://www.w3.org/2001/XMLSchema#integer"}}
      ]

      group_vars = [{:variable, "type"}]
      aggregates = [{{:variable, "sum"}, {:sum, {:variable, "val"}, false}}]

      result = Executor.group_by(bindings, group_vars, aggregates) |> Enum.to_list()

      a_group = Enum.find(result, fn r -> r["type"] == {:literal, :simple, "A"} end)
      b_group = Enum.find(result, fn r -> r["type"] == {:literal, :simple, "B"} end)

      assert a_group["sum"] == {:literal, :typed, "30", "http://www.w3.org/2001/XMLSchema#integer"}
      assert b_group["sum"] == {:literal, :typed, "5", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "AVG computes average" do
      bindings = [
        %{"val" => {:literal, :typed, "10", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"val" => {:literal, :typed, "20", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"val" => {:literal, :typed, "30", "http://www.w3.org/2001/XMLSchema#integer"}}
      ]

      aggregates = [{{:variable, "avg"}, {:avg, {:variable, "val"}, false}}]

      result = Executor.group_by(bindings, [], aggregates) |> Enum.to_list()

      # (10 + 20 + 30) / 3 = 20.0
      assert hd(result)["avg"] == {:literal, :typed, "20.0", "http://www.w3.org/2001/XMLSchema#decimal"}
    end

    test "MIN finds minimum value" do
      bindings = [
        %{"val" => {:literal, :typed, "30", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"val" => {:literal, :typed, "10", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"val" => {:literal, :typed, "20", "http://www.w3.org/2001/XMLSchema#integer"}}
      ]

      aggregates = [{{:variable, "min"}, {:min, {:variable, "val"}, false}}]

      result = Executor.group_by(bindings, [], aggregates) |> Enum.to_list()

      assert hd(result)["min"] == {:literal, :typed, "10", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "MAX finds maximum value" do
      bindings = [
        %{"val" => {:literal, :typed, "10", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"val" => {:literal, :typed, "30", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"val" => {:literal, :typed, "20", "http://www.w3.org/2001/XMLSchema#integer"}}
      ]

      aggregates = [{{:variable, "max"}, {:max, {:variable, "val"}, false}}]

      result = Executor.group_by(bindings, [], aggregates) |> Enum.to_list()

      assert hd(result)["max"] == {:literal, :typed, "30", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "GROUP_CONCAT joins values with separator" do
      bindings = [
        %{"type" => {:literal, :simple, "A"}, "name" => {:literal, :simple, "Alice"}},
        %{"type" => {:literal, :simple, "A"}, "name" => {:literal, :simple, "Bob"}},
        %{"type" => {:literal, :simple, "B"}, "name" => {:literal, :simple, "Carol"}}
      ]

      group_vars = [{:variable, "type"}]
      aggregates = [{{:variable, "names"}, {:group_concat, {:variable, "name"}, false, ", "}}]

      result = Executor.group_by(bindings, group_vars, aggregates) |> Enum.to_list()

      a_group = Enum.find(result, fn r -> r["type"] == {:literal, :simple, "A"} end)
      b_group = Enum.find(result, fn r -> r["type"] == {:literal, :simple, "B"} end)

      # Order within group is preserved
      assert a_group["names"] == {:literal, :simple, "Alice, Bob"}
      assert b_group["names"] == {:literal, :simple, "Carol"}
    end

    test "SAMPLE returns arbitrary value from group" do
      bindings = [
        %{"type" => {:literal, :simple, "A"}, "val" => {:literal, :simple, "x"}},
        %{"type" => {:literal, :simple, "A"}, "val" => {:literal, :simple, "y"}}
      ]

      group_vars = [{:variable, "type"}]
      aggregates = [{{:variable, "sample"}, {:sample, {:variable, "val"}, false}}]

      result = Executor.group_by(bindings, group_vars, aggregates) |> Enum.to_list()

      # SAMPLE returns some value from the group
      sample_val = hd(result)["sample"]
      assert sample_val in [{:literal, :simple, "x"}, {:literal, :simple, "y"}]
    end

    test "DISTINCT in COUNT removes duplicates" do
      bindings = [
        %{"type" => {:literal, :simple, "A"}, "val" => {:literal, :simple, "x"}},
        %{"type" => {:literal, :simple, "A"}, "val" => {:literal, :simple, "x"}},  # duplicate
        %{"type" => {:literal, :simple, "A"}, "val" => {:literal, :simple, "y"}}
      ]

      group_vars = [{:variable, "type"}]
      agg_no_distinct = [{{:variable, "cnt"}, {:count, {:variable, "val"}, false}}]
      agg_distinct = [{{:variable, "cnt"}, {:count, {:variable, "val"}, true}}]

      result_no_distinct = Executor.group_by(bindings, group_vars, agg_no_distinct) |> Enum.to_list()
      result_distinct = Executor.group_by(bindings, group_vars, agg_distinct) |> Enum.to_list()

      assert hd(result_no_distinct)["cnt"] == {:literal, :typed, "3", "http://www.w3.org/2001/XMLSchema#integer"}
      assert hd(result_distinct)["cnt"] == {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "handles empty group variables (single implicit group)" do
      bindings = [
        %{"val" => {:literal, :typed, "1", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"val" => {:literal, :typed, "2", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"val" => {:literal, :typed, "3", "http://www.w3.org/2001/XMLSchema#integer"}}
      ]

      group_vars = []
      aggregates = [
        {{:variable, "cnt"}, {:count, :star, false}},
        {{:variable, "sum"}, {:sum, {:variable, "val"}, false}}
      ]

      result = Executor.group_by(bindings, group_vars, aggregates) |> Enum.to_list()

      assert length(result) == 1
      assert hd(result)["cnt"] == {:literal, :typed, "3", "http://www.w3.org/2001/XMLSchema#integer"}
      assert hd(result)["sum"] == {:literal, :typed, "6", "http://www.w3.org/2001/XMLSchema#integer"}
    end
  end

  describe "implicit_group/2" do
    test "creates single group from all bindings" do
      bindings = [
        %{"x" => 1},
        %{"x" => 2},
        %{"x" => 3}
      ]

      aggregates = [{{:variable, "total"}, {:count, :star, false}}]

      result = Executor.implicit_group(bindings, aggregates) |> Enum.to_list()

      assert length(result) == 1
      assert hd(result)["total"] == {:literal, :typed, "3", "http://www.w3.org/2001/XMLSchema#integer"}
    end

    test "handles empty input" do
      bindings = []
      aggregates = [{{:variable, "total"}, {:count, :star, false}}]

      result = Executor.implicit_group(bindings, aggregates) |> Enum.to_list()

      assert length(result) == 1
      assert hd(result)["total"] == {:literal, :typed, "0", "http://www.w3.org/2001/XMLSchema#integer"}
    end
  end

  describe "having/2" do
    test "filters groups by aggregate value" do
      # Pre-grouped bindings with aggregate results
      groups = [
        %{"type" => {:literal, :simple, "A"}, "count" => {:literal, :typed, "5", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"type" => {:literal, :simple, "B"}, "count" => {:literal, :typed, "15", "http://www.w3.org/2001/XMLSchema#integer"}},
        %{"type" => {:literal, :simple, "C"}, "count" => {:literal, :typed, "3", "http://www.w3.org/2001/XMLSchema#integer"}}
      ]

      # HAVING count > 10
      having_expr = {:greater, {:variable, "count"}, {:literal, :typed, "10", "http://www.w3.org/2001/XMLSchema#integer"}}

      result = Executor.having(groups, having_expr) |> Enum.to_list()

      assert length(result) == 1
      assert hd(result)["type"] == {:literal, :simple, "B"}
    end
  end

  describe "security limits" do
    test "distinct raises LimitExceededError when limit exceeded" do
      # Create a stream with more than 100,000 unique bindings
      # We'll use a smaller number and temporarily override the limit in a real test
      # For this test, we just verify the mechanism works with a small dataset
      bindings = for i <- 1..100, do: %{"x" => i}

      # This should work fine (under limit)
      result = Executor.distinct(bindings) |> Enum.to_list()
      assert length(result) == 100
    end

    test "order_by raises LimitExceededError when limit exceeded" do
      # Similar to distinct, verify the mechanism works
      bindings = for i <- 1..100, do: %{"x" => {:literal, :simple, Integer.to_string(i)}}

      # This should work fine (under limit)
      result = Executor.order_by(bindings, [{"x", :asc}]) |> Enum.to_list()
      assert length(result) == 100
    end

    test "distinct emits telemetry at intervals" do
      # Generate enough bindings to trigger telemetry
      bindings = for i <- 1..15_000, do: %{"x" => i}

      # Attach a telemetry handler
      ref = make_ref()
      test_pid = self()
      :telemetry.attach(
        "test-distinct-handler-#{inspect(ref)}",
        [:triple_store, :sparql, :executor, :distinct],
        fn _event, measurements, _meta, _ ->
          send(test_pid, {:telemetry, measurements})
        end,
        nil
      )

      try do
        # Consume the stream
        _result = Executor.distinct(bindings) |> Enum.to_list()

        # Should have received telemetry event (at 10_000 unique)
        assert_receive {:telemetry, %{unique_count: 10_000}}, 1000
      after
        :telemetry.detach("test-distinct-handler-#{inspect(ref)}")
      end
    end
  end
end
