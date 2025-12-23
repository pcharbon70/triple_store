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
end
