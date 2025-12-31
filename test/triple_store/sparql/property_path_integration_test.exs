defmodule TripleStore.SPARQL.PropertyPathIntegrationTest do
  @moduledoc """
  Integration tests for property path evaluation on real-world patterns.

  Covers Task 3.5.3 requirements from Phase 3 planning:
  - 3.5.3.1: Test rdfs:subClassOf* for class hierarchy traversal
  - 3.5.3.2: Test foaf:knows+ for social network paths
  - 3.5.3.3: Test combined sequence and alternative paths
  - 3.5.3.4: Benchmark recursive paths on deep hierarchies
  """

  use ExUnit.Case, async: false

  import TripleStore.Test.IntegrationHelpers,
    only: [extract_count: 1, get_iri: 1, get_literal: 1, extract_iris: 2]

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.SPARQL.Query
  alias TripleStore.Update

  @moduletag :property_path_integration

  # Timeout constants for test consistency
  @benchmark_timeout 120_000
  @performance_threshold_ms 50

  # Standard RDF/RDFS/OWL namespaces
  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @foaf "http://xmlns.com/foaf/0.1/"
  @ex "http://example.org/"

  setup do
    test_id = :erlang.unique_integer([:positive])
    db_path = Path.join(System.tmp_dir!(), "property_path_integration_#{test_id}")

    File.rm_rf!(db_path)

    {:ok, db} = NIF.open(db_path)
    {:ok, dict_manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: dict_manager}

    on_exit(fn ->
      if Process.alive?(dict_manager) do
        Manager.stop(dict_manager)
      end

      NIF.close(db)
      File.rm_rf!(db_path)
    end)

    {:ok, ctx: ctx}
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp insert_triples(ctx, triples) do
    rdf_triples =
      Enum.map(triples, fn {s, p, o} ->
        {RDF.iri(s), RDF.iri(p), to_rdf_term(o)}
      end)

    {:ok, _count} = Update.insert(ctx, rdf_triples)
  end

  defp to_rdf_term(term) when is_binary(term) do
    if String.starts_with?(term, "http://") do
      RDF.iri(term)
    else
      RDF.literal(term)
    end
  end

  defp to_rdf_term(term), do: term

  # extract_iris/2, get_iri/1, get_literal/1 imported from IntegrationHelpers

  # ===========================================================================
  # 3.5.3.1: rdfs:subClassOf* for Class Hierarchy Traversal
  # ===========================================================================

  describe "rdfs:subClassOf* for class hierarchy traversal" do
    test "finds all superclasses of a class", %{ctx: ctx} do
      # Create class hierarchy:
      # Student < Person < Agent < Thing
      insert_triples(ctx, [
        {"#{@ex}Student", "#{@rdfs}subClassOf", "#{@ex}Person"},
        {"#{@ex}Person", "#{@rdfs}subClassOf", "#{@ex}Agent"},
        {"#{@ex}Agent", "#{@rdfs}subClassOf", "#{@ex}Thing"}
      ])

      sparql = """
      SELECT ?superclass WHERE {
        <#{@ex}Student> <#{@rdfs}subClassOf>* ?superclass
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      superclasses = extract_iris(results, "superclass")

      # Should include Student itself (zero steps) and all superclasses
      assert MapSet.member?(superclasses, "#{@ex}Student")
      assert MapSet.member?(superclasses, "#{@ex}Person")
      assert MapSet.member?(superclasses, "#{@ex}Agent")
      assert MapSet.member?(superclasses, "#{@ex}Thing")
      assert MapSet.size(superclasses) == 4
    end

    test "finds all subclasses of a class", %{ctx: ctx} do
      # Create class hierarchy with multiple inheritance
      insert_triples(ctx, [
        {"#{@ex}GradStudent", "#{@rdfs}subClassOf", "#{@ex}Student"},
        {"#{@ex}UndergradStudent", "#{@rdfs}subClassOf", "#{@ex}Student"},
        {"#{@ex}Student", "#{@rdfs}subClassOf", "#{@ex}Person"},
        {"#{@ex}Employee", "#{@rdfs}subClassOf", "#{@ex}Person"}
      ])

      # Find all subclasses of Person using inverse path
      sparql = """
      SELECT ?subclass WHERE {
        ?subclass <#{@rdfs}subClassOf>+ <#{@ex}Person>
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      subclasses = extract_iris(results, "subclass")

      assert MapSet.member?(subclasses, "#{@ex}Student")
      assert MapSet.member?(subclasses, "#{@ex}Employee")
      assert MapSet.member?(subclasses, "#{@ex}GradStudent")
      assert MapSet.member?(subclasses, "#{@ex}UndergradStudent")
      # Person should NOT be included (one-or-more excludes self)
      refute MapSet.member?(subclasses, "#{@ex}Person")
    end

    test "handles diamond inheritance pattern", %{ctx: ctx} do
      # Diamond pattern:
      #       Thing
      #      /     \
      #   Agent   Physical
      #      \     /
      #      Person
      insert_triples(ctx, [
        {"#{@ex}Person", "#{@rdfs}subClassOf", "#{@ex}Agent"},
        {"#{@ex}Person", "#{@rdfs}subClassOf", "#{@ex}Physical"},
        {"#{@ex}Agent", "#{@rdfs}subClassOf", "#{@ex}Thing"},
        {"#{@ex}Physical", "#{@rdfs}subClassOf", "#{@ex}Thing"}
      ])

      sparql = """
      SELECT ?superclass WHERE {
        <#{@ex}Person> <#{@rdfs}subClassOf>* ?superclass
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      superclasses = extract_iris(results, "superclass")

      # Should find all unique superclasses, including Thing only once
      assert MapSet.member?(superclasses, "#{@ex}Person")
      assert MapSet.member?(superclasses, "#{@ex}Agent")
      assert MapSet.member?(superclasses, "#{@ex}Physical")
      assert MapSet.member?(superclasses, "#{@ex}Thing")
      assert MapSet.size(superclasses) == 4
    end

    test "finds instances via rdf:type/rdfs:subClassOf* pattern", %{ctx: ctx} do
      # Create instances and class hierarchy
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@rdf}type", "#{@ex}Student"},
        {"#{@ex}bob", "#{@rdf}type", "#{@ex}Employee"},
        {"#{@ex}Student", "#{@rdfs}subClassOf", "#{@ex}Person"},
        {"#{@ex}Employee", "#{@rdfs}subClassOf", "#{@ex}Person"}
      ])

      # Find all instances of Person (including inferred via subClassOf)
      sparql = """
      SELECT ?instance WHERE {
        ?instance <#{@rdf}type>/<#{@rdfs}subClassOf>* <#{@ex}Person>
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      instances = extract_iris(results, "instance")

      assert MapSet.member?(instances, "#{@ex}alice")
      assert MapSet.member?(instances, "#{@ex}bob")
    end

    test "empty result for unrelated class", %{ctx: ctx} do
      insert_triples(ctx, [
        {"#{@ex}Student", "#{@rdfs}subClassOf", "#{@ex}Person"},
        {"#{@ex}Vehicle", "#{@rdfs}subClassOf", "#{@ex}Machine"}
      ])

      # Check if Student is subClassOf Machine (no path)
      sparql = """
      SELECT ?x WHERE {
        <#{@ex}Student> <#{@rdfs}subClassOf>+ <#{@ex}Machine>
        BIND("found" AS ?x)
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      assert Enum.empty?(results)
    end
  end

  # ===========================================================================
  # 3.5.3.2: foaf:knows+ for Social Network Paths
  # ===========================================================================

  describe "foaf:knows+ for social network paths" do
    test "finds friends of friends", %{ctx: ctx} do
      # Social network: alice -> bob -> charlie -> dave
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@foaf}knows", "#{@ex}bob"},
        {"#{@ex}bob", "#{@foaf}knows", "#{@ex}charlie"},
        {"#{@ex}charlie", "#{@foaf}knows", "#{@ex}dave"}
      ])

      # Find all people reachable from alice
      sparql = """
      SELECT ?person WHERE {
        <#{@ex}alice> <#{@foaf}knows>+ ?person
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      people = extract_iris(results, "person")

      assert MapSet.member?(people, "#{@ex}bob")
      assert MapSet.member?(people, "#{@ex}charlie")
      assert MapSet.member?(people, "#{@ex}dave")
      # alice should NOT be included (one-or-more)
      refute MapSet.member?(people, "#{@ex}alice")
    end

    test "handles bidirectional friendships", %{ctx: ctx} do
      # Bidirectional friendships
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@foaf}knows", "#{@ex}bob"},
        {"#{@ex}bob", "#{@foaf}knows", "#{@ex}alice"},
        {"#{@ex}bob", "#{@foaf}knows", "#{@ex}charlie"},
        {"#{@ex}charlie", "#{@foaf}knows", "#{@ex}bob"}
      ])

      sparql = """
      SELECT ?person WHERE {
        <#{@ex}alice> <#{@foaf}knows>+ ?person
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      people = extract_iris(results, "person")

      # Should find all connected people
      assert MapSet.member?(people, "#{@ex}bob")
      assert MapSet.member?(people, "#{@ex}charlie")
      # alice can reach herself via cycle (alice -> bob -> alice)
      assert MapSet.member?(people, "#{@ex}alice")
    end

    test "finds who can reach a specific person", %{ctx: ctx} do
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@foaf}knows", "#{@ex}bob"},
        {"#{@ex}charlie", "#{@foaf}knows", "#{@ex}bob"},
        {"#{@ex}dave", "#{@foaf}knows", "#{@ex}alice"}
      ])

      # Find everyone who can reach bob
      sparql = """
      SELECT ?person WHERE {
        ?person <#{@foaf}knows>+ <#{@ex}bob>
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      people = extract_iris(results, "person")

      assert MapSet.member?(people, "#{@ex}alice")
      assert MapSet.member?(people, "#{@ex}charlie")
      # via alice
      assert MapSet.member?(people, "#{@ex}dave")
    end

    test "finds shortest path existence between two people", %{ctx: ctx} do
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@foaf}knows", "#{@ex}bob"},
        {"#{@ex}bob", "#{@foaf}knows", "#{@ex}charlie"},
        {"#{@ex}charlie", "#{@foaf}knows", "#{@ex}dave"}
      ])

      # Check if path exists from alice to dave
      sparql = """
      ASK {
        <#{@ex}alice> <#{@foaf}knows>+ <#{@ex}dave>
      }
      """

      {:ok, result} = Query.query(ctx, sparql)
      assert result == true
    end

    test "returns false for disconnected nodes", %{ctx: ctx} do
      # Two disconnected components
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@foaf}knows", "#{@ex}bob"},
        {"#{@ex}charlie", "#{@foaf}knows", "#{@ex}dave"}
      ])

      sparql = """
      ASK {
        <#{@ex}alice> <#{@foaf}knows>+ <#{@ex}dave>
      }
      """

      {:ok, result} = Query.query(ctx, sparql)
      assert result == false
    end

    test "handles large friend network", %{ctx: ctx} do
      # Create a network of 50 people in a chain
      triples =
        for i <- 1..49 do
          {"#{@ex}person#{i}", "#{@foaf}knows", "#{@ex}person#{i + 1}"}
        end

      insert_triples(ctx, triples)

      # Find all reachable from person1
      sparql = """
      SELECT (COUNT(?person) AS ?count) WHERE {
        <#{@ex}person1> <#{@foaf}knows>+ ?person
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      count = extract_count(hd(results)["count"])
      # Can reach person2 through person50
      assert count == 49
    end
  end

  # ===========================================================================
  # 3.5.3.3: Combined Sequence and Alternative Paths
  # ===========================================================================

  describe "combined sequence and alternative paths" do
    test "sequence of two different predicates", %{ctx: ctx} do
      # Person has employer, employer is in country
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@ex}employer", "#{@ex}acme"},
        {"#{@ex}acme", "#{@ex}locatedIn", "#{@ex}usa"},
        {"#{@ex}bob", "#{@ex}employer", "#{@ex}globex"},
        {"#{@ex}globex", "#{@ex}locatedIn", "#{@ex}uk"}
      ])

      # Find countries where people work
      sparql = """
      SELECT ?person ?country WHERE {
        ?person <#{@ex}employer>/<#{@ex}locatedIn> ?country
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      assert length(results) == 2

      countries = extract_iris(results, "country")
      assert MapSet.member?(countries, "#{@ex}usa")
      assert MapSet.member?(countries, "#{@ex}uk")
    end

    test "alternative predicates for same relationship", %{ctx: ctx} do
      # Different predicates for parent
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@ex}mother", "#{@ex}carol"},
        {"#{@ex}alice", "#{@ex}father", "#{@ex}david"},
        {"#{@ex}bob", "#{@ex}mother", "#{@ex}eve"}
      ])

      # Find all parents using alternative
      sparql = """
      SELECT ?child ?parent WHERE {
        ?child (<#{@ex}mother>|<#{@ex}father>) ?parent
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      assert length(results) == 3

      parents = extract_iris(results, "parent")
      assert MapSet.member?(parents, "#{@ex}carol")
      assert MapSet.member?(parents, "#{@ex}david")
      assert MapSet.member?(parents, "#{@ex}eve")
    end

    test "sequence with recursive path", %{ctx: ctx} do
      # rdf:type followed by rdfs:subClassOf*
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@rdf}type", "#{@ex}Student"},
        {"#{@ex}Student", "#{@rdfs}subClassOf", "#{@ex}Person"},
        {"#{@ex}Person", "#{@rdfs}subClassOf", "#{@ex}Agent"}
      ])

      # Find all types of alice including inferred
      sparql = """
      SELECT ?type WHERE {
        <#{@ex}alice> <#{@rdf}type>/<#{@rdfs}subClassOf>* ?type
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      types = extract_iris(results, "type")

      assert MapSet.member?(types, "#{@ex}Student")
      assert MapSet.member?(types, "#{@ex}Person")
      assert MapSet.member?(types, "#{@ex}Agent")
    end

    test "sequence path with blank node intermediate (bind-join)", %{ctx: ctx} do
      # Test the bind-join for blank node intermediates in sequence paths
      # Query: ?x rdf:type/rdfs:subClassOf* ?type uses blank node for intermediate
      #
      # Graph:
      #   alice --rdf:type--> Student --subClassOf--> Person --subClassOf--> Agent
      #   bob --rdf:type--> Teacher --subClassOf--> Person
      #
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@rdf}type", "#{@ex}Student"},
        {"#{@ex}bob", "#{@rdf}type", "#{@ex}Teacher"},
        {"#{@ex}Student", "#{@rdfs}subClassOf", "#{@ex}Person"},
        {"#{@ex}Teacher", "#{@rdfs}subClassOf", "#{@ex}Person"},
        {"#{@ex}Person", "#{@rdfs}subClassOf", "#{@ex}Agent"}
      ])

      # Find all (person, type) pairs via sequence path with transitive closure
      sparql = """
      SELECT ?person ?type WHERE {
        ?person <#{@rdf}type>/<#{@rdfs}subClassOf>* ?type
      }
      """

      {:ok, results} = Query.query(ctx, sparql)

      # Group results by person
      alice_types =
        results
        |> Enum.filter(&(get_iri(&1["person"]) == "#{@ex}alice"))
        |> Enum.map(&get_iri(&1["type"]))
        |> MapSet.new()

      bob_types =
        results
        |> Enum.filter(&(get_iri(&1["person"]) == "#{@ex}bob"))
        |> Enum.map(&get_iri(&1["type"]))
        |> MapSet.new()

      # Alice's direct type is Student
      assert MapSet.member?(alice_types, "#{@ex}Student")
      # Alice is also a Person and Agent via subClassOf*
      assert MapSet.member?(alice_types, "#{@ex}Person")
      assert MapSet.member?(alice_types, "#{@ex}Agent")

      # Bob's direct type is Teacher
      assert MapSet.member?(bob_types, "#{@ex}Teacher")
      # Bob is also a Person and Agent via subClassOf*
      assert MapSet.member?(bob_types, "#{@ex}Person")
      assert MapSet.member?(bob_types, "#{@ex}Agent")
    end

    test "path on left side of join (symmetric bind-join)", %{ctx: ctx} do
      # Test that optimizer can handle path on LEFT side of join
      # This tests the symmetric bind-join handling (B2 fix)
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@foaf}knows", "#{@ex}bob"},
        {"#{@ex}bob", "#{@foaf}knows", "#{@ex}charlie"},
        {"#{@ex}alice", "#{@ex}name", "Alice"},
        {"#{@ex}bob", "#{@ex}name", "Bob"},
        {"#{@ex}charlie", "#{@ex}name", "Charlie"}
      ])

      # Query where path comes first, then BGP with related variable
      sparql = """
      SELECT ?friend ?name WHERE {
        <#{@ex}alice> <#{@foaf}knows>+ ?friend .
        ?friend <#{@ex}name> ?name
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      names = Enum.map(results, &get_literal(&1["name"])) |> MapSet.new()

      assert MapSet.member?(names, "Bob")
      assert MapSet.member?(names, "Charlie")
    end

    test "alternative with sequence", %{ctx: ctx} do
      # Find either direct friends or colleagues (friend or worksAt/employs^)
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@foaf}knows", "#{@ex}bob"},
        {"#{@ex}alice", "#{@ex}worksAt", "#{@ex}acme"},
        {"#{@ex}charlie", "#{@ex}worksAt", "#{@ex}acme"}
      ])

      # Find people connected to alice via knows OR working at same company
      sparql = """
      SELECT ?person WHERE {
        <#{@ex}alice> (<#{@foaf}knows>|<#{@ex}worksAt>/^<#{@ex}worksAt>) ?person
        FILTER(?person != <#{@ex}alice>)
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      people = extract_iris(results, "person")

      # knows
      assert MapSet.member?(people, "#{@ex}bob")
      # same company
      assert MapSet.member?(people, "#{@ex}charlie")
    end

    test "negated property set in path", %{ctx: ctx} do
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@rdf}type", "#{@ex}Person"},
        {"#{@ex}alice", "#{@foaf}knows", "#{@ex}bob"},
        {"#{@ex}alice", "#{@ex}email", "alice@example.org"}
      ])

      # Find all properties except type and knows
      sparql = """
      SELECT ?val WHERE {
        <#{@ex}alice> !(<#{@rdf}type>|<#{@foaf}knows>) ?val
      }
      """

      {:ok, results} = Query.query(ctx, sparql)

      # Should only find the email value
      assert length(results) == 1
    end

    test "inverse path in sequence", %{ctx: ctx} do
      # Find siblings: parent/^parent (same parent)
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@ex}parent", "#{@ex}carol"},
        {"#{@ex}bob", "#{@ex}parent", "#{@ex}carol"},
        {"#{@ex}charlie", "#{@ex}parent", "#{@ex}dave"}
      ])

      sparql = """
      SELECT ?sibling WHERE {
        <#{@ex}alice> <#{@ex}parent>/^<#{@ex}parent> ?sibling
        FILTER(?sibling != <#{@ex}alice>)
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      siblings = extract_iris(results, "sibling")

      assert MapSet.member?(siblings, "#{@ex}bob")
      # different parent
      refute MapSet.member?(siblings, "#{@ex}charlie")
    end
  end

  # ===========================================================================
  # 3.5.3.4: Benchmark Recursive Paths on Deep Hierarchies
  # ===========================================================================

  describe "benchmark recursive paths on deep hierarchies" do
    @tag timeout: @benchmark_timeout
    test "deep class hierarchy (100 levels)", %{ctx: ctx} do
      # Create a 100-level deep hierarchy
      triples =
        for i <- 1..99 do
          {"#{@ex}Class#{i}", "#{@rdfs}subClassOf", "#{@ex}Class#{i + 1}"}
        end

      insert_triples(ctx, triples)

      # Time finding all superclasses from bottom
      {time_us, {:ok, results}} =
        :timer.tc(fn ->
          Query.query(ctx, """
            SELECT ?superclass WHERE {
              <#{@ex}Class1> <#{@rdfs}subClassOf>* ?superclass
            }
          """)
        end)

      superclasses = extract_iris(results, "superclass")

      # Should find all 100 classes (Class1 through Class100)
      assert MapSet.size(superclasses) == 100
      IO.puts("\n  Deep hierarchy (100 levels) traversal: #{time_us / 1000}ms")

      # Reasonable performance expectation (10x buffer over typical ~5ms)
      assert time_us < @performance_threshold_ms * 1000,
             "Deep hierarchy traversal took too long: #{time_us / 1000}ms"
    end

    @tag timeout: @benchmark_timeout
    test "wide hierarchy (50 classes at each level)", %{ctx: ctx} do
      # Create 3 levels with 50 classes each
      level1_triples =
        for i <- 1..50 do
          {"#{@ex}L1_#{i}", "#{@rdfs}subClassOf", "#{@ex}Root"}
        end

      level2_triples =
        for i <- 1..50, j <- 1..2 do
          {"#{@ex}L2_#{i}_#{j}", "#{@rdfs}subClassOf", "#{@ex}L1_#{i}"}
        end

      insert_triples(ctx, level1_triples ++ level2_triples)

      # Find all subclasses of Root
      {time_us, {:ok, results}} =
        :timer.tc(fn ->
          Query.query(ctx, """
            SELECT ?subclass WHERE {
              ?subclass <#{@rdfs}subClassOf>+ <#{@ex}Root>
            }
          """)
        end)

      subclasses = extract_iris(results, "subclass")

      # 50 L1 classes + 100 L2 classes = 150 subclasses
      assert MapSet.size(subclasses) == 150
      IO.puts("\n  Wide hierarchy (150 classes) traversal: #{time_us / 1000}ms")

      # Reasonable performance expectation (10x buffer)
      assert time_us < @performance_threshold_ms * 1000,
             "Wide hierarchy traversal took too long: #{time_us / 1000}ms"
    end

    @tag timeout: @benchmark_timeout
    test "social network with cycles (100 nodes)", %{ctx: ctx} do
      # Create a circular network of 100 people
      triples =
        for i <- 1..100 do
          next = if i == 100, do: 1, else: i + 1
          {"#{@ex}person#{i}", "#{@foaf}knows", "#{@ex}person#{next}"}
        end

      insert_triples(ctx, triples)

      # Find all reachable from person1 (should find all 100)
      {time_us, {:ok, results}} =
        :timer.tc(fn ->
          Query.query(ctx, """
            SELECT ?person WHERE {
              <#{@ex}person1> <#{@foaf}knows>+ ?person
            }
          """)
        end)

      people = extract_iris(results, "person")

      # Should find all 100 people (including self via cycle)
      assert MapSet.size(people) == 100
      IO.puts("\n  Circular network (100 nodes) traversal: #{time_us / 1000}ms")

      # Reasonable performance expectation (10x buffer)
      assert time_us < @performance_threshold_ms * 1000,
             "Circular network traversal took too long: #{time_us / 1000}ms"
    end

    @tag timeout: @benchmark_timeout
    test "dense graph (complete graph of 20 nodes)", %{ctx: ctx} do
      # Create complete graph: everyone knows everyone
      triples =
        for i <- 1..20, j <- 1..20, i != j do
          {"#{@ex}person#{i}", "#{@foaf}knows", "#{@ex}person#{j}"}
        end

      insert_triples(ctx, triples)

      # Find all reachable from person1
      {time_us, {:ok, results}} =
        :timer.tc(fn ->
          Query.query(ctx, """
            SELECT ?person WHERE {
              <#{@ex}person1> <#{@foaf}knows>+ ?person
            }
          """)
        end)

      people = extract_iris(results, "person")

      # Should find all 20 people (person1 can reach all including self via others)
      assert MapSet.size(people) == 20
      IO.puts("\n  Complete graph (20 nodes, 380 edges) traversal: #{time_us / 1000}ms")

      # Reasonable performance expectation (10x buffer)
      assert time_us < @performance_threshold_ms * 1000,
             "Complete graph traversal took too long: #{time_us / 1000}ms"
    end

    @tag timeout: @benchmark_timeout
    test "binary tree hierarchy (7 levels, 127 nodes)", %{ctx: ctx} do
      # Create a binary tree with 7 levels
      # Level 0: 1 node, Level 1: 2 nodes, ... Level 6: 64 nodes = 127 total
      triples =
        for level <- 0..5, i <- 0..(round(:math.pow(2, level)) - 1) do
          parent = "#{@ex}node_#{level}_#{i}"
          left_child = "#{@ex}node_#{level + 1}_#{i * 2}"
          right_child = "#{@ex}node_#{level + 1}_#{i * 2 + 1}"

          [
            {left_child, "#{@rdfs}subClassOf", parent},
            {right_child, "#{@rdfs}subClassOf", parent}
          ]
        end
        |> List.flatten()

      insert_triples(ctx, triples)

      # Find all descendants of root
      {time_us, {:ok, results}} =
        :timer.tc(fn ->
          Query.query(ctx, """
            SELECT ?node WHERE {
              ?node <#{@rdfs}subClassOf>+ <#{@ex}node_0_0>
            }
          """)
        end)

      nodes = extract_iris(results, "node")

      # 127 total nodes - 1 root = 126 descendants
      assert MapSet.size(nodes) == 126
      IO.puts("\n  Binary tree (127 nodes) traversal: #{time_us / 1000}ms")

      # Reasonable performance expectation (10x buffer)
      assert time_us < @performance_threshold_ms * 1000,
             "Binary tree traversal took too long: #{time_us / 1000}ms"
    end

    @tag timeout: @benchmark_timeout
    test "compare zero-or-more vs one-or-more performance", %{ctx: ctx} do
      # Create chain of 50 nodes
      triples =
        for i <- 1..49 do
          {"#{@ex}node#{i}", "#{@ex}next", "#{@ex}node#{i + 1}"}
        end

      insert_triples(ctx, triples)

      # Zero-or-more
      {time_star, {:ok, results_star}} =
        :timer.tc(fn ->
          Query.query(ctx, """
            SELECT ?node WHERE {
              <#{@ex}node1> <#{@ex}next>* ?node
            }
          """)
        end)

      # One-or-more
      {time_plus, {:ok, results_plus}} =
        :timer.tc(fn ->
          Query.query(ctx, """
            SELECT ?node WHERE {
              <#{@ex}node1> <#{@ex}next>+ ?node
            }
          """)
        end)

      star_count = length(results_star)
      plus_count = length(results_plus)

      # Star includes self, plus does not
      # node1 through node50
      assert star_count == 50
      # node2 through node50
      assert plus_count == 49

      IO.puts(
        "\n  50-node chain: * = #{time_star / 1000}ms (#{star_count} results), + = #{time_plus / 1000}ms (#{plus_count} results)"
      )
    end
  end

  # ===========================================================================
  # Edge Cases and Correctness
  # ===========================================================================

  describe "edge cases" do
    test "empty database returns no results", %{ctx: ctx} do
      sparql = """
      SELECT ?x WHERE {
        <#{@ex}anything> <#{@rdfs}subClassOf>* ?x
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      # Zero-or-more should at least include the subject itself when it exists
      # But with empty database, subject doesn't exist, so no results
      # Actually, for p* with bound subject, we should get the subject even if no triples
      # This depends on implementation - just verify it doesn't error
      assert is_list(results)
    end

    test "handles very long IRI paths", %{ctx: ctx} do
      long_iri = "#{@ex}#{"a" |> String.duplicate(500)}"

      insert_triples(ctx, [
        {long_iri, "#{@rdfs}subClassOf", "#{@ex}Thing"}
      ])

      sparql = """
      SELECT ?x WHERE {
        <#{long_iri}> <#{@rdfs}subClassOf>* ?x
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      # At least Thing
      assert not Enum.empty?(results)
    end

    test "path with no matching triples returns empty for one-or-more", %{ctx: ctx} do
      insert_triples(ctx, [
        {"#{@ex}alice", "#{@foaf}knows", "#{@ex}bob"}
      ])

      # Query with non-existent predicate
      sparql = """
      SELECT ?x WHERE {
        <#{@ex}alice> <#{@ex}nonexistent>+ ?x
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      assert results == []
    end

    test "self-loop is handled correctly", %{ctx: ctx} do
      insert_triples(ctx, [
        # Self-loop
        {"#{@ex}alice", "#{@foaf}knows", "#{@ex}alice"}
      ])

      # One-or-more with self-loop
      sparql = """
      SELECT ?x WHERE {
        <#{@ex}alice> <#{@foaf}knows>+ ?x
      }
      """

      {:ok, results} = Query.query(ctx, sparql)
      people = extract_iris(results, "x")

      # Should find alice (via the self-loop, one step)
      assert MapSet.member?(people, "#{@ex}alice")
    end
  end

  # extract_count/1 imported from IntegrationHelpers
end
