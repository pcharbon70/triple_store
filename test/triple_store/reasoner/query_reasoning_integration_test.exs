defmodule TripleStore.Reasoner.QueryReasoningIntegrationTest do
  @moduledoc """
  Integration tests for Task 4.6.3: Query with Reasoning Testing.

  These tests verify that queries over materialized data return the
  expected inferred results:
  - Class hierarchy queries return inferred types
  - Transitive property queries return inferred relationships
  - sameAs queries return canonicalized results
  - Comparison of materialized vs query-time reasoning results

  ## Test Coverage

  - 4.6.3.1: Test class hierarchy query returns inferred types
  - 4.6.3.2: Test transitive property query returns inferred relationships
  - 4.6.3.3: Test sameAs query returns canonicalized results
  - 4.6.3.4: Compare materialized vs query-time reasoning results
  """
  use ExUnit.Case, async: false

  alias TripleStore.Reasoner.{
    SemiNaive,
    ReasoningProfile,
    ReasoningConfig,
    ReasoningMode,
    PatternMatcher
  }

  @moduletag :integration

  # ============================================================================
  # Namespace Constants
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ex "http://example.org/"

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp ex_iri(name), do: {:iri, @ex <> name}
  defp rdf_type, do: {:iri, @rdf <> "type"}
  defp rdfs_subClassOf, do: {:iri, @rdfs <> "subClassOf"}
  defp rdfs_subPropertyOf, do: {:iri, @rdfs <> "subPropertyOf"}
  defp owl_TransitiveProperty, do: {:iri, @owl <> "TransitiveProperty"}
  defp owl_SymmetricProperty, do: {:iri, @owl <> "SymmetricProperty"}
  defp owl_sameAs, do: {:iri, @owl <> "sameAs"}
  defp owl_inverseOf, do: {:iri, @owl <> "inverseOf"}

  @doc """
  Simulates a SPARQL-like query by finding all triples matching a pattern.

  Pattern uses {:var, :name} for unbound variables.

  ## Examples

      # Find all types of alice
      query(facts, {ex_iri("alice"), rdf_type(), {:var, :type}})

      # Find all instances of Person
      query(facts, {{:var, :x}, rdf_type(), ex_iri("Person")})
  """
  def query(facts, {s, p, o}) do
    pattern = {:pattern, [s, p, o]}
    PatternMatcher.filter_matching(facts, pattern)
  end

  @doc """
  Executes a simple SELECT-like query returning bindings.

  ## Examples

      # SELECT ?type WHERE { alice rdf:type ?type }
      select_types(facts, ex_iri("alice"))
  """
  def select_types(facts, subject) do
    query(facts, {subject, rdf_type(), {:var, :type}})
    |> Enum.map(fn {_, _, type} -> type end)
  end

  @doc """
  Executes a query for property values.

  ## Examples

      # SELECT ?object WHERE { subject predicate ?object }
      select_objects(facts, subject, predicate)
  """
  def select_objects(facts, subject, predicate) do
    query(facts, {subject, predicate, {:var, :object}})
    |> Enum.map(fn {_, _, obj} -> obj end)
  end

  @doc """
  Executes a query for property subjects.

  ## Examples

      # SELECT ?subject WHERE { ?subject predicate object }
      select_subjects(facts, predicate, object)
  """
  def select_subjects(facts, predicate, object) do
    query(facts, {{:var, :subject}, predicate, object})
    |> Enum.map(fn {subj, _, _} -> subj end)
  end

  @doc """
  Creates a test ontology with class hierarchy.
  """
  def create_class_hierarchy_ontology do
    MapSet.new([
      # Class hierarchy: GradStudent < Student < Person < Agent < Thing
      {ex_iri("GradStudent"), rdfs_subClassOf(), ex_iri("Student")},
      {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
      {ex_iri("Person"), rdfs_subClassOf(), ex_iri("Agent")},
      {ex_iri("Agent"), rdfs_subClassOf(), ex_iri("Thing")},

      # Faculty < Person
      {ex_iri("Faculty"), rdfs_subClassOf(), ex_iri("Person")},

      # Course (standalone)
      {ex_iri("Course"), rdfs_subClassOf(), ex_iri("Thing")}
    ])
  end

  @doc """
  Creates a test ontology with transitive properties.
  """
  def create_transitive_property_ontology do
    MapSet.new([
      # Transitive properties
      {ex_iri("ancestorOf"), rdf_type(), owl_TransitiveProperty()},
      {ex_iri("partOf"), rdf_type(), owl_TransitiveProperty()},
      {ex_iri("locatedIn"), rdf_type(), owl_TransitiveProperty()},

      # Symmetric property
      {ex_iri("knows"), rdf_type(), owl_SymmetricProperty()}
    ])
  end

  @doc """
  Creates a test ontology with sameAs relationships.
  """
  def create_sameas_ontology do
    MapSet.new([
      # Basic class
      {ex_iri("Person"), rdfs_subClassOf(), ex_iri("Thing")}
    ])
  end

  # ============================================================================
  # 4.6.3.1: Test class hierarchy query returns inferred types
  # ============================================================================

  describe "4.6.3.1 class hierarchy queries return inferred types" do
    test "query returns direct and inferred types via subClassOf" do
      tbox = create_class_hierarchy_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Add a GradStudent instance
      abox = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("GradStudent")}
      ])

      initial = MapSet.union(tbox, abox)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Query: SELECT ?type WHERE { alice rdf:type ?type }
      types = select_types(all_facts, ex_iri("alice"))

      # Should return all inferred types
      assert ex_iri("GradStudent") in types, "Expected direct type GradStudent"
      assert ex_iri("Student") in types, "Expected inferred type Student"
      assert ex_iri("Person") in types, "Expected inferred type Person"
      assert ex_iri("Agent") in types, "Expected inferred type Agent"
      assert ex_iri("Thing") in types, "Expected inferred type Thing"
    end

    test "query returns instances of class including inferred instances" do
      tbox = create_class_hierarchy_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Add instances at different levels
      abox = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("GradStudent")},
        {ex_iri("bob"), rdf_type(), ex_iri("Student")},
        {ex_iri("carol"), rdf_type(), ex_iri("Faculty")},
        {ex_iri("dave"), rdf_type(), ex_iri("Person")}
      ])

      initial = MapSet.union(tbox, abox)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Query: SELECT ?x WHERE { ?x rdf:type Person }
      persons = select_subjects(all_facts, rdf_type(), ex_iri("Person"))

      # All should be inferred as Person
      assert ex_iri("alice") in persons, "GradStudent should be Person"
      assert ex_iri("bob") in persons, "Student should be Person"
      assert ex_iri("carol") in persons, "Faculty should be Person"
      assert ex_iri("dave") in persons, "Person should be Person"

      # Query: SELECT ?x WHERE { ?x rdf:type Agent }
      agents = select_subjects(all_facts, rdf_type(), ex_iri("Agent"))

      # All persons should also be agents
      assert ex_iri("alice") in agents
      assert ex_iri("bob") in agents
      assert ex_iri("carol") in agents
      assert ex_iri("dave") in agents
    end

    test "query for specific level returns only that level and below" do
      tbox = create_class_hierarchy_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      abox = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("GradStudent")},
        {ex_iri("bob"), rdf_type(), ex_iri("Student")},
        {ex_iri("carol"), rdf_type(), ex_iri("Person")}
      ])

      initial = MapSet.union(tbox, abox)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Query: SELECT ?x WHERE { ?x rdf:type Student }
      students = select_subjects(all_facts, rdf_type(), ex_iri("Student"))

      # Only alice and bob should be students
      assert ex_iri("alice") in students, "GradStudent is Student"
      assert ex_iri("bob") in students, "Student is Student"
      refute ex_iri("carol") in students, "Person is not Student"

      # Query: SELECT ?x WHERE { ?x rdf:type GradStudent }
      grad_students = select_subjects(all_facts, rdf_type(), ex_iri("GradStudent"))

      # Only alice should be GradStudent
      assert ex_iri("alice") in grad_students
      refute ex_iri("bob") in grad_students
      refute ex_iri("carol") in grad_students
    end

    test "query with multiple subclass paths returns all inferred types" do
      # Diamond inheritance: Employee < Person, Employee < Worker, both < Agent
      diamond_tbox = MapSet.new([
        {ex_iri("Person"), rdfs_subClassOf(), ex_iri("Agent")},
        {ex_iri("Worker"), rdfs_subClassOf(), ex_iri("Agent")},
        {ex_iri("Employee"), rdfs_subClassOf(), ex_iri("Person")},
        {ex_iri("Employee"), rdfs_subClassOf(), ex_iri("Worker")}
      ])

      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      abox = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("Employee")}
      ])

      initial = MapSet.union(diamond_tbox, abox)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      types = select_types(all_facts, ex_iri("alice"))

      assert ex_iri("Employee") in types
      assert ex_iri("Person") in types, "Via Employee < Person"
      assert ex_iri("Worker") in types, "Via Employee < Worker"
      assert ex_iri("Agent") in types, "Via both paths"
    end
  end

  # ============================================================================
  # 4.6.3.2: Test transitive property query returns inferred relationships
  # ============================================================================

  describe "4.6.3.2 transitive property queries return inferred relationships" do
    test "query returns transitive closure of ancestorOf" do
      tbox = create_transitive_property_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Ancestor chain: alice -> bob -> carol -> dave
      abox = MapSet.new([
        {ex_iri("bob"), ex_iri("ancestorOf"), ex_iri("alice")},
        {ex_iri("carol"), ex_iri("ancestorOf"), ex_iri("bob")},
        {ex_iri("dave"), ex_iri("ancestorOf"), ex_iri("carol")}
      ])

      initial = MapSet.union(tbox, abox)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Query: SELECT ?ancestor WHERE { ?ancestor ancestorOf alice }
      ancestors_of_alice = select_subjects(all_facts, ex_iri("ancestorOf"), ex_iri("alice"))

      # Should include all ancestors via transitivity
      assert ex_iri("bob") in ancestors_of_alice, "Direct ancestor"
      assert ex_iri("carol") in ancestors_of_alice, "Grandparent via transitivity"
      assert ex_iri("dave") in ancestors_of_alice, "Great-grandparent via transitivity"

      # Query: SELECT ?descendant WHERE { carol ancestorOf ?descendant }
      descendants_of_carol = select_objects(all_facts, ex_iri("carol"), ex_iri("ancestorOf"))

      assert ex_iri("bob") in descendants_of_carol, "Direct descendant"
      assert ex_iri("alice") in descendants_of_carol, "Grandchild via transitivity"
    end

    test "query returns transitive closure of partOf" do
      tbox = create_transitive_property_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Part hierarchy: handle partOf door partOf car
      abox = MapSet.new([
        {ex_iri("handle"), ex_iri("partOf"), ex_iri("door")},
        {ex_iri("door"), ex_iri("partOf"), ex_iri("car")}
      ])

      initial = MapSet.union(tbox, abox)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Query: What is handle partOf?
      containers = select_objects(all_facts, ex_iri("handle"), ex_iri("partOf"))

      assert ex_iri("door") in containers, "Direct container"
      assert ex_iri("car") in containers, "Transitive container"
    end

    test "query returns transitive closure of locatedIn" do
      tbox = create_transitive_property_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Location hierarchy: room locatedIn building locatedIn campus locatedIn city
      abox = MapSet.new([
        {ex_iri("room101"), ex_iri("locatedIn"), ex_iri("building1")},
        {ex_iri("building1"), ex_iri("locatedIn"), ex_iri("campus")},
        {ex_iri("campus"), ex_iri("locatedIn"), ex_iri("city")}
      ])

      initial = MapSet.union(tbox, abox)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Query: Where is room101 located?
      locations = select_objects(all_facts, ex_iri("room101"), ex_iri("locatedIn"))

      assert ex_iri("building1") in locations, "Direct location"
      assert ex_iri("campus") in locations, "Transitive location (1 hop)"
      assert ex_iri("city") in locations, "Transitive location (2 hops)"
    end

    test "query with multiple transitive chains" do
      tbox = create_transitive_property_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Two parallel ancestor chains meeting at common ancestor
      abox = MapSet.new([
        # Chain 1: alice <- bob <- ancestor1
        {ex_iri("bob"), ex_iri("ancestorOf"), ex_iri("alice")},
        {ex_iri("ancestor1"), ex_iri("ancestorOf"), ex_iri("bob")},
        # Chain 2: carol <- dave <- ancestor1
        {ex_iri("dave"), ex_iri("ancestorOf"), ex_iri("carol")},
        {ex_iri("ancestor1"), ex_iri("ancestorOf"), ex_iri("dave")}
      ])

      initial = MapSet.union(tbox, abox)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # ancestor1 should be ancestor of both alice and carol
      descendants_of_ancestor1 = select_objects(all_facts, ex_iri("ancestor1"), ex_iri("ancestorOf"))

      assert ex_iri("bob") in descendants_of_ancestor1
      assert ex_iri("alice") in descendants_of_ancestor1
      assert ex_iri("dave") in descendants_of_ancestor1
      assert ex_iri("carol") in descendants_of_ancestor1
    end

    test "symmetric property query returns inverse relationships" do
      tbox = create_transitive_property_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      abox = MapSet.new([
        {ex_iri("alice"), ex_iri("knows"), ex_iri("bob")}
      ])

      initial = MapSet.union(tbox, abox)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Query: Who does bob know?
      bob_knows = select_objects(all_facts, ex_iri("bob"), ex_iri("knows"))

      assert ex_iri("alice") in bob_knows, "Symmetric property should infer inverse"

      # Query: Who knows alice?
      knows_alice = select_subjects(all_facts, ex_iri("knows"), ex_iri("alice"))

      assert ex_iri("bob") in knows_alice, "Original assertion"
    end
  end

  # ============================================================================
  # 4.6.3.3: Test sameAs query returns canonicalized results
  # ============================================================================

  describe "4.6.3.3 sameAs queries return canonicalized results" do
    test "sameAs is symmetric" do
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      facts = MapSet.new([
        {ex_iri("alice"), owl_sameAs(), ex_iri("alice_smith")}
      ])

      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, facts)

      # Query: What is alice_smith sameAs?
      same_as_alice_smith = select_objects(all_facts, ex_iri("alice_smith"), owl_sameAs())

      assert ex_iri("alice") in same_as_alice_smith, "sameAs should be symmetric"
    end

    test "sameAs is transitive" do
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      facts = MapSet.new([
        {ex_iri("alice"), owl_sameAs(), ex_iri("alice_smith")},
        {ex_iri("alice_smith"), owl_sameAs(), ex_iri("asmith")}
      ])

      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, facts)

      # Query: What is alice sameAs?
      same_as_alice = select_objects(all_facts, ex_iri("alice"), owl_sameAs())

      assert ex_iri("alice_smith") in same_as_alice, "Direct sameAs"
      assert ex_iri("asmith") in same_as_alice, "Transitive sameAs"

      # Query: What is asmith sameAs?
      same_as_asmith = select_objects(all_facts, ex_iri("asmith"), owl_sameAs())

      assert ex_iri("alice") in same_as_asmith, "Transitive symmetric sameAs"
    end

    test "sameAs propagates type assertions" do
      tbox = create_sameas_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # alice is Person, alice sameAs alice_smith
      abox = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("Person")},
        {ex_iri("alice"), owl_sameAs(), ex_iri("alice_smith")}
      ])

      initial = MapSet.union(tbox, abox)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Query: What types does alice_smith have?
      types = select_types(all_facts, ex_iri("alice_smith"))

      assert ex_iri("Person") in types, "Type should propagate via sameAs"
    end

    test "sameAs propagates property assertions" do
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # alice hasName "Alice", alice sameAs alice_smith
      facts = MapSet.new([
        {ex_iri("alice"), ex_iri("hasAge"), {:literal, "30", {:iri, "http://www.w3.org/2001/XMLSchema#integer"}}},
        {ex_iri("alice"), owl_sameAs(), ex_iri("alice_smith")}
      ])

      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, facts)

      # Query: What is alice_smith's age?
      ages = select_objects(all_facts, ex_iri("alice_smith"), ex_iri("hasAge"))

      expected_age = {:literal, "30", {:iri, "http://www.w3.org/2001/XMLSchema#integer"}}
      assert expected_age in ages, "Property should propagate via sameAs"
    end

    test "sameAs chain with multiple entities" do
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Chain: a sameAs b sameAs c sameAs d
      facts = MapSet.new([
        {ex_iri("a"), owl_sameAs(), ex_iri("b")},
        {ex_iri("b"), owl_sameAs(), ex_iri("c")},
        {ex_iri("c"), owl_sameAs(), ex_iri("d")},
        {ex_iri("a"), rdf_type(), ex_iri("Thing")}
      ])

      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, facts)

      # All should be sameAs each other (transitive + symmetric closure)
      for x <- [ex_iri("a"), ex_iri("b"), ex_iri("c"), ex_iri("d")] do
        same_as_x = select_objects(all_facts, x, owl_sameAs())
        others = [ex_iri("a"), ex_iri("b"), ex_iri("c"), ex_iri("d")] -- [x]
        for y <- others do
          assert y in same_as_x, "#{inspect(x)} should be sameAs #{inspect(y)}"
        end
      end

      # All should have type Thing
      for x <- [ex_iri("a"), ex_iri("b"), ex_iri("c"), ex_iri("d")] do
        types = select_types(all_facts, x)
        assert ex_iri("Thing") in types, "#{inspect(x)} should have type Thing via sameAs"
      end
    end

    test "sameAs with inverse property propagation" do
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # bob knows alice, alice sameAs alice_smith
      facts = MapSet.new([
        {ex_iri("bob"), ex_iri("likes"), ex_iri("alice")},
        {ex_iri("alice"), owl_sameAs(), ex_iri("alice_smith")}
      ])

      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, facts)

      # Query: Who does bob like? Should include alice_smith via sameAs
      bob_likes = select_objects(all_facts, ex_iri("bob"), ex_iri("likes"))

      assert ex_iri("alice") in bob_likes
      assert ex_iri("alice_smith") in bob_likes, "Object should be substituted via sameAs"
    end
  end

  # ============================================================================
  # 4.6.3.4: Compare materialized vs query-time reasoning results
  # ============================================================================

  describe "4.6.3.4 materialized vs query-time reasoning comparison" do
    test "materialized mode pre-computes all inferences" do
      tbox = create_class_hierarchy_ontology()

      abox = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("GradStudent")}
      ])

      initial = MapSet.union(tbox, abox)

      # Simulate materialized mode - run full materialization
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, materialized_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # In materialized mode, all inferences should be stored
      # Query is just a lookup
      types = select_types(materialized_facts, ex_iri("alice"))

      assert length(types) >= 5, "Materialized should have all inferred types"
      assert ex_iri("GradStudent") in types
      assert ex_iri("Student") in types
      assert ex_iri("Person") in types
      assert ex_iri("Agent") in types
      assert ex_iri("Thing") in types
    end

    test "query-time mode computes inferences on demand" do
      tbox = create_class_hierarchy_ontology()

      abox = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("GradStudent")}
      ])

      initial = MapSet.union(tbox, abox)

      # Simulate query-time mode - only explicit facts stored
      # For this test, we simulate by computing fresh each time

      # Before any reasoning, only explicit type exists
      explicit_types = select_types(initial, ex_iri("alice"))
      assert explicit_types == [ex_iri("GradStudent")], "Only explicit type before reasoning"

      # Compute inferences "on demand" for the query
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, computed_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      computed_types = select_types(computed_facts, ex_iri("alice"))
      assert length(computed_types) >= 5, "Query-time should compute all types"
    end

    test "materialized and query-time produce identical results" do
      tbox = MapSet.union(create_class_hierarchy_ontology(), create_transitive_property_ontology())

      abox = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("GradStudent")},
        {ex_iri("bob"), ex_iri("ancestorOf"), ex_iri("alice")},
        {ex_iri("carol"), ex_iri("ancestorOf"), ex_iri("bob")}
      ])

      initial = MapSet.union(tbox, abox)

      # Run materialization (simulating materialized mode)
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)
      {:ok, materialized, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Run again (simulating query-time, which would compute fresh)
      {:ok, query_time, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Results should be identical
      assert MapSet.equal?(materialized, query_time),
             "Materialized and query-time should produce identical facts"
    end

    test "ReasoningConfig correctly identifies mode characteristics" do
      # Materialized mode
      {:ok, mat_config} = ReasoningConfig.new(mode: :materialized, profile: :rdfs)
      assert ReasoningConfig.requires_materialization?(mat_config)
      refute ReasoningConfig.requires_backward_chaining?(mat_config)

      # Query-time mode
      {:ok, qt_config} = ReasoningConfig.new(mode: :query_time, profile: :rdfs)
      refute ReasoningConfig.requires_materialization?(qt_config)
      assert ReasoningConfig.requires_backward_chaining?(qt_config)

      # Hybrid mode
      {:ok, hybrid_config} = ReasoningConfig.new(mode: :hybrid, profile: :owl2rl)
      assert ReasoningConfig.requires_materialization?(hybrid_config)
      assert ReasoningConfig.requires_backward_chaining?(hybrid_config)
    end

    test "ReasoningMode provides correct configuration defaults" do
      mat_config = ReasoningMode.default_config(:materialized)
      assert mat_config.mode == :materialized
      assert mat_config.max_iterations > 0
      refute ReasoningMode.requires_backward_chaining?(mat_config)

      qt_config = ReasoningMode.default_config(:query_time)
      assert qt_config.mode == :query_time
      assert qt_config.max_depth > 0
      assert ReasoningMode.requires_backward_chaining?(qt_config)

      hybrid_config = ReasoningMode.default_config(:hybrid)
      assert hybrid_config.mode == :hybrid
      assert is_list(hybrid_config.materialized_rules)
      assert is_list(hybrid_config.query_time_rules)
    end

    test "hybrid mode separates RDFS and OWL rules" do
      {:ok, config} = ReasoningConfig.new(mode: :hybrid, profile: :owl2rl)

      materialized_rules = ReasoningConfig.materialization_rules(config)
      query_time_rules = ReasoningConfig.query_time_rules(config)

      # RDFS rules should be materialized
      assert :scm_sco in materialized_rules, "scm_sco should be materialized"
      assert :cax_sco in materialized_rules, "cax_sco should be materialized"

      # OWL-specific rules should be query-time
      # Note: actual rule split depends on implementation
      assert length(query_time_rules) > 0, "Some rules should be query-time"
    end
  end

  # ============================================================================
  # Additional Query Patterns
  # ============================================================================

  describe "additional query patterns" do
    test "query with property chain inference" do
      # subPropertyOf chain: headOf < worksFor < affiliatedWith
      tbox = MapSet.new([
        {ex_iri("headOf"), rdfs_subPropertyOf(), ex_iri("worksFor")},
        {ex_iri("worksFor"), rdfs_subPropertyOf(), ex_iri("affiliatedWith")}
      ])

      abox = MapSet.new([
        {ex_iri("alice"), ex_iri("headOf"), ex_iri("dept1")}
      ])

      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, MapSet.union(tbox, abox))

      # Query: What is alice affiliatedWith?
      affiliations = select_objects(all_facts, ex_iri("alice"), ex_iri("affiliatedWith"))

      assert ex_iri("dept1") in affiliations, "headOf should imply affiliatedWith"

      # Query: What does alice work for?
      works_for = select_objects(all_facts, ex_iri("alice"), ex_iri("worksFor"))

      assert ex_iri("dept1") in works_for, "headOf should imply worksFor"
    end

    test "query with inverse property inference" do
      tbox = MapSet.new([
        {ex_iri("parentOf"), owl_inverseOf(), ex_iri("childOf")}
      ])

      abox = MapSet.new([
        {ex_iri("alice"), ex_iri("parentOf"), ex_iri("bob")}
      ])

      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, MapSet.union(tbox, abox))

      # Query: Who is bob childOf?
      parents = select_objects(all_facts, ex_iri("bob"), ex_iri("childOf"))

      assert ex_iri("alice") in parents, "childOf should be inferred via inverseOf"
    end

    test "complex query combining multiple inference types" do
      tbox = MapSet.new([
        # Class hierarchy
        {ex_iri("Professor"), rdfs_subClassOf(), ex_iri("Faculty")},
        {ex_iri("Faculty"), rdfs_subClassOf(), ex_iri("Person")},

        # Property characteristics
        {ex_iri("collaboratesWith"), rdf_type(), owl_SymmetricProperty()},

        # Inverse properties
        {ex_iri("supervises"), owl_inverseOf(), ex_iri("supervisedBy")}
      ])

      abox = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("Professor")},
        {ex_iri("alice"), ex_iri("collaboratesWith"), ex_iri("bob")},
        {ex_iri("alice"), ex_iri("supervises"), ex_iri("carol")}
      ])

      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, MapSet.union(tbox, abox))

      # alice should be Faculty and Person
      alice_types = select_types(all_facts, ex_iri("alice"))
      assert ex_iri("Faculty") in alice_types
      assert ex_iri("Person") in alice_types

      # bob should collaboratesWith alice (symmetric)
      bob_collaborates = select_objects(all_facts, ex_iri("bob"), ex_iri("collaboratesWith"))
      assert ex_iri("alice") in bob_collaborates

      # carol should be supervisedBy alice (inverse)
      carol_supervisors = select_objects(all_facts, ex_iri("carol"), ex_iri("supervisedBy"))
      assert ex_iri("alice") in carol_supervisors
    end
  end
end
