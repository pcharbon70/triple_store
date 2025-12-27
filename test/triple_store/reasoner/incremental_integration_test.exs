defmodule TripleStore.Reasoner.IncrementalIntegrationTest do
  @moduledoc """
  Integration tests for Task 4.6.2: Incremental Reasoning Testing.

  These tests verify the correctness of incremental materialization maintenance:
  - Adding instances and verifying new inferences are derived
  - Deleting instances and verifying dependent inferences are retracted
  - Preserving derived facts when alternative derivations exist
  - Triggering rematerialization on TBox updates

  ## Test Coverage

  - 4.6.2.1: Test add instance -> new type inferences derived
  - 4.6.2.2: Test delete instance -> dependent inferences retracted
  - 4.6.2.3: Test delete with alternative derivation preserves fact
  - 4.6.2.4: Test TBox update triggers rematerialization
  """
  use ExUnit.Case, async: false

  alias TripleStore.Reasoner.{
    Incremental,
    DeleteWithReasoning,
    SemiNaive,
    ReasoningProfile,
    ReasoningStatus,
    TBoxCache
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
  defp rdfs_domain, do: {:iri, @rdfs <> "domain"}
  defp rdfs_range, do: {:iri, @rdfs <> "range"}
  defp owl_TransitiveProperty, do: {:iri, @owl <> "TransitiveProperty"}
  defp owl_SymmetricProperty, do: {:iri, @owl <> "SymmetricProperty"}

  @doc """
  Creates a base ontology for testing incremental operations.
  """
  def create_base_ontology do
    MapSet.new([
      # Class hierarchy: Student < Person < Thing
      {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
      {ex_iri("Person"), rdfs_subClassOf(), ex_iri("Thing")},

      # Class hierarchy: GradStudent < Student
      {ex_iri("GradStudent"), rdfs_subClassOf(), ex_iri("Student")},

      # Class hierarchy: Faculty < Person
      {ex_iri("Faculty"), rdfs_subClassOf(), ex_iri("Person")},

      # Property hierarchy: teaches < involves
      {ex_iri("teaches"), rdfs_subPropertyOf(), ex_iri("involves")},

      # Domain and range
      {ex_iri("teaches"), rdfs_domain(), ex_iri("Faculty")},
      {ex_iri("teaches"), rdfs_range(), ex_iri("Course")},
      {ex_iri("enrolledIn"), rdfs_domain(), ex_iri("Student")},
      {ex_iri("enrolledIn"), rdfs_range(), ex_iri("Course")},

      # Transitive property
      {ex_iri("ancestorOf"), rdf_type(), owl_TransitiveProperty()},

      # Symmetric property
      {ex_iri("knows"), rdf_type(), owl_SymmetricProperty()}
    ])
  end

  @doc """
  Materializes the base ontology and returns the full fact set.
  """
  def materialize_base(tbox, abox_facts \\ []) do
    initial = Enum.reduce(abox_facts, tbox, &MapSet.put(&2, &1))
    {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
    {:ok, all_facts, _stats} = SemiNaive.materialize_in_memory(rules, initial)
    all_facts
  end

  @doc """
  Computes which facts are derived (not in initial set).
  """
  def compute_derived(initial_facts, all_facts) do
    MapSet.difference(all_facts, initial_facts)
  end

  # ============================================================================
  # 4.6.2.1: Test add instance -> new type inferences derived
  # ============================================================================

  describe "4.6.2.1 incremental addition derives new inferences" do
    test "adding Student instance derives Person type" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Start with just the TBox
      {:ok, base_facts, _} = SemiNaive.materialize_in_memory(rules, tbox)

      # Add a new student
      new_triple = {ex_iri("alice"), rdf_type(), ex_iri("Student")}

      {:ok, all_facts, stats} = Incremental.add_in_memory([new_triple], base_facts, rules)

      # Verify the new instance type was added
      assert MapSet.member?(all_facts, new_triple)

      # Verify Person type was derived
      alice_person = {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      assert MapSet.member?(all_facts, alice_person),
             "Expected alice to be inferred as Person via Student < Person"

      # Verify Thing type was derived (transitive)
      alice_thing = {ex_iri("alice"), rdf_type(), ex_iri("Thing")}
      assert MapSet.member?(all_facts, alice_thing),
             "Expected alice to be inferred as Thing via Person < Thing"

      # Verify stats
      assert stats.explicit_added == 1
      assert stats.derived_count >= 2, "Expected at least 2 derived facts (Person, Thing)"
    end

    test "adding GradStudent instance derives full class chain" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, base_facts, _} = SemiNaive.materialize_in_memory(rules, tbox)

      # Add a grad student
      new_triple = {ex_iri("bob"), rdf_type(), ex_iri("GradStudent")}
      {:ok, all_facts, stats} = Incremental.add_in_memory([new_triple], base_facts, rules)

      # Verify full chain: GradStudent -> Student -> Person -> Thing
      assert MapSet.member?(all_facts, {ex_iri("bob"), rdf_type(), ex_iri("GradStudent")})
      assert MapSet.member?(all_facts, {ex_iri("bob"), rdf_type(), ex_iri("Student")})
      assert MapSet.member?(all_facts, {ex_iri("bob"), rdf_type(), ex_iri("Person")})
      assert MapSet.member?(all_facts, {ex_iri("bob"), rdf_type(), ex_iri("Thing")})

      assert stats.derived_count >= 3, "Expected at least 3 derived facts"
    end

    test "adding property instance derives domain and range types" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, base_facts, _} = SemiNaive.materialize_in_memory(rules, tbox)

      # Add a teaches relationship
      new_triple = {ex_iri("prof1"), ex_iri("teaches"), ex_iri("course101")}
      {:ok, all_facts, _stats} = Incremental.add_in_memory([new_triple], base_facts, rules)

      # Verify domain inference: prof1 rdf:type Faculty
      assert MapSet.member?(all_facts, {ex_iri("prof1"), rdf_type(), ex_iri("Faculty")}),
             "Expected prof1 to be inferred as Faculty via domain of teaches"

      # Verify range inference: course101 rdf:type Course
      assert MapSet.member?(all_facts, {ex_iri("course101"), rdf_type(), ex_iri("Course")}),
             "Expected course101 to be inferred as Course via range of teaches"

      # Verify transitive class inference: Faculty < Person
      assert MapSet.member?(all_facts, {ex_iri("prof1"), rdf_type(), ex_iri("Person")}),
             "Expected prof1 to be inferred as Person via Faculty < Person"
    end

    test "adding subproperty instance derives superproperty" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, base_facts, _} = SemiNaive.materialize_in_memory(rules, tbox)

      # Add a teaches relationship (subproperty of involves)
      new_triple = {ex_iri("prof1"), ex_iri("teaches"), ex_iri("course101")}
      {:ok, all_facts, _stats} = Incremental.add_in_memory([new_triple], base_facts, rules)

      # Verify superproperty inference
      assert MapSet.member?(all_facts, {ex_iri("prof1"), ex_iri("involves"), ex_iri("course101")}),
             "Expected involves relationship via teaches < involves"
    end

    test "adding multiple instances derives all consequences" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, base_facts, _} = SemiNaive.materialize_in_memory(rules, tbox)

      # Add multiple instances at once
      new_triples = [
        {ex_iri("alice"), rdf_type(), ex_iri("Student")},
        {ex_iri("bob"), rdf_type(), ex_iri("GradStudent")},
        {ex_iri("carol"), rdf_type(), ex_iri("Faculty")}
      ]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, base_facts, rules)

      # All should be Person
      assert MapSet.member?(all_facts, {ex_iri("alice"), rdf_type(), ex_iri("Person")})
      assert MapSet.member?(all_facts, {ex_iri("bob"), rdf_type(), ex_iri("Person")})
      assert MapSet.member?(all_facts, {ex_iri("carol"), rdf_type(), ex_iri("Person")})

      assert stats.explicit_added == 3
      assert stats.derived_count >= 6, "Expected multiple derived facts"
    end

    test "adding duplicate instance does not create redundant work" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # First, materialize with an existing student
      existing_triple = {ex_iri("alice"), rdf_type(), ex_iri("Student")}
      initial = MapSet.put(tbox, existing_triple)
      {:ok, base_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Try to add the same triple again
      {:ok, all_facts, stats} = Incremental.add_in_memory([existing_triple], base_facts, rules)

      # No new facts should be added
      assert stats.explicit_added == 0
      assert stats.derived_count == 0
      assert MapSet.size(all_facts) == MapSet.size(base_facts)
    end
  end

  # ============================================================================
  # 4.6.2.2: Test delete instance -> dependent inferences retracted
  # ============================================================================

  describe "4.6.2.2 deletion retracts dependent inferences" do
    test "deleting only type assertion retracts derived types" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Materialize with a student
      student_triple = {ex_iri("alice"), rdf_type(), ex_iri("Student")}
      initial = MapSet.put(tbox, student_triple)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Compute derived facts
      derived = compute_derived(initial, all_facts)

      # Verify alice is Person before deletion
      alice_person = {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      assert MapSet.member?(all_facts, alice_person)

      # Delete the student type
      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        [student_triple],
        all_facts,
        derived,
        rules
      )

      # Verify alice is no longer Person
      refute MapSet.member?(result.final_facts, alice_person),
             "Expected Person type to be retracted when Student type is deleted"

      # Verify alice is no longer Thing
      alice_thing = {ex_iri("alice"), rdf_type(), ex_iri("Thing")}
      refute MapSet.member?(result.final_facts, alice_thing),
             "Expected Thing type to be retracted transitively"

      # Verify stats
      assert result.stats.explicit_deleted == 1
      assert result.stats.derived_deleted >= 2
    end

    test "deleting property instance retracts domain/range inferences" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Materialize with a teaches relationship
      teaches_triple = {ex_iri("prof1"), ex_iri("teaches"), ex_iri("course101")}
      initial = MapSet.put(tbox, teaches_triple)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)
      derived = compute_derived(initial, all_facts)

      # Verify domain/range types before deletion
      assert MapSet.member?(all_facts, {ex_iri("prof1"), rdf_type(), ex_iri("Faculty")})
      assert MapSet.member?(all_facts, {ex_iri("course101"), rdf_type(), ex_iri("Course")})

      # Delete the teaches relationship
      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        [teaches_triple],
        all_facts,
        derived,
        rules
      )

      # Verify domain/range types are retracted
      refute MapSet.member?(result.final_facts, {ex_iri("prof1"), rdf_type(), ex_iri("Faculty")}),
             "Expected Faculty type to be retracted when teaches is deleted"
      refute MapSet.member?(result.final_facts, {ex_iri("course101"), rdf_type(), ex_iri("Course")}),
             "Expected Course type to be retracted when teaches is deleted"
    end

    test "deleting subproperty instance retracts superproperty assertion" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Materialize with a teaches relationship
      teaches_triple = {ex_iri("prof1"), ex_iri("teaches"), ex_iri("course101")}
      initial = MapSet.put(tbox, teaches_triple)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)
      derived = compute_derived(initial, all_facts)

      # Verify superproperty exists
      involves_triple = {ex_iri("prof1"), ex_iri("involves"), ex_iri("course101")}
      assert MapSet.member?(all_facts, involves_triple)

      # Delete the teaches relationship
      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        [teaches_triple],
        all_facts,
        derived,
        rules
      )

      # Verify superproperty is retracted
      refute MapSet.member?(result.final_facts, involves_triple),
             "Expected involves to be retracted when teaches is deleted"
    end

    test "deleting from class chain retracts downstream inferences" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Materialize with a grad student (full chain)
      grad_triple = {ex_iri("bob"), rdf_type(), ex_iri("GradStudent")}
      initial = MapSet.put(tbox, grad_triple)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)
      derived = compute_derived(initial, all_facts)

      # Verify full chain exists
      assert MapSet.member?(all_facts, {ex_iri("bob"), rdf_type(), ex_iri("Student")})
      assert MapSet.member?(all_facts, {ex_iri("bob"), rdf_type(), ex_iri("Person")})
      assert MapSet.member?(all_facts, {ex_iri("bob"), rdf_type(), ex_iri("Thing")})

      # Delete the GradStudent type
      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        [grad_triple],
        all_facts,
        derived,
        rules
      )

      # All downstream types should be retracted
      refute MapSet.member?(result.final_facts, {ex_iri("bob"), rdf_type(), ex_iri("Student")})
      refute MapSet.member?(result.final_facts, {ex_iri("bob"), rdf_type(), ex_iri("Person")})
      refute MapSet.member?(result.final_facts, {ex_iri("bob"), rdf_type(), ex_iri("Thing")})
    end
  end

  # ============================================================================
  # 4.6.2.3: Test delete with alternative derivation preserves fact
  # ============================================================================

  describe "4.6.2.3 alternative derivation preserves facts" do
    test "Person type preserved when both Student and Faculty exist" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Materialize with both Student and Faculty types for alice
      initial = tbox
        |> MapSet.put({ex_iri("alice"), rdf_type(), ex_iri("Student")})
        |> MapSet.put({ex_iri("alice"), rdf_type(), ex_iri("Faculty")})

      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)
      derived = compute_derived(initial, all_facts)

      # Verify Person type exists
      alice_person = {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      assert MapSet.member?(all_facts, alice_person)

      # Delete only the Student type
      student_triple = {ex_iri("alice"), rdf_type(), ex_iri("Student")}
      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        [student_triple],
        all_facts,
        derived,
        rules
      )

      # Person type should still exist via Faculty
      assert MapSet.member?(result.final_facts, alice_person),
             "Expected Person type to be preserved via Faculty alternative derivation"

      # Thing type should also be preserved
      alice_thing = {ex_iri("alice"), rdf_type(), ex_iri("Thing")}
      assert MapSet.member?(result.final_facts, alice_thing),
             "Expected Thing type to be preserved via Faculty -> Person -> Thing"

      # Stats should show the fact was kept
      assert result.stats.derived_kept >= 1
    end

    test "preserves fact with multiple alternative paths" do
      # Create a diamond hierarchy:
      #       Thing
      #      /     \
      #   Person  Agent
      #      \     /
      #     Employee
      diamond_tbox = MapSet.new([
        {ex_iri("Person"), rdfs_subClassOf(), ex_iri("Thing")},
        {ex_iri("Agent"), rdfs_subClassOf(), ex_iri("Thing")},
        {ex_iri("Employee"), rdfs_subClassOf(), ex_iri("Person")},
        {ex_iri("Employee"), rdfs_subClassOf(), ex_iri("Agent")}
      ])

      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Materialize with Employee type
      initial = MapSet.put(diamond_tbox, {ex_iri("alice"), rdf_type(), ex_iri("Employee")})
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)
      derived = compute_derived(initial, all_facts)

      # Thing should be derived via two paths
      alice_thing = {ex_iri("alice"), rdf_type(), ex_iri("Thing")}
      assert MapSet.member?(all_facts, alice_thing)

      # Delete Person (one path to Thing)
      person_triple = {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        [person_triple],
        all_facts,
        derived,
        rules
      )

      # Thing should still exist via Agent path
      assert MapSet.member?(result.final_facts, alice_thing),
             "Expected Thing to be preserved via Agent alternative path"

      # Agent should still exist
      alice_agent = {ex_iri("alice"), rdf_type(), ex_iri("Agent")}
      assert MapSet.member?(result.final_facts, alice_agent)
    end

    test "preserves property when multiple instances support it" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Two teaches relationships supporting involves
      initial = tbox
        |> MapSet.put({ex_iri("prof1"), ex_iri("teaches"), ex_iri("course101")})
        |> MapSet.put({ex_iri("prof1"), ex_iri("teaches"), ex_iri("course102")})

      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)
      derived = compute_derived(initial, all_facts)

      # Both involves relationships should exist
      involves1 = {ex_iri("prof1"), ex_iri("involves"), ex_iri("course101")}
      involves2 = {ex_iri("prof1"), ex_iri("involves"), ex_iri("course102")}
      assert MapSet.member?(all_facts, involves1)
      assert MapSet.member?(all_facts, involves2)

      # Delete one teaches
      teaches1 = {ex_iri("prof1"), ex_iri("teaches"), ex_iri("course101")}
      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        [teaches1],
        all_facts,
        derived,
        rules
      )

      # involves1 should be deleted, involves2 should remain
      refute MapSet.member?(result.final_facts, involves1)
      assert MapSet.member?(result.final_facts, involves2)

      # Faculty type should still exist via teaches course102
      assert MapSet.member?(result.final_facts, {ex_iri("prof1"), rdf_type(), ex_iri("Faculty")})
    end

    test "correctly handles multiple independent entities" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Two students
      initial = tbox
        |> MapSet.put({ex_iri("alice"), rdf_type(), ex_iri("Student")})
        |> MapSet.put({ex_iri("bob"), rdf_type(), ex_iri("Student")})

      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)
      derived = compute_derived(initial, all_facts)

      # Delete alice's type
      alice_student = {ex_iri("alice"), rdf_type(), ex_iri("Student")}
      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        [alice_student],
        all_facts,
        derived,
        rules
      )

      # Alice's derived types should be gone
      refute MapSet.member?(result.final_facts, {ex_iri("alice"), rdf_type(), ex_iri("Person")})

      # Bob's types should still exist
      assert MapSet.member?(result.final_facts, {ex_iri("bob"), rdf_type(), ex_iri("Student")})
      assert MapSet.member?(result.final_facts, {ex_iri("bob"), rdf_type(), ex_iri("Person")})
    end
  end

  # ============================================================================
  # 4.6.2.4: Test TBox update triggers rematerialization
  # ============================================================================

  describe "4.6.2.4 TBox update triggers rematerialization" do
    test "adding subclass relationship triggers new derivations" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Start with a Worker (not in hierarchy)
      initial = MapSet.put(tbox, {ex_iri("charlie"), rdf_type(), ex_iri("Worker")})
      {:ok, base_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Charlie should not be Person yet
      charlie_person = {ex_iri("charlie"), rdf_type(), ex_iri("Person")}
      refute MapSet.member?(base_facts, charlie_person)

      # Add TBox triple: Worker < Person
      tbox_update = {ex_iri("Worker"), rdfs_subClassOf(), ex_iri("Person")}
      {:ok, all_facts, stats} = Incremental.add_in_memory([tbox_update], base_facts, rules)

      # Now charlie should be inferred as Person
      assert MapSet.member?(all_facts, charlie_person),
             "Expected charlie to become Person after TBox update"

      # And Thing
      assert MapSet.member?(all_facts, {ex_iri("charlie"), rdf_type(), ex_iri("Thing")})

      assert stats.derived_count >= 2
    end

    test "adding property characteristic triggers new derivations" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:owl2rl)

      # Start with a likes relationship
      initial = tbox
        |> MapSet.put({ex_iri("alice"), ex_iri("likes"), ex_iri("bob")})

      {:ok, base_facts, _} = SemiNaive.materialize_in_memory(rules, initial)

      # Inverse should not exist yet
      inverse = {ex_iri("bob"), ex_iri("likes"), ex_iri("alice")}
      refute MapSet.member?(base_facts, inverse)

      # Add TBox: likes is symmetric
      symmetric_decl = {ex_iri("likes"), rdf_type(), owl_SymmetricProperty()}
      {:ok, all_facts, _stats} = Incremental.add_in_memory([symmetric_decl], base_facts, rules)

      # Now inverse should be derived
      assert MapSet.member?(all_facts, inverse),
             "Expected symmetric inverse after property characteristic added"
    end

    test "ReasoningStatus marks stale on TBox change" do
      # Create a status
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_materialization(status, %{
        derived_count: 100,
        iterations: 3,
        duration_ms: 500
      })

      # Verify it's materialized
      assert ReasoningStatus.state(status) == :materialized
      refute ReasoningStatus.needs_rematerialization?(status)

      # Simulate TBox update by marking stale
      status = ReasoningStatus.mark_stale(status)

      # Should now need rematerialization
      assert ReasoningStatus.state(status) == :stale
      assert ReasoningStatus.needs_rematerialization?(status)
    end

    test "removing subclass relationship invalidates derivations" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)

      # Materialize with a student
      initial = MapSet.put(tbox, {ex_iri("alice"), rdf_type(), ex_iri("Student")})
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)
      derived = compute_derived(initial, all_facts)

      # Verify alice is Person
      assert MapSet.member?(all_facts, {ex_iri("alice"), rdf_type(), ex_iri("Person")})

      # Delete TBox triple: Student < Person
      tbox_triple = {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")}
      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        [tbox_triple],
        all_facts,
        derived,
        rules
      )

      # Alice should no longer be Person (after TBox change)
      refute MapSet.member?(result.final_facts, {ex_iri("alice"), rdf_type(), ex_iri("Person")}),
             "Expected Person derivation to be invalidated when subclass removed"
    end

    test "TBoxCache invalidation on hierarchy change" do
      # Create a fact set
      facts = MapSet.new([
        {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
        {ex_iri("Person"), rdfs_subClassOf(), ex_iri("Thing")}
      ])

      # Compute hierarchy
      {:ok, cache} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Verify Student's superclasses
      superclasses = TBoxCache.superclasses_from(cache, ex_iri("Student"))
      assert MapSet.member?(superclasses, ex_iri("Person"))
      assert MapSet.member?(superclasses, ex_iri("Thing"))

      # Add new subclass relationship
      new_facts = MapSet.put(facts, {ex_iri("GradStudent"), rdfs_subClassOf(), ex_iri("Student")})

      # Recompute (simulating cache invalidation)
      {:ok, new_cache} = TBoxCache.compute_class_hierarchy_in_memory(new_facts)

      # Verify GradStudent's superclasses include full chain
      grad_superclasses = TBoxCache.superclasses_from(new_cache, ex_iri("GradStudent"))
      assert MapSet.member?(grad_superclasses, ex_iri("Student"))
      assert MapSet.member?(grad_superclasses, ex_iri("Person"))
      assert MapSet.member?(grad_superclasses, ex_iri("Thing"))
    end
  end

  # ============================================================================
  # Additional Edge Cases
  # ============================================================================

  describe "edge cases" do
    test "empty addition produces no changes" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, base_facts, _} = SemiNaive.materialize_in_memory(rules, tbox)

      {:ok, all_facts, stats} = Incremental.add_in_memory([], base_facts, rules)

      assert MapSet.size(all_facts) == MapSet.size(base_facts)
      assert stats.explicit_added == 0
      assert stats.derived_count == 0
    end

    test "empty deletion produces no changes" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      initial = MapSet.put(tbox, {ex_iri("alice"), rdf_type(), ex_iri("Student")})
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)
      derived = compute_derived(initial, all_facts)

      {:ok, result} = DeleteWithReasoning.delete_in_memory([], all_facts, derived, rules)

      assert MapSet.size(result.final_facts) == MapSet.size(all_facts)
      assert result.stats.explicit_deleted == 0
      assert result.stats.derived_deleted == 0
    end

    test "deleting non-existent fact is a no-op" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, tbox)
      derived = compute_derived(tbox, all_facts)

      # Try to delete a fact that doesn't exist
      nonexistent = {ex_iri("nobody"), rdf_type(), ex_iri("Student")}
      {:ok, result} = DeleteWithReasoning.delete_in_memory([nonexistent], all_facts, derived, rules)

      assert MapSet.size(result.final_facts) == MapSet.size(all_facts)
    end

    test "preview_in_memory shows what would be derived" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      {:ok, base_facts, _} = SemiNaive.materialize_in_memory(rules, tbox)

      new_triple = {ex_iri("alice"), rdf_type(), ex_iri("Student")}
      {:ok, would_derive} = Incremental.preview_in_memory([new_triple], base_facts, rules)

      # Should include Person and Thing
      assert MapSet.member?(would_derive, {ex_iri("alice"), rdf_type(), ex_iri("Person")})
      assert MapSet.member?(would_derive, {ex_iri("alice"), rdf_type(), ex_iri("Thing")})
    end

    test "preview_delete_in_memory shows what would be deleted" do
      tbox = create_base_ontology()
      {:ok, rules} = ReasoningProfile.rules_for(:rdfs)
      initial = MapSet.put(tbox, {ex_iri("alice"), rdf_type(), ex_iri("Student")})
      {:ok, all_facts, _} = SemiNaive.materialize_in_memory(rules, initial)
      derived = compute_derived(initial, all_facts)

      student_triple = {ex_iri("alice"), rdf_type(), ex_iri("Student")}
      {:ok, {explicit_deleted, derived_deleted}} =
        DeleteWithReasoning.preview_delete_in_memory([student_triple], all_facts, derived, rules)

      assert MapSet.member?(explicit_deleted, student_triple)
      assert MapSet.member?(derived_deleted, {ex_iri("alice"), rdf_type(), ex_iri("Person")})
    end
  end
end
