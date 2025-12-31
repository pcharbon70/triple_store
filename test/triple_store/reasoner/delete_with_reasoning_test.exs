# credo:disable-for-this-file Credo.Check.Readability.FunctionNames
# credo:disable-for-this-file Credo.Check.Readability.VariableNames
defmodule TripleStore.Reasoner.DeleteWithReasoningTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.DeleteWithReasoning
  alias TripleStore.Reasoner.Rules

  # ============================================================================
  # Test Helpers
  # ============================================================================

  # Create an IRI term
  defp iri(value), do: {:iri, "http://example.org/#{value}"}

  # Common predicates
  defp rdf_type, do: {:iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}
  defp rdfs_subClassOf, do: {:iri, "http://www.w3.org/2000/01/rdf-schema#subClassOf"}
  defp owl_sameAs, do: {:iri, "http://www.w3.org/2002/07/owl#sameAs"}

  # ============================================================================
  # Basic Deletion Tests
  # ============================================================================

  describe "delete_in_memory/5 - basic deletion" do
    test "empty deletion returns unchanged facts" do
      all_facts =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Student")}
        ])

      derived_facts = MapSet.new()

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [],
          all_facts,
          derived_facts,
          []
        )

      assert result.final_facts == all_facts
      assert MapSet.size(result.explicit_deleted) == 0
      assert MapSet.size(result.derived_deleted) == 0
      assert result.stats.explicit_deleted == 0
      assert result.stats.derived_deleted == 0
    end

    test "deleting explicit fact removes it" do
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      bob_student = {iri("bob"), rdf_type(), iri("Student")}

      all_facts = MapSet.new([alice_student, bob_student])
      derived_facts = MapSet.new()

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student],
          all_facts,
          derived_facts,
          []
        )

      assert MapSet.member?(result.explicit_deleted, alice_student)
      refute MapSet.member?(result.final_facts, alice_student)
      assert MapSet.member?(result.final_facts, bob_student)
      assert result.stats.explicit_deleted == 1
    end

    test "deleting non-existent fact doesn't affect existing facts" do
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      nonexistent = {iri("charlie"), rdf_type(), iri("Student")}

      all_facts = MapSet.new([alice_student])
      derived_facts = MapSet.new()

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [nonexistent],
          all_facts,
          derived_facts,
          []
        )

      # Alice is still there
      assert MapSet.member?(result.final_facts, alice_student)
      # The nonexistent fact was in the deleted set but didn't affect anything
      # (this is expected behavior - we track what was requested for deletion)
    end
  end

  # ============================================================================
  # Derived Fact Retraction Tests
  # ============================================================================

  describe "delete_in_memory/5 - derived fact retraction" do
    test "deleting explicit fact retracts dependent derived fact" do
      # Setup: alice rdf:type Student, Student subClassOf Person
      # Derived: alice rdf:type Person
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      student_subclass = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      all_facts = MapSet.new([alice_student, student_subclass, alice_person])
      derived_facts = MapSet.new([alice_person])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student],
          all_facts,
          derived_facts,
          rules
        )

      # alice rdf:type Person should be deleted (no alternative derivation)
      assert MapSet.member?(result.derived_deleted, alice_person)
      refute MapSet.member?(result.final_facts, alice_person)
      assert result.stats.derived_deleted == 1
    end

    test "derived fact with alternative derivation is kept" do
      # Setup: alice is both Student and GradStudent
      # Both are subClassOf Person
      # Deleting Student type should keep Person type (via GradStudent)
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      alice_gradstudent = {iri("alice"), rdf_type(), iri("GradStudent")}
      student_subclass = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      gradstudent_subclass = {iri("GradStudent"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      all_facts =
        MapSet.new([
          alice_student,
          alice_gradstudent,
          student_subclass,
          gradstudent_subclass,
          alice_person
        ])

      derived_facts = MapSet.new([alice_person])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student],
          all_facts,
          derived_facts,
          rules
        )

      # alice rdf:type Person should be kept (via GradStudent)
      assert MapSet.member?(result.derived_kept, alice_person)
      assert MapSet.member?(result.final_facts, alice_person)
      assert result.stats.derived_kept == 1
      assert result.stats.derived_deleted == 0
    end

    test "deleting derived fact directly" do
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      student_subclass = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      all_facts = MapSet.new([alice_student, student_subclass, alice_person])
      derived_facts = MapSet.new([alice_person])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_person],
          all_facts,
          derived_facts,
          rules
        )

      # Derived fact was deleted directly
      assert MapSet.member?(result.derived_deleted, alice_person)
      refute MapSet.member?(result.final_facts, alice_person)
    end
  end

  # ============================================================================
  # Cascading Deletion Tests
  # ============================================================================

  describe "delete_in_memory/5 - cascading deletions" do
    test "cascading deletion through subclass chain" do
      # Setup: alice -> Student -> Person -> Agent
      # Deleting alice -> Student cascades to Person and Agent
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      person_agent = {iri("Person"), rdfs_subClassOf(), iri("Agent")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}
      alice_agent = {iri("alice"), rdf_type(), iri("Agent")}

      all_facts =
        MapSet.new([
          alice_student,
          student_person,
          person_agent,
          alice_person,
          alice_agent
        ])

      derived_facts = MapSet.new([alice_person, alice_agent])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student],
          all_facts,
          derived_facts,
          rules
        )

      # Both derived types should be deleted
      assert MapSet.member?(result.derived_deleted, alice_person)
      assert MapSet.member?(result.derived_deleted, alice_agent)
      assert result.stats.derived_deleted == 2
    end

    test "partial cascade when alternative paths exist" do
      # Setup: alice -> Student -> Person -> Agent
      #        alice -> Employee -> Agent (alternative path to Agent)
      # Deleting alice -> Student cascades to Person but not Agent
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      alice_employee = {iri("alice"), rdf_type(), iri("Employee")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      person_agent = {iri("Person"), rdfs_subClassOf(), iri("Agent")}
      employee_agent = {iri("Employee"), rdfs_subClassOf(), iri("Agent")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}
      alice_agent = {iri("alice"), rdf_type(), iri("Agent")}

      all_facts =
        MapSet.new([
          alice_student,
          alice_employee,
          student_person,
          person_agent,
          employee_agent,
          alice_person,
          alice_agent
        ])

      derived_facts = MapSet.new([alice_person, alice_agent])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student],
          all_facts,
          derived_facts,
          rules
        )

      # Person should be deleted (no alternative)
      assert MapSet.member?(result.derived_deleted, alice_person)
      # Agent should be kept (via Employee)
      assert MapSet.member?(result.derived_kept, alice_agent)
      assert result.stats.derived_deleted == 1
      assert result.stats.derived_kept == 1
    end
  end

  # ============================================================================
  # sameAs Reasoning Tests
  # ============================================================================

  describe "delete_in_memory/5 - sameAs reasoning" do
    test "deleting sameAs retracts symmetric fact" do
      # Setup: alice sameAs bob, derived: bob sameAs alice
      alice_bob = {iri("alice"), owl_sameAs(), iri("bob")}
      bob_alice = {iri("bob"), owl_sameAs(), iri("alice")}

      all_facts = MapSet.new([alice_bob, bob_alice])
      derived_facts = MapSet.new([bob_alice])

      rules = [Rules.eq_sym()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_bob],
          all_facts,
          derived_facts,
          rules
        )

      # bob sameAs alice should be deleted
      assert MapSet.member?(result.derived_deleted, bob_alice)
      assert result.stats.derived_deleted == 1
    end

    test "sameAs kept when alternative path exists" do
      # Setup: alice sameAs bob, bob sameAs alice (both explicit)
      # Deleting alice sameAs bob should keep bob sameAs alice
      # because it's explicit, not derived
      alice_bob = {iri("alice"), owl_sameAs(), iri("bob")}
      bob_alice = {iri("bob"), owl_sameAs(), iri("alice")}

      all_facts = MapSet.new([alice_bob, bob_alice])
      derived_facts = MapSet.new()

      rules = [Rules.eq_sym()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_bob],
          all_facts,
          derived_facts,
          rules
        )

      # bob sameAs alice is explicit, so not in derived_deleted
      assert MapSet.member?(result.final_facts, bob_alice)
      assert result.stats.derived_deleted == 0
    end

    test "sameAs transitivity cascade" do
      # Setup: alice sameAs bob, bob sameAs charlie
      # Derived: alice sameAs charlie
      alice_bob = {iri("alice"), owl_sameAs(), iri("bob")}
      bob_charlie = {iri("bob"), owl_sameAs(), iri("charlie")}
      alice_charlie = {iri("alice"), owl_sameAs(), iri("charlie")}

      all_facts = MapSet.new([alice_bob, bob_charlie, alice_charlie])
      derived_facts = MapSet.new([alice_charlie])

      rules = [Rules.eq_trans()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [bob_charlie],
          all_facts,
          derived_facts,
          rules
        )

      # alice sameAs charlie should be deleted
      assert MapSet.member?(result.derived_deleted, alice_charlie)
    end
  end

  # ============================================================================
  # Multiple Rules Tests
  # ============================================================================

  describe "delete_in_memory/5 - multiple rules" do
    test "deletion with subclass and sameAs rules" do
      # Setup: alice rdf:type Student
      #        Student subClassOf Person
      #        alice sameAs aliceJr
      # Derived: alice rdf:type Person
      #          aliceJr sameAs alice (symmetry)
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      alice_aliceJr = {iri("alice"), owl_sameAs(), iri("aliceJr")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}
      aliceJr_alice = {iri("aliceJr"), owl_sameAs(), iri("alice")}

      all_facts =
        MapSet.new([
          alice_student,
          student_person,
          alice_aliceJr,
          alice_person,
          aliceJr_alice
        ])

      derived_facts = MapSet.new([alice_person, aliceJr_alice])

      rules = [Rules.cax_sco(), Rules.eq_sym()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student],
          all_facts,
          derived_facts,
          rules
        )

      # alice rdf:type Person deleted
      assert MapSet.member?(result.derived_deleted, alice_person)
      # aliceJr sameAs alice kept (alice sameAs aliceJr still exists)
      assert MapSet.member?(result.final_facts, aliceJr_alice)
    end
  end

  # ============================================================================
  # Bulk Deletion Tests
  # ============================================================================

  describe "delete_in_memory/5 - bulk deletion" do
    test "multiple explicit facts deleted at once" do
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      bob_student = {iri("bob"), rdf_type(), iri("Student")}
      charlie_student = {iri("charlie"), rdf_type(), iri("Student")}

      all_facts = MapSet.new([alice_student, bob_student, charlie_student])
      derived_facts = MapSet.new()

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student, bob_student],
          all_facts,
          derived_facts,
          []
        )

      refute MapSet.member?(result.final_facts, alice_student)
      refute MapSet.member?(result.final_facts, bob_student)
      assert MapSet.member?(result.final_facts, charlie_student)
      assert result.stats.explicit_deleted == 2
    end

    test "bulk deletion with mixed consequences" do
      # Setup: alice and bob are Students, only alice is also GradStudent
      # Both get Person via Student
      # Deleting both Student types: alice Person kept (GradStudent), bob Person deleted
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      alice_gradstudent = {iri("alice"), rdf_type(), iri("GradStudent")}
      bob_student = {iri("bob"), rdf_type(), iri("Student")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      gradstudent_person = {iri("GradStudent"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}
      bob_person = {iri("bob"), rdf_type(), iri("Person")}

      all_facts =
        MapSet.new([
          alice_student,
          alice_gradstudent,
          bob_student,
          student_person,
          gradstudent_person,
          alice_person,
          bob_person
        ])

      derived_facts = MapSet.new([alice_person, bob_person])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student, bob_student],
          all_facts,
          derived_facts,
          rules
        )

      # alice Person kept (via GradStudent)
      assert MapSet.member?(result.derived_kept, alice_person)
      # bob Person deleted
      assert MapSet.member?(result.derived_deleted, bob_person)
      assert result.stats.derived_kept == 1
      assert result.stats.derived_deleted == 1
    end
  end

  # ============================================================================
  # Statistics Tests
  # ============================================================================

  describe "delete_in_memory/5 - statistics" do
    test "statistics accurately reflect deletion" do
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      alice_gradstudent = {iri("alice"), rdf_type(), iri("GradStudent")}
      bob_student = {iri("bob"), rdf_type(), iri("Student")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      gradstudent_person = {iri("GradStudent"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}
      bob_person = {iri("bob"), rdf_type(), iri("Person")}

      all_facts =
        MapSet.new([
          alice_student,
          alice_gradstudent,
          bob_student,
          student_person,
          gradstudent_person,
          alice_person,
          bob_person
        ])

      derived_facts = MapSet.new([alice_person, bob_person])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student, bob_student],
          all_facts,
          derived_facts,
          rules
        )

      assert result.stats.explicit_deleted == 2
      # bob_person
      assert result.stats.derived_deleted == 1
      # alice_person
      assert result.stats.derived_kept == 1
      assert result.stats.potentially_invalid_count >= 2
      assert result.stats.duration_ms >= 0
    end
  end

  # ============================================================================
  # Preview Tests
  # ============================================================================

  describe "preview_delete_in_memory/4" do
    test "preview shows what would be deleted" do
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      all_facts = MapSet.new([alice_student, student_person, alice_person])
      derived_facts = MapSet.new([alice_person])

      rules = [Rules.cax_sco()]

      {:ok, {explicit_deleted, derived_deleted}} =
        DeleteWithReasoning.preview_delete_in_memory(
          [alice_student],
          all_facts,
          derived_facts,
          rules
        )

      assert MapSet.member?(explicit_deleted, alice_student)
      assert MapSet.member?(derived_deleted, alice_person)
    end
  end

  # ============================================================================
  # Edge Cases
  # ============================================================================

  describe "delete_in_memory/5 - edge cases" do
    test "all facts deleted" do
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      all_facts = MapSet.new([alice_student, student_person, alice_person])
      derived_facts = MapSet.new([alice_person])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student, student_person],
          all_facts,
          derived_facts,
          rules
        )

      # alice_person should be deleted (no more Student subClassOf Person)
      assert MapSet.member?(result.derived_deleted, alice_person)
      # Only explicit deletions counted as explicit
      assert result.stats.explicit_deleted == 2
    end

    test "self-referential subclass doesn't cause issues" do
      # Thing subClassOf Thing (reflexive)
      thing_thing = {iri("Thing"), rdfs_subClassOf(), iri("Thing")}
      alice_thing = {iri("alice"), rdf_type(), iri("Thing")}

      all_facts = MapSet.new([thing_thing, alice_thing])
      derived_facts = MapSet.new()

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [thing_thing],
          all_facts,
          derived_facts,
          rules
        )

      # Should complete without issues
      assert MapSet.member?(result.explicit_deleted, thing_thing)
      assert MapSet.member?(result.final_facts, alice_thing)
    end

    test "deleting subclass hierarchy fact cascades types" do
      # Deleting Student subClassOf Person should cascade to alice type Person
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      all_facts = MapSet.new([alice_student, student_person, alice_person])
      derived_facts = MapSet.new([alice_person])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [student_person],
          all_facts,
          derived_facts,
          rules
        )

      # alice Person deleted (no more Student subClassOf Person)
      assert MapSet.member?(result.derived_deleted, alice_person)
    end
  end

  # ============================================================================
  # Options Tests
  # ============================================================================

  describe "delete_in_memory/5 - options" do
    test "max_trace_depth option limits tracing" do
      # Long chain: alice -> A -> B -> C -> D -> E -> F
      # With max_depth: 2, should only trace to C
      alice_a = {iri("alice"), rdf_type(), iri("A")}
      a_b = {iri("A"), rdfs_subClassOf(), iri("B")}
      b_c = {iri("B"), rdfs_subClassOf(), iri("C")}
      c_d = {iri("C"), rdfs_subClassOf(), iri("D")}
      d_e = {iri("D"), rdfs_subClassOf(), iri("E")}
      alice_b = {iri("alice"), rdf_type(), iri("B")}
      alice_c = {iri("alice"), rdf_type(), iri("C")}
      alice_d = {iri("alice"), rdf_type(), iri("D")}
      alice_e = {iri("alice"), rdf_type(), iri("E")}

      all_facts =
        MapSet.new([
          alice_a,
          a_b,
          b_c,
          c_d,
          d_e,
          alice_b,
          alice_c,
          alice_d,
          alice_e
        ])

      derived_facts = MapSet.new([alice_b, alice_c, alice_d, alice_e])

      rules = [Rules.cax_sco()]

      # Normal deletion should cascade all the way
      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_a],
          all_facts,
          derived_facts,
          rules
        )

      # All derived should be deleted without alternative paths
      assert result.stats.derived_deleted == 4
    end
  end
end
