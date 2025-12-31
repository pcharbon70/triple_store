# credo:disable-for-this-file Credo.Check.Readability.FunctionNames
defmodule TripleStore.Reasoner.IncrementalMaintenanceIntegrationTest do
  @moduledoc """
  Integration tests for the complete incremental maintenance system.

  These tests verify that all components of Section 4.3 work together:
  - Incremental Addition (4.3.1)
  - Backward Trace (4.3.2)
  - Forward Re-derivation (4.3.3)
  - Delete with Reasoning (4.3.4)

  The tests cover end-to-end scenarios that exercise multiple modules
  in realistic usage patterns.
  """

  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.BackwardTrace
  alias TripleStore.Reasoner.DeleteWithReasoning
  alias TripleStore.Reasoner.ForwardRederive
  alias TripleStore.Reasoner.Incremental
  alias TripleStore.Reasoner.Rules

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp iri(value), do: {:iri, "http://example.org/#{value}"}

  defp rdf_type, do: {:iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}
  defp rdfs_subClassOf, do: {:iri, "http://www.w3.org/2000/01/rdf-schema#subClassOf"}
  defp owl_sameAs, do: {:iri, "http://www.w3.org/2002/07/owl#sameAs"}

  # ============================================================================
  # End-to-End Workflow Tests
  # ============================================================================

  describe "incremental addition derives new consequences" do
    test "adding instance fact derives type through subclass hierarchy" do
      # Schema: Student subClassOf Person subClassOf Agent
      schema =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")},
          {iri("Person"), rdfs_subClassOf(), iri("Agent")}
        ])

      rules = [Rules.cax_sco(), Rules.scm_sco()]

      # Add alice as Student
      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, schema, rules)

      # Should derive Person and Agent types
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Person")})
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Agent")})
      assert stats.derived_count >= 2
    end

    test "adding subclass fact derives transitive closure" do
      existing =
        MapSet.new([
          {iri("A"), rdfs_subClassOf(), iri("B")},
          {iri("C"), rdfs_subClassOf(), iri("D")}
        ])

      rules = [Rules.scm_sco()]

      # Add B subClassOf C, creating chain A -> B -> C -> D
      new_triples = [{iri("B"), rdfs_subClassOf(), iri("C")}]

      {:ok, all_facts, _stats} = Incremental.add_in_memory(new_triples, existing, rules)

      # Should derive A subClassOf C, A subClassOf D, B subClassOf D
      assert MapSet.member?(all_facts, {iri("A"), rdfs_subClassOf(), iri("C")})
      assert MapSet.member?(all_facts, {iri("A"), rdfs_subClassOf(), iri("D")})
      assert MapSet.member?(all_facts, {iri("B"), rdfs_subClassOf(), iri("D")})
    end

    test "adding sameAs derives symmetric and transitive facts" do
      existing =
        MapSet.new([
          {iri("bob"), owl_sameAs(), iri("robert")}
        ])

      rules = [Rules.eq_sym(), Rules.eq_trans()]

      # Add alice sameAs bob
      new_triples = [{iri("alice"), owl_sameAs(), iri("bob")}]

      {:ok, all_facts, _stats} = Incremental.add_in_memory(new_triples, existing, rules)

      # Should derive bob sameAs alice (symmetry)
      assert MapSet.member?(all_facts, {iri("bob"), owl_sameAs(), iri("alice")})
      # Should derive alice sameAs robert (transitivity via bob)
      assert MapSet.member?(all_facts, {iri("alice"), owl_sameAs(), iri("robert")})
    end
  end

  describe "backward trace finds dependent derivations" do
    test "trace finds direct dependents" do
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      _student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      all_derived = MapSet.new([alice_person])
      deleted = MapSet.new([alice_student])

      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, all_derived, rules)

      assert MapSet.member?(result.potentially_invalid, alice_person)
    end

    test "trace finds transitive dependents through chain" do
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}
      alice_agent = {iri("alice"), rdf_type(), iri("Agent")}

      # Both Person and Agent are derived
      all_derived = MapSet.new([alice_person, alice_agent])
      deleted = MapSet.new([alice_student])

      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, all_derived, rules)

      # Both should be potentially invalid
      assert MapSet.member?(result.potentially_invalid, alice_person)
      assert MapSet.member?(result.potentially_invalid, alice_agent)
    end
  end

  describe "forward phase re-derives facts with alternatives" do
    test "fact with alternative path is kept" do
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      potentially_invalid = MapSet.new([alice_person])

      # Alice has GradStudent type, and GradStudent subClassOf Person
      all_facts =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("GradStudent")},
          {iri("GradStudent"), rdfs_subClassOf(), iri("Person")},
          alice_person
        ])

      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        ForwardRederive.rederive_in_memory(
          potentially_invalid,
          all_facts,
          deleted,
          rules
        )

      assert MapSet.member?(result.keep, alice_person)
      assert MapSet.size(result.delete) == 0
    end

    test "fact without alternative path is deleted" do
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      potentially_invalid = MapSet.new([alice_person])

      # Only the subclass hierarchy remains, no instance type
      all_facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")},
          alice_person
        ])

      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        ForwardRederive.rederive_in_memory(
          potentially_invalid,
          all_facts,
          deleted,
          rules
        )

      assert MapSet.member?(result.delete, alice_person)
      assert MapSet.size(result.keep) == 0
    end
  end

  describe "delete removes derived facts without alternatives" do
    test "complete deletion workflow removes invalid derivations" do
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      all_facts = MapSet.new([alice_student, student_person, alice_person])
      derived_facts = MapSet.new([alice_person])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student],
          all_facts,
          derived_facts,
          rules
        )

      # alice Person should be deleted
      assert MapSet.member?(result.derived_deleted, alice_person)
      refute MapSet.member?(result.final_facts, alice_person)
      # Schema should remain
      assert MapSet.member?(result.final_facts, student_person)
    end
  end

  describe "delete preserves derived facts with alternatives" do
    test "deletion preserves facts re-derivable via alternative paths" do
      # alice is both Student and GradStudent
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      alice_gradstudent = {iri("alice"), rdf_type(), iri("GradStudent")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      gradstudent_person = {iri("GradStudent"), rdfs_subClassOf(), iri("Person")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}

      all_facts =
        MapSet.new([
          alice_student,
          alice_gradstudent,
          student_person,
          gradstudent_person,
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

      # alice Person should be kept (via GradStudent)
      assert MapSet.member?(result.derived_kept, alice_person)
      assert MapSet.member?(result.final_facts, alice_person)
    end
  end

  describe "cascading delete handles chains correctly" do
    test "deletion cascades through derivation chain" do
      # alice -> Student -> Person -> Agent
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

    test "partial cascade when middle of chain has alternative" do
      # alice -> Student -> Person -> Agent
      # alice -> Employee -> Agent (alternative path to Agent only)
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
    end
  end

  describe "bulk operations" do
    test "bulk addition processes multiple facts efficiently" do
      schema =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      # Add multiple students at once
      new_triples =
        for i <- 1..100 do
          {iri("student#{i}"), rdf_type(), iri("Student")}
        end

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, schema, rules)

      # All students should have Person type derived
      for i <- 1..100 do
        assert MapSet.member?(all_facts, {iri("student#{i}"), rdf_type(), iri("Person")})
      end

      assert stats.explicit_added == 100
      assert stats.derived_count == 100
    end

    test "bulk deletion handles many facts correctly" do
      # Create 50 students and their derived Person types
      student_facts =
        for i <- 1..50 do
          {iri("student#{i}"), rdf_type(), iri("Student")}
        end

      person_facts =
        for i <- 1..50 do
          {iri("student#{i}"), rdf_type(), iri("Person")}
        end

      schema = MapSet.new([{iri("Student"), rdfs_subClassOf(), iri("Person")}])

      all_facts = MapSet.new(student_facts ++ person_facts) |> MapSet.union(schema)
      derived_facts = MapSet.new(person_facts)

      rules = [Rules.cax_sco()]

      # Delete all student facts
      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          student_facts,
          all_facts,
          derived_facts,
          rules
        )

      # All Person types should be deleted
      for i <- 1..50 do
        refute MapSet.member?(result.final_facts, {iri("student#{i}"), rdf_type(), iri("Person")})
      end

      assert result.stats.explicit_deleted == 50
      assert result.stats.derived_deleted == 50
    end
  end

  # ============================================================================
  # Add-then-Delete Workflow Tests
  # ============================================================================

  describe "add-then-delete workflow" do
    test "add and delete returns to original state" do
      schema =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      # Add alice as Student
      new_triple = {iri("alice"), rdf_type(), iri("Student")}
      {:ok, after_add, _} = Incremental.add_in_memory([new_triple], schema, rules)

      # Verify derivation happened
      alice_person = {iri("alice"), rdf_type(), iri("Person")}
      assert MapSet.member?(after_add, alice_person)

      # Now delete alice as Student
      derived_facts = MapSet.new([alice_person])

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [new_triple],
          after_add,
          derived_facts,
          rules
        )

      # Should be back to just schema
      assert result.final_facts == schema
    end

    test "partial delete after multiple additions" do
      schema =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")},
          {iri("GradStudent"), rdfs_subClassOf(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      # Add alice as both Student and GradStudent
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      alice_gradstudent = {iri("alice"), rdf_type(), iri("GradStudent")}

      {:ok, after_add1, _} = Incremental.add_in_memory([alice_student], schema, rules)
      {:ok, after_add2, _} = Incremental.add_in_memory([alice_gradstudent], after_add1, rules)

      alice_person = {iri("alice"), rdf_type(), iri("Person")}
      assert MapSet.member?(after_add2, alice_person)

      # Delete only Student type
      derived_facts = MapSet.new([alice_person])

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student],
          after_add2,
          derived_facts,
          rules
        )

      # Person should still be there (via GradStudent)
      assert MapSet.member?(result.final_facts, alice_person)
      assert MapSet.member?(result.derived_kept, alice_person)
    end
  end

  # ============================================================================
  # Complex Scenario Tests
  # ============================================================================

  describe "complex multi-rule scenarios" do
    test "combined subclass and sameAs reasoning" do
      # alice is Student, bob sameAs alice
      # Student subClassOf Person
      # bob should inherit Person type via sameAs + cax-sco
      schema =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      rules = [Rules.cax_sco(), Rules.eq_sym()]

      # First add alice as Student
      {:ok, after_alice, _} =
        Incremental.add_in_memory(
          [{iri("alice"), rdf_type(), iri("Student")}],
          schema,
          rules
        )

      # Verify alice is Person
      assert MapSet.member?(after_alice, {iri("alice"), rdf_type(), iri("Person")})

      # Add bob sameAs alice
      {:ok, after_bob, _} =
        Incremental.add_in_memory(
          [{iri("bob"), owl_sameAs(), iri("alice")}],
          after_alice,
          rules
        )

      # Verify symmetry: alice sameAs bob
      assert MapSet.member?(after_bob, {iri("alice"), owl_sameAs(), iri("bob")})
    end

    test "multiple independent derivation paths" do
      # alice is Student and Employee
      # Student -> Person, Employee -> Worker
      # Deleting Student should only affect Person, not Worker
      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      alice_employee = {iri("alice"), rdf_type(), iri("Employee")}
      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      employee_worker = {iri("Employee"), rdfs_subClassOf(), iri("Worker")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}
      alice_worker = {iri("alice"), rdf_type(), iri("Worker")}

      all_facts =
        MapSet.new([
          alice_student,
          alice_employee,
          student_person,
          employee_worker,
          alice_person,
          alice_worker
        ])

      derived_facts = MapSet.new([alice_person, alice_worker])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student],
          all_facts,
          derived_facts,
          rules
        )

      # Person deleted, Worker kept
      assert MapSet.member?(result.derived_deleted, alice_person)
      refute MapSet.member?(result.derived_deleted, alice_worker)
      assert MapSet.member?(result.final_facts, alice_worker)
    end
  end

  # ============================================================================
  # Edge Case Tests
  # ============================================================================

  describe "edge cases" do
    test "empty operations" do
      empty = MapSet.new()

      {:ok, add_result, _} = Incremental.add_in_memory([], empty, [])
      assert add_result == empty

      {:ok, delete_result} = DeleteWithReasoning.delete_in_memory([], empty, empty, [])
      assert delete_result.final_facts == empty
    end

    test "no rules means no derivations" do
      existing =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, [])

      # No derivations without rules
      assert stats.derived_count == 0
      refute MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Person")})
    end

    test "self-referential class hierarchies" do
      # Thing is its own superclass
      thing_thing = {iri("Thing"), rdfs_subClassOf(), iri("Thing")}
      alice_thing = {iri("alice"), rdf_type(), iri("Thing")}

      all_facts = MapSet.new([thing_thing, alice_thing])

      rules = [Rules.cax_sco(), Rules.scm_sco()]

      # Should handle without infinite loop
      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [thing_thing],
          all_facts,
          MapSet.new(),
          rules
        )

      assert MapSet.member?(result.final_facts, alice_thing)
    end

    test "diamond inheritance pattern" do
      # Classic diamond: Student and Employee both subClassOf Person and Worker
      #                  Person and Worker both subClassOf Agent
      #
      # alice is Student -> deleting should cascade through Person to Agent
      # but Agent might be re-derivable via other paths

      student_person = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      student_worker = {iri("Student"), rdfs_subClassOf(), iri("Worker")}
      person_agent = {iri("Person"), rdfs_subClassOf(), iri("Agent")}
      worker_agent = {iri("Worker"), rdfs_subClassOf(), iri("Agent")}

      alice_student = {iri("alice"), rdf_type(), iri("Student")}
      alice_person = {iri("alice"), rdf_type(), iri("Person")}
      alice_worker = {iri("alice"), rdf_type(), iri("Worker")}
      alice_agent = {iri("alice"), rdf_type(), iri("Agent")}

      all_facts =
        MapSet.new([
          student_person,
          student_worker,
          person_agent,
          worker_agent,
          alice_student,
          alice_person,
          alice_worker,
          alice_agent
        ])

      derived_facts = MapSet.new([alice_person, alice_worker, alice_agent])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_student],
          all_facts,
          derived_facts,
          rules
        )

      # All derived types should be deleted (all came from Student)
      assert MapSet.member?(result.derived_deleted, alice_person)
      assert MapSet.member?(result.derived_deleted, alice_worker)
      assert MapSet.member?(result.derived_deleted, alice_agent)
    end
  end

  # ============================================================================
  # Statistics Verification Tests
  # ============================================================================

  describe "statistics accuracy" do
    test "addition statistics are accurate" do
      schema =
        MapSet.new([
          {iri("A"), rdfs_subClassOf(), iri("B")},
          {iri("B"), rdfs_subClassOf(), iri("C")}
        ])

      rules = [Rules.cax_sco()]

      new_triples = [{iri("x"), rdf_type(), iri("A")}]

      {:ok, _all_facts, stats} = Incremental.add_in_memory(new_triples, schema, rules)

      assert stats.explicit_added == 1
      # x type B, x type C
      assert stats.derived_count == 2
      assert stats.iterations >= 1
      assert stats.duration_ms >= 0
    end

    test "deletion statistics are accurate" do
      alice_a = {iri("alice"), rdf_type(), iri("A")}
      a_b = {iri("A"), rdfs_subClassOf(), iri("B")}
      alice_b = {iri("alice"), rdf_type(), iri("B")}

      all_facts = MapSet.new([alice_a, a_b, alice_b])
      derived_facts = MapSet.new([alice_b])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        DeleteWithReasoning.delete_in_memory(
          [alice_a],
          all_facts,
          derived_facts,
          rules
        )

      assert result.stats.explicit_deleted == 1
      assert result.stats.derived_deleted == 1
      assert result.stats.derived_kept == 0
      assert result.stats.potentially_invalid_count >= 1
      assert result.stats.duration_ms >= 0
    end
  end
end
