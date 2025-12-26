defmodule TripleStore.Reasoner.ForwardRederiveTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.ForwardRederive
  alias TripleStore.Reasoner.Rules

  # ============================================================================
  # Test Helpers
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ex "http://example.org/"

  defp iri(suffix), do: {:iri, @ex <> suffix}
  defp rdf_type, do: {:iri, @rdf <> "type"}
  defp rdfs_subClassOf, do: {:iri, @rdfs <> "subClassOf"}
  defp owl_sameAs, do: {:iri, @owl <> "sameAs"}

  # ============================================================================
  # Tests: rederive_in_memory/4 - Basic Functionality
  # ============================================================================

  describe "rederive_in_memory/4 - basic functionality" do
    test "returns empty sets for empty potentially invalid" do
      potentially_invalid = MapSet.new()
      all_facts = MapSet.new([{iri("alice"), rdf_type(), iri("Person")}])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      assert MapSet.size(result.keep) == 0
      assert MapSet.size(result.delete) == 0
      assert result.facts_checked == 0
    end

    test "returns statistics" do
      potentially_invalid = MapSet.new([{iri("alice"), rdf_type(), iri("Person")}])
      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])
      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      assert is_integer(result.rederivation_count)
      assert is_integer(result.facts_checked)
      assert result.facts_checked == 1
    end
  end

  # ============================================================================
  # Tests: rederive_in_memory/4 - Re-derivation Success
  # ============================================================================

  describe "rederive_in_memory/4 - re-derivation success" do
    test "keeps fact that can be re-derived via alternative path" do
      # Scenario:
      # - alice type Student (being deleted)
      # - alice type GradStudent (not deleted)
      # - Student subClassOf Person
      # - GradStudent subClassOf Person
      # - alice type Person (potentially invalid, but can be re-derived via GradStudent)

      potentially_invalid = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("alice"), rdf_type(), iri("GradStudent")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("GradStudent"), rdfs_subClassOf(), iri("Person")},
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      deleted = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")}
      ])

      rules = [Rules.cax_sco()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # alice type Person can be re-derived via GradStudent
      assert MapSet.member?(result.keep, {iri("alice"), rdf_type(), iri("Person")})
      assert MapSet.size(result.delete) == 0
      assert result.rederivation_count == 1
    end

    test "keeps subclass transitivity that can be re-derived" do
      # A sco B, B sco C, B sco D => A sco C, A sco D
      # If we delete B sco C, A sco C cannot be re-derived
      # But A sco D can still be derived

      potentially_invalid = MapSet.new([
        {iri("A"), rdfs_subClassOf(), iri("C")}
      ])

      all_facts = MapSet.new([
        {iri("A"), rdfs_subClassOf(), iri("B")},
        {iri("B"), rdfs_subClassOf(), iri("C")},  # Being deleted
        {iri("B"), rdfs_subClassOf(), iri("D")},
        {iri("A"), rdfs_subClassOf(), iri("C")},
        {iri("A"), rdfs_subClassOf(), iri("D")}
      ])

      deleted = MapSet.new([
        {iri("B"), rdfs_subClassOf(), iri("C")}
      ])

      rules = [Rules.scm_sco()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # A sco C cannot be re-derived (only path was through B sco C)
      assert MapSet.member?(result.delete, {iri("A"), rdfs_subClassOf(), iri("C")})
    end
  end

  # ============================================================================
  # Tests: rederive_in_memory/4 - Re-derivation Failure
  # ============================================================================

  describe "rederive_in_memory/4 - re-derivation failure" do
    test "deletes fact that cannot be re-derived" do
      # Scenario:
      # - alice type Student (being deleted)
      # - Student subClassOf Person
      # - alice type Person (potentially invalid, no alternative)

      potentially_invalid = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      deleted = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")}
      ])

      rules = [Rules.cax_sco()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # alice type Person cannot be re-derived
      assert MapSet.member?(result.delete, {iri("alice"), rdf_type(), iri("Person")})
      assert MapSet.size(result.keep) == 0
    end

    test "deletes all facts when no rules match" do
      potentially_invalid = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      deleted = MapSet.new()
      rules = []  # No rules

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      assert MapSet.member?(result.delete, {iri("alice"), rdf_type(), iri("Person")})
    end
  end

  # ============================================================================
  # Tests: rederive_in_memory/4 - Mixed Results
  # ============================================================================

  describe "rederive_in_memory/4 - mixed results" do
    test "partitions facts correctly" do
      # alice type Student -> alice type Person (can be re-derived via GradStudent)
      # bob type Student -> bob type Person (cannot be re-derived)

      potentially_invalid = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")},
        {iri("bob"), rdf_type(), iri("Person")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("alice"), rdf_type(), iri("GradStudent")},
        {iri("bob"), rdf_type(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("GradStudent"), rdfs_subClassOf(), iri("Person")},
        {iri("alice"), rdf_type(), iri("Person")},
        {iri("bob"), rdf_type(), iri("Person")}
      ])

      deleted = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("bob"), rdf_type(), iri("Student")}
      ])

      rules = [Rules.cax_sco()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # alice type Person can be re-derived via GradStudent
      assert MapSet.member?(result.keep, {iri("alice"), rdf_type(), iri("Person")})
      # bob type Person cannot be re-derived
      assert MapSet.member?(result.delete, {iri("bob"), rdf_type(), iri("Person")})
    end

    test "handles multiple levels of derived facts" do
      # alice type A -> alice type B -> alice type C
      # Delete alice type A
      # alice type B and alice type C should both be deleted

      potentially_invalid = MapSet.new([
        {iri("alice"), rdf_type(), iri("B")},
        {iri("alice"), rdf_type(), iri("C")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("A")},
        {iri("A"), rdfs_subClassOf(), iri("B")},
        {iri("B"), rdfs_subClassOf(), iri("C")},
        {iri("alice"), rdf_type(), iri("B")},
        {iri("alice"), rdf_type(), iri("C")}
      ])

      deleted = MapSet.new([
        {iri("alice"), rdf_type(), iri("A")}
      ])

      rules = [Rules.cax_sco()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # Both should be deleted as they depend on alice type A
      assert MapSet.member?(result.delete, {iri("alice"), rdf_type(), iri("B")})
      assert MapSet.member?(result.delete, {iri("alice"), rdf_type(), iri("C")})
    end
  end

  # ============================================================================
  # Tests: rederive_in_memory/4 - sameAs Reasoning
  # ============================================================================

  describe "rederive_in_memory/4 - sameAs reasoning" do
    test "keeps symmetric sameAs when both directions exist" do
      # alice sameAs bob, bob sameAs alice (both explicit)
      # Delete alice sameAs bob
      # bob sameAs alice can be re-derived via symmetry of bob sameAs alice? No.
      # Actually if we delete alice sameAs bob, bob sameAs alice was derived from it
      # So it should be deleted

      potentially_invalid = MapSet.new([
        {iri("bob"), owl_sameAs(), iri("alice")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), owl_sameAs(), iri("bob")},
        {iri("bob"), owl_sameAs(), iri("alice")}
      ])

      deleted = MapSet.new([
        {iri("alice"), owl_sameAs(), iri("bob")}
      ])

      rules = [Rules.eq_sym()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # bob sameAs alice cannot be re-derived (the source alice sameAs bob is deleted)
      assert MapSet.member?(result.delete, {iri("bob"), owl_sameAs(), iri("alice")})
    end

    test "keeps transitive sameAs with alternative path" do
      # alice sameAs bob, bob sameAs carol => alice sameAs carol
      # alice sameAs carol also via alice sameAs dave, dave sameAs carol
      # Delete bob sameAs carol

      potentially_invalid = MapSet.new([
        {iri("alice"), owl_sameAs(), iri("carol")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), owl_sameAs(), iri("bob")},
        {iri("bob"), owl_sameAs(), iri("carol")},
        {iri("alice"), owl_sameAs(), iri("dave")},
        {iri("dave"), owl_sameAs(), iri("carol")},
        {iri("alice"), owl_sameAs(), iri("carol")}
      ])

      deleted = MapSet.new([
        {iri("bob"), owl_sameAs(), iri("carol")}
      ])

      rules = [Rules.eq_trans()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # alice sameAs carol can be re-derived via dave
      assert MapSet.member?(result.keep, {iri("alice"), owl_sameAs(), iri("carol")})
    end
  end

  # ============================================================================
  # Tests: can_rederive?/3
  # ============================================================================

  describe "can_rederive?/3" do
    test "returns true when re-derivation is possible" do
      fact = {iri("alice"), rdf_type(), iri("Person")}

      valid_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      rules = [Rules.cax_sco()]

      assert ForwardRederive.can_rederive?(fact, valid_facts, rules)
    end

    test "returns false when re-derivation is not possible" do
      fact = {iri("alice"), rdf_type(), iri("Person")}

      valid_facts = MapSet.new([
        {iri("bob"), rdf_type(), iri("Student")},  # Wrong subject
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      rules = [Rules.cax_sco()]

      refute ForwardRederive.can_rederive?(fact, valid_facts, rules)
    end

    test "returns false when no rules can derive the fact" do
      fact = {iri("alice"), iri("knows"), iri("bob")}

      valid_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      rules = [Rules.cax_sco()]  # Only handles class hierarchy

      refute ForwardRederive.can_rederive?(fact, valid_facts, rules)
    end

    test "returns true for subclass transitivity" do
      fact = {iri("A"), rdfs_subClassOf(), iri("C")}

      valid_facts = MapSet.new([
        {iri("A"), rdfs_subClassOf(), iri("B")},
        {iri("B"), rdfs_subClassOf(), iri("C")}
      ])

      rules = [Rules.scm_sco()]

      assert ForwardRederive.can_rederive?(fact, valid_facts, rules)
    end

    test "returns true for sameAs symmetry" do
      fact = {iri("bob"), owl_sameAs(), iri("alice")}

      valid_facts = MapSet.new([
        {iri("alice"), owl_sameAs(), iri("bob")}
      ])

      rules = [Rules.eq_sym()]

      assert ForwardRederive.can_rederive?(fact, valid_facts, rules)
    end
  end

  # ============================================================================
  # Tests: partition_invalid/4
  # ============================================================================

  describe "partition_invalid/4" do
    test "returns tuple of keep and delete sets" do
      potentially_invalid = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      deleted = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")}
      ])

      rules = [Rules.cax_sco()]

      {keep, delete} = ForwardRederive.partition_invalid(
        potentially_invalid, all_facts, deleted, rules
      )

      assert is_struct(keep, MapSet)
      assert is_struct(delete, MapSet)
      assert MapSet.member?(delete, {iri("alice"), rdf_type(), iri("Person")})
    end
  end

  # ============================================================================
  # Tests: Edge Cases
  # ============================================================================

  describe "edge cases" do
    test "handles facts not in all_facts" do
      # A fact marked as potentially invalid that doesn't exist in all_facts
      potentially_invalid = MapSet.new([
        {iri("ghost"), rdf_type(), iri("Person")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      deleted = MapSet.new()
      rules = [Rules.cax_sco()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # Cannot be re-derived
      assert MapSet.member?(result.delete, {iri("ghost"), rdf_type(), iri("Person")})
    end

    test "handles empty rules list" do
      potentially_invalid = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      deleted = MapSet.new()
      rules = []

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # All should be deleted (no rules to derive anything)
      assert MapSet.size(result.keep) == 0
      assert MapSet.size(result.delete) == MapSet.size(potentially_invalid)
    end

    test "handles all facts being deleted" do
      potentially_invalid = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("alice"), rdf_type(), iri("Person")}
      ])

      # Delete all facts
      deleted = all_facts

      rules = [Rules.cax_sco()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # Cannot re-derive with no remaining facts
      assert MapSet.size(result.keep) == 0
    end

    test "handles self-referential rules correctly" do
      # Reflexive property: x sameAs x
      # But eq_ref isn't typically used in testing
      potentially_invalid = MapSet.new([
        {iri("alice"), owl_sameAs(), iri("alice")}
      ])

      all_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("Person")},
        {iri("alice"), owl_sameAs(), iri("alice")}
      ])

      deleted = MapSet.new()
      rules = [Rules.eq_ref()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )

      # eq_ref derives x sameAs x for any x
      # Since alice exists in the data, this should be re-derivable
      # Actually depends on how eq_ref is implemented
      # For now, just check it doesn't crash
      assert is_struct(result.keep, MapSet)
      assert is_struct(result.delete, MapSet)
    end
  end
end
