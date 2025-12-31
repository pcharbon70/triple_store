# credo:disable-for-this-file Credo.Check.Readability.FunctionNames
defmodule TripleStore.Reasoner.BackwardTraceTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.BackwardTrace
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
  # Tests: trace_in_memory/4 - Basic Functionality
  # ============================================================================

  describe "trace_in_memory/4 - basic functionality" do
    test "returns empty result for empty deleted set" do
      deleted = MapSet.new()
      derived = MapSet.new([{iri("alice"), rdf_type(), iri("Person")}])
      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.size(result.potentially_invalid) == 0
      assert result.trace_depth == 0
      assert result.facts_examined == 0
    end

    test "returns empty result for empty derived set" do
      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])
      derived = MapSet.new()
      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.size(result.potentially_invalid) == 0
    end

    test "returns empty result for empty rules" do
      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])
      derived = MapSet.new([{iri("alice"), rdf_type(), iri("Person")}])
      rules = []

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.size(result.potentially_invalid) == 0
    end

    test "returns trace statistics" do
      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])
      derived = MapSet.new([{iri("alice"), rdf_type(), iri("Person")}])
      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert is_integer(result.trace_depth)
      assert is_integer(result.facts_examined)
      assert result.facts_examined >= 0
    end
  end

  # ============================================================================
  # Tests: trace_in_memory/4 - Class Hierarchy Dependencies
  # ============================================================================

  describe "trace_in_memory/4 - class hierarchy" do
    test "finds derived class membership when instance type is deleted" do
      # Scenario: alice rdf:type Student
      #           Student rdfs:subClassOf Person
      #           Derived: alice rdf:type Person
      # Deleting: alice rdf:type Student
      # Should find: alice rdf:type Person as potentially invalid

      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])

      # The derived store contains the derived fact
      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.member?(result.potentially_invalid, {iri("alice"), rdf_type(), iri("Person")})
    end

    test "finds derived class membership when subclass relationship is deleted" do
      # Scenario: alice rdf:type Student
      #           Student rdfs:subClassOf Person
      #           Derived: alice rdf:type Person
      # Deleting: Student rdfs:subClassOf Person
      # Should find: alice rdf:type Person as potentially invalid

      deleted = MapSet.new([{iri("Student"), rdfs_subClassOf(), iri("Person")}])

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.member?(result.potentially_invalid, {iri("alice"), rdf_type(), iri("Person")})
    end

    test "finds transitive class memberships" do
      # Scenario: alice rdf:type Student
      #           Student rdfs:subClassOf Person
      #           Person rdfs:subClassOf Animal
      #           Derived: alice rdf:type Person, alice rdf:type Animal
      # Deleting: alice rdf:type Student
      # Should find: both derived facts

      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")},
          {iri("alice"), rdf_type(), iri("Animal")}
        ])

      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      # Direct dependency
      assert MapSet.member?(result.potentially_invalid, {iri("alice"), rdf_type(), iri("Person")})
      # Transitive dependency (alice type Person led to alice type Animal)
      assert MapSet.member?(result.potentially_invalid, {iri("alice"), rdf_type(), iri("Animal")})
    end
  end

  # ============================================================================
  # Tests: trace_in_memory/4 - Subclass Transitivity Dependencies
  # ============================================================================

  describe "trace_in_memory/4 - subclass transitivity" do
    test "finds derived subclass when middle class is removed" do
      # Scenario: Student rdfs:subClassOf Person
      #           Person rdfs:subClassOf Animal
      #           Derived: Student rdfs:subClassOf Animal
      # Deleting: Person rdfs:subClassOf Animal
      # Should find: Student rdfs:subClassOf Animal

      deleted = MapSet.new([{iri("Person"), rdfs_subClassOf(), iri("Animal")}])

      derived =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Animal")}
        ])

      rules = [Rules.scm_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.member?(
               result.potentially_invalid,
               {iri("Student"), rdfs_subClassOf(), iri("Animal")}
             )
    end

    test "finds multiple derived subclass relationships" do
      # Scenario: A sco B, B sco C, C sco D
      #           Derived: A sco C, A sco D, B sco D
      # Deleting: B sco C
      # Should find: A sco C, A sco D (depends on A sco C), B sco D

      deleted = MapSet.new([{iri("B"), rdfs_subClassOf(), iri("C")}])

      derived =
        MapSet.new([
          {iri("A"), rdfs_subClassOf(), iri("C")},
          {iri("A"), rdfs_subClassOf(), iri("D")},
          {iri("B"), rdfs_subClassOf(), iri("D")}
        ])

      rules = [Rules.scm_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      # Direct dependencies
      assert MapSet.member?(result.potentially_invalid, {iri("A"), rdfs_subClassOf(), iri("C")})
      assert MapSet.member?(result.potentially_invalid, {iri("B"), rdfs_subClassOf(), iri("D")})
    end
  end

  # ============================================================================
  # Tests: trace_in_memory/4 - sameAs Dependencies
  # ============================================================================

  describe "trace_in_memory/4 - sameAs reasoning" do
    test "finds symmetric sameAs when original is deleted" do
      # Scenario: alice sameAs alicia
      #           Derived: alicia sameAs alice (symmetry)
      # Deleting: alice sameAs alicia
      # Should find: alicia sameAs alice

      deleted = MapSet.new([{iri("alice"), owl_sameAs(), iri("alicia")}])

      derived =
        MapSet.new([
          {iri("alicia"), owl_sameAs(), iri("alice")}
        ])

      rules = [Rules.eq_sym()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.member?(
               result.potentially_invalid,
               {iri("alicia"), owl_sameAs(), iri("alice")}
             )
    end

    test "finds transitive sameAs when link is deleted" do
      # Scenario: alice sameAs bob, bob sameAs carol
      #           Derived: alice sameAs carol
      # Deleting: bob sameAs carol
      # Should find: alice sameAs carol

      deleted = MapSet.new([{iri("bob"), owl_sameAs(), iri("carol")}])

      derived =
        MapSet.new([
          {iri("alice"), owl_sameAs(), iri("carol")}
        ])

      rules = [Rules.eq_trans()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.member?(
               result.potentially_invalid,
               {iri("alice"), owl_sameAs(), iri("carol")}
             )
    end
  end

  # ============================================================================
  # Tests: trace_in_memory/4 - Recursive Dependencies
  # ============================================================================

  describe "trace_in_memory/4 - recursive dependencies" do
    test "traces through multiple levels of dependencies" do
      # Scenario: Chain of derived facts where each depends on previous
      # alice type Student -> alice type Person -> alice type Animal
      # Deleting alice type Student should find both derived facts

      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")},
          {iri("alice"), rdf_type(), iri("Animal")}
        ])

      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.member?(result.potentially_invalid, {iri("alice"), rdf_type(), iri("Person")})
      assert MapSet.member?(result.potentially_invalid, {iri("alice"), rdf_type(), iri("Animal")})
      assert result.trace_depth >= 1
    end

    test "handles cycles without infinite loop" do
      # Scenario: Circular sameAs (shouldn't happen but we handle it)
      # a sameAs b, b sameAs a
      # Should terminate and not loop forever

      deleted = MapSet.new([{iri("a"), owl_sameAs(), iri("b")}])

      derived =
        MapSet.new([
          {iri("b"), owl_sameAs(), iri("a")}
        ])

      rules = [Rules.eq_sym()]

      # Should complete without hanging
      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.member?(result.potentially_invalid, {iri("b"), owl_sameAs(), iri("a")})
    end
  end

  # ============================================================================
  # Tests: trace_in_memory/4 - Options
  # ============================================================================

  describe "trace_in_memory/4 - options" do
    test "respects max_depth option" do
      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("A")}])

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("B")},
          {iri("alice"), rdf_type(), iri("C")},
          {iri("alice"), rdf_type(), iri("D")}
        ])

      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules, max_depth: 1)

      # With max_depth 1, should still find direct dependencies
      assert result.trace_depth <= 1
    end

    test "include_deleted option adds deleted facts to result" do
      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        BackwardTrace.trace_in_memory(deleted, derived, rules, include_deleted: true)

      # Should include the deleted fact itself
      assert MapSet.member?(
               result.potentially_invalid,
               {iri("alice"), rdf_type(), iri("Student")}
             )
    end

    test "include_deleted false excludes deleted facts" do
      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      {:ok, result} =
        BackwardTrace.trace_in_memory(deleted, derived, rules, include_deleted: false)

      # Should NOT include the deleted fact
      refute MapSet.member?(
               result.potentially_invalid,
               {iri("alice"), rdf_type(), iri("Student")}
             )

      # But should include the derived fact
      assert MapSet.member?(result.potentially_invalid, {iri("alice"), rdf_type(), iri("Person")})
    end
  end

  # ============================================================================
  # Tests: find_direct_dependents/3
  # ============================================================================

  describe "find_direct_dependents/3" do
    test "finds single direct dependent" do
      fact = {iri("alice"), rdf_type(), iri("Student")}

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      dependents = BackwardTrace.find_direct_dependents(fact, derived, rules)

      assert MapSet.member?(dependents, {iri("alice"), rdf_type(), iri("Person")})
    end

    test "finds multiple direct dependents" do
      # If alice is Student and Student sco Person and Student sco Human
      # Then deleting alice type Student affects both derivations
      fact = {iri("alice"), rdf_type(), iri("Student")}

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")},
          {iri("alice"), rdf_type(), iri("Human")}
        ])

      rules = [Rules.cax_sco()]

      dependents = BackwardTrace.find_direct_dependents(fact, derived, rules)

      assert MapSet.member?(dependents, {iri("alice"), rdf_type(), iri("Person")})
      assert MapSet.member?(dependents, {iri("alice"), rdf_type(), iri("Human")})
    end

    test "returns empty for unrelated facts" do
      fact = {iri("alice"), iri("knows"), iri("bob")}

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      dependents = BackwardTrace.find_direct_dependents(fact, derived, rules)

      assert MapSet.size(dependents) == 0
    end
  end

  # ============================================================================
  # Tests: could_derive?/4
  # ============================================================================

  describe "could_derive?/4" do
    test "returns true when derivation is possible" do
      derived = {iri("alice"), rdf_type(), iri("Person")}
      input = {iri("alice"), rdf_type(), iri("Student")}

      all_facts =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Student")},
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      rule = Rules.cax_sco()

      assert BackwardTrace.could_derive?(derived, input, rule, all_facts)
    end

    test "returns false when derivation is not possible" do
      derived = {iri("bob"), rdf_type(), iri("Person")}
      input = {iri("alice"), rdf_type(), iri("Student")}

      all_facts =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Student")},
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      rule = Rules.cax_sco()

      # bob type Person could not come from alice type Student
      refute BackwardTrace.could_derive?(derived, input, rule, all_facts)
    end

    test "returns false for non-matching rule" do
      derived = {iri("alice"), rdf_type(), iri("Person")}
      input = {iri("alice"), owl_sameAs(), iri("alicia")}

      all_facts = MapSet.new()
      # Class hierarchy rule, not sameAs
      rule = Rules.cax_sco()

      refute BackwardTrace.could_derive?(derived, input, rule, all_facts)
    end
  end

  # ============================================================================
  # Tests: Edge Cases
  # ============================================================================

  describe "edge cases" do
    test "handles multiple rules finding same dependent" do
      # Multiple rules might identify the same derived fact as dependent
      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      # Use multiple rules that could match
      # Duplicate rule
      rules = [Rules.cax_sco(), Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      # Should still only have one entry for the derived fact
      assert MapSet.size(result.potentially_invalid) == 1
    end

    test "handles deleting multiple facts" do
      deleted =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Student")},
          {iri("bob"), rdf_type(), iri("Student")}
        ])

      derived =
        MapSet.new([
          {iri("alice"), rdf_type(), iri("Person")},
          {iri("bob"), rdf_type(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      assert MapSet.member?(result.potentially_invalid, {iri("alice"), rdf_type(), iri("Person")})
      assert MapSet.member?(result.potentially_invalid, {iri("bob"), rdf_type(), iri("Person")})
    end

    test "does not include explicit facts as dependents" do
      # If a fact is explicit (not derived), it shouldn't be marked invalid
      deleted = MapSet.new([{iri("Student"), rdfs_subClassOf(), iri("Person")}])

      # Empty derived set - all facts are explicit
      derived = MapSet.new()

      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)

      # No derived facts to invalidate
      assert MapSet.size(result.potentially_invalid) == 0
    end
  end
end
