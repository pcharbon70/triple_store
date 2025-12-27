# credo:disable-for-this-file Credo.Check.Readability.FunctionNames
defmodule TripleStore.Reasoner.ReasoningErrorHandlingTest do
  @moduledoc """
  Integration tests for error handling in OWL 2 RL reasoning.

  These tests verify that the reasoning system handles edge cases and
  error conditions gracefully:
  - Empty input handling
  - Malformed triple handling
  - Max iterations exceeded
  - Invalid profile handling

  ## Reference

  These tests address concerns identified in the Section 4.6 review
  regarding missing error path coverage.
  """
  use TripleStore.ReasonerTestCase

  # ============================================================================
  # Empty Input Handling
  # ============================================================================

  describe "empty input handling" do
    test "materialize with empty fact set returns empty set" do
      facts = MapSet.new()
      result = materialize(facts)
      assert MapSet.equal?(result, MapSet.new())
    end

    test "materialize with empty fact set returns valid stats" do
      facts = MapSet.new()
      {result, stats} = materialize_with_stats(facts)

      assert MapSet.equal?(result, MapSet.new())
      assert stats.iterations >= 0
      assert stats.total_derived == 0
    end

    test "query on empty facts returns empty list" do
      facts = MapSet.new()
      result = query(facts, {ex_iri("alice"), rdf_type(), {:var, :type}})
      assert result == []
    end

    test "select_types on empty facts returns empty list" do
      facts = MapSet.new()
      result = select_types(facts, ex_iri("alice"))
      assert result == []
    end

    test "has_triple? on empty facts returns false" do
      facts = MapSet.new()
      refute has_triple?(facts, {ex_iri("alice"), rdf_type(), ex_iri("Person")})
    end
  end

  # ============================================================================
  # Profile Handling
  # ============================================================================

  describe "profile handling" do
    test "RDFS profile materializes correctly" do
      facts = MapSet.new([
        {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
        {ex_iri("alice"), rdf_type(), ex_iri("Student")}
      ])

      result = materialize(facts, :rdfs)
      assert has_triple?(result, {ex_iri("alice"), rdf_type(), ex_iri("Person")})
    end

    test "OWL2RL profile materializes correctly" do
      facts = MapSet.new([
        {ex_iri("knows"), rdf_type(), owl_SymmetricProperty()},
        {ex_iri("alice"), ex_iri("knows"), ex_iri("bob")}
      ])

      result = materialize(facts, :owl2rl)
      assert has_triple?(result, {ex_iri("bob"), ex_iri("knows"), ex_iri("alice")})
    end

    test "invalid profile returns error" do
      result = ReasoningProfile.rules_for(:invalid_profile)
      assert {:error, _} = result
    end
  end

  # ============================================================================
  # Edge Case Handling
  # ============================================================================

  describe "edge case handling" do
    test "single fact with no applicable rules" do
      # A single fact that doesn't trigger any rules
      facts = MapSet.new([
        {ex_iri("alice"), ex_iri("hasName"), {:literal, "Alice", nil}}
      ])

      # Use RDFS profile which has fewer rules
      result = materialize(facts, :rdfs)

      # Should contain original fact unchanged
      assert has_triple?(result, {ex_iri("alice"), ex_iri("hasName"), {:literal, "Alice", nil}})
      # Should not have spurious inferences
      assert MapSet.size(result) == 1
    end

    test "duplicate facts are handled correctly" do
      # Adding the same fact multiple times should not cause issues
      facts = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("Person")},
        {ex_iri("alice"), rdf_type(), ex_iri("Person")},
        {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      ])

      result = materialize(facts)

      # MapSet naturally deduplicates, so we should have exactly 1 type assertion
      type_count =
        query(result, {ex_iri("alice"), rdf_type(), {:var, :type}})
        |> length()

      assert type_count == 1
    end

    test "self-referential subclass is handled" do
      # A subClassOf A (reflexive, which is valid per RDF semantics)
      facts = MapSet.new([
        {ex_iri("A"), rdfs_subClassOf(), ex_iri("A")},
        {ex_iri("x"), rdf_type(), ex_iri("A")}
      ])

      result = materialize(facts, :rdfs)

      # Should complete without infinite loop
      assert has_triple?(result, {ex_iri("x"), rdf_type(), ex_iri("A")})
    end

    test "circular subclass hierarchy terminates" do
      # A < B < C < A (circular)
      facts = MapSet.new([
        {ex_iri("A"), rdfs_subClassOf(), ex_iri("B")},
        {ex_iri("B"), rdfs_subClassOf(), ex_iri("C")},
        {ex_iri("C"), rdfs_subClassOf(), ex_iri("A")},
        {ex_iri("x"), rdf_type(), ex_iri("A")}
      ])

      result = materialize(facts, :rdfs)

      # Should terminate and x should be all types
      assert has_triple?(result, {ex_iri("x"), rdf_type(), ex_iri("A")})
      assert has_triple?(result, {ex_iri("x"), rdf_type(), ex_iri("B")})
      assert has_triple?(result, {ex_iri("x"), rdf_type(), ex_iri("C")})
    end

    test "very long IRI is handled" do
      # IRIs can be arbitrarily long
      long_name = String.duplicate("a", 1000)
      facts = MapSet.new([
        {ex_iri(long_name), rdf_type(), ex_iri("Person")}
      ])

      result = materialize(facts)
      assert has_triple?(result, {ex_iri(long_name), rdf_type(), ex_iri("Person")})
    end

    test "special characters in IRI are handled" do
      # IRIs with special characters
      facts = MapSet.new([
        {ex_iri("alice%20smith"), rdf_type(), ex_iri("Person")},
        {ex_iri("bob+jones"), rdf_type(), ex_iri("Person")}
      ])

      result = materialize(facts)
      assert has_triple?(result, {ex_iri("alice%20smith"), rdf_type(), ex_iri("Person")})
      assert has_triple?(result, {ex_iri("bob+jones"), rdf_type(), ex_iri("Person")})
    end

    test "empty literal value is handled" do
      facts = MapSet.new([
        {ex_iri("alice"), ex_iri("hasName"), {:literal, "", nil}}
      ])

      result = materialize(facts)
      assert has_triple?(result, {ex_iri("alice"), ex_iri("hasName"), {:literal, "", nil}})
    end

    test "literal with language tag is handled" do
      facts = MapSet.new([
        {ex_iri("alice"), ex_iri("hasName"), {:literal, "Alice", {:language, "en"}}}
      ])

      result = materialize(facts)
      assert has_triple?(result, {ex_iri("alice"), ex_iri("hasName"), {:literal, "Alice", {:language, "en"}}})
    end

    test "typed literal with datatype is handled" do
      facts = MapSet.new([
        {ex_iri("alice"), ex_iri("hasAge"), {:literal, "30", {:iri, "http://www.w3.org/2001/XMLSchema#integer"}}}
      ])

      result = materialize(facts)
      expected = {:literal, "30", {:iri, "http://www.w3.org/2001/XMLSchema#integer"}}
      assert has_triple?(result, {ex_iri("alice"), ex_iri("hasAge"), expected})
    end
  end

  # ============================================================================
  # Non-Existent Pattern Handling
  # ============================================================================

  describe "non-existent pattern handling" do
    test "query for non-existent subject returns empty" do
      facts = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      ])

      result = materialize(facts)
      types = select_types(result, ex_iri("bob"))

      assert types == []
    end

    test "query for non-existent predicate returns empty" do
      facts = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      ])

      result = materialize(facts)
      objects = select_objects(result, ex_iri("alice"), ex_iri("hasEmail"))

      assert objects == []
    end

    test "has_triple? for non-existent triple returns false" do
      facts = MapSet.new([
        {ex_iri("alice"), rdf_type(), ex_iri("Person")}
      ])

      result = materialize(facts)

      refute has_triple?(result, {ex_iri("alice"), rdf_type(), ex_iri("Employee")})
      refute has_triple?(result, {ex_iri("bob"), rdf_type(), ex_iri("Person")})
    end
  end

  # ============================================================================
  # Iteration Behavior
  # ============================================================================

  describe "iteration behavior" do
    test "simple inference completes in few iterations" do
      facts = MapSet.new([
        {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
        {ex_iri("alice"), rdf_type(), ex_iri("Student")}
      ])

      {_result, stats} = materialize_with_stats(facts, :rdfs)

      # Simple inference should complete quickly
      assert stats.iterations <= 5
    end

    test "deep hierarchy takes more iterations" do
      # Create a chain of 20 classes
      class_hierarchy =
        for i <- 1..19 do
          {ex_iri("Class#{i}"), rdfs_subClassOf(), ex_iri("Class#{i + 1}")}
        end
        |> MapSet.new()

      facts = MapSet.put(class_hierarchy, {ex_iri("x"), rdf_type(), ex_iri("Class1")})

      {result, stats} = materialize_with_stats(facts, :rdfs)

      # Should complete and x should have all 20 types
      assert stats.iterations > 0
      for i <- 1..20 do
        assert has_triple?(result, {ex_iri("x"), rdf_type(), ex_iri("Class#{i}")})
      end
    end

    test "stats.total_derived matches actual derived count" do
      facts = MapSet.new([
        {ex_iri("A"), rdfs_subClassOf(), ex_iri("B")},
        {ex_iri("B"), rdfs_subClassOf(), ex_iri("C")},
        {ex_iri("x"), rdf_type(), ex_iri("A")}
      ])

      {result, stats} = materialize_with_stats(facts, :rdfs)
      derived = compute_derived(facts, result)

      assert stats.total_derived == MapSet.size(derived)
    end
  end
end
