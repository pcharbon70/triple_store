defmodule TripleStore.Reasoner.IncrementalTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.Incremental
  alias TripleStore.Reasoner.Rules

  # ============================================================================
  # Test Helpers - Using IRI terms like the rest of the reasoning subsystem
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @owl "http://www.w3.org/2002/07/owl#"
  @ex "http://example.org/"

  defp iri(suffix), do: {:iri, @ex <> suffix}
  defp rdf_type, do: {:iri, @rdf <> "type"}
  defp rdfs_subClassOf, do: {:iri, @rdfs <> "subClassOf"}
  defp rdfs_subPropertyOf, do: {:iri, @rdfs <> "subPropertyOf"}
  defp owl_sameAs, do: {:iri, @owl <> "sameAs"}
  defp owl_TransitiveProperty, do: {:iri, @owl <> "TransitiveProperty"}

  # ============================================================================
  # Tests: add_in_memory/4 - Basic Functionality
  # ============================================================================

  describe "add_in_memory/4 - basic functionality" do
    test "returns success with empty triple list" do
      existing = MapSet.new()
      rules = [Rules.cax_sco()]

      {:ok, _all_facts, stats} = Incremental.add_in_memory([], existing, rules)

      assert stats.explicit_added == 0
      assert stats.derived_count == 0
      assert stats.iterations == 0
    end

    test "adds explicit facts to existing store" do
      existing = MapSet.new()
      new_triples = [{iri("alice"), rdf_type(), iri("Person")}]
      rules = []

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.explicit_added == 1
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Person")})
    end

    test "adds multiple explicit facts" do
      existing = MapSet.new()

      new_triples = [
        {iri("alice"), rdf_type(), iri("Person")},
        {iri("bob"), rdf_type(), iri("Student")}
      ]

      rules = []

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.explicit_added == 2
      assert MapSet.size(all_facts) == 2
    end

    test "skips facts that already exist" do
      existing = MapSet.new([{iri("alice"), rdf_type(), iri("Person")}])
      # Same fact
      new_triples = [{iri("alice"), rdf_type(), iri("Person")}]
      rules = []

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.explicit_added == 0
      assert MapSet.size(all_facts) == 1
    end

    test "returns duration in milliseconds" do
      existing = MapSet.new()
      new_triples = [{iri("alice"), rdf_type(), iri("Person")}]
      rules = []

      {:ok, _all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert is_integer(stats.duration_ms)
      assert stats.duration_ms >= 0
    end
  end

  # ============================================================================
  # Tests: add_in_memory/4 - Reasoning with Class Hierarchies
  # ============================================================================

  describe "add_in_memory/4 - class hierarchy reasoning" do
    test "derives class membership from subclass hierarchy" do
      # Set up hierarchy: Student subClassOf Person
      existing =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      # Add alice as Student
      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.explicit_added == 1
      assert stats.derived_count >= 1

      # Check derived fact
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Person")})
    end

    test "derives transitive class membership" do
      # Set up hierarchy: Person subClassOf Animal subClassOf LivingThing
      existing =
        MapSet.new([
          {iri("Person"), rdfs_subClassOf(), iri("Animal")},
          {iri("Animal"), rdfs_subClassOf(), iri("LivingThing")}
        ])

      # Add alice as Person
      new_triples = [{iri("alice"), rdf_type(), iri("Person")}]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.explicit_added == 1
      assert stats.derived_count >= 2

      # Check derived facts
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Animal")})
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("LivingThing")})
    end

    test "handles deep class hierarchies" do
      # Set up 4-level hierarchy
      existing =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")},
          {iri("Person"), rdfs_subClassOf(), iri("Animal")},
          {iri("Animal"), rdfs_subClassOf(), iri("LivingThing")}
        ])

      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      # Should derive all levels of class membership
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Person")})
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Animal")})
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("LivingThing")})
      assert stats.iterations >= 1
    end

    test "reports correct iteration count" do
      existing =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")},
          {iri("Person"), rdfs_subClassOf(), iri("Animal")}
        ])

      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]
      rules = [Rules.cax_sco()]

      {:ok, _all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.iterations >= 1
    end
  end

  # ============================================================================
  # Tests: add_in_memory/4 - Multiple Rules
  # ============================================================================

  describe "add_in_memory/4 - multiple rules" do
    test "handles multiple rules" do
      existing =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]
      # Use multiple rules
      rules = [Rules.cax_sco(), Rules.scm_sco()]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.explicit_added == 1
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Person")})
    end

    test "does not duplicate existing derived facts" do
      # Pre-derive the fact
      existing =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")},
          # Already derived
          {iri("alice"), rdf_type(), iri("Person")}
        ])

      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]
      rules = [Rules.cax_sco()]

      {:ok, _all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.explicit_added == 1
      # Should not count the existing derived fact
      assert stats.derived_count == 0
    end
  end

  # ============================================================================
  # Tests: add_in_memory/4 - Subclass Transitivity
  # ============================================================================

  describe "add_in_memory/4 - subclass transitivity" do
    test "derives subclass transitivity when adding subclass assertion" do
      # Existing: Person subClassOf Animal
      existing =
        MapSet.new([
          {iri("Person"), rdfs_subClassOf(), iri("Animal")}
        ])

      # Add: Student subClassOf Person
      new_triples = [{iri("Student"), rdfs_subClassOf(), iri("Person")}]
      rules = [Rules.scm_sco()]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      # Should derive: Student subClassOf Animal
      assert MapSet.member?(all_facts, {iri("Student"), rdfs_subClassOf(), iri("Animal")})
      assert stats.derived_count >= 1
    end
  end

  # ============================================================================
  # Tests: add_in_memory/4 - sameAs Reasoning
  # ============================================================================

  describe "add_in_memory/4 - sameAs reasoning" do
    test "propagates sameAs symmetry" do
      existing = MapSet.new()

      # Add: alice sameAs alicia
      new_triples = [{iri("alice"), owl_sameAs(), iri("alicia")}]
      rules = [Rules.eq_sym()]

      {:ok, all_facts, _stats} = Incremental.add_in_memory(new_triples, existing, rules)

      # Should derive: alicia sameAs alice (symmetry)
      assert MapSet.member?(all_facts, {iri("alicia"), owl_sameAs(), iri("alice")})
    end

    test "propagates sameAs transitivity" do
      # Existing: alice sameAs bob
      existing =
        MapSet.new([
          {iri("alice"), owl_sameAs(), iri("bob")}
        ])

      # Add: bob sameAs carol
      new_triples = [{iri("bob"), owl_sameAs(), iri("carol")}]
      rules = [Rules.eq_trans()]

      {:ok, all_facts, _stats} = Incremental.add_in_memory(new_triples, existing, rules)

      # Should derive: alice sameAs carol
      assert MapSet.member?(all_facts, {iri("alice"), owl_sameAs(), iri("carol")})
    end
  end

  # ============================================================================
  # Tests: add_in_memory/4 - Options
  # ============================================================================

  describe "add_in_memory/4 - options" do
    test "respects parallel option" do
      existing = MapSet.new([{iri("Student"), rdfs_subClassOf(), iri("Person")}])
      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, _stats} =
        Incremental.add_in_memory(new_triples, existing, rules, parallel: true)

      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Person")})
    end

    test "respects max_iterations option" do
      # Set up deep hierarchy
      existing =
        MapSet.new([
          {iri("A"), rdfs_subClassOf(), iri("B")},
          {iri("B"), rdfs_subClassOf(), iri("C")},
          {iri("C"), rdfs_subClassOf(), iri("D")},
          {iri("D"), rdfs_subClassOf(), iri("E")}
        ])

      new_triples = [{iri("x"), rdf_type(), iri("A")}]
      rules = [Rules.cax_sco()]

      # With unlimited iterations
      {:ok, _all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)
      assert stats.iterations >= 1

      # Limit to 1 iteration - may not fully propagate but should complete
      result = Incremental.add_in_memory(new_triples, existing, rules, max_iterations: 1)

      case result do
        {:ok, _all, _s} -> :ok
        {:error, :max_iterations_exceeded} -> :ok
        other -> flunk("Unexpected result: #{inspect(other)}")
      end
    end

    test "respects emit_telemetry option" do
      existing = MapSet.new()
      new_triples = [{iri("alice"), rdf_type(), iri("Person")}]
      rules = []

      # Should not raise with telemetry disabled
      {:ok, _all_facts, _stats} =
        Incremental.add_in_memory(new_triples, existing, rules, emit_telemetry: false)
    end
  end

  # ============================================================================
  # Tests: add_in_memory/4 - Edge Cases
  # ============================================================================

  describe "add_in_memory/4 - edge cases" do
    test "handles facts with no applicable rules" do
      existing = MapSet.new()
      # Add facts with no matching rules
      new_triples = [{iri("alice"), iri("knows"), iri("bob")}]
      # Only matches rdf:type patterns
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.explicit_added == 1
      assert stats.derived_count == 0
      assert MapSet.size(all_facts) == 1
    end

    test "handles multiple additions sequentially" do
      # First addition
      existing1 = MapSet.new([{iri("Student"), rdfs_subClassOf(), iri("Person")}])

      {:ok, facts1, stats1} =
        Incremental.add_in_memory([{iri("alice"), rdf_type(), iri("Student")}], existing1, [
          Rules.cax_sco()
        ])

      assert stats1.derived_count >= 1
      assert MapSet.member?(facts1, {iri("alice"), rdf_type(), iri("Person")})

      # Second addition using previous result as existing
      {:ok, facts2, stats2} =
        Incremental.add_in_memory([{iri("bob"), rdf_type(), iri("Student")}], facts1, [
          Rules.cax_sco()
        ])

      assert stats2.derived_count >= 1
      assert MapSet.member?(facts2, {iri("bob"), rdf_type(), iri("Person")})
      # First derivation should still be there
      assert MapSet.member?(facts2, {iri("alice"), rdf_type(), iri("Person")})
    end

    test "handles empty rules" do
      existing = MapSet.new()
      new_triples = [{iri("alice"), rdf_type(), iri("Person")}]
      rules = []

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.explicit_added == 1
      assert stats.derived_count == 0
      assert MapSet.size(all_facts) == 1
    end

    test "handles empty existing facts" do
      existing = MapSet.new()

      new_triples = [
        {iri("alice"), rdf_type(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ]

      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} = Incremental.add_in_memory(new_triples, existing, rules)

      assert stats.explicit_added == 2
      # Adding both hierarchy and instance should still derive
      assert MapSet.member?(all_facts, {iri("alice"), rdf_type(), iri("Person")})
    end
  end

  # ============================================================================
  # Tests: preview_in_memory/3
  # ============================================================================

  describe "preview_in_memory/3" do
    test "returns empty set for empty triples" do
      existing = MapSet.new()
      rules = [Rules.cax_sco()]

      {:ok, derived} = Incremental.preview_in_memory([], existing, rules)

      assert MapSet.size(derived) == 0
    end

    test "returns derived facts without modifying existing" do
      existing =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      # Preview additions
      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]
      {:ok, derived} = Incremental.preview_in_memory(new_triples, existing, rules)

      # Should show what would be derived
      assert MapSet.member?(derived, {iri("alice"), rdf_type(), iri("Person")})

      # Existing should be unchanged
      assert MapSet.size(existing) == 1
    end

    test "excludes new facts from derived set" do
      existing =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      rules = [Rules.cax_sco()]

      new_triples = [{iri("alice"), rdf_type(), iri("Student")}]
      {:ok, derived} = Incremental.preview_in_memory(new_triples, existing, rules)

      # Should NOT include the input fact - only derived facts
      refute MapSet.member?(derived, {iri("alice"), rdf_type(), iri("Student")})
      # Should include derived fact
      assert MapSet.member?(derived, {iri("alice"), rdf_type(), iri("Person")})
    end
  end
end
