defmodule TripleStore.Reasoner.IncrementalQuadTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.IncrementalQuad
  alias TripleStore.Reasoner.Rules

  # ============================================================================
  # Test Helpers
  # ============================================================================

  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @ex "http://example.org/"

  defp iri(suffix), do: {:iri, @ex <> suffix}
  defp rdf_type, do: {:iri, @rdf <> "type"}
  defp rdfs_subclass_of, do: {:iri, @rdfs <> "subClassOf"}

  defp quad(g, s, p, o), do: {g, s, p, o}

  # ============================================================================
  # Tests: add_quads_in_memory/4 - Basic Functionality
  # ============================================================================

  describe "add_quads_in_memory/4 - basic functionality" do
    test "returns success with empty quad list" do
      existing = MapSet.new()
      new_quads = []
      rules = []

      {:ok, _all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 0
      assert stats.derived_count == 0
      assert stats.iterations == 0
    end

    test "adds explicit quads to existing store" do
      existing = MapSet.new()
      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Person"))]
      rules = []

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 1
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Person")))
    end

    test "adds multiple explicit quads" do
      existing = MapSet.new()

      new_quads = [
        quad(1, iri("alice"), rdf_type(), iri("Person")),
        quad(1, iri("bob"), rdf_type(), iri("Student"))
      ]

      rules = []

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 2
      assert MapSet.size(all_facts) == 2
    end

    test "skips quads that already exist" do
      existing = MapSet.new([quad(1, iri("alice"), rdf_type(), iri("Person"))])
      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Person"))]
      rules = []

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 0
      assert MapSet.size(all_facts) == 1
    end

    test "returns duration in milliseconds" do
      existing = MapSet.new()
      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Person"))]
      rules = []

      {:ok, _all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert is_integer(stats.duration_ms)
      assert stats.duration_ms >= 0
    end
  end

  # ============================================================================
  # Tests: add_quads_in_memory/4 - Graph-Scoped Reasoning
  # ============================================================================

  describe "add_quads_in_memory/4 - graph-scoped reasoning" do
    test "derives facts within the target graph" do
      # Set up hierarchy in graph 1
      existing = MapSet.new([quad(1, iri("Student"), rdfs_subclass_of(), iri("Person"))])

      # Add alice as Student in graph 1
      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 1
      assert stats.derived_count >= 1

      # Check derived fact is in graph 1
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Person")))
    end

    test "does not derive facts for other graphs" do
      # Set up hierarchy in graph 1
      existing = MapSet.new([quad(1, iri("Student"), rdfs_subclass_of(), iri("Person"))])

      # Add alice as Student in graph 1
      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, _stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      # alice rdf:type Person should be in graph 1, not graph 2
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Person")))
      refute MapSet.member?(all_facts, quad(2, iri("alice"), rdf_type(), iri("Person")))
    end

    test "handles TBox sharing when configured" do
      # TBox in graph 0, data in graph 1
      existing =
        MapSet.new([
          quad(0, iri("Student"), rdfs_subclass_of(), iri("Person"))
        ])

      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(
          new_quads,
          existing,
          rules,
          graph_id: 1,
          tbox_graph_id: 0
        )

      assert stats.explicit_added == 1
      assert stats.derived_count >= 1

      # Check derived fact - should be in graph 1
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Person")))
    end

    test "derives transitive class hierarchy" do
      # Set up 3-level hierarchy
      existing =
        MapSet.new([
          quad(1, iri("Person"), rdfs_subclass_of(), iri("Animal")),
          quad(1, iri("Animal"), rdfs_subclass_of(), iri("LivingThing"))
        ])

      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Person"))]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 1
      assert stats.derived_count >= 2

      # Check all derived facts
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Animal")))
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("LivingThing")))
    end
  end

  # ============================================================================
  # Tests: add_quads_in_memory/4 - Multiple Graphs
  # ============================================================================

  describe "add_quads_in_memory/4 - multiple graphs" do
    test "maintains separate fact sets for different graphs" do
      existing = MapSet.new()

      quads_graph1 = [quad(1, iri("alice"), rdf_type(), iri("Person"))]
      quads_graph2 = [quad(2, iri("bob"), rdf_type(), iri("Person"))]

      rules = []

      {:ok, all_facts_1, _stats1} =
        IncrementalQuad.add_quads_in_memory(quads_graph1, existing, rules, graph_id: 1)

      {:ok, all_facts_2, _stats2} =
        IncrementalQuad.add_quads_in_memory(quads_graph2, all_facts_1, rules, graph_id: 2)

      # Both facts should exist
      assert MapSet.member?(all_facts_2, quad(1, iri("alice"), rdf_type(), iri("Person")))
      assert MapSet.member?(all_facts_2, quad(2, iri("bob"), rdf_type(), iri("Person")))
    end

    test "does not mix quads between graphs" do
      existing =
        MapSet.new([
          quad(1, iri("alice"), rdf_type(), iri("Person")),
          quad(2, iri("bob"), rdf_type(), iri("Student"))
        ])

      # Add hierarchy for graph 1 only
      new_quads = [quad(1, iri("Student"), rdfs_subclass_of(), iri("Person"))]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, _stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      # No derivation should happen for graph 2
      assert MapSet.member?(all_facts, quad(2, iri("bob"), rdf_type(), iri("Student")))
      refute MapSet.member?(all_facts, quad(2, iri("bob"), rdf_type(), iri("Person")))
    end
  end

  # ============================================================================
  # Tests: add_quads_in_memory/4 - Deduplication
  # ============================================================================

  describe "add_quads_in_memory/4 - deduplication" do
    test "normalizes graph_id when not specified" do
      existing = MapSet.new()
      # Quad without graph_id (as triple)
      new_quads = [{iri("alice"), rdf_type(), iri("Person")}]
      rules = []

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 1
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Person")))
    end

    test "uses provided graph_id when different from default" do
      existing = MapSet.new()
      # Quad with different graph_id
      new_quads = [quad(2, iri("alice"), rdf_type(), iri("Person"))]
      rules = []

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 1
      # Should use the quad's graph_id (2), not the default (1)
      assert MapSet.member?(all_facts, quad(2, iri("alice"), rdf_type(), iri("Person")))
      refute MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Person")))
    end
  end

  # ============================================================================
  # Tests: add_quads_in_memory/4 - Rule Application
  # ============================================================================

  describe "add_quads_in_memory/4 - rule application" do
    test "applies cax_sco rule for class hierarchy" do
      existing =
        MapSet.new([
          quad(1, iri("Student"), rdfs_subclass_of(), iri("Person")),
          quad(1, iri("Person"), rdfs_subclass_of(), iri("Agent"))
        ])

      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      # Should derive: alice rdf:type Person
      # Should also derive: alice rdf:type Agent (transitive)
      assert stats.derived_count >= 2
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Person")))
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Agent")))
    end

    test "handles multiple rules" do
      # Test cax_sco (class membership through subClassOf)
      existing =
        MapSet.new([
          quad(1, iri("Person"), rdfs_subclass_of(), iri("Agent"))
        ])

      new_quads = [
        quad(1, iri("alice"), rdf_type(), iri("Student")),
        quad(1, iri("Student"), rdfs_subclass_of(), iri("Person"))
      ]

      # cax_sco: class membership through subclass
      # scm_sco: subClassOf transitivity (if A subClassOf B and B subClassOf C, then A subClassOf C)
      rules = [Rules.cax_sco(), Rules.scm_sco()]

      {:ok, all_facts, _stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      # Should derive class membership (cax_sco): alice rdf:type Person
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Person")))
      # Should derive transitive subClassOf relationship (scm_sco): Student subClassOf Agent
      assert MapSet.member?(all_facts, quad(1, iri("Student"), rdfs_subclass_of(), iri("Agent")))
      # And transitive class membership through both rules
      assert MapSet.member?(all_facts, quad(1, iri("alice"), rdf_type(), iri("Agent")))
    end
  end

  # ============================================================================
  # Tests: add_quads_in_memory/4 - Iteration Tracking
  # ============================================================================

  describe "add_quads_in_memory/4 - iteration tracking" do
    test "returns iterations count" do
      existing =
        MapSet.new([
          quad(1, iri("Student"), rdfs_subclass_of(), iri("Person"))
        ])

      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = [Rules.cax_sco()]

      {:ok, _all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert is_integer(stats.iterations)
      assert stats.iterations > 0
    end

    test "handles deep hierarchies with multiple iterations" do
      # Create 5-level hierarchy
      existing =
        MapSet.new([
          quad(1, iri("L2"), rdfs_subclass_of(), iri("L1")),
          quad(1, iri("L3"), rdfs_subclass_of(), iri("L2")),
          quad(1, iri("L4"), rdfs_subclass_of(), iri("L3")),
          quad(1, iri("L5"), rdfs_subclass_of(), iri("L4"))
        ])

      new_quads = [quad(1, iri("alice"), rdf_type(), iri("L5"))]
      rules = [Rules.cax_sco()]

      {:ok, _all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      # Should take multiple iterations to derive all levels
      assert stats.iterations >= 4
    end
  end

  # ============================================================================
  # Tests: add_quads_in_memory/4 - Edge Cases
  # ============================================================================

  describe "add_quads_in_memory/4 - edge cases" do
    test "handles empty existing facts" do
      existing = MapSet.new()
      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Person"))]
      rules = []

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 1
      assert MapSet.size(all_facts) == 1
    end

    test "handles empty rules list" do
      existing = MapSet.new()
      new_quads = [quad(1, iri("alice"), rdf_type(), iri("Student"))]
      rules = []

      {:ok, _all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 1
      assert stats.derived_count == 0
    end

    test "handles large batch of quads" do
      existing = MapSet.new()

      # Create 100 quads
      new_quads =
        for i <- 1..100 do
          quad(1, iri("entity#{i}"), rdf_type(), iri("Thing"))
        end

      rules = []

      {:ok, all_facts, stats} =
        IncrementalQuad.add_quads_in_memory(new_quads, existing, rules, graph_id: 1)

      assert stats.explicit_added == 100
      assert MapSet.size(all_facts) == 100
    end
  end
end
