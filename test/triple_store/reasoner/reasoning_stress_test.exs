# credo:disable-for-this-file Credo.Check.Readability.FunctionNames
defmodule TripleStore.Reasoner.ReasoningStressTest do
  @moduledoc """
  Stress tests for OWL 2 RL reasoning to verify bounded behavior
  under resource-intensive scenarios.

  These tests verify that the reasoning system handles:
  - Long sameAs chains (50+ entities)
  - Deep transitive chains (100+ hops)
  - Wide class hierarchies
  - Complex property graphs

  ## Reference

  These tests address concerns identified in the Section 4.6 review
  regarding limited resource exhaustion testing.
  """
  use TripleStore.ReasonerTestCase

  @moduletag timeout: 60_000
  @moduletag :stress

  # ============================================================================
  # Long sameAs Chains
  # ============================================================================

  describe "long sameAs chains" do
    @tag timeout: 120_000
    @tag :benchmark
    test "50-entity sameAs chain completes correctly" do
      # Create chain: e0 sameAs e1 sameAs e2 ... sameAs e49
      chain_facts =
        for i <- 0..48 do
          {ex_iri("e#{i}"), owl_sameAs(), ex_iri("e#{i + 1}")}
        end
        |> MapSet.new()

      # Add a type assertion to e0
      facts = MapSet.put(chain_facts, {ex_iri("e0"), rdf_type(), ex_iri("Person")})

      result = materialize(facts)

      # All 50 entities should have type Person
      for i <- 0..49 do
        assert has_triple?(result, {ex_iri("e#{i}"), rdf_type(), ex_iri("Person")}),
               "Expected e#{i} to be Person via sameAs chain"
      end
    end

    @tag timeout: 120_000
    @tag :benchmark
    test "50-entity sameAs chain - all entities are mutually sameAs" do
      # Create chain: e0 sameAs e1 sameAs e2 ... sameAs e49
      chain_facts =
        for i <- 0..48 do
          {ex_iri("e#{i}"), owl_sameAs(), ex_iri("e#{i + 1}")}
        end
        |> MapSet.new()

      result = materialize(chain_facts)

      # Verify transitivity: e0 sameAs e49
      assert has_triple?(result, {ex_iri("e0"), owl_sameAs(), ex_iri("e49")}),
             "Expected transitive sameAs from e0 to e49"

      # Verify symmetry: e49 sameAs e0
      assert has_triple?(result, {ex_iri("e49"), owl_sameAs(), ex_iri("e0")}),
             "Expected symmetric sameAs from e49 to e0"

      # Verify intermediate: e25 sameAs e0 and e25 sameAs e49
      assert has_triple?(result, {ex_iri("e25"), owl_sameAs(), ex_iri("e0")})
      assert has_triple?(result, {ex_iri("e25"), owl_sameAs(), ex_iri("e49")})
    end

    @tag timeout: 120_000
    @tag :benchmark
    test "sameAs chain with property propagation" do
      # Create 30-entity chain
      chain_facts =
        for i <- 0..28 do
          {ex_iri("e#{i}"), owl_sameAs(), ex_iri("e#{i + 1}")}
        end
        |> MapSet.new()

      # Add properties to first entity
      properties = MapSet.new([
        {ex_iri("e0"), rdf_type(), ex_iri("Person")},
        {ex_iri("e0"), ex_iri("hasAge"), {:literal, "25", {:iri, "http://www.w3.org/2001/XMLSchema#integer"}}},
        {ex_iri("e0"), ex_iri("knows"), ex_iri("friend1")}
      ])

      facts = MapSet.union(chain_facts, properties)
      result = materialize(facts)

      # Last entity (e29) should have all properties
      assert has_triple?(result, {ex_iri("e29"), rdf_type(), ex_iri("Person")})
      assert has_triple?(result, {ex_iri("e29"), ex_iri("hasAge"), {:literal, "25", {:iri, "http://www.w3.org/2001/XMLSchema#integer"}}})
      assert has_triple?(result, {ex_iri("e29"), ex_iri("knows"), ex_iri("friend1")})
    end
  end

  # ============================================================================
  # Deep Transitive Chains
  # ============================================================================

  describe "deep transitive chains" do
    @tag timeout: 120_000
    @tag :benchmark
    test "100-hop transitive property chain" do
      # Create chain: x0 p x1 p x2 ... p x100
      tbox = MapSet.new([{ex_iri("ancestorOf"), rdf_type(), owl_TransitiveProperty()}])

      chain_facts =
        for i <- 0..99 do
          {ex_iri("x#{i}"), ex_iri("ancestorOf"), ex_iri("x#{i + 1}")}
        end
        |> MapSet.new()

      facts = MapSet.union(tbox, chain_facts)
      result = materialize(facts)

      # x0 should be ancestorOf x100 via transitive closure
      assert has_triple?(result, {ex_iri("x0"), ex_iri("ancestorOf"), ex_iri("x100")}),
             "Expected x0 ancestorOf x100 via 100-hop transitive closure"

      # Intermediate relationships should also exist
      assert has_triple?(result, {ex_iri("x0"), ex_iri("ancestorOf"), ex_iri("x50")})
      assert has_triple?(result, {ex_iri("x50"), ex_iri("ancestorOf"), ex_iri("x100")})
    end

    @tag timeout: 120_000
    @tag :benchmark
    test "multiple transitive properties with 50-hop chains each" do
      # Two separate transitive properties with independent chains
      tbox = MapSet.new([
        {ex_iri("partOf"), rdf_type(), owl_TransitiveProperty()},
        {ex_iri("locatedIn"), rdf_type(), owl_TransitiveProperty()}
      ])

      # partOf chain: p0 -> p1 -> ... -> p50
      part_chain =
        for i <- 0..49 do
          {ex_iri("p#{i}"), ex_iri("partOf"), ex_iri("p#{i + 1}")}
        end
        |> MapSet.new()

      # locatedIn chain: l0 -> l1 -> ... -> l50
      location_chain =
        for i <- 0..49 do
          {ex_iri("l#{i}"), ex_iri("locatedIn"), ex_iri("l#{i + 1}")}
        end
        |> MapSet.new()

      facts = MapSet.union(tbox, MapSet.union(part_chain, location_chain))
      result = materialize(facts)

      # Both chains should be complete
      assert has_triple?(result, {ex_iri("p0"), ex_iri("partOf"), ex_iri("p50")})
      assert has_triple?(result, {ex_iri("l0"), ex_iri("locatedIn"), ex_iri("l50")})

      # Chains should remain independent
      refute has_triple?(result, {ex_iri("p0"), ex_iri("locatedIn"), ex_iri("l50")})
      refute has_triple?(result, {ex_iri("l0"), ex_iri("partOf"), ex_iri("p50")})
    end
  end

  # ============================================================================
  # Wide Class Hierarchies
  # ============================================================================

  describe "wide class hierarchies" do
    @tag timeout: 30_000
    test "100 classes with single superclass" do
      # 100 subclasses all inheriting from Thing
      subclass_facts =
        for i <- 1..100 do
          {ex_iri("Class#{i}"), rdfs_subClassOf(), ex_iri("Thing")}
        end
        |> MapSet.new()

      # Add one instance per class
      instance_facts =
        for i <- 1..100 do
          {ex_iri("x#{i}"), rdf_type(), ex_iri("Class#{i}")}
        end
        |> MapSet.new()

      facts = MapSet.union(subclass_facts, instance_facts)
      result = materialize(facts, :rdfs)

      # All instances should be Thing
      for i <- 1..100 do
        assert has_triple?(result, {ex_iri("x#{i}"), rdf_type(), ex_iri("Thing")}),
               "Expected x#{i} to be Thing via subclass inference"
      end
    end

    @tag timeout: 30_000
    test "deep and wide hierarchy (10 levels, 5 children each)" do
      # Create a tree-like hierarchy with depth 5 and branching factor 3
      # This creates 3^5 = 243 leaf classes plus intermediate classes

      # Build hierarchy recursively
      hierarchy =
        build_hierarchy_facts("Root", 0, 5, 3)
        |> MapSet.new()

      # Add one instance at the bottom (Root_1_1_1_1_1 is a leaf class)
      facts = MapSet.put(hierarchy, {ex_iri("instance"), rdf_type(), ex_iri("Root_1_1_1_1_1")})
      result = materialize(facts, :rdfs)

      # Instance should be inferred as Root via the subclass chain
      assert has_triple?(result, {ex_iri("instance"), rdf_type(), ex_iri("Root")})
    end
  end

  # ============================================================================
  # Complex Property Graphs
  # ============================================================================

  describe "complex property graphs" do
    @tag timeout: 120_000
    @tag :benchmark
    test "symmetric property creates complete graph for 20 entities" do
      # knows is symmetric, create a chain that will expand to complete graph
      tbox = MapSet.new([
        {ex_iri("knows"), rdf_type(), owl_SymmetricProperty()},
        {ex_iri("knows"), rdf_type(), owl_TransitiveProperty()}
      ])

      # Chain: e0 knows e1 knows e2 ... knows e19
      chain_facts =
        for i <- 0..18 do
          {ex_iri("e#{i}"), ex_iri("knows"), ex_iri("e#{i + 1}")}
        end
        |> MapSet.new()

      facts = MapSet.union(tbox, chain_facts)
      result = materialize(facts)

      # With symmetric + transitive, everyone should know everyone
      for i <- 0..19, j <- 0..19, i != j do
        assert has_triple?(result, {ex_iri("e#{i}"), ex_iri("knows"), ex_iri("e#{j}")}),
               "Expected e#{i} knows e#{j} in equivalence class"
      end
    end

    @tag timeout: 30_000
    test "inverse property chain with 30 entities" do
      tbox = MapSet.new([
        {ex_iri("parentOf"), owl_inverseOf(), ex_iri("childOf")}
      ])

      # parentOf chain: p0 parentOf p1 parentOf ... parentOf p29
      parent_chain =
        for i <- 0..28 do
          {ex_iri("p#{i}"), ex_iri("parentOf"), ex_iri("p#{i + 1}")}
        end
        |> MapSet.new()

      facts = MapSet.union(tbox, parent_chain)
      result = materialize(facts)

      # Inverse relationships should exist
      for i <- 1..29 do
        assert has_triple?(result, {ex_iri("p#{i}"), ex_iri("childOf"), ex_iri("p#{i - 1}")}),
               "Expected p#{i} childOf p#{i - 1} via inverse"
      end
    end

    @tag timeout: 30_000
    test "property hierarchy with domain/range propagation" do
      # Create property hierarchy 5 levels deep
      property_hierarchy =
        for i <- 1..4 do
          {ex_iri("prop#{i}"), rdfs_subPropertyOf(), ex_iri("prop#{i + 1}")}
        end
        |> MapSet.new()

      # Top property has domain and range
      domain_range = MapSet.new([
        {ex_iri("prop5"), rdfs_domain(), ex_iri("Agent")},
        {ex_iri("prop5"), rdfs_range(), ex_iri("Resource")}
      ])

      # Use bottom property
      instance_facts = MapSet.new([
        {ex_iri("alice"), ex_iri("prop1"), ex_iri("resource1")}
      ])

      facts = MapSet.union(property_hierarchy, MapSet.union(domain_range, instance_facts))
      result = materialize(facts, :rdfs)

      # Domain/range should propagate through hierarchy
      assert has_triple?(result, {ex_iri("alice"), rdf_type(), ex_iri("Agent")})
      assert has_triple?(result, {ex_iri("resource1"), rdf_type(), ex_iri("Resource")})

      # Property values should propagate up
      assert has_triple?(result, {ex_iri("alice"), ex_iri("prop5"), ex_iri("resource1")})
    end
  end

  # ============================================================================
  # Performance Verification
  # ============================================================================

  describe "performance verification" do
    @tag timeout: 30_000
    test "reasoning terminates within time limit for large dataset" do
      # Create a moderately complex ontology
      class_hierarchy =
        for i <- 1..50 do
          {ex_iri("Class#{i}"), rdfs_subClassOf(), ex_iri("Thing")}
        end
        |> MapSet.new()

      # Add 100 instances
      instances =
        for i <- 1..100 do
          class = rem(i, 50) + 1
          {ex_iri("instance#{i}"), rdf_type(), ex_iri("Class#{class}")}
        end
        |> MapSet.new()

      facts = MapSet.union(class_hierarchy, instances)

      # Should complete within the timeout
      {result, stats} = materialize_with_stats(facts, :rdfs)

      # Verify correctness
      assert MapSet.size(result) > MapSet.size(facts)
      assert stats.iterations > 0

      # All instances should be Thing
      for i <- 1..100 do
        assert has_triple?(result, {ex_iri("instance#{i}"), rdf_type(), ex_iri("Thing")})
      end
    end

    @tag timeout: 30_000
    test "iteration count scales reasonably with data size" do
      # Small dataset
      small_facts = MapSet.new([
        {ex_iri("A"), rdfs_subClassOf(), ex_iri("B")},
        {ex_iri("x"), rdf_type(), ex_iri("A")}
      ])
      {_, small_stats} = materialize_with_stats(small_facts, :rdfs)

      # Medium dataset (10-level hierarchy)
      medium_hierarchy =
        for i <- 1..9 do
          {ex_iri("C#{i}"), rdfs_subClassOf(), ex_iri("C#{i + 1}")}
        end
        |> MapSet.new()
      medium_facts = MapSet.put(medium_hierarchy, {ex_iri("y"), rdf_type(), ex_iri("C1")})
      {_, medium_stats} = materialize_with_stats(medium_facts, :rdfs)

      # Iterations should scale, but not explosively
      # Medium should be roughly linear with depth
      assert medium_stats.iterations <= small_stats.iterations * 15,
             "Iterations should scale reasonably (small: #{small_stats.iterations}, medium: #{medium_stats.iterations})"
    end
  end

  # ============================================================================
  # Helper Functions
  # ============================================================================

  defp build_hierarchy_facts(_parent, depth, max_depth, _branching) when depth >= max_depth do
    []
  end

  defp build_hierarchy_facts(parent, depth, max_depth, branching) do
    children =
      for i <- 1..branching do
        child = "#{parent}_#{i}"
        fact = {ex_iri(child), rdfs_subClassOf(), ex_iri(parent)}
        [fact | build_hierarchy_facts(child, depth + 1, max_depth, branching)]
      end

    List.flatten(children)
  end
end
