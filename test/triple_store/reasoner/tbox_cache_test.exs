defmodule TripleStore.Reasoner.TBoxCacheTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.TBoxCache

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp iri(value), do: {:iri, "http://example.org/#{value}"}

  defp rdfs_subClassOf, do: {:iri, "http://www.w3.org/2000/01/rdf-schema#subClassOf"}

  # ============================================================================
  # Tests: Basic Hierarchy Computation
  # ============================================================================

  describe "compute_class_hierarchy_in_memory/1" do
    test "returns empty hierarchy for empty facts" do
      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(MapSet.new())

      assert hierarchy.class_count == 0
      assert hierarchy.superclass_map == %{}
      assert hierarchy.subclass_map == %{}
    end

    test "computes direct superclass relationship" do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Student has Person as superclass
      assert MapSet.member?(
               TBoxCache.superclasses_from(hierarchy, iri("Student")),
               iri("Person")
             )

      # Person has Student as subclass
      assert MapSet.member?(
               TBoxCache.subclasses_from(hierarchy, iri("Person")),
               iri("Student")
             )
    end

    test "computes transitive superclass closure" do
      facts = MapSet.new([
        {iri("GradStudent"), rdfs_subClassOf(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("Person"), rdfs_subClassOf(), iri("Agent")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # GradStudent has all ancestors
      grad_supers = TBoxCache.superclasses_from(hierarchy, iri("GradStudent"))
      assert MapSet.member?(grad_supers, iri("Student"))
      assert MapSet.member?(grad_supers, iri("Person"))
      assert MapSet.member?(grad_supers, iri("Agent"))

      # Agent has all descendants
      agent_subs = TBoxCache.subclasses_from(hierarchy, iri("Agent"))
      assert MapSet.member?(agent_subs, iri("Person"))
      assert MapSet.member?(agent_subs, iri("Student"))
      assert MapSet.member?(agent_subs, iri("GradStudent"))
    end

    test "handles multiple superclasses" do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("Student"), rdfs_subClassOf(), iri("LearningAgent")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      student_supers = TBoxCache.superclasses_from(hierarchy, iri("Student"))
      assert MapSet.member?(student_supers, iri("Person"))
      assert MapSet.member?(student_supers, iri("LearningAgent"))
    end

    test "handles diamond inheritance" do
      #       Thing
      #      /     \
      #   Person  Machine
      #      \     /
      #       Robot
      facts = MapSet.new([
        {iri("Person"), rdfs_subClassOf(), iri("Thing")},
        {iri("Machine"), rdfs_subClassOf(), iri("Thing")},
        {iri("Robot"), rdfs_subClassOf(), iri("Person")},
        {iri("Robot"), rdfs_subClassOf(), iri("Machine")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Robot has all three as superclasses
      robot_supers = TBoxCache.superclasses_from(hierarchy, iri("Robot"))
      assert MapSet.member?(robot_supers, iri("Person"))
      assert MapSet.member?(robot_supers, iri("Machine"))
      assert MapSet.member?(robot_supers, iri("Thing"))

      # Thing has all three as subclasses
      thing_subs = TBoxCache.subclasses_from(hierarchy, iri("Thing"))
      assert MapSet.member?(thing_subs, iri("Person"))
      assert MapSet.member?(thing_subs, iri("Machine"))
      assert MapSet.member?(thing_subs, iri("Robot"))
    end

    test "ignores non-subClassOf triples" do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("alice"), {:iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}, iri("Student")},
        {iri("bob"), {:iri, "http://example.org/knows"}, iri("alice")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Only the subClassOf triple should be processed
      assert hierarchy.class_count == 2
    end

    test "handles reflexive subClassOf (class subClassOf itself)" do
      facts = MapSet.new([
        {iri("Person"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Person is its own superclass
      person_supers = TBoxCache.superclasses_from(hierarchy, iri("Person"))
      assert MapSet.member?(person_supers, iri("Person"))
    end

    test "handles cycles in hierarchy" do
      # A -> B -> C -> A (cycle)
      facts = MapSet.new([
        {iri("A"), rdfs_subClassOf(), iri("B")},
        {iri("B"), rdfs_subClassOf(), iri("C")},
        {iri("C"), rdfs_subClassOf(), iri("A")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # All classes should have all others as superclasses
      a_supers = TBoxCache.superclasses_from(hierarchy, iri("A"))
      assert MapSet.member?(a_supers, iri("B"))
      assert MapSet.member?(a_supers, iri("C"))
      # A should also have itself due to cycle
      assert MapSet.member?(a_supers, iri("A"))
    end

    test "returns statistics" do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("Person"), rdfs_subClassOf(), iri("Agent")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      assert hierarchy.stats.class_count == 3
      assert hierarchy.stats.relationship_count > 0
      assert hierarchy.stats.computation_time_ms >= 0
    end
  end

  # ============================================================================
  # Tests: Query Functions
  # ============================================================================

  describe "superclasses_from/2" do
    test "returns empty set for unknown class" do
      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(MapSet.new())

      assert MapSet.size(TBoxCache.superclasses_from(hierarchy, iri("Unknown"))) == 0
    end

    test "returns empty set for root class" do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Person has no explicit superclasses in this ontology
      person_supers = TBoxCache.superclasses_from(hierarchy, iri("Person"))
      assert MapSet.size(person_supers) == 0
    end
  end

  describe "subclasses_from/2" do
    test "returns empty set for leaf class" do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Student has no subclasses
      student_subs = TBoxCache.subclasses_from(hierarchy, iri("Student"))
      assert MapSet.size(student_subs) == 0
    end
  end

  describe "is_superclass?/3" do
    test "returns true for direct superclass" do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      assert TBoxCache.is_superclass?(hierarchy, iri("Student"), iri("Person"))
    end

    test "returns true for transitive superclass" do
      facts = MapSet.new([
        {iri("GradStudent"), rdfs_subClassOf(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      assert TBoxCache.is_superclass?(hierarchy, iri("GradStudent"), iri("Person"))
    end

    test "returns false for non-superclass" do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      refute TBoxCache.is_superclass?(hierarchy, iri("Person"), iri("Student"))
    end
  end

  describe "is_subclass?/3" do
    test "returns true for direct subclass" do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      assert TBoxCache.is_subclass?(hierarchy, iri("Person"), iri("Student"))
    end

    test "returns false for non-subclass" do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      refute TBoxCache.is_subclass?(hierarchy, iri("Student"), iri("Person"))
    end
  end

  # ============================================================================
  # Tests: Persistent Term Storage
  # ============================================================================

  describe "compute_and_store_class_hierarchy/2" do
    setup do
      # Use unique keys for each test to avoid interference
      key = :"test_key_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:class_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "stores hierarchy in persistent_term", %{key: key} do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, stats} = TBoxCache.compute_and_store_class_hierarchy(facts, key)

      assert stats.class_count == 2
      assert TBoxCache.cached?(:class_hierarchy, key)
    end

    test "returns statistics", %{key: key} do
      facts = MapSet.new([
        {iri("A"), rdfs_subClassOf(), iri("B")},
        {iri("B"), rdfs_subClassOf(), iri("C")}
      ])

      {:ok, stats} = TBoxCache.compute_and_store_class_hierarchy(facts, key)

      assert stats.class_count == 3
      assert stats.relationship_count > 0
    end
  end

  describe "superclasses/2" do
    setup do
      key = :"test_key_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:class_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "returns superclasses from cached hierarchy", %{key: key} do
      facts = MapSet.new([
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("Person"), rdfs_subClassOf(), iri("Agent")}
      ])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key)

      supers = TBoxCache.superclasses(iri("Student"), key)
      assert MapSet.member?(supers, iri("Person"))
      assert MapSet.member?(supers, iri("Agent"))
    end

    test "returns empty set if not cached", %{key: key} do
      supers = TBoxCache.superclasses(iri("Unknown"), key)
      assert MapSet.size(supers) == 0
    end
  end

  describe "subclasses/2" do
    setup do
      key = :"test_key_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:class_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "returns subclasses from cached hierarchy", %{key: key} do
      facts = MapSet.new([
        {iri("GradStudent"), rdfs_subClassOf(), iri("Student")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key)

      subs = TBoxCache.subclasses(iri("Person"), key)
      assert MapSet.member?(subs, iri("Student"))
      assert MapSet.member?(subs, iri("GradStudent"))
    end
  end

  # ============================================================================
  # Tests: Cache Management
  # ============================================================================

  describe "cached?/2" do
    test "returns false for non-existent cache" do
      key = :"nonexistent_#{System.unique_integer([:positive])}"
      refute TBoxCache.cached?(:class_hierarchy, key)
    end
  end

  describe "clear/2" do
    test "removes cached hierarchy" do
      key = :"clear_test_#{System.unique_integer([:positive])}"

      facts = MapSet.new([
        {iri("A"), rdfs_subClassOf(), iri("B")}
      ])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key)
      assert TBoxCache.cached?(:class_hierarchy, key)

      TBoxCache.clear(:class_hierarchy, key)
      refute TBoxCache.cached?(:class_hierarchy, key)
    end
  end

  describe "version/2" do
    setup do
      key = :"version_test_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:class_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "returns version for cached hierarchy", %{key: key} do
      facts = MapSet.new([
        {iri("A"), rdfs_subClassOf(), iri("B")}
      ])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key)

      {:ok, version} = TBoxCache.version(:class_hierarchy, key)
      assert is_binary(version)
      assert String.length(version) > 0
    end

    test "returns error for non-existent cache", %{key: key} do
      assert {:error, :not_found} = TBoxCache.version(:class_hierarchy, key)
    end
  end

  describe "stats/2" do
    setup do
      key = :"stats_test_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:class_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "returns stats for cached hierarchy", %{key: key} do
      facts = MapSet.new([
        {iri("A"), rdfs_subClassOf(), iri("B")},
        {iri("B"), rdfs_subClassOf(), iri("C")}
      ])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key)

      {:ok, stats} = TBoxCache.stats(:class_hierarchy, key)
      assert stats.class_count == 3
    end
  end

  # ============================================================================
  # Tests: Edge Cases
  # ============================================================================

  describe "edge cases" do
    test "handles large hierarchy" do
      # Create a linear chain of 100 classes
      facts =
        0..98
        |> Enum.map(fn i ->
          {iri("Class#{i}"), rdfs_subClassOf(), iri("Class#{i + 1}")}
        end)
        |> MapSet.new()

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Class0 should have 99 superclasses
      class0_supers = TBoxCache.superclasses_from(hierarchy, iri("Class0"))
      assert MapSet.size(class0_supers) == 99

      # Class99 should have 99 subclasses
      class99_subs = TBoxCache.subclasses_from(hierarchy, iri("Class99"))
      assert MapSet.size(class99_subs) == 99
    end

    test "handles wide hierarchy" do
      # Create 50 classes all subclass of one root
      facts =
        0..49
        |> Enum.map(fn i ->
          {iri("Subclass#{i}"), rdfs_subClassOf(), iri("Root")}
        end)
        |> MapSet.new()

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Root should have 50 subclasses
      root_subs = TBoxCache.subclasses_from(hierarchy, iri("Root"))
      assert MapSet.size(root_subs) == 50
    end

    test "works with different term types" do
      # Test with blank nodes and other term types
      blank_node = {:bnode, "_:b1"}
      facts = MapSet.new([
        {blank_node, rdfs_subClassOf(), iri("Person")}
      ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      supers = TBoxCache.superclasses_from(hierarchy, blank_node)
      assert MapSet.member?(supers, iri("Person"))
    end
  end
end
