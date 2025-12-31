# credo:disable-for-this-file Credo.Check.Readability.FunctionNames
defmodule TripleStore.Reasoner.TBoxCacheTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.TBoxCache

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp iri(value), do: {:iri, "http://example.org/#{value}"}

  defp rdfs_subClassOf, do: {:iri, "http://www.w3.org/2000/01/rdf-schema#subClassOf"}
  defp rdfs_subPropertyOf, do: {:iri, "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"}
  defp rdf_type, do: {:iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}
  defp owl_TransitiveProperty, do: {:iri, "http://www.w3.org/2002/07/owl#TransitiveProperty"}
  defp owl_SymmetricProperty, do: {:iri, "http://www.w3.org/2002/07/owl#SymmetricProperty"}
  defp owl_FunctionalProperty, do: {:iri, "http://www.w3.org/2002/07/owl#FunctionalProperty"}

  defp owl_InverseFunctionalProperty,
    do: {:iri, "http://www.w3.org/2002/07/owl#InverseFunctionalProperty"}

  defp owl_inverseOf, do: {:iri, "http://www.w3.org/2002/07/owl#inverseOf"}

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
      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")},
          {iri("alice"), {:iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"},
           iri("Student")},
          {iri("bob"), {:iri, "http://example.org/knows"}, iri("alice")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Only the subClassOf triple should be processed
      assert hierarchy.class_count == 2
    end

    test "handles reflexive subClassOf (class subClassOf itself)" do
      facts =
        MapSet.new([
          {iri("Person"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Person is its own superclass
      person_supers = TBoxCache.superclasses_from(hierarchy, iri("Person"))
      assert MapSet.member?(person_supers, iri("Person"))
    end

    test "handles cycles in hierarchy" do
      # A -> B -> C -> A (cycle)
      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      # Student has no subclasses
      student_subs = TBoxCache.subclasses_from(hierarchy, iri("Student"))
      assert MapSet.size(student_subs) == 0
    end
  end

  describe "superclass?/3" do
    test "returns true for direct superclass" do
      facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      assert TBoxCache.superclass?(hierarchy, iri("Student"), iri("Person"))
    end

    test "returns true for transitive superclass" do
      facts =
        MapSet.new([
          {iri("GradStudent"), rdfs_subClassOf(), iri("Student")},
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      assert TBoxCache.superclass?(hierarchy, iri("GradStudent"), iri("Person"))
    end

    test "returns false for non-superclass" do
      facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      refute TBoxCache.superclass?(hierarchy, iri("Person"), iri("Student"))
    end
  end

  describe "subclass?/3" do
    test "returns true for direct subclass" do
      facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      assert TBoxCache.subclass?(hierarchy, iri("Person"), iri("Student"))
    end

    test "returns false for non-subclass" do
      facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      refute TBoxCache.subclass?(hierarchy, iri("Student"), iri("Person"))
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
      facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, stats} = TBoxCache.compute_and_store_class_hierarchy(facts, key)

      assert stats.class_count == 2
      assert TBoxCache.cached?(:class_hierarchy, key)
    end

    test "returns statistics", %{key: key} do
      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
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

      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
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
      facts =
        MapSet.new([
          {iri("A"), rdfs_subClassOf(), iri("B")},
          {iri("B"), rdfs_subClassOf(), iri("C")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key)

      {:ok, stats} = TBoxCache.stats(:class_hierarchy, key)
      assert stats.class_count == 3
    end

    test "returns error for non-existent cache", %{key: key} do
      assert {:error, :not_found} = TBoxCache.stats(:class_hierarchy, key)
    end
  end

  describe "clear_all/0" do
    test "clears all cached hierarchies" do
      key1 = :"clear_all_test_1_#{System.unique_integer([:positive])}"
      key2 = :"clear_all_test_2_#{System.unique_integer([:positive])}"

      facts =
        MapSet.new([
          {iri("A"), rdfs_subClassOf(), iri("B")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key1)
      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key2)

      assert TBoxCache.cached?(:class_hierarchy, key1)
      assert TBoxCache.cached?(:class_hierarchy, key2)

      TBoxCache.clear_all()

      refute TBoxCache.cached?(:class_hierarchy, key1)
      refute TBoxCache.cached?(:class_hierarchy, key2)
    end

    test "clears both class and property hierarchies" do
      key = :"clear_all_test_both_#{System.unique_integer([:positive])}"

      class_facts = MapSet.new([{iri("A"), rdfs_subClassOf(), iri("B")}])
      prop_facts = MapSet.new([{iri("p1"), rdfs_subPropertyOf(), iri("p2")}])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(class_facts, key)
      {:ok, _} = TBoxCache.compute_and_store_property_hierarchy(prop_facts, key)

      assert TBoxCache.cached?(:class_hierarchy, key)
      assert TBoxCache.cached?(:property_hierarchy, key)

      TBoxCache.clear_all()

      refute TBoxCache.cached?(:class_hierarchy, key)
      refute TBoxCache.cached?(:property_hierarchy, key)
    end
  end

  describe "list_cached/0" do
    test "returns empty list when nothing cached" do
      # Clear all first to ensure clean state
      TBoxCache.clear_all()

      cached = TBoxCache.list_cached()
      assert cached == []
    end

    test "returns list of cached hierarchy keys" do
      # Clear all first to ensure clean state
      TBoxCache.clear_all()

      key1 = :"list_cached_test_1_#{System.unique_integer([:positive])}"
      key2 = :"list_cached_test_2_#{System.unique_integer([:positive])}"

      facts = MapSet.new([{iri("A"), rdfs_subClassOf(), iri("B")}])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key1)
      {:ok, _} = TBoxCache.compute_and_store_property_hierarchy(facts, key2)

      cached = TBoxCache.list_cached()

      assert {:class_hierarchy, key1} in cached
      assert {:property_hierarchy, key2} in cached

      # Cleanup
      TBoxCache.clear_all()
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

      facts =
        MapSet.new([
          {blank_node, rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_class_hierarchy_in_memory(facts)

      supers = TBoxCache.superclasses_from(hierarchy, blank_node)
      assert MapSet.member?(supers, iri("Person"))
    end
  end

  # ============================================================================
  # Tests: Property Hierarchy - Basic Computation
  # ============================================================================

  describe "compute_property_hierarchy_in_memory/1" do
    test "returns empty hierarchy for empty facts" do
      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(MapSet.new())

      assert hierarchy.property_count == 0
      assert hierarchy.superproperty_map == %{}
      assert hierarchy.subproperty_map == %{}
      assert MapSet.size(hierarchy.characteristics.transitive) == 0
      assert MapSet.size(hierarchy.characteristics.symmetric) == 0
      assert MapSet.size(hierarchy.characteristics.functional) == 0
      assert MapSet.size(hierarchy.characteristics.inverse_functional) == 0
      assert hierarchy.characteristics.inverse_pairs == %{}
    end

    test "computes direct superproperty relationship" do
      facts =
        MapSet.new([
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # hasChild has hasDescendant as superproperty
      assert MapSet.member?(
               TBoxCache.superproperties_from(hierarchy, iri("hasChild")),
               iri("hasDescendant")
             )

      # hasDescendant has hasChild as subproperty
      assert MapSet.member?(
               TBoxCache.subproperties_from(hierarchy, iri("hasDescendant")),
               iri("hasChild")
             )
    end

    test "computes transitive superproperty closure" do
      facts =
        MapSet.new([
          {iri("hasGrandchild"), rdfs_subPropertyOf(), iri("hasChild")},
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")},
          {iri("hasDescendant"), rdfs_subPropertyOf(), iri("hasRelative")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # hasGrandchild has all ancestors
      grandchild_supers = TBoxCache.superproperties_from(hierarchy, iri("hasGrandchild"))
      assert MapSet.member?(grandchild_supers, iri("hasChild"))
      assert MapSet.member?(grandchild_supers, iri("hasDescendant"))
      assert MapSet.member?(grandchild_supers, iri("hasRelative"))

      # hasRelative has all descendants
      relative_subs = TBoxCache.subproperties_from(hierarchy, iri("hasRelative"))
      assert MapSet.member?(relative_subs, iri("hasDescendant"))
      assert MapSet.member?(relative_subs, iri("hasChild"))
      assert MapSet.member?(relative_subs, iri("hasGrandchild"))
    end

    test "handles multiple superproperties" do
      facts =
        MapSet.new([
          {iri("hasParent"), rdfs_subPropertyOf(), iri("hasAncestor")},
          {iri("hasParent"), rdfs_subPropertyOf(), iri("hasRelative")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      parent_supers = TBoxCache.superproperties_from(hierarchy, iri("hasParent"))
      assert MapSet.member?(parent_supers, iri("hasAncestor"))
      assert MapSet.member?(parent_supers, iri("hasRelative"))
    end

    test "handles diamond inheritance" do
      #       topProperty
      #        /       \
      #   propA        propB
      #        \       /
      #        bottomProp
      facts =
        MapSet.new([
          {iri("propA"), rdfs_subPropertyOf(), iri("topProperty")},
          {iri("propB"), rdfs_subPropertyOf(), iri("topProperty")},
          {iri("bottomProp"), rdfs_subPropertyOf(), iri("propA")},
          {iri("bottomProp"), rdfs_subPropertyOf(), iri("propB")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # bottomProp has all three as superproperties
      bottom_supers = TBoxCache.superproperties_from(hierarchy, iri("bottomProp"))
      assert MapSet.member?(bottom_supers, iri("propA"))
      assert MapSet.member?(bottom_supers, iri("propB"))
      assert MapSet.member?(bottom_supers, iri("topProperty"))

      # topProperty has all three as subproperties
      top_subs = TBoxCache.subproperties_from(hierarchy, iri("topProperty"))
      assert MapSet.member?(top_subs, iri("propA"))
      assert MapSet.member?(top_subs, iri("propB"))
      assert MapSet.member?(top_subs, iri("bottomProp"))
    end

    test "ignores non-subPropertyOf triples" do
      facts =
        MapSet.new([
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")},
          {iri("alice"), iri("hasChild"), iri("bob")},
          {iri("Person"), rdfs_subClassOf(), iri("Agent")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # Only the subPropertyOf triple should be processed for hierarchy
      assert hierarchy.property_count == 2
    end

    test "returns statistics" do
      facts =
        MapSet.new([
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")},
          {iri("hasDescendant"), rdfs_subPropertyOf(), iri("hasRelative")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      assert hierarchy.stats.property_count == 3
      assert hierarchy.stats.relationship_count > 0
      assert hierarchy.stats.computation_time_ms >= 0
    end
  end

  # ============================================================================
  # Tests: Property Characteristics
  # ============================================================================

  describe "property characteristics" do
    test "extracts transitive properties" do
      facts =
        MapSet.new([
          {iri("contains"), rdf_type(), owl_TransitiveProperty()},
          {iri("ancestor"), rdf_type(), owl_TransitiveProperty()}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      assert TBoxCache.transitive_property?(hierarchy, iri("contains"))
      assert TBoxCache.transitive_property?(hierarchy, iri("ancestor"))
      refute TBoxCache.transitive_property?(hierarchy, iri("unknown"))

      transitive = TBoxCache.transitive_properties(hierarchy)
      assert MapSet.size(transitive) == 2
    end

    test "extracts symmetric properties" do
      facts =
        MapSet.new([
          {iri("knows"), rdf_type(), owl_SymmetricProperty()},
          {iri("sibling"), rdf_type(), owl_SymmetricProperty()}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      assert TBoxCache.symmetric_property?(hierarchy, iri("knows"))
      assert TBoxCache.symmetric_property?(hierarchy, iri("sibling"))
      refute TBoxCache.symmetric_property?(hierarchy, iri("unknown"))

      symmetric = TBoxCache.symmetric_properties(hierarchy)
      assert MapSet.size(symmetric) == 2
    end

    test "extracts functional properties" do
      facts =
        MapSet.new([
          {iri("hasMother"), rdf_type(), owl_FunctionalProperty()},
          {iri("hasBirthDate"), rdf_type(), owl_FunctionalProperty()}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      assert TBoxCache.functional_property?(hierarchy, iri("hasMother"))
      assert TBoxCache.functional_property?(hierarchy, iri("hasBirthDate"))
      refute TBoxCache.functional_property?(hierarchy, iri("unknown"))

      functional = TBoxCache.functional_properties(hierarchy)
      assert MapSet.size(functional) == 2
    end

    test "extracts inverse functional properties" do
      facts =
        MapSet.new([
          {iri("hasSSN"), rdf_type(), owl_InverseFunctionalProperty()},
          {iri("hasEmail"), rdf_type(), owl_InverseFunctionalProperty()}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      assert TBoxCache.inverse_functional_property?(hierarchy, iri("hasSSN"))
      assert TBoxCache.inverse_functional_property?(hierarchy, iri("hasEmail"))
      refute TBoxCache.inverse_functional_property?(hierarchy, iri("unknown"))

      inv_func = TBoxCache.inverse_functional_properties(hierarchy)
      assert MapSet.size(inv_func) == 2
    end

    test "extracts inverse property pairs" do
      facts =
        MapSet.new([
          {iri("hasChild"), owl_inverseOf(), iri("hasParent")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # Bidirectional lookup
      assert TBoxCache.inverse_of(hierarchy, iri("hasChild")) == iri("hasParent")
      assert TBoxCache.inverse_of(hierarchy, iri("hasParent")) == iri("hasChild")
      assert TBoxCache.inverse_of(hierarchy, iri("unknown")) == nil

      pairs = TBoxCache.inverse_pairs(hierarchy)
      assert map_size(pairs) == 2
    end

    test "handles multiple inverse pairs" do
      facts =
        MapSet.new([
          {iri("hasChild"), owl_inverseOf(), iri("hasParent")},
          {iri("owns"), owl_inverseOf(), iri("ownedBy")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      pairs = TBoxCache.inverse_pairs(hierarchy)
      assert map_size(pairs) == 4

      assert TBoxCache.inverse_of(hierarchy, iri("owns")) == iri("ownedBy")
      assert TBoxCache.inverse_of(hierarchy, iri("ownedBy")) == iri("owns")
    end

    test "property with multiple characteristics" do
      facts =
        MapSet.new([
          {iri("hasMother"), rdf_type(), owl_FunctionalProperty()},
          {iri("hasMother"), owl_inverseOf(), iri("motherOf")},
          {iri("hasMother"), rdfs_subPropertyOf(), iri("hasParent")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      assert TBoxCache.functional_property?(hierarchy, iri("hasMother"))
      assert TBoxCache.inverse_of(hierarchy, iri("hasMother")) == iri("motherOf")

      assert MapSet.member?(
               TBoxCache.superproperties_from(hierarchy, iri("hasMother")),
               iri("hasParent")
             )
    end

    test "counts characteristics in stats" do
      facts =
        MapSet.new([
          {iri("p1"), rdf_type(), owl_TransitiveProperty()},
          {iri("p2"), rdf_type(), owl_SymmetricProperty()},
          {iri("p3"), rdf_type(), owl_FunctionalProperty()},
          {iri("p4"), rdf_type(), owl_InverseFunctionalProperty()},
          {iri("p5"), owl_inverseOf(), iri("p6")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      assert hierarchy.stats.transitive_count == 1
      assert hierarchy.stats.symmetric_count == 1
      assert hierarchy.stats.functional_count == 1
      assert hierarchy.stats.inverse_functional_count == 1
      assert hierarchy.stats.inverse_pair_count == 2
    end
  end

  # ============================================================================
  # Tests: Property Hierarchy Query Functions
  # ============================================================================

  describe "superproperties_from/2" do
    test "returns empty set for unknown property" do
      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(MapSet.new())

      assert MapSet.size(TBoxCache.superproperties_from(hierarchy, iri("Unknown"))) == 0
    end

    test "returns empty set for root property" do
      facts =
        MapSet.new([
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasRelative")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # hasRelative has no explicit superproperties
      relative_supers = TBoxCache.superproperties_from(hierarchy, iri("hasRelative"))
      assert MapSet.size(relative_supers) == 0
    end
  end

  describe "subproperties_from/2" do
    test "returns empty set for leaf property" do
      facts =
        MapSet.new([
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasRelative")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # hasChild has no subproperties
      child_subs = TBoxCache.subproperties_from(hierarchy, iri("hasChild"))
      assert MapSet.size(child_subs) == 0
    end
  end

  # ============================================================================
  # Tests: Property Hierarchy - Persistent Term Storage
  # ============================================================================

  describe "compute_and_store_property_hierarchy/2" do
    setup do
      key = :"prop_test_key_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:property_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "stores hierarchy in persistent_term", %{key: key} do
      facts =
        MapSet.new([
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")},
          {iri("hasChild"), rdf_type(), owl_TransitiveProperty()}
        ])

      {:ok, stats} = TBoxCache.compute_and_store_property_hierarchy(facts, key)

      assert stats.property_count == 2
      assert TBoxCache.cached?(:property_hierarchy, key)
    end

    test "returns statistics", %{key: key} do
      facts =
        MapSet.new([
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")},
          {iri("hasDescendant"), rdfs_subPropertyOf(), iri("hasRelative")}
        ])

      {:ok, stats} = TBoxCache.compute_and_store_property_hierarchy(facts, key)

      assert stats.property_count == 3
      assert stats.relationship_count > 0
    end
  end

  describe "superproperties/2" do
    setup do
      key = :"prop_super_test_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:property_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "returns superproperties from cached hierarchy", %{key: key} do
      facts =
        MapSet.new([
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")},
          {iri("hasDescendant"), rdfs_subPropertyOf(), iri("hasRelative")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_property_hierarchy(facts, key)

      supers = TBoxCache.superproperties(iri("hasChild"), key)
      assert MapSet.member?(supers, iri("hasDescendant"))
      assert MapSet.member?(supers, iri("hasRelative"))
    end

    test "returns empty set if not cached", %{key: key} do
      supers = TBoxCache.superproperties(iri("Unknown"), key)
      assert MapSet.size(supers) == 0
    end
  end

  describe "subproperties/2" do
    setup do
      key = :"prop_sub_test_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:property_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "returns subproperties from cached hierarchy", %{key: key} do
      facts =
        MapSet.new([
          {iri("hasGrandchild"), rdfs_subPropertyOf(), iri("hasChild")},
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_property_hierarchy(facts, key)

      subs = TBoxCache.subproperties(iri("hasDescendant"), key)
      assert MapSet.member?(subs, iri("hasChild"))
      assert MapSet.member?(subs, iri("hasGrandchild"))
    end
  end

  # ============================================================================
  # Tests: Property Hierarchy - Cache Management
  # ============================================================================

  describe "property hierarchy cache management" do
    test "cached?/2 returns false for non-existent property cache" do
      key = :"prop_nonexistent_#{System.unique_integer([:positive])}"
      refute TBoxCache.cached?(:property_hierarchy, key)
    end

    test "clear/2 removes cached property hierarchy" do
      key = :"prop_clear_test_#{System.unique_integer([:positive])}"

      facts =
        MapSet.new([
          {iri("a"), rdfs_subPropertyOf(), iri("b")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_property_hierarchy(facts, key)
      assert TBoxCache.cached?(:property_hierarchy, key)

      TBoxCache.clear(:property_hierarchy, key)
      refute TBoxCache.cached?(:property_hierarchy, key)
    end

    test "version/2 returns version for cached property hierarchy" do
      key = :"prop_version_test_#{System.unique_integer([:positive])}"

      facts =
        MapSet.new([
          {iri("a"), rdfs_subPropertyOf(), iri("b")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_property_hierarchy(facts, key)

      {:ok, version} = TBoxCache.version(:property_hierarchy, key)
      assert is_binary(version)
      assert String.length(version) > 0

      TBoxCache.clear(:property_hierarchy, key)
    end

    test "stats/2 returns stats for cached property hierarchy" do
      key = :"prop_stats_test_#{System.unique_integer([:positive])}"

      facts =
        MapSet.new([
          {iri("a"), rdfs_subPropertyOf(), iri("b")},
          {iri("b"), rdfs_subPropertyOf(), iri("c")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_property_hierarchy(facts, key)

      {:ok, stats} = TBoxCache.stats(:property_hierarchy, key)
      assert stats.property_count == 3

      TBoxCache.clear(:property_hierarchy, key)
    end
  end

  # ============================================================================
  # Tests: Property Hierarchy - Edge Cases
  # ============================================================================

  describe "property hierarchy edge cases" do
    test "handles large property hierarchy" do
      # Create a linear chain of 100 properties
      facts =
        0..98
        |> Enum.map(fn i ->
          {iri("prop#{i}"), rdfs_subPropertyOf(), iri("prop#{i + 1}")}
        end)
        |> MapSet.new()

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # prop0 should have 99 superproperties
      prop0_supers = TBoxCache.superproperties_from(hierarchy, iri("prop0"))
      assert MapSet.size(prop0_supers) == 99

      # prop99 should have 99 subproperties
      prop99_subs = TBoxCache.subproperties_from(hierarchy, iri("prop99"))
      assert MapSet.size(prop99_subs) == 99
    end

    test "handles wide property hierarchy" do
      # Create 50 properties all subproperty of one root
      facts =
        0..49
        |> Enum.map(fn i ->
          {iri("subprop#{i}"), rdfs_subPropertyOf(), iri("rootProp")}
        end)
        |> MapSet.new()

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # rootProp should have 50 subproperties
      root_subs = TBoxCache.subproperties_from(hierarchy, iri("rootProp"))
      assert MapSet.size(root_subs) == 50
    end

    test "handles cycles in property hierarchy" do
      # p1 -> p2 -> p3 -> p1 (cycle)
      facts =
        MapSet.new([
          {iri("p1"), rdfs_subPropertyOf(), iri("p2")},
          {iri("p2"), rdfs_subPropertyOf(), iri("p3")},
          {iri("p3"), rdfs_subPropertyOf(), iri("p1")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # All properties should have all others as superproperties
      p1_supers = TBoxCache.superproperties_from(hierarchy, iri("p1"))
      assert MapSet.member?(p1_supers, iri("p2"))
      assert MapSet.member?(p1_supers, iri("p3"))
      assert MapSet.member?(p1_supers, iri("p1"))
    end

    test "works with blank nodes in property hierarchy" do
      blank_node = {:bnode, "_:prop1"}

      facts =
        MapSet.new([
          {blank_node, rdfs_subPropertyOf(), iri("namedProp")}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      supers = TBoxCache.superproperties_from(hierarchy, blank_node)
      assert MapSet.member?(supers, iri("namedProp"))
    end

    test "combines hierarchy and characteristics correctly" do
      facts =
        MapSet.new([
          # Hierarchy
          {iri("contains"), rdfs_subPropertyOf(), iri("locatedIn")},
          {iri("locatedIn"), rdfs_subPropertyOf(), iri("relatedTo")},
          # Characteristics
          {iri("contains"), rdf_type(), owl_TransitiveProperty()},
          {iri("locatedIn"), rdf_type(), owl_TransitiveProperty()},
          {iri("relatedTo"), rdf_type(), owl_SymmetricProperty()}
        ])

      {:ok, hierarchy} = TBoxCache.compute_property_hierarchy_in_memory(facts)

      # Check hierarchy
      contains_supers = TBoxCache.superproperties_from(hierarchy, iri("contains"))
      assert MapSet.member?(contains_supers, iri("locatedIn"))
      assert MapSet.member?(contains_supers, iri("relatedTo"))

      # Check characteristics
      assert TBoxCache.transitive_property?(hierarchy, iri("contains"))
      assert TBoxCache.transitive_property?(hierarchy, iri("locatedIn"))
      refute TBoxCache.transitive_property?(hierarchy, iri("relatedTo"))
      assert TBoxCache.symmetric_property?(hierarchy, iri("relatedTo"))
    end
  end

  # ============================================================================
  # Tests: TBox Update Detection
  # ============================================================================

  describe "tbox_predicates/0" do
    test "returns set of TBox-modifying predicates" do
      predicates = TBoxCache.tbox_predicates()

      assert MapSet.member?(predicates, {:iri, "http://www.w3.org/2000/01/rdf-schema#subClassOf"})

      assert MapSet.member?(
               predicates,
               {:iri, "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"}
             )

      assert MapSet.member?(predicates, {:iri, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"})
      assert MapSet.member?(predicates, {:iri, "http://www.w3.org/2002/07/owl#inverseOf"})
      assert MapSet.member?(predicates, {:iri, "http://www.w3.org/2000/01/rdf-schema#domain"})
      assert MapSet.member?(predicates, {:iri, "http://www.w3.org/2000/01/rdf-schema#range"})
    end
  end

  describe "property_characteristic_types/0" do
    test "returns set of property characteristic types" do
      types = TBoxCache.property_characteristic_types()

      assert MapSet.member?(types, {:iri, "http://www.w3.org/2002/07/owl#TransitiveProperty"})
      assert MapSet.member?(types, {:iri, "http://www.w3.org/2002/07/owl#SymmetricProperty"})
      assert MapSet.member?(types, {:iri, "http://www.w3.org/2002/07/owl#FunctionalProperty"})

      assert MapSet.member?(
               types,
               {:iri, "http://www.w3.org/2002/07/owl#InverseFunctionalProperty"}
             )
    end
  end

  describe "tbox_triple?/1" do
    test "returns true for rdfs:subClassOf triple" do
      triple = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      assert TBoxCache.tbox_triple?(triple)
    end

    test "returns true for rdfs:subPropertyOf triple" do
      triple = {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")}
      assert TBoxCache.tbox_triple?(triple)
    end

    test "returns true for owl:inverseOf triple" do
      triple = {iri("hasChild"), owl_inverseOf(), iri("hasParent")}
      assert TBoxCache.tbox_triple?(triple)
    end

    test "returns true for property characteristic declaration" do
      triple = {iri("knows"), rdf_type(), owl_SymmetricProperty()}
      assert TBoxCache.tbox_triple?(triple)
    end

    test "returns true for TransitiveProperty declaration" do
      triple = {iri("contains"), rdf_type(), owl_TransitiveProperty()}
      assert TBoxCache.tbox_triple?(triple)
    end

    test "returns true for FunctionalProperty declaration" do
      triple = {iri("hasMother"), rdf_type(), owl_FunctionalProperty()}
      assert TBoxCache.tbox_triple?(triple)
    end

    test "returns true for InverseFunctionalProperty declaration" do
      triple = {iri("hasSSN"), rdf_type(), owl_InverseFunctionalProperty()}
      assert TBoxCache.tbox_triple?(triple)
    end

    test "returns false for instance type declaration" do
      triple = {iri("alice"), rdf_type(), iri("Person")}
      refute TBoxCache.tbox_triple?(triple)
    end

    test "returns false for instance data triple" do
      triple = {iri("alice"), iri("knows"), iri("bob")}
      refute TBoxCache.tbox_triple?(triple)
    end

    test "returns true for rdfs:domain triple" do
      domain_iri = {:iri, "http://www.w3.org/2000/01/rdf-schema#domain"}
      triple = {iri("knows"), domain_iri, iri("Person")}
      assert TBoxCache.tbox_triple?(triple)
    end

    test "returns true for rdfs:range triple" do
      range_iri = {:iri, "http://www.w3.org/2000/01/rdf-schema#range"}
      triple = {iri("knows"), range_iri, iri("Person")}
      assert TBoxCache.tbox_triple?(triple)
    end
  end

  describe "contains_tbox_triples?/1" do
    test "returns true when collection contains TBox triple" do
      triples = [
        {iri("alice"), iri("knows"), iri("bob")},
        {iri("Student"), rdfs_subClassOf(), iri("Person")}
      ]

      assert TBoxCache.contains_tbox_triples?(triples)
    end

    test "returns false when collection has no TBox triples" do
      triples = [
        {iri("alice"), iri("knows"), iri("bob")},
        {iri("bob"), rdf_type(), iri("Person")}
      ]

      refute TBoxCache.contains_tbox_triples?(triples)
    end

    test "returns false for empty collection" do
      refute TBoxCache.contains_tbox_triples?([])
    end
  end

  describe "filter_tbox_triples/1" do
    test "returns only TBox triples" do
      subclass_triple = {iri("Student"), rdfs_subClassOf(), iri("Person")}
      instance_triple = {iri("alice"), rdf_type(), iri("Person")}
      property_triple = {iri("knows"), rdf_type(), owl_SymmetricProperty()}

      triples = [subclass_triple, instance_triple, property_triple]
      filtered = TBoxCache.filter_tbox_triples(triples)

      assert length(filtered) == 2
      assert subclass_triple in filtered
      assert property_triple in filtered
      refute instance_triple in filtered
    end

    test "returns empty list when no TBox triples" do
      triples = [
        {iri("alice"), iri("knows"), iri("bob")}
      ]

      assert TBoxCache.filter_tbox_triples(triples) == []
    end
  end

  describe "categorize_tbox_triples/1" do
    test "categorizes triples by type" do
      triples = [
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")},
        {iri("knows"), rdf_type(), owl_SymmetricProperty()},
        {iri("hasChild"), owl_inverseOf(), iri("hasParent")},
        {iri("alice"), iri("knows"), iri("bob")}
      ]

      categorized = TBoxCache.categorize_tbox_triples(triples)

      assert length(categorized.class_hierarchy) == 1
      assert length(categorized.property_hierarchy) == 1
      assert length(categorized.property_characteristics) == 1
      assert length(categorized.inverse_properties) == 1
      assert categorized.domain_range == []
    end

    test "categorizes domain/range triples" do
      domain_iri = {:iri, "http://www.w3.org/2000/01/rdf-schema#domain"}
      range_iri = {:iri, "http://www.w3.org/2000/01/rdf-schema#range"}

      triples = [
        {iri("knows"), domain_iri, iri("Person")},
        {iri("knows"), range_iri, iri("Person")}
      ]

      categorized = TBoxCache.categorize_tbox_triples(triples)

      assert length(categorized.domain_range) == 2
    end

    test "ignores non-TBox triples" do
      triples = [
        {iri("alice"), iri("knows"), iri("bob")},
        {iri("bob"), rdf_type(), iri("Person")}
      ]

      categorized = TBoxCache.categorize_tbox_triples(triples)

      assert categorized.class_hierarchy == []
      assert categorized.property_hierarchy == []
      assert categorized.property_characteristics == []
      assert categorized.inverse_properties == []
      assert categorized.domain_range == []
    end
  end

  # ============================================================================
  # Tests: TBox Cache Invalidation
  # ============================================================================

  describe "invalidate_affected/2" do
    setup do
      key = :"invalidate_test_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:class_hierarchy, key)
        TBoxCache.clear(:property_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "invalidates class hierarchy when subClassOf triples provided", %{key: key} do
      # First, create cached hierarchies
      facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key)
      assert TBoxCache.cached?(:class_hierarchy, key)

      # Now invalidate with a subClassOf triple
      triples = [{iri("GradStudent"), rdfs_subClassOf(), iri("Student")}]
      result = TBoxCache.invalidate_affected(triples, key)

      assert result.class_hierarchy == true
      refute TBoxCache.cached?(:class_hierarchy, key)
    end

    test "invalidates property hierarchy when subPropertyOf triples provided", %{key: key} do
      facts =
        MapSet.new([
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_property_hierarchy(facts, key)
      assert TBoxCache.cached?(:property_hierarchy, key)

      triples = [{iri("hasSon"), rdfs_subPropertyOf(), iri("hasChild")}]
      result = TBoxCache.invalidate_affected(triples, key)

      assert result.property_hierarchy == true
      refute TBoxCache.cached?(:property_hierarchy, key)
    end

    test "invalidates property hierarchy when characteristic triples provided", %{key: key} do
      facts =
        MapSet.new([
          {iri("knows"), rdf_type(), owl_SymmetricProperty()}
        ])

      {:ok, _} = TBoxCache.compute_and_store_property_hierarchy(facts, key)
      assert TBoxCache.cached?(:property_hierarchy, key)

      triples = [{iri("likes"), rdf_type(), owl_TransitiveProperty()}]
      result = TBoxCache.invalidate_affected(triples, key)

      assert result.property_hierarchy == true
      refute TBoxCache.cached?(:property_hierarchy, key)
    end

    test "does not invalidate when only instance triples provided", %{key: key} do
      facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(facts, key)
      assert TBoxCache.cached?(:class_hierarchy, key)

      triples = [{iri("alice"), rdf_type(), iri("Student")}]
      result = TBoxCache.invalidate_affected(triples, key)

      assert result.class_hierarchy == false
      assert result.property_hierarchy == false
      assert TBoxCache.cached?(:class_hierarchy, key)
    end
  end

  # ============================================================================
  # Tests: TBox Recomputation
  # ============================================================================

  describe "recompute_hierarchies/2" do
    setup do
      key = :"recompute_test_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:class_hierarchy, key)
        TBoxCache.clear(:property_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "recomputes both class and property hierarchies", %{key: key} do
      facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")},
          {iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")},
          {iri("knows"), rdf_type(), owl_SymmetricProperty()}
        ])

      {:ok, stats} = TBoxCache.recompute_hierarchies(facts, key)

      assert stats.class.class_count == 2
      assert stats.property.property_count == 3
      assert TBoxCache.cached?(:class_hierarchy, key)
      assert TBoxCache.cached?(:property_hierarchy, key)
    end
  end

  describe "handle_tbox_update/4" do
    setup do
      key = :"handle_update_test_#{System.unique_integer([:positive])}"

      on_exit(fn ->
        TBoxCache.clear(:class_hierarchy, key)
        TBoxCache.clear(:property_hierarchy, key)
      end)

      {:ok, key: key}
    end

    test "returns tbox_modified: false when no TBox triples", %{key: key} do
      modified = [{iri("alice"), iri("knows"), iri("bob")}]
      current_facts = MapSet.new(modified)

      {:ok, result} = TBoxCache.handle_tbox_update(modified, current_facts, key)

      assert result.tbox_modified == false
      assert result.invalidated.class_hierarchy == false
      assert result.invalidated.property_hierarchy == false
      assert result.recomputed == nil
    end

    test "invalidates and recomputes class hierarchy when subClassOf added", %{key: key} do
      # Start with initial hierarchy
      initial_facts =
        MapSet.new([
          {iri("Student"), rdfs_subClassOf(), iri("Person")}
        ])

      {:ok, _} = TBoxCache.compute_and_store_class_hierarchy(initial_facts, key)

      # Add a new subClassOf triple
      new_triple = {iri("GradStudent"), rdfs_subClassOf(), iri("Student")}
      modified = [new_triple]
      current_facts = MapSet.put(initial_facts, new_triple)

      {:ok, result} = TBoxCache.handle_tbox_update(modified, current_facts, key)

      assert result.tbox_modified == true
      assert result.invalidated.class_hierarchy == true
      assert result.recomputed != nil
      assert result.recomputed.class.class_count == 3
    end

    test "skips recomputation when recompute: false option provided", %{key: key} do
      modified = [{iri("Student"), rdfs_subClassOf(), iri("Person")}]
      current_facts = MapSet.new(modified)

      {:ok, result} = TBoxCache.handle_tbox_update(modified, current_facts, key, recompute: false)

      assert result.tbox_modified == true
      assert result.invalidated.class_hierarchy == true
      assert result.recomputed == nil
      refute TBoxCache.cached?(:class_hierarchy, key)
    end

    test "handles property characteristic updates", %{key: key} do
      modified = [{iri("knows"), rdf_type(), owl_SymmetricProperty()}]
      current_facts = MapSet.new(modified)

      {:ok, result} = TBoxCache.handle_tbox_update(modified, current_facts, key)

      assert result.tbox_modified == true
      assert result.invalidated.property_hierarchy == true
      assert result.recomputed.property.symmetric_count == 1
    end
  end

  describe "needs_recomputation?/1" do
    test "returns class_hierarchy: true for subClassOf triples" do
      triples = [{iri("Student"), rdfs_subClassOf(), iri("Person")}]
      result = TBoxCache.needs_recomputation?(triples)

      assert result.class_hierarchy == true
      assert result.property_hierarchy == false
      assert result.any == true
    end

    test "returns property_hierarchy: true for subPropertyOf triples" do
      triples = [{iri("hasChild"), rdfs_subPropertyOf(), iri("hasDescendant")}]
      result = TBoxCache.needs_recomputation?(triples)

      assert result.class_hierarchy == false
      assert result.property_hierarchy == true
      assert result.any == true
    end

    test "returns property_hierarchy: true for characteristic triples" do
      triples = [{iri("knows"), rdf_type(), owl_SymmetricProperty()}]
      result = TBoxCache.needs_recomputation?(triples)

      assert result.class_hierarchy == false
      assert result.property_hierarchy == true
      assert result.any == true
    end

    test "returns both: true for mixed TBox triples" do
      triples = [
        {iri("Student"), rdfs_subClassOf(), iri("Person")},
        {iri("knows"), rdf_type(), owl_SymmetricProperty()}
      ]

      result = TBoxCache.needs_recomputation?(triples)

      assert result.class_hierarchy == true
      assert result.property_hierarchy == true
      assert result.any == true
    end

    test "returns all false for instance triples" do
      triples = [
        {iri("alice"), iri("knows"), iri("bob")},
        {iri("alice"), rdf_type(), iri("Person")}
      ]

      result = TBoxCache.needs_recomputation?(triples)

      assert result.class_hierarchy == false
      assert result.property_hierarchy == false
      assert result.any == false
    end
  end
end
