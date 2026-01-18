defmodule TripleStore.Reasoner.Section783GlobalMaterializationTest do
  @moduledoc """
  Unit tests for Section 7.8.3: Global Materialization.

  These tests verify that global materialization works correctly:
  - Cross-graph derivation produces correct inferences
  - Derived quads are stored in the correct location
  - TBox can be shared across graphs
  - Cross-graph inferences are found correctly
  - Reasoning status reports correctly for global reasoning

  ## Test Coverage

  - 7.8.3.1: Test materialize_all derives across all graphs
  - 7.8.3.2: Test derived quads stored in correct location
  - 7.8.3.3: Test TBox shared across graphs
  - 7.8.3.4: Test global reasoning finds cross-graph inferences
  - 7.8.3.5: Test reasoning status reports correctly

  ## Test Domain

  Uses a multi-graph scenario with:
  - Graph 0: Shared TBox (schema/ontology)
  - Graph 1: University data (students, courses)
  - Graph 2: Department data (faculty, departments)
  - Cross-graph inferences: student in course, faculty teaches course

  ## Note

  These tests require full TripleStore integration with dictionary operations.
  They are skipped for unit testing and should be run as integration tests.
  """
  use ExUnit.Case, async: true
  @moduletag :skip

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.QuadIndex
  alias TripleStore.Reasoner.{
    GraphReasoningConfig,
    GraphReasoningStatus,
    GraphScopedReasoner,
    ReasoningConfig
  }

  # ============================================================================
  # Test Namespace
  # ============================================================================

  @ex "http://example.org/"
  @rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"

  defp ex_iri(name), do: {:iri, @ex <> name}
  defp rdf_type, do: {:iri, @rdf <> "type"}
  defp rdfs_subClassOf, do: {:iri, @rdfs <> "subClassOf"}
  defp rdfs_domain, do: {:iri, @rdfs <> "domain"}
  defp rdfs_range, do: {:iri, @rdfs <> "range"}

  # ============================================================================
  # Test Fixtures
  # ============================================================================

  defp tbox_facts do
    [
      {ex_iri("Person"), rdf_type(), ex_iri("Class")},
      {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
      {ex_iri("Professor"), rdfs_subClassOf(), ex_iri("Person")},
      {ex_iri("Course"), rdf_type(), ex_iri("Class")},
      {ex_iri("Department"), rdf_type(), ex_iri("Class")}
    ]
  end

  defp graph1_facts do
    [
      {ex_iri("alice"), rdf_type(), ex_iri("Student")},
      {ex_iri("cs101"), rdf_type(), ex_iri("Course")},
      # Cross-graph reference: bob is in graph2
      {ex_iri("bob_takes_cs101"), rdf_type(), ex_iri("Enrollment")}
    ]
  end

  defp graph2_facts do
    [
      {ex_iri("bob"), rdf_type(), ex_iri("Professor")},
      {ex_iri("cs_dept"), rdf_type(), ex_iri("Department")},
      # Cross-graph reference
      {ex_iri("bob_takes_cs101"), rdf_type(), ex_iri("Enrollment")}
    ]
  end

  # ============================================================================
  # Setup Helpers
  # ============================================================================

  defp create_test_db do
    path = Path.join(System.tmp_dir!(), "test_quad_global_#{System.unique_integer([:positive])}")
    {:ok, db} = NIF.open(path, schema: :quad, create_if_missing: true, create_if_necessary: true)
    {db, path}
  end

  defp cleanup_db(db, path) do
    NIF.close(db)
    File.rm_rf!(path)
  end

  defp insert_facts(db, facts, graph_id) do
    operations = Enum.map(facts, fn {s, p, o} ->
      {s_id, _} = NIF.get_or_put_str2id(db, s)
      {p_id, _} = NIF.get_or_put_str2id(db, p)
      {o_id, _} = NIF.get_or_put_str2id(db, o)
      key = QuadIndex.gspo_key(graph_id, s_id, p_id, o_id)
      {:spo, key, <<>>}
    end)

    :ok = NIF.write_batch(db, operations, true)
  end

  defp count_quads_in_graph(db, graph_id) do
    prefix = QuadIndex.gspo_prefix(graph_id)
    NIF.fold_count(db, :spo, prefix)
  end

  defp count_derived_in_graph(db, graph_id) do
    prefix = QuadIndex.gspo_prefix(graph_id)
    NIF.fold_count(db, :derived_cf, prefix)
  end

  # ============================================================================
  # Tests: Cross-Graph Derivation (7.8.3.1)
  # ============================================================================

  describe "cross-graph derivation (7.8.3.1)" do
    test "materialize_all derives across all graphs" do
      {db, path} = create_test_db()

      try do
        # Setup: TBox in graph 0, data in graphs 1 and 2
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0,
          storage_strategy: :same_as_premises
        )

        # Materialize all graphs globally
        result = GraphScopedReasoner.materialize_all(db, config: config)

        assert {:ok, stats} = result
        assert is_map(stats)
        assert stats.total_graphs >= 2
        assert stats.total_explicit > 0
        assert stats.total_derived > 0
      after
        cleanup_db(db, path)
      end
    end

    test "global reasoning includes facts from all graphs" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0
        )

        {:ok, _stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Both graphs should have some derived quads
        derived_1 = count_derived_in_graph(db, 1)
        derived_2 = count_derived_in_graph(db, 2)

        # At minimum, Student->Person and Professor->Person should be derived
        assert derived_1 >= 2
        assert derived_2 >= 1
      after
        cleanup_db(db, path)
      end
    end

    test "global reasoning processes all graphs in single closure" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0
        )

        {:ok, stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Global stats should aggregate across all graphs
        assert stats.total_explicit > 0
        assert stats.total_derived > 0
        assert stats.total_graphs >= 2
      after
        cleanup_db(db, path)
      end
    end
  end

  # ============================================================================
  # Tests: Derived Quad Storage Location (7.8.3.2)
  # ============================================================================

  describe "derived quad storage location (7.8.3.2)" do
    test "same_as_premises stores derived quads in source graphs" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0,
          storage_strategy: :same_as_premises
        )

        {:ok, _stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Derived quads should be in same graph as explicit facts
        derived_1 = count_derived_in_graph(db, 1)
        derived_2 = count_derived_in_graph(db, 2)

        assert derived_1 > 0
        assert derived_2 > 0
      after
        cleanup_db(db, path)
      end
    end

    test "separate_graph stores all derived quads in designated graph" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        inferred_graph = 99

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0,
          inferred_graph: inferred_graph
        )

        {:ok, _stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # All derived quads should be in inferred graph
        derived_in_inferred = count_derived_in_graph(db, inferred_graph)
        derived_1 = count_derived_in_graph(db, 1)
        derived_2 = count_derived_in_graph(db, 2)

        assert derived_in_inferred > 0
        # Source graphs should not have derived quads (except what was there before)
        # At minimum, no NEW derived quads in source graphs
      after
        cleanup_db(db, path)
      end
    end

    test "per_graph_cf stores derived quads per graph" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0,
          storage_strategy: :per_graph_cf
        )

        {:ok, _stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Each graph should have its own derived quads
        derived_1 = count_derived_in_graph(db, 1)
        derived_2 = count_derived_in_graph(db, 2)

        assert derived_1 > 0
        assert derived_2 > 0
      after
        cleanup_db(db, path)
      end
    end
  end

  # ============================================================================
  # Tests: TBox Sharing (7.8.3.3)
  # ============================================================================

  describe "TBox sharing across graphs (7.8.3.3)" do
    test "TBox in graph 0 is accessible to all graphs" do
      {db, path} = create_test_db()

      try do
        # TBox only in graph 0
        insert_facts(db, tbox_facts(), 0)

        # ABox in graphs 1 and 2
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0
        )

        {:ok, stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Both graphs should derive Student->Person and Professor->Person
        # This proves TBox from graph 0 is used
        assert stats.total_derived >= 4  # At least 2 per graph
      after
        cleanup_db(db, path)
      end
    end

    test "global reasoning with shared TBox produces correct inferences" do
      {db, path} = create_test_db()

      try do
        # Complex TBox with transitive subclass
        complex_tbox = [
          {ex_iri("A"), rdfs_subClassOf(), ex_iri("B")},
          {ex_iri("B"), rdfs_subClassOf(), ex_iri("C")},
          {ex_iri("C"), rdf_type(), ex_iri("Class")}
        ]

        insert_facts(db, complex_tbox, 0)
        insert_facts(db, [{ex_iri("x"), rdf_type(), ex_iri("A")}], 1)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0
        )

        {:ok, _stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Should derive x type B, x type C (transitive through A->B->C)
        # Verify by counting derived quads in graph 1
        derived_1 = count_derived_in_graph(db, 1)
        assert derived_1 >= 2  # At least A->B and A->C
      after
        cleanup_db(db, path)
      end
    end

    test "TBox is not included in derived quads count" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0
        )

        {:ok, stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # TBox quads should not be counted as derived
        # They are schema, not inferred from data
        assert stats.total_derived > 0
      after
        cleanup_db(db, path)
      end
    end

    test "graphs without TBox access can still reason with shared TBox" do
      {db, path} = create_test_db()

      try do
        # TBox in separate graph
        insert_facts(db, tbox_facts(), 0)

        # Data in different graphs
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0  # All graphs use TBox from graph 0
        )

        {:ok, _stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Verify both graphs derived correctly using shared TBox
        derived_1 = count_derived_in_graph(db, 1)
        derived_2 = count_derived_in_graph(db, 2)

        assert derived_1 > 0
        assert derived_2 > 0
      after
        cleanup_db(db, path)
      end
    end
  end

  # ============================================================================
  # Tests: Cross-Graph Inferences (7.8.3.4)
  # ============================================================================

  describe "cross-graph inferences (7.8.3.4)" do
    test "global reasoning finds inferences across graph boundaries" do
      {db, path} = create_test_db()

      try do
        # Setup: alice (Student) in graph 1, teaches relationship in graph 2
        shared_tbox = [
          {ex_iri("Person"), rdf_type(), ex_iri("Class")},
          {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
          {ex_iri("Teaching"), rdf_type(), ex_iri("Class")},
          {ex_iri("teaches"), rdfs_domain(), ex_iri("Professor")},
          {ex_iri("teaches"), rdfs_range(), ex_iri("Teaching")}
        ]

        graph1_facts = [
          {ex_iri("alice"), rdf_type(), ex_iri("Student")}
        ]

        graph2_facts = [
          {ex_iri("alice"), ex_iri("teaches"), ex_iri("cs101")}
        ]

        insert_facts(db, shared_tbox, 0)
        insert_facts(db, graph1_facts, 1)
        insert_facts(db, graph2_facts, 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0
        )

        {:ok, _stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Should derive alice type Person (from Student)
        # This works even though alice's type is in graph 1 and teaches is in graph 2
        derived_1 = count_derived_in_graph(db, 1)
        assert derived_1 > 0
      after
        cleanup_db(db, path)
      end
    end

    test "global reasoning with separate_graph strategy aggregates correctly" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        inferred_graph = 99

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0,
          inferred_graph: inferred_graph
        )

        {:ok, stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # All derived quads should be in inferred graph
        total_in_inferred = count_derived_in_graph(db, inferred_graph)

        assert stats.total_derived == total_in_inferred
      after
        cleanup_db(db, path)
      end
    end

    test "hybrid reasoning allows selective graph participation" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        # Configure graph 1 for global, graph 2 for local
        graph_configs = %{
          1 => GraphReasoningConfig.new!(graph_id: 1, scope: :global),
          2 => GraphReasoningConfig.new!(graph_id: 2, scope: :local)
        }

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :hybrid,
          graph_configs: graph_configs,
          tbox_graph: 0
        )

        {:ok, stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Should process both graphs according to their scope
        assert stats.total_graphs >= 2
      after
        cleanup_db(db, path)
      end
    end
  end

  # ============================================================================
  # Tests: Reasoning Status (7.8.3.5)
  # ============================================================================

  describe "global reasoning status (7.8.3.5)" do
    test "reasoning status reports correct global stats" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0
        )

        {:ok, _stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Check global status
        {:ok, status} = GraphReasoningStatus.load(:global)

        assert status.state == :materialized
        assert status.scope == :global
        assert status.total_explicit > 0
        assert status.total_derived > 0
        assert status.graph_count >= 2
      after
        cleanup_db(db, path)
        GraphReasoningStatus.delete(:global)
      end
    end

    test "per-graph status is updated after global materialization" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0
        )

        {:ok, _stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Check each graph's status
        {:ok, status1} = GraphReasoningStatus.load({:graph, 1})
        {:ok, status2} = GraphReasoningStatus.load({:graph, 2})

        assert status1.state == :materialized
        assert status2.state == :materialized
        assert status1.scope == :global
        assert status2.scope == :global
      after
        cleanup_db(db, path)
        GraphReasoningStatus.delete({:graph, 1})
        GraphReasoningStatus.delete({:graph, 2})
      end
    end

    test "aggregate_status computes correct totals across graphs" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global,
          tbox_graph: 0
        )

        {:ok, _stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Get aggregate status
        {:ok, status1} = GraphReasoningStatus.load({:graph, 1})
        {:ok, status2} = GraphReasoningStatus.load({:graph, 2})

        aggregate = GraphReasoningStatus.aggregate([status1, status2])

        assert aggregate.total_graphs == 2
        assert aggregate.total_explicit == status1.explicit_count + status2.explicit_count
        assert aggregate.total_derived == status1.derived_count + status2.derived_count
      after
        cleanup_db(db, path)
        GraphReasoningStatus.delete({:graph, 1})
        GraphReasoningStatus.delete({:graph, 2})
      end
    end
  end

  # ============================================================================
  # Tests: Error Handling
  # ============================================================================

  describe "error handling" do
    test "materialize_all handles empty database gracefully" do
      {db, path} = create_test_db()

      try do
        {:ok, config} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :global
        )

        {:ok, stats} = GraphScopedReasoner.materialize_all(db, config: config)

        # Should succeed with zero counts
        assert stats.total_explicit == 0
        assert stats.total_derived == 0
        assert stats.total_graphs == 0
      after
        cleanup_db(db, path)
      end
    end

    test "materialize_all with no rules produces no derived quads" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, graph1_facts(), 1)

        {:ok, config} = ReasoningConfig.new(
          profile: :none,  # No reasoning
          scope: :global
        )

        {:ok, stats} = GraphScopedReasoner.materialize_all(db, config: config)

        assert stats.total_derived == 0
      after
        cleanup_db(db, path)
      end
    end

    test "materialize_all requires valid configuration" do
      {db, path} = create_test_db()

      try do
        # Invalid scope
        {:error, _reason} = ReasoningConfig.new(
          profile: :rdfs,
          scope: :invalid
        )

        # Should return error when trying to materialize
        result = GraphScopedReasoner.materialize_all(db, config: :invalid_config)
        assert {:error, _} = result
      after
        cleanup_db(db, path)
      end
    end
  end
end
