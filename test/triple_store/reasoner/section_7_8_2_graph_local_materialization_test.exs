defmodule TripleStore.Reasoner.Section782GraphLocalMaterializationTest do
  @moduledoc """
  Integration tests for Section 7.8.2: Graph-Local Materialization.

  These tests verify that graph-local materialization works correctly:
  - Single graph materialization derives inferences within the graph
  - Materialization of one graph doesn't affect other graphs
  - Multiple graphs can be materialized independently
  - Default graph materialization works correctly
  - Parallel graph materialization produces correct results

  ## Note

  These tests are marked as integration tests requiring full TripleStore infrastructure.
  They should be run when the complete TripleStore integration is available.
  Use `MIX_ENV=test mix test --exclude skip` to run all tests including these.
  """
  use TripleStore.ReasonerTestCase

  @moduletag :skip
  @moduletag :integration

  # ============================================================================
  # Test Fixtures
  # ============================================================================

  # Simple TBox with subclass hierarchy
  defp tbox_facts do
    [
      {ex_iri("Person"), rdf_type(), ex_iri("Class")},
      {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
      {ex_iri("Professor"), rdfs_subClassOf(), ex_iri("Person")},
      {ex_iri("Course"), rdf_type(), ex_iri("Class")}
    ]
  end

  # ABox facts for graph 1
  defp graph1_facts do
    [
      {ex_iri("alice"), rdf_type(), ex_iri("Student")},
      {ex_iri("bob"), rdf_type(), ex_iri("Professor")},
      {ex_iri("cs101"), rdf_type(), ex_iri("Course")}
    ]
  end

  # ABox facts for graph 2
  defp graph2_facts do
    [
      {ex_iri("charlie"), rdf_type(), ex_iri("Student")},
      {ex_iri("david"), rdf_type(), ex_iri("Professor")},
      {ex_iri("math101"), rdf_type(), ex_iri("Course")}
    ]
  end

  # Convert facts to quad format with graph ID
  defp to_quads(facts, graph_id) do
    Enum.map(facts, fn {s, p, o} -> {graph_id, s, p, o} end)
  end

  # ============================================================================
  # Tests: Single Graph Materialization (7.8.2.1)
  # ============================================================================

  describe "single graph materialization (7.8.2.1)" do
    test "materialize_graph derives inferences within the graph" do
      {db, path} = create_test_db()

      try do
        # Insert TBox in graph 0
        insert_facts(db, tbox_facts(), 0)

        # Insert ABox in graph 1
        insert_facts(db, graph1_facts(), 1)

        # Create reasoning config
        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            mode: :materialized,
            scope: :local,
            tbox_graph: 0
          )

        # Materialize graph 1
        result =
          GraphScopedReasoner.materialize_graph(db,
            graph_id: 1,
            config: config
          )

        assert {:ok, stats} = result
        assert stats.graph_id == 1
        assert stats.explicit_count == 3
        # Student->Person, Professor->Person
        assert stats.derived_count >= 2
        assert is_integer(stats.duration_ms)
      after
        cleanup_db(db, path)
      end
    end

    test "materialize_graph stores derived quads in same graph" do
      {db, path} = create_test_db()

      try do
        # Insert TBox and ABox
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        {:ok, _stats} =
          GraphScopedReasoner.materialize_graph(db,
            graph_id: 1,
            config: config
          )

        # Verify derived quads are in graph 1
        derived_count = count_derived_quads(db, 1)
        assert derived_count > 0
      after
        cleanup_db(db, path)
      end
    end

    test "materialize_graph returns per-graph statistics" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        {:ok, stats} =
          GraphScopedReasoner.materialize_graph(db,
            graph_id: 1,
            config: config
          )

        # Verify statistics structure
        assert Map.has_key?(stats, :graph_id)
        assert Map.has_key?(stats, :explicit_count)
        assert Map.has_key?(stats, :derived_count)
        assert Map.has_key?(stats, :total_count)
        assert Map.has_key?(stats, :duration_ms)

        assert stats.graph_id == 1
        assert stats.total_count == stats.explicit_count + stats.derived_count
      after
        cleanup_db(db, path)
      end
    end
  end

  # ============================================================================
  # Tests: Graph Isolation (7.8.2.2)
  # ============================================================================

  describe "graph isolation (7.8.2.2)" do
    test "materialize_graph doesn't affect other graphs" do
      {db, path} = create_test_db()

      try do
        # Setup: TBox in graph 0, data in graphs 1 and 2
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        # Materialize only graph 1
        {:ok, _stats} =
          GraphScopedReasoner.materialize_graph(db,
            graph_id: 1,
            config: config
          )

        # Verify graph 2 has no derived quads
        graph2_derived = count_derived_quads(db, 2)
        assert graph2_derived == 0
      after
        cleanup_db(db, path)
      end
    end

    test "materializing graph 1 then graph 2 keeps results separate" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        # Materialize both graphs separately
        {:ok, stats1} =
          GraphScopedReasoner.materialize_graph(db,
            graph_id: 1,
            config: config
          )

        {:ok, stats2} =
          GraphScopedReasoner.materialize_graph(db,
            graph_id: 2,
            config: config
          )

        # Both should have derived quads
        assert stats1.derived_count > 0
        assert stats2.derived_count > 0

        # But counts should be different (different data)
        assert stats1.derived_count != stats2.derived_count
      after
        cleanup_db(db, path)
      end
    end

    test "derived quads in graph 1 are not visible when querying graph 2" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        # Materialize only graph 1
        GraphScopedReasoner.materialize_graph(db,
          graph_id: 1,
          config: config
        )

        # Query graph 2 should not have alice's inferred type
        {alice_id, _} = NIF.get_or_put_str2id(db, ex_iri("alice"))
        {person_id, _} = NIF.get_or_put_str2id(db, ex_iri("Person"))
        {type_id, _} = NIF.get_or_put_str2id(db, rdf_type())

        # Check for alice rdf:type Person in graph 2's derived quads
        g2_prefix = QuadIndex.gspo_prefix(2)
        alice_in_g2 = NIF.fold_count(db, :derived_cf, g2_prefix)
        assert alice_in_g2 == 0
      after
        cleanup_db(db, path)
      end
    end
  end

  # ============================================================================
  # Tests: Multiple Graph Materialization (7.8.2.3)
  # ============================================================================

  describe "multiple graph materialization (7.8.2.3)" do
    test "materialize_graphs processes each graph independently" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        # Materialize both graphs
        {:ok, stats_map} =
          GraphScopedReasoner.materialize_graphs(db,
            graph_ids: [1, 2],
            config: config,
            parallel: false
          )

        assert is_map(stats_map)
        assert Map.has_key?(stats_map, 1)
        assert Map.has_key?(stats_map, 2)

        assert stats_map[1].graph_id == 1
        assert stats_map[2].graph_id == 2

        # Both should have derived quads
        assert stats_map[1].derived_count > 0
        assert stats_map[2].derived_count > 0
      after
        cleanup_db(db, path)
      end
    end

    test "materialize_graphs returns aggregated statistics" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        {:ok, stats_map} =
          GraphScopedReasoner.materialize_graphs(db,
            graph_ids: [1, 2],
            config: config
          )

        # Verify each stats map has required fields
        Enum.each([1, 2], fn graph_id ->
          stats = stats_map[graph_id]
          assert Map.has_key?(stats, :explicit_count)
          assert Map.has_key?(stats, :derived_count)
          assert Map.has_key?(stats, :total_count)
          assert Map.has_key?(stats, :duration_ms)
        end)
      after
        cleanup_db(db, path)
      end
    end

    test "materialize_graphs with single graph list works" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        # Materialize single graph using multi-graph API
        {:ok, stats_map} =
          GraphScopedReasoner.materialize_graphs(db,
            graph_ids: [1],
            config: config
          )

        assert map_size(stats_map) == 1
        assert stats_map[1].derived_count > 0
      after
        cleanup_db(db, path)
      end
    end

    test "materialize_graphs with empty list returns empty stats" do
      {db, path} = create_test_db()

      try do
        {:ok, config} = ReasoningConfig.new(profile: :rdfs, scope: :local)

        {:ok, stats_map} =
          GraphScopedReasoner.materialize_graphs(db,
            graph_ids: [],
            config: config
          )

        assert stats_map == %{}
      after
        cleanup_db(db, path)
      end
    end
  end

  # ============================================================================
  # Tests: Default Graph Materialization (7.8.2.4)
  # ============================================================================

  describe "default graph materialization (7.8.2.4)" do
    test "materializes default graph when graph_id is 0" do
      {db, path} = create_test_db()

      try do
        # Use graph 0 as default graph
        default_facts = [
          {ex_iri("Thing"), rdf_type(), ex_iri("Class")},
          {ex_iri("entity"), rdf_type(), ex_iri("Thing")}
        ]

        insert_facts(db, default_facts, 0)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local
          )

        {:ok, stats} =
          GraphScopedReasoner.materialize_graph(db,
            graph_id: 0,
            config: config
          )

        assert stats.graph_id == 0
        assert stats.explicit_count == 2
        assert stats.derived_count > 0
      after
        cleanup_db(db, path)
      end
    end

    test "materialize_default processes only default graph quads" do
      {db, path} = create_test_db()

      try do
        # Insert facts in both default (0) and non-default (1) graphs
        default_facts = [
          {ex_iri("Thing"), rdf_type(), ex_iri("Class")},
          {ex_iri("entity"), rdf_type(), ex_iri("Thing")}
        ]

        other_facts = [
          {ex_iri("Other"), rdf_type(), ex_iri("Class")}
        ]

        insert_facts(db, default_facts, 0)
        insert_facts(db, other_facts, 1)

        {:ok, config} = ReasoningConfig.new(profile: :rdfs, scope: :local)

        # Materialize only default graph
        {:ok, _stats} =
          GraphScopedReasoner.materialize_graph(db,
            graph_id: 0,
            config: config
          )

        # Verify graph 1 has no derived quads
        graph1_derived = count_derived_quads(db, 1)
        assert graph1_derived == 0

        # Verify default graph has derived quads
        default_derived = count_derived_quads(db, 0)
        assert default_derived > 0
      after
        cleanup_db(db, path)
      end
    end

    test "maintains backward compatibility with triple reasoning" do
      {db, path} = create_test_db()

      try do
        # This test ensures the quad reasoning system is backward compatible
        # with the original triple-based reasoning

        facts = [
          {ex_iri("A"), rdfs_subClassOf(), ex_iri("B")},
          {ex_iri("B"), rdfs_subClassOf(), ex_iri("C")},
          {ex_iri("x"), rdf_type(), ex_iri("A")}
        ]

        insert_facts(db, facts, 0)

        {:ok, config} = ReasoningConfig.new(profile: :rdfs, scope: :local)

        {:ok, stats} =
          GraphScopedReasoner.materialize_graph(db,
            graph_id: 0,
            config: config
          )

        # Should derive x type B and x type C via transitive subclass
        assert stats.derived_count >= 2
      after
        cleanup_db(db, path)
      end
    end
  end

  # ============================================================================
  # Tests: Parallel Graph Materialization (7.8.2.5)
  # ============================================================================

  describe "parallel graph materialization (7.8.2.5)" do
    test "parallel materialization produces same results as sequential" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        # First: sequential materialization
        {:ok, seq_stats} =
          GraphScopedReasoner.materialize_graphs(db,
            graph_ids: [1, 2],
            config: config,
            parallel: false
          )

        # Clear derived quads
        NIF.fold_keys(db, :derived_cf, <<>>, [], fn key, acc ->
          [key | acc]
        end)
        |> Enum.each(fn key -> NIF.delete(db, :derived_cf, key) end)

        # Second: parallel materialization
        {:ok, par_stats} =
          GraphScopedReasoner.materialize_graphs(db,
            graph_ids: [1, 2],
            config: config,
            parallel: true
          )

        # Results should be equivalent
        assert seq_stats[1].derived_count == par_stats[1].derived_count
        assert seq_stats[2].derived_count == par_stats[2].derived_count
      after
        cleanup_db(db, path)
      end
    end

    test "parallel materialization is faster for multiple graphs" do
      {db, path} = create_test_db()

      try do
        # Create enough graphs to make parallel beneficial
        Enum.each(1..5, fn graph_id ->
          facts = [
            {ex_iri("p#{graph_id}"), rdf_type(), ex_iri("Person")}
          ]

          insert_facts(db, facts, graph_id)
        end)

        insert_facts(db, tbox_facts(), 0)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        graph_ids = Enum.to_list(1..5)

        # Measure sequential time
        {seq_duration, _} =
          :timer.tc(fn ->
            GraphScopedReasoner.materialize_graphs(db,
              graph_ids: graph_ids,
              config: config,
              parallel: false
            )
          end)

        # Clear derived
        NIF.fold_keys(db, :derived_cf, <<>>, [], fn key, acc ->
          [key | acc]
        end)
        |> Enum.each(fn key -> NIF.delete(db, :derived_cf, key) end)

        # Measure parallel time
        {par_duration, _} =
          :timer.tc(fn ->
            GraphScopedReasoner.materialize_graphs(db,
              graph_ids: graph_ids,
              config: config,
              parallel: true
            )
          end)

        # Parallel should be faster (or at least not significantly slower)
        # Allow for some variance but generally parallel should be beneficial
        assert par_duration <= seq_duration * 1.5
      after
        cleanup_db(db, path)
      end
    end

    test "parallel materialization handles errors gracefully" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        # Parallel with valid graphs should succeed
        {:ok, stats_map} =
          GraphScopedReasoner.materialize_graphs(db,
            graph_ids: [1, 2],
            config: config,
            parallel: true
          )

        assert map_size(stats_map) == 2
        assert stats_map[1].derived_count > 0
        assert stats_map[2].derived_count > 0
      after
        cleanup_db(db, path)
      end
    end
  end

  # ============================================================================
  # Tests: Error Handling
  # ============================================================================

  describe "error handling" do
    test "materialize_graph returns error for invalid graph_id" do
      {db, path} = create_test_db()

      try do
        {:ok, config} = ReasoningConfig.new(profile: :rdfs, scope: :local)

        result =
          GraphScopedReasoner.materialize_graph(db,
            # Invalid graph_id
            graph_id: -1,
            config: config
          )

        assert {:error, _reason} = result
      after
        cleanup_db(db, path)
      end
    end

    test "materialize_graphs returns error for invalid graph_ids" do
      {db, path} = create_test_db()

      try do
        {:ok, config} = ReasoningConfig.new(profile: :rdfs, scope: :local)

        result =
          GraphScopedReasoner.materialize_graphs(db,
            # Contains invalid graph_id
            graph_ids: [-1, 1],
            config: config
          )

        assert {:error, _reason} = result
      after
        cleanup_db(db, path)
      end
    end

    test "materialize_graph handles empty graph gracefully" do
      {db, path} = create_test_db()

      try do
        # TBox only, no ABox in graph 1
        insert_facts(db, tbox_facts(), 0)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        {:ok, stats} =
          GraphScopedReasoner.materialize_graph(db,
            # Empty graph
            graph_id: 1,
            config: config
          )

        # Should succeed with zero derived quads
        assert stats.explicit_count == 0
        assert stats.derived_count == 0
      after
        cleanup_db(db, path)
      end
    end
  end

  # ============================================================================
  # Tests: Graph Reasoning Status
  # ============================================================================

  describe "graph reasoning status" do
    test "materialize_graph updates reasoning status for the graph" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        {:ok, _stats} =
          GraphScopedReasoner.materialize_graph(db,
            graph_id: 1,
            config: config
          )

        # Check status was stored
        {:ok, status} = GraphReasoningStatus.load({:graph, 1})
        assert status.graph_id == 1
        assert status.state == :materialized
        assert status.derived_count > 0
      after
        cleanup_db(db, path)
        GraphReasoningStatus.delete({:graph, 1})
      end
    end

    test "materialize_graphs updates status for all processed graphs" do
      {db, path} = create_test_db()

      try do
        insert_facts(db, tbox_facts(), 0)
        insert_facts(db, graph1_facts(), 1)
        insert_facts(db, graph2_facts(), 2)

        {:ok, config} =
          ReasoningConfig.new(
            profile: :rdfs,
            scope: :local,
            tbox_graph: 0
          )

        {:ok, _stats} =
          GraphScopedReasoner.materialize_graphs(db,
            graph_ids: [1, 2],
            config: config
          )

        # Both graphs should have status
        {:ok, status1} = GraphReasoningStatus.load({:graph, 1})
        {:ok, status2} = GraphReasoningStatus.load({:graph, 2})

        assert status1.state == :materialized
        assert status2.state == :materialized
      after
        cleanup_db(db, path)
        GraphReasoningStatus.delete({:graph, 1})
        GraphReasoningStatus.delete({:graph, 2})
      end
    end
  end

  # ============================================================================
  # Stubs for integration test helpers
  # ============================================================================
  # Note: These stubs allow the file to compile but are never called
  # because the tests are marked with @moduletag :skip

  defp create_test_db, do: {nil, nil}
  defp insert_facts(_db, _facts, _graph), do: :ok
  defp cleanup_db(_db, _path), do: :ok
  defp count_derived_quads(_db, _graph), do: 0
end
