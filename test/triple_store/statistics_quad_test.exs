defmodule TripleStore.Statistics.QuadTest do
  @moduledoc """
  Unit tests for per-graph statistics (Section 5.1).

  Tests the per-graph statistics functions for quad stores including:
  - Graph quad counts
  - Graph predicate histograms
  - Graph distinct subjects/objects
  - Graph summaries
  - All graphs summaries
  """

  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.Statistics

  @test_db_base "/tmp/statistics_quad_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"

    # Ensure clean directory
    File.rm_rf(test_path)

    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      try do
        if Process.alive?(manager), do: Manager.stop(manager)
      catch
        :exit, _ -> :ok
      end

      ErlangAdapter.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager}
  end

  # Helper to insert test quads using raw integer IDs
  defp insert_test_quads(db, _manager, graph_id, count) do
    quads =
      for i <- 1..count do
        # Create quad with: subject=i, predicate=1, object=i*10, graph=graph_id
        s_id = i
        p_id = 1
        o_id = i * 10
        {s_id, p_id, o_id, graph_id}
      end

    # Insert using QuadOperations
    :ok = QuadOperations.insert_quads(db, quads, [])
  end

  # ===========================================================================
  # 5.1.1 Graph Quad Counts
  # ===========================================================================

  describe "graph_quad_count/2" do
    test "returns correct count for populated graph", %{db: db, manager: manager} do
      graph_id = 100
      insert_test_quads(db, manager, graph_id, 50)

      assert {:ok, 50} = Statistics.graph_quad_count(db, graph_id)
    end

    test "returns 0 for empty graph", %{db: db} do
      graph_id = 200
      # Don't insert any quads

      assert {:ok, 0} = Statistics.graph_quad_count(db, graph_id)
    end

    test "works with default graph (ID 0)", %{db: db, manager: manager} do
      insert_test_quads(db, manager, 0, 25)

      assert {:ok, 25} = Statistics.graph_quad_count(db, 0)
    end
  end

  # ===========================================================================
  # 5.1.2 Graph Predicate Statistics
  # ===========================================================================

  describe "graph_predicate_histogram/2" do
    test "returns correct histogram for single predicate", %{db: db, manager: manager} do
      graph_id = 100
      insert_test_quads(db, manager, graph_id, 30)

      assert {:ok, histogram} = Statistics.graph_predicate_histogram(db, graph_id)
      assert map_size(histogram) == 1
      assert Map.get(histogram, 1) == 30
    end

    test "returns correct histogram for multiple predicates", %{db: db, manager: manager} do
      graph_id = 100

      # Insert quads with different predicates
      quads =
        for i <- 1..30 do
          # Predicates 1, 2, 3
          predicate_id = rem(i, 3) + 1
          {i, predicate_id, i * 10, graph_id}
        end

      :ok = QuadOperations.insert_quads(db, quads, [])

      assert {:ok, histogram} = Statistics.graph_predicate_histogram(db, graph_id)
      assert map_size(histogram) == 3
      # Each predicate should have 10 quads
      assert Map.get(histogram, 1) == 10
      assert Map.get(histogram, 2) == 10
      assert Map.get(histogram, 3) == 10
    end

    test "returns empty map for empty graph", %{db: db} do
      graph_id = 999

      assert {:ok, histogram} = Statistics.graph_predicate_histogram(db, graph_id)
      assert histogram == %{}
    end
  end

  # ===========================================================================
  # 5.1.3 Graph Subject/Object Statistics
  # ===========================================================================

  describe "graph_distinct_subjects/2" do
    test "returns correct count for distinct subjects", %{db: db, manager: manager} do
      graph_id = 100
      insert_test_quads(db, manager, graph_id, 20)

      assert {:ok, count} = Statistics.graph_distinct_subjects(db, graph_id)
      assert count == 20
    end

    test "handles duplicate subjects correctly", %{db: db, manager: manager} do
      graph_id = 100

      # Insert quads with duplicate subjects (same subject, different predicates/objects)
      quads =
        for i <- 1..10 do
          # Subject repeats every 2 times
          subject_id = div(i, 2) + 1
          {subject_id, 1, i * 10, graph_id}
        end

      :ok = QuadOperations.insert_quads(db, quads, [])

      assert {:ok, count} = Statistics.graph_distinct_subjects(db, graph_id)
      # Subjects 1-6
      assert count == 6
    end
  end

  describe "graph_object_count/2" do
    test "returns correct count for distinct objects", %{db: db, manager: manager} do
      graph_id = 100
      insert_test_quads(db, manager, graph_id, 15)

      assert {:ok, count} = Statistics.graph_object_count(db, graph_id)
      assert count == 15
    end

    test "handles duplicate objects correctly", %{db: db, manager: manager} do
      graph_id = 100

      # Insert quads with duplicate objects
      quads =
        for i <- 1..10 do
          # Object repeats every 2 times
          object_id = div(i, 2) * 10 + 10
          {i, 1, object_id, graph_id}
        end

      :ok = QuadOperations.insert_quads(db, quads, [])

      assert {:ok, count} = Statistics.graph_object_count(db, graph_id)
      # Objects 10, 20, 30, 40, 50, 60
      assert count == 6
    end
  end

  # ===========================================================================
  # 5.1.4 Graph Summary
  # ===========================================================================

  describe "graph_summary/3" do
    test "returns complete statistics for graph", %{db: db, manager: manager} do
      graph_id = 100
      insert_test_quads(db, manager, graph_id, 40)

      assert {:ok, summary} = Statistics.graph_summary(db, graph_id)
      assert summary.graph_id == graph_id
      assert summary.quad_count == 40
      assert summary.distinct_subjects == 40
      assert summary.distinct_predicates == 1
      assert summary.distinct_objects == 40
      assert is_map(summary.predicate_counts)
      assert summary.accuracy == :exact
    end

    test "marks large graphs as approximate", %{db: db, manager: manager} do
      graph_id = 100

      # Insert more than default sampling threshold (10000)
      quads = for i <- 1..11001, do: {i, 1, i * 10, graph_id}
      :ok = QuadOperations.insert_quads(db, quads, [])

      assert {:ok, summary} = Statistics.graph_summary(db, graph_id, sampling_threshold: 10000)
      assert summary.quad_count == 11001
      assert summary.accuracy == :approximate
    end

    test "returns exact for graphs below threshold", %{db: db, manager: manager} do
      graph_id = 100
      insert_test_quads(db, manager, graph_id, 5000)

      assert {:ok, summary} = Statistics.graph_summary(db, graph_id, sampling_threshold: 10000)
      assert summary.accuracy == :exact
    end

    test "can skip object count computation", %{db: db, manager: manager} do
      graph_id = 100
      insert_test_quads(db, manager, graph_id, 10)

      assert {:ok, summary} = Statistics.graph_summary(db, graph_id, include_object_count: false)
      assert summary.distinct_objects == :not_computed
    end

    test "returns not_found for non-existent graph", %{db: db} do
      # Non-zero graph ID with no quads should return not_found
      assert {:error, :not_found} = Statistics.graph_summary(db, 9999)
    end

    test "allows empty default graph", %{db: db} do
      # Default graph (ID 0) can be empty
      assert {:ok, summary} = Statistics.graph_summary(db, 0)
      assert summary.graph_id == 0
      assert summary.quad_count == 0
    end
  end

  # ===========================================================================
  # 5.1.5 All Graphs Summary
  # ===========================================================================

  describe "all_graphs_summary/2" do
    test "aggregates statistics across all graphs", %{db: db, manager: manager} do
      # Insert quads into multiple graphs
      # Default graph
      insert_test_quads(db, manager, 0, 100)
      # Named graph 100
      insert_test_quads(db, manager, 100, 200)
      # Named graph 200
      insert_test_quads(db, manager, 200, 150)

      assert {:ok, summary} = Statistics.all_graphs_summary(db)
      assert summary.total_quads == 450
      assert summary.graph_count == 3
      assert summary.largest_graph_id == 100
      assert summary.largest_graph_count == 200
      assert is_map(summary.per_graph)
    end

    test "includes per-graph breakdown when requested", %{db: db, manager: manager} do
      insert_test_quads(db, manager, 100, 50)
      insert_test_quads(db, manager, 200, 75)

      assert {:ok, summary} = Statistics.all_graphs_summary(db, include_per_graph: true)
      assert is_map(summary.per_graph)
      assert map_size(summary.per_graph) >= 2
      assert %{quad_count: 50} = summary.per_graph[100]
      assert %{quad_count: 75} = summary.per_graph[200]
    end

    test "can exclude per-graph breakdown", %{db: db, manager: manager} do
      insert_test_quads(db, manager, 100, 50)

      assert {:ok, summary} = Statistics.all_graphs_summary(db, include_per_graph: false)
      assert summary.per_graph == nil
    end

    test "can exclude default graph", %{db: db, manager: manager} do
      # Default graph
      insert_test_quads(db, manager, 0, 100)
      # Named graph
      insert_test_quads(db, manager, 100, 200)

      assert {:ok, summary} = Statistics.all_graphs_summary(db, include_default: false)
      # Should only count the named graph
      assert summary.total_quads == 200
      assert summary.graph_count == 1
    end

    test "handles empty database", %{db: db} do
      assert {:ok, summary} = Statistics.all_graphs_summary(db)
      # Empty database - default graph exists but has no quads
      assert summary.total_quads == 0
      # No graphs with quads
      assert summary.graph_count == 0
    end
  end

  # ===========================================================================
  # build_per_graph_histograms/2
  # ===========================================================================

  describe "build_per_graph_histograms/2" do
    test "builds histograms for all graphs", %{db: db, manager: manager} do
      insert_test_quads(db, manager, 100, 30)
      insert_test_quads(db, manager, 200, 20)

      assert {:ok, histograms} = Statistics.build_per_graph_histograms(db)
      assert is_map(histograms)
      assert map_size(histograms) >= 2
      assert Map.has_key?(histograms, 100)
      assert Map.has_key?(histograms, 200)
    end

    test "includes default graph by default", %{db: db, manager: manager} do
      insert_test_quads(db, manager, 0, 10)
      insert_test_quads(db, manager, 100, 20)

      assert {:ok, histograms} = Statistics.build_per_graph_histograms(db)
      assert Map.has_key?(histograms, 0)
      assert Map.has_key?(histograms, 100)
    end

    test "can exclude default graph", %{db: db, manager: manager} do
      insert_test_quads(db, manager, 0, 10)
      insert_test_quads(db, manager, 100, 20)

      assert {:ok, histograms} = Statistics.build_per_graph_histograms(db, include_default: false)
      refute Map.has_key?(histograms, 0)
      assert Map.has_key?(histograms, 100)
    end
  end

  # ===========================================================================
  # graph_statistics/2 (existing function)
  # ===========================================================================

  describe "graph_statistics/2" do
    test "returns statistics map for existing graph", %{db: db, manager: manager} do
      graph_id = 100
      insert_test_quads(db, manager, graph_id, 25)

      assert {:ok, stats} = Statistics.graph_statistics(db, graph_id)
      assert stats.quad_count == 25
      assert stats.distinct_subjects == 25
      assert stats.distinct_predicates == 1
      assert is_map(stats.predicate_counts)
    end

    test "returns statistics for default graph", %{db: db, manager: manager} do
      insert_test_quads(db, manager, 0, 15)

      assert {:ok, stats} = Statistics.graph_statistics(db, 0)
      assert stats.quad_count == 15
    end
  end
end
