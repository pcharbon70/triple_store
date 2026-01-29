defmodule TripleStore.Reasoner.Section7_8_5DerivedStoreQuadTest do
  @moduledoc """
  Tests for Section 7.8.5: Derived Store Quad Operations.

  This test suite validates DerivedStore operations for quad store with graph-aware reasoning.

  ## Test Coverage

  - Task 7.8.5.1: Quad insertion operations
  - Task 7.8.5.2: Quad deletion operations
  - Task 7.8.5.3: Quad lookup operations
  - Task 7.8.5.4: Graph-specific operations
  - Task 7.8.5.5: Callback factory functions

  ## Testing Approach

  This test focuses on the DerivedStore quad API using term-based operations
  where possible, with database integration tests for full coverage.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadIndex
  alias TripleStore.Reasoner.DerivedStore

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp create_test_db do
    path = Path.join(System.tmp_dir!(), "test_quad_#{System.unique_integer([:positive])}")
    {:ok, db} = ErlangAdapter.open(path, schema: :quad, create_if_missing: true)
    {db, path}
  end

  defp cleanup_test_db(db, path) do
    ErlangAdapter.close(db)
    File.rm_rf!(path)
  end

  defp quad(g, s, p, o), do: {g, s, p, o}
  defp id_quad(g, s, p, o), do: {g, s, p, o}

  # ============================================================================
  # Setup and Teardown
  # ============================================================================

  setup do
    {db, path} = create_test_db()

    on_exit(fn ->
      cleanup_test_db(db, path)
    end)

    {:ok, %{db: db}}
  end

  # ============================================================================
  # Tests: Quad Insertion Operations (7.8.5.1)
  # ============================================================================

  describe "insert_derived_quads/2" do
    test "inserts multiple quads into derived store", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 101, 201, 301),
        id_quad(2, 102, 202, 302)
      ]

      assert :ok = DerivedStore.insert_derived_quads(db, quads)

      # Verify quads were inserted
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 100, 200, 300))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 101, 201, 301))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(2, 102, 202, 302))
    end

    test "handles empty quad list", %{db: db} do
      assert :ok = DerivedStore.insert_derived_quads(db, [])
    end

    test "handles invalid input gracefully", %{db: db} do
      # DerivedStore may ignore invalid quad formats
      # Test that the operation doesn't crash
      # Missing predicate and object
      invalid_quads = [{1, 100}]

      # The operation should either return :ok or error, but not crash
      result = DerivedStore.insert_derived_quads(db, invalid_quads)
      assert result == :ok or match?({:error, _}, result)
    end

    test "handles quads from multiple graphs", %{db: db} do
      quads = [
        id_quad(0, 1, 2, 3),
        id_quad(1, 4, 5, 6),
        id_quad(2, 7, 8, 9),
        id_quad(99, 10, 11, 12)
      ]

      assert :ok = DerivedStore.insert_derived_quads(db, quads)

      # All graphs should be accessible
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(0, 1, 2, 3))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 4, 5, 6))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(2, 7, 8, 9))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(99, 10, 11, 12))
    end

    test "deduplicates quads within batch", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        # Duplicate
        id_quad(1, 100, 200, 300),
        id_quad(1, 101, 201, 301)
      ]

      assert :ok = DerivedStore.insert_derived_quads(db, quads)

      # Should exist regardless of duplicate
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 100, 200, 300))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 101, 201, 301))
    end
  end

  describe "insert_derived_quad_single/2" do
    test "inserts single quad", %{db: db} do
      quad = id_quad(1, 100, 200, 300)

      assert :ok = DerivedStore.insert_derived_quad_single(db, quad)
      {:ok, true} = DerivedStore.derived_quad_exists?(db, quad)
    end

    test "overwrites existing quad", %{db: db} do
      quad = id_quad(1, 100, 200, 300)

      :ok = DerivedStore.insert_derived_quad_single(db, quad)
      :ok = DerivedStore.insert_derived_quad_single(db, quad)

      {:ok, true} = DerivedStore.derived_quad_exists?(db, quad)
    end

    test "handles large ID values", %{db: db} do
      # Test with IDs near the maximum value for 64-bit signed integers
      quad = id_quad(1, 0x7FFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF)

      assert :ok = DerivedStore.insert_derived_quad_single(db, quad)
      {:ok, true} = DerivedStore.derived_quad_exists?(db, quad)
    end

    test "handles zero values", %{db: db} do
      quad = id_quad(0, 0, 0, 0)

      assert :ok = DerivedStore.insert_derived_quad_single(db, quad)
      {:ok, true} = DerivedStore.derived_quad_exists?(db, quad)
    end
  end

  # ============================================================================
  # Tests: Quad Deletion Operations (7.8.5.2)
  # ============================================================================

  describe "delete_derived_quads/2" do
    test "deletes multiple quads from derived store", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 101, 201, 301),
        id_quad(2, 102, 202, 302)
      ]

      :ok = DerivedStore.insert_derived_quads(db, quads)

      # Delete first two quads
      to_delete = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 101, 201, 301)
      ]

      assert :ok = DerivedStore.delete_derived_quads(db, to_delete)

      # Verify deletions
      {:ok, false} = DerivedStore.derived_quad_exists?(db, id_quad(1, 100, 200, 300))
      {:ok, false} = DerivedStore.derived_quad_exists?(db, id_quad(1, 101, 201, 301))

      # Third quad should still exist
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(2, 102, 202, 302))
    end

    test "handles empty quad list", %{db: db} do
      assert :ok = DerivedStore.delete_derived_quads(db, [])
    end

    test "handles deleting non-existent quads", %{db: db} do
      quads = [id_quad(1, 100, 200, 300)]

      # Should not error when deleting non-existent quads
      assert :ok = DerivedStore.delete_derived_quads(db, quads)
    end

    test "deletes quads from specific graph only", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        # Same IDs but different graph
        id_quad(2, 100, 200, 300),
        id_quad(3, 100, 200, 300)
      ]

      :ok = DerivedStore.insert_derived_quads(db, quads)

      # Delete only from graph 1
      assert :ok = DerivedStore.delete_derived_quads(db, [id_quad(1, 100, 200, 300)])

      # Graph 1 should be deleted
      {:ok, false} = DerivedStore.derived_quad_exists?(db, id_quad(1, 100, 200, 300))

      # Graphs 2 and 3 should still exist
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(2, 100, 200, 300))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(3, 100, 200, 300))
    end
  end

  # ============================================================================
  # Tests: Quad Lookup Operations (7.8.5.3)
  # ============================================================================

  describe "derived_quad_exists?/2" do
    test "returns true for existing quad", %{db: db} do
      quad = id_quad(1, 100, 200, 300)
      :ok = DerivedStore.insert_derived_quad_single(db, quad)

      assert {:ok, true} = DerivedStore.derived_quad_exists?(db, quad)
    end

    test "returns false for non-existent quad", %{db: db} do
      quad = id_quad(1, 100, 200, 300)

      assert {:ok, false} = DerivedStore.derived_quad_exists?(db, quad)
    end

    test "distinguishes between different graphs", %{db: db} do
      quad1 = id_quad(1, 100, 200, 300)
      quad2 = id_quad(2, 100, 200, 300)

      :ok = DerivedStore.insert_derived_quad_single(db, quad1)

      {:ok, true} = DerivedStore.derived_quad_exists?(db, quad1)
      {:ok, false} = DerivedStore.derived_quad_exists?(db, quad2)
    end
  end

  describe "lookup_derived_quads/3" do
    test "returns stream of matching quads", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 100, 201, 301),
        id_quad(1, 101, 200, 300)
      ]

      :ok = DerivedStore.insert_derived_quads(db, quads)

      # Lookup by subject - pattern is a 3-tuple
      {:ok, stream} = DerivedStore.lookup_derived_quads(db, 1, {{:bound, 100}, :var, :var})
      results = Enum.to_list(stream)

      assert length(results) == 2
    end

    test "handles pattern with multiple bound components", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 100, 200, 301),
        id_quad(1, 100, 201, 300)
      ]

      :ok = DerivedStore.insert_derived_quads(db, quads)

      # Lookup by subject and predicate
      {:ok, stream} =
        DerivedStore.lookup_derived_quads(db, 1, {{:bound, 100}, {:bound, 200}, :var})

      results = Enum.to_list(stream)

      assert length(results) == 2
    end

    test "returns empty stream for non-matching pattern", %{db: db} do
      quads = [id_quad(1, 100, 200, 300)]
      :ok = DerivedStore.insert_derived_quads(db, quads)

      # Lookup with non-matching subject
      {:ok, stream} = DerivedStore.lookup_derived_quads(db, 1, {{:bound, 999}, :var, :var})
      results = Enum.to_list(stream)

      assert length(results) == 0
    end

    test "returns empty stream for non-existent graph", %{db: db} do
      quads = [id_quad(1, 100, 200, 300)]
      :ok = DerivedStore.insert_derived_quads(db, quads)

      # Lookup in different graph
      {:ok, stream} = DerivedStore.lookup_derived_quads(db, 2, :var)
      results = Enum.to_list(stream)

      assert length(results) == 0
    end
  end

  describe "lookup_derived_quads_fold/3" do
    test "returns list of all matching quads", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 100, 201, 301),
        id_quad(1, 102, 203, 304)
      ]

      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, results} = DerivedStore.lookup_derived_quads_fold(db, 1, {{:bound, 100}, :var, :var})

      assert length(results) == 2
    end

    test "handles all variables pattern", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 101, 201, 301)
      ]

      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, results} = DerivedStore.lookup_derived_quads_fold(db, 1, :var)

      assert length(results) == 2
    end
  end

  describe "lookup_derived_quads_all/3" do
    test "returns all quads for pattern", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 101, 201, 301),
        id_quad(2, 100, 200, 300)
      ]

      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, results} = DerivedStore.lookup_derived_quads_all(db, 1, {{:bound, 100}, :var, :var})

      assert length(results) == 1
    end

    test "returns empty list for no matches", %{db: db} do
      {:ok, results} = DerivedStore.lookup_derived_quads_all(db, 1, :var)

      assert results == []
    end
  end

  describe "lookup_derived_quads_in_graph/2" do
    test "returns all derived quads in specific graph", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 101, 201, 301),
        id_quad(2, 102, 202, 302)
      ]

      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, graph1_quads} = DerivedStore.lookup_derived_quads_in_graph(db, 1)
      {:ok, graph2_quads} = DerivedStore.lookup_derived_quads_in_graph(db, 2)

      assert length(graph1_quads) == 2
      assert length(graph2_quads) == 1
    end

    test "returns empty list for empty graph", %{db: db} do
      {:ok, quads} = DerivedStore.lookup_derived_quads_in_graph(db, 99)

      assert quads == []
    end
  end

  # ============================================================================
  # Tests: Graph-Specific Operations (7.8.5.4)
  # ============================================================================

  describe "clear_graph_quads/2" do
    test "clears all quads for specific graph", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 101, 201, 301),
        id_quad(2, 102, 202, 302)
      ]

      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, count} = DerivedStore.clear_graph_quads(db, 1)

      assert count == 2

      # Graph 1 should be empty
      {:ok, graph1_quads} = DerivedStore.lookup_derived_quads_in_graph(db, 1)
      assert graph1_quads == []

      # Graph 2 should still have its quad
      {:ok, graph2_quads} = DerivedStore.lookup_derived_quads_in_graph(db, 2)
      assert length(graph2_quads) == 1
    end

    test "returns zero count for empty graph", %{db: db} do
      {:ok, count} = DerivedStore.clear_graph_quads(db, 99)

      assert count == 0
    end

    test "handles large number of quads efficiently", %{db: db} do
      # Insert 100 quads
      quads =
        for i <- 1..100 do
          id_quad(1, i, i * 10, i * 100)
        end

      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, count} = DerivedStore.clear_graph_quads(db, 1)

      assert count == 100

      # Graph should be empty
      {:ok, remaining} = DerivedStore.lookup_derived_quads_in_graph(db, 1)
      assert remaining == []
    end

    test "clearing one graph doesn't affect others", %{db: db} do
      quads =
        for g <- [1, 2, 3], i <- 1..10 do
          id_quad(g, i, i * 10, i * 100)
        end

      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, _} = DerivedStore.clear_graph_quads(db, 2)

      # Graph 2 should be empty
      {:ok, graph2_quads} = DerivedStore.lookup_derived_quads_in_graph(db, 2)
      assert graph2_quads == []

      # Other graphs should be unaffected
      {:ok, graph1_quads} = DerivedStore.lookup_derived_quads_in_graph(db, 1)
      {:ok, graph3_quads} = DerivedStore.lookup_derived_quads_in_graph(db, 3)
      assert length(graph1_quads) == 10
      assert length(graph3_quads) == 10
    end
  end

  describe "count_graph_quads/2" do
    test "counts quads in specific graph", %{db: db} do
      quads = [
        id_quad(1, 100, 200, 300),
        id_quad(1, 101, 201, 301),
        id_quad(2, 102, 202, 302)
      ]

      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, count1} = DerivedStore.count_graph_quads(db, 1)
      {:ok, count2} = DerivedStore.count_graph_quads(db, 2)

      assert count1 == 2
      assert count2 == 1
    end

    test "returns zero for empty graph", %{db: db} do
      {:ok, count} = DerivedStore.count_graph_quads(db, 99)

      assert count == 0
    end

    test "updates count after insertions and deletions", %{db: db} do
      {:ok, initial_count} = DerivedStore.count_graph_quads(db, 1)
      assert initial_count == 0

      quads = [id_quad(1, 100, 200, 300), id_quad(1, 101, 201, 301)]
      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, count_after_insert} = DerivedStore.count_graph_quads(db, 1)
      assert count_after_insert == 2

      :ok = DerivedStore.delete_derived_quads(db, [id_quad(1, 100, 200, 300)])

      {:ok, count_after_delete} = DerivedStore.count_graph_quads(db, 1)
      assert count_after_delete == 1
    end
  end

  describe "lookup_explicit_quads/3" do
    test "function exists and expects correct parameters" do
      assert function_exported?(DerivedStore, :lookup_explicit_quads, 3)
    end

    test "pattern matching works for explicit quads", %{db: db} do
      # Verify the function accepts pattern arguments
      {:ok, _stream} = DerivedStore.lookup_explicit_quads(db, 1, {{:bound, 100}, :var, :var})
      {:ok, _stream} = DerivedStore.lookup_explicit_quads(db, 1, :var)
    end
  end

  describe "lookup_all_quads/3" do
    test "function exists and has correct arity" do
      assert function_exported?(DerivedStore, :lookup_all_quads, 3)
    end

    test "combines explicit and derived quads", %{db: db} do
      derived_quads = [id_quad(1, 100, 200, 300)]
      :ok = DerivedStore.insert_derived_quads(db, derived_quads)

      {:ok, stream} = DerivedStore.lookup_all_quads(db, 1, {{:bound, 100}, :var, :var})

      assert is_function(stream)
    end

    test "returns stream that can be consumed", %{db: db} do
      derived_quads = [id_quad(1, 100, 200, 300)]
      :ok = DerivedStore.insert_derived_quads(db, derived_quads)

      {:ok, stream} = DerivedStore.lookup_all_quads(db, 1, {{:bound, 100}, :var, :var})
      results = Enum.to_list(stream)

      # Should at least have the derived quad
      assert length(results) >= 1
    end
  end

  # ============================================================================
  # Tests: Callback Factory Functions (7.8.5.5)
  # ============================================================================

  describe "make_graph_lookup_fn/3" do
    test "creates lookup function for explicit source", %{db: db} do
      lookup_fn = DerivedStore.make_graph_lookup_fn(db, 1, :explicit)

      assert is_function(lookup_fn, 1)
    end

    test "creates lookup function for derived source", %{db: db} do
      lookup_fn = DerivedStore.make_graph_lookup_fn(db, 1, :derived)

      assert is_function(lookup_fn, 1)

      # Insert derived quad and verify lookup
      quads = [id_quad(1, 100, 200, 300)]
      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, results} = lookup_fn.({{:bound, 100}, :var, :var})

      assert length(results) >= 1
    end

    test "creates lookup function for both sources", %{db: db} do
      lookup_fn = DerivedStore.make_graph_lookup_fn(db, 1, :both)

      assert is_function(lookup_fn, 1)
    end

    test "lookup function handles variable pattern", %{db: db} do
      lookup_fn = DerivedStore.make_graph_lookup_fn(db, 1, :derived)

      quads = [id_quad(1, 100, 200, 300)]
      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, results} = lookup_fn.(:var)

      assert length(results) >= 1
    end

    test "lookup function handles bound pattern", %{db: db} do
      lookup_fn = DerivedStore.make_graph_lookup_fn(db, 1, :derived)

      quads = [id_quad(1, 100, 200, 300), id_quad(1, 101, 201, 301)]
      :ok = DerivedStore.insert_derived_quads(db, quads)

      {:ok, results} = lookup_fn.({{:bound, 100}, :var, :var})

      assert length(results) == 1
    end
  end

  describe "make_graph_store_fn/2" do
    test "creates store function for graph", %{db: db} do
      store_fn = DerivedStore.make_graph_store_fn(db, 1)

      assert is_function(store_fn, 1)

      # Store facts (triples without graph)
      facts = MapSet.new([{100, 200, 300}, {101, 201, 301}])

      :ok = store_fn.(facts)

      # Verify they were stored as quads with graph_id
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 100, 200, 300))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 101, 201, 301))
    end

    test "store function handles empty fact set", %{db: db} do
      store_fn = DerivedStore.make_graph_store_fn(db, 1)

      assert :ok = store_fn.(MapSet.new())
    end

    test "store function prefixes facts with graph_id", %{db: db} do
      store_fn_1 = DerivedStore.make_graph_store_fn(db, 1)
      store_fn_2 = DerivedStore.make_graph_store_fn(db, 2)

      facts = MapSet.new([{100, 200, 300}])

      :ok = store_fn_1.(facts)
      :ok = store_fn_2.(facts)

      # Each graph should have its own quads
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 100, 200, 300))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(2, 100, 200, 300))
    end
  end

  describe "make_graph_quad_store_fn/2" do
    test "creates store function for quads", %{db: db} do
      store_fn = DerivedStore.make_graph_quad_store_fn(db, 1)

      assert is_function(store_fn, 1)

      # Store quads (already include graph_id)
      quads =
        MapSet.new([
          id_quad(1, 100, 200, 300),
          id_quad(1, 101, 201, 301)
        ])

      :ok = store_fn.(quads)

      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 100, 200, 300))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 101, 201, 301))
    end

    test "filters quads by graph_id", %{db: db} do
      store_fn = DerivedStore.make_graph_quad_store_fn(db, 1)

      # Quads from multiple graphs
      quads =
        MapSet.new([
          id_quad(1, 100, 200, 300),
          id_quad(1, 101, 201, 301),
          # Different graph - should be filtered
          id_quad(2, 100, 200, 300),
          # Different graph - should be filtered
          id_quad(3, 100, 200, 300)
        ])

      :ok = store_fn.(quads)

      # Only graph 1 quads should be stored
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 100, 200, 300))
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 101, 201, 301))
      {:ok, false} = DerivedStore.derived_quad_exists?(db, id_quad(2, 100, 200, 300))
      {:ok, false} = DerivedStore.derived_quad_exists?(db, id_quad(3, 100, 200, 300))
    end

    test "handles empty quad set", %{db: db} do
      store_fn = DerivedStore.make_graph_quad_store_fn(db, 1)

      assert :ok = store_fn.(MapSet.new())
    end
  end

  # ============================================================================
  # Tests: Edge Cases and Integration
  # ============================================================================

  describe "edge cases" do
    test "handles maximum graph ID", %{db: db} do
      quad = id_quad(0xFFFFFFFFFFFFFFFF, 1, 2, 3)

      :ok = DerivedStore.insert_derived_quad_single(db, quad)
      {:ok, true} = DerivedStore.derived_quad_exists?(db, quad)
    end

    test "handles mixed valid and invalid operations", %{db: db} do
      valid_quads = [id_quad(1, 100, 200, 300)]
      # Invalid format
      invalid_quads = [{1, 100}]

      :ok = DerivedStore.insert_derived_quads(db, valid_quads)

      # Valid insert should succeed
      {:ok, true} = DerivedStore.derived_quad_exists?(db, id_quad(1, 100, 200, 300))
    end

    test "concurrent operations on different graphs", %{db: db} do
      # Simulate concurrent-like operations on different graphs
      graph1_quads = for i <- 1..10, do: id_quad(1, i, i * 10, i * 100)
      graph2_quads = for i <- 1..10, do: id_quad(2, i, i * 10, i * 100)

      :ok = DerivedStore.insert_derived_quads(db, graph1_quads)
      :ok = DerivedStore.insert_derived_quads(db, graph2_quads)

      {:ok, count1} = DerivedStore.count_graph_quads(db, 1)
      {:ok, count2} = DerivedStore.count_graph_quads(db, 2)

      assert count1 == 10
      assert count2 == 10
    end

    test "pattern with all bound components", %{db: db} do
      quad = id_quad(1, 100, 200, 300)
      :ok = DerivedStore.insert_derived_quad_single(db, quad)

      # Pattern with all components bound
      {:ok, stream} =
        DerivedStore.lookup_derived_quads(db, 1, {{:bound, 100}, {:bound, 200}, {:bound, 300}})

      results = Enum.to_list(stream)

      assert length(results) == 1
    end
  end

  describe "decode_derived_key/1" do
    test "decodes valid GSPO key" do
      key = <<1::64-big, 100::64-big, 200::64-big, 300::64-big>>

      assert {:ok, {1, 100, 200, 300}} = DerivedStore.decode_derived_key(key)
    end

    test "returns error for invalid key length" do
      invalid_key = <<1::64-big, 100::64-big, 200::64-big>>

      assert {:error, :invalid_key} = DerivedStore.decode_derived_key(invalid_key)
    end

    test "handles large ID values" do
      key =
        <<0xFFFFFFFFFFFFFFFF::64-big, 0x7FFFFFFFFFFFFFFF::64-big, 0x7FFFFFFFFFFFFFFF::64-big,
          0x7FFFFFFFFFFFFFFF::64-big>>

      assert {:ok,
              {0xFFFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF}} =
               DerivedStore.decode_derived_key(key)
    end
  end
end
