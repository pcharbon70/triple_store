defmodule TripleStore.DatasetOperationsTest do
  @moduledoc """
  Tests for Section 2.4: Dataset Operations.

  Verifies that:
  - list_graphs returns all named graphs
  - graph_exists? correctly reports graph existence
  - delete_graph removes all quads from a graph
  - copy_graph copies quads between graphs
  - graph_quad_count returns accurate counts
  - graphs_summary returns per-graph statistics
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/triple_store_dataset_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  # Insert test quads with term IDs
  defp insert_test_quads(db, manager) do
    # Create test terms
    s1 = RDF.iri("http://example.org/s1")
    s2 = RDF.iri("http://example.org/s2")
    p = RDF.iri("http://example.org/p")
    o1 = RDF.literal("o1")
    o2 = RDF.literal("o2")
    o3 = RDF.literal("o3")

    g1 = RDF.iri("http://example.org/g1")
    g2 = RDF.iri("http://example.org/g2")

    {:ok, s1_id} = Manager.get_or_create_id(manager, s1)
    {:ok, s2_id} = Manager.get_or_create_id(manager, s2)
    {:ok, p_id} = Manager.get_or_create_id(manager, p)
    {:ok, o1_id} = Manager.get_or_create_id(manager, o1)
    {:ok, o2_id} = Manager.get_or_create_id(manager, o2)
    {:ok, o3_id} = Manager.get_or_create_id(manager, o3)

    {:ok, g1_id} = Manager.get_or_create_id(manager, g1)
    {:ok, g2_id} = Manager.get_or_create_id(manager, g2)

    # Insert quads in default graph
    :ok = QuadOperations.insert_quad(db, {s1_id, p_id, o1_id, 0})
    :ok = QuadOperations.insert_quad(db, {s1_id, p_id, o2_id, 0})

    # Insert quads in g1
    :ok = QuadOperations.insert_quad(db, {s1_id, p_id, o1_id, g1_id})
    :ok = QuadOperations.insert_quad(db, {s1_id, p_id, o2_id, g1_id})
    :ok = QuadOperations.insert_quad(db, {s1_id, p_id, o1_id, g1_id})  # duplicate, idempotent

    # Insert quads in g2 (different quad so copy adds new quads)
    :ok = QuadOperations.insert_quad(db, {s2_id, p_id, o3_id, g2_id})

    %{
      g1: g1,
      g1_id: g1_id,
      g2: g2,
      g2_id: g2_id,
      s2_id: s2_id,
      o3_id: o3_id
    }
  end

  # ===========================================================================
  # list_graphs Tests
  # ===========================================================================

  describe "list_graphs/2" do
    test "returns all named graphs", %{db: db, manager: manager} do
      insert_test_quads(db, manager)

      {:ok, graphs} = QuadOperations.list_graphs(db)

      # Should have 2 named graphs (g1 and g2)
      assert length(graphs) == 2

      # Check that the graph IRIs are in the list
      graph_values = Enum.map(graphs, fn %RDF.IRI{value: v} -> v end)
      assert "http://example.org/g1" in graph_values
      assert "http://example.org/g2" in graph_values
    end

    test "excludes default graph by default", %{db: db, manager: manager} do
      insert_test_quads(db, manager)

      {:ok, graphs} = QuadOperations.list_graphs(db)

      # Should not include :default
      refute :default in graphs
      refute Enum.any?(graphs, &(&1 == :default))
    end

    test "includes default graph when option is set", %{db: db, manager: manager} do
      insert_test_quads(db, manager)

      {:ok, graphs} = QuadOperations.list_graphs(db, include_default: true)

      # Should include :default
      assert :default in graphs
    end

    test "returns empty list when no graphs exist", %{db: db} do
      {:ok, graphs} = QuadOperations.list_graphs(db)

      assert graphs == []
    end
  end

  # ===========================================================================
  # graph_exists? Tests
  # ===========================================================================

  describe "graph_exists?/3" do
    test "returns true for existing named graph", %{db: db, manager: manager} do
      %{g1: g1} = insert_test_quads(db, manager)

      assert QuadOperations.graph_exists?(db, manager, g1) == true
    end

    test "returns false for non-existent graph", %{db: db, manager: manager} do
      g = RDF.iri("http://example.org/nonexistent")

      assert QuadOperations.graph_exists?(db, manager, g) == false
    end

    test "returns true for graph with multiple quads", %{db: db, manager: manager} do
      %{g1: g1} = insert_test_quads(db, manager)

      assert QuadOperations.graph_exists?(db, manager, g1) == true
    end
  end

  # ===========================================================================
  # default_graph_exists? Tests
  # ===========================================================================

  describe "default_graph_exists?/1" do
    test "returns true when default graph has quads", %{db: db, manager: manager} do
      insert_test_quads(db, manager)

      assert QuadOperations.default_graph_exists?(db) == true
    end

    test "returns false when default graph is empty", %{db: db} do
      assert QuadOperations.default_graph_exists?(db) == false
    end
  end

  # ===========================================================================
  # delete_graph Tests
  # ===========================================================================

  describe "delete_graph/3" do
    test "deletes all quads from named graph", %{db: db, manager: manager} do
      %{g1: g1} = insert_test_quads(db, manager)

      # Verify graph exists
      assert QuadOperations.graph_exists?(db, manager, g1) == true

      # Delete graph
      {:ok, count} = QuadOperations.delete_graph(db, manager, g1)

      # Should have deleted 2 quads (duplicate was idempotent)
      assert count == 2

      # Verify graph no longer exists
      assert QuadOperations.graph_exists?(db, manager, g1) == false
    end

    test "deletes default graph when :default is passed", %{db: db, manager: manager} do
      insert_test_quads(db, manager)

      # Verify default graph exists
      assert QuadOperations.default_graph_exists?(db) == true

      # Delete default graph
      {:ok, count} = QuadOperations.delete_graph(db, manager, :default)

      # Should have deleted 2 quads
      assert count == 2

      # Verify default graph no longer exists
      assert QuadOperations.default_graph_exists?(db) == false

      # Named graphs should still exist
      assert QuadOperations.graph_exists?(db, manager, RDF.iri("http://example.org/g1")) == true
    end

    test "returns ok with 0 for non-existent graph", %{db: db, manager: manager} do
      g = RDF.iri("http://example.org/nonexistent")

      {:ok, count} = QuadOperations.delete_graph(db, manager, g)

      assert count == 0
    end

    test "deleting graph removes quads from all indices", %{db: db, manager: manager} do
      %{g1: g1, g1_id: g1_id} = insert_test_quads(db, manager)

      # Delete graph
      {:ok, _count} = QuadOperations.delete_graph(db, manager, g1)

      # Verify no quads can be found in any index
      quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g1_id})
      assert length(quads) == 0
    end
  end

  # ===========================================================================
  # copy_graph Tests
  # ===========================================================================

  describe "copy_graph/4" do
    test "copies quads from source to target graph", %{db: db, manager: manager} do
      %{g1: g1, g2: g2} = insert_test_quads(db, manager)

      # Copy g1 to g2 (g2 has 1 different quad)
      {:ok, count} = QuadOperations.copy_graph(db, manager, g1, g2)

      # Should have copied 2 quads
      assert count == 2

      # Verify g2 now has 3 quads (1 original + 2 copied)
      {:ok, g2_count} = QuadOperations.graph_quad_count(db, manager, g2)
      assert g2_count == 3
    end

    test "copy to new graph creates target", %{db: db, manager: manager} do
      %{g1: g1} = insert_test_quads(db, manager)

      g3 = RDF.iri("http://example.org/g3")

      # Copy g1 to new graph g3
      {:ok, count} = QuadOperations.copy_graph(db, manager, g1, g3)

      # Should have copied 2 quads
      assert count == 2

      # Verify g3 exists
      assert QuadOperations.graph_exists?(db, manager, g3) == true
    end

    test "copy with :merge merges into existing target", %{db: db, manager: manager} do
      %{g1: g1, g2: g2} = insert_test_quads(db, manager)

      # Copy with :merge (default)
      {:ok, count} = QuadOperations.copy_graph(db, manager, g1, g2, on_conflict: :merge)

      assert count == 2

      # Verify g2 has original + copied quads (1 + 2 = 3)
      {:ok, new_g2_count} = QuadOperations.graph_quad_count(db, manager, g2)
      assert new_g2_count == 3
    end

    test "copy with :replace clears target first", %{db: db, manager: manager} do
      %{g1: g1, g2: g2, g1_id: g1_id} = insert_test_quads(db, manager)

      # Get g1 count (should be 2, duplicate was idempotent)
      {:ok, g1_count} = QuadOperations.graph_quad_count(db, manager, g1)

      # Copy with :replace
      {:ok, count} = QuadOperations.copy_graph(db, manager, g1, g2, on_conflict: :replace)

      # Should have copied exactly g1's quads
      assert count == g1_count

      # Verify g2 has exactly g1's quads (no original)
      {:ok, g2_count} = QuadOperations.graph_quad_count(db, manager, g2)
      assert g2_count == g1_count

      # Verify the actual quads match
      g1_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g1_id})
      {:ok, g2_id} = Manager.get_or_create_id(manager, g2)
      g2_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g2_id})
      assert length(g1_quads) == length(g2_quads)
    end

    test "copy with :error fails if target exists", %{db: db, manager: manager} do
      %{g1: g1, g2: g2} = insert_test_quads(db, manager)

      # Copy with :error
      result = QuadOperations.copy_graph(db, manager, g1, g2, on_conflict: :error)

      assert {:error, :graph_exists} = result
    end

    test "copy with :error succeeds if target doesn't exist", %{db: db, manager: manager} do
      %{g1: g1} = insert_test_quads(db, manager)

      g3 = RDF.iri("http://example.org/g3")

      # Copy with :error to non-existent target
      {:ok, count} = QuadOperations.copy_graph(db, manager, g1, g3, on_conflict: :error)

      assert count == 2
    end

    test "copy from :default works", %{db: db, manager: manager} do
      %{g1: g1} = insert_test_quads(db, manager)

      # Get original g1 count
      {:ok, original_g1_count} = QuadOperations.graph_quad_count(db, manager, g1)

      # Copy from default to g1
      # Note: default and g1 have overlapping quads (same s1, p, o1 and o2)
      # So only the overlapping quads will be "copied" (but insert is idempotent)
      {:ok, count} = QuadOperations.copy_graph(db, manager, :default, g1)

      # Both quads overlap, so count is 2 but no new quads added
      assert count == 2

      # Verify g1 still has 2 quads (same as before, due to overlap)
      {:ok, new_g1_count} = QuadOperations.graph_quad_count(db, manager, g1)
      assert new_g1_count == original_g1_count
    end

    test "copy to :default works", %{db: db, manager: manager} do
      %{g1: g1} = insert_test_quads(db, manager)

      # Get original default count
      {:ok, original_default_count} = QuadOperations.graph_quad_count(db, manager, :default)

      # Copy g1 to default
      # Note: g1 and default have overlapping quads (same s1, p, o1 and o2)
      {:ok, count} = QuadOperations.copy_graph(db, manager, g1, :default)

      # Both quads overlap, so count is 2 but no new quads added
      assert count == 2

      # Verify default still has 2 quads (same as before, due to overlap)
      {:ok, new_default_count} = QuadOperations.graph_quad_count(db, manager, :default)
      assert new_default_count == original_default_count
    end

    test "copy same graph returns 0", %{db: db, manager: manager} do
      %{g1: g1} = insert_test_quads(db, manager)

      # Copy g1 to itself
      {:ok, count} = QuadOperations.copy_graph(db, manager, g1, g1)

      assert count == 0
    end
  end

  # ===========================================================================
  # graph_quad_count Tests
  # ===========================================================================

  describe "graph_quad_count/3" do
    test "returns accurate count for named graph", %{db: db, manager: manager} do
      %{g1: g1} = insert_test_quads(db, manager)

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, g1)

      # g1 has 2 quads stored (duplicate was idempotent)
      assert count == 2
    end

    test "returns accurate count for default graph", %{db: db, manager: manager} do
      insert_test_quads(db, manager)

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, :default)

      # Default graph has 2 quads
      assert count == 2
    end

    test "returns 0 for non-existent graph", %{db: db, manager: manager} do
      g = RDF.iri("http://example.org/nonexistent")

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, g)

      assert count == 0
    end

    test "returns 0 for empty graph", %{db: db, manager: manager} do
      g = RDF.iri("http://example.org/empty")

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, g)

      assert count == 0
    end
  end

  # ===========================================================================
  # graphs_summary Tests
  # ===========================================================================

  describe "graphs_summary/2" do
    test "returns summary for all graphs including default", %{db: db, manager: manager} do
      %{g1: g1, g2: g2} = insert_test_quads(db, manager)

      {:ok, summary} = QuadOperations.graphs_summary(db)

      # Should have 3 entries (default, g1, g2)
      assert map_size(summary) == 3

      # Check default graph count
      assert Map.get(summary, :default) == 2

      # Check g1 count (2, duplicate was idempotent)
      assert Map.get(summary, g1) == 2

      # Check g2 count
      assert Map.get(summary, g2) == 1
    end

    test "excludes default graph when option is false", %{db: db, manager: manager} do
      %{g1: _g1, g2: _g2} = insert_test_quads(db, manager)

      {:ok, summary} = QuadOperations.graphs_summary(db, include_default: false)

      # Should have 2 entries (g1, g2)
      assert map_size(summary) == 2

      # Should not include default
      refute Map.has_key?(summary, :default)
    end

    test "returns empty map when no graphs exist", %{db: db} do
      {:ok, summary} = QuadOperations.graphs_summary(db)

      assert summary == %{}
    end

    test "returns correct counts after graph operations", %{db: db, manager: manager} do
      %{g1: g1, g2: g2} = insert_test_quads(db, manager)

      # Delete g1
      {:ok, _} = QuadOperations.delete_graph(db, manager, g1)

      {:ok, summary} = QuadOperations.graphs_summary(db)

      # Should have 2 entries (default, g2)
      assert map_size(summary) == 2

      # g1 should not be in summary
      refute Map.has_key?(summary, g1)

      # g2 should still be there
      assert Map.get(summary, g2) == 1
    end
  end

  # ===========================================================================
  # Integration Tests
  # ===========================================================================

  describe "integration" do
    test "complete graph lifecycle", %{db: db, manager: manager} do
      g1 = RDF.iri("http://example.org/g1")
      g2 = RDF.iri("http://example.org/g2")

      # Initially no graphs
      {:ok, graphs} = QuadOperations.list_graphs(db)
      assert length(graphs) == 0

      # Insert quads into g1
      s = RDF.iri("http://example.org/s")
      p = RDF.iri("http://example.org/p")
      o = RDF.literal("o")

      {:ok, s_id} = Manager.get_or_create_id(manager, s)
      {:ok, p_id} = Manager.get_or_create_id(manager, p)
      {:ok, o_id} = Manager.get_or_create_id(manager, o)
      {:ok, g1_id} = Manager.get_or_create_id(manager, g1)

      :ok = QuadOperations.insert_quad(db, {s_id, p_id, o_id, g1_id})

      # Now g1 should exist
      assert QuadOperations.graph_exists?(db, manager, g1) == true
      {:ok, graphs} = QuadOperations.list_graphs(db)
      assert length(graphs) == 1

      # Copy to g2
      {:ok, _} = QuadOperations.copy_graph(db, manager, g1, g2)

      # Both should exist
      assert QuadOperations.graph_exists?(db, manager, g1) == true
      assert QuadOperations.graph_exists?(db, manager, g2) == true
      {:ok, graphs} = QuadOperations.list_graphs(db)
      assert length(graphs) == 2

      # Delete g1
      {:ok, _} = QuadOperations.delete_graph(db, manager, g1)

      # Only g2 should remain
      assert QuadOperations.graph_exists?(db, manager, g1) == false
      assert QuadOperations.graph_exists?(db, manager, g2) == true
      {:ok, graphs} = QuadOperations.list_graphs(db)
      assert length(graphs) == 1
    end
  end
end
