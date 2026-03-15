defmodule TripleStore.Integration.QuadDeleteTest do
  @moduledoc """
  Integration tests for Section 6.1.3: Quad Delete.

  Tests quad deletion end-to-end:
  - Delete single quad removes from all indices
  - Delete batch quads removes all
  - Delete from default graph
  - Delete from named graph
  - Delete non-existent quad is no-op
  - Delete all quads from graph
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Integration.Helpers
  alias TripleStore.QuadIndex
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/quad_delete_test"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    Helpers.unique_path("quad_delete_test")
  end

  defp cleanup_path(path) do
    Helpers.cleanup_path(path)
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    test_path = unique_path()

    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      cleanup_path(test_path)
    end)

    %{db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # 6.1.3.1: Test delete single quad removes from all indices
  # ===========================================================================

  describe "6.1.3.1 delete single quad removes from all indices" do
    test "deleted quad removed from all four indices", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/g")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      quad = {s_id, p_id, o_id, g_id}

      # Insert quad
      :ok = QuadOperations.insert_quad(db, quad)

      # Verify it exists in all indices
      keys = QuadIndex.encode_quad_keys(s_id, p_id, o_id, g_id)
      assert {:ok, _} = ErlangAdapter.get(db, :gspo, keys.gspo)
      assert {:ok, _} = ErlangAdapter.get(db, :gpos, keys.gpos)
      assert {:ok, _} = ErlangAdapter.get(db, :spog, keys.spog)
      assert {:ok, _} = ErlangAdapter.get(db, :posg, keys.posg)

      # Delete quad
      :ok = QuadOperations.delete_quad(db, quad)

      # Verify removed from all indices
      assert :not_found = ErlangAdapter.get(db, :gspo, keys.gspo)
      assert :not_found = ErlangAdapter.get(db, :gpos, keys.gpos)
      assert :not_found = ErlangAdapter.get(db, :spog, keys.spog)
      assert :not_found = ErlangAdapter.get(db, :posg, keys.posg)

      # Verify exists? returns false
      refute QuadOperations.quad_exists?(db, quad)
    end

    test "deleted quad not findable via any index pattern", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/g")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      quad = {s_id, p_id, o_id, g_id}

      :ok = QuadOperations.insert_quad(db, quad)
      :ok = QuadOperations.delete_quad(db, quad)

      # Try various lookup patterns - lookup_quads returns list directly
      full_results =
        QuadOperations.lookup_quads(
          db,
          {:bound, :bound, :bound, :bound},
          %{s: s_id, p: p_id, o: o_id, g: g_id}
        )

      assert full_results == []

      graph_results =
        QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g_id})

      assert quad not in graph_results

      subject_results =
        QuadOperations.lookup_quads(db, {:bound, :var, :var, :var}, %{s: s_id})

      assert quad not in subject_results
    end
  end

  # ===========================================================================
  # 6.1.3.2: Test delete batch quads removes all
  # ===========================================================================

  describe "6.1.3.2 delete batch quads removes all" do
    test "batch delete removes all specified quads", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/g")

      quads =
        Enum.map(1..5, fn i ->
          subject = RDF.iri("http://example.org/s#{i}")
          predicate = RDF.iri("http://example.org/p")
          object = RDF.literal("O#{i}")

          {:ok, s_id} = Manager.get_or_create_id(manager, subject)
          {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
          {:ok, o_id} = Manager.get_or_create_id(manager, object)
          {:ok, g_id} = Manager.get_or_create_id(manager, graph)

          QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})
          {s_id, p_id, o_id, g_id}
        end)

      # Verify all exist
      {:ok, count_before} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count_before == 5

      # Delete first 3 quads - delete_quads requires opts as 3rd arg
      :ok = QuadOperations.delete_quads(db, Enum.take(quads, 3), [])

      # Verify 2 remain
      {:ok, count_after} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count_after == 2

      # Verify deleted quads don't exist
      Enum.each(Enum.take(quads, 3), fn quad ->
        refute QuadOperations.quad_exists?(db, quad)
      end)

      # Verify remaining quads exist
      Enum.each(Enum.drop(quads, 3), fn quad ->
        assert QuadOperations.quad_exists?(db, quad)
      end)
    end

    test "batch delete removes from all indices", %{db: db, manager: manager} do
      quads =
        Enum.map(1..3, fn i ->
          subject = RDF.iri("http://example.org/s#{i}")
          predicate = RDF.iri("http://example.org/p#{rem(i, 2)}")
          object = RDF.literal("O#{i}")
          graph = RDF.iri("http://example.org/g#{rem(i, 2)}")

          {:ok, s_id} = Manager.get_or_create_id(manager, subject)
          {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
          {:ok, o_id} = Manager.get_or_create_id(manager, object)
          {:ok, g_id} = Manager.get_or_create_id(manager, graph)

          QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})
          {s_id, p_id, o_id, g_id}
        end)

      # Delete all - delete_quads requires opts as 3rd arg
      :ok = QuadOperations.delete_quads(db, quads, [])

      # Verify all removed from indices
      Enum.each(quads, fn {s, p, o, g} ->
        refute QuadOperations.quad_exists?(db, {s, p, o, g})

        keys = QuadIndex.encode_quad_keys(s, p, o, g)
        assert :not_found = ErlangAdapter.get(db, :gspo, keys.gspo)
      end)
    end

    test "empty batch delete is no-op", %{db: db} do
      assert :ok = QuadOperations.delete_quads(db, [], [])
    end
  end

  # ===========================================================================
  # 6.1.3.3: Test delete from default graph
  # ===========================================================================

  describe "6.1.3.3 delete from default graph" do
    test "delete single quad from default graph", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)

      quad = {s_id, p_id, o_id, 0}

      :ok = QuadOperations.insert_quad(db, quad)

      assert QuadOperations.default_graph_exists?(db)

      {:ok, count_before} = QuadOperations.graph_quad_count(db, manager, :default)
      assert count_before == 1

      # Delete from default graph
      :ok = QuadOperations.delete_quad(db, quad)

      refute QuadOperations.quad_exists?(db, quad)

      {:ok, count_after} = QuadOperations.graph_quad_count(db, manager, :default)
      assert count_after == 0
    end

    test "clear all quads from default graph", %{db: db, manager: manager} do
      # Insert multiple quads to default graph
      quads =
        Enum.map(1..10, fn i ->
          subject = RDF.iri("http://example.org/s#{i}")
          predicate = RDF.iri("http://example.org/p#{rem(i, 2)}")
          object = RDF.literal("O#{i}")

          {:ok, s_id} = Manager.get_or_create_id(manager, subject)
          {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
          {:ok, o_id} = Manager.get_or_create_id(manager, object)

          QuadOperations.insert_quad(db, {s_id, p_id, o_id, 0})
          {s_id, p_id, o_id, 0}
        end)

      assert {:ok, 10} = QuadOperations.graph_quad_count(db, manager, :default)

      # Clear default graph
      {:ok, count} = QuadOperations.clear_graph(db, manager, :default)
      assert count == 10

      assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, :default)
      refute QuadOperations.default_graph_exists?(db)
    end

    test "delete default graph removes all quads", %{db: db, manager: manager} do
      Enum.each(1..5, fn i ->
        subject = RDF.iri("http://example.org/s#{i}")
        predicate = RDF.iri("http://example.org/p")
        object = RDF.literal("O#{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager, object)

        QuadOperations.insert_quad(db, {s_id, p_id, o_id, 0})
      end)

      assert {:ok, 5} = QuadOperations.graph_quad_count(db, manager, :default)

      # Delete entire default graph
      {:ok, count} = QuadOperations.delete_graph(db, manager, :default)
      assert count == 5

      refute QuadOperations.default_graph_exists?(db)
    end
  end

  # ===========================================================================
  # 6.1.3.4: Test delete from named graph
  # ===========================================================================

  describe "6.1.3.4 delete from named graph" do
    test "delete single quad from named graph", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/named-graph")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      quad = {s_id, p_id, o_id, g_id}

      :ok = QuadOperations.insert_quad(db, quad)

      assert QuadOperations.graph_exists?(db, manager, graph)

      {:ok, count_before} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count_before == 1

      :ok = QuadOperations.delete_quad(db, quad)

      refute QuadOperations.quad_exists?(db, quad)

      {:ok, count_after} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count_after == 0
    end

    test "clear all quads from named graph", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/my-graph")

      # Insert multiple quads
      Enum.each(1..10, fn i ->
        subject = RDF.iri("http://example.org/s#{i}")
        predicate = RDF.iri("http://example.org/p")
        object = RDF.literal("O#{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager, object)
        {:ok, g_id} = Manager.get_or_create_id(manager, graph)

        QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})
      end)

      assert {:ok, 10} = QuadOperations.graph_quad_count(db, manager, graph)

      # Clear the graph
      {:ok, count} = QuadOperations.clear_graph(db, manager, graph)
      assert count == 10

      assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, graph)
    end

    test "delete named graph removes all quads", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")

      # Add quads to both graphs
      Enum.each(1..5, fn i ->
        subject = RDF.iri("http://example.org/s#{i}")
        predicate = RDF.iri("http://example.org/p")
        object = RDF.literal("O#{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager, object)
        {:ok, g1_id} = Manager.get_or_create_id(manager, graph1)
        {:ok, g2_id} = Manager.get_or_create_id(manager, graph2)

        QuadOperations.insert_quad(db, {s_id, p_id, o_id, g1_id})
        QuadOperations.insert_quad(db, {s_id, p_id, o_id, g2_id})
      end)

      # Delete graph1
      {:ok, count} = QuadOperations.delete_graph(db, manager, graph1)
      assert count == 5

      # Verify graph1 is empty
      assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, graph1)

      # Verify graph2 still has data
      assert {:ok, 5} = QuadOperations.graph_quad_count(db, manager, graph2)
    end
  end

  # ===========================================================================
  # 6.1.3.5: Test delete non-existent quad is no-op
  # ===========================================================================

  describe "6.1.3.5 delete non-existent quad is no-op" do
    test "deleting non-existent quad is safe", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/g")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      quad = {s_id, p_id, o_id, g_id}

      # Delete without inserting
      :ok = QuadOperations.delete_quad(db, quad)

      # Should be a no-op - no error
      refute QuadOperations.quad_exists?(db, quad)
    end

    test "deleting already-deleted quad is safe", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/s")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/g")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      quad = {s_id, p_id, o_id, g_id}

      # Insert then delete
      :ok = QuadOperations.insert_quad(db, quad)
      :ok = QuadOperations.delete_quad(db, quad)

      # Delete again - should be safe
      :ok = QuadOperations.delete_quad(db, quad)
    end

    test "batch delete with non-existent quads is safe", %{db: db, manager: manager} do
      # Insert only one quad
      subject = RDF.iri("http://example.org/s1")
      predicate = RDF.iri("http://example.org/p")
      object = RDF.literal("O")
      graph = RDF.iri("http://example.org/g")

      {:ok, s_id} = Manager.get_or_create_id(manager, subject)
      {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
      {:ok, o_id} = Manager.get_or_create_id(manager, object)
      {:ok, g_id} = Manager.get_or_create_id(manager, graph)

      real_quad = {s_id, p_id, o_id, g_id}

      # Create fake quads with different IDs
      fake_quads = [
        {s_id + 1000, p_id, o_id, g_id},
        {s_id, p_id + 1000, o_id, g_id},
        {s_id, p_id, o_id, g_id + 1000}
      ]

      :ok = QuadOperations.insert_quad(db, real_quad)

      # Mix of real and fake quads - delete_quads requires opts as 3rd arg
      :ok = QuadOperations.delete_quads(db, [real_quad | fake_quads], [])

      # Real quad should be deleted
      refute QuadOperations.quad_exists?(db, real_quad)
    end
  end

  # ===========================================================================
  # 6.1.3.6: Test delete all quads from graph
  # ===========================================================================

  describe "6.1.3.6 delete all quads from graph" do
    test "delete_graph removes all quads from named graph", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/test-graph")

      # Insert various quads with different predicates
      Enum.each(1..20, fn i ->
        subject = RDF.iri("http://example.org/s#{rem(i, 5)}")
        predicate = RDF.iri("http://example.org/p#{rem(i, 3)}")
        object = RDF.literal("O#{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager, object)
        {:ok, g_id} = Manager.get_or_create_id(manager, graph)

        QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})
      end)

      assert {:ok, 20} = QuadOperations.graph_quad_count(db, manager, graph)

      # Delete all quads from graph
      {:ok, count} = QuadOperations.delete_graph(db, manager, graph)
      assert count == 20

      # Verify empty
      assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, graph)
      refute QuadOperations.graph_exists?(db, manager, graph)
    end

    test "delete_graph affects only specified graph", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")

      # Insert to both graphs
      Enum.each(1..10, fn i ->
        subject = RDF.iri("http://example.org/s#{i}")
        predicate = RDF.iri("http://example.org/p")
        object = RDF.literal("O#{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager, object)
        {:ok, g1_id} = Manager.get_or_create_id(manager, graph1)
        {:ok, g2_id} = Manager.get_or_create_id(manager, graph2)

        QuadOperations.insert_quad(db, {s_id, p_id, o_id, g1_id})
        QuadOperations.insert_quad(db, {s_id, p_id, o_id, g2_id})
      end)

      # Delete graph1
      {:ok, _} = QuadOperations.delete_graph(db, manager, graph1)

      # graph1 should be empty
      assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, graph1)

      # graph2 should still have data
      assert {:ok, 10} = QuadOperations.graph_quad_count(db, manager, graph2)
    end

    test "clear_graph vs delete_graph semantics", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/test")

      # Insert quads
      Enum.each(1..5, fn i ->
        subject = RDF.iri("http://example.org/s#{i}")
        predicate = RDF.iri("http://example.org/p")
        object = RDF.literal("O#{i}")

        {:ok, s_id} = Manager.get_or_create_id(manager, subject)
        {:ok, p_id} = Manager.get_or_create_id(manager, predicate)
        {:ok, o_id} = Manager.get_or_create_id(manager, object)
        {:ok, g_id} = Manager.get_or_create_id(manager, graph)

        QuadOperations.insert_quad(db, {s_id, p_id, o_id, g_id})
      end)

      # clear_graph removes quads but keeps graph ID valid
      {:ok, count} = QuadOperations.clear_graph(db, manager, graph)
      assert count == 5

      # Can still get graph ID (it exists in dictionary)
      {:ok, g_id} = Manager.lookup_id(manager, graph)
      assert is_integer(g_id) and g_id > 0

      # But no quads
      assert {:ok, 0} = QuadOperations.graph_quad_count(db, manager, graph)
    end
  end
end
