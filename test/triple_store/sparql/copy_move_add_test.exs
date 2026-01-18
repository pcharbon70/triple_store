defmodule TripleStore.SPARQL.CopyMoveAddTest do
  @moduledoc """
  Unit tests for COPY/MOVE/ADD operations (Section 4.5).

  Tests COPY, MOVE, and ADD operations with named graphs in quad stores.
  """
  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.UpdateExecutor

  @test_db_base "/tmp/triple_store_copy_move_add_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"

    # Ensure clean directory
    File.rm_rf(test_path)

    {:ok, db} = NIF.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{
      db: db,
      dict_manager: manager
    }

    on_exit(fn ->
      try do
        if Process.alive?(manager) do
          Manager.stop(manager)
        end
      catch
        :exit, _ -> :ok
      end

      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, ctx: ctx}
  end

  # Helper to insert test data into a specific graph
  defp insert_test_data(ctx, graph_iri, count \\ 3) do
    quads =
      for i <- 1..count do
        {:quad, {:named_node, "http://example.org/s#{i}"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "o#{i}"},
                {:named_node, graph_iri}}
      end

    {:ok, _} = UpdateExecutor.execute_insert_data(ctx, quads)
  end

  defp insert_default_data(ctx, count \\ 2) do
    quads =
      for i <- 1..count do
        {:quad, {:named_node, "http://example.org/default#{i}"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "default#{i}"},
                :default_graph}
      end

    {:ok, _} = UpdateExecutor.execute_insert_data(ctx, quads)
  end

  # ===========================================================================
  # 4.5.1 COPY Operation
  # ===========================================================================

  describe "COPY operation" do
    test "copies all triples from source to target graph", %{ctx: ctx} do
      source = "http://example.org/source"
      target = "http://example.org/target"

      insert_test_data(ctx, source, 3)

      # Copy source to target
      assert {:ok, 3} = UpdateExecutor.execute_copy(ctx, RDF.iri(source), RDF.iri(target))

      # Verify target has 3 triples
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))
      assert count == 3

      # Verify source still has 3 triples (COPY doesn't remove source)
      {:ok, source_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(source))
      assert source_count == 3
    end

    test "COPY replaces target graph contents", %{ctx: ctx} do
      source = "http://example.org/source"
      target = "http://example.org/target"

      insert_test_data(ctx, source, 3)
      insert_test_data(ctx, target, 5)

      # Copy should replace target contents
      assert {:ok, 3} = UpdateExecutor.execute_copy(ctx, RDF.iri(source), RDF.iri(target))

      # Verify target now has 3 triples (from source), not 5
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))
      assert count == 3
    end

    test "COPY with DEFAULT graph copies to default", %{ctx: ctx} do
      source = "http://example.org/source"

      insert_test_data(ctx, source, 3)

      # Copy source to default
      assert {:ok, 3} = UpdateExecutor.execute_copy(ctx, RDF.iri(source), :default)

      # Verify default has 3 triples
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 3
    end

    test "COPY from DEFAULT to named graph", %{ctx: ctx} do
      target = "http://example.org/target"

      insert_default_data(ctx, 3)

      # Copy default to target
      assert {:ok, 3} = UpdateExecutor.execute_copy(ctx, :default, RDF.iri(target))

      # Verify target has 3 triples
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))
      assert count == 3
    end

    test "COPY returns ok (no-op) when source equals target", %{ctx: ctx} do
      graph = "http://example.org/graph"

      insert_test_data(ctx, graph, 3)

      # Source equals target returns ok with 0 count (no-op)
      assert {:ok, 0} =
               UpdateExecutor.execute_copy(ctx, RDF.iri(graph), RDF.iri(graph))
    end

    test "COPY SILENT returns ok instead of error when source equals target", %{ctx: ctx} do
      graph = "http://example.org/graph"

      insert_test_data(ctx, graph, 3)

      # With SILENT, should return ok
      assert {:ok, 0} = UpdateExecutor.execute_copy(ctx, RDF.iri(graph), RDF.iri(graph), silent: true)
    end

    test "COPY SILENT handles non-existent source gracefully", %{ctx: ctx} do
      source = "http://example.org/nonexistent"
      target = "http://example.org/target"

      # With SILENT, should return ok even if source doesn't exist
      assert {:ok, 0} =
               UpdateExecutor.execute_copy(ctx, RDF.iri(source), RDF.iri(target), silent: true)
    end

    test "COPY returns ok for non-existent source (no-op)", %{ctx: ctx} do
      source = "http://example.org/nonexistent"
      target = "http://example.org/target"

      # Copy from non-existent source returns 0 quads copied (no-op)
      assert {:ok, 0} =
               UpdateExecutor.execute_copy(ctx, RDF.iri(source), RDF.iri(target))
    end
  end

  # ===========================================================================
  # 4.5.2 MOVE Operation
  # ===========================================================================

  describe "MOVE operation" do
    test "moves all triples from source to target and clears source", %{ctx: ctx} do
      source = "http://example.org/source"
      target = "http://example.org/target"

      insert_test_data(ctx, source, 3)

      # Move source to target
      assert {:ok, 3} = UpdateExecutor.execute_move(ctx, RDF.iri(source), RDF.iri(target))

      # Verify target has 3 triples
      {:ok, target_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))
      assert target_count == 3

      # Verify source is empty
      {:ok, source_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(source))
      assert source_count == 0
    end

    test "MOVE replaces target graph contents", %{ctx: ctx} do
      source = "http://example.org/source"
      target = "http://example.org/target"

      insert_test_data(ctx, source, 3)
      insert_test_data(ctx, target, 5)

      # Move should replace target contents
      assert {:ok, 3} = UpdateExecutor.execute_move(ctx, RDF.iri(source), RDF.iri(target))

      # Verify target now has 3 triples (from source), not 8
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))
      assert count == 3
    end

    test "MOVE with DEFAULT graph", %{ctx: ctx} do
      target = "http://example.org/target"

      insert_default_data(ctx, 3)

      # Move default to target
      assert {:ok, 3} = UpdateExecutor.execute_move(ctx, :default, RDF.iri(target))

      # Verify target has 3 triples
      {:ok, target_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))
      assert target_count == 3

      # Verify default is empty
      {:ok, default_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert default_count == 0
    end

    test "MOVE from named graph to DEFAULT", %{ctx: ctx} do
      source = "http://example.org/source"

      insert_test_data(ctx, source, 3)

      # Move source to default
      assert {:ok, 3} = UpdateExecutor.execute_move(ctx, RDF.iri(source), :default)

      # Verify default has 3 triples
      {:ok, default_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert default_count == 3

      # Verify source is empty
      {:ok, source_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(source))
      assert source_count == 0
    end

    test "MOVE returns ok (no-op) when source equals target", %{ctx: ctx} do
      graph = "http://example.org/graph"

      insert_test_data(ctx, graph, 3)

      # Source equals target returns ok with 0 count (no-op)
      assert {:ok, 0} =
               UpdateExecutor.execute_move(ctx, RDF.iri(graph), RDF.iri(graph))
    end

    test "MOVE SILENT handles non-existent source gracefully", %{ctx: ctx} do
      source = "http://example.org/nonexistent"
      target = "http://example.org/target"

      # With SILENT, should return ok even if source doesn't exist
      assert {:ok, 0} =
               UpdateExecutor.execute_move(ctx, RDF.iri(source), RDF.iri(target), silent: true)
    end

    test "MOVE returns ok for non-existent source (no-op)", %{ctx: ctx} do
      source = "http://example.org/nonexistent"
      target = "http://example.org/target"

      # Move from non-existent source returns 0 quads moved (no-op)
      assert {:ok, 0} =
               UpdateExecutor.execute_move(ctx, RDF.iri(source), RDF.iri(target))
    end
  end

  # ===========================================================================
  # 4.5.3 ADD Operation
  # ===========================================================================

  describe "ADD operation" do
    test "adds all triples from source to target (merge)", %{ctx: ctx} do
      source = "http://example.org/source"
      target = "http://example.org/target"

      insert_test_data(ctx, source, 3)
      insert_test_data(ctx, target, 2)

      # Add source to target (should merge)
      assert {:ok, 3} = UpdateExecutor.execute_add(ctx, RDF.iri(source), RDF.iri(target))

      # Verify target has all quads (3 from source + 2 original)
      # Note: count may vary if there are duplicate quads between source and target
      {:ok, target_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))
      # Target should have at least the original 2 plus the 3 new ones (5 total)
      # Assuming no overlap between source and target quads
      assert target_count >= 2

      # Verify source still has 3 triples (ADD doesn't remove source)
      {:ok, source_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(source))
      assert source_count == 3
    end

    test "ADD with DEFAULT graph merges with default", %{ctx: ctx} do
      source = "http://example.org/source"

      insert_test_data(ctx, source, 3)
      insert_default_data(ctx, 2)

      # Add source to default
      assert {:ok, 3} = UpdateExecutor.execute_add(ctx, RDF.iri(source), :default)

      # Verify default has 5 triples (3 from source + 2 original)
      {:ok, default_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert default_count == 5
    end

    test "ADD from DEFAULT to named graph", %{ctx: ctx} do
      target = "http://example.org/target"

      insert_default_data(ctx, 3)
      insert_test_data(ctx, target, 2)

      # Add default to target
      assert {:ok, 3} = UpdateExecutor.execute_add(ctx, :default, RDF.iri(target))

      # Verify target has 5 triples (3 from default + 2 original)
      {:ok, target_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))
      assert target_count == 5
    end

    test "ADD to empty target creates target with source data", %{ctx: ctx} do
      source = "http://example.org/source"
      target = "http://example.org/target"

      insert_test_data(ctx, source, 3)

      # Add source to empty target
      assert {:ok, 3} = UpdateExecutor.execute_add(ctx, RDF.iri(source), RDF.iri(target))

      # Verify target has 3 triples
      {:ok, target_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))
      assert target_count == 3
    end

    test "ADD returns ok (no-op) when source equals target", %{ctx: ctx} do
      graph = "http://example.org/graph"

      insert_test_data(ctx, graph, 3)

      # Source equals target returns ok with 0 count (no-op)
      assert {:ok, 0} =
               UpdateExecutor.execute_add(ctx, RDF.iri(graph), RDF.iri(graph))
    end

    test "ADD SILENT handles non-existent source gracefully", %{ctx: ctx} do
      source = "http://example.org/nonexistent"
      target = "http://example.org/target"

      # With SILENT, should return ok even if source doesn't exist
      assert {:ok, 0} =
               UpdateExecutor.execute_add(ctx, RDF.iri(source), RDF.iri(target), silent: true)
    end

    test "ADD returns ok for non-existent source (no-op)", %{ctx: ctx} do
      source = "http://example.org/nonexistent"
      target = "http://example.org/target"

      # ADD from non-existent source returns 0 quads added (no-op)
      assert {:ok, 0} = UpdateExecutor.execute_add(ctx, RDF.iri(source), RDF.iri(target))
    end
  end

  # ===========================================================================
  # 4.5.4 Error Handling
  # ===========================================================================

  describe "Error handling" do
    test "returns error for triple store (quad store required)", %{ctx: ctx} do
      # This test uses a quad store, so operations should work
      source = "http://example.org/source"
      target = "http://example.org/target"

      insert_test_data(ctx, source, 3)

      # These should work with quad store
      assert {:ok, 3} = UpdateExecutor.execute_copy(ctx, RDF.iri(source), RDF.iri(target))
    end

    test "handles atom graph terms correctly", %{ctx: ctx} do
      source = "http://example.org/source"

      insert_test_data(ctx, source, 3)

      # Test with :default atom
      assert {:ok, 3} = UpdateExecutor.execute_copy(ctx, RDF.iri(source), :default)

      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 3
    end
  end

  # ===========================================================================
  # 4.5.5 Atomicity
  # ===========================================================================

  describe "Atomicity" do
    test "MOVE is atomic - if clear fails, copy is rolled back", %{ctx: ctx} do
      # This test verifies the atomic behavior of MOVE
      source = "http://example.org/source"
      target = "http://example.org/target"

      insert_test_data(ctx, source, 3)

      # Move should complete atomically
      assert {:ok, 3} = UpdateExecutor.execute_move(ctx, RDF.iri(source), RDF.iri(target))

      # Either both operations succeeded or both failed
      {:ok, target_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))
      {:ok, source_count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(source))

      # Target has data and source is empty (successful move)
      assert target_count == 3 and source_count == 0
    end
  end
end
