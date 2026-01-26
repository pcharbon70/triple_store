defmodule TripleStore.SPARQL.DeleteDataQuadTest do
  @moduledoc """
  Unit tests for DELETE DATA with quad store (Section 4.3).

  Tests DELETE DATA operations with named graph support in quad stores.
  """

  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.UpdateExecutor
  alias TripleStore.SPARQL.Parser

  @test_db_base "/tmp/triple_store_delete_data_quad_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"

    # Ensure clean directory
    File.rm_rf(test_path)

    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{
      db: db,
      dict_manager: manager
    }

    on_exit(fn ->
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      ErlangAdapter.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, ctx: ctx}
  end

  # ===========================================================================
  # 4.3.1 Single Graph DELETE
  # ===========================================================================

  describe "DELETE DATA from default graph" do
    test "deletes single quad from default graph", %{ctx: ctx} do
      # First insert a quad
      quads = [
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
         {:literal, :simple, "value"}, :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)
      assert {:ok, 1} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)

      # Now delete it
      assert {:ok, 1} = UpdateExecutor.execute_delete_data(ctx, quads)
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end

    test "deletes multiple quads from default graph", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s1"}, {:named_node, "http://example.org/p1"},
         {:literal, :simple, "value1"}, :default_graph},
        {:quad, {:named_node, "http://example.org/s2"}, {:named_node, "http://example.org/p2"},
         {:literal, :simple, "value2"}, :default_graph}
      ]

      assert {:ok, 2} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Delete both
      assert {:ok, 2} = UpdateExecutor.execute_delete_data(ctx, quads)
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end

    test "deletes with different literal types", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p1"},
         {:literal, :simple, "simple"}, :default_graph},
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p2"},
         {:literal, :lang, "hello", "en"}, :default_graph},
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p3"},
         {:literal, :typed, "42", "http://www.w3.org/2001/XMLSchema#integer"}, :default_graph}
      ]

      assert {:ok, 3} = UpdateExecutor.execute_insert_data(ctx, quads)
      assert {:ok, 3} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)

      # Delete all
      assert {:ok, 3} = UpdateExecutor.execute_delete_data(ctx, quads)
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end
  end

  # ===========================================================================
  # 4.3.2 Named Graph DELETE
  # ===========================================================================

  describe "DELETE DATA from named graph" do
    test "deletes single quad from named graph", %{ctx: ctx} do
      graph_iri = "http://example.org/named"

      quads = [
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
         {:literal, :simple, "value"}, {:named_node, graph_iri}}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)

      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))

      # Now delete it
      assert {:ok, 1} = UpdateExecutor.execute_delete_data(ctx, quads)

      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "deletes from multiple named graphs", %{ctx: ctx} do
      graph1 = "http://example.org/g1"
      graph2 = "http://example.org/g2"

      quads = [
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
         {:literal, :simple, "v1"}, {:named_node, graph1}},
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
         {:literal, :simple, "v2"}, {:named_node, graph2}}
      ]

      assert {:ok, 2} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Delete both
      assert {:ok, 2} = UpdateExecutor.execute_delete_data(ctx, quads)

      # Verify both graphs are empty
      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph1))

      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph2))
    end

    test "deletes only from specified graph", %{ctx: ctx} do
      graph1 = "http://example.org/g1"
      graph2 = "http://example.org/g2"

      # Insert quads to both graphs
      quads = [
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
         {:literal, :simple, "value"}, {:named_node, graph1}},
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
         {:literal, :simple, "value"}, {:named_node, graph2}}
      ]

      assert {:ok, 2} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Delete only from graph1
      delete_quads = [
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
         {:literal, :simple, "value"}, {:named_node, graph1}}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_delete_data(ctx, delete_quads)

      # Verify graph1 is empty, graph2 still has data
      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph1))

      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph2))
    end
  end

  # ===========================================================================
  # 4.3.3 Parser-based DELETE
  # ===========================================================================

  describe "DELETE DATA via parser" do
    test "parses and executes DELETE DATA for default graph", %{ctx: ctx} do
      # First insert
      insert_query = """
      INSERT DATA {
        <http://example.org/s> <http://example.org/p> "value" .
      }
      """

      {:ok, ast} = Parser.parse_update(insert_query)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

      # Now delete
      delete_query = """
      DELETE DATA {
        <http://example.org/s> <http://example.org/p> "value" .
      }
      """

      {:ok, ast} = Parser.parse_update(delete_query)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

      # Verify the quad was deleted
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end

    test "parses and executes DELETE DATA with GRAPH clause", %{ctx: ctx} do
      graph_iri = "http://example.org/named"

      # First insert
      insert_query = """
      INSERT DATA {
        GRAPH <#{graph_iri}> {
          <http://example.org/s> <http://example.org/p> "value" .
        }
      }
      """

      {:ok, ast} = Parser.parse_update(insert_query)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

      # Now delete
      delete_query = """
      DELETE DATA {
        GRAPH <#{graph_iri}> {
          <http://example.org/s> <http://example.org/p> "value" .
        }
      }
      """

      {:ok, ast} = Parser.parse_update(delete_query)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

      # Verify the quad was deleted from the named graph
      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "parses and executes DELETE DATA with multiple triples", %{ctx: ctx} do
      # First insert
      insert_query = """
      INSERT DATA {
        <http://example.org/s1> <http://example.org/p1> "v1" .
        <http://example.org/s2> <http://example.org/p2> "v2" .
        <http://example.org/s3> <http://example.org/p3> "v3" .
      }
      """

      {:ok, ast} = Parser.parse_update(insert_query)
      assert {:ok, 3} = UpdateExecutor.execute(ctx, ast)

      # Delete all
      delete_query = """
      DELETE DATA {
        <http://example.org/s1> <http://example.org/p1> "v1" .
        <http://example.org/s2> <http://example.org/p2> "v2" .
        <http://example.org/s3> <http://example.org/p3> "v3" .
      }
      """

      {:ok, ast} = Parser.parse_update(delete_query)
      assert {:ok, 3} = UpdateExecutor.execute(ctx, ast)

      # Verify all quads were deleted
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end
  end

  # ===========================================================================
  # 4.3.4 Error Handling
  # ===========================================================================

  describe "DELETE DATA error handling" do
    test "returns error for too many triples", %{ctx: ctx} do
      max = UpdateExecutor.max_data_triples()

      quads =
        for i <- 1..(max + 1) do
          {:quad, {:named_node, "http://example.org/s#{i}"},
           {:named_node, "http://example.org/p"}, {:literal, :simple, "value#{i}"},
           :default_graph}
        end

      assert {:error, :too_many_triples} = UpdateExecutor.execute_delete_data(ctx, quads)
    end

    test "handles empty quad list", %{ctx: ctx} do
      assert {:ok, 0} = UpdateExecutor.execute_delete_data(ctx, [])
    end

    test "deleting non-existent quads returns ok with count 0", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/nonexistent"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "value"}, :default_graph}
      ]

      # Should succeed but delete nothing
      assert {:ok, 0} = UpdateExecutor.execute_delete_data(ctx, quads)
    end

    test "deleting from non-existent named graph returns ok with count 0", %{ctx: ctx} do
      graph_iri = "http://example.org/nonexistent_graph"

      quads = [
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
         {:literal, :simple, "value"}, {:named_node, graph_iri}}
      ]

      # Should succeed but delete nothing (graph doesn't exist)
      assert {:ok, 0} = UpdateExecutor.execute_delete_data(ctx, quads)
    end
  end

  # ===========================================================================
  # 4.3.5 Idempotent Deletion
  # ===========================================================================

  describe "DELETE DATA idempotence" do
    test "deleting same quad twice is idempotent", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
         {:literal, :simple, "value"}, :default_graph}
      ]

      # Insert
      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)

      # First delete
      assert {:ok, 1} = UpdateExecutor.execute_delete_data(ctx, quads)
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)

      # Second delete (idempotent)
      assert {:ok, 0} = UpdateExecutor.execute_delete_data(ctx, quads)
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end
  end
end
