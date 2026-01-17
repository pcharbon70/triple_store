defmodule TripleStore.SPARQL.InsertDataQuadTest do
  @moduledoc """
  Unit tests for INSERT DATA with quad store (Section 4.2).

  Tests INSERT DATA operations with named graph support in quad stores.
  """

  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.UpdateExecutor
  alias TripleStore.SPARQL.Parser

  @test_db_base "/tmp/triple_store_insert_data_quad_test"

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

  # ===========================================================================
  # 4.2.1 Single Graph INSERT
  # ===========================================================================

  describe "INSERT DATA to default graph" do
    test "inserts single triple to default graph", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Verify the quad was inserted
      assert QuadOperations.default_graph_exists?(ctx.db) == true
      assert {:ok, 1} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end

    test "inserts multiple triples to default graph", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s1"},
                {:named_node, "http://example.org/p1"},
                {:literal, :simple, "value1"},
                :default_graph},
        {:quad, {:named_node, "http://example.org/s2"},
                {:named_node, "http://example.org/p2"},
                {:literal, :simple, "value2"},
                :default_graph}
      ]

      assert {:ok, 2} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Verify both quads were inserted
      assert {:ok, 2} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end

    test "inserts with different literal types", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p1"},
                {:literal, :simple, "simple"},
                :default_graph},
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p2"},
                {:literal, :lang, "hello", "en"},
                :default_graph},
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p3"},
                {:literal, :typed, "42", "http://www.w3.org/2001/XMLSchema#integer"},
                :default_graph}
      ]

      assert {:ok, 3} = UpdateExecutor.execute_insert_data(ctx, quads)
      assert {:ok, 3} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end
  end

  # ===========================================================================
  # 4.2.2 Named Graph INSERT
  # ===========================================================================

  describe "INSERT DATA to named graph" do
    test "inserts single triple to named graph", %{ctx: ctx} do
      graph_iri = "http://example.org/named"

      quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                {:named_node, graph_iri}}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Verify the quad was inserted
      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "inserts to multiple named graphs", %{ctx: ctx} do
      graph1 = "http://example.org/g1"
      graph2 = "http://example.org/g2"

      quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "v1"},
                {:named_node, graph1}},
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "v2"},
                {:named_node, graph2}}
      ]

      assert {:ok, 2} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Verify both graphs have data
      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph1))

      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph2))
    end

    test "creates graph ID for new named graph", %{ctx: ctx} do
      graph_iri = "http://example.org/new_graph"

      # Graph shouldn't have any quads initially
      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))

      quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                {:named_node, graph_iri}}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Verify the graph now has data (ID was created)
      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end
  end

  # ===========================================================================
  # 4.2.3 Parser-based INSERT
  # ===========================================================================

  describe "INSERT DATA via parser" do
    test "parses and executes INSERT DATA for default graph", %{ctx: ctx} do
      query = """
      INSERT DATA {
        <http://example.org/s> <http://example.org/p> "value" .
      }
      """

      {:ok, ast} = Parser.parse_update(query)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

      # Verify the quad was inserted
      assert {:ok, 1} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end

    test "parses and executes INSERT DATA with GRAPH clause", %{ctx: ctx} do
      graph_iri = "http://example.org/named"

      query = """
      INSERT DATA {
        GRAPH <#{graph_iri}> {
          <http://example.org/s> <http://example.org/p> "value" .
        }
      }
      """

      {:ok, ast} = Parser.parse_update(query)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

      # Verify the quad was inserted to the named graph
      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "parses and executes INSERT DATA with multiple triples", %{ctx: ctx} do
      query = """
      INSERT DATA {
        <http://example.org/s1> <http://example.org/p1> "v1" .
        <http://example.org/s2> <http://example.org/p2> "v2" .
        <http://example.org/s3> <http://example.org/p3> "v3" .
      }
      """

      {:ok, ast} = Parser.parse_update(query)
      assert {:ok, 3} = UpdateExecutor.execute(ctx, ast)

      # Verify all quads were inserted
      assert {:ok, 3} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end
  end

  # ===========================================================================
  # 4.2.4 Error Handling
  # ===========================================================================

  describe "INSERT DATA error handling" do
    test "returns error for too many triples", %{ctx: ctx} do
      # Create more than @max_data_triples quads
      max = UpdateExecutor.max_data_triples()

      quads =
        for i <- 1..(max + 1) do
          {:quad, {:named_node, "http://example.org/s#{i}"},
                  {:named_node, "http://example.org/p"},
                  {:literal, :simple, "value#{i}"},
                  :default_graph}
        end

      assert {:error, :too_many_triples} = UpdateExecutor.execute_insert_data(ctx, quads)
    end

    test "handles empty quad list", %{ctx: ctx} do
      assert {:ok, 0} = UpdateExecutor.execute_insert_data(ctx, [])
    end
  end

  # ===========================================================================
  # 4.2.5 Internal Helper Tests
  # ===========================================================================

  describe "quads_to_rdf_quads conversion" do
    test "converts AST quads with default graph via execute", %{ctx: ctx} do
      ast_quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, ast_quads)
    end

    test "converts AST quads with named graph via execute", %{ctx: ctx} do
      graph_iri = "http://example.org/named"

      ast_quads = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                {:named_node, graph_iri}}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, ast_quads)

      # Verify it went to the named graph
      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "handles legacy triple format via execute", %{ctx: ctx} do
      ast_quads = [
        {:triple, {:named_node, "http://example.org/s"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "value"}}
      ]

      # Legacy triples default to default graph
      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, ast_quads)
      assert {:ok, 1} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end
  end
end
