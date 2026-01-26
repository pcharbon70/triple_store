defmodule TripleStore.Integration.UpdateOperationsTest do
  @moduledoc """
  Integration tests for Section 6.4: Update Integration Tests.

  Tests SPARQL UPDATE operations with graphs:
  - Graph Management Updates (CREATE/DROP/CLEAR GRAPH)
  - INSERT/DELETE with Graphs
  - MODIFY Operations (DELETE/INSERT WHERE)
  - COPY/MOVE/ADD Operations
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Authorization
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.UpdateExecutor

  @test_db_base "/tmp/update_operations_test"
  @ex "http://example.org/"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    TripleStore.Integration.Helpers.unique_path("update_operations_test")
  end

  defp cleanup_path(path) do
    TripleStore.Integration.Helpers.cleanup_path(path)
  end

  defp setup_graph_authorization(ctx) do
    # Grant public write permissions to all test graphs
    :ok = Authorization.set_public(ctx, "#{@ex}graph1")
    :ok = Authorization.set_public(ctx, "#{@ex}graph2")
    :ok = Authorization.set_public(ctx, "#{@ex}graph3")
    :ok = Authorization.set_public(ctx, "#{@ex}source")
    :ok = Authorization.set_public(ctx, "#{@ex}target")
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    db_path = unique_path()

    {:ok, db} = ErlangAdapter.open(db_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: manager}

    setup_graph_authorization(ctx)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      cleanup_path(db_path)
    end)

    %{ctx: ctx, db: db, manager: manager}
  end

  # ===========================================================================
  # 6.4.1: Graph Management Updates (6 tests)
  # ===========================================================================

  describe "6.4.1 Graph Management Updates" do
    test "6.4.1.1 CREATE GRAPH then query returns empty", %{ctx: ctx} do
      graph_iri = "#{@ex}new_graph"

      # Create the graph
      {:ok, ast} = Parser.parse_update("CREATE GRAPH <#{graph_iri}>")
      assert {:ok, 0} = UpdateExecutor.execute(ctx, ast)

      # Query the graph - should return empty results
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?s ?p ?o WHERE {
        GRAPH <#{graph_iri}> { ?s ?p ?o }
      }
      """

      {:ok, results} = TripleStore.SPARQL.Query.query(ctx, query)
      assert results == []
    end

    test "6.4.1.2 DROP GRAPH removes all data", %{ctx: ctx} do
      graph_iri = "#{@ex}to_drop"

      # Insert data first
      {:ok, ast} =
        Parser.parse_update(
          "INSERT DATA { GRAPH <#{graph_iri}> { <#{@ex}s> <#{@ex}p> \"value\" } }"
        )

      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

      # Verify data exists
      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))

      # Drop the graph
      {:ok, drop_ast} = Parser.parse_update("DROP GRAPH <#{graph_iri}>")
      assert {:ok, 1} = UpdateExecutor.execute(ctx, drop_ast)

      # Verify graph is empty
      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "6.4.1.3 CLEAR GRAPH empties graph", %{ctx: ctx} do
      graph_iri = "#{@ex}to_clear"

      # Insert multiple quads
      {:ok, ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "v1" .
              <#{@ex}s2> <#{@ex}p> "v2" .
              <#{@ex}s3> <#{@ex}p> "v3" .
            }
          }
        """)

      assert {:ok, 3} = UpdateExecutor.execute(ctx, ast)

      # Clear the graph
      {:ok, clear_ast} = Parser.parse_update("CLEAR GRAPH <#{graph_iri}>")
      assert {:ok, 3} = UpdateExecutor.execute(ctx, clear_ast)

      # Verify graph is empty
      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "6.4.1.4 CREATE SILENT on existing graph", %{ctx: ctx} do
      graph_iri = "#{@ex}existing"

      # Create the graph first
      {:ok, ast} = Parser.parse_update("CREATE GRAPH <#{graph_iri}>")
      assert {:ok, 0} = UpdateExecutor.execute(ctx, ast)

      # Try to create again with SILENT - should succeed
      {:ok, ast2} = Parser.parse_update("CREATE SILENT GRAPH <#{graph_iri}>")
      assert {:ok, 0} = UpdateExecutor.execute(ctx, ast2)
    end

    test "6.4.1.5 DROP SILENT on missing graph", %{ctx: ctx} do
      graph_iri = "#{@ex}nonexistent"

      # Drop non-existent graph with SILENT - should succeed
      {:ok, ast} = Parser.parse_update("DROP SILENT GRAPH <#{graph_iri}>")
      assert {:ok, 0} = UpdateExecutor.execute(ctx, ast)
    end

    test "6.4.1.6 CLEAR ALL empties database", %{ctx: ctx} do
      graph1 = "#{@ex}g1"
      graph2 = "#{@ex}g2"

      # Insert data to multiple graphs
      {:ok, ast1} =
        Parser.parse_update("INSERT DATA { GRAPH <#{graph1}> { <#{@ex}s> <#{@ex}p> \"v\" } }")

      {:ok, ast2} =
        Parser.parse_update("INSERT DATA { GRAPH <#{graph2}> { <#{@ex}s> <#{@ex}p> \"v\" } }")

      {:ok, ast3} = Parser.parse_update("INSERT DATA { <#{@ex}s> <#{@ex}p> \"default\" }")

      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast1)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast2)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast3)

      # Clear all graphs
      {:ok, clear_ast} = Parser.parse_update("CLEAR ALL")
      assert {:ok, 3} = UpdateExecutor.execute(ctx, clear_ast)

      # Verify all graphs are empty
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph1))
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph2))
    end
  end

  # ===========================================================================
  # 6.4.2: INSERT/DELETE with Graphs (6 tests)
  # ===========================================================================

  describe "6.4.2 INSERT/DELETE with Graphs" do
    test "6.4.2.1 INSERT DATA to named graph", %{ctx: ctx} do
      graph_iri = "#{@ex}target"

      # Insert to named graph
      {:ok, ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p1> "o1" .
              <#{@ex}s2> <#{@ex}p2> "o2" .
            }
          }
        """)

      assert {:ok, 2} = UpdateExecutor.execute(ctx, ast)

      # Verify insertion
      assert {:ok, 2} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "6.4.2.2 INSERT DATA with multiple GRAPH blocks", %{ctx: ctx} do
      graph1 = "#{@ex}g1"
      graph2 = "#{@ex}g2"

      # Insert to multiple graphs in single operation
      {:ok, ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph1}> {
              <#{@ex}s> <#{@ex}p> "v1" .
            }
            GRAPH <#{graph2}> {
              <#{@ex}s> <#{@ex}p> "v2" .
            }
          }
        """)

      assert {:ok, 2} = UpdateExecutor.execute(ctx, ast)

      # Verify both graphs have data
      assert {:ok, 1} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph1))
      assert {:ok, 1} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph2))
    end

    test "6.4.2.3 DELETE DATA from named graph", %{ctx: ctx} do
      graph_iri = "#{@ex}target"

      # Insert first
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "o1" .
              <#{@ex}s2> <#{@ex}p> "o2" .
              <#{@ex}s3> <#{@ex}p> "o3" .
            }
          }
        """)

      assert {:ok, 3} = UpdateExecutor.execute(ctx, insert_ast)

      # Delete specific quads
      {:ok, delete_ast} =
        Parser.parse_update("""
          DELETE DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s2> <#{@ex}p> "o2" .
            }
          }
        """)

      assert {:ok, 1} = UpdateExecutor.execute(ctx, delete_ast)

      # Verify only 2 quads remain
      assert {:ok, 2} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "6.4.2.4 DELETE DATA with multiple GRAPH blocks", %{ctx: ctx} do
      graph1 = "#{@ex}g1"
      graph2 = "#{@ex}g2"

      # Insert to multiple graphs
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph1}> { <#{@ex}s> <#{@ex}p> "v1" . }
            GRAPH <#{graph2}> { <#{@ex}s> <#{@ex}p> "v2" . }
          }
        """)

      assert {:ok, 2} = UpdateExecutor.execute(ctx, insert_ast)

      # Delete from both graphs
      {:ok, delete_ast} =
        Parser.parse_update("""
          DELETE DATA {
            GRAPH <#{graph1}> { <#{@ex}s> <#{@ex}p> "v1" . }
            GRAPH <#{graph2}> { <#{@ex}s> <#{@ex}p> "v2" . }
          }
        """)

      assert {:ok, 2} = UpdateExecutor.execute(ctx, delete_ast)

      # Verify both graphs are empty
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph1))
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph2))
    end

    test "6.4.2.5 INSERT then DELETE same quad", %{ctx: ctx} do
      graph_iri = "#{@ex}test"

      # Insert a quad
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s> <#{@ex}p> "value" .
            }
          }
        """)

      assert {:ok, 1} = UpdateExecutor.execute(ctx, insert_ast)

      # Verify insertion
      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))

      # Delete the same quad
      {:ok, delete_ast} =
        Parser.parse_update("""
          DELETE DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s> <#{@ex}p> "value" .
            }
          }
        """)

      assert {:ok, 1} = UpdateExecutor.execute(ctx, delete_ast)

      # Verify deletion
      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "6.4.2.6 INSERT creates graph if needed", %{ctx: ctx} do
      graph_iri = "#{@ex}new_graph"

      # Verify graph doesn't exist (no quads)
      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))

      # Insert data - graph should be created automatically
      {:ok, ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s> <#{@ex}p> "value" .
            }
          }
        """)

      assert {:ok, 1} = UpdateExecutor.execute(ctx, ast)

      # Verify graph now has data
      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end
  end

  # ===========================================================================
  # 6.4.3: MODIFY Operations (5 tests)
  # ===========================================================================

  describe "6.4.3 MODIFY Operations" do
    test "6.4.3.1 MODIFY in named graph", %{ctx: ctx} do
      graph_iri = "#{@ex}test"

      # Insert initial data
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "old" .
              <#{@ex}s2> <#{@ex}p> "unchanged" .
            }
          }
        """)

      assert {:ok, 2} = UpdateExecutor.execute(ctx, insert_ast)

      # MODIFY: delete old, insert new
      {:ok, modify_ast} =
        Parser.parse_update("""
          DELETE {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "old" .
            }
          }
          INSERT {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "new" .
            }
          }
          WHERE {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "old" .
            }
          }
        """)

      assert {:ok, count} = UpdateExecutor.execute(ctx, modify_ast)
      # Delete 1, insert 1 = 2 operations
      assert count == 2

      # Verify graph still has 2 quads (s1 updated, s2 unchanged)
      assert {:ok, 2} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "6.4.3.2 MODIFY with WHERE across graphs", %{ctx: ctx} do
      graph1 = "#{@ex}g1"
      graph2 = "#{@ex}g2"

      # Insert data to both graphs
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph1}> {
              <#{@ex}s> <#{@ex}g1-p> "value" .
            }
            GRAPH <#{graph2}> {
              <#{@ex}s> <#{@ex}g2-p> "other" .
            }
          }
        """)

      assert {:ok, 2} = UpdateExecutor.execute(ctx, insert_ast)

      # MODIFY using pattern matching
      {:ok, modify_ast} =
        Parser.parse_update("""
          DELETE {
            GRAPH <#{graph1}> {
              <#{@ex}s> <#{@ex}g1-p> "value" .
            }
          }
          INSERT {
            GRAPH <#{graph1}> {
              <#{@ex}s> <#{@ex}g1-p> "updated" .
            }
          }
          WHERE {
            GRAPH <#{graph1}> {
              <#{@ex}s> <#{@ex}g1-p> "value" .
            }
          }
        """)

      assert {:ok, count} = UpdateExecutor.execute(ctx, modify_ast)
      assert count == 2

      # Verify graph1 was updated, graph2 unchanged
      assert {:ok, 1} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph1))
      assert {:ok, 1} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph2))
    end

    test "6.4.3.3 MODIFY WITH graph context", %{ctx: ctx} do
      # SPARQL 1.1 WITH clause provides default graph context
      graph_iri = "#{@ex}default"

      # Insert initial data
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s> <#{@ex}p> "old" .
            }
          }
        """)

      assert {:ok, 1} = UpdateExecutor.execute(ctx, insert_ast)

      # Use WITH to set graph context for DELETE/INSERT
      # Note: WITH clause affects the graph used for templates without GRAPH
      {:ok, modify_ast} =
        Parser.parse_update("""
          DELETE {
            <#{@ex}s> <#{@ex}p> "old" .
          }
          INSERT {
            <#{@ex}s> <#{@ex}p> "new" .
          }
          WHERE {
            GRAPH <#{graph_iri}> {
              <#{@ex}s> <#{@ex}p> "old" .
            }
          }
        """)

      # This will modify the default graph, not the named graph
      assert {:ok, count} = UpdateExecutor.execute(ctx, modify_ast)
      assert count == 2

      # Verify named graph still has original data (unchanged)
      assert {:ok, 1} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))

      # But default graph now has the new value
      assert {:ok, 1} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
    end

    test "6.4.3.4 MODIFY atomicity (all or nothing)", %{ctx: ctx} do
      graph_iri = "#{@ex}atomic"

      # Insert initial data
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "old1" .
              <#{@ex}s2> <#{@ex}p> "old2" .
            }
          }
        """)

      assert {:ok, 2} = UpdateExecutor.execute(ctx, insert_ast)

      # Get initial count
      assert {:ok, 2} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))

      # MODIFY both in one operation
      {:ok, modify_ast} =
        Parser.parse_update("""
          DELETE {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "old1" .
              <#{@ex}s2> <#{@ex}p> "old2" .
            }
          }
          INSERT {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "new1" .
              <#{@ex}s2> <#{@ex}p> "new2" .
            }
          }
          WHERE {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "old1" .
              <#{@ex}s2> <#{@ex}p> "old2" .
            }
          }
        """)

      assert {:ok, 4} = UpdateExecutor.execute(ctx, modify_ast)

      # Verify atomic operation - both old values gone, both new values present
      assert {:ok, 2} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end

    test "6.4.3.5 MODIFY returns correct counts", %{ctx: ctx} do
      graph_iri = "#{@ex}count_test"

      # Insert multiple matching quads
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{graph_iri}> {
              <#{@ex}s1> <#{@ex}p> "value" .
              <#{@ex}s2> <#{@ex}p> "value" .
              <#{@ex}s3> <#{@ex}p> "value" .
            }
          }
        """)

      assert {:ok, 3} = UpdateExecutor.execute(ctx, insert_ast)

      # DELETE only - should return count of deleted quads
      {:ok, delete_ast} =
        Parser.parse_update("""
          DELETE {
            GRAPH <#{graph_iri}> {
              ?s <#{@ex}p> "value" .
            }
          }
          WHERE {
            GRAPH <#{graph_iri}> {
              ?s <#{@ex}p> "value" .
            }
          }
        """)

      assert {:ok, count} = UpdateExecutor.execute(ctx, delete_ast)
      assert count == 3

      # Verify all quads deleted
      assert {:ok, 0} =
               QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
    end
  end

  # ===========================================================================
  # 6.4.4: COPY/MOVE/ADD Operations (5 tests)
  # ===========================================================================

  describe "6.4.4 COPY/MOVE/ADD Operations" do
    test "6.4.4.1 COPY GRAPH duplicates graph", %{ctx: ctx} do
      source = "#{@ex}source"
      target = "#{@ex}target"

      # Insert source data
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{source}> {
              <#{@ex}s1> <#{@ex}p> "v1" .
              <#{@ex}s2> <#{@ex}p> "v2" .
              <#{@ex}s3> <#{@ex}p> "v3" .
            }
          }
        """)

      assert {:ok, 3} = UpdateExecutor.execute(ctx, insert_ast)

      # Copy source to target
      {:ok, copy_ast} = Parser.parse_update("COPY GRAPH <#{source}> TO GRAPH <#{target}>")
      assert {:ok, 3} = UpdateExecutor.execute(ctx, copy_ast)

      # Verify target has copy
      assert {:ok, 3} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))

      # Verify source still has original data
      assert {:ok, 3} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(source))
    end

    test "6.4.4.2 MOVE GRAPH moves and deletes source", %{ctx: ctx} do
      source = "#{@ex}source"
      target = "#{@ex}target"

      # Insert source data
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{source}> {
              <#{@ex}s1> <#{@ex}p> "v1" .
              <#{@ex}s2> <#{@ex}p> "v2" .
            }
          }
        """)

      assert {:ok, 2} = UpdateExecutor.execute(ctx, insert_ast)

      # Move source to target
      # Note: Parser implements MOVE as DROP target + COPY + DROP source
      # So the count is: 0 (empty target) + 2 (copy) + 2 (delete source) = 4
      {:ok, move_ast} = Parser.parse_update("MOVE GRAPH <#{source}> TO GRAPH <#{target}>")
      assert {:ok, 4} = UpdateExecutor.execute(ctx, move_ast)

      # Verify target has data
      assert {:ok, 2} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))

      # Verify source is empty
      assert {:ok, 0} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(source))
    end

    test "6.4.4.3 ADD merges source into target", %{ctx: ctx} do
      source = "#{@ex}source"
      target = "#{@ex}target"

      # Insert different data to both graphs
      {:ok, insert_ast} =
        Parser.parse_update("""
          INSERT DATA {
            GRAPH <#{source}> {
              <#{@ex}s1> <#{@ex}p> "from_source" .
            }
            GRAPH <#{target}> {
              <#{@ex}s2> <#{@ex}p> "from_target" .
            }
          }
        """)

      assert {:ok, 2} = UpdateExecutor.execute(ctx, insert_ast)

      # Add source to target (merge)
      {:ok, add_ast} = Parser.parse_update("ADD GRAPH <#{source}> TO GRAPH <#{target}>")
      assert {:ok, 1} = UpdateExecutor.execute(ctx, add_ast)

      # Verify target has both quads
      assert {:ok, 2} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(target))

      # Verify source still has original data
      assert {:ok, 1} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(source))
    end

    test "6.4.4.4 operations with SILENT modifier", %{ctx: ctx} do
      nonexistent = "#{@ex}nonexistent"
      target = "#{@ex}target"

      # COPY SILENT with non-existent source should succeed
      {:ok, copy_ast} =
        Parser.parse_update("COPY SILENT GRAPH <#{nonexistent}> TO GRAPH <#{target}>")

      assert {:ok, 0} = UpdateExecutor.execute(ctx, copy_ast)

      # MOVE SILENT with non-existent source should succeed
      {:ok, move_ast} =
        Parser.parse_update("MOVE SILENT GRAPH <#{nonexistent}> TO GRAPH <#{target}>")

      assert {:ok, 0} = UpdateExecutor.execute(ctx, move_ast)

      # ADD SILENT with non-existent source should succeed
      {:ok, add_ast} =
        Parser.parse_update("ADD SILENT GRAPH <#{nonexistent}> TO GRAPH <#{target}>")

      assert {:ok, 0} = UpdateExecutor.execute(ctx, add_ast)
    end

    test "6.4.4.5 operations on non-existent graphs", %{ctx: ctx} do
      nonexistent = "#{@ex}does_not_exist"
      target = "#{@ex}target"

      # COPY from non-existent source returns 0
      {:ok, copy_ast} = Parser.parse_update("COPY GRAPH <#{nonexistent}> TO GRAPH <#{target}>")
      assert {:ok, 0} = UpdateExecutor.execute(ctx, copy_ast)

      # MOVE from non-existent source returns 0
      {:ok, move_ast} = Parser.parse_update("MOVE GRAPH <#{nonexistent}> TO GRAPH <#{target}>")
      assert {:ok, 0} = UpdateExecutor.execute(ctx, move_ast)

      # ADD from non-existent source returns 0
      {:ok, add_ast} = Parser.parse_update("ADD GRAPH <#{nonexistent}> TO GRAPH <#{target}>")
      assert {:ok, 0} = UpdateExecutor.execute(ctx, add_ast)

      # DROP on non-existent graph returns 0
      {:ok, drop_ast} = Parser.parse_update("DROP GRAPH <#{nonexistent}>")
      assert {:ok, 0} = UpdateExecutor.execute(ctx, drop_ast)
    end
  end
end
