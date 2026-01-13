defmodule TripleStore.SPARQL.ModifyQuadTest do
  @moduledoc """
  Unit tests for MODIFY (DELETE/INSERT WHERE) with quad store (Section 4.4).

  Tests MODIFY operations with named graph support in quad stores.

  Note: Full WHERE clause execution with graph-scoped pattern matching is a complex
  feature that requires significant updates to the BGP executor. This test file focuses
  on ground template patterns (no variables) that work with the current implementation.
  """
  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.UpdateExecutor
  alias TripleStore.SPARQL.Parser

  @test_db_base "/tmp/triple_store_modify_quad_test"

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
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, ctx: ctx}
  end

  # Helper to insert test data
  defp insert_test_data(ctx) do
    quads = [
      {:quad, {:named_node, "http://example.org/s1"},
              {:named_node, "http://example.org/p"},
              {:literal, :simple, "old"},
              :default_graph},
      {:quad, {:named_node, "http://example.org/s2"},
              {:named_node, "http://example.org/p"},
              {:literal, :simple, "value"},
              :default_graph}
    ]

    {:ok, _} = UpdateExecutor.execute_insert_data(ctx, quads)
  end

  defp insert_named_graph_data(ctx, graph_iri) do
    quads = [
      {:quad, {:named_node, "http://example.org/s"},
              {:named_node, "http://example.org/p"},
              {:literal, :simple, "old"},
              {:named_node, graph_iri}}
    ]

    {:ok, _} = UpdateExecutor.execute_insert_data(ctx, quads)
  end

  # ===========================================================================
  # 4.4.1 MODIFY with Ground Quad Patterns (No Variables)
  # ===========================================================================

  describe "MODIFY with ground quad patterns" do
    test "DELETE/INSERT with ground quad patterns", %{ctx: ctx} do
      graph_iri = "http://example.org/graph"
      insert_named_graph_data(ctx, graph_iri)

      delete_template = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "old"},
                {:named_node, graph_iri}}
      ]

      insert_template = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "new"},
                {:named_node, graph_iri}}
      ]

      # Empty pattern (no WHERE clause)
      pattern = nil

      assert {:ok, 2} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, pattern)

      # Verify the graph still has 1 quad (deleted old, inserted new)
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
      assert count == 1
    end

    test "DELETE with ground quad pattern, INSERT to default", %{ctx: ctx} do
      graph_iri = "http://example.org/graph"
      insert_named_graph_data(ctx, graph_iri)

      delete_template = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "old"},
                {:named_node, graph_iri}}
      ]

      insert_template = [
        {:quad, {:named_node, "http://example.org/s"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "new"},
                :default_graph}
      ]

      pattern = nil

      assert {:ok, 2} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, pattern)

      # Verify named graph is now empty
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
      assert count == 0

      # Verify default graph has the new value
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 1
    end
  end

  # ===========================================================================
  # 4.4.2 MODIFY with Triple Templates (Legacy Support)
  # ===========================================================================

  describe "MODIFY with triple templates (treated as default graph)" do
    test "DELETE/INSERT with ground triple patterns", %{ctx: ctx} do
      insert_test_data(ctx)

      delete_template = [
        {:triple, {:named_node, "http://example.org/s1"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "old"}}
      ]

      insert_template = [
        {:triple, {:named_node, "http://example.org/s1"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "new"}}
      ]

      pattern = nil

      assert {:ok, 2} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, pattern)

      # Verify default graph still has 2 quads (s1 with new value, s2 unchanged)
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 2
    end

    test "DELETE with ground pattern only", %{ctx: ctx} do
      insert_test_data(ctx)

      delete_template = [
        {:triple, {:named_node, "http://example.org/s1"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "old"}}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_modify(ctx, delete_template, [], nil)

      # Verify deletion - only s2 remains
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 1
    end

    test "INSERT with ground pattern only", %{ctx: ctx} do
      insert_test_data(ctx)

      insert_template = [
        {:triple, {:named_node, "http://example.org/new"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "value"}}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_modify(ctx, [], insert_template, nil)

      # Verify insertion
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 3
    end
  end

  # ===========================================================================
  # 4.4.3 Parser-based MODIFY
  # ===========================================================================

  describe "MODIFY via parser" do
    test "parses and executes DELETE/INSERT for named graph", %{ctx: ctx} do
      graph_iri = "http://example.org/graph"
      insert_named_graph_data(ctx, graph_iri)

      # SPARQL MODIFY requires WHERE clause
      # The WHERE clause matches the quad in the named graph
      query = """
      DELETE {
        GRAPH <#{graph_iri}> {
          <http://example.org/s> <http://example.org/p> "old" .
        }
      }
      INSERT {
        GRAPH <#{graph_iri}> {
          <http://example.org/s> <http://example.org/p> "new" .
        }
      }
      WHERE {
        GRAPH <#{graph_iri}> {
          <http://example.org/s> <http://example.org/p> "old" .
        }
      }
      """

      {:ok, ast} = Parser.parse_update(query)
      assert {:ok, count} = UpdateExecutor.execute(ctx, ast)
      # Should delete 1 and insert 1, so count is 2
      assert count == 2

      # Verify the named graph has the new value
      {:ok, count_after} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, RDF.iri(graph_iri))
      assert count_after == 1
    end

    test "parses and executes DELETE/INSERT for default graph", %{ctx: ctx} do
      insert_test_data(ctx)

      query = """
      DELETE {
        <http://example.org/s1> <http://example.org/p> "old" .
      }
      INSERT {
        <http://example.org/s1> <http://example.org/p> "updated" .
      }
      WHERE {
        <http://example.org/s1> <http://example.org/p> "old" .
      }
      """

      {:ok, ast} = Parser.parse_update(query)
      assert {:ok, count} = UpdateExecutor.execute(ctx, ast)
      # Should delete 1 and insert 1, so count is 2
      assert count == 2

      # Verify default graph still has 2 quads (s1 with updated value, s2 unchanged)
      {:ok, count_after} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count_after == 2
    end

    test "parses MODIFY with DELETE WHERE for default graph", %{ctx: ctx} do
      # Test DELETE WHERE (no INSERT)
      insert_test_data(ctx)

      query = """
      DELETE {
        <http://example.org/s1> <http://example.org/p> "old" .
      }
      WHERE {
        <http://example.org/s1> <http://example.org/p> "old" .
      }
      """

      {:ok, ast} = Parser.parse_update(query)
      assert {:ok, count} = UpdateExecutor.execute(ctx, ast)
      # Should delete 1 matching quad
      assert count == 1

      # Verify only s2 remains
      {:ok, count_after} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count_after == 1
    end
  end

  # ===========================================================================
  # 4.4.4 MODIFY with WHERE Clause and Variables
  # ===========================================================================

  describe "MODIFY with WHERE clause and variables" do
    test "DELETE/INSERT WHERE with variable substitution", %{ctx: ctx} do
      insert_test_data(ctx)

      delete_template = [
        {:triple, {:variable, "s"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "old"}}
      ]

      insert_template = [
        {:triple, {:variable, "s"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "updated"}}
      ]

      # WHERE pattern that matches s1 with "old" and binds s
      pattern = {:bgp, [
        {:triple, {:variable, "s"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "old"}}
      ]}

      assert {:ok, count} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, pattern)
      # Should delete 1 and insert 1, so count is 2
      assert count == 2

      # Verify: s1 with "updated" + s2 unchanged = 2 total
      {:ok, count_after} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count_after == 2
    end

    test "DELETE WHERE with variable substitution only", %{ctx: ctx} do
      insert_test_data(ctx)

      delete_template = [
        {:triple, {:variable, "s"},
                 {:variable, "p"},
                 {:literal, :simple, "old"}}
      ]

      # WHERE pattern that matches s1 with "old" and binds both s and p
      pattern = {:bgp, [
        {:triple, {:variable, "s"},
                 {:variable, "p"},
                 {:literal, :simple, "old"}}
      ]}

      assert {:ok, count} = UpdateExecutor.execute_modify(ctx, delete_template, [], pattern)
      # Should delete 1 (s1)
      assert count == 1

      # Verify: s1 deleted, s2 remains
      {:ok, count_after} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count_after == 1
    end

    test "INSERT WHERE with variable substitution only", %{ctx: ctx} do
      insert_test_data(ctx)

      insert_template = [
        {:triple, {:variable, "s"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "new"}}
      ]

      # WHERE pattern that matches s1 and binds s
      pattern = {:bgp, [
        {:triple, {:variable, "s"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "old"}}
      ]}

      assert {:ok, count} = UpdateExecutor.execute_modify(ctx, [], insert_template, pattern)
      # Should insert 1 (s1 with new value)
      assert count == 1

      # Verify: original 2 + 1 new = 3 total
      {:ok, count_after} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count_after == 3
    end
  end

  # ===========================================================================
  # 4.4.5 Error Handling
  # ===========================================================================

  describe "MODIFY error handling" do
    test "handles empty templates", %{ctx: ctx} do
      assert {:ok, 0} = UpdateExecutor.execute_modify(ctx, [], [], nil)
    end

    test "handles non-existent data gracefully", %{ctx: ctx} do
      delete_template = [
        {:quad, {:named_node, "http://example.org/nonexistent"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                :default_graph}
      ]

      insert_template = [
        {:quad, {:named_node, "http://example.org/new"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "value"},
                :default_graph}
      ]

      # Delete fails (not found), insert succeeds
      assert {:ok, 1} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, nil)

      # Verify insertion happened
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 1
    end

    test "handles mixed quad and triple templates", %{ctx: ctx} do
      insert_test_data(ctx)

      delete_template = [
        {:triple, {:named_node, "http://example.org/s1"},
                 {:named_node, "http://example.org/p"},
                 {:literal, :simple, "old"}}
      ]

      insert_template = [
        {:quad, {:named_node, "http://example.org/s3"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "new"},
                :default_graph}
      ]

      assert {:ok, 2} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, nil)

      # Verify results: s2 (unchanged) + s3 (inserted) = 2 quads total (s1 deleted)
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 2
    end
  end

  # ===========================================================================
  # 4.4.6 Atomicity
  # ===========================================================================

  describe "MODIFY atomicity" do
    test "DELETE and INSERT are atomic", %{ctx: ctx} do
      insert_test_data(ctx)

      delete_template = [
        {:quad, {:named_node, "http://example.org/s1"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "old"},
                :default_graph}
      ]

      insert_template = [
        {:quad, {:named_node, "http://example.org/s1"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "new"},
                :default_graph}
      ]

      assert {:ok, 2} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, nil)

      # Verify both operations completed
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 2  # s2 (unchanged) + s1 with new value
    end

    test "DELETE only removes specified quads", %{ctx: ctx} do
      insert_test_data(ctx)

      delete_template = [
        {:quad, {:named_node, "http://example.org/s1"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "old"},
                :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_modify(ctx, delete_template, [], nil)

      # Verify only s1 was deleted, s2 remains
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 1
    end

    test "INSERT adds to existing quads", %{ctx: ctx} do
      insert_test_data(ctx)

      insert_template = [
        {:quad, {:named_node, "http://example.org/s3"},
                {:named_node, "http://example.org/p"},
                {:literal, :simple, "new"},
                :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_modify(ctx, [], insert_template, nil)

      # Verify insertion added to existing
      {:ok, count} = QuadOperations.graph_quad_count(ctx.db, ctx.dict_manager, :default)
      assert count == 3
    end
  end
end
