defmodule TripleStore.SPARQL.UpdateExecutorTest do
  @moduledoc """
  Tests for SPARQL UPDATE operation execution.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.UpdateExecutor

  @moduletag :update_executor

  setup do
    # Create unique temp directory for each test
    test_id = :erlang.unique_integer([:positive])
    db_path = Path.join(System.tmp_dir!(), "update_executor_test_#{test_id}")

    # Clean up any existing directory
    File.rm_rf!(db_path)

    # Open database
    {:ok, db} = NIF.open(db_path)

    # Start dictionary manager
    {:ok, manager} = Manager.start_link(db: db)

    ctx = %{db: db, dict_manager: manager}

    on_exit(fn ->
      # Cleanup - check if process is alive before stopping
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      NIF.close(db)
      File.rm_rf!(db_path)
    end)

    {:ok, ctx: ctx, db: db, manager: manager}
  end

  # ===========================================================================
  # INSERT DATA Tests
  # ===========================================================================

  describe "execute_insert_data/2" do
    test "inserts single triple", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "value"}, :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Verify triple was inserted
      assert {:ok, count} = Index.count(ctx.db, {:var, :var, :var})
      assert count == 1
    end

    test "inserts multiple triples", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s1"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "v1"}, :default_graph},
        {:quad, {:named_node, "http://example.org/s2"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "v2"}, :default_graph},
        {:quad, {:named_node, "http://example.org/s3"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "v3"}, :default_graph}
      ]

      assert {:ok, 3} = UpdateExecutor.execute_insert_data(ctx, quads)
      assert {:ok, 3} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "handles empty quad list", %{ctx: ctx} do
      assert {:ok, 0} = UpdateExecutor.execute_insert_data(ctx, [])
    end

    test "handles typed literals", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
         {:named_node, "http://example.org/age"},
         {:literal, :typed, "42", "http://www.w3.org/2001/XMLSchema#integer"}, :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)
    end

    test "handles language-tagged literals", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
         {:named_node, "http://example.org/label"}, {:literal, :lang, "hello", "en"},
         :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)
    end

    test "handles blank nodes", %{ctx: ctx} do
      quads = [
        {:quad, {:blank_node, "b1"}, {:named_node, "http://example.org/p"},
         {:blank_node, "b2"}, :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)
    end

    test "inserts are idempotent", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "value"}, :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)
      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Should still be just 1 triple (duplicates are no-op)
      assert {:ok, 1} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "rejects too many triples", %{ctx: ctx} do
      # Generate more than max_data_triples
      quads =
        for i <- 1..(UpdateExecutor.max_data_triples() + 1) do
          {:quad, {:named_node, "http://example.org/s#{i}"},
           {:named_node, "http://example.org/p"}, {:literal, :simple, "v"}, :default_graph}
        end

      assert {:error, :too_many_triples} = UpdateExecutor.execute_insert_data(ctx, quads)
    end
  end

  # ===========================================================================
  # DELETE DATA Tests
  # ===========================================================================

  describe "execute_delete_data/2" do
    test "deletes existing triple", %{ctx: ctx} do
      # First insert a triple
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "value"}, :default_graph}
      ]

      {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)
      assert {:ok, 1} = Index.count(ctx.db, {:var, :var, :var})

      # Now delete it
      assert {:ok, 1} = UpdateExecutor.execute_delete_data(ctx, quads)
      assert {:ok, 0} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "deletes multiple triples", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s1"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "v1"}, :default_graph},
        {:quad, {:named_node, "http://example.org/s2"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "v2"}, :default_graph}
      ]

      {:ok, 2} = UpdateExecutor.execute_insert_data(ctx, quads)

      # Delete one of them
      delete_quads = [
        {:quad, {:named_node, "http://example.org/s1"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "v1"}, :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_delete_data(ctx, delete_quads)
      assert {:ok, 1} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "handles empty quad list", %{ctx: ctx} do
      assert {:ok, 0} = UpdateExecutor.execute_delete_data(ctx, [])
    end

    test "deleting non-existent triple is idempotent", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/nonexistent"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "v"}, :default_graph}
      ]

      # Should succeed even though triple doesn't exist
      assert {:ok, 0} = UpdateExecutor.execute_delete_data(ctx, quads)
    end

    test "rejects too many triples", %{ctx: ctx} do
      quads =
        for i <- 1..(UpdateExecutor.max_data_triples() + 1) do
          {:quad, {:named_node, "http://example.org/s#{i}"},
           {:named_node, "http://example.org/p"}, {:literal, :simple, "v"}, :default_graph}
        end

      assert {:error, :too_many_triples} = UpdateExecutor.execute_delete_data(ctx, quads)
    end
  end

  # ===========================================================================
  # Full SPARQL UPDATE Parsing and Execution Tests
  # ===========================================================================

  describe "execute/2 with parsed AST" do
    test "executes INSERT DATA from parsed SPARQL", %{ctx: ctx} do
      sparql = """
      INSERT DATA {
        <http://example.org/alice> <http://example.org/name> "Alice" .
        <http://example.org/alice> <http://example.org/age> "30" .
      }
      """

      {:ok, ast} = Parser.parse_update(sparql)
      assert {:ok, 2} = UpdateExecutor.execute(ctx, ast)
      assert {:ok, 2} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "executes DELETE DATA from parsed SPARQL", %{ctx: ctx} do
      # First insert some data
      insert_sparql = """
      INSERT DATA {
        <http://example.org/alice> <http://example.org/name> "Alice" .
        <http://example.org/bob> <http://example.org/name> "Bob" .
      }
      """

      {:ok, insert_ast} = Parser.parse_update(insert_sparql)
      {:ok, 2} = UpdateExecutor.execute(ctx, insert_ast)

      # Now delete one
      delete_sparql = """
      DELETE DATA {
        <http://example.org/alice> <http://example.org/name> "Alice" .
      }
      """

      {:ok, delete_ast} = Parser.parse_update(delete_sparql)
      assert {:ok, 1} = UpdateExecutor.execute(ctx, delete_ast)
      assert {:ok, 1} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "executes multiple operations in sequence", %{ctx: ctx} do
      # Multiple operations in single update
      sparql = """
      INSERT DATA { <http://example.org/s1> <http://example.org/p> "v1" } ;
      INSERT DATA { <http://example.org/s2> <http://example.org/p> "v2" }
      """

      {:ok, ast} = Parser.parse_update(sparql)
      assert {:ok, total} = UpdateExecutor.execute(ctx, ast)
      assert total == 2
      assert {:ok, 2} = Index.count(ctx.db, {:var, :var, :var})
    end
  end

  # ===========================================================================
  # DELETE WHERE Tests
  # ===========================================================================

  describe "execute_delete_where/2" do
    test "deletes triples matching pattern", %{ctx: ctx} do
      # Insert test data
      insert_sparql = """
      INSERT DATA {
        <http://example.org/alice> <http://example.org/name> "Alice" .
        <http://example.org/bob> <http://example.org/name> "Bob" .
        <http://example.org/alice> <http://example.org/age> "30" .
      }
      """

      {:ok, insert_ast} = Parser.parse_update(insert_sparql)
      {:ok, 3} = UpdateExecutor.execute(ctx, insert_ast)

      # Delete all :name triples using DELETE WHERE
      pattern = {:bgp,
       [
         {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"},
          {:variable, "o"}}
       ]}

      assert {:ok, _count} = UpdateExecutor.execute_delete_where(ctx, pattern)

      # Only the :age triple should remain
      assert {:ok, 1} = Index.count(ctx.db, {:var, :var, :var})
    end
  end

  # ===========================================================================
  # INSERT WHERE Tests
  # ===========================================================================

  describe "execute_insert_where/3" do
    test "inserts triples based on pattern matches", %{ctx: ctx} do
      # Insert test data
      insert_sparql = """
      INSERT DATA {
        <http://example.org/alice> <http://example.org/name> "Alice" .
        <http://example.org/bob> <http://example.org/name> "Bob" .
      }
      """

      {:ok, insert_ast} = Parser.parse_update(insert_sparql)
      {:ok, 2} = UpdateExecutor.execute(ctx, insert_ast)

      # Copy all :name values to :label
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/label"},
         {:variable, "name"}}
      ]

      pattern = {:bgp,
       [
         {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"},
          {:variable, "name"}}
       ]}

      assert {:ok, 2} = UpdateExecutor.execute_insert_where(ctx, template, pattern)

      # Should now have 4 triples (2 name + 2 label)
      assert {:ok, 4} = Index.count(ctx.db, {:var, :var, :var})
    end
  end

  # ===========================================================================
  # MODIFY (DELETE/INSERT WHERE) Tests
  # ===========================================================================

  describe "execute_modify/4" do
    test "performs combined delete and insert", %{ctx: ctx} do
      # Insert test data
      insert_sparql = """
      INSERT DATA {
        <http://example.org/alice> <http://example.org/status> "pending" .
      }
      """

      {:ok, insert_ast} = Parser.parse_update(insert_sparql)
      {:ok, 1} = UpdateExecutor.execute(ctx, insert_ast)

      # Change status from "pending" to "active"
      # This requires both delete (old value) and insert (new value)
      delete_template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/status"},
         {:literal, :simple, "pending"}}
      ]

      insert_template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/status"},
         {:literal, :simple, "active"}}
      ]

      pattern = {:bgp,
       [
         {:triple, {:variable, "s"}, {:named_node, "http://example.org/status"},
          {:literal, :simple, "pending"}}
       ]}

      assert {:ok, _count} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, pattern)

      # Should still have 1 triple, but with different value
      assert {:ok, 1} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "handles empty delete template (insert only)", %{ctx: ctx} do
      pattern = {:bgp, []}

      insert_template = [
        {:triple, {:named_node, "http://example.org/s"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "value"}}
      ]

      # This inserts without deleting anything
      assert {:ok, 1} = UpdateExecutor.execute_modify(ctx, [], insert_template, pattern)
      assert {:ok, 1} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "handles empty insert template (delete only)", %{ctx: ctx} do
      # Insert test data
      insert_sparql = """
      INSERT DATA {
        <http://example.org/s> <http://example.org/p> "value" .
      }
      """

      {:ok, insert_ast} = Parser.parse_update(insert_sparql)
      {:ok, 1} = UpdateExecutor.execute(ctx, insert_ast)

      # Delete without inserting
      delete_template = [
        {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      ]

      pattern = {:bgp,
       [
         {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
       ]}

      assert {:ok, _count} = UpdateExecutor.execute_modify(ctx, delete_template, [], pattern)
      assert {:ok, 0} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "returns 0 for empty templates", %{ctx: ctx} do
      assert {:ok, 0} = UpdateExecutor.execute_modify(ctx, [], [], nil)
    end
  end

  # ===========================================================================
  # CLEAR Tests
  # ===========================================================================

  describe "execute_clear/2" do
    test "clears all triples", %{ctx: ctx} do
      # Insert test data
      insert_sparql = """
      INSERT DATA {
        <http://example.org/s1> <http://example.org/p> "v1" .
        <http://example.org/s2> <http://example.org/p> "v2" .
        <http://example.org/s3> <http://example.org/p> "v3" .
      }
      """

      {:ok, insert_ast} = Parser.parse_update(insert_sparql)
      {:ok, 3} = UpdateExecutor.execute(ctx, insert_ast)

      assert {:ok, 3} = UpdateExecutor.execute_clear(ctx, target: :all)
      assert {:ok, 0} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "clears default graph", %{ctx: ctx} do
      insert_sparql = """
      INSERT DATA {
        <http://example.org/s> <http://example.org/p> "v" .
      }
      """

      {:ok, insert_ast} = Parser.parse_update(insert_sparql)
      {:ok, 1} = UpdateExecutor.execute(ctx, insert_ast)

      assert {:ok, 1} = UpdateExecutor.execute_clear(ctx, target: :default)
      assert {:ok, 0} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "handles empty database", %{ctx: ctx} do
      assert {:ok, 0} = UpdateExecutor.execute_clear(ctx, target: :all)
    end
  end

  # ===========================================================================
  # Error Handling Tests
  # ===========================================================================

  describe "error handling" do
    test "returns error for invalid AST", %{ctx: ctx} do
      assert {:error, :invalid_update_ast} = UpdateExecutor.execute(ctx, {:not_update, []})
    end

    test "handles unsupported operations gracefully", %{ctx: ctx} do
      # LOAD is not implemented
      assert {:error, :load_not_implemented} =
               UpdateExecutor.execute_operation(ctx, {:load, []})
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles special characters in literals", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
         {:named_node, "http://example.org/p"},
         {:literal, :simple, "Hello \"World\"!\nNew line\ttab"}, :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)
    end

    test "handles unicode in values", %{ctx: ctx} do
      quads = [
        {:quad, {:named_node, "http://example.org/s"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "Hello "}, :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)
    end

    test "handles long URIs", %{ctx: ctx} do
      long_uri = "http://example.org/" <> String.duplicate("a", 1000)

      quads = [
        {:quad, {:named_node, long_uri}, {:named_node, "http://example.org/p"},
         {:literal, :simple, "v"}, :default_graph}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, quads)
    end

    test "triple format works same as quad format", %{ctx: ctx} do
      # Using triple format (without graph)
      triples = [
        {:triple, {:named_node, "http://example.org/s"},
         {:named_node, "http://example.org/p"}, {:literal, :simple, "value"}}
      ]

      assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, triples)
    end
  end

  # ===========================================================================
  # Configuration Tests
  # ===========================================================================

  describe "configuration" do
    test "max_data_triples returns configured limit" do
      assert UpdateExecutor.max_data_triples() == 100_000
    end

    test "max_pattern_matches returns configured limit" do
      assert UpdateExecutor.max_pattern_matches() == 1_000_000
    end
  end

  # ===========================================================================
  # Integration Tests
  # ===========================================================================

  describe "integration" do
    test "full update workflow with multiple operations", %{ctx: ctx} do
      # Insert initial data
      insert1 = """
      INSERT DATA {
        <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
        <http://example.org/alice> <http://example.org/name> "Alice" .
        <http://example.org/bob> <http://example.org/name> "Bob" .
      }
      """

      {:ok, ast1} = Parser.parse_update(insert1)
      {:ok, 3} = UpdateExecutor.execute(ctx, ast1)

      # Add more data
      insert2 = """
      INSERT DATA {
        <http://example.org/charlie> <http://example.org/name> "Charlie" .
        <http://example.org/alice> <http://example.org/knows> <http://example.org/charlie> .
      }
      """

      {:ok, ast2} = Parser.parse_update(insert2)
      {:ok, 2} = UpdateExecutor.execute(ctx, ast2)

      # Delete some data
      delete1 = """
      DELETE DATA {
        <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
      }
      """

      {:ok, ast3} = Parser.parse_update(delete1)
      {:ok, 1} = UpdateExecutor.execute(ctx, ast3)

      # Final count should be 4
      assert {:ok, 4} = Index.count(ctx.db, {:var, :var, :var})
    end

    test "preserves data integrity across operations", %{ctx: ctx} do
      # Insert data
      insert = """
      INSERT DATA {
        <http://example.org/s> <http://example.org/p> "original" .
      }
      """

      {:ok, insert_ast} = Parser.parse_update(insert)
      {:ok, 1} = UpdateExecutor.execute(ctx, insert_ast)

      # Delete data
      delete = """
      DELETE DATA {
        <http://example.org/s> <http://example.org/p> "original" .
      }
      """

      {:ok, delete_ast} = Parser.parse_update(delete)
      {:ok, 1} = UpdateExecutor.execute(ctx, delete_ast)

      # Insert new data with same subject
      insert2 = """
      INSERT DATA {
        <http://example.org/s> <http://example.org/p> "new" .
      }
      """

      {:ok, insert2_ast} = Parser.parse_update(insert2)
      {:ok, 1} = UpdateExecutor.execute(ctx, insert2_ast)

      # Verify correct state
      assert {:ok, 1} = Index.count(ctx.db, {:var, :var, :var})
    end
  end
end
