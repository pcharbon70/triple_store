defmodule TripleStore.SPARQL.UpdateIntegrationTest do
  @moduledoc """
  Integration tests for SPARQL UPDATE operations.

  These tests verify the complete UPDATE workflow including:
  - Transaction isolation
  - Concurrent read consistency during updates
  - Rollback semantics on failure
  - Plan cache invalidation

  Covers Task 3.3.4 requirements from Phase 3 planning.
  """
  use ExUnit.Case, async: false

  alias TripleStore.Transaction
  alias TripleStore.Update
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager, as: DictManager
  alias TripleStore.SPARQL.Query
  alias TripleStore.SPARQL.PlanCache
  alias TripleStore.SPARQL.UpdateExecutor

  @moduletag :update_integration

  setup do
    db_path = "/tmp/triple_store_update_integration_#{System.unique_integer([:positive])}"
    {:ok, db} = NIF.open(db_path)

    {:ok, manager} = DictManager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: DictManager.stop(manager)
      NIF.close(db)
      File.rm_rf!(db_path)
    end)

    %{db: db, manager: manager, db_path: db_path}
  end

  # ===========================================================================
  # INSERT DATA Tests
  # ===========================================================================

  describe "INSERT DATA adds triples" do
    test "single triple is queryable after insert", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
        }
      """)

      # Verify queryable
      {:ok, results} = Transaction.query(txn, """
        SELECT ?name WHERE {
          <http://example.org/alice> <http://example.org/name> ?name .
        }
      """)

      assert length(results) == 1
      Transaction.stop(txn)
    end

    test "multiple triples are all queryable after insert", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert multiple
      {:ok, 3} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
          <http://example.org/bob> <http://example.org/name> "Bob" .
          <http://example.org/charlie> <http://example.org/name> "Charlie" .
        }
      """)

      # Verify all queryable
      {:ok, results} = Transaction.query(txn, """
        SELECT ?s ?name WHERE {
          ?s <http://example.org/name> ?name .
        }
      """)

      assert length(results) == 3
      Transaction.stop(txn)
    end
  end

  # ===========================================================================
  # DELETE DATA Tests
  # ===========================================================================

  describe "DELETE DATA removes triples" do
    test "deleted triple is not queryable", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
        }
      """)

      # Verify exists
      {:ok, results1} = Transaction.query(txn, """
        SELECT ?name WHERE {
          <http://example.org/alice> <http://example.org/name> ?name .
        }
      """)
      assert length(results1) == 1

      # Delete
      {:ok, 1} = Transaction.update(txn, """
        DELETE DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
        }
      """)

      # Verify not queryable
      {:ok, results2} = Transaction.query(txn, """
        SELECT ?name WHERE {
          <http://example.org/alice> <http://example.org/name> ?name .
        }
      """)
      assert length(results2) == 0

      Transaction.stop(txn)
    end

    test "only specified triples are deleted", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert multiple
      {:ok, 2} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
          <http://example.org/bob> <http://example.org/name> "Bob" .
        }
      """)

      # Delete only Alice
      {:ok, 1} = Transaction.update(txn, """
        DELETE DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
        }
      """)

      # Verify Bob still exists
      {:ok, results} = Transaction.query(txn, """
        SELECT ?name WHERE {
          ?s <http://example.org/name> ?name .
        }
      """)
      assert length(results) == 1

      Transaction.stop(txn)
    end
  end

  # ===========================================================================
  # DELETE WHERE Tests
  # ===========================================================================

  describe "DELETE WHERE removes matching triples" do
    test "pattern-based deletion removes all matches", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert triples
      {:ok, 3} = Update.update(ctx, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/type> "person" .
          <http://example.org/bob> <http://example.org/type> "person" .
          <http://example.org/company> <http://example.org/type> "organization" .
        }
      """)

      # Delete all persons using DELETE WHERE via pattern
      pattern = {:bgp,
        [
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/type"},
           {:literal, :simple, "person"}}
        ]}

      {:ok, _deleted_count} = UpdateExecutor.execute_delete_where(ctx, pattern)

      # Verify only organization remains
      {:ok, prepared} = Query.prepare("""
        SELECT ?s WHERE {
          ?s <http://example.org/type> ?type .
        }
      """)
      {:ok, results} = Query.execute(ctx, prepared)
      assert length(results) == 1
    end
  end

  # ===========================================================================
  # INSERT WHERE Tests
  # ===========================================================================

  describe "INSERT WHERE adds templated triples" do
    test "template instantiation creates new triples", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert base data
      {:ok, 2} = Update.update(ctx, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
          <http://example.org/bob> <http://example.org/name> "Bob" .
        }
      """)

      # Use INSERT WHERE to add derived triples via direct API
      template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/hasName"},
         {:literal, :simple, "true"}}
      ]

      pattern = {:bgp,
        [
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/name"},
           {:variable, "name"}}
        ]}

      {:ok, insert_count} = UpdateExecutor.execute_insert_where(ctx, template, pattern)
      assert insert_count == 2

      # Verify derived triples exist
      {:ok, prepared} = Query.prepare("""
        SELECT ?s WHERE {
          ?s <http://example.org/hasName> "true" .
        }
      """)
      {:ok, results} = Query.execute(ctx, prepared)
      assert length(results) == 2
    end
  end

  # ===========================================================================
  # MODIFY Tests
  # ===========================================================================

  describe "MODIFY combines delete and insert" do
    test "combined delete and insert in single operation", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert initial data
      {:ok, 2} = Update.update(ctx, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/status> "active" .
          <http://example.org/bob> <http://example.org/status> "active" .
        }
      """)

      # Use MODIFY to change status from active to archived via direct API
      delete_template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/status"},
         {:literal, :simple, "active"}}
      ]

      insert_template = [
        {:triple, {:variable, "s"}, {:named_node, "http://example.org/status"},
         {:literal, :simple, "archived"}}
      ]

      pattern = {:bgp,
        [
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/status"},
           {:literal, :simple, "active"}}
        ]}

      {:ok, _count} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, pattern)

      # Verify no active status remains
      {:ok, prepared1} = Query.prepare("""
        SELECT ?s WHERE {
          ?s <http://example.org/status> "active" .
        }
      """)
      {:ok, active_results} = Query.execute(ctx, prepared1)
      assert length(active_results) == 0

      # Verify archived status exists
      {:ok, prepared2} = Query.prepare("""
        SELECT ?s WHERE {
          ?s <http://example.org/status> "archived" .
        }
      """)
      {:ok, archived_results} = Query.execute(ctx, prepared2)
      assert length(archived_results) == 2
    end
  end

  # ===========================================================================
  # Concurrent Read Consistency Tests
  # ===========================================================================

  describe "concurrent reads see consistent snapshot during update" do
    test "concurrent queries during updates are serialized correctly", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert initial data
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/counter> <http://example.org/value> "0" .
        }
      """)

      # Run concurrent updates and reads
      tasks = for i <- 1..5 do
        Task.async(fn ->
          # Each task does an update
          Transaction.update(txn, """
            INSERT DATA {
              <http://example.org/item#{i}> <http://example.org/seq> "#{i}" .
            }
          """)
        end)
      end

      # Wait for all updates
      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, fn {:ok, 1} -> true; _ -> false end)

      # Verify all 6 triples exist (1 counter + 5 items)
      {:ok, query_results} = Transaction.query(txn, """
        SELECT ?s ?p ?o WHERE {
          ?s ?p ?o .
        }
      """)
      assert length(query_results) == 6

      Transaction.stop(txn)
    end

    test "read during write sees consistent state", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert initial data
      {:ok, 10} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s1> <http://example.org/p> "v1" .
          <http://example.org/s2> <http://example.org/p> "v2" .
          <http://example.org/s3> <http://example.org/p> "v3" .
          <http://example.org/s4> <http://example.org/p> "v4" .
          <http://example.org/s5> <http://example.org/p> "v5" .
          <http://example.org/s6> <http://example.org/p> "v6" .
          <http://example.org/s7> <http://example.org/p> "v7" .
          <http://example.org/s8> <http://example.org/p> "v8" .
          <http://example.org/s9> <http://example.org/p> "v9" .
          <http://example.org/s10> <http://example.org/p> "v10" .
        }
      """)

      # Launch concurrent read and write
      read_task = Task.async(fn ->
        Transaction.query(txn, """
          SELECT ?s ?o WHERE {
            ?s <http://example.org/p> ?o .
          }
        """)
      end)

      write_task = Task.async(fn ->
        Transaction.update(txn, """
          INSERT DATA {
            <http://example.org/s11> <http://example.org/p> "v11" .
          }
        """)
      end)

      {:ok, read_results} = Task.await(read_task)
      {:ok, 1} = Task.await(write_task)

      # Read should see either 10 or 11 triples (consistent snapshot)
      assert length(read_results) in [10, 11]

      Transaction.stop(txn)
    end
  end

  # ===========================================================================
  # Plan Cache Invalidation Tests
  # ===========================================================================

  describe "plan cache invalidated after update" do
    test "cache is invalidated after INSERT DATA", %{db: db, manager: manager} do
      cache_name = :"cache_#{System.unique_integer([:positive])}"
      {:ok, _} = PlanCache.start_link(name: cache_name)

      # Warm the cache
      PlanCache.get_or_compute("test_query", fn -> {:ok, :cached_plan} end, name: cache_name)
      assert PlanCache.stats(name: cache_name).size == 1

      # Start transaction with cache
      {:ok, txn} = Transaction.start_link(
        db: db,
        dict_manager: manager,
        plan_cache: cache_name
      )

      # Perform update
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      # Cache should be invalidated
      assert PlanCache.stats(name: cache_name).size == 0

      Transaction.stop(txn)
      GenServer.stop(cache_name)
    end

    test "cache is invalidated after DELETE DATA", %{db: db, manager: manager} do
      cache_name = :"cache_#{System.unique_integer([:positive])}"
      {:ok, _} = PlanCache.start_link(name: cache_name)

      # Warm the cache
      PlanCache.get_or_compute("test_query", fn -> {:ok, :cached_plan} end, name: cache_name)
      assert PlanCache.stats(name: cache_name).size == 1

      {:ok, txn} = Transaction.start_link(
        db: db,
        dict_manager: manager,
        plan_cache: cache_name
      )

      # Insert then delete to trigger cache invalidation
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      # Re-warm cache
      PlanCache.get_or_compute("test_query2", fn -> {:ok, :cached_plan2} end, name: cache_name)
      assert PlanCache.stats(name: cache_name).size == 1

      # Delete
      {:ok, 1} = Transaction.update(txn, """
        DELETE DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      # Cache should be invalidated again
      assert PlanCache.stats(name: cache_name).size == 0

      Transaction.stop(txn)
      GenServer.stop(cache_name)
    end
  end

  # ===========================================================================
  # Rollback / Failure Tests
  # ===========================================================================

  describe "update failure leaves database unchanged" do
    test "parse error leaves database unchanged", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert initial data
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      # Verify initial state
      {:ok, results1} = Transaction.query(txn, """
        SELECT ?s WHERE { ?s ?p ?o }
      """)
      assert length(results1) == 1

      # Attempt invalid update
      result = Transaction.update(txn, "INVALID SPARQL SYNTAX !!!@#$")
      assert {:error, _} = result

      # Database should be unchanged
      {:ok, results2} = Transaction.query(txn, """
        SELECT ?s WHERE { ?s ?p ?o }
      """)
      assert length(results2) == 1

      Transaction.stop(txn)
    end

    test "transaction manager remains functional after error", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Multiple failed operations
      for _ <- 1..3 do
        result = Transaction.update(txn, "INVALID")
        assert {:error, _} = result
      end

      # Should still be able to perform valid operations
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      {:ok, results} = Transaction.query(txn, """
        SELECT ?s WHERE { ?s ?p ?o }
      """)
      assert length(results) == 1

      Transaction.stop(txn)
    end

    test "WriteBatch atomicity - partial failure does not commit", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert some initial data
      {:ok, 1} = Update.insert(ctx, [
        {RDF.iri("http://example.org/existing"),
         RDF.iri("http://example.org/p"),
         RDF.literal("value")}
      ])

      # Count initial triples
      {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s ?p ?o }")
      {:ok, results_before} = Query.execute(ctx, prepared)
      count_before = length(results_before)

      # Attempt an operation that will fail with invalid AST
      result = UpdateExecutor.execute(ctx, {:invalid_operation, []})
      assert {:error, _} = result

      # Count should be unchanged
      {:ok, results_after} = Query.execute(ctx, prepared)
      assert length(results_after) == count_before
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "empty update returns 0 count", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Delete non-existent triple
      {:ok, 0} = Transaction.update(txn, """
        DELETE DATA {
          <http://example.org/nonexistent> <http://example.org/p> <http://example.org/o> .
        }
      """)

      Transaction.stop(txn)
    end

    test "duplicate inserts are idempotent at storage level", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert same triple twice - both return {:ok, 1} because that's the count
      # of triples in the request, but storage is idempotent
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      # Only one triple should exist (storage is idempotent)
      {:ok, results} = Transaction.query(txn, """
        SELECT ?s WHERE { ?s ?p ?o }
      """)
      assert length(results) == 1

      Transaction.stop(txn)
    end
  end
end
