defmodule TripleStore.TransactionTest do
  use ExUnit.Case, async: false

  alias TripleStore.Transaction
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager, as: DictManager
  alias TripleStore.SPARQL.PlanCache

  @moduletag :transaction

  setup do
    db_path = "/tmp/triple_store_transaction_test_#{System.unique_integer([:positive])}"
    {:ok, db} = NIF.open(db_path)

    {:ok, manager} = DictManager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: DictManager.stop(manager)
      NIF.close(db)
      File.rm_rf!(db_path)
    end)

    %{db: db, manager: manager, db_path: db_path}
  end

  describe "start_link/1 and stop/1" do
    test "starts and stops the transaction manager", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)
      assert Process.alive?(txn)
      assert :ok = Transaction.stop(txn)
      refute Process.alive?(txn)
    end

    test "starts with a name", %{db: db, manager: manager} do
      name = :"test_txn_#{System.unique_integer([:positive])}"
      {:ok, _txn} = Transaction.start_link(db: db, dict_manager: manager, name: name)
      assert Process.whereis(name) != nil
      Transaction.stop(name)
    end

    test "starts with plan cache reference", %{db: db, manager: manager} do
      cache_name = :"test_cache_#{System.unique_integer([:positive])}"
      {:ok, cache} = PlanCache.start_link(name: cache_name)
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager, plan_cache: cache)
      assert Process.alive?(txn)
      Transaction.stop(txn)
      GenServer.stop(cache)
    end

    test "starts with stats callback", %{db: db, manager: manager} do
      callback_called = :ets.new(:callback_test, [:set, :public])
      :ets.insert(callback_called, {:called, false})

      callback = fn ->
        :ets.insert(callback_called, {:called, true})
        :ok
      end

      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager, stats_callback: callback)

      # Insert something to trigger callback
      {:ok, 1} = Transaction.update(txn, "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }")

      [{:called, was_called}] = :ets.lookup(callback_called, :called)
      assert was_called == true

      Transaction.stop(txn)
      :ets.delete(callback_called)
    end
  end

  describe "update/2 - INSERT DATA" do
    test "inserts a single triple", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
        }
      """)

      assert {:ok, 1} = result
      Transaction.stop(txn)
    end

    test "inserts multiple triples", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
          <http://example.org/bob> <http://example.org/name> "Bob" .
          <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
        }
      """)

      assert {:ok, 3} = result
      Transaction.stop(txn)
    end

    test "handles typed literals", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
        }
      """)

      assert {:ok, 1} = result
      Transaction.stop(txn)
    end

    test "handles language-tagged literals", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice"@en .
        }
      """)

      assert {:ok, 1} = result
      Transaction.stop(txn)
    end

    test "returns parse error for invalid SPARQL", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.update(txn, "NOT VALID SPARQL")

      assert {:error, _} = result
      Transaction.stop(txn)
    end
  end

  describe "update/2 - DELETE DATA" do
    test "deletes an existing triple", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # First insert
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
        }
      """)

      # Then delete
      result = Transaction.update(txn, """
        DELETE DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
        }
      """)

      assert {:ok, 1} = result
      Transaction.stop(txn)
    end

    test "returns 0 for non-existent triple (idempotent)", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.update(txn, """
        DELETE DATA {
          <http://example.org/nonexistent> <http://example.org/p> <http://example.org/o> .
        }
      """)

      assert {:ok, 0} = result
      Transaction.stop(txn)
    end
  end

  describe "query/2" do
    test "queries inserted data", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert data
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
        }
      """)

      # Query for it
      result = Transaction.query(txn, """
        SELECT ?name WHERE {
          <http://example.org/alice> <http://example.org/name> ?name .
        }
      """)

      assert {:ok, results} = result
      assert length(results) == 1
      Transaction.stop(txn)
    end

    test "returns empty for non-matching query", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.query(txn, """
        SELECT ?s ?p ?o WHERE {
          ?s ?p ?o .
        }
      """)

      assert {:ok, []} = result
      Transaction.stop(txn)
    end

    test "returns parse error for invalid query", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.query(txn, "NOT VALID SPARQL")

      assert {:error, _} = result
      Transaction.stop(txn)
    end
  end

  describe "insert/2 - direct API" do
    test "inserts triples using RDF.ex terms", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]

      result = Transaction.insert(txn, triples)
      assert {:ok, 1} = result
      Transaction.stop(txn)
    end

    test "inserts multiple triples", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), RDF.literal("v1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.literal("v2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"), RDF.literal("v3")}
      ]

      result = Transaction.insert(txn, triples)
      assert {:ok, 3} = result
      Transaction.stop(txn)
    end

    test "handles empty list", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.insert(txn, [])
      assert {:ok, 0} = result
      Transaction.stop(txn)
    end
  end

  describe "delete/2 - direct API" do
    test "deletes triples using RDF.ex terms", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert first
      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]
      {:ok, 1} = Transaction.insert(txn, triples)

      # Then delete
      result = Transaction.delete(txn, triples)
      assert {:ok, 1} = result
      Transaction.stop(txn)
    end

    test "handles non-existent triples", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      triples = [
        {RDF.iri("http://example.org/nonexistent"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]

      result = Transaction.delete(txn, triples)
      assert {:ok, 0} = result
      Transaction.stop(txn)
    end
  end

  describe "execute_update/2 - pre-parsed AST" do
    test "executes pre-parsed UPDATE AST", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Parse the update
      {:ok, ast} = TripleStore.SPARQL.Parser.parse_update("""
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      result = Transaction.execute_update(txn, ast)
      assert {:ok, 1} = result
      Transaction.stop(txn)
    end
  end

  describe "update_in_progress?/1 and current_snapshot/1" do
    test "returns false when no update is in progress", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      assert Transaction.update_in_progress?(txn) == false
      assert Transaction.current_snapshot(txn) == nil

      Transaction.stop(txn)
    end
  end

  describe "get_context/1" do
    test "returns execution context", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      ctx = Transaction.get_context(txn)
      assert ctx.db == db
      assert ctx.dict_manager == manager

      Transaction.stop(txn)
    end
  end

  describe "plan cache invalidation" do
    test "invalidates cache after successful update", %{db: db, manager: manager} do
      cache_name = :"test_cache_#{System.unique_integer([:positive])}"
      {:ok, _cache_pid} = PlanCache.start_link(name: cache_name)

      # Warm the cache - API is get_or_compute(query, compute_fn, opts)
      PlanCache.get_or_compute("test_query", fn -> {:ok, :dummy_plan} end, name: cache_name)
      stats_before = PlanCache.stats(name: cache_name)
      assert stats_before.size == 1

      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager, plan_cache: cache_name)

      # Insert data (should invalidate cache)
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      stats_after = PlanCache.stats(name: cache_name)
      assert stats_after.size == 0

      Transaction.stop(txn)
      GenServer.stop(cache_name)
    end

    test "does not invalidate cache when no changes made", %{db: db, manager: manager} do
      cache_name = :"test_cache_#{System.unique_integer([:positive])}"
      {:ok, _cache_pid} = PlanCache.start_link(name: cache_name)

      # Warm the cache - API is get_or_compute(query, compute_fn, opts)
      PlanCache.get_or_compute("test_query", fn -> {:ok, :dummy_plan} end, name: cache_name)

      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager, plan_cache: cache_name)

      # Delete non-existent data (no actual changes)
      {:ok, 0} = Transaction.update(txn, """
        DELETE DATA {
          <http://example.org/nonexistent> <http://example.org/p> <http://example.org/o> .
        }
      """)

      stats_after = PlanCache.stats(name: cache_name)
      assert stats_after.size == 1  # Cache should still have the entry

      Transaction.stop(txn)
      GenServer.stop(cache_name)
    end
  end

  describe "serialization" do
    test "serializes concurrent updates", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Launch multiple concurrent updates
      tasks = for i <- 1..5 do
        Task.async(fn ->
          Transaction.update(txn, """
            INSERT DATA {
              <http://example.org/s#{i}> <http://example.org/p> <http://example.org/o#{i}> .
            }
          """)
        end)
      end

      # All should succeed
      results = Task.await_many(tasks, 10_000)
      assert Enum.all?(results, fn {:ok, 1} -> true; _ -> false end)

      # Verify all 5 were inserted
      {:ok, results} = Transaction.query(txn, """
        SELECT ?s ?o WHERE {
          ?s <http://example.org/p> ?o .
        }
      """)
      assert length(results) == 5

      Transaction.stop(txn)
    end
  end

  describe "snapshot isolation" do
    test "creates and releases snapshots during updates", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Before update
      assert Transaction.current_snapshot(txn) == nil

      # Perform update (snapshot is created and released within the call)
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      # After update (snapshot should be released)
      assert Transaction.current_snapshot(txn) == nil

      Transaction.stop(txn)
    end
  end

  describe "error handling" do
    test "handles invalid update gracefully", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.update(txn, "INVALID SPARQL SYNTAX")
      assert {:error, _} = result

      # Transaction manager should still be alive and functional
      assert Process.alive?(txn)

      # Should still be able to do valid updates
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """)

      Transaction.stop(txn)
    end
  end

  describe "timeouts" do
    test "update_timeout/0 returns default timeout", %{db: _db, manager: _manager} do
      assert Transaction.update_timeout() == 300_000
    end

    test "query_timeout/0 returns default timeout", %{db: _db, manager: _manager} do
      assert Transaction.query_timeout() == 120_000
    end

    test "update accepts custom timeout", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      result = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o> .
        }
      """, timeout: 60_000)

      assert {:ok, 1} = result
      Transaction.stop(txn)
    end
  end

  describe "data integrity" do
    test "insert then query returns inserted data", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert
      {:ok, 3} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
          <http://example.org/alice> <http://example.org/age> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
          <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
        }
      """)

      # Query all
      {:ok, results} = Transaction.query(txn, """
        SELECT ?p ?o WHERE {
          <http://example.org/alice> ?p ?o .
        }
      """)

      assert length(results) == 3
      Transaction.stop(txn)
    end

    test "delete removes data from query results", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert
      {:ok, 2} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
          <http://example.org/bob> <http://example.org/name> "Bob" .
        }
      """)

      # Delete one
      {:ok, 1} = Transaction.update(txn, """
        DELETE DATA {
          <http://example.org/alice> <http://example.org/name> "Alice" .
        }
      """)

      # Query - should only find Bob
      {:ok, results} = Transaction.query(txn, """
        SELECT ?name WHERE {
          ?s <http://example.org/name> ?name .
        }
      """)

      assert length(results) == 1
      Transaction.stop(txn)
    end
  end
end
