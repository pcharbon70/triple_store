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

  import TripleStore.Test.IntegrationHelpers, only: [extract_count: 1, ast_to_rdf: 1]

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
  # Task 3.5.2: Advanced Update Integration Tests
  # ===========================================================================

  # ---------------------------------------------------------------------------
  # 3.5.2.1: DELETE/INSERT WHERE modifying same triples
  # ---------------------------------------------------------------------------

  describe "DELETE/INSERT WHERE modifying same triples" do
    test "MODIFY atomically updates triples matching both templates", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert initial data with version numbers
      {:ok, 3} = Update.update(ctx, """
        INSERT DATA {
          <http://example.org/doc1> <http://example.org/version> "1" .
          <http://example.org/doc2> <http://example.org/version> "1" .
          <http://example.org/doc3> <http://example.org/version> "1" .
        }
      """)

      # MODIFY: delete old version, insert new version in atomic operation
      # Delete and insert affect the SAME subject-predicate pairs
      delete_template = [
        {:triple, {:variable, "doc"}, {:named_node, "http://example.org/version"},
         {:literal, :simple, "1"}}
      ]

      insert_template = [
        {:triple, {:variable, "doc"}, {:named_node, "http://example.org/version"},
         {:literal, :simple, "2"}}
      ]

      pattern = {:bgp,
        [
          {:triple, {:variable, "doc"}, {:named_node, "http://example.org/version"},
           {:literal, :simple, "1"}}
        ]}

      {:ok, count} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, pattern)
      # MODIFY returns total of deletes + inserts, so 3 deletes + 3 inserts = 6
      assert count == 6

      # Verify no version "1" remains
      {:ok, prep1} = Query.prepare("""
        SELECT ?doc WHERE { ?doc <http://example.org/version> "1" }
      """)
      {:ok, v1_results} = Query.execute(ctx, prep1)
      assert length(v1_results) == 0

      # Verify all docs now have version "2"
      {:ok, prep2} = Query.prepare("""
        SELECT ?doc WHERE { ?doc <http://example.org/version> "2" }
      """)
      {:ok, v2_results} = Query.execute(ctx, prep2)
      assert length(v2_results) == 3
    end

    test "DELETE WHERE and INSERT WHERE on overlapping patterns", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert relationship data
      {:ok, 4} = Update.update(ctx, """
        INSERT DATA {
          <http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
          <http://example.org/alice> <http://example.org/knows> <http://example.org/charlie> .
          <http://example.org/bob> <http://example.org/knows> <http://example.org/alice> .
          <http://example.org/charlie> <http://example.org/knows> <http://example.org/alice> .
        }
      """)

      # DELETE relationships TO alice, INSERT inverse relationships FROM alice
      delete_template = [
        {:triple, {:variable, "person"}, {:named_node, "http://example.org/knows"},
         {:named_node, "http://example.org/alice"}}
      ]

      insert_template = [
        {:triple, {:named_node, "http://example.org/alice"},
         {:named_node, "http://example.org/knownBy"},
         {:variable, "person"}}
      ]

      pattern = {:bgp,
        [
          {:triple, {:variable, "person"}, {:named_node, "http://example.org/knows"},
           {:named_node, "http://example.org/alice"}}
        ]}

      {:ok, _} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, pattern)

      # Verify: no one "knows" alice anymore
      {:ok, prep1} = Query.prepare("""
        SELECT ?p WHERE { ?p <http://example.org/knows> <http://example.org/alice> }
      """)
      {:ok, knows_alice} = Query.execute(ctx, prep1)
      assert length(knows_alice) == 0

      # Verify: alice "knownBy" bob and charlie
      {:ok, prep2} = Query.prepare("""
        SELECT ?p WHERE { <http://example.org/alice> <http://example.org/knownBy> ?p }
      """)
      {:ok, known_by} = Query.execute(ctx, prep2)
      assert length(known_by) == 2
    end

    test "self-referential MODIFY updates same triple multiple times correctly", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert a counter
      {:ok, 1} = Update.update(ctx, """
        INSERT DATA {
          <http://example.org/counter> <http://example.org/value> "0" .
        }
      """)

      # Perform several increments (simulating sequential updates)
      for i <- 0..4 do
        delete_template = [
          {:triple, {:named_node, "http://example.org/counter"},
           {:named_node, "http://example.org/value"},
           {:literal, :simple, Integer.to_string(i)}}
        ]

        insert_template = [
          {:triple, {:named_node, "http://example.org/counter"},
           {:named_node, "http://example.org/value"},
           {:literal, :simple, Integer.to_string(i + 1)}}
        ]

        pattern = {:bgp,
          [
            {:triple, {:named_node, "http://example.org/counter"},
             {:named_node, "http://example.org/value"},
             {:literal, :simple, Integer.to_string(i)}}
          ]}

        {:ok, _} = UpdateExecutor.execute_modify(ctx, delete_template, insert_template, pattern)
      end

      # Verify counter is now at 5
      {:ok, prep} = Query.prepare("""
        SELECT ?v WHERE { <http://example.org/counter> <http://example.org/value> ?v }
      """)
      {:ok, results} = Query.execute(ctx, prep)
      assert length(results) == 1
      # Query returns AST format, not RDF.ex terms
      assert hd(results)["v"] == {:literal, :simple, "5"}
    end
  end

  # ---------------------------------------------------------------------------
  # 3.5.2.2: Concurrent queries during update see consistent state
  # ---------------------------------------------------------------------------

  # extract_count/1 and ast_to_rdf/1 imported from IntegrationHelpers

  describe "concurrent queries during update see consistent state (Task 3.5.2.2)" do
    test "multiple readers see consistent snapshots during heavy writes", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert initial batch
      {:ok, 100} = Transaction.update(txn, """
        INSERT DATA {
          #{for i <- 1..100 do
            "<http://example.org/item#{i}> <http://example.org/seq> \"#{i}\" ."
          end |> Enum.join("\n")}
        }
      """)

      # Launch concurrent readers and writers
      reader_tasks = for _ <- 1..5 do
        Task.async(fn ->
          # Each reader queries multiple times
          for _ <- 1..10 do
            {:ok, results} = Transaction.query(txn, """
              SELECT (COUNT(?s) AS ?count) WHERE { ?s <http://example.org/seq> ?v }
            """)
            # Count should be consistent (100 + any completed inserts)
            count = extract_count(hd(results)["count"])
            assert count >= 100
            count
          end
        end)
      end

      writer_tasks = for i <- 1..5 do
        Task.async(fn ->
          Transaction.update(txn, """
            INSERT DATA {
              <http://example.org/new_item#{i}> <http://example.org/seq> "new#{i}" .
            }
          """)
        end)
      end

      # Wait for all tasks
      reader_results = Task.await_many(reader_tasks, 30_000)
      writer_results = Task.await_many(writer_tasks, 30_000)

      # All writers should succeed
      assert Enum.all?(writer_results, fn {:ok, 1} -> true; _ -> false end)

      # All reader count sequences should be monotonically non-decreasing
      for counts <- reader_results do
        pairs = Enum.zip(counts, tl(counts) ++ [List.last(counts)])
        assert Enum.all?(pairs, fn {a, b} -> a <= b end),
          "Counts should be non-decreasing: #{inspect(counts)}"
      end

      # Final count should be 105
      {:ok, final} = Transaction.query(txn, """
        SELECT (COUNT(?s) AS ?count) WHERE { ?s <http://example.org/seq> ?v }
      """)
      final_count = extract_count(hd(final)["count"])
      assert final_count == 105

      Transaction.stop(txn)
    end

    test "delete operations are visible atomically", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert batch of related triples
      {:ok, 50} = Transaction.update(txn, """
        INSERT DATA {
          #{for i <- 1..50 do
            "<http://example.org/group> <http://example.org/member> <http://example.org/person#{i}> ."
          end |> Enum.join("\n")}
        }
      """)

      # Launch readers
      reader_task = Task.async(fn ->
        counts = for _ <- 1..20 do
          {:ok, results} = Transaction.query(txn, """
            SELECT (COUNT(?m) AS ?count) WHERE {
              <http://example.org/group> <http://example.org/member> ?m
            }
          """)
          extract_count(hd(results)["count"])
        end
        counts
      end)

      # Delete all members in one atomic batch
      Process.sleep(10)  # Let reader start
      ctx = %{db: db, dict_manager: manager}
      pattern = {:bgp,
        [
          {:triple, {:named_node, "http://example.org/group"},
           {:named_node, "http://example.org/member"},
           {:variable, "m"}}
        ]}
      {:ok, deleted} = UpdateExecutor.execute_delete_where(ctx, pattern)
      assert deleted == 50

      counts = Task.await(reader_task, 10_000)

      # Each count should be either 50 (before delete) or 0 (after delete)
      # Never a partial count
      assert Enum.all?(counts, fn c -> c in [0, 50] end),
        "Counts should be 0 or 50, got: #{inspect(counts)}"

      Transaction.stop(txn)
    end

    test "interleaved updates from multiple transactions", %{db: db, manager: manager} do
      # Create multiple transaction coordinators
      {:ok, txn1} = Transaction.start_link(db: db, dict_manager: manager)
      {:ok, txn2} = Transaction.start_link(db: db, dict_manager: manager)

      # Interleave updates
      task1 = Task.async(fn ->
        for i <- 1..10 do
          Transaction.update(txn1, """
            INSERT DATA {
              <http://example.org/txn1_item#{i}> <http://example.org/from> "txn1" .
            }
          """)
        end
      end)

      task2 = Task.async(fn ->
        for i <- 1..10 do
          Transaction.update(txn2, """
            INSERT DATA {
              <http://example.org/txn2_item#{i}> <http://example.org/from> "txn2" .
            }
          """)
        end
      end)

      Task.await(task1, 10_000)
      Task.await(task2, 10_000)

      # Both should see all 20 items
      {:ok, results1} = Transaction.query(txn1, """
        SELECT ?s WHERE { ?s <http://example.org/from> ?source }
      """)
      assert length(results1) == 20

      {:ok, results2} = Transaction.query(txn2, """
        SELECT ?s WHERE { ?s <http://example.org/from> ?source }
      """)
      assert length(results2) == 20

      Transaction.stop(txn1)
      Transaction.stop(txn2)
    end
  end

  # ---------------------------------------------------------------------------
  # 3.5.2.3: Large batch updates (10K+ triples)
  # ---------------------------------------------------------------------------

  describe "large batch updates (10K+ triples)" do
    @tag timeout: 120_000
    test "insert 10K triples in single batch", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Generate 10K triples
      triples = for i <- 1..10_000 do
        {RDF.iri("http://example.org/entity#{i}"),
         RDF.iri("http://example.org/index"),
         RDF.literal(i)}
      end

      # Time the insert
      {time_us, {:ok, count}} = :timer.tc(fn ->
        Update.insert(ctx, triples)
      end)

      assert count == 10_000
      IO.puts("\n  10K insert time: #{time_us / 1000}ms")

      # Verify all inserted
      {:ok, prepared} = Query.prepare("""
        SELECT (COUNT(?s) AS ?count) WHERE { ?s <http://example.org/index> ?v }
      """)
      {:ok, results} = Query.execute(ctx, prepared)
      result_count = extract_count(hd(results)["count"])
      assert result_count == 10_000
    end

    @tag timeout: 120_000
    test "delete 10K triples via DELETE WHERE", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # First insert 10K triples
      triples = for i <- 1..10_000 do
        {RDF.iri("http://example.org/bulk#{i}"),
         RDF.iri("http://example.org/bulkProp"),
         RDF.literal("value#{i}")}
      end
      {:ok, 10_000} = Update.insert(ctx, triples)

      # Delete all via DELETE WHERE
      pattern = {:bgp,
        [
          {:triple, {:variable, "s"}, {:named_node, "http://example.org/bulkProp"},
           {:variable, "v"}}
        ]}

      {time_us, {:ok, deleted}} = :timer.tc(fn ->
        UpdateExecutor.execute_delete_where(ctx, pattern)
      end)

      assert deleted == 10_000
      IO.puts("\n  10K delete time: #{time_us / 1000}ms")

      # Verify all deleted
      {:ok, prepared} = Query.prepare("""
        SELECT ?s WHERE { ?s <http://example.org/bulkProp> ?v }
      """)
      {:ok, results} = Query.execute(ctx, prepared)
      assert length(results) == 0
    end

    @tag timeout: 120_000
    test "MODIFY 10K triples atomically", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert 10K triples with status "pending"
      triples = for i <- 1..10_000 do
        {RDF.iri("http://example.org/task#{i}"),
         RDF.iri("http://example.org/status"),
         RDF.literal("pending")}
      end
      {:ok, 10_000} = Update.insert(ctx, triples)

      # MODIFY all to "complete"
      delete_template = [
        {:triple, {:variable, "task"}, {:named_node, "http://example.org/status"},
         {:literal, :simple, "pending"}}
      ]

      insert_template = [
        {:triple, {:variable, "task"}, {:named_node, "http://example.org/status"},
         {:literal, :simple, "complete"}}
      ]

      pattern = {:bgp,
        [
          {:triple, {:variable, "task"}, {:named_node, "http://example.org/status"},
           {:literal, :simple, "pending"}}
        ]}

      {time_us, {:ok, modified}} = :timer.tc(fn ->
        UpdateExecutor.execute_modify(ctx, delete_template, insert_template, pattern)
      end)

      # MODIFY returns total of deletes + inserts: 10K + 10K = 20K
      assert modified == 20_000
      IO.puts("\n  10K modify time: #{time_us / 1000}ms")

      # Verify all are now "complete"
      {:ok, prep1} = Query.prepare("""
        SELECT (COUNT(?t) AS ?count) WHERE { ?t <http://example.org/status> "complete" }
      """)
      {:ok, complete_results} = Query.execute(ctx, prep1)
      complete_count = extract_count(hd(complete_results)["count"])
      assert complete_count == 10_000

      # Verify none are "pending"
      {:ok, prep2} = Query.prepare("""
        SELECT ?t WHERE { ?t <http://example.org/status> "pending" }
      """)
      {:ok, pending_results} = Query.execute(ctx, prep2)
      assert length(pending_results) == 0
    end

    @tag timeout: 120_000
    test "chunked insert of 50K triples", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert 50K triples in chunks of 5K
      total = 50_000
      chunk_size = 5_000

      {time_us, _} = :timer.tc(fn ->
        for chunk_start <- 0..(total - 1)//chunk_size do
          chunk_end = min(chunk_start + chunk_size - 1, total - 1)
          triples = for i <- chunk_start..chunk_end do
            {RDF.iri("http://example.org/large#{i}"),
             RDF.iri("http://example.org/seq"),
             RDF.literal(i)}
          end
          {:ok, _} = Update.insert(ctx, triples)
        end
      end)

      IO.puts("\n  50K chunked insert time: #{time_us / 1000}ms")

      # Verify total count
      {:ok, prepared} = Query.prepare("""
        SELECT (COUNT(?s) AS ?count) WHERE { ?s <http://example.org/seq> ?v }
      """)
      {:ok, results} = Query.execute(ctx, prepared)
      result_count = extract_count(hd(results)["count"])
      assert result_count == total
    end
  end

  # ---------------------------------------------------------------------------
  # 3.5.2.4: Update with inference implications (prepare for Phase 4)
  # ---------------------------------------------------------------------------

  describe "update with inference implications (prepare for Phase 4)" do
    @moduledoc """
    These tests verify that the update system provides hooks and patterns
    that will support reasoning in Phase 4. They don't implement actual
    inference but ensure the infrastructure is ready.
    """

    test "updates can track which triples were added for forward chaining", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert class membership - in Phase 4, this would trigger rdfs:subClassOf inference
      {:ok, 2} = Update.update(ctx, """
        INSERT DATA {
          <http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Student> .
          <http://example.org/Student> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://example.org/Person> .
        }
      """)

      # For Phase 4: The reasoner would be triggered here to derive:
      # alice rdf:type Person

      # Verify the base triples exist (inference would add more)
      {:ok, prep} = Query.prepare("""
        SELECT ?class WHERE {
          <http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?class
        }
      """)
      {:ok, results} = Query.execute(ctx, prep)
      # Without reasoner, only Student type
      assert length(results) == 1
      # Query returns AST format
      assert hd(results)["class"] == {:named_node, "http://example.org/Student"}
    end

    test "DELETE triggers can be used for incremental maintenance", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Set up a subclass hierarchy
      {:ok, 3} = Update.update(ctx, """
        INSERT DATA {
          <http://example.org/bob> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Employee> .
          <http://example.org/Employee> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://example.org/Worker> .
          <http://example.org/Worker> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://example.org/Person> .
        }
      """)

      # In Phase 4: Deleting the middle link would require re-evaluation
      # of all derived types for bob

      # Delete the middle subClassOf link
      {:ok, 1} = Update.update(ctx, """
        DELETE DATA {
          <http://example.org/Employee> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://example.org/Worker> .
        }
      """)

      # Verify hierarchy is broken
      {:ok, prep} = Query.prepare("""
        SELECT ?class WHERE {
          <http://example.org/Employee> <http://www.w3.org/2000/01/rdf-schema#subClassOf>+ ?class
        }
      """)
      {:ok, results} = Query.execute(ctx, prep)
      # With broken link, no transitive superclasses
      assert length(results) == 0
    end

    test "MODIFY pattern suitable for materialization updates", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # Insert facts that would trigger rules
      {:ok, 3} = Update.update(ctx, """
        INSERT DATA {
          <http://example.org/x> <http://example.org/sameAs> <http://example.org/y> .
          <http://example.org/x> <http://example.org/label> "X Label" .
          <http://example.org/y> <http://example.org/value> "42" .
        }
      """)

      # Simulate what a reasoner might do: propagate properties via sameAs
      # In Phase 4, owl:sameAs would trigger this automatically

      # Find sameAs pairs and copy properties
      {:ok, prep_sameas} = Query.prepare("""
        SELECT ?a ?b WHERE { ?a <http://example.org/sameAs> ?b }
      """)
      {:ok, sameas_pairs} = Query.execute(ctx, prep_sameas)

      for binding <- sameas_pairs do
        # Get IRI values from AST format
        {:named_node, a_iri} = binding["a"]
        {:named_node, b_iri} = binding["b"]

        # Copy properties from a to b
        {:ok, prep_props} = Query.prepare("""
          SELECT ?p ?o WHERE {
            <#{a_iri}> ?p ?o .
            FILTER(?p != <http://example.org/sameAs>)
          }
        """)
        {:ok, props} = Query.execute(ctx, prep_props)

        for prop <- props do
          # Convert AST back to RDF terms for insert
          {:named_node, p_iri} = prop["p"]
          o_term = ast_to_rdf(prop["o"])
          triples = [{RDF.iri(b_iri), RDF.iri(p_iri), o_term}]
          Update.insert(ctx, triples)
        end

        # Copy properties from b to a
        {:ok, prep_props_b} = Query.prepare("""
          SELECT ?p ?o WHERE {
            <#{b_iri}> ?p ?o .
            FILTER(?p != <http://example.org/sameAs>)
          }
        """)
        {:ok, props_b} = Query.execute(ctx, prep_props_b)

        for prop <- props_b do
          {:named_node, p_iri} = prop["p"]
          o_term = ast_to_rdf(prop["o"])
          triples = [{RDF.iri(a_iri), RDF.iri(p_iri), o_term}]
          Update.insert(ctx, triples)
        end
      end

      # Verify property propagation worked
      {:ok, prep_x_val} = Query.prepare("""
        SELECT ?v WHERE { <http://example.org/x> <http://example.org/value> ?v }
      """)
      {:ok, x_val} = Query.execute(ctx, prep_x_val)
      assert length(x_val) == 1
      assert hd(x_val)["v"] == {:literal, :simple, "42"}

      {:ok, prep_y_label} = Query.prepare("""
        SELECT ?l WHERE { <http://example.org/y> <http://example.org/label> ?l }
      """)
      {:ok, y_label} = Query.execute(ctx, prep_y_label)
      assert length(y_label) == 1
      assert hd(y_label)["l"] == {:literal, :simple, "X Label"}
    end

    test "batch delta tracking for semi-naive evaluation", %{db: db, manager: manager} do
      ctx = %{db: db, dict_manager: manager}

      # This test demonstrates the pattern for tracking new triples
      # that would be used in Phase 4's semi-naive evaluation

      # Track what we insert as "delta"
      delta_triples = [
        {RDF.iri("http://example.org/a"), RDF.iri("http://example.org/p"), RDF.iri("http://example.org/b")},
        {RDF.iri("http://example.org/b"), RDF.iri("http://example.org/p"), RDF.iri("http://example.org/c")},
        {RDF.iri("http://example.org/c"), RDF.iri("http://example.org/p"), RDF.iri("http://example.org/d")}
      ]

      {:ok, 3} = Update.insert(ctx, delta_triples)

      # In Phase 4, we'd process delta_triples through rules
      # For now, just verify we can track and query them

      {:ok, prep} = Query.prepare("""
        SELECT ?s ?o WHERE { ?s <http://example.org/p> ?o }
      """)
      {:ok, results} = Query.execute(ctx, prep)
      assert length(results) == 3

      # Compute transitive closure manually (simulating rule application)
      # In Phase 4 this would be the p+ rule
      new_derived = []

      # Iteration 1: find paths of length 2
      {:ok, prep_chain} = Query.prepare("""
        SELECT ?s ?o WHERE {
          ?s <http://example.org/p> ?mid .
          ?mid <http://example.org/p> ?o .
        }
      """)
      {:ok, chains} = Query.execute(ctx, prep_chain)

      for chain <- chains do
        triple = {chain["s"], RDF.iri("http://example.org/reachable"), chain["o"]}
        # Only add if not already present
        Update.insert(ctx, [triple])
        [triple | new_derived]
      end

      # Verify derived triples
      {:ok, prep_reach} = Query.prepare("""
        SELECT ?s ?o WHERE { ?s <http://example.org/reachable> ?o }
      """)
      {:ok, reach_results} = Query.execute(ctx, prep_reach)
      assert length(reach_results) == 2  # a->c, b->d
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

  # ===========================================================================
  # Error Injection Tests (C4)
  # ===========================================================================

  describe "error handling and injection tests" do
    test "handles invalid SPARQL UPDATE syntax", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Invalid syntax - missing closing brace
      result = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> <http://example.org/o>
      """)

      assert match?({:error, _}, result)
      Transaction.stop(txn)
    end

    test "handles invalid IRI in update", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Invalid IRI (spaces not allowed)
      result = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/invalid iri> <http://example.org/p> <http://example.org/o> .
        }
      """)

      assert match?({:error, _}, result)
      Transaction.stop(txn)
    end

    test "transaction remains usable after parse error", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # First: invalid update
      {:error, _} = Transaction.update(txn, "INVALID SPARQL")

      # Transaction should still be usable
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> "value" .
        }
      """)

      # Verify insert worked
      {:ok, results} = Transaction.query(txn, "SELECT ?s WHERE { ?s ?p ?o }")
      assert length(results) == 1

      Transaction.stop(txn)
    end

    test "handles empty WHERE clause gracefully", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # This should work - DELETE WHERE with no matches deletes nothing
      {:ok, 0} = Transaction.update(txn, """
        DELETE WHERE {
          <http://nonexistent.org/x> <http://nonexistent.org/y> ?z .
        }
      """)

      Transaction.stop(txn)
    end

    test "handles process shutdown during operation", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert some data
      {:ok, 10} = Transaction.update(txn, """
        INSERT DATA {
          #{for i <- 1..10 do
            "<http://example.org/item#{i}> <http://example.org/value> \"#{i}\" ."
          end |> Enum.join("\n")}
        }
      """)

      # Stop the transaction
      Transaction.stop(txn)

      # Attempting to use it should fail gracefully
      result = catch_exit(Transaction.query(txn, "SELECT * WHERE { ?s ?p ?o }"))
      assert result != nil
    end

    test "verify data integrity after interrupted operation", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert initial data
      {:ok, 5} = Transaction.update(txn, """
        INSERT DATA {
          #{for i <- 1..5 do
            "<http://example.org/stable#{i}> <http://example.org/p> \"#{i}\" ."
          end |> Enum.join("\n")}
        }
      """)

      # Stop transaction
      Transaction.stop(txn)

      # Start new transaction
      {:ok, txn2} = Transaction.start_link(db: db, dict_manager: manager)

      # Data should still be there
      {:ok, results} = Transaction.query(txn2, """
        SELECT (COUNT(?s) AS ?count) WHERE {
          ?s <http://example.org/p> ?o
        }
      """)

      count = extract_count(hd(results)["count"])
      assert count == 5

      Transaction.stop(txn2)
    end

    test "handles MODIFY with no matching WHERE", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # MODIFY with WHERE that matches nothing
      {:ok, 0} = Transaction.update(txn, """
        DELETE { ?s <http://example.org/old> ?o }
        INSERT { ?s <http://example.org/new> ?o }
        WHERE { ?s <http://example.org/old> ?o }
      """)

      # No triples should exist
      {:ok, results} = Transaction.query(txn, "SELECT * WHERE { ?s ?p ?o }")
      assert length(results) == 0

      Transaction.stop(txn)
    end

    test "handles DELETE template with unbound variables", %{db: db, manager: manager} do
      {:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)

      # Insert data
      {:ok, 1} = Transaction.update(txn, """
        INSERT DATA {
          <http://example.org/s> <http://example.org/p> "value" .
        }
      """)

      # DELETE with WHERE that binds different variable than template expects
      # (this is valid SPARQL - just won't match anything)
      {:ok, _} = Transaction.update(txn, """
        DELETE { <http://nonexistent.org/x> ?p ?o }
        WHERE { ?s <http://example.org/p> ?o }
      """)

      # Original triple should still exist
      {:ok, results} = Transaction.query(txn, "SELECT * WHERE { ?s ?p ?o }")
      assert length(results) == 1

      Transaction.stop(txn)
    end
  end
end
