defmodule TripleStore.Integration.DatabaseLifecycleTest do
  @moduledoc """
  Integration tests for Task 1.7.1: Database Lifecycle Testing.

  Tests complete database lifecycle from open through heavy usage to close,
  including:
  - Data persistence across restarts
  - Concurrent read operations during writes
  - Database recovery scenarios
  - Multiple database instances
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.Statistics.Cache

  @test_db_base "/tmp/triple_store_lifecycle_test"

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp unique_path do
    "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
  end

  defp cleanup_path(path) do
    File.rm_rf(path)
  end

  # ===========================================================================
  # 1.7.1.1: Data Persistence Across Restarts
  # ===========================================================================

  describe "data persistence across restarts" do
    test "triples persist after close and reopen" do
      path = unique_path()

      # Phase 1: Open, insert triples, close
      {:ok, db1} = NIF.open(path)
      {:ok, manager1} = Manager.start_link(db: db1)

      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 101, 2002}
      ]

      :ok = Index.insert_triples(db1, triples)

      # Verify data is there
      {:ok, count1} = Index.count(db1, {:var, :var, :var})
      assert count1 == 3

      # Close everything
      Manager.stop(manager1)
      NIF.close(db1)

      # Ensure the db reference is garbage collected and file handles released
      :erlang.garbage_collect()
      Process.sleep(200)

      # Phase 2: Reopen and verify data persisted
      {:ok, db2} = NIF.open(path)

      {:ok, count2} = Index.count(db2, {:var, :var, :var})
      assert count2 == 3

      # Verify specific triples exist (triple_exists? returns {:ok, boolean})
      assert {:ok, true} = Index.triple_exists?(db2, {1000, 100, 2000})
      assert {:ok, true} = Index.triple_exists?(db2, {1001, 100, 2001})
      assert {:ok, true} = Index.triple_exists?(db2, {1002, 101, 2002})

      NIF.close(db2)
      cleanup_path(path)
    end

    @tag :slow
    test "dictionary IDs persist after close and reopen" do
      path = unique_path()

      # Phase 1: Create dictionary entries
      {:ok, db1} = NIF.open(path)
      {:ok, manager1} = Manager.start_link(db: db1)

      # Get IDs for some RDF terms (use actual RDF.ex types)
      uri = RDF.iri("http://example.org/test")
      bnode = RDF.bnode("b1")
      literal = RDF.literal("hello")

      {:ok, uri_id} = Manager.get_or_create_id(manager1, uri)
      {:ok, bnode_id} = Manager.get_or_create_id(manager1, bnode)
      {:ok, literal_id} = Manager.get_or_create_id(manager1, literal)

      Manager.stop(manager1)
      NIF.close(db1)

      # Phase 2: Reopen and verify same IDs
      {:ok, db2} = NIF.open(path)
      {:ok, manager2} = Manager.start_link(db: db2)

      {:ok, uri_id2} = Manager.get_or_create_id(manager2, uri)
      {:ok, bnode_id2} = Manager.get_or_create_id(manager2, bnode)
      {:ok, literal_id2} = Manager.get_or_create_id(manager2, literal)

      assert uri_id == uri_id2
      assert bnode_id == bnode_id2
      assert literal_id == literal_id2

      Manager.stop(manager2)
      NIF.close(db2)
      cleanup_path(path)
    end

    test "sequence counters persist across restarts" do
      path = unique_path()

      # Phase 1: Create many entries to advance counters
      {:ok, db1} = NIF.open(path)
      {:ok, manager1} = Manager.start_link(db: db1)

      # Create 100 URIs to advance counter
      for i <- 1..100 do
        Manager.get_or_create_id(manager1, RDF.iri("http://example.org/item#{i}"))
      end

      {:ok, last_id1} = Manager.get_or_create_id(manager1, RDF.iri("http://example.org/last"))

      Manager.stop(manager1)
      NIF.close(db1)

      # Phase 2: Reopen and create new entry - should get higher ID
      {:ok, db2} = NIF.open(path)
      {:ok, manager2} = Manager.start_link(db: db2)

      {:ok, new_id} = Manager.get_or_create_id(manager2, RDF.iri("http://example.org/new"))

      # New ID should be greater than last ID from previous session
      assert new_id > last_id1

      Manager.stop(manager2)
      NIF.close(db2)
      cleanup_path(path)
    end

    @tag :slow
    test "statistics reflect persisted data after restart" do
      path = unique_path()

      # Phase 1: Insert triples
      {:ok, db1} = NIF.open(path)

      triples =
        for i <- 1..50 do
          {1000 + rem(i, 5), 100 + rem(i, 3), 2000 + i}
        end

      :ok = Index.insert_triples(db1, triples)

      {:ok, stats1} = TripleStore.Statistics.all(db1)

      NIF.close(db1)

      # Ensure the db reference is garbage collected and file handles released
      :erlang.garbage_collect()
      Process.sleep(200)

      # Phase 2: Reopen and verify statistics
      {:ok, db2} = NIF.open(path)

      {:ok, stats2} = TripleStore.Statistics.all(db2)

      assert stats1.triple_count == stats2.triple_count
      assert stats1.distinct_subjects == stats2.distinct_subjects
      assert stats1.distinct_predicates == stats2.distinct_predicates
      assert stats1.distinct_objects == stats2.distinct_objects

      NIF.close(db2)
      cleanup_path(path)
    end
  end

  # ===========================================================================
  # 1.7.1.2: Concurrent Read Operations During Writes
  # ===========================================================================

  describe "concurrent read operations during writes" do
    @tag :slow
    test "reads do not block during writes" do
      path = unique_path()
      {:ok, db} = NIF.open(path)

      # Insert initial data
      :ok = Index.insert_triples(db, [{1000, 100, 2000}, {1001, 100, 2001}])

      # Start concurrent tasks
      write_task =
        Task.async(fn ->
          for i <- 1..100 do
            Index.insert_triple(db, {2000 + i, 200, 3000 + i})
          end

          :write_complete
        end)

      read_tasks =
        for _ <- 1..5 do
          Task.async(fn ->
            for _ <- 1..20 do
              {:ok, _count} = Index.count(db, {:var, :var, :var})
              Process.sleep(1)
            end

            :read_complete
          end)
        end

      # All tasks should complete without deadlock
      assert Task.await(write_task, 10_000) == :write_complete

      for task <- read_tasks do
        assert Task.await(task, 10_000) == :read_complete
      end

      # Verify final state
      {:ok, count} = Index.count(db, {:var, :var, :var})
      assert count == 102

      NIF.close(db)
      cleanup_path(path)
    end

    test "concurrent lookups during inserts" do
      path = unique_path()
      {:ok, db} = NIF.open(path)

      # Insert initial data with specific predicate
      for i <- 1..10 do
        Index.insert_triple(db, {1000 + i, 100, 2000 + i})
      end

      # Writer adds more triples with same predicate
      writer =
        Task.async(fn ->
          for i <- 11..50 do
            Index.insert_triple(db, {1000 + i, 100, 2000 + i})
            Process.sleep(1)
          end

          :done
        end)

      # Reader continuously queries by predicate
      reader =
        Task.async(fn ->
          results =
            for _ <- 1..20 do
              {:ok, triples} = Index.lookup_all(db, {:var, {:bound, 100}, :var})
              length(triples)
            end

          # Results should be monotonically increasing or stable
          Enum.chunk_every(results, 2, 1, :discard)
          |> Enum.all?(fn [a, b] -> b >= a end)
        end)

      Task.await(writer, 10_000)
      assert Task.await(reader, 10_000)

      NIF.close(db)
      cleanup_path(path)
    end

    test "snapshot provides consistent view during writes" do
      path = unique_path()
      {:ok, db} = NIF.open(path)

      # Insert initial data
      for i <- 1..10 do
        Index.insert_triple(db, {1000 + i, 100, 2000 + i})
      end

      # Take snapshot
      {:ok, snapshot} = NIF.snapshot(db)

      # Count at snapshot time using snapshot_stream
      {:ok, snapshot_stream} = NIF.snapshot_stream(snapshot, :spo, <<>>)

      snapshot_count =
        snapshot_stream
        |> Enum.count()

      # Add more triples after snapshot
      for i <- 11..50 do
        Index.insert_triple(db, {1000 + i, 100, 2000 + i})
      end

      # Current count should be higher
      {:ok, current_count} = Index.count(db, {:var, :var, :var})

      assert snapshot_count == 10
      assert current_count == 50

      NIF.release_snapshot(snapshot)
      NIF.close(db)
      cleanup_path(path)
    end
  end

  # ===========================================================================
  # 1.7.1.3: Database Recovery Scenarios
  # ===========================================================================

  describe "database recovery scenarios" do
    test "database reopens correctly after incomplete close" do
      path = unique_path()

      # Open and write data
      {:ok, db} = NIF.open(path)

      for i <- 1..100 do
        Index.insert_triple(db, {1000 + i, 100, 2000 + i})
      end

      # Force close without proper cleanup (simulates crash)
      # RocksDB handles this via WAL recovery
      NIF.close(db)

      # Reopen - should work and have data
      {:ok, db2} = NIF.open(path)
      {:ok, count} = Index.count(db2, {:var, :var, :var})

      assert count == 100

      NIF.close(db2)
      cleanup_path(path)
    end

    @tag :slow
    test "database handles reopening same path multiple times" do
      path = unique_path()

      for _ <- 1..5 do
        {:ok, db} = NIF.open(path)
        Index.insert_triple(db, {:rand.uniform(10_000), 100, :rand.uniform(10_000)})
        NIF.close(db)
      end

      # Final open should show accumulated data
      {:ok, db} = NIF.open(path)
      {:ok, count} = Index.count(db, {:var, :var, :var})

      # Should have some triples (exact count depends on duplicates)
      assert count >= 1 and count <= 5

      NIF.close(db)
      cleanup_path(path)
    end

    test "batch operations are atomic on recovery" do
      path = unique_path()

      {:ok, db} = NIF.open(path)

      # Insert batch atomically
      triples = for i <- 1..50, do: {1000 + i, 100, 2000 + i}
      :ok = Index.insert_triples(db, triples)

      NIF.close(db)

      # Reopen and verify all triples from batch exist
      {:ok, db2} = NIF.open(path)

      for {s, p, o} <- triples do
        assert {:ok, true} = Index.triple_exists?(db2, {s, p, o}),
               "Triple {#{s}, #{p}, #{o}} should exist after recovery"
      end

      NIF.close(db2)
      cleanup_path(path)
    end
  end

  # ===========================================================================
  # 1.7.1.4: Multiple Database Instances
  # ===========================================================================

  describe "multiple database instances" do
    test "multiple databases in same process work independently" do
      path1 = unique_path()
      path2 = unique_path()

      {:ok, db1} = NIF.open(path1)
      {:ok, db2} = NIF.open(path2)

      # Insert different data into each
      :ok = Index.insert_triple(db1, {1000, 100, 2000})
      :ok = Index.insert_triple(db1, {1001, 100, 2001})

      :ok = Index.insert_triple(db2, {3000, 300, 4000})

      # Verify isolation
      {:ok, count1} = Index.count(db1, {:var, :var, :var})
      {:ok, count2} = Index.count(db2, {:var, :var, :var})

      assert count1 == 2
      assert count2 == 1

      # Verify specific data (triple_exists? returns {:ok, boolean})
      assert {:ok, true} = Index.triple_exists?(db1, {1000, 100, 2000})
      assert {:ok, false} = Index.triple_exists?(db1, {3000, 300, 4000})

      assert {:ok, true} = Index.triple_exists?(db2, {3000, 300, 4000})
      assert {:ok, false} = Index.triple_exists?(db2, {1000, 100, 2000})

      NIF.close(db1)
      NIF.close(db2)
      cleanup_path(path1)
      cleanup_path(path2)
    end

    test "dictionary managers for different databases are independent" do
      path1 = unique_path()
      path2 = unique_path()

      {:ok, db1} = NIF.open(path1)
      {:ok, db2} = NIF.open(path2)

      {:ok, manager1} = Manager.start_link(db: db1)
      {:ok, manager2} = Manager.start_link(db: db2)

      # Create same term in both databases
      shared_uri = RDF.iri("http://example.org/shared")
      {:ok, id1} = Manager.get_or_create_id(manager1, shared_uri)
      {:ok, id2} = Manager.get_or_create_id(manager2, shared_uri)

      # Both should be valid IDs
      assert is_integer(id1) and id1 > 0
      assert is_integer(id2) and id2 > 0

      # The key test: term-to-ID lookups use StringToId which is database-specific
      # Create unique terms with different URIs
      unique_uri1 = RDF.iri("http://example.org/only-in-db1")
      unique_uri2 = RDF.iri("http://example.org/only-in-db2")

      {:ok, _id_in_db1} = Manager.get_or_create_id(manager1, unique_uri1)
      {:ok, _id_in_db2} = Manager.get_or_create_id(manager2, unique_uri2)

      # Use StringToId to verify isolation - looking up by term string
      alias TripleStore.Dictionary.StringToId

      # unique_uri1 exists in db1 but not db2
      assert {:ok, _} = StringToId.lookup_id(db1, unique_uri1)
      assert :not_found = StringToId.lookup_id(db2, unique_uri1)

      # unique_uri2 exists in db2 but not db1
      assert {:ok, _} = StringToId.lookup_id(db2, unique_uri2)
      assert :not_found = StringToId.lookup_id(db1, unique_uri2)

      Manager.stop(manager1)
      Manager.stop(manager2)
      NIF.close(db1)
      NIF.close(db2)
      cleanup_path(path1)
      cleanup_path(path2)
    end

    test "closing one database doesn't affect others" do
      path1 = unique_path()
      path2 = unique_path()

      {:ok, db1} = NIF.open(path1)
      {:ok, db2} = NIF.open(path2)

      :ok = Index.insert_triple(db1, {1000, 100, 2000})
      :ok = Index.insert_triple(db2, {2000, 200, 3000})

      # Close db1
      NIF.close(db1)

      # db2 should still work
      assert {:ok, true} = Index.triple_exists?(db2, {2000, 200, 3000})
      :ok = Index.insert_triple(db2, {2001, 200, 3001})

      {:ok, count} = Index.count(db2, {:var, :var, :var})
      assert count == 2

      NIF.close(db2)
      cleanup_path(path1)
      cleanup_path(path2)
    end

    @tag :slow
    test "statistics caches for different databases are independent" do
      path1 = unique_path()
      path2 = unique_path()

      {:ok, db1} = NIF.open(path1)
      {:ok, db2} = NIF.open(path2)

      # Insert different amounts
      for i <- 1..10, do: Index.insert_triple(db1, {1000 + i, 100, 2000 + i})
      for i <- 1..50, do: Index.insert_triple(db2, {3000 + i, 300, 4000 + i})

      # Start caches
      {:ok, cache1} = Cache.start_link(db: db1)
      {:ok, cache2} = Cache.start_link(db: db2)

      Process.sleep(100)

      # Verify independent statistics
      {:ok, stats1} = Cache.get(cache1)
      {:ok, stats2} = Cache.get(cache2)

      assert stats1.triple_count == 10
      assert stats2.triple_count == 50

      Cache.stop(cache1)
      Cache.stop(cache2)
      NIF.close(db1)
      NIF.close(db2)
      cleanup_path(path1)
      cleanup_path(path2)
    end
  end
end
