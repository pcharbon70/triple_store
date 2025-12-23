defmodule TripleStore.Integration.IndexConsistencyTest do
  @moduledoc """
  Integration tests for Task 1.7.3: Index Consistency Testing.

  Tests all three indices (SPO, POS, OSP) remain consistent through
  insert/delete cycles, including:
  - Triple found via all applicable patterns after insert
  - Triple not found via any pattern after delete
  - Index consistency after interleaved inserts and deletes
  - Batch operations maintain cross-index consistency
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index

  @test_db_base "/tmp/triple_store_index_consistency_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, path: test_path}
  end

  # ===========================================================================
  # 1.7.3.1: Triple Found Via All Applicable Patterns After Insert
  # ===========================================================================

  describe "triple found via all applicable patterns after insert" do
    test "single triple findable via all 8 patterns", %{db: db} do
      triple = {1000, 100, 2000}
      :ok = Index.insert_triple(db, triple)

      # Pattern: {S, P, O} - exact match
      {:ok, results} = Index.lookup_all(db, {{:bound, 1000}, {:bound, 100}, {:bound, 2000}})
      assert length(results) == 1
      assert hd(results) == triple

      # Pattern: {S, P, ?} - subject-predicate
      {:ok, results} = Index.lookup_all(db, {{:bound, 1000}, {:bound, 100}, :var})
      assert triple in results

      # Pattern: {S, ?, ?} - subject only
      {:ok, results} = Index.lookup_all(db, {{:bound, 1000}, :var, :var})
      assert triple in results

      # Pattern: {?, P, O} - predicate-object
      {:ok, results} = Index.lookup_all(db, {:var, {:bound, 100}, {:bound, 2000}})
      assert triple in results

      # Pattern: {?, P, ?} - predicate only
      {:ok, results} = Index.lookup_all(db, {:var, {:bound, 100}, :var})
      assert triple in results

      # Pattern: {?, ?, O} - object only
      {:ok, results} = Index.lookup_all(db, {:var, :var, {:bound, 2000}})
      assert triple in results

      # Pattern: {S, ?, O} - subject-object (requires filter)
      {:ok, results} = Index.lookup_all(db, {{:bound, 1000}, :var, {:bound, 2000}})
      assert triple in results

      # Pattern: {?, ?, ?} - full scan
      {:ok, results} = Index.lookup_all(db, {:var, :var, :var})
      assert triple in results
    end

    test "triple exists? returns true after insert", %{db: db} do
      triple = {1001, 101, 2001}
      :ok = Index.insert_triple(db, triple)

      assert {:ok, true} = Index.triple_exists?(db, triple)
    end

    test "count reflects inserted triples", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 101, 2002}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, total} = Index.count(db, {:var, :var, :var})
      assert total == 3

      {:ok, pred_100} = Index.count(db, {:var, {:bound, 100}, :var})
      assert pred_100 == 2

      {:ok, pred_101} = Index.count(db, {:var, {:bound, 101}, :var})
      assert pred_101 == 1
    end
  end

  # ===========================================================================
  # 1.7.3.2: Triple Not Found Via Any Pattern After Delete
  # ===========================================================================

  describe "triple not found via any pattern after delete" do
    test "deleted triple not findable via any pattern", %{db: db} do
      triple = {1000, 100, 2000}
      :ok = Index.insert_triple(db, triple)

      # Verify it exists first
      assert {:ok, true} = Index.triple_exists?(db, triple)

      # Delete it
      :ok = Index.delete_triple(db, triple)

      # Now verify it's not findable via any pattern
      {:ok, results} = Index.lookup_all(db, {{:bound, 1000}, {:bound, 100}, {:bound, 2000}})
      assert results == []

      {:ok, results} = Index.lookup_all(db, {{:bound, 1000}, {:bound, 100}, :var})
      assert triple not in results

      {:ok, results} = Index.lookup_all(db, {{:bound, 1000}, :var, :var})
      assert triple not in results

      {:ok, results} = Index.lookup_all(db, {:var, {:bound, 100}, {:bound, 2000}})
      assert triple not in results

      {:ok, results} = Index.lookup_all(db, {:var, {:bound, 100}, :var})
      assert triple not in results

      {:ok, results} = Index.lookup_all(db, {:var, :var, {:bound, 2000}})
      assert triple not in results

      {:ok, results} = Index.lookup_all(db, {{:bound, 1000}, :var, {:bound, 2000}})
      assert triple not in results

      {:ok, results} = Index.lookup_all(db, {:var, :var, :var})
      assert triple not in results
    end

    test "triple exists? returns false after delete", %{db: db} do
      triple = {1001, 101, 2001}
      :ok = Index.insert_triple(db, triple)
      :ok = Index.delete_triple(db, triple)

      assert {:ok, false} = Index.triple_exists?(db, triple)
    end

    test "count reflects deleted triples", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 101, 2002}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, count1} = Index.count(db, {:var, :var, :var})
      assert count1 == 3

      # Delete one triple
      :ok = Index.delete_triple(db, {1001, 100, 2001})

      {:ok, count2} = Index.count(db, {:var, :var, :var})
      assert count2 == 2

      {:ok, pred_100} = Index.count(db, {:var, {:bound, 100}, :var})
      assert pred_100 == 1
    end

    test "deleting non-existent triple is safe", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      # Delete a triple that doesn't exist
      :ok = Index.delete_triple(db, {9999, 999, 9999})

      # Original triple should still exist
      assert {:ok, true} = Index.triple_exists?(db, {1000, 100, 2000})
      {:ok, count} = Index.count(db, {:var, :var, :var})
      assert count == 1
    end
  end

  # ===========================================================================
  # 1.7.3.3: Index Consistency After Interleaved Inserts and Deletes
  # ===========================================================================

  describe "index consistency after interleaved inserts and deletes" do
    test "interleaved operations maintain consistency", %{db: db} do
      # Insert some triples
      :ok = Index.insert_triple(db, {1000, 100, 2000})
      :ok = Index.insert_triple(db, {1001, 100, 2001})

      {:ok, count1} = Index.count(db, {:var, :var, :var})
      assert count1 == 2

      # Delete one, insert another
      :ok = Index.delete_triple(db, {1000, 100, 2000})
      :ok = Index.insert_triple(db, {1002, 100, 2002})

      {:ok, count2} = Index.count(db, {:var, :var, :var})
      assert count2 == 2

      # Verify correct triples exist
      assert {:ok, false} = Index.triple_exists?(db, {1000, 100, 2000})
      assert {:ok, true} = Index.triple_exists?(db, {1001, 100, 2001})
      assert {:ok, true} = Index.triple_exists?(db, {1002, 100, 2002})

      # More interleaved operations
      :ok = Index.insert_triple(db, {1003, 101, 2003})
      :ok = Index.delete_triple(db, {1001, 100, 2001})
      :ok = Index.insert_triple(db, {1004, 101, 2004})
      :ok = Index.delete_triple(db, {1002, 100, 2002})

      # Final state
      {:ok, final_count} = Index.count(db, {:var, :var, :var})
      assert final_count == 2

      {:ok, results} = Index.lookup_all(db, {:var, :var, :var})
      assert length(results) == 2
      assert {1003, 101, 2003} in results
      assert {1004, 101, 2004} in results
    end

    test "re-inserting deleted triple works correctly", %{db: db} do
      triple = {1000, 100, 2000}

      # Insert
      :ok = Index.insert_triple(db, triple)
      assert {:ok, true} = Index.triple_exists?(db, triple)

      # Delete
      :ok = Index.delete_triple(db, triple)
      assert {:ok, false} = Index.triple_exists?(db, triple)

      # Re-insert
      :ok = Index.insert_triple(db, triple)
      assert {:ok, true} = Index.triple_exists?(db, triple)

      # Verify findable via all patterns
      {:ok, results} = Index.lookup_all(db, {:var, {:bound, 100}, :var})
      assert triple in results
    end

    test "many insert-delete cycles maintain consistency", %{db: db} do
      for i <- 1..50 do
        triple = {1000 + i, 100, 2000 + i}
        :ok = Index.insert_triple(db, triple)

        if rem(i, 3) == 0 do
          # Delete every 3rd triple
          :ok = Index.delete_triple(db, triple)
        end
      end

      # Should have 50 - 16 (deleted: 3,6,9,...,48) = 34 remaining
      # Actually: 3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48 = 16 deleted
      {:ok, count} = Index.count(db, {:var, :var, :var})
      assert count == 34

      # Verify deleted ones don't exist
      for i <- [3, 6, 9, 12, 48] do
        assert {:ok, false} = Index.triple_exists?(db, {1000 + i, 100, 2000 + i})
      end

      # Verify non-deleted ones exist
      for i <- [1, 2, 4, 5, 50] do
        assert {:ok, true} = Index.triple_exists?(db, {1000 + i, 100, 2000 + i})
      end
    end
  end

  # ===========================================================================
  # 1.7.3.4: Batch Operations Maintain Cross-Index Consistency
  # ===========================================================================

  describe "batch operations maintain cross-index consistency" do
    test "batch insert writes to all three indices atomically", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 101, 2002},
        {1003, 101, 2003}
      ]

      :ok = Index.insert_triples(db, triples)

      # Each triple should be findable via SPO, POS, and OSP patterns
      for triple <- triples do
        {s, p, o} = triple

        # Via SPO (subject lookup)
        {:ok, results} = Index.lookup_all(db, {{:bound, s}, :var, :var})
        assert triple in results

        # Via POS (predicate lookup)
        {:ok, results} = Index.lookup_all(db, {:var, {:bound, p}, :var})
        assert triple in results

        # Via OSP (object lookup)
        {:ok, results} = Index.lookup_all(db, {:var, :var, {:bound, o}})
        assert triple in results
      end
    end

    test "batch delete removes from all three indices atomically", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 101, 2002}
      ]

      :ok = Index.insert_triples(db, triples)

      # Delete first two
      :ok = Index.delete_triples(db, [{1000, 100, 2000}, {1001, 100, 2001}])

      # Verify deleted triples not in any index
      for triple <- [{1000, 100, 2000}, {1001, 100, 2001}] do
        {s, p, o} = triple

        {:ok, results} = Index.lookup_all(db, {{:bound, s}, :var, :var})
        assert triple not in results

        {:ok, results} = Index.lookup_all(db, {:var, {:bound, p}, :var})
        assert triple not in results

        {:ok, results} = Index.lookup_all(db, {:var, :var, {:bound, o}})
        assert triple not in results
      end

      # Verify remaining triple still exists
      assert {:ok, true} = Index.triple_exists?(db, {1002, 101, 2002})
    end

    test "duplicate inserts are idempotent", %{db: db} do
      triple = {1000, 100, 2000}

      # Insert same triple multiple times
      :ok = Index.insert_triple(db, triple)
      :ok = Index.insert_triple(db, triple)
      :ok = Index.insert_triples(db, [triple, triple, triple])

      # Should only have one triple
      {:ok, count} = Index.count(db, {:var, :var, :var})
      assert count == 1

      # All patterns should return exactly one result
      {:ok, results} = Index.lookup_all(db, {{:bound, 1000}, :var, :var})
      assert length(results) == 1
    end

    test "large batch maintains consistency", %{db: db} do
      # Insert 1000 triples
      triples =
        for i <- 1..1000 do
          {i, rem(i, 10), i + 10_000}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, total} = Index.count(db, {:var, :var, :var})
      assert total == 1000

      # Verify each predicate has expected count (100 each for predicates 0-9)
      for p <- 0..9 do
        {:ok, count} = Index.count(db, {:var, {:bound, p}, :var})
        assert count == 100
      end

      # Spot check some triples
      assert {:ok, true} = Index.triple_exists?(db, {1, 1, 10_001})
      assert {:ok, true} = Index.triple_exists?(db, {500, 0, 10_500})
      assert {:ok, true} = Index.triple_exists?(db, {1000, 0, 11_000})
    end

    test "concurrent batch operations maintain consistency", %{db: db} do
      # Multiple concurrent inserts
      tasks =
        for batch <- 0..4 do
          Task.async(fn ->
            triples =
              for i <- 1..100 do
                {batch * 1000 + i, batch, batch * 10_000 + i}
              end

            Index.insert_triples(db, triples)
          end)
        end

      # Wait for all to complete
      for task <- tasks do
        assert :ok = Task.await(task, 10_000)
      end

      # Should have 500 total triples
      {:ok, total} = Index.count(db, {:var, :var, :var})
      assert total == 500

      # Each batch's predicate should have 100 triples
      for p <- 0..4 do
        {:ok, count} = Index.count(db, {:var, {:bound, p}, :var})
        assert count == 100
      end
    end
  end
end
