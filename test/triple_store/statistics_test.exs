defmodule TripleStore.StatisticsTest do
  @moduledoc """
  Tests for Task 1.6.1: Triple Counts.

  Verifies:
  - triple_count/1 returns accurate count
  - predicate_count/2 returns accurate per-predicate count
  - distinct_subjects/1 returns accurate distinct count
  - distinct_predicates/1 returns accurate distinct count
  - distinct_objects/1 returns accurate distinct count
  - all/1 returns all statistics
  """

  use ExUnit.Case, async: false

  alias TripleStore.Statistics
  alias TripleStore.Index
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager

  @test_db_base "/tmp/triple_store_statistics_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # triple_count/1 Tests
  # ===========================================================================

  describe "triple_count/1" do
    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = Statistics.triple_count(db)
    end

    test "returns 1 for single triple", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 1001, 1002})
      assert {:ok, 1} = Statistics.triple_count(db)
    end

    test "returns correct count for multiple triples", %{db: db} do
      triples = [
        {1000, 1001, 1002},
        {1000, 1001, 1003},
        {1000, 1004, 1005},
        {1006, 1001, 1002},
        {1007, 1008, 1009}
      ]

      :ok = Index.insert_triples(db, triples)
      assert {:ok, 5} = Statistics.triple_count(db)
    end

    test "count remains consistent after insert and delete", %{db: db} do
      triples = [{1000, 1001, 1002}, {1003, 1004, 1005}, {1006, 1007, 1008}]
      :ok = Index.insert_triples(db, triples)
      assert {:ok, 3} = Statistics.triple_count(db)

      :ok = Index.delete_triple(db, {1003, 1004, 1005})
      assert {:ok, 2} = Statistics.triple_count(db)
    end
  end

  # ===========================================================================
  # predicate_count/2 Tests
  # ===========================================================================

  describe "predicate_count/2" do
    test "returns 0 for predicate with no triples", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 1001, 1002})
      assert {:ok, 0} = Statistics.predicate_count(db, 9999)
    end

    test "returns correct count for predicate", %{db: db} do
      triples = [
        {1000, 100, 2000},  # predicate 100
        {1001, 100, 2001},  # predicate 100
        {1002, 100, 2002},  # predicate 100
        {1003, 200, 2003},  # predicate 200
        {1004, 200, 2004}   # predicate 200
      ]

      :ok = Index.insert_triples(db, triples)

      assert {:ok, 3} = Statistics.predicate_count(db, 100)
      assert {:ok, 2} = Statistics.predicate_count(db, 200)
    end

    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = Statistics.predicate_count(db, 100)
    end
  end

  # ===========================================================================
  # distinct_subjects/1 Tests
  # ===========================================================================

  describe "distinct_subjects/1" do
    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = Statistics.distinct_subjects(db)
    end

    test "returns 1 for single subject", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1000, 101, 2001},
        {1000, 102, 2002}
      ]

      :ok = Index.insert_triples(db, triples)
      assert {:ok, 1} = Statistics.distinct_subjects(db)
    end

    test "returns correct count for multiple subjects", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 100, 2002},
        {1000, 101, 2003},  # Same subject 1000
        {1001, 102, 2004}   # Same subject 1001
      ]

      :ok = Index.insert_triples(db, triples)
      # Should have 3 distinct subjects: 1000, 1001, 1002
      assert {:ok, 3} = Statistics.distinct_subjects(db)
    end
  end

  # ===========================================================================
  # distinct_predicates/1 Tests
  # ===========================================================================

  describe "distinct_predicates/1" do
    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = Statistics.distinct_predicates(db)
    end

    test "returns 1 for single predicate", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 100, 2002}
      ]

      :ok = Index.insert_triples(db, triples)
      assert {:ok, 1} = Statistics.distinct_predicates(db)
    end

    test "returns correct count for multiple predicates", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 101, 2001},
        {1002, 102, 2002},
        {1003, 100, 2003},  # Same predicate 100
        {1004, 101, 2004}   # Same predicate 101
      ]

      :ok = Index.insert_triples(db, triples)
      # Should have 3 distinct predicates: 100, 101, 102
      assert {:ok, 3} = Statistics.distinct_predicates(db)
    end
  end

  # ===========================================================================
  # distinct_objects/1 Tests
  # ===========================================================================

  describe "distinct_objects/1" do
    test "returns 0 for empty store", %{db: db} do
      assert {:ok, 0} = Statistics.distinct_objects(db)
    end

    test "returns 1 for single object", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 101, 2000},
        {1002, 102, 2000}
      ]

      :ok = Index.insert_triples(db, triples)
      assert {:ok, 1} = Statistics.distinct_objects(db)
    end

    test "returns correct count for multiple objects", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 101, 2001},
        {1002, 102, 2002},
        {1003, 103, 2000},  # Same object 2000
        {1004, 104, 2001}   # Same object 2001
      ]

      :ok = Index.insert_triples(db, triples)
      # Should have 3 distinct objects: 2000, 2001, 2002
      assert {:ok, 3} = Statistics.distinct_objects(db)
    end
  end

  # ===========================================================================
  # all/1 Tests
  # ===========================================================================

  describe "all/1" do
    test "returns all zeros for empty store", %{db: db} do
      {:ok, stats} = Statistics.all(db)

      assert stats.triple_count == 0
      assert stats.distinct_subjects == 0
      assert stats.distinct_predicates == 0
      assert stats.distinct_objects == 0
    end

    test "returns correct statistics", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1000, 101, 2001},
        {1001, 100, 2000},
        {1001, 102, 2002},
        {1002, 100, 2001}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, stats} = Statistics.all(db)

      assert stats.triple_count == 5
      assert stats.distinct_subjects == 3  # 1000, 1001, 1002
      assert stats.distinct_predicates == 3  # 100, 101, 102
      assert stats.distinct_objects == 3  # 2000, 2001, 2002
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles large term IDs", %{db: db} do
      large_id = 0xFFFFFFFFFFFFFFFF
      :ok = Index.insert_triple(db, {large_id, 100, 200})

      assert {:ok, 1} = Statistics.triple_count(db)
      assert {:ok, 1} = Statistics.distinct_subjects(db)
    end

    test "handles many triples efficiently", %{db: db} do
      # Insert 100 triples
      triples =
        for i <- 1..100 do
          {1000 + rem(i, 10), 100 + rem(i, 5), 2000 + i}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, stats} = Statistics.all(db)

      assert stats.triple_count == 100
      assert stats.distinct_subjects == 10  # 1000-1009
      assert stats.distinct_predicates == 5  # 100-104
      assert stats.distinct_objects == 100  # All unique
    end
  end
end
