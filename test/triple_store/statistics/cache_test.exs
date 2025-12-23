defmodule TripleStore.Statistics.CacheTest do
  @moduledoc """
  Tests for Task 1.6.2: Statistics Cache.

  Verifies:
  - Cache starts and computes initial statistics
  - get/1 returns cached statistics
  - predicate_histogram/1 returns frequency map
  - invalidate/1 clears cached data
  - refresh/1 forces synchronous refresh
  - Periodic refresh via timer
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index
  alias TripleStore.Statistics.Cache

  @test_db_base "/tmp/triple_store_stats_cache_test"

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
  # start_link/1 Tests
  # ===========================================================================

  describe "start_link/1" do
    test "starts the cache GenServer", %{db: db} do
      {:ok, cache} = Cache.start_link(db: db)
      assert Process.alive?(cache)
      Cache.stop(cache)
    end

    test "accepts name option", %{db: db} do
      {:ok, _cache} = Cache.start_link(db: db, name: :test_stats_cache)
      assert Process.whereis(:test_stats_cache) != nil
      Cache.stop(:test_stats_cache)
    end

    test "accepts refresh_interval option", %{db: db} do
      {:ok, cache} = Cache.start_link(db: db, refresh_interval: 100)
      assert Process.alive?(cache)
      Cache.stop(cache)
    end
  end

  # ===========================================================================
  # get/1 Tests
  # ===========================================================================

  describe "get/1" do
    test "returns cached statistics for empty store", %{db: db} do
      {:ok, cache} = Cache.start_link(db: db)
      # Give time for initial computation
      Process.sleep(50)

      {:ok, stats} = Cache.get(cache)

      assert stats.triple_count == 0
      assert stats.distinct_subjects == 0
      assert stats.distinct_predicates == 0
      assert stats.distinct_objects == 0
      assert %DateTime{} = stats.computed_at

      Cache.stop(cache)
    end

    test "returns cached statistics with data", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1000, 101, 2001},
        {1001, 100, 2002}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(50)

      {:ok, stats} = Cache.get(cache)

      assert stats.triple_count == 3
      assert stats.distinct_subjects == 2
      assert stats.distinct_predicates == 2
      assert stats.distinct_objects == 3

      Cache.stop(cache)
    end

    test "computes stats synchronously if not yet cached", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      {:ok, cache} = Cache.start_link(db: db)
      # Don't wait - call immediately
      {:ok, stats} = Cache.get(cache)

      assert stats.triple_count == 1

      Cache.stop(cache)
    end

    test "returns same cached stats on repeated calls", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(50)

      {:ok, stats1} = Cache.get(cache)
      {:ok, stats2} = Cache.get(cache)

      # Same computed_at means same cached result
      assert stats1.computed_at == stats2.computed_at

      Cache.stop(cache)
    end
  end

  # ===========================================================================
  # predicate_histogram/1 Tests
  # ===========================================================================

  describe "predicate_histogram/1" do
    test "returns empty histogram for empty store", %{db: db} do
      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(50)

      {:ok, histogram} = Cache.predicate_histogram(cache)
      assert histogram == %{}

      Cache.stop(cache)
    end

    test "returns correct histogram", %{db: db} do
      triples = [
        {1000, 100, 2000},
        {1001, 100, 2001},
        {1002, 100, 2002},
        {1003, 200, 2003},
        {1004, 200, 2004},
        {1005, 300, 2005}
      ]

      :ok = Index.insert_triples(db, triples)

      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(50)

      {:ok, histogram} = Cache.predicate_histogram(cache)

      assert histogram[100] == 3
      assert histogram[200] == 2
      assert histogram[300] == 1

      Cache.stop(cache)
    end

    test "computes histogram synchronously if not cached", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      {:ok, cache} = Cache.start_link(db: db)
      # Don't wait - call immediately
      {:ok, histogram} = Cache.predicate_histogram(cache)

      assert histogram[100] == 1

      Cache.stop(cache)
    end
  end

  # ===========================================================================
  # invalidate/1 Tests
  # ===========================================================================

  describe "invalidate/1" do
    test "clears cached data", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(50)

      {:ok, stats1} = Cache.get(cache)
      assert stats1.triple_count == 1

      # Add more data
      :ok = Index.insert_triple(db, {1001, 101, 2001})

      # Invalidate to force refresh
      :ok = Cache.invalidate(cache)
      Process.sleep(50)

      # Next get should compute new stats
      {:ok, stats2} = Cache.get(cache)
      assert stats2.triple_count == 2

      Cache.stop(cache)
    end

    test "clears histogram as well", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(50)

      {:ok, histogram1} = Cache.predicate_histogram(cache)
      assert histogram1[100] == 1

      # Add more data with different predicate
      :ok = Index.insert_triple(db, {1001, 200, 2001})

      # Invalidate
      :ok = Cache.invalidate(cache)

      # Next call should compute new histogram
      {:ok, histogram2} = Cache.predicate_histogram(cache)
      assert histogram2[100] == 1
      assert histogram2[200] == 1

      Cache.stop(cache)
    end
  end

  # ===========================================================================
  # refresh/1 Tests
  # ===========================================================================

  describe "refresh/1" do
    test "forces synchronous refresh", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(50)

      {:ok, stats1} = Cache.get(cache)
      computed_at1 = stats1.computed_at

      # Add more data
      :ok = Index.insert_triple(db, {1001, 101, 2001})

      # Force refresh
      {:ok, stats2} = Cache.refresh(cache)

      assert stats2.triple_count == 2
      assert DateTime.compare(stats2.computed_at, computed_at1) == :gt

      Cache.stop(cache)
    end

    test "updates histogram as well", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(50)

      # Add more data
      :ok = Index.insert_triple(db, {1001, 200, 2001})

      # Force refresh
      {:ok, _stats} = Cache.refresh(cache)

      # Histogram should be updated
      {:ok, histogram} = Cache.predicate_histogram(cache)
      assert histogram[100] == 1
      assert histogram[200] == 1

      Cache.stop(cache)
    end
  end

  # ===========================================================================
  # Periodic Refresh Tests
  # ===========================================================================

  describe "periodic refresh" do
    test "automatically refreshes after interval", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      # Use short interval for testing
      {:ok, cache} = Cache.start_link(db: db, refresh_interval: 100)
      Process.sleep(50)

      {:ok, stats1} = Cache.get(cache)
      assert stats1.triple_count == 1

      # Add more data
      :ok = Index.insert_triple(db, {1001, 101, 2001})

      # Wait for automatic refresh (interval + some buffer)
      Process.sleep(150)

      {:ok, stats2} = Cache.get(cache)
      assert stats2.triple_count == 2

      Cache.stop(cache)
    end
  end

  # ===========================================================================
  # stop/1 Tests
  # ===========================================================================

  describe "stop/1" do
    test "stops the cache", %{db: db} do
      {:ok, cache} = Cache.start_link(db: db)
      assert Process.alive?(cache)

      :ok = Cache.stop(cache)
      refute Process.alive?(cache)
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles large datasets", %{db: db} do
      # Insert 100 triples
      triples =
        for i <- 1..100 do
          {1000 + rem(i, 10), 100 + rem(i, 5), 2000 + i}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(100)

      {:ok, stats} = Cache.get(cache)
      assert stats.triple_count == 100
      assert stats.distinct_subjects == 10
      assert stats.distinct_predicates == 5
      assert stats.distinct_objects == 100

      {:ok, histogram} = Cache.predicate_histogram(cache)
      # Each predicate should have 20 occurrences (100/5)
      assert histogram[100] == 20
      assert histogram[101] == 20

      Cache.stop(cache)
    end

    test "handles rapid invalidation", %{db: db} do
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(50)

      # Rapid invalidations
      for _ <- 1..10 do
        :ok = Cache.invalidate(cache)
      end

      # Should still work
      {:ok, stats} = Cache.get(cache)
      assert stats.triple_count == 1

      Cache.stop(cache)
    end

    test "handles concurrent access", %{db: db} do
      triples =
        for i <- 1..50 do
          {1000 + i, 100 + rem(i, 5), 2000 + i}
        end

      :ok = Index.insert_triples(db, triples)

      {:ok, cache} = Cache.start_link(db: db)
      Process.sleep(50)

      # Concurrent reads
      tasks =
        for _ <- 1..10 do
          Task.async(fn -> Cache.get(cache) end)
        end

      results = Task.await_many(tasks)

      for {:ok, stats} <- results do
        assert stats.triple_count == 50
      end

      Cache.stop(cache)
    end
  end
end
