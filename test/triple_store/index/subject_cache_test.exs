defmodule TripleStore.Index.SubjectCacheTest do
  @moduledoc """
  Tests for Subject Cache LRU caching.

  Task 2.2.3: Subject Cache
  - Implement LRU cache for subject property maps
  - Test cache hit/miss behavior
  - Test cache invalidation
  - Test LRU eviction
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index
  alias TripleStore.Index.SubjectCache

  @moduletag :tmp_dir

  # ===========================================================================
  # Setup Helpers
  # ===========================================================================

  setup do
    # Clear cache before each test
    SubjectCache.clear()
    :ok
  end

  defp setup_db(tmp_dir) do
    db_path = Path.join(tmp_dir, "test_db_#{:erlang.unique_integer([:positive])}")
    {:ok, db} = NIF.open(db_path)
    db
  end

  # ===========================================================================
  # Initialization Tests
  # ===========================================================================

  describe "init/0" do
    test "creates cache tables" do
      SubjectCache.init()

      assert :ets.whereis(:triple_store_subject_cache) != :undefined
      assert :ets.whereis(:triple_store_subject_cache_lru) != :undefined
      assert :ets.whereis(:triple_store_subject_cache_config) != :undefined
    end

    test "is idempotent" do
      SubjectCache.init()
      SubjectCache.init()
      SubjectCache.init()

      # Should not raise
      assert :ets.whereis(:triple_store_subject_cache) != :undefined
    end
  end

  # ===========================================================================
  # Configuration Tests
  # ===========================================================================

  describe "configure/1" do
    test "sets max_entries" do
      SubjectCache.configure(max_entries: 500)

      stats = SubjectCache.stats()
      assert stats.max_entries == 500
    end
  end

  # ===========================================================================
  # get_or_fetch/2 Tests
  # ===========================================================================

  describe "get_or_fetch/2" do
    test "fetches and caches properties on miss", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      # Insert test data
      :ok = Index.insert_triple(db, {1, 10, 100})
      :ok = Index.insert_triple(db, {1, 20, 200})

      # First call should fetch from index
      {:ok, properties} = SubjectCache.get_or_fetch(db, 1)

      assert properties[10] == [100]
      assert properties[20] == [200]

      # Verify it's now cached
      assert SubjectCache.stats().size == 1
    end

    test "returns cached properties on hit", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      :ok = Index.insert_triple(db, {1, 10, 100})

      # Prime the cache
      {:ok, _} = SubjectCache.get_or_fetch(db, 1)

      # Delete from index (but cache still has it)
      :ok = Index.delete_triple(db, {1, 10, 100})

      # Should return cached value, not empty
      {:ok, properties} = SubjectCache.get_or_fetch(db, 1)

      assert properties[10] == [100]
    end

    test "emits telemetry on cache hit", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)
      test_pid = self()
      handler_id = "test-cache-hit-#{:erlang.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :index, :subject_cache, :hit],
        fn _event, measurements, metadata, _ ->
          send(test_pid, {:cache_hit, measurements, metadata})
        end,
        nil
      )

      :ok = Index.insert_triple(db, {1, 10, 100})

      # Prime the cache
      {:ok, _} = SubjectCache.get_or_fetch(db, 1)

      # Second call should be a hit
      {:ok, _} = SubjectCache.get_or_fetch(db, 1)

      :telemetry.detach(handler_id)

      assert_receive {:cache_hit, %{count: 1}, %{subject_id: 1}}, 1000
    end

    test "emits telemetry on cache miss", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)
      test_pid = self()
      handler_id = "test-cache-miss-#{:erlang.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :index, :subject_cache, :miss],
        fn _event, measurements, metadata, _ ->
          send(test_pid, {:cache_miss, measurements, metadata})
        end,
        nil
      )

      :ok = Index.insert_triple(db, {1, 10, 100})

      # First call should be a miss
      {:ok, _} = SubjectCache.get_or_fetch(db, 1)

      :telemetry.detach(handler_id)

      assert_receive {:cache_miss, %{count: 1}, %{subject_id: 1, property_count: 1}}, 1000
    end
  end

  # ===========================================================================
  # get/1 Tests
  # ===========================================================================

  describe "get/1" do
    test "returns :not_found for uncached subject" do
      assert SubjectCache.get(999) == :not_found
    end

    test "returns cached properties", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      :ok = Index.insert_triple(db, {1, 10, 100})
      {:ok, _} = SubjectCache.get_or_fetch(db, 1)

      {:ok, properties} = SubjectCache.get(1)
      assert properties[10] == [100]
    end
  end

  # ===========================================================================
  # put/2 Tests
  # ===========================================================================

  describe "put/2" do
    test "explicitly caches properties" do
      properties = %{10 => [100], 20 => [200]}

      :ok = SubjectCache.put(1, properties)

      {:ok, cached} = SubjectCache.get(1)
      assert cached == properties
    end
  end

  # ===========================================================================
  # invalidate/1 Tests
  # ===========================================================================

  describe "invalidate/1" do
    test "removes subject from cache", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      :ok = Index.insert_triple(db, {1, 10, 100})
      {:ok, _} = SubjectCache.get_or_fetch(db, 1)

      assert SubjectCache.stats().size == 1

      SubjectCache.invalidate(1)

      assert SubjectCache.stats().size == 0
      assert SubjectCache.get(1) == :not_found
    end

    test "handles non-existent subject" do
      # Should not raise
      :ok = SubjectCache.invalidate(999)
    end
  end

  # ===========================================================================
  # clear/0 Tests
  # ===========================================================================

  describe "clear/0" do
    test "removes all entries", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      for subject <- 1..5 do
        :ok = Index.insert_triple(db, {subject, 10, 100})
        {:ok, _} = SubjectCache.get_or_fetch(db, subject)
      end

      assert SubjectCache.stats().size == 5

      SubjectCache.clear()

      assert SubjectCache.stats().size == 0
    end
  end

  # ===========================================================================
  # LRU Eviction Tests
  # ===========================================================================

  describe "LRU eviction" do
    test "evicts oldest entry when at capacity", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      # Configure small cache
      SubjectCache.configure(max_entries: 3)

      # Insert 3 subjects with small delays to ensure different timestamps
      for subject <- 1..3 do
        :ok = Index.insert_triple(db, {subject, 10, subject * 100})
        {:ok, _} = SubjectCache.get_or_fetch(db, subject)
        Process.sleep(1)
      end

      assert SubjectCache.stats().size == 3

      # Add 4th subject - should evict oldest (subject 1)
      :ok = Index.insert_triple(db, {4, 10, 400})
      {:ok, _} = SubjectCache.get_or_fetch(db, 4)

      # Size should still be 3
      assert SubjectCache.stats().size == 3

      # Subject 1 should have been evicted (oldest)
      assert SubjectCache.get(1) == :not_found

      # Subjects 2, 3, 4 should be cached
      assert {:ok, _} = SubjectCache.get(2)
      assert {:ok, _} = SubjectCache.get(3)
      assert {:ok, _} = SubjectCache.get(4)
    end

    test "accessing entry updates its LRU position", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      SubjectCache.configure(max_entries: 3)

      # Insert 3 subjects in order
      for subject <- 1..3 do
        :ok = Index.insert_triple(db, {subject, 10, subject * 100})
        {:ok, _} = SubjectCache.get_or_fetch(db, subject)
        # Small delay to ensure different timestamps
        Process.sleep(1)
      end

      # Access subject 1 to make it most recently used
      {:ok, _} = SubjectCache.get(1)
      Process.sleep(1)

      # Add subject 4 - should evict subject 2 (oldest not recently accessed)
      :ok = Index.insert_triple(db, {4, 10, 400})
      {:ok, _} = SubjectCache.get_or_fetch(db, 4)

      # Subject 1 should still be cached (was accessed)
      assert {:ok, _} = SubjectCache.get(1)

      # Subject 2 should have been evicted (was oldest)
      assert SubjectCache.get(2) == :not_found
    end

    test "emits telemetry on eviction", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)
      test_pid = self()
      handler_id = "test-eviction-#{:erlang.unique_integer()}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :index, :subject_cache, :eviction],
        fn _event, measurements, metadata, _ ->
          send(test_pid, {:eviction, measurements, metadata})
        end,
        nil
      )

      SubjectCache.configure(max_entries: 2)

      for subject <- 1..2 do
        :ok = Index.insert_triple(db, {subject, 10, subject * 100})
        {:ok, _} = SubjectCache.get_or_fetch(db, subject)
      end

      # Adding 3rd subject should trigger eviction
      :ok = Index.insert_triple(db, {3, 10, 300})
      {:ok, _} = SubjectCache.get_or_fetch(db, 3)

      :telemetry.detach(handler_id)

      assert_receive {:eviction, %{count: 1}, %{subject_id: _}}, 1000
    end
  end

  # ===========================================================================
  # stats/0 Tests
  # ===========================================================================

  describe "stats/0" do
    test "returns size and max_entries", %{tmp_dir: tmp_dir} do
      db = setup_db(tmp_dir)

      SubjectCache.configure(max_entries: 100)

      for subject <- 1..5 do
        :ok = Index.insert_triple(db, {subject, 10, 100})
        {:ok, _} = SubjectCache.get_or_fetch(db, subject)
      end

      stats = SubjectCache.stats()

      assert stats.size == 5
      assert stats.max_entries == 100
    end
  end
end
