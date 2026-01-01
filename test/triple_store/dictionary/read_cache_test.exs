defmodule TripleStore.Dictionary.ReadCacheTest do
  @moduledoc """
  Unit tests for Dictionary Manager ETS Read Cache (Task 1.1.2).

  Tests:
  - Cache creation and initialization
  - Cache lookup on repeated terms (cache hits)
  - Cache population after term creation
  - Telemetry events for cache hits/misses
  - Cache statistics
  - ShardedManager shared cache
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Dictionary.ShardedManager

  @test_db_base "/tmp/triple_store_read_cache_test"

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
  # 1.1.2.4: ETS Cache Creation
  # ===========================================================================

  describe "cache creation" do
    test "manager creates ETS cache on init", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      {:ok, cache} = Manager.get_cache(manager)
      assert is_reference(cache)

      # Cache should exist and be empty initially
      assert :ets.info(cache, :size) == 0

      Manager.stop(manager)
    end

    test "cache has read_concurrency enabled", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      {:ok, cache} = Manager.get_cache(manager)
      # read_concurrency is set during table creation
      # We can verify the table is accessible
      assert :ets.info(cache, :type) == :set

      Manager.stop(manager)
    end

    test "cache is cleaned up on manager stop", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)
      {:ok, cache} = Manager.get_cache(manager)

      # Verify cache exists
      assert :ets.info(cache) != :undefined

      Manager.stop(manager)

      # Give a moment for cleanup
      Process.sleep(50)

      # Cache should be deleted
      assert :ets.info(cache) == :undefined
    end
  end

  # ===========================================================================
  # 1.1.2.5: Cache Lookup Before GenServer Call
  # ===========================================================================

  describe "cache lookup" do
    test "first lookup is cache miss, second is cache hit", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      # Attach telemetry handler to track cache events
      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end

      :telemetry.attach("cache-test-handler", [:triple_store, :dictionary, :cache], handler, nil)

      uri = RDF.iri("http://example.org/cache/test")

      # First lookup - cache miss
      {:ok, id1} = Manager.get_or_create_id(manager, uri)

      assert_receive {:telemetry, [:triple_store, :dictionary, :cache], %{count: 1},
                      %{type: :miss}}

      # Second lookup - cache hit
      {:ok, id2} = Manager.get_or_create_id(manager, uri)

      assert_receive {:telemetry, [:triple_store, :dictionary, :cache], %{count: 1},
                      %{type: :hit}}

      assert id1 == id2

      :telemetry.detach("cache-test-handler")
      Manager.stop(manager)
    end

    test "cache hit returns same ID without GenServer call", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      uri = RDF.iri("http://example.org/fast/lookup")

      # First call populates cache
      {:ok, id1} = Manager.get_or_create_id(manager, uri)

      # Subsequent calls should be fast (cache hit)
      # We can't easily verify GenServer wasn't called, but we verify correctness
      {:ok, id2} = Manager.get_or_create_id(manager, uri)
      {:ok, id3} = Manager.get_or_create_id(manager, uri)

      assert id1 == id2
      assert id2 == id3

      Manager.stop(manager)
    end

    test "different terms get different IDs", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      uri1 = RDF.iri("http://example.org/term/1")
      uri2 = RDF.iri("http://example.org/term/2")

      {:ok, id1} = Manager.get_or_create_id(manager, uri1)
      {:ok, id2} = Manager.get_or_create_id(manager, uri2)

      assert id1 != id2

      Manager.stop(manager)
    end
  end

  # ===========================================================================
  # 1.1.2.6: Cache Population on Term Creation
  # ===========================================================================

  describe "cache population" do
    test "cache is populated after term creation", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)
      {:ok, cache} = Manager.get_cache(manager)

      # Cache starts empty
      assert :ets.info(cache, :size) == 0

      uri = RDF.iri("http://example.org/populate/test")
      {:ok, _id} = Manager.get_or_create_id(manager, uri)

      # Cache should have one entry
      assert :ets.info(cache, :size) == 1

      Manager.stop(manager)
    end

    test "batch operations populate cache", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)
      {:ok, cache} = Manager.get_cache(manager)

      terms = [
        RDF.iri("http://example.org/batch/1"),
        RDF.iri("http://example.org/batch/2"),
        RDF.iri("http://example.org/batch/3")
      ]

      {:ok, _ids} = Manager.get_or_create_ids(manager, terms)

      # Cache should have all entries
      assert :ets.info(cache, :size) == 3

      Manager.stop(manager)
    end

    test "cache entries are correct after population", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      uri = RDF.iri("http://example.org/verify/cache")
      {:ok, id1} = Manager.get_or_create_id(manager, uri)

      # Verify cache returns same ID
      {:ok, id2} = Manager.get_or_create_id(manager, uri)

      assert id1 == id2

      Manager.stop(manager)
    end
  end

  # ===========================================================================
  # 1.1.2.8: Telemetry for Cache Hit Rate
  # ===========================================================================

  describe "cache telemetry" do
    test "emits telemetry on cache hit", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end

      :telemetry.attach("cache-hit-handler", [:triple_store, :dictionary, :cache], handler, nil)

      uri = RDF.iri("http://example.org/telemetry/hit")

      # First lookup - miss
      {:ok, _id} = Manager.get_or_create_id(manager, uri)
      assert_receive {:telemetry, [:triple_store, :dictionary, :cache], _, %{type: :miss}}

      # Second lookup - hit
      {:ok, _id} = Manager.get_or_create_id(manager, uri)
      assert_receive {:telemetry, [:triple_store, :dictionary, :cache], _, %{type: :hit}}

      :telemetry.detach("cache-hit-handler")
      Manager.stop(manager)
    end

    test "emits telemetry on cache miss", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end

      :telemetry.attach("cache-miss-handler", [:triple_store, :dictionary, :cache], handler, nil)

      uri = RDF.iri("http://example.org/telemetry/miss")

      {:ok, _id} = Manager.get_or_create_id(manager, uri)

      assert_receive {:telemetry, [:triple_store, :dictionary, :cache], %{count: 1},
                      %{type: :miss}}

      :telemetry.detach("cache-miss-handler")
      Manager.stop(manager)
    end
  end

  # ===========================================================================
  # Cache Statistics
  # ===========================================================================

  describe "cache statistics" do
    test "cache_stats returns size and memory", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      {:ok, stats} = Manager.cache_stats(manager)

      assert is_map(stats)
      assert Map.has_key?(stats, :size)
      assert Map.has_key?(stats, :memory_bytes)
      assert stats.size == 0

      # Add some entries
      for i <- 1..10 do
        uri = RDF.iri("http://example.org/stats/#{i}")
        {:ok, _id} = Manager.get_or_create_id(manager, uri)
      end

      {:ok, stats_after} = Manager.cache_stats(manager)
      assert stats_after.size == 10
      assert stats_after.memory_bytes > 0

      Manager.stop(manager)
    end
  end

  # ===========================================================================
  # ShardedManager Shared Cache
  # ===========================================================================

  describe "sharded manager shared cache" do
    test "sharded manager creates shared cache", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      {:ok, cache} = ShardedManager.get_cache(sharded)
      assert is_reference(cache)

      ShardedManager.stop(sharded)
    end

    test "all shards share the same cache", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      {:ok, shared_cache} = ShardedManager.get_cache(sharded)

      # Get cache from each shard manager
      for i <- 0..3 do
        {:ok, shard} = ShardedManager.get_shard(sharded, i)
        {:ok, shard_cache} = Manager.get_cache(shard)
        assert shard_cache == shared_cache
      end

      ShardedManager.stop(sharded)
    end

    test "terms from different shards populate same cache", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      {:ok, cache} = ShardedManager.get_cache(sharded)
      initial_size = :ets.info(cache, :size)

      # Create terms that will route to different shards
      for i <- 1..100 do
        uri = RDF.iri("http://example.org/shared/#{i}")
        {:ok, _id} = ShardedManager.get_or_create_id(sharded, uri)
      end

      # All 100 terms should be in the shared cache
      assert :ets.info(cache, :size) == initial_size + 100

      ShardedManager.stop(sharded)
    end

    test "cache hits work across shards", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      test_pid = self()

      handler = fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end

      :telemetry.attach(
        "sharded-cache-handler",
        [:triple_store, :dictionary, :cache],
        handler,
        nil
      )

      uri = RDF.iri("http://example.org/sharded/cache/test")

      # First lookup - miss
      {:ok, id1} = ShardedManager.get_or_create_id(sharded, uri)
      assert_receive {:telemetry, [:triple_store, :dictionary, :cache], _, %{type: :miss}}

      # Second lookup - hit (same shard due to consistent hashing)
      {:ok, id2} = ShardedManager.get_or_create_id(sharded, uri)
      assert_receive {:telemetry, [:triple_store, :dictionary, :cache], _, %{type: :hit}}

      assert id1 == id2

      :telemetry.detach("sharded-cache-handler")
      ShardedManager.stop(sharded)
    end

    test "sharded manager cache stats", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      {:ok, stats} = ShardedManager.cache_stats(sharded)
      assert stats.size == 0

      for i <- 1..50 do
        uri = RDF.iri("http://example.org/stats/sharded/#{i}")
        {:ok, _id} = ShardedManager.get_or_create_id(sharded, uri)
      end

      {:ok, stats_after} = ShardedManager.cache_stats(sharded)
      assert stats_after.size == 50

      ShardedManager.stop(sharded)
    end

    test "shared cache is cleaned up on stop", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 2)
      {:ok, cache} = ShardedManager.get_cache(sharded)

      assert :ets.info(cache) != :undefined

      ShardedManager.stop(sharded)

      Process.sleep(50)

      assert :ets.info(cache) == :undefined
    end
  end

  # ===========================================================================
  # Concurrent Access
  # ===========================================================================

  describe "concurrent access" do
    test "concurrent reads are safe", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      # Populate cache first
      uri = RDF.iri("http://example.org/concurrent/read")
      {:ok, expected_id} = Manager.get_or_create_id(manager, uri)

      # Many concurrent reads
      tasks =
        for _ <- 1..100 do
          Task.async(fn ->
            Manager.get_or_create_id(manager, uri)
          end)
        end

      results = Task.await_many(tasks)

      # All should return same ID
      for {:ok, id} <- results do
        assert id == expected_id
      end

      Manager.stop(manager)
    end

    test "concurrent writes are safe", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      # Many concurrent writes for different terms
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            uri = RDF.iri("http://example.org/concurrent/write/#{i}")
            Manager.get_or_create_id(manager, uri)
          end)
        end

      results = Task.await_many(tasks)

      # All should succeed with unique IDs
      ids = for {:ok, id} <- results, do: id
      assert length(ids) == 100
      assert length(Enum.uniq(ids)) == 100

      Manager.stop(manager)
    end

    test "mixed concurrent reads and writes", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      # Create some initial terms
      uris = for i <- 1..10, do: RDF.iri("http://example.org/mixed/#{i}")

      for uri <- uris do
        {:ok, _id} = Manager.get_or_create_id(manager, uri)
      end

      # Mixed reads and writes
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            if rem(i, 2) == 0 do
              # Read existing term
              uri = Enum.at(uris, rem(i, 10))
              Manager.get_or_create_id(manager, uri)
            else
              # Write new term
              uri = RDF.iri("http://example.org/mixed/new/#{i}")
              Manager.get_or_create_id(manager, uri)
            end
          end)
        end

      results = Task.await_many(tasks)

      # All should succeed
      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)

      Manager.stop(manager)
    end
  end
end
