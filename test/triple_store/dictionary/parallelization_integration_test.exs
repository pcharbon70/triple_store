defmodule TripleStore.Dictionary.ParallelizationIntegrationTest do
  @moduledoc """
  Integration tests for Dictionary Parallelization (Section 1.1).

  This test file consolidates the test requirements from Task 1.1.4:
  - 1.1.4.1 Shard distribution evenness
  - 1.1.4.2 Concurrent get_or_create_id operations
  - 1.1.4.3 ETS cache hit behavior
  - 1.1.4.4 ETS cache miss fallthrough
  - 1.1.4.5 Cache population on creation
  - 1.1.4.6 Batch partitioning by shard
  - 1.1.4.7 Sequence allocation under contention
  - 1.1.4.8 Dictionary consistency after parallel operations

  These tests verify the integration of:
  - ShardedManager (Task 1.1.1)
  - Lock-Free Read Cache (Task 1.1.2)
  - Batch Sequence Allocation (Task 1.1.3)
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Dictionary.SequenceCounter
  alias TripleStore.Dictionary.ShardedManager
  alias TripleStore.Dictionary.StringToId

  @test_db_base "/tmp/triple_store_parallelization_integration_test"

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
  # 1.1.4.1: Shard Distribution Evenness
  # ===========================================================================

  describe "1.1.4.1 shard distribution evenness" do
    test "terms are distributed across shards with reasonable evenness", %{db: db} do
      shard_count = 4
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: shard_count)

      # Generate 1000 unique URIs
      terms = for i <- 1..1000, do: RDF.iri("http://example.org/even/#{i}")

      # Process all terms and track which shard each goes to
      shard_counts =
        terms
        |> Enum.reduce(%{}, fn term, acc ->
          # Get the shard index for this term using the internal routing
          {:ok, _key} = StringToId.encode_term(term)
          hash_key = term_hash_key(term)
          shard_idx = :erlang.phash2(hash_key, shard_count)

          Map.update(acc, shard_idx, 1, &(&1 + 1))
        end)

      # Each shard should have roughly 250 terms (1000/4)
      # Allow 30% deviation from perfect distribution
      expected_per_shard = 1000 / shard_count
      min_expected = trunc(expected_per_shard * 0.7)
      max_expected = trunc(expected_per_shard * 1.3)

      for shard_idx <- 0..(shard_count - 1) do
        count = Map.get(shard_counts, shard_idx, 0)

        assert count >= min_expected,
               "Shard #{shard_idx} has #{count} terms, expected at least #{min_expected}"

        assert count <= max_expected,
               "Shard #{shard_idx} has #{count} terms, expected at most #{max_expected}"
      end

      ShardedManager.stop(sharded)
    end

    test "consistent hashing ensures same term always goes to same shard", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 8)

      uri = RDF.iri("http://example.org/consistent/test")

      # Get ID multiple times
      results =
        for _ <- 1..100 do
          {:ok, id} = ShardedManager.get_or_create_id(sharded, uri)
          id
        end

      # All results should be identical (same shard, same ID)
      assert length(Enum.uniq(results)) == 1

      ShardedManager.stop(sharded)
    end
  end

  # ===========================================================================
  # 1.1.4.2: Concurrent get_or_create_id Operations
  # ===========================================================================

  describe "1.1.4.2 concurrent get_or_create_id" do
    test "concurrent operations on same term return same ID", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      uri = RDF.iri("http://example.org/concurrent/same")

      # 50 concurrent requests for same term
      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            ShardedManager.get_or_create_id(sharded, uri)
          end)
        end

      results = Task.await_many(tasks)
      ids = for {:ok, id} <- results, do: id

      # All should return same ID
      assert length(Enum.uniq(ids)) == 1

      ShardedManager.stop(sharded)
    end

    test "concurrent operations on different terms return unique IDs", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      # 100 concurrent requests for different terms
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            uri = RDF.iri("http://example.org/concurrent/different/#{i}")
            ShardedManager.get_or_create_id(sharded, uri)
          end)
        end

      results = Task.await_many(tasks)
      ids = for {:ok, id} <- results, do: id

      # All IDs should be unique
      assert length(ids) == 100
      assert length(Enum.uniq(ids)) == 100

      ShardedManager.stop(sharded)
    end

    test "high concurrency stress test", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 8)

      # 20 processes each doing 100 operations
      tasks =
        for proc_id <- 1..20 do
          Task.async(fn ->
            for op_id <- 1..100 do
              uri = RDF.iri("http://example.org/stress/#{proc_id}/#{op_id}")
              {:ok, _id} = ShardedManager.get_or_create_id(sharded, uri)
            end

            :ok
          end)
        end

      results = Task.await_many(tasks, 60_000)
      assert Enum.all?(results, &(&1 == :ok))

      ShardedManager.stop(sharded)
    end
  end

  # ===========================================================================
  # 1.1.4.3 & 1.1.4.4: Cache Hit and Miss Behavior
  # ===========================================================================

  describe "1.1.4.3/1.1.4.4 cache hit and miss behavior" do
    test "first access is cache miss, subsequent is cache hit", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      test_pid = self()

      handler = fn _event, _measurements, metadata, _config ->
        send(test_pid, {:cache_event, metadata.type})
      end

      :telemetry.attach("cache-behavior-test", [:triple_store, :dictionary, :cache], handler, nil)

      uri = RDF.iri("http://example.org/cache/behavior")

      # First access - should be miss
      {:ok, id1} = Manager.get_or_create_id(manager, uri)
      assert_receive {:cache_event, :miss}

      # Second access - should be hit
      {:ok, id2} = Manager.get_or_create_id(manager, uri)
      assert_receive {:cache_event, :hit}

      # Third access - should be hit
      {:ok, id3} = Manager.get_or_create_id(manager, uri)
      assert_receive {:cache_event, :hit}

      assert id1 == id2
      assert id2 == id3

      :telemetry.detach("cache-behavior-test")
      Manager.stop(manager)
    end

    test "cache miss falls through to GenServer and RocksDB", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)

      uri = RDF.iri("http://example.org/cache/fallthrough")

      # First call - cache miss, should create via GenServer
      {:ok, id1} = Manager.get_or_create_id(manager, uri)

      # Verify it's in RocksDB
      {:ok, key} = StringToId.encode_term(uri)
      {:ok, <<stored_id::64-big>>} = NIF.get(db, :str2id, key)
      assert stored_id == id1

      Manager.stop(manager)
    end

    test "cache works correctly after manager restart", %{db: db} do
      # First session
      {:ok, manager1} = Manager.start_link(db: db)
      uri = RDF.iri("http://example.org/cache/restart")
      {:ok, id1} = Manager.get_or_create_id(manager1, uri)
      Manager.stop(manager1)

      # Second session - cache is empty, but RocksDB has the ID
      {:ok, manager2} = Manager.start_link(db: db)

      test_pid = self()

      handler = fn _event, _measurements, metadata, _config ->
        send(test_pid, {:cache_event, metadata.type})
      end

      :telemetry.attach("cache-restart-test", [:triple_store, :dictionary, :cache], handler, nil)

      # First access after restart - cache miss (cache is fresh)
      {:ok, id2} = Manager.get_or_create_id(manager2, uri)
      assert_receive {:cache_event, :miss}

      # But should still get same ID from RocksDB
      assert id1 == id2

      # Now it should be in cache
      {:ok, id3} = Manager.get_or_create_id(manager2, uri)
      assert_receive {:cache_event, :hit}
      assert id2 == id3

      :telemetry.detach("cache-restart-test")
      Manager.stop(manager2)
    end
  end

  # ===========================================================================
  # 1.1.4.5: Cache Population on Creation
  # ===========================================================================

  describe "1.1.4.5 cache population on creation" do
    test "newly created IDs are immediately in cache", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)
      {:ok, cache} = Manager.get_cache(manager)

      # Cache starts empty
      assert :ets.info(cache, :size) == 0

      uri = RDF.iri("http://example.org/cache/creation")
      {:ok, id} = Manager.get_or_create_id(manager, uri)

      # Cache should now have the entry
      assert :ets.info(cache, :size) == 1

      # Verify the cached value is correct
      {:ok, key} = StringToId.encode_term(uri)
      [{^key, cached_id}] = :ets.lookup(cache, key)
      assert cached_id == id

      Manager.stop(manager)
    end

    test "batch creation populates cache for all terms", %{db: db} do
      {:ok, manager} = Manager.start_link(db: db)
      {:ok, cache} = Manager.get_cache(manager)

      terms = for i <- 1..50, do: RDF.iri("http://example.org/batch/cache/#{i}")
      {:ok, ids} = Manager.get_or_create_ids(manager, terms)

      # All 50 should be in cache
      assert :ets.info(cache, :size) == 50

      # Verify each cached value
      for {term, expected_id} <- Enum.zip(terms, ids) do
        {:ok, key} = StringToId.encode_term(term)
        [{^key, cached_id}] = :ets.lookup(cache, key)
        assert cached_id == expected_id
      end

      Manager.stop(manager)
    end

    test "RocksDB lookup also populates cache", %{db: db} do
      # First manager creates the term
      {:ok, manager1} = Manager.start_link(db: db)
      uri = RDF.iri("http://example.org/cache/rocksdb/lookup")
      {:ok, id1} = Manager.get_or_create_id(manager1, uri)
      Manager.stop(manager1)

      # Second manager - term exists in RocksDB, not in cache
      {:ok, manager2} = Manager.start_link(db: db)
      {:ok, cache} = Manager.get_cache(manager2)

      assert :ets.info(cache, :size) == 0

      # Lookup should populate cache
      {:ok, id2} = Manager.get_or_create_id(manager2, uri)
      assert id1 == id2
      assert :ets.info(cache, :size) == 1

      Manager.stop(manager2)
    end
  end

  # ===========================================================================
  # 1.1.4.6: Batch Partitioning by Shard
  # ===========================================================================

  describe "1.1.4.6 batch partitioning by shard" do
    test "batch operations correctly partition terms across shards", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      # Create terms that will go to different shards
      terms = for i <- 1..100, do: RDF.iri("http://example.org/partition/#{i}")

      {:ok, ids} = ShardedManager.get_or_create_ids(sharded, terms)

      # All IDs should be valid and unique
      assert length(ids) == 100
      assert length(Enum.uniq(ids)) == 100

      # Verify all terms are retrievable
      for {term, expected_id} <- Enum.zip(terms, ids) do
        {:ok, retrieved_id} = ShardedManager.get_or_create_id(sharded, term)
        assert retrieved_id == expected_id
      end

      ShardedManager.stop(sharded)
    end

    test "batch ordering is preserved across shards", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      terms = [
        RDF.iri("http://example.org/order/first"),
        RDF.bnode("middle_bnode"),
        RDF.literal("last literal")
      ]

      {:ok, ids1} = ShardedManager.get_or_create_ids(sharded, terms)

      # Request same terms individually
      individual_ids =
        for term <- terms do
          {:ok, id} = ShardedManager.get_or_create_id(sharded, term)
          id
        end

      assert ids1 == individual_ids

      ShardedManager.stop(sharded)
    end

    test "mixed existing and new terms in batch", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      # Create some terms first
      existing_terms = for i <- 1..10, do: RDF.iri("http://example.org/existing/#{i}")
      {:ok, existing_ids} = ShardedManager.get_or_create_ids(sharded, existing_terms)

      # Create batch with mix of existing and new
      new_terms = for i <- 1..10, do: RDF.iri("http://example.org/new/#{i}")
      mixed_terms = Enum.zip(existing_terms, new_terms) |> Enum.flat_map(fn {a, b} -> [a, b] end)

      {:ok, mixed_ids} = ShardedManager.get_or_create_ids(sharded, mixed_terms)

      # Verify existing terms have same IDs
      existing_in_mix = for i <- 0..9, do: Enum.at(mixed_ids, i * 2)
      assert existing_in_mix == existing_ids

      # Verify new terms have unique IDs
      new_in_mix = for i <- 0..9, do: Enum.at(mixed_ids, i * 2 + 1)
      assert length(Enum.uniq(new_in_mix)) == 10

      ShardedManager.stop(sharded)
    end
  end

  # ===========================================================================
  # 1.1.4.7: Sequence Allocation Under Contention
  # ===========================================================================

  describe "1.1.4.7 sequence allocation under contention" do
    test "concurrent range allocations produce non-overlapping sequences", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # 20 concurrent allocations of 100 each
      tasks =
        for _ <- 1..20 do
          Task.async(fn ->
            SequenceCounter.allocate_range(counter, :uri, 100)
          end)
        end

      results = Task.await_many(tasks)
      starts = for {:ok, start} <- results, do: start

      # Create ranges and verify no overlap
      ranges = for start <- starts, do: MapSet.new(start..(start + 99))

      for {r1, i1} <- Enum.with_index(ranges),
          {r2, i2} <- Enum.with_index(ranges),
          i1 < i2 do
        intersection = MapSet.intersection(r1, r2)

        assert MapSet.size(intersection) == 0,
               "Ranges #{i1} and #{i2} overlap at #{inspect(MapSet.to_list(intersection))}"
      end

      SequenceCounter.stop(counter)
    end

    test "sequence counter handles mixed single and range allocations", %{db: db} do
      {:ok, counter} = SequenceCounter.start_link(db: db)

      # Interleave single and range allocations concurrently
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            if rem(i, 2) == 0 do
              {:single, SequenceCounter.next_id(counter, :uri)}
            else
              {:range, SequenceCounter.allocate_range(counter, :uri, 10)}
            end
          end)
        end

      results = Task.await_many(tasks)

      # Collect all allocated sequences
      all_seqs =
        Enum.flat_map(results, fn
          {:single, {:ok, id}} ->
            {_type, seq} = Dictionary.decode_id(id)
            [seq]

          {:range, {:ok, start}} ->
            Enum.to_list(start..(start + 9))
        end)

      # All sequences should be unique
      assert length(all_seqs) == length(Enum.uniq(all_seqs))

      SequenceCounter.stop(counter)
    end
  end

  # ===========================================================================
  # 1.1.4.8: Dictionary Consistency After Parallel Operations
  # ===========================================================================

  describe "1.1.4.8 dictionary consistency after parallel operations" do
    test "all terms have consistent IDs after parallel creation", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      # Create 500 terms across 10 concurrent processes
      tasks =
        for proc_id <- 1..10 do
          Task.async(fn ->
            terms = for i <- 1..50, do: RDF.iri("http://example.org/consistency/#{proc_id}/#{i}")
            ShardedManager.get_or_create_ids(sharded, terms)
          end)
        end

      all_results = Task.await_many(tasks)
      all_ids = Enum.flat_map(all_results, fn {:ok, ids} -> ids end)

      # All 500 IDs should be unique
      assert length(all_ids) == 500
      assert length(Enum.uniq(all_ids)) == 500

      # Verify each term retrieves the same ID
      for proc_id <- 1..10, i <- 1..50 do
        term = RDF.iri("http://example.org/consistency/#{proc_id}/#{i}")
        {:ok, id} = ShardedManager.get_or_create_id(sharded, term)
        assert id in all_ids
      end

      ShardedManager.stop(sharded)
    end

    test "dictionary encode/decode roundtrip after parallel operations", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      # Create various term types in parallel
      tasks = [
        Task.async(fn ->
          terms = for i <- 1..50, do: RDF.iri("http://example.org/roundtrip/uri/#{i}")
          {:uri, ShardedManager.get_or_create_ids(sharded, terms), terms}
        end),
        Task.async(fn ->
          terms = for i <- 1..50, do: RDF.bnode("roundtrip_bnode_#{i}")
          {:bnode, ShardedManager.get_or_create_ids(sharded, terms), terms}
        end),
        Task.async(fn ->
          terms = for i <- 1..50, do: RDF.literal("roundtrip literal #{i}")
          {:literal, ShardedManager.get_or_create_ids(sharded, terms), terms}
        end)
      ]

      results = Task.await_many(tasks)

      # Verify each term's ID decodes to correct type
      for {type, {:ok, ids}, _terms} <- results do
        for id <- ids do
          {decoded_type, _seq} = Dictionary.decode_id(id)

          assert decoded_type == type,
                 "Expected type #{type}, got #{decoded_type} for ID #{id}"
        end
      end

      ShardedManager.stop(sharded)
    end

    test "str2id and id2str remain consistent after parallel writes", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      terms = for i <- 1..100, do: RDF.iri("http://example.org/bidirectional/#{i}")

      # Create all terms in parallel batches
      tasks =
        terms
        |> Enum.chunk_every(20)
        |> Enum.map(fn chunk ->
          Task.async(fn ->
            ShardedManager.get_or_create_ids(sharded, chunk)
          end)
        end)

      all_results = Task.await_many(tasks)
      all_ids = Enum.flat_map(all_results, fn {:ok, ids} -> ids end)

      # Verify bidirectional consistency
      for {term, id} <- Enum.zip(terms, all_ids) do
        {:ok, key} = StringToId.encode_term(term)
        id_binary = <<id::64-big>>

        # str2id lookup
        {:ok, <<stored_id::64-big>>} = NIF.get(db, :str2id, key)
        assert stored_id == id

        # id2str lookup
        {:ok, stored_key} = NIF.get(db, :id2str, id_binary)
        assert stored_key == key
      end

      ShardedManager.stop(sharded)
    end

    test "no ID reuse after parallel operations", %{db: db} do
      {:ok, sharded} = ShardedManager.start_link(db: db, shards: 4)

      # Create terms in multiple waves
      wave1_terms = for i <- 1..100, do: RDF.iri("http://example.org/wave1/#{i}")
      wave2_terms = for i <- 1..100, do: RDF.iri("http://example.org/wave2/#{i}")
      wave3_terms = for i <- 1..100, do: RDF.iri("http://example.org/wave3/#{i}")

      # Process waves concurrently
      tasks = [
        Task.async(fn -> ShardedManager.get_or_create_ids(sharded, wave1_terms) end),
        Task.async(fn -> ShardedManager.get_or_create_ids(sharded, wave2_terms) end),
        Task.async(fn -> ShardedManager.get_or_create_ids(sharded, wave3_terms) end)
      ]

      results = Task.await_many(tasks)
      all_ids = Enum.flat_map(results, fn {:ok, ids} -> ids end)

      # All 300 IDs should be unique
      assert length(all_ids) == 300
      assert length(Enum.uniq(all_ids)) == 300

      ShardedManager.stop(sharded)
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  # Replicate the hash key generation from ShardedManager
  defp term_hash_key(%RDF.IRI{} = iri), do: {:iri, to_string(iri)}
  defp term_hash_key(%RDF.BlankNode{} = bnode), do: {:bnode, to_string(bnode)}

  defp term_hash_key(%RDF.Literal{} = literal) do
    {:literal, RDF.Literal.lexical(literal), RDF.Literal.datatype_id(literal),
     RDF.Literal.language(literal)}
  end
end
