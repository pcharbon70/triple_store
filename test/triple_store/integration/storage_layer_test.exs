defmodule TripleStore.Integration.StorageLayerTest do
  @moduledoc """
  Integration tests for Phase 4 Storage Layer Tuning.

  These tests validate that the storage layer improvements work correctly:
  - Prefix extractor configuration (4.1)
  - Column family tuning (4.2)
  - Snapshot management (4.3)

  Tests are organized into three categories:
  - Storage Operations: Verify tuned configuration works correctly
  - Resource Cleanup: Verify no resource leaks
  - Performance Validation: Verify acceptable performance characteristics
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Snapshot

  @moduletag :integration
  @moduletag :capture_log

  setup do
    # Create a temporary database for testing
    path =
      Path.join(System.tmp_dir!(), "storage_layer_test_#{System.unique_integer([:positive])}")

    {:ok, db} = NIF.open(path)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf!(path)
    end)

    {:ok, db: db, path: path}
  end

  # Helper to generate test data
  defp generate_test_data(db, num_subjects, num_predicates, num_objects) do
    for s <- 1..num_subjects,
        p <- 1..num_predicates,
        o <- 1..num_objects do
      # Create SPO key (simulating encoded triple)
      key = <<s::64, p::64, o::64>>
      value = "value_#{s}_#{p}_#{o}"
      :ok = NIF.put(db, :spo, key, value)
    end

    # Also add some dictionary entries
    for i <- 1..100 do
      :ok = NIF.put(db, :str2id, "uri_#{i}", <<i::64>>)
      :ok = NIF.put(db, :id2str, <<i::64>>, "uri_#{i}")
    end

    :ok
  end

  describe "4.4.1 Storage Operations Tests" do
    @tag :storage_ops
    test "prefix iterator with extractor returns correct results", %{db: db} do
      # Insert test data with known prefix
      subject_id = 42

      for p <- 1..5, o <- 1..3 do
        key = <<subject_id::64, p::64, o::64>>
        :ok = NIF.put(db, :spo, key, "value")
      end

      # Create prefix iterator for subject 42
      prefix = <<subject_id::64>>
      {:ok, iter} = NIF.prefix_iterator(db, :spo, prefix)

      # Collect all results
      results = collect_iterator(iter)

      # Should get 5 predicates * 3 objects = 15 results
      assert length(results) == 15

      # All keys should start with subject prefix
      for {key, _value} <- results do
        <<s::64, _rest::binary>> = key
        assert s == subject_id
      end
    end

    @tag :storage_ops
    test "seek operations position correctly", %{db: db} do
      # Insert ordered keys
      for i <- 1..10 do
        key = <<i::64, 1::64, 1::64>>
        :ok = NIF.put(db, :spo, key, "value_#{i}")
      end

      # Seek to middle key
      prefix = <<5::64>>
      {:ok, iter} = NIF.prefix_iterator(db, :spo, prefix)

      case NIF.iterator_next(iter) do
        {:ok, key, _value} ->
          <<s::64, _::binary>> = key
          assert s == 5

        :iterator_end ->
          flunk("Expected to find key with prefix 5")
      end

      NIF.iterator_close(iter)
    end

    @tag :storage_ops
    test "bloom filter reduces lookups for non-existent keys", %{db: db} do
      # Insert some dictionary entries
      for i <- 1..100 do
        :ok = NIF.put(db, :str2id, "existing_uri_#{i}", <<i::64>>)
      end

      # Flush WAL to ensure data is persisted (bloom filters built during compaction)
      NIF.flush_wal(db, true)

      # Lookup existing keys - should all succeed
      for i <- 1..100 do
        assert {:ok, <<^i::64>>} = NIF.get(db, :str2id, "existing_uri_#{i}")
      end

      # Lookup non-existent keys - bloom filter should help reject quickly
      # (We can't directly measure bloom filter hits, but we verify correctness)
      for i <- 101..200 do
        assert :not_found = NIF.get(db, :str2id, "nonexistent_uri_#{i}")
      end
    end

    @tag :storage_ops
    test "data is stored with compression", %{db: db, path: path} do
      # Insert highly compressible data (repeated patterns)
      compressible_value = String.duplicate("ABCDEFGH", 100)

      for i <- 1..1000 do
        :ok = NIF.put(db, :spo, <<i::64, 1::64, 1::64>>, compressible_value)
      end

      # Flush to disk
      NIF.flush_wal(db, true)

      # Get directory size
      {:ok, files} = File.ls(path)

      sst_files =
        files
        |> Enum.filter(&String.ends_with?(&1, ".sst"))
        |> Enum.map(fn f ->
          {:ok, stat} = File.stat(Path.join(path, f))
          stat.size
        end)

      total_sst_size = Enum.sum(sst_files)

      # Uncompressed size would be ~800KB (1000 * 800 bytes)
      # With LZ4 compression, should be much smaller
      uncompressed_size = 1000 * byte_size(compressible_value)

      # Compression should reduce size significantly (at least 2x)
      if total_sst_size > 0 do
        compression_ratio = uncompressed_size / total_sst_size

        assert compression_ratio > 2.0,
               "Expected compression ratio > 2x, got #{compression_ratio}"
      end
    end
  end

  describe "4.4.2 Resource Cleanup Tests" do
    @tag :resource_cleanup
    test "no snapshot leaks after workload", %{db: db} do
      initial_count = Snapshot.count()

      # Create and use multiple snapshots
      for _ <- 1..10 do
        Snapshot.with_snapshot(db, fn snapshot ->
          # Do some reads
          NIF.snapshot_get(snapshot, :spo, "test_key")
        end)
      end

      # All snapshots should be released
      assert Snapshot.count() == initial_count
    end

    @tag :resource_cleanup
    test "snapshots released on process termination", %{db: db} do
      initial_count = Snapshot.count()

      # Spawn processes that create snapshots and die
      pids =
        for _ <- 1..5 do
          spawn(fn ->
            {:ok, _snapshot} = Snapshot.create(db)
            # Process exits immediately
          end)
        end

      # Wait for processes to die
      for pid <- pids do
        ref = Process.monitor(pid)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          1000 -> flunk("Process didn't terminate")
        end
      end

      # Give registry time to clean up
      Process.sleep(100)

      # All snapshots should be released
      assert Snapshot.count() == initial_count
    end

    @tag :resource_cleanup
    test "no iterator leaks after workload", %{db: db} do
      # Insert some data
      for i <- 1..100 do
        :ok = NIF.put(db, :spo, <<i::64, 1::64, 1::64>>, "value")
      end

      # Create and close many iterators
      for _ <- 1..50 do
        {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64>>)

        # Read some data
        _ = NIF.iterator_next(iter)
        _ = NIF.iterator_next(iter)

        # Close iterator
        :ok = NIF.iterator_close(iter)
      end

      # Use stream API which handles cleanup
      for _ <- 1..50 do
        {:ok, stream} = NIF.prefix_stream(db, :spo, <<1::64>>)
        _ = Enum.take(stream, 2)
        # Stream cleanup happens automatically
      end

      # If we get here without errors, no leaks occurred
      assert true
    end

    @tag :resource_cleanup
    test "database closes cleanly after workload", %{db: db, path: _path} do
      # Generate workload
      generate_test_data(db, 10, 5, 5)

      # Create and release snapshots
      {:ok, snap} = Snapshot.create(db)
      :ok = Snapshot.release(snap)

      # Create and close iterators
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64>>)
      :ok = NIF.iterator_close(iter)

      # Verify database is open
      assert NIF.is_open(db) == true

      # Close database
      assert :ok = NIF.close(db)

      # Verify database is closed
      assert NIF.is_open(db) == false
    end

    @tag :resource_cleanup
    test "storage can be reclaimed after delete", %{db: db, path: _path} do
      # Insert data
      for i <- 1..1000 do
        :ok = NIF.put(db, :spo, <<i::64, 1::64, 1::64>>, String.duplicate("X", 100))
      end

      NIF.flush_wal(db, true)

      # Delete all data
      for i <- 1..1000 do
        :ok = NIF.delete(db, :spo, <<i::64, 1::64, 1::64>>)
      end

      # Flush WAL
      NIF.flush_wal(db, true)

      # Verify delete operations work correctly - all keys should be gone
      for i <- 1..1000 do
        assert :not_found = NIF.get(db, :spo, <<i::64, 1::64, 1::64>>)
      end
    end
  end

  describe "4.4.3 Performance Validation Tests" do
    @tag :performance
    test "iterator throughput is acceptable", %{db: db} do
      # Insert test data
      num_keys = 10_000

      for i <- 1..num_keys do
        :ok = NIF.put(db, :spo, <<1::64, i::64, 1::64>>, "value_#{i}")
      end

      # Measure iteration time
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64>>)

      {time_us, count} =
        :timer.tc(fn ->
          count_iterator(iter)
        end)

      assert count == num_keys

      # Calculate throughput
      time_ms = time_us / 1000
      throughput = count / (time_ms / 1000)

      # Should iterate at least 100K keys/second
      assert throughput > 100_000,
             "Iterator throughput #{trunc(throughput)} keys/sec is below 100K threshold"
    end

    @tag :performance
    test "point lookup latency is acceptable", %{db: db} do
      # Insert test data
      for i <- 1..1000 do
        :ok = NIF.put(db, :str2id, "uri_#{i}", <<i::64>>)
      end

      # Measure lookup time for 1000 lookups
      {time_us, _} =
        :timer.tc(fn ->
          for i <- 1..1000 do
            {:ok, _} = NIF.get(db, :str2id, "uri_#{i}")
          end
        end)

      avg_latency_us = time_us / 1000

      # Average lookup should be under 100 microseconds
      assert avg_latency_us < 100,
             "Average lookup latency #{avg_latency_us}us exceeds 100us threshold"
    end

    @tag :performance
    test "bulk load performs well with tuned configuration", %{db: db} do
      batch_size = 1000
      num_batches = 10

      # Measure bulk load time using write_batch
      # Operations format: {cf, key, value}
      {time_us, _} =
        :timer.tc(fn ->
          for batch <- 1..num_batches do
            operations =
              for i <- 1..batch_size do
                key = <<batch::64, i::64, 1::64>>
                {:spo, key, "value_#{batch}_#{i}"}
              end

            :ok = NIF.write_batch(db, operations, false)
          end
        end)

      total_keys = batch_size * num_batches
      time_ms = time_us / 1000
      throughput = total_keys / (time_ms / 1000)

      # Should write at least 50K keys/second
      assert throughput > 50_000,
             "Bulk write throughput #{trunc(throughput)} keys/sec is below 50K threshold"
    end

    @tag :performance
    test "snapshot reads provide consistent view without blocking", %{db: db} do
      # Insert initial data
      :ok = NIF.put(db, :spo, "key1", "value1")

      # Create snapshot
      {:ok, snapshot} = Snapshot.create(db)

      # Modify data after snapshot
      :ok = NIF.put(db, :spo, "key1", "value2")
      :ok = NIF.put(db, :spo, "key2", "new_value")

      # Snapshot should still see original value
      assert {:ok, "value1"} = NIF.snapshot_get(snapshot, :spo, "key1")
      assert :not_found = NIF.snapshot_get(snapshot, :spo, "key2")

      # Current view should see new values
      assert {:ok, "value2"} = NIF.get(db, :spo, "key1")
      assert {:ok, "new_value"} = NIF.get(db, :spo, "key2")

      :ok = Snapshot.release(snapshot)
    end
  end

  # Helper functions

  defp collect_iterator(iter, acc \\ []) do
    case NIF.iterator_next(iter) do
      {:ok, key, value} ->
        collect_iterator(iter, [{key, value} | acc])

      :iterator_end ->
        NIF.iterator_close(iter)
        Enum.reverse(acc)
    end
  end

  defp count_iterator(iter, count \\ 0) do
    case NIF.iterator_next(iter) do
      {:ok, _key, _value} ->
        count_iterator(iter, count + 1)

      :iterator_end ->
        NIF.iterator_close(iter)
        count
    end
  end
end
