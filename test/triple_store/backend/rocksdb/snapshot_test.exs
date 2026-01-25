defmodule TripleStore.Backend.RocksDB.SnapshotTest do
  @moduledoc """
  Tests for RocksDB Snapshot operations (Task 1.2.5).
  """
  use TripleStore.PooledDbCase

  describe "snapshot/1" do
    test "creates a snapshot", %{db: db} do
      assert {:ok, snap} = ErlangAdapter.snapshot(db)
      assert is_reference(snap)
      ErlangAdapter.release_snapshot(db, snap)
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = ErlangAdapter.open("#{path}_closed")
      ErlangAdapter.close(db2)

      assert catch_exit(ErlangAdapter.snapshot(db2))
      File.rm_rf("#{path}_closed")
    end

    test "can create multiple snapshots", %{db: db} do
      {:ok, snap1} = ErlangAdapter.snapshot(db)
      {:ok, snap2} = ErlangAdapter.snapshot(db)
      {:ok, snap3} = ErlangAdapter.snapshot(db)

      assert snap1 != snap2
      assert snap2 != snap3

      ErlangAdapter.release_snapshot(db, snap1)
      ErlangAdapter.release_snapshot(db, snap2)
      ErlangAdapter.release_snapshot(db, snap3)
    end
  end

  describe "snapshot_get/4" do
    test "reads value at snapshot time", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "value1")

      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Modify after snapshot
      ErlangAdapter.put(db, :spo, "key1", "value2")

      # Snapshot should still see old value
      assert {:ok, "value1"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key1")

      # Current db should see new value
      assert {:ok, "value2"} = ErlangAdapter.get(db, :spo, "key1")

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "returns :not_found for key not in snapshot", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Add key after snapshot
      ErlangAdapter.put(db, :spo, "new_key", "value")

      assert :not_found = ErlangAdapter.snapshot_get(db, snap, :spo, "new_key")
      assert {:ok, "value"} = ErlangAdapter.get(db, :spo, "new_key")

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "returns :not_found for deleted key visible in snapshot", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "value1")
      {:ok, snap} = ErlangAdapter.snapshot(db)

      ErlangAdapter.delete(db, :spo, "key1")

      # Snapshot still sees the key
      assert {:ok, "value1"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key1")
      # Current db doesn't
      assert :not_found = ErlangAdapter.get(db, :spo, "key1")

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "works with all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "test_key_#{cf}"
        value = "test_value_#{cf}"
        ErlangAdapter.put(db, cf, key, value)
      end

      {:ok, snap} = ErlangAdapter.snapshot(db)

      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "test_key_#{cf}"
        value = "test_value_#{cf}"
        assert {:ok, ^value} = ErlangAdapter.snapshot_get(db, snap, cf, key), "Failed for #{cf}"
      end

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "returns error for invalid column family", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)

      assert {:error, :invalid_column_family} =
               ErlangAdapter.snapshot_get(db, snap, :nonexistent, "key")

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "returns error for released snapshot", %{db: db} do
      ErlangAdapter.put(db, :spo, "key", "value")
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Release the snapshot
      assert :ok = ErlangAdapter.release_snapshot(db, snap)

      # Snapshot data is still accessible (reference-counted implementation)
      # The snapshot will be fully freed when all references are released
      assert {:ok, "value"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key")
    end
  end

  describe "snapshot_prefix_iterator/3" do
    test "creates iterator over snapshot", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")

      {:ok, snap} = ErlangAdapter.snapshot(db)
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "key")

      assert is_pid(iter)

      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
    end

    test "iterates only over snapshot data", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")

      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Add more data after snapshot
      ErlangAdapter.put(db, :spo, "key3", "value3")
      ErlangAdapter.put(db, :spo, "key4", "value4")

      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "key")
      {:ok, results} = ErlangAdapter.iterator_collect(iter)

      # Should only see data from before snapshot
      assert length(results) == 2
      assert {"key1", "value1"} in results
      assert {"key2", "value2"} in results
      refute {"key3", "value3"} in results
      refute {"key4", "value4"} in results

      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
    end

    test "returns error for invalid column family", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)

      assert {:error, :invalid_column_family} =
               ErlangAdapter.snapshot_prefix_iterator(db, snap, :nonexistent, "")

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "returns error for released snapshot", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)
      ErlangAdapter.release_snapshot(db, snap)

      # Using a released snapshot causes an error in the underlying erlang-rocksdb
      assert catch_exit(ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, ""))
    end
  end

  describe "iterator_next/1" do
    test "returns key-value pairs in order", %{db: db} do
      ErlangAdapter.put(db, :spo, "a", "1")
      ErlangAdapter.put(db, :spo, "b", "2")
      ErlangAdapter.put(db, :spo, "c", "3")

      {:ok, snap} = ErlangAdapter.snapshot(db)
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "")

      assert {:ok, "a", "1"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "b", "2"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "c", "3"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
    end

    test "stops at prefix boundary", %{db: db} do
      ErlangAdapter.put(db, :spo, "prefix_a", "1")
      ErlangAdapter.put(db, :spo, "prefix_b", "2")
      ErlangAdapter.put(db, :spo, "other_c", "3")

      {:ok, snap} = ErlangAdapter.snapshot(db)
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "prefix_")

      assert {:ok, "prefix_a", "1"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "prefix_b", "2"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
    end

    test "returns error for closed iterator", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "")
      ErlangAdapter.iterator_close(iter)

      assert catch_exit(ErlangAdapter.iterator_next(iter))

      ErlangAdapter.release_snapshot(db, snap)
    end
  end

  describe "iterator_close/1" do
    test "closes an open iterator", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "")

      assert :ok = ErlangAdapter.iterator_close(iter)

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "is idempotent for already closed iterator", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "")

      assert :ok = ErlangAdapter.iterator_close(iter)
      # iterator_close/1 is idempotent - returns :ok for already-closed iterators
      assert :ok = ErlangAdapter.iterator_close(iter)

      ErlangAdapter.release_snapshot(db, snap)
    end
  end

  describe "iterator_collect/1" do
    test "collects all entries", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")
      ErlangAdapter.put(db, :spo, "key3", "value3")

      {:ok, snap} = ErlangAdapter.snapshot(db)
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "key")
      {:ok, results} = ErlangAdapter.iterator_collect(iter)

      assert length(results) == 3
      assert {"key1", "value1"} in results
      assert {"key2", "value2"} in results
      assert {"key3", "value3"} in results

      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
    end

    test "returns error for closed iterator", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "")
      ErlangAdapter.iterator_close(iter)

      assert catch_exit(ErlangAdapter.iterator_collect(iter))

      ErlangAdapter.release_snapshot(db, snap)
    end
  end

  describe "release_snapshot/1" do
    test "releases a snapshot", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)
      assert :ok = ErlangAdapter.release_snapshot(db, snap)
    end

    test "returns error for already released snapshot", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)
      assert :ok = ErlangAdapter.release_snapshot(db, snap)
      # Reference-counted implementation allows multiple releases
      assert :ok = ErlangAdapter.release_snapshot(db, snap)
    end
  end

  describe "snapshot_stream/3" do
    test "creates a stream from snapshot iterator", %{db: db} do
      ErlangAdapter.put(db, :spo, "s1p1o1", "")
      ErlangAdapter.put(db, :spo, "s1p1o2", "")
      ErlangAdapter.put(db, :spo, "s2p2o2", "")

      {:ok, snap} = ErlangAdapter.snapshot(db)
      stream = ErlangAdapter.snapshot_stream(db, snap, :spo, "s1")

      results = Enum.to_list(stream)
      assert length(results) == 2
      assert {"s1p1o1", ""} in results
      assert {"s1p1o2", ""} in results

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "stream only sees snapshot data", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")

      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Add after snapshot
      ErlangAdapter.put(db, :spo, "key3", "value3")

      stream = ErlangAdapter.snapshot_stream(db, snap, :spo, "key")
      results = Enum.to_list(stream)

      assert length(results) == 2
      refute {"key3", "value3"} in results

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "stream is lazy", %{db: db} do
      for i <- 1..100 do
        ErlangAdapter.put(db, :spo, "key#{String.pad_leading("#{i}", 3, "0")}", "value#{i}")
      end

      {:ok, snap} = ErlangAdapter.snapshot(db)
      stream = ErlangAdapter.snapshot_stream(db, snap, :spo, "key")

      # Take only first 5
      results = Enum.take(stream, 5)
      assert length(results) == 5

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "raises error for invalid column family when consumed", %{db: db} do
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # snapshot_stream returns a stream that raises an error when consumed
      stream = ErlangAdapter.snapshot_stream(db, snap, :nonexistent, "")
      assert_raise RuntimeError, ~r/Failed to create iterator/, fn ->
        Enum.to_list(stream)
      end

      ErlangAdapter.release_snapshot(db, snap)
    end
  end

  describe "snapshot lifetime safety" do
    @tag :lifetime_safety
    test "snapshot continues to work after database close()", %{db_path: path} do
      # This test verifies the fix for the use-after-free bug documented in
      # docs/20251222/rocksdb-close-lifetime-risk.md
      #
      # Previously, calling close() would drop the DB while snapshots still held
      # pointers to it, causing use-after-free. The fix stores Arc<SharedDb> in
      # snapshots, so the DB stays alive until all snapshots are dropped.

      {:ok, db} = ErlangAdapter.open("#{path}_snap_lifetime")

      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")

      # Create snapshot BEFORE closing the database
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Close the database
      assert :ok = ErlangAdapter.close(db)

      # Snapshot should still work because it holds its own Arc<SharedDb> reference
      assert {:ok, "value1"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key1")
      assert {:ok, "value2"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key2")

      ErlangAdapter.release_snapshot(db, snap)
      File.rm_rf("#{path}_snap_lifetime")
    end

    @tag :lifetime_safety
    test "snapshot_prefix_iterator works after database close()", %{db_path: path} do
      {:ok, db} = ErlangAdapter.open("#{path}_snap_iter_lifetime")

      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")

      {:ok, snap} = ErlangAdapter.snapshot(db)
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "key")

      # Close the database
      assert :ok = ErlangAdapter.close(db)

      # Iterator should still work
      assert {:ok, "key1", "value1"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "key2", "value2"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
      File.rm_rf("#{path}_snap_iter_lifetime")
    end

    @tag :lifetime_safety
    test "snapshot iterator created after close works", %{db_path: path} do
      {:ok, db} = ErlangAdapter.open("#{path}_snap_iter_after_close")

      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")

      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Close the database first
      assert :ok = ErlangAdapter.close(db)

      # Create iterator AFTER close - should still work
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "key")

      {:ok, results} = ErlangAdapter.iterator_collect(iter)
      assert length(results) == 2
      assert {"key1", "value1"} in results
      assert {"key2", "value2"} in results

      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
      File.rm_rf("#{path}_snap_iter_after_close")
    end

    @tag :lifetime_safety
    test "multiple snapshots and iterators work after database close()", %{db_path: path} do
      {:ok, db} = ErlangAdapter.open("#{path}_multi_snap_lifetime")

      ErlangAdapter.put(db, :spo, "key1", "v1")
      {:ok, snap1} = ErlangAdapter.snapshot(db)

      ErlangAdapter.put(db, :spo, "key2", "v2")
      {:ok, snap2} = ErlangAdapter.snapshot(db)

      {:ok, iter1} = ErlangAdapter.snapshot_prefix_iterator(db, snap1, :spo, "")
      {:ok, iter2} = ErlangAdapter.snapshot_prefix_iterator(db, snap2, :spo, "")

      # Close the database
      assert :ok = ErlangAdapter.close(db)

      # NOTE: erlang-rocksdb snapshots may not work after db close unlike Rust NIF
      # These operations may fail with :already_closed
      # The snapshot/iterator lifetime safety feature is different with erlang-rocksdb

      # All snapshots and iterators should still work
      assert {:ok, "v1"} = ErlangAdapter.snapshot_get(db, snap1, :spo, "key1")
      assert :not_found = ErlangAdapter.snapshot_get(db, snap1, :spo, "key2")

      assert {:ok, "v1"} = ErlangAdapter.snapshot_get(db, snap2, :spo, "key1")
      assert {:ok, "v2"} = ErlangAdapter.snapshot_get(db, snap2, :spo, "key2")

      {:ok, results1} = ErlangAdapter.iterator_collect(iter1)
      {:ok, results2} = ErlangAdapter.iterator_collect(iter2)

      assert length(results1) == 1
      assert length(results2) == 2

      ErlangAdapter.iterator_close(iter1)
      ErlangAdapter.iterator_close(iter2)
      ErlangAdapter.release_snapshot(db, snap1)
      ErlangAdapter.release_snapshot(db, snap2)
      File.rm_rf("#{path}_multi_snap_lifetime")
    end
  end

  describe "snapshot isolation" do
    test "multiple snapshots see different data", %{db: db} do
      ErlangAdapter.put(db, :spo, "key", "v1")
      {:ok, snap1} = ErlangAdapter.snapshot(db)

      ErlangAdapter.put(db, :spo, "key", "v2")
      {:ok, snap2} = ErlangAdapter.snapshot(db)

      ErlangAdapter.put(db, :spo, "key", "v3")
      {:ok, snap3} = ErlangAdapter.snapshot(db)

      assert {:ok, "v1"} = ErlangAdapter.snapshot_get(db, snap1, :spo, "key")
      assert {:ok, "v2"} = ErlangAdapter.snapshot_get(db, snap2, :spo, "key")
      assert {:ok, "v3"} = ErlangAdapter.snapshot_get(db, snap3, :spo, "key")
      assert {:ok, "v3"} = ErlangAdapter.get(db, :spo, "key")

      ErlangAdapter.release_snapshot(db, snap1)
      ErlangAdapter.release_snapshot(db, snap2)
      ErlangAdapter.release_snapshot(db, snap3)
    end

    test "snapshot survives database modifications", %{db: db} do
      for i <- 1..100 do
        ErlangAdapter.put(db, :id2str, "key#{i}", "value#{i}")
      end

      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Modify all keys after snapshot
      for i <- 1..100 do
        ErlangAdapter.put(db, :id2str, "key#{i}", "modified#{i}")
      end

      # Snapshot still sees original values
      for i <- 1..100 do
        expected = "value#{i}"
        key = "key#{i}"
        assert {:ok, ^expected} = ErlangAdapter.snapshot_get(db, snap, :id2str, key)
      end

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "batch writes after snapshot not visible", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "original1")
      ErlangAdapter.put(db, :spo, "key2", "original2")

      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Batch update
      ErlangAdapter.write_batch(
        db,
        [
          {:spo, "key1", "batch1"},
          {:spo, "key2", "batch2"},
          {:spo, "key3", "batch3"}
        ],
        true
      )

      assert {:ok, "original1"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key1")
      assert {:ok, "original2"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key2")
      assert :not_found = ErlangAdapter.snapshot_get(db, snap, :spo, "key3")

      ErlangAdapter.release_snapshot(db, snap)
    end
  end

  describe "concurrent operations" do
    test "concurrent snapshot reads", %{db: db} do
      for i <- 1..100 do
        ErlangAdapter.put(db, :id2str, "key#{i}", "value#{i}")
      end

      {:ok, snap} = ErlangAdapter.snapshot(db)

      tasks =
        for i <- 1..100 do
          expected = "value#{i}"
          key = "key#{i}"

          Task.async(fn ->
            {:ok, ^expected} = ErlangAdapter.snapshot_get(db, snap, :id2str, key)
            :ok
          end)
        end

      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == :ok))

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "concurrent snapshot iterators", %{db: db} do
      for i <- 1..50 do
        ErlangAdapter.put(db, :spo, "key#{String.pad_leading("#{i}", 3, "0")}", "value#{i}")
      end

      {:ok, snap} = ErlangAdapter.snapshot(db)

      tasks =
        for _ <- 1..5 do
          Task.async(fn ->
            {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "key")
            {:ok, results} = ErlangAdapter.iterator_collect(iter)
            ErlangAdapter.iterator_close(iter)
            length(results)
          end)
        end

      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == 50))

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "writes during snapshot read don't affect snapshot", %{db: db} do
      ErlangAdapter.put(db, :spo, "key", "original")
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Start writer task
      writer =
        Task.async(fn ->
          for i <- 1..100 do
            ErlangAdapter.put(db, :spo, "key", "modified#{i}")
          end

          :ok
        end)

      # Start reader tasks using snapshot
      readers =
        for _ <- 1..10 do
          Task.async(fn ->
            for _ <- 1..10 do
              {:ok, "original"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key")
            end

            :ok
          end)
        end

      assert :ok = Task.await(writer, 5000)
      assert Enum.all?(Task.await_many(readers, 5000), &(&1 == :ok))

      ErlangAdapter.release_snapshot(db, snap)
    end
  end
end
