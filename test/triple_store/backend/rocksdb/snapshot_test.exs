defmodule TripleStore.Backend.RocksDB.SnapshotTest do
  @moduledoc """
  Tests for RocksDB Snapshot operations (Task 1.2.5).
  """
  use TripleStore.PooledDbCase

  describe "snapshot/1" do
    test "creates a snapshot", %{db: db} do
      assert {:ok, snap} = NIF.snapshot(db)
      assert is_reference(snap)
      NIF.release_snapshot(snap)
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = NIF.open("#{path}_closed")
      NIF.close(db2)

      assert {:error, :already_closed} = NIF.snapshot(db2)
      File.rm_rf("#{path}_closed")
    end

    test "can create multiple snapshots", %{db: db} do
      {:ok, snap1} = NIF.snapshot(db)
      {:ok, snap2} = NIF.snapshot(db)
      {:ok, snap3} = NIF.snapshot(db)

      assert snap1 != snap2
      assert snap2 != snap3

      NIF.release_snapshot(snap1)
      NIF.release_snapshot(snap2)
      NIF.release_snapshot(snap3)
    end
  end

  describe "snapshot_get/3" do
    test "reads value at snapshot time", %{db: db} do
      NIF.put(db, :spo, "key1", "value1")

      {:ok, snap} = NIF.snapshot(db)

      # Modify after snapshot
      NIF.put(db, :spo, "key1", "value2")

      # Snapshot should still see old value
      assert {:ok, "value1"} = NIF.snapshot_get(snap, :spo, "key1")

      # Current db should see new value
      assert {:ok, "value2"} = NIF.get(db, :spo, "key1")

      NIF.release_snapshot(snap)
    end

    test "returns :not_found for key not in snapshot", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)

      # Add key after snapshot
      NIF.put(db, :spo, "new_key", "value")

      assert :not_found = NIF.snapshot_get(snap, :spo, "new_key")
      assert {:ok, "value"} = NIF.get(db, :spo, "new_key")

      NIF.release_snapshot(snap)
    end

    test "returns :not_found for deleted key visible in snapshot", %{db: db} do
      NIF.put(db, :spo, "key1", "value1")
      {:ok, snap} = NIF.snapshot(db)

      NIF.delete(db, :spo, "key1")

      # Snapshot still sees the key
      assert {:ok, "value1"} = NIF.snapshot_get(snap, :spo, "key1")
      # Current db doesn't
      assert :not_found = NIF.get(db, :spo, "key1")

      NIF.release_snapshot(snap)
    end

    test "works with all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "test_key_#{cf}"
        value = "test_value_#{cf}"
        NIF.put(db, cf, key, value)
      end

      {:ok, snap} = NIF.snapshot(db)

      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "test_key_#{cf}"
        value = "test_value_#{cf}"
        assert {:ok, ^value} = NIF.snapshot_get(snap, cf, key), "Failed for #{cf}"
      end

      NIF.release_snapshot(snap)
    end

    test "returns error for invalid column family", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)
      assert {:error, {:invalid_cf, :nonexistent}} = NIF.snapshot_get(snap, :nonexistent, "key")
      NIF.release_snapshot(snap)
    end

    test "returns error for released snapshot", %{db: db} do
      NIF.put(db, :spo, "key", "value")
      {:ok, snap} = NIF.snapshot(db)
      NIF.release_snapshot(snap)

      assert {:error, :snapshot_released} = NIF.snapshot_get(snap, :spo, "key")
    end
  end

  describe "snapshot_prefix_iterator/3" do
    test "creates iterator over snapshot", %{db: db} do
      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")

      {:ok, snap} = NIF.snapshot(db)
      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "key")

      assert is_reference(iter)

      NIF.snapshot_iterator_close(iter)
      NIF.release_snapshot(snap)
    end

    test "iterates only over snapshot data", %{db: db} do
      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")

      {:ok, snap} = NIF.snapshot(db)

      # Add more data after snapshot
      NIF.put(db, :spo, "key3", "value3")
      NIF.put(db, :spo, "key4", "value4")

      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "key")
      {:ok, results} = NIF.snapshot_iterator_collect(iter)

      # Should only see data from before snapshot
      assert length(results) == 2
      assert {"key1", "value1"} in results
      assert {"key2", "value2"} in results
      refute {"key3", "value3"} in results
      refute {"key4", "value4"} in results

      NIF.snapshot_iterator_close(iter)
      NIF.release_snapshot(snap)
    end

    test "returns error for invalid column family", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)

      assert {:error, {:invalid_cf, :nonexistent}} =
               NIF.snapshot_prefix_iterator(snap, :nonexistent, "")

      NIF.release_snapshot(snap)
    end

    test "returns error for released snapshot", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)
      NIF.release_snapshot(snap)

      assert {:error, :snapshot_released} = NIF.snapshot_prefix_iterator(snap, :spo, "")
    end
  end

  describe "snapshot_iterator_next/1" do
    test "returns key-value pairs in order", %{db: db} do
      NIF.put(db, :spo, "a", "1")
      NIF.put(db, :spo, "b", "2")
      NIF.put(db, :spo, "c", "3")

      {:ok, snap} = NIF.snapshot(db)
      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "")

      assert {:ok, "a", "1"} = NIF.snapshot_iterator_next(iter)
      assert {:ok, "b", "2"} = NIF.snapshot_iterator_next(iter)
      assert {:ok, "c", "3"} = NIF.snapshot_iterator_next(iter)
      assert :iterator_end = NIF.snapshot_iterator_next(iter)

      NIF.snapshot_iterator_close(iter)
      NIF.release_snapshot(snap)
    end

    test "stops at prefix boundary", %{db: db} do
      NIF.put(db, :spo, "prefix_a", "1")
      NIF.put(db, :spo, "prefix_b", "2")
      NIF.put(db, :spo, "other_c", "3")

      {:ok, snap} = NIF.snapshot(db)
      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "prefix_")

      assert {:ok, "prefix_a", "1"} = NIF.snapshot_iterator_next(iter)
      assert {:ok, "prefix_b", "2"} = NIF.snapshot_iterator_next(iter)
      assert :iterator_end = NIF.snapshot_iterator_next(iter)

      NIF.snapshot_iterator_close(iter)
      NIF.release_snapshot(snap)
    end

    test "returns error for closed iterator", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)
      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "")
      NIF.snapshot_iterator_close(iter)

      assert {:error, :iterator_closed} = NIF.snapshot_iterator_next(iter)

      NIF.release_snapshot(snap)
    end
  end

  describe "snapshot_iterator_close/1" do
    test "closes an open iterator", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)
      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "")

      assert :ok = NIF.snapshot_iterator_close(iter)

      NIF.release_snapshot(snap)
    end

    test "returns error for already closed iterator", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)
      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "")

      assert :ok = NIF.snapshot_iterator_close(iter)
      assert {:error, :iterator_closed} = NIF.snapshot_iterator_close(iter)

      NIF.release_snapshot(snap)
    end
  end

  describe "snapshot_iterator_collect/1" do
    test "collects all entries", %{db: db} do
      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")
      NIF.put(db, :spo, "key3", "value3")

      {:ok, snap} = NIF.snapshot(db)
      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "key")
      {:ok, results} = NIF.snapshot_iterator_collect(iter)

      assert length(results) == 3
      assert {"key1", "value1"} in results
      assert {"key2", "value2"} in results
      assert {"key3", "value3"} in results

      NIF.snapshot_iterator_close(iter)
      NIF.release_snapshot(snap)
    end

    test "returns error for closed iterator", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)
      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "")
      NIF.snapshot_iterator_close(iter)

      assert {:error, :iterator_closed} = NIF.snapshot_iterator_collect(iter)

      NIF.release_snapshot(snap)
    end
  end

  describe "release_snapshot/1" do
    test "releases a snapshot", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)
      assert :ok = NIF.release_snapshot(snap)
    end

    test "returns error for already released snapshot", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)
      assert :ok = NIF.release_snapshot(snap)
      assert {:error, :snapshot_released} = NIF.release_snapshot(snap)
    end
  end

  describe "snapshot_stream/3" do
    test "creates a stream from snapshot iterator", %{db: db} do
      NIF.put(db, :spo, "s1p1o1", "")
      NIF.put(db, :spo, "s1p1o2", "")
      NIF.put(db, :spo, "s2p2o2", "")

      {:ok, snap} = NIF.snapshot(db)
      {:ok, stream} = NIF.snapshot_stream(snap, :spo, "s1")

      results = Enum.to_list(stream)
      assert length(results) == 2
      assert {"s1p1o1", ""} in results
      assert {"s1p1o2", ""} in results

      NIF.release_snapshot(snap)
    end

    test "stream only sees snapshot data", %{db: db} do
      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")

      {:ok, snap} = NIF.snapshot(db)

      # Add after snapshot
      NIF.put(db, :spo, "key3", "value3")

      {:ok, stream} = NIF.snapshot_stream(snap, :spo, "key")
      results = Enum.to_list(stream)

      assert length(results) == 2
      refute {"key3", "value3"} in results

      NIF.release_snapshot(snap)
    end

    test "stream is lazy", %{db: db} do
      for i <- 1..100 do
        NIF.put(db, :spo, "key#{String.pad_leading("#{i}", 3, "0")}", "value#{i}")
      end

      {:ok, snap} = NIF.snapshot(db)
      {:ok, stream} = NIF.snapshot_stream(snap, :spo, "key")

      # Take only first 5
      results = Enum.take(stream, 5)
      assert length(results) == 5

      NIF.release_snapshot(snap)
    end

    test "returns error for invalid column family", %{db: db} do
      {:ok, snap} = NIF.snapshot(db)
      assert {:error, {:invalid_cf, :nonexistent}} = NIF.snapshot_stream(snap, :nonexistent, "")
      NIF.release_snapshot(snap)
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

      {:ok, db} = NIF.open("#{path}_snap_lifetime")

      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")

      # Create snapshot BEFORE closing the database
      {:ok, snap} = NIF.snapshot(db)

      # Close the database
      assert :ok = NIF.close(db)

      # Snapshot should still work because it holds its own Arc<SharedDb> reference
      assert {:ok, "value1"} = NIF.snapshot_get(snap, :spo, "key1")
      assert {:ok, "value2"} = NIF.snapshot_get(snap, :spo, "key2")

      NIF.release_snapshot(snap)
      File.rm_rf("#{path}_snap_lifetime")
    end

    @tag :lifetime_safety
    test "snapshot_prefix_iterator works after database close()", %{db_path: path} do
      {:ok, db} = NIF.open("#{path}_snap_iter_lifetime")

      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")

      {:ok, snap} = NIF.snapshot(db)
      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "key")

      # Close the database
      assert :ok = NIF.close(db)

      # Iterator should still work
      assert {:ok, "key1", "value1"} = NIF.snapshot_iterator_next(iter)
      assert {:ok, "key2", "value2"} = NIF.snapshot_iterator_next(iter)
      assert :iterator_end = NIF.snapshot_iterator_next(iter)

      NIF.snapshot_iterator_close(iter)
      NIF.release_snapshot(snap)
      File.rm_rf("#{path}_snap_iter_lifetime")
    end

    @tag :lifetime_safety
    test "snapshot iterator created after close works", %{db_path: path} do
      {:ok, db} = NIF.open("#{path}_snap_iter_after_close")

      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")

      {:ok, snap} = NIF.snapshot(db)

      # Close the database first
      assert :ok = NIF.close(db)

      # Create iterator AFTER close - should still work
      {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "key")

      {:ok, results} = NIF.snapshot_iterator_collect(iter)
      assert length(results) == 2
      assert {"key1", "value1"} in results
      assert {"key2", "value2"} in results

      NIF.snapshot_iterator_close(iter)
      NIF.release_snapshot(snap)
      File.rm_rf("#{path}_snap_iter_after_close")
    end

    @tag :lifetime_safety
    test "multiple snapshots and iterators work after database close()", %{db_path: path} do
      {:ok, db} = NIF.open("#{path}_multi_snap_lifetime")

      NIF.put(db, :spo, "key1", "v1")
      {:ok, snap1} = NIF.snapshot(db)

      NIF.put(db, :spo, "key2", "v2")
      {:ok, snap2} = NIF.snapshot(db)

      {:ok, iter1} = NIF.snapshot_prefix_iterator(snap1, :spo, "")
      {:ok, iter2} = NIF.snapshot_prefix_iterator(snap2, :spo, "")

      # Close the database
      assert :ok = NIF.close(db)

      # All snapshots and iterators should still work
      assert {:ok, "v1"} = NIF.snapshot_get(snap1, :spo, "key1")
      assert :not_found = NIF.snapshot_get(snap1, :spo, "key2")

      assert {:ok, "v1"} = NIF.snapshot_get(snap2, :spo, "key1")
      assert {:ok, "v2"} = NIF.snapshot_get(snap2, :spo, "key2")

      {:ok, results1} = NIF.snapshot_iterator_collect(iter1)
      {:ok, results2} = NIF.snapshot_iterator_collect(iter2)

      assert length(results1) == 1
      assert length(results2) == 2

      NIF.snapshot_iterator_close(iter1)
      NIF.snapshot_iterator_close(iter2)
      NIF.release_snapshot(snap1)
      NIF.release_snapshot(snap2)
      File.rm_rf("#{path}_multi_snap_lifetime")
    end
  end

  describe "snapshot isolation" do
    test "multiple snapshots see different data", %{db: db} do
      NIF.put(db, :spo, "key", "v1")
      {:ok, snap1} = NIF.snapshot(db)

      NIF.put(db, :spo, "key", "v2")
      {:ok, snap2} = NIF.snapshot(db)

      NIF.put(db, :spo, "key", "v3")
      {:ok, snap3} = NIF.snapshot(db)

      assert {:ok, "v1"} = NIF.snapshot_get(snap1, :spo, "key")
      assert {:ok, "v2"} = NIF.snapshot_get(snap2, :spo, "key")
      assert {:ok, "v3"} = NIF.snapshot_get(snap3, :spo, "key")
      assert {:ok, "v3"} = NIF.get(db, :spo, "key")

      NIF.release_snapshot(snap1)
      NIF.release_snapshot(snap2)
      NIF.release_snapshot(snap3)
    end

    test "snapshot survives database modifications", %{db: db} do
      for i <- 1..100 do
        NIF.put(db, :id2str, "key#{i}", "value#{i}")
      end

      {:ok, snap} = NIF.snapshot(db)

      # Modify all keys after snapshot
      for i <- 1..100 do
        NIF.put(db, :id2str, "key#{i}", "modified#{i}")
      end

      # Snapshot still sees original values
      for i <- 1..100 do
        expected = "value#{i}"
        key = "key#{i}"
        assert {:ok, ^expected} = NIF.snapshot_get(snap, :id2str, key)
      end

      NIF.release_snapshot(snap)
    end

    test "batch writes after snapshot not visible", %{db: db} do
      NIF.put(db, :spo, "key1", "original1")
      NIF.put(db, :spo, "key2", "original2")

      {:ok, snap} = NIF.snapshot(db)

      # Batch update
      NIF.write_batch(db, [
        {:spo, "key1", "batch1"},
        {:spo, "key2", "batch2"},
        {:spo, "key3", "batch3"}
      ], true)

      assert {:ok, "original1"} = NIF.snapshot_get(snap, :spo, "key1")
      assert {:ok, "original2"} = NIF.snapshot_get(snap, :spo, "key2")
      assert :not_found = NIF.snapshot_get(snap, :spo, "key3")

      NIF.release_snapshot(snap)
    end
  end

  describe "concurrent operations" do
    test "concurrent snapshot reads", %{db: db} do
      for i <- 1..100 do
        NIF.put(db, :id2str, "key#{i}", "value#{i}")
      end

      {:ok, snap} = NIF.snapshot(db)

      tasks =
        for i <- 1..100 do
          expected = "value#{i}"
          key = "key#{i}"

          Task.async(fn ->
            {:ok, ^expected} = NIF.snapshot_get(snap, :id2str, key)
            :ok
          end)
        end

      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == :ok))

      NIF.release_snapshot(snap)
    end

    test "concurrent snapshot iterators", %{db: db} do
      for i <- 1..50 do
        NIF.put(db, :spo, "key#{String.pad_leading("#{i}", 3, "0")}", "value#{i}")
      end

      {:ok, snap} = NIF.snapshot(db)

      tasks =
        for _ <- 1..5 do
          Task.async(fn ->
            {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "key")
            {:ok, results} = NIF.snapshot_iterator_collect(iter)
            NIF.snapshot_iterator_close(iter)
            length(results)
          end)
        end

      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == 50))

      NIF.release_snapshot(snap)
    end

    test "writes during snapshot read don't affect snapshot", %{db: db} do
      NIF.put(db, :spo, "key", "original")
      {:ok, snap} = NIF.snapshot(db)

      # Start writer task
      writer =
        Task.async(fn ->
          for i <- 1..100 do
            NIF.put(db, :spo, "key", "modified#{i}")
          end

          :ok
        end)

      # Start reader tasks using snapshot
      readers =
        for _ <- 1..10 do
          Task.async(fn ->
            for _ <- 1..10 do
              {:ok, "original"} = NIF.snapshot_get(snap, :spo, "key")
            end

            :ok
          end)
        end

      assert :ok = Task.await(writer, 5000)
      assert Enum.all?(Task.await_many(readers, 5000), &(&1 == :ok))

      NIF.release_snapshot(snap)
    end
  end
end
