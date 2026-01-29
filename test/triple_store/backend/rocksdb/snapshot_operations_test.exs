defmodule TripleStore.Backend.RocksDB.SnapshotOperationsTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter

  @moduletag :snapshot_operations
  @moduletag timeout: 120_000

  describe "Section 2.2.1: Snapshot Creation and Release" do
    test "2.2.1.1 snapshot/1 creates a point-in-time snapshot" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      # Add some data
      :ok = ErlangAdapter.put(db, :spo, "key1", "value1")
      :ok = ErlangAdapter.put(db, :spo, "key2", "value2")

      # Create snapshot
      {:ok, snapshot} = ErlangAdapter.snapshot(db)
      assert is_reference(snapshot)

      # Clean up
      ErlangAdapter.release_snapshot(db, snapshot)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end

    test "2.2.1.2 release_snapshot/1 releases snapshot resources" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      {:ok, snapshot} = ErlangAdapter.snapshot(db)
      assert :ok = ErlangAdapter.release_snapshot(db, snapshot)

      ErlangAdapter.close(db)
      File.rm_rf(path)
    end

    test "2.2.1.3 multiple snapshots can coexist" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      # Create first snapshot
      :ok = ErlangAdapter.put(db, :spo, "key1", "value1")
      {:ok, snap1} = ErlangAdapter.snapshot(db)

      # Add more data
      :ok = ErlangAdapter.put(db, :spo, "key2", "value2")

      # Create second snapshot
      {:ok, snap2} = ErlangAdapter.snapshot(db)

      # Both snapshots should be valid references
      assert is_reference(snap1)
      assert is_reference(snap2)
      refute snap1 == snap2

      # Clean up
      ErlangAdapter.release_snapshot(db, snap1)
      ErlangAdapter.release_snapshot(db, snap2)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end
  end

  describe "Section 2.2.2: Snapshot Read Operations" do
    test "2.2.2.1 snapshot_get/4 reads value at snapshot time" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      # Add initial data
      :ok = ErlangAdapter.put(db, :spo, "key1", "value1")

      # Create snapshot
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Modify the data
      :ok = ErlangAdapter.put(db, :spo, "key1", "value2")
      :ok = ErlangAdapter.put(db, :spo, "key2", "value2")

      # Snapshot should still see old value
      assert {:ok, "value1"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key1")
      # Snapshot doesn't see new key
      assert :not_found = ErlangAdapter.snapshot_get(db, snap, :spo, "key2")

      # Current db sees new values
      assert {:ok, "value2"} = ErlangAdapter.get(db, :spo, "key1")
      assert {:ok, "value2"} = ErlangAdapter.get(db, :spo, "key2")

      # Clean up
      ErlangAdapter.release_snapshot(db, snap)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end

    test "2.2.2.2 snapshot_get/4 handles not_found consistently" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Key doesn't exist in snapshot
      assert :not_found = ErlangAdapter.snapshot_get(db, snap, :spo, "nonexistent")

      ErlangAdapter.release_snapshot(db, snap)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end

    test "2.2.2.3 snapshot provides point-in-time consistency across CFs" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      # Add data to multiple column families
      :ok = ErlangAdapter.put(db, :spo, "key1", "spo_value1")
      :ok = ErlangAdapter.put(db, :pos, "key1", "pos_value1")
      :ok = ErlangAdapter.put(db, :osp, "key1", "osp_value1")

      # Create snapshot
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Modify all CFs
      :ok = ErlangAdapter.put(db, :spo, "key1", "spo_value2")
      :ok = ErlangAdapter.put(db, :pos, "key1", "pos_value2")
      :ok = ErlangAdapter.put(db, :osp, "key1", "osp_value2")

      # Snapshot sees old values in all CFs
      assert {:ok, "spo_value1"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key1")
      assert {:ok, "pos_value1"} = ErlangAdapter.snapshot_get(db, snap, :pos, "key1")
      assert {:ok, "osp_value1"} = ErlangAdapter.snapshot_get(db, snap, :osp, "key1")

      # Current db sees new values
      assert {:ok, "spo_value2"} = ErlangAdapter.get(db, :spo, "key1")
      assert {:ok, "pos_value2"} = ErlangAdapter.get(db, :pos, "key1")
      assert {:ok, "osp_value2"} = ErlangAdapter.get(db, :osp, "key1")

      ErlangAdapter.release_snapshot(db, snap)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end
  end

  describe "Section 2.2.3: Snapshot Iterator Operations" do
    test "2.2.3.1 snapshot_prefix_iterator/4 iterates over snapshot state" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      # Add data: <<1>>, <<2>>, <<3>>
      :ok = ErlangAdapter.put(db, :spo, <<1::64-big>>, "value1")
      :ok = ErlangAdapter.put(db, :spo, <<2::64-big>>, "value2")
      :ok = ErlangAdapter.put(db, :spo, <<3::64-big>>, "value3")

      # Create snapshot
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Add more data after snapshot
      :ok = ErlangAdapter.put(db, :spo, <<4::64-big>>, "value4")
      :ok = ErlangAdapter.put(db, :spo, <<5::64-big>>, "value5")

      # Create iterator from snapshot
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, <<>>)

      # Iterator should only see data as of snapshot time
      assert {:ok, <<1::64-big>>, "value1"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, <<2::64-big>>, "value2"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, <<3::64-big>>, "value3"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      # Clean up
      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end

    test "2.2.3.2 snapshot iterator respects prefix boundaries" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      # Add data with different prefixes
      :ok = ErlangAdapter.put(db, :spo, <<1::64-big, 1::64-big>>, "value1_1")
      :ok = ErlangAdapter.put(db, :spo, <<1::64-big, 2::64-big>>, "value1_2")
      :ok = ErlangAdapter.put(db, :spo, <<2::64-big, 1::64-big>>, "value2_1")

      # Create snapshot
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Add more data after snapshot
      :ok = ErlangAdapter.put(db, :spo, <<1::64-big, 3::64-big>>, "value1_3")

      # Create iterator with prefix <<1::64-big>>
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, <<1::64-big>>)

      # Should only see keys with prefix <<1::64-big>> as of snapshot time
      assert {:ok, <<1::64-big, 1::64-big>>, "value1_1"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, <<1::64-big, 2::64-big>>, "value1_2"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end

    test "2.2.3.3 snapshot iterator can be collected" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      # Add data
      for i <- 1..5 do
        :ok = ErlangAdapter.put(db, :spo, <<i::64-big>>, "value#{i}")
      end

      # Create snapshot
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Add more data after snapshot
      for i <- 6..10 do
        :ok = ErlangAdapter.put(db, :spo, <<i::64-big>>, "value#{i}")
      end

      # Create iterator from snapshot and collect
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, <<>>)
      {:ok, entries} = ErlangAdapter.iterator_collect(iter)

      # Should only have 5 entries from snapshot time
      assert length(entries) == 5

      # Verify all entries are from snapshot time
      Enum.each(entries, fn {k, v} ->
        <<i::64-big>> = k
        expected = "value#{i}"
        assert v == expected
        assert i <= 5
      end)

      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end
  end

  describe "Section 2.2.4: Integration Tests" do
    test "2.2.4.1 snapshot doesn't see subsequent writes" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      :ok = ErlangAdapter.put(db, :spo, "key1", "original")

      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Perform many writes
      for i <- 1..100 do
        :ok = ErlangAdapter.put(db, :spo, "key#{i}", "value#{i}")
      end

      # Snapshot still sees original value
      assert {:ok, "original"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key1")
      assert :not_found = ErlangAdapter.snapshot_get(db, snap, :spo, "key2")

      ErlangAdapter.release_snapshot(db, snap)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end

    test "2.2.4.2 multiple snapshots see different time points" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      # Time 1: Add key1
      :ok = ErlangAdapter.put(db, :spo, "key", "value1")
      {:ok, snap1} = ErlangAdapter.snapshot(db)

      # Time 2: Modify key
      :ok = ErlangAdapter.put(db, :spo, "key", "value2")
      {:ok, snap2} = ErlangAdapter.snapshot(db)

      # Time 3: Modify again
      :ok = ErlangAdapter.put(db, :spo, "key", "value3")
      {:ok, snap3} = ErlangAdapter.snapshot(db)

      # Each snapshot sees different value
      assert {:ok, "value1"} = ErlangAdapter.snapshot_get(db, snap1, :spo, "key")
      assert {:ok, "value2"} = ErlangAdapter.snapshot_get(db, snap2, :spo, "key")
      assert {:ok, "value3"} = ErlangAdapter.snapshot_get(db, snap3, :spo, "key")
      assert {:ok, "value3"} = ErlangAdapter.get(db, :spo, "key")

      # Clean up
      ErlangAdapter.release_snapshot(db, snap1)
      ErlangAdapter.release_snapshot(db, snap2)
      ErlangAdapter.release_snapshot(db, snap3)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end

    test "2.2.4.3 snapshot with deletions" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      # Add data
      :ok = ErlangAdapter.put(db, :spo, "key1", "value1")
      :ok = ErlangAdapter.put(db, :spo, "key2", "value2")

      # Create snapshot
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Delete data
      :ok = ErlangAdapter.delete(db, :spo, "key1")
      :ok = ErlangAdapter.delete(db, :spo, "key2")

      # Snapshot still sees the data
      assert {:ok, "value1"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key1")
      assert {:ok, "value2"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key2")

      # Current db sees not_found
      assert :not_found = ErlangAdapter.get(db, :spo, "key1")
      assert :not_found = ErlangAdapter.get(db, :spo, "key2")

      ErlangAdapter.release_snapshot(db, snap)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end

    test "2.2.4.4 snapshot iterator sees historical data after modifications" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = ErlangAdapter.open(path)

      # Add initial data
      for i <- 1..10 do
        :ok = ErlangAdapter.put(db, :spo, <<i::64-big>>, "original_#{i}")
      end

      # Create snapshot
      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Modify all values
      for i <- 1..10 do
        :ok = ErlangAdapter.put(db, :spo, <<i::64-big>>, "modified_#{i}")
      end

      # Delete some keys
      :ok = ErlangAdapter.delete(db, :spo, <<5::64-big>>)
      :ok = ErlangAdapter.delete(db, :spo, <<6::64-big>>)

      # Create iterator from snapshot
      {:ok, iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, <<>>)
      {:ok, entries} = ErlangAdapter.iterator_collect(iter)

      # Should see all 10 original values
      assert length(entries) == 10

      Enum.each(entries, fn {k, v} ->
        <<i::64-big>> = k
        expected = "original_#{i}"
        assert v == expected
      end)

      ErlangAdapter.iterator_close(iter)
      ErlangAdapter.release_snapshot(db, snap)
      ErlangAdapter.close(db)
      File.rm_rf(path)
    end
  end
end
