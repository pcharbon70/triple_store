defmodule TripleStore.Backend.RocksDB.SnapshotOperationsTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF

  @moduletag :snapshot_operations
  @moduletag timeout: 120_000

  describe "Section 2.2.1: Snapshot Creation and Release" do
    test "2.2.1.1 snapshot/1 creates a point-in-time snapshot" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add some data
      :ok = NIF.put(db, :spo, "key1", "value1")
      :ok = NIF.put(db, :spo, "key2", "value2")

      # Create snapshot
      {:ok, snapshot} = NIF.snapshot(db)
      assert is_reference(snapshot)

      # Clean up
      NIF.release_snapshot(db, snapshot)
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.2.1.2 release_snapshot/1 releases snapshot resources" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      {:ok, snapshot} = NIF.snapshot(db)
      assert :ok = NIF.release_snapshot(db, snapshot)

      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.2.1.3 multiple snapshots can coexist" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Create first snapshot
      :ok = NIF.put(db, :spo, "key1", "value1")
      {:ok, snap1} = NIF.snapshot(db)

      # Add more data
      :ok = NIF.put(db, :spo, "key2", "value2")

      # Create second snapshot
      {:ok, snap2} = NIF.snapshot(db)

      # Both snapshots should be valid references
      assert is_reference(snap1)
      assert is_reference(snap2)
      refute snap1 == snap2

      # Clean up
      NIF.release_snapshot(db, snap1)
      NIF.release_snapshot(db, snap2)
      NIF.close(db)
      File.rm_rf(path)
    end
  end

  describe "Section 2.2.2: Snapshot Read Operations" do
    test "2.2.2.1 snapshot_get/4 reads value at snapshot time" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add initial data
      :ok = NIF.put(db, :spo, "key1", "value1")

      # Create snapshot
      {:ok, snap} = NIF.snapshot(db)

      # Modify the data
      :ok = NIF.put(db, :spo, "key1", "value2")
      :ok = NIF.put(db, :spo, "key2", "value2")

      # Snapshot should still see old value
      assert {:ok, "value1"} = NIF.snapshot_get(db, snap, :spo, "key1")
      # Snapshot doesn't see new key
      assert :not_found = NIF.snapshot_get(db, snap, :spo, "key2")

      # Current db sees new values
      assert {:ok, "value2"} = NIF.get(db, :spo, "key1")
      assert {:ok, "value2"} = NIF.get(db, :spo, "key2")

      # Clean up
      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.2.2.2 snapshot_get/4 handles not_found consistently" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      {:ok, snap} = NIF.snapshot(db)

      # Key doesn't exist in snapshot
      assert :not_found = NIF.snapshot_get(db, snap, :spo, "nonexistent")

      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.2.2.3 snapshot provides point-in-time consistency across CFs" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data to multiple column families
      :ok = NIF.put(db, :spo, "key1", "spo_value1")
      :ok = NIF.put(db, :pos, "key1", "pos_value1")
      :ok = NIF.put(db, :osp, "key1", "osp_value1")

      # Create snapshot
      {:ok, snap} = NIF.snapshot(db)

      # Modify all CFs
      :ok = NIF.put(db, :spo, "key1", "spo_value2")
      :ok = NIF.put(db, :pos, "key1", "pos_value2")
      :ok = NIF.put(db, :osp, "key1", "osp_value2")

      # Snapshot sees old values in all CFs
      assert {:ok, "spo_value1"} = NIF.snapshot_get(db, snap, :spo, "key1")
      assert {:ok, "pos_value1"} = NIF.snapshot_get(db, snap, :pos, "key1")
      assert {:ok, "osp_value1"} = NIF.snapshot_get(db, snap, :osp, "key1")

      # Current db sees new values
      assert {:ok, "spo_value2"} = NIF.get(db, :spo, "key1")
      assert {:ok, "pos_value2"} = NIF.get(db, :pos, "key1")
      assert {:ok, "osp_value2"} = NIF.get(db, :osp, "key1")

      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end
  end

  describe "Section 2.2.3: Snapshot Iterator Operations" do
    test "2.2.3.1 snapshot_prefix_iterator/4 iterates over snapshot state" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data: <<1>>, <<2>>, <<3>>
      :ok = NIF.put(db, :spo, <<1::64-big>>, "value1")
      :ok = NIF.put(db, :spo, <<2::64-big>>, "value2")
      :ok = NIF.put(db, :spo, <<3::64-big>>, "value3")

      # Create snapshot
      {:ok, snap} = NIF.snapshot(db)

      # Add more data after snapshot
      :ok = NIF.put(db, :spo, <<4::64-big>>, "value4")
      :ok = NIF.put(db, :spo, <<5::64-big>>, "value5")

      # Create iterator from snapshot
      {:ok, iter} = NIF.snapshot_prefix_iterator(db, snap, :spo, <<>>)

      # Iterator should only see data as of snapshot time
      assert {:ok, <<1::64-big>>, "value1"} = NIF.iterator_next(iter)
      assert {:ok, <<2::64-big>>, "value2"} = NIF.iterator_next(iter)
      assert {:ok, <<3::64-big>>, "value3"} = NIF.iterator_next(iter)
      assert :iterator_end = NIF.iterator_next(iter)

      # Clean up
      NIF.iterator_close(iter)
      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.2.3.2 snapshot iterator respects prefix boundaries" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data with different prefixes
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big>>, "value1_1")
      :ok = NIF.put(db, :spo, <<1::64-big, 2::64-big>>, "value1_2")
      :ok = NIF.put(db, :spo, <<2::64-big, 1::64-big>>, "value2_1")

      # Create snapshot
      {:ok, snap} = NIF.snapshot(db)

      # Add more data after snapshot
      :ok = NIF.put(db, :spo, <<1::64-big, 3::64-big>>, "value1_3")

      # Create iterator with prefix <<1::64-big>>
      {:ok, iter} = NIF.snapshot_prefix_iterator(db, snap, :spo, <<1::64-big>>)

      # Should only see keys with prefix <<1::64-big>> as of snapshot time
      assert {:ok, <<1::64-big, 1::64-big>>, "value1_1"} = NIF.iterator_next(iter)
      assert {:ok, <<1::64-big, 2::64-big>>, "value1_2"} = NIF.iterator_next(iter)
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.2.3.3 snapshot iterator can be collected" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      for i <- 1..5 do
        :ok = NIF.put(db, :spo, <<i::64-big>>, "value#{i}")
      end

      # Create snapshot
      {:ok, snap} = NIF.snapshot(db)

      # Add more data after snapshot
      for i <- 6..10 do
        :ok = NIF.put(db, :spo, <<i::64-big>>, "value#{i}")
      end

      # Create iterator from snapshot and collect
      {:ok, iter} = NIF.snapshot_prefix_iterator(db, snap, :spo, <<>>)
      {:ok, entries} = NIF.snapshot_iterator_collect(iter)

      # Should only have 5 entries from snapshot time
      assert length(entries) == 5

      # Verify all entries are from snapshot time
      Enum.each(entries, fn {k, v} ->
        <<i::64-big>> = k
        expected = "value#{i}"
        assert v == expected
        assert i <= 5
      end)

      NIF.iterator_close(iter)
      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end
  end

  describe "Section 2.2.4: Integration Tests" do
    test "2.2.4.1 snapshot doesn't see subsequent writes" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      :ok = NIF.put(db, :spo, "key1", "original")

      {:ok, snap} = NIF.snapshot(db)

      # Perform many writes
      for i <- 1..100 do
        :ok = NIF.put(db, :spo, "key#{i}", "value#{i}")
      end

      # Snapshot still sees original value
      assert {:ok, "original"} = NIF.snapshot_get(db, snap, :spo, "key1")
      assert :not_found = NIF.snapshot_get(db, snap, :spo, "key2")

      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.2.4.2 multiple snapshots see different time points" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Time 1: Add key1
      :ok = NIF.put(db, :spo, "key", "value1")
      {:ok, snap1} = NIF.snapshot(db)

      # Time 2: Modify key
      :ok = NIF.put(db, :spo, "key", "value2")
      {:ok, snap2} = NIF.snapshot(db)

      # Time 3: Modify again
      :ok = NIF.put(db, :spo, "key", "value3")
      {:ok, snap3} = NIF.snapshot(db)

      # Each snapshot sees different value
      assert {:ok, "value1"} = NIF.snapshot_get(db, snap1, :spo, "key")
      assert {:ok, "value2"} = NIF.snapshot_get(db, snap2, :spo, "key")
      assert {:ok, "value3"} = NIF.snapshot_get(db, snap3, :spo, "key")
      assert {:ok, "value3"} = NIF.get(db, :spo, "key")

      # Clean up
      NIF.release_snapshot(db, snap1)
      NIF.release_snapshot(db, snap2)
      NIF.release_snapshot(db, snap3)
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.2.4.3 snapshot with deletions" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      :ok = NIF.put(db, :spo, "key1", "value1")
      :ok = NIF.put(db, :spo, "key2", "value2")

      # Create snapshot
      {:ok, snap} = NIF.snapshot(db)

      # Delete data
      :ok = NIF.delete(db, :spo, "key1")
      :ok = NIF.delete(db, :spo, "key2")

      # Snapshot still sees the data
      assert {:ok, "value1"} = NIF.snapshot_get(db, snap, :spo, "key1")
      assert {:ok, "value2"} = NIF.snapshot_get(db, snap, :spo, "key2")

      # Current db sees not_found
      assert :not_found = NIF.get(db, :spo, "key1")
      assert :not_found = NIF.get(db, :spo, "key2")

      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.2.4.4 snapshot iterator sees historical data after modifications" do
      path = "/tmp/test_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add initial data
      for i <- 1..10 do
        :ok = NIF.put(db, :spo, <<i::64-big>>, "original_#{i}")
      end

      # Create snapshot
      {:ok, snap} = NIF.snapshot(db)

      # Modify all values
      for i <- 1..10 do
        :ok = NIF.put(db, :spo, <<i::64-big>>, "modified_#{i}")
      end

      # Delete some keys
      :ok = NIF.delete(db, :spo, <<5::64-big>>)
      :ok = NIF.delete(db, :spo, <<6::64-big>>)

      # Create iterator from snapshot
      {:ok, iter} = NIF.snapshot_prefix_iterator(db, snap, :spo, <<>>)
      {:ok, entries} = NIF.snapshot_iterator_collect(iter)

      # Should see all 10 original values
      assert length(entries) == 10

      Enum.each(entries, fn {k, v} ->
        <<i::64-big>> = k
        expected = "original_#{i}"
        assert v == expected
      end)

      NIF.iterator_close(iter)
      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end
  end
end
