defmodule TripleStore.Backend.RocksDB.ReadWriteTest do
  @moduledoc """
  Tests for RocksDB basic read/write operations (Task 1.2.2).
  """
  use TripleStore.PooledDbCase

  describe "put/4" do
    test "writes a key-value pair successfully", %{db: db} do
      assert :ok = ErlangAdapter.put(db, :id2str, "key1", "value1")
    end

    test "writes to all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "test_key_#{cf}"
        value = "test_value_#{cf}"
        assert :ok = ErlangAdapter.put(db, cf, key, value), "Failed to put to #{cf}"
      end
    end

    test "overwrites existing key", %{db: db} do
      assert :ok = ErlangAdapter.put(db, :id2str, "key1", "value1")
      assert :ok = ErlangAdapter.put(db, :id2str, "key1", "value2")
      assert {:ok, "value2"} = ErlangAdapter.get(db, :id2str, "key1")
    end

    test "handles binary keys and values", %{db: db} do
      key = <<1, 2, 3, 4, 5>>
      value = <<255, 254, 253, 0, 1>>
      assert :ok = ErlangAdapter.put(db, :spo, key, value)
      assert {:ok, ^value} = ErlangAdapter.get(db, :spo, key)
    end

    test "handles empty value", %{db: db} do
      assert :ok = ErlangAdapter.put(db, :id2str, "empty_key", "")
      assert {:ok, ""} = ErlangAdapter.get(db, :id2str, "empty_key")
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, {:invalid_cf, :nonexistent}} = ErlangAdapter.put(db, :nonexistent, "key", "value")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = ErlangAdapter.open("#{path}_closed")
      ErlangAdapter.close(db2)
      assert {:error, :already_closed} = ErlangAdapter.put(db2, :id2str, "key", "value")
      File.rm_rf("#{path}_closed")
    end
  end

  describe "get/3" do
    test "retrieves an existing key", %{db: db} do
      ErlangAdapter.put(db, :id2str, "key1", "value1")
      assert {:ok, "value1"} = ErlangAdapter.get(db, :id2str, "key1")
    end

    test "returns :not_found for missing key", %{db: db} do
      assert :not_found = ErlangAdapter.get(db, :id2str, "nonexistent")
    end

    test "retrieves from all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "test_key_#{cf}"
        value = "test_value_#{cf}"
        ErlangAdapter.put(db, cf, key, value)
        assert {:ok, ^value} = ErlangAdapter.get(db, cf, key), "Failed to get from #{cf}"
      end
    end

    test "handles binary keys", %{db: db} do
      key = <<1, 2, 3, 4, 5>>
      value = "binary_key_value"
      ErlangAdapter.put(db, :spo, key, value)
      assert {:ok, ^value} = ErlangAdapter.get(db, :spo, key)
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, {:invalid_cf, :nonexistent}} = ErlangAdapter.get(db, :nonexistent, "key")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = ErlangAdapter.open("#{path}_closed")
      ErlangAdapter.close(db2)
      assert {:error, :already_closed} = ErlangAdapter.get(db2, :id2str, "key")
      File.rm_rf("#{path}_closed")
    end
  end

  describe "delete/3" do
    test "deletes an existing key", %{db: db} do
      ErlangAdapter.put(db, :id2str, "key1", "value1")
      assert :ok = ErlangAdapter.delete(db, :id2str, "key1")
      assert :not_found = ErlangAdapter.get(db, :id2str, "key1")
    end

    test "succeeds even if key doesn't exist", %{db: db} do
      assert :ok = ErlangAdapter.delete(db, :id2str, "nonexistent")
    end

    test "deletes from all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "delete_key_#{cf}"
        ErlangAdapter.put(db, cf, key, "value")
        assert :ok = ErlangAdapter.delete(db, cf, key), "Failed to delete from #{cf}"
        assert :not_found = ErlangAdapter.get(db, cf, key)
      end
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, {:invalid_cf, :nonexistent}} = ErlangAdapter.delete(db, :nonexistent, "key")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = ErlangAdapter.open("#{path}_closed")
      ErlangAdapter.close(db2)
      assert {:error, :already_closed} = ErlangAdapter.delete(db2, :id2str, "key")
      File.rm_rf("#{path}_closed")
    end
  end

  describe "exists/3" do
    test "returns true for existing key", %{db: db} do
      ErlangAdapter.put(db, :id2str, "key1", "value1")
      assert {:ok, true} = ErlangAdapter.exists(db, :id2str, "key1")
    end

    test "returns false for missing key", %{db: db} do
      assert {:ok, false} = ErlangAdapter.exists(db, :id2str, "nonexistent")
    end

    test "returns false after delete", %{db: db} do
      ErlangAdapter.put(db, :id2str, "key1", "value1")
      ErlangAdapter.delete(db, :id2str, "key1")
      assert {:ok, false} = ErlangAdapter.exists(db, :id2str, "key1")
    end

    test "checks existence in all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "exists_key_#{cf}"
        ErlangAdapter.put(db, cf, key, "value")
        assert {:ok, true} = ErlangAdapter.exists(db, cf, key), "Failed exists check for #{cf}"
      end
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, {:invalid_cf, :nonexistent}} = ErlangAdapter.exists(db, :nonexistent, "key")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = ErlangAdapter.open("#{path}_closed")
      ErlangAdapter.close(db2)
      assert {:error, :already_closed} = ErlangAdapter.exists(db2, :id2str, "key")
      File.rm_rf("#{path}_closed")
    end
  end

  describe "data persistence" do
    test "data persists after close and reopen", %{db_path: path} do
      {:ok, db1} = ErlangAdapter.open("#{path}_persist")
      ErlangAdapter.put(db1, :id2str, "persist_key", "persist_value")
      ErlangAdapter.close(db1)

      {:ok, db2} = ErlangAdapter.open("#{path}_persist")
      assert {:ok, "persist_value"} = ErlangAdapter.get(db2, :id2str, "persist_key")
      ErlangAdapter.close(db2)
      File.rm_rf("#{path}_persist")
    end
  end

  describe "concurrent access" do
    test "handles concurrent reads", %{db: db} do
      # Write some data first
      for i <- 1..100 do
        ErlangAdapter.put(db, :id2str, "key#{i}", "value#{i}")
      end

      # Concurrent reads
      tasks =
        for i <- 1..100 do
          expected = "value#{i}"
          key = "key#{i}"

          Task.async(fn ->
            assert {:ok, ^expected} = ErlangAdapter.get(db, :id2str, key)
            :ok
          end)
        end

      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == :ok))
    end

    test "handles concurrent writes to different keys", %{db: db} do
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            :ok = ErlangAdapter.put(db, :id2str, "concurrent_key#{i}", "value#{i}")
          end)
        end

      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == :ok))

      # Verify all writes succeeded
      for i <- 1..50 do
        expected = "value#{i}"
        assert {:ok, ^expected} = ErlangAdapter.get(db, :id2str, "concurrent_key#{i}")
      end
    end
  end
end
