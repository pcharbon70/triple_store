defmodule TripleStore.Backend.RocksDB.ReadWriteTest do
  @moduledoc """
  Tests for RocksDB basic read/write operations (Task 1.2.2).
  """
  use TripleStore.PooledDbCase

  describe "put/4" do
    test "writes a key-value pair successfully", %{db: db} do
      assert :ok = NIF.put(db, :id2str, "key1", "value1")
    end

    test "writes to all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "test_key_#{cf}"
        value = "test_value_#{cf}"
        assert :ok = NIF.put(db, cf, key, value), "Failed to put to #{cf}"
      end
    end

    test "overwrites existing key", %{db: db} do
      assert :ok = NIF.put(db, :id2str, "key1", "value1")
      assert :ok = NIF.put(db, :id2str, "key1", "value2")
      assert {:ok, "value2"} = NIF.get(db, :id2str, "key1")
    end

    test "handles binary keys and values", %{db: db} do
      key = <<1, 2, 3, 4, 5>>
      value = <<255, 254, 253, 0, 1>>
      assert :ok = NIF.put(db, :spo, key, value)
      assert {:ok, ^value} = NIF.get(db, :spo, key)
    end

    test "handles empty value", %{db: db} do
      assert :ok = NIF.put(db, :id2str, "empty_key", "")
      assert {:ok, ""} = NIF.get(db, :id2str, "empty_key")
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, {:invalid_cf, :nonexistent}} = NIF.put(db, :nonexistent, "key", "value")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = NIF.open("#{path}_closed")
      NIF.close(db2)
      assert {:error, :already_closed} = NIF.put(db2, :id2str, "key", "value")
      File.rm_rf("#{path}_closed")
    end
  end

  describe "get/3" do
    test "retrieves an existing key", %{db: db} do
      NIF.put(db, :id2str, "key1", "value1")
      assert {:ok, "value1"} = NIF.get(db, :id2str, "key1")
    end

    test "returns :not_found for missing key", %{db: db} do
      assert :not_found = NIF.get(db, :id2str, "nonexistent")
    end

    test "retrieves from all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "test_key_#{cf}"
        value = "test_value_#{cf}"
        NIF.put(db, cf, key, value)
        assert {:ok, ^value} = NIF.get(db, cf, key), "Failed to get from #{cf}"
      end
    end

    test "handles binary keys", %{db: db} do
      key = <<1, 2, 3, 4, 5>>
      value = "binary_key_value"
      NIF.put(db, :spo, key, value)
      assert {:ok, ^value} = NIF.get(db, :spo, key)
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, {:invalid_cf, :nonexistent}} = NIF.get(db, :nonexistent, "key")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = NIF.open("#{path}_closed")
      NIF.close(db2)
      assert {:error, :already_closed} = NIF.get(db2, :id2str, "key")
      File.rm_rf("#{path}_closed")
    end
  end

  describe "delete/3" do
    test "deletes an existing key", %{db: db} do
      NIF.put(db, :id2str, "key1", "value1")
      assert :ok = NIF.delete(db, :id2str, "key1")
      assert :not_found = NIF.get(db, :id2str, "key1")
    end

    test "succeeds even if key doesn't exist", %{db: db} do
      assert :ok = NIF.delete(db, :id2str, "nonexistent")
    end

    test "deletes from all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "delete_key_#{cf}"
        NIF.put(db, cf, key, "value")
        assert :ok = NIF.delete(db, cf, key), "Failed to delete from #{cf}"
        assert :not_found = NIF.get(db, cf, key)
      end
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, {:invalid_cf, :nonexistent}} = NIF.delete(db, :nonexistent, "key")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = NIF.open("#{path}_closed")
      NIF.close(db2)
      assert {:error, :already_closed} = NIF.delete(db2, :id2str, "key")
      File.rm_rf("#{path}_closed")
    end
  end

  describe "exists/3" do
    test "returns true for existing key", %{db: db} do
      NIF.put(db, :id2str, "key1", "value1")
      assert {:ok, true} = NIF.exists(db, :id2str, "key1")
    end

    test "returns false for missing key", %{db: db} do
      assert {:ok, false} = NIF.exists(db, :id2str, "nonexistent")
    end

    test "returns false after delete", %{db: db} do
      NIF.put(db, :id2str, "key1", "value1")
      NIF.delete(db, :id2str, "key1")
      assert {:ok, false} = NIF.exists(db, :id2str, "key1")
    end

    test "checks existence in all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        key = "exists_key_#{cf}"
        NIF.put(db, cf, key, "value")
        assert {:ok, true} = NIF.exists(db, cf, key), "Failed exists check for #{cf}"
      end
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, {:invalid_cf, :nonexistent}} = NIF.exists(db, :nonexistent, "key")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = NIF.open("#{path}_closed")
      NIF.close(db2)
      assert {:error, :already_closed} = NIF.exists(db2, :id2str, "key")
      File.rm_rf("#{path}_closed")
    end
  end

  describe "data persistence" do
    test "data persists after close and reopen", %{db_path: path} do
      {:ok, db1} = NIF.open("#{path}_persist")
      NIF.put(db1, :id2str, "persist_key", "persist_value")
      NIF.close(db1)

      {:ok, db2} = NIF.open("#{path}_persist")
      assert {:ok, "persist_value"} = NIF.get(db2, :id2str, "persist_key")
      NIF.close(db2)
      File.rm_rf("#{path}_persist")
    end
  end

  describe "concurrent access" do
    test "handles concurrent reads", %{db: db} do
      # Write some data first
      for i <- 1..100 do
        NIF.put(db, :id2str, "key#{i}", "value#{i}")
      end

      # Concurrent reads
      tasks =
        for i <- 1..100 do
          expected = "value#{i}"
          key = "key#{i}"

          Task.async(fn ->
            assert {:ok, ^expected} = NIF.get(db, :id2str, key)
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
            :ok = NIF.put(db, :id2str, "concurrent_key#{i}", "value#{i}")
          end)
        end

      results = Task.await_many(tasks, 5000)
      assert Enum.all?(results, &(&1 == :ok))

      # Verify all writes succeeded
      for i <- 1..50 do
        expected = "value#{i}"
        assert {:ok, ^expected} = NIF.get(db, :id2str, "concurrent_key#{i}")
      end
    end
  end
end
