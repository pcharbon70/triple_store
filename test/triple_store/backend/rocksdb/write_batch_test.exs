defmodule TripleStore.Backend.RocksDB.WriteBatchTest do
  @moduledoc """
  Tests for RocksDB WriteBatch operations (Task 1.2.3).
  """
  use TripleStore.PooledDbCase

  describe "write_batch/2" do
    test "writes multiple key-value pairs atomically", %{db: db} do
      operations = [
        {:id2str, "key1", "value1"},
        {:id2str, "key2", "value2"},
        {:id2str, "key3", "value3"}
      ]

      assert :ok = NIF.write_batch(db, operations)

      assert {:ok, "value1"} = NIF.get(db, :id2str, "key1")
      assert {:ok, "value2"} = NIF.get(db, :id2str, "key2")
      assert {:ok, "value3"} = NIF.get(db, :id2str, "key3")
    end

    test "writes to multiple column families atomically", %{db: db} do
      operations = [
        {:id2str, "id1", "string1"},
        {:str2id, "string1", "id1"},
        {:spo, "s1p1o1", ""},
        {:pos, "p1o1s1", ""},
        {:osp, "o1s1p1", ""}
      ]

      assert :ok = NIF.write_batch(db, operations)

      assert {:ok, "string1"} = NIF.get(db, :id2str, "id1")
      assert {:ok, "id1"} = NIF.get(db, :str2id, "string1")
      assert {:ok, ""} = NIF.get(db, :spo, "s1p1o1")
      assert {:ok, ""} = NIF.get(db, :pos, "p1o1s1")
      assert {:ok, ""} = NIF.get(db, :osp, "o1s1p1")
    end

    test "handles empty operation list", %{db: db} do
      assert :ok = NIF.write_batch(db, [])
    end

    test "handles binary keys and values", %{db: db} do
      key = <<1, 2, 3, 4, 5>>
      value = <<255, 254, 253>>

      operations = [
        {:spo, key, value}
      ]

      assert :ok = NIF.write_batch(db, operations)
      assert {:ok, ^value} = NIF.get(db, :spo, key)
    end

    test "returns error for invalid column family", %{db: db} do
      NIF.put(db, :id2str, "existing", "value")

      operations = [
        {:id2str, "key1", "value1"},
        {:nonexistent, "key2", "value2"}
      ]

      assert {:error, {:invalid_cf, :nonexistent}} = NIF.write_batch(db, operations)
      assert :not_found = NIF.get(db, :id2str, "key1")
      assert {:ok, "value"} = NIF.get(db, :id2str, "existing")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = NIF.open("#{path}_closed")
      NIF.close(db2)

      operations = [{:id2str, "key1", "value1"}]
      assert {:error, :already_closed} = NIF.write_batch(db2, operations)
      File.rm_rf("#{path}_closed")
    end

    test "handles large batch", %{db: db} do
      operations =
        for i <- 1..1000 do
          {:id2str, "key#{i}", "value#{i}"}
        end

      assert :ok = NIF.write_batch(db, operations)

      # Verify a sample
      assert {:ok, "value1"} = NIF.get(db, :id2str, "key1")
      assert {:ok, "value500"} = NIF.get(db, :id2str, "key500")
      assert {:ok, "value1000"} = NIF.get(db, :id2str, "key1000")
    end
  end

  describe "delete_batch/2" do
    test "deletes multiple keys atomically", %{db: db} do
      # First, insert some data
      NIF.write_batch(db, [
        {:id2str, "key1", "value1"},
        {:id2str, "key2", "value2"},
        {:id2str, "key3", "value3"}
      ])

      # Now delete
      operations = [
        {:id2str, "key1"},
        {:id2str, "key2"}
      ]

      assert :ok = NIF.delete_batch(db, operations)

      assert :not_found = NIF.get(db, :id2str, "key1")
      assert :not_found = NIF.get(db, :id2str, "key2")
      assert {:ok, "value3"} = NIF.get(db, :id2str, "key3")
    end

    test "deletes from multiple column families atomically", %{db: db} do
      # First, insert some data
      NIF.write_batch(db, [
        {:id2str, "id1", "string1"},
        {:str2id, "string1", "id1"},
        {:spo, "s1p1o1", ""}
      ])

      # Now delete
      operations = [
        {:id2str, "id1"},
        {:str2id, "string1"},
        {:spo, "s1p1o1"}
      ]

      assert :ok = NIF.delete_batch(db, operations)

      assert :not_found = NIF.get(db, :id2str, "id1")
      assert :not_found = NIF.get(db, :str2id, "string1")
      assert :not_found = NIF.get(db, :spo, "s1p1o1")
    end

    test "handles empty operation list", %{db: db} do
      assert :ok = NIF.delete_batch(db, [])
    end

    test "succeeds for non-existent keys", %{db: db} do
      operations = [
        {:id2str, "nonexistent1"},
        {:id2str, "nonexistent2"}
      ]

      assert :ok = NIF.delete_batch(db, operations)
    end

    test "returns error for invalid column family", %{db: db} do
      NIF.write_batch(db, [
        {:id2str, "key1", "value1"}
      ])

      operations = [
        {:id2str, "key1"},
        {:nonexistent, "key2"}
      ]

      assert {:error, {:invalid_cf, :nonexistent}} = NIF.delete_batch(db, operations)
      assert {:ok, "value1"} = NIF.get(db, :id2str, "key1")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = NIF.open("#{path}_closed")
      NIF.close(db2)

      operations = [{:id2str, "key1"}]
      assert {:error, :already_closed} = NIF.delete_batch(db2, operations)
      File.rm_rf("#{path}_closed")
    end
  end

  describe "mixed_batch/2" do
    test "performs mixed puts and deletes atomically", %{db: db} do
      # First, insert some data to delete
      NIF.write_batch(db, [
        {:id2str, "old_key1", "old_value1"},
        {:id2str, "old_key2", "old_value2"}
      ])

      # Now do mixed operations
      operations = [
        {:put, :id2str, "new_key1", "new_value1"},
        {:put, :id2str, "new_key2", "new_value2"},
        {:delete, :id2str, "old_key1"},
        {:delete, :id2str, "old_key2"}
      ]

      assert :ok = NIF.mixed_batch(db, operations)

      # Verify puts
      assert {:ok, "new_value1"} = NIF.get(db, :id2str, "new_key1")
      assert {:ok, "new_value2"} = NIF.get(db, :id2str, "new_key2")

      # Verify deletes
      assert :not_found = NIF.get(db, :id2str, "old_key1")
      assert :not_found = NIF.get(db, :id2str, "old_key2")
    end

    test "handles triple index update pattern", %{db: db} do
      # Simulate adding a new triple while removing an old one
      # This is the atomic update pattern needed for triple store

      # First add old triple
      NIF.write_batch(db, [
        {:spo, "old_spo", ""},
        {:pos, "old_pos", ""},
        {:osp, "old_osp", ""}
      ])

      # Now atomically replace with new triple
      operations = [
        {:delete, :spo, "old_spo"},
        {:delete, :pos, "old_pos"},
        {:delete, :osp, "old_osp"},
        {:put, :spo, "new_spo", ""},
        {:put, :pos, "new_pos", ""},
        {:put, :osp, "new_osp", ""}
      ]

      assert :ok = NIF.mixed_batch(db, operations)

      # Old triple gone
      assert :not_found = NIF.get(db, :spo, "old_spo")
      assert :not_found = NIF.get(db, :pos, "old_pos")
      assert :not_found = NIF.get(db, :osp, "old_osp")

      # New triple present
      assert {:ok, ""} = NIF.get(db, :spo, "new_spo")
      assert {:ok, ""} = NIF.get(db, :pos, "new_pos")
      assert {:ok, ""} = NIF.get(db, :osp, "new_osp")
    end

    test "handles empty operation list", %{db: db} do
      assert :ok = NIF.mixed_batch(db, [])
    end

    test "handles puts only", %{db: db} do
      operations = [
        {:put, :id2str, "key1", "value1"},
        {:put, :id2str, "key2", "value2"}
      ]

      assert :ok = NIF.mixed_batch(db, operations)

      assert {:ok, "value1"} = NIF.get(db, :id2str, "key1")
      assert {:ok, "value2"} = NIF.get(db, :id2str, "key2")
    end

    test "handles deletes only", %{db: db} do
      NIF.write_batch(db, [
        {:id2str, "key1", "value1"},
        {:id2str, "key2", "value2"}
      ])

      operations = [
        {:delete, :id2str, "key1"},
        {:delete, :id2str, "key2"}
      ]

      assert :ok = NIF.mixed_batch(db, operations)

      assert :not_found = NIF.get(db, :id2str, "key1")
      assert :not_found = NIF.get(db, :id2str, "key2")
    end

    test "returns error for invalid operation type", %{db: db} do
      NIF.put(db, :id2str, "to_delete", "value")

      operations = [
        {:put, :id2str, "key1", "value1"},
        {:delete, :id2str, "to_delete"},
        {:invalid_op, :id2str, "key2"}
      ]

      assert {:error, {:invalid_operation, :invalid_op}} = NIF.mixed_batch(db, operations)
      assert :not_found = NIF.get(db, :id2str, "key1")
      assert {:ok, "value"} = NIF.get(db, :id2str, "to_delete")
    end

    test "returns error for invalid column family in put", %{db: db} do
      operations = [
        {:put, :nonexistent, "key1", "value1"}
      ]

      assert {:error, {:invalid_cf, :nonexistent}} = NIF.mixed_batch(db, operations)
    end

    test "returns error for invalid column family in delete", %{db: db} do
      operations = [
        {:delete, :nonexistent, "key1"}
      ]

      assert {:error, {:invalid_cf, :nonexistent}} = NIF.mixed_batch(db, operations)
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = NIF.open("#{path}_closed")
      NIF.close(db2)

      operations = [{:put, :id2str, "key1", "value1"}]
      assert {:error, :already_closed} = NIF.mixed_batch(db2, operations)
      File.rm_rf("#{path}_closed")
    end
  end

  describe "atomicity" do
    test "write_batch is atomic - all or nothing", %{db: db} do
      # First write some data
      NIF.put(db, :id2str, "existing", "value")

      # Attempt batch with invalid CF - should fail
      operations = [
        {:id2str, "key1", "value1"},
        {:nonexistent, "key2", "value2"}
      ]

      assert {:error, {:invalid_cf, :nonexistent}} = NIF.write_batch(db, operations)

      # The first key should NOT have been written due to atomic failure
      # Note: In RocksDB, validation happens before write, so partial write doesn't occur
      assert :not_found = NIF.get(db, :id2str, "key1")

      # Original data should be unchanged
      assert {:ok, "value"} = NIF.get(db, :id2str, "existing")
    end

    test "data persists after batch write and reopen", %{db_path: path} do
      {:ok, db1} = NIF.open("#{path}_persist")

      operations = [
        {:id2str, "key1", "value1"},
        {:id2str, "key2", "value2"}
      ]

      NIF.write_batch(db1, operations)
      NIF.close(db1)

      {:ok, db2} = NIF.open("#{path}_persist")
      assert {:ok, "value1"} = NIF.get(db2, :id2str, "key1")
      assert {:ok, "value2"} = NIF.get(db2, :id2str, "key2")
      NIF.close(db2)
      File.rm_rf("#{path}_persist")
    end
  end
end
