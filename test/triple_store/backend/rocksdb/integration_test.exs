defmodule TripleStore.Backend.RocksDB.IntegrationTest do
  @moduledoc """
  Integration tests for RocksDB NIF wrapper (Task 1.2.6).

  These tests verify cross-component functionality and integration scenarios
  that span multiple NIF operations working together.
  """
  use TripleStore.PooledDbCase

  describe "database lifecycle with operations" do
    test "data persists across close and reopen cycles", %{db_path: path} do
      # First session: write data
      {:ok, db1} = ErlangAdapter.open("#{path}_persist")

      ErlangAdapter.put(db1, :spo, "key1", "value1")

      ErlangAdapter.write_batch(
        db1,
        [
          {:id2str, "id1", "string1"},
          {:str2id, "string1", "id1"}
        ],
        true
      )

      ErlangAdapter.close(db1)

      # Second session: verify and modify
      {:ok, db2} = ErlangAdapter.open("#{path}_persist")

      assert {:ok, "value1"} = ErlangAdapter.get(db2, :spo, "key1")
      assert {:ok, "string1"} = ErlangAdapter.get(db2, :id2str, "id1")

      ErlangAdapter.put(db2, :spo, "key2", "value2")
      ErlangAdapter.close(db2)

      # Third session: verify all data
      {:ok, db3} = ErlangAdapter.open("#{path}_persist")

      assert {:ok, "value1"} = ErlangAdapter.get(db3, :spo, "key1")
      assert {:ok, "value2"} = ErlangAdapter.get(db3, :spo, "key2")
      assert {:ok, "string1"} = ErlangAdapter.get(db3, :id2str, "id1")

      ErlangAdapter.close(db3)
      File.rm_rf("#{path}_persist")
    end

    test "operations fail gracefully after database close", %{db_path: path} do
      {:ok, db} = ErlangAdapter.open("#{path}_close_test")
      ErlangAdapter.put(db, :spo, "key", "value")
      ErlangAdapter.close(db)

      # All operations should return already_closed error
      assert {:error, :already_closed} = ErlangAdapter.get(db, :spo, "key")
      assert {:error, :already_closed} = ErlangAdapter.put(db, :spo, "key", "new")
      assert {:error, :already_closed} = ErlangAdapter.delete(db, :spo, "key")
      assert {:error, :already_closed} = ErlangAdapter.exists(db, :spo, "key")
      assert {:error, :already_closed} = ErlangAdapter.write_batch(db, [{:spo, "k", "v"}], true)
      assert {:error, :already_closed} = ErlangAdapter.prefix_iterator(db, :spo, "")
      assert {:error, :already_closed} = ErlangAdapter.snapshot(db)

      File.rm_rf("#{path}_close_test")
    end
  end

  describe "column family isolation" do
    test "same key in different column families are independent", %{db: db} do
      key = "shared_key"

      # Write different values to same key in each CF
      for {cf, value} <- [
            {:id2str, "id2str_value"},
            {:str2id, "str2id_value"},
            {:spo, "spo_value"},
            {:pos, "pos_value"},
            {:osp, "osp_value"},
            {:derived, "derived_value"}
          ] do
        ErlangAdapter.put(db, cf, key, value)
      end

      # Verify each CF has its own value
      assert {:ok, "id2str_value"} = ErlangAdapter.get(db, :id2str, key)
      assert {:ok, "str2id_value"} = ErlangAdapter.get(db, :str2id, key)
      assert {:ok, "spo_value"} = ErlangAdapter.get(db, :spo, key)
      assert {:ok, "pos_value"} = ErlangAdapter.get(db, :pos, key)
      assert {:ok, "osp_value"} = ErlangAdapter.get(db, :osp, key)
      assert {:ok, "derived_value"} = ErlangAdapter.get(db, :derived, key)

      # Delete from one CF doesn't affect others
      ErlangAdapter.delete(db, :spo, key)

      assert :not_found = ErlangAdapter.get(db, :spo, key)
      assert {:ok, "id2str_value"} = ErlangAdapter.get(db, :id2str, key)
    end
  end

  describe "batch operations with iterators" do
    test "iterator sees batch-written data", %{db: db} do
      operations =
        for i <- 1..10 do
          {:spo, "key#{String.pad_leading("#{i}", 2, "0")}", "value#{i}"}
        end

      ErlangAdapter.write_batch(db, operations, true)

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "key")
      {:ok, results} = ErlangAdapter.iterator_collect(iter)
      ErlangAdapter.iterator_close(iter)

      assert length(results) == 10

      # Verify order
      keys = Enum.map(results, fn {k, _v} -> k end)
      assert keys == Enum.sort(keys)
    end

    test "mixed_batch updates are visible to iterator", %{db: db} do
      # Initial data
      ErlangAdapter.write_batch(
        db,
        [
          {:spo, "a", "1"},
          {:spo, "b", "2"},
          {:spo, "c", "3"}
        ],
        true
      )

      # Mixed batch: delete b, add d
      ErlangAdapter.mixed_batch(
        db,
        [
          {:delete, :spo, "b"},
          {:put, :spo, "d", "4"}
        ],
        true
      )

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")
      {:ok, results} = ErlangAdapter.iterator_collect(iter)
      ErlangAdapter.iterator_close(iter)

      keys = Enum.map(results, fn {k, _v} -> k end)
      assert "a" in keys
      refute "b" in keys
      assert "c" in keys
      assert "d" in keys
    end
  end

  describe "snapshot with iterators" do
    test "snapshot iterator sees data at snapshot time only", %{db: db} do
      # Initial data
      ErlangAdapter.write_batch(
        db,
        [
          {:spo, "key1", "v1"},
          {:spo, "key2", "v2"}
        ],
        true
      )

      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Add more data after snapshot
      ErlangAdapter.write_batch(
        db,
        [
          {:spo, "key3", "v3"},
          {:spo, "key4", "v4"}
        ],
        true
      )

      # Snapshot iterator
      {:ok, snap_iter} = ErlangAdapter.snapshot_prefix_iterator(db, snap, :spo, "key")
      {:ok, snap_results} = ErlangAdapter.iterator_collect(snap_iter)
      ErlangAdapter.iterator_close(snap_iter)

      # Regular iterator
      {:ok, reg_iter} = ErlangAdapter.prefix_iterator(db, :spo, "key")
      {:ok, reg_results} = ErlangAdapter.iterator_collect(reg_iter)
      ErlangAdapter.iterator_close(reg_iter)

      assert length(snap_results) == 2
      assert length(reg_results) == 4

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "multiple snapshots at different points see different data", %{db: db} do
      ErlangAdapter.put(db, :spo, "key", "v1")
      {:ok, snap1} = ErlangAdapter.snapshot(db)

      ErlangAdapter.put(db, :spo, "key", "v2")
      {:ok, snap2} = ErlangAdapter.snapshot(db)

      ErlangAdapter.put(db, :spo, "key", "v3")
      {:ok, snap3} = ErlangAdapter.snapshot(db)

      # Create iterators from each snapshot
      {:ok, iter1} = ErlangAdapter.snapshot_prefix_iterator(db, snap1, :spo, "key")
      {:ok, iter2} = ErlangAdapter.snapshot_prefix_iterator(db, snap2, :spo, "key")
      {:ok, iter3} = ErlangAdapter.snapshot_prefix_iterator(db, snap3, :spo, "key")

      assert {:ok, "key", "v1"} = ErlangAdapter.iterator_next(iter1)
      assert {:ok, "key", "v2"} = ErlangAdapter.iterator_next(iter2)
      assert {:ok, "key", "v3"} = ErlangAdapter.iterator_next(iter3)

      ErlangAdapter.iterator_close(iter1)
      ErlangAdapter.iterator_close(iter2)
      ErlangAdapter.iterator_close(iter3)

      ErlangAdapter.release_snapshot(db, snap1)
      ErlangAdapter.release_snapshot(db, snap2)
      ErlangAdapter.release_snapshot(db, snap3)
    end
  end

  describe "stream wrappers" do
    test "prefix_stream works with Enum operations", %{db: db} do
      for i <- 1..100 do
        ErlangAdapter.put(db, :id2str, "item#{String.pad_leading("#{i}", 3, "0")}", "value#{i}")
      end

      {:ok, stream} = ErlangAdapter.prefix_stream(db, :id2str, "item")

      # Various Enum operations
      assert Enum.count(stream) == 100

      {:ok, stream2} = ErlangAdapter.prefix_stream(db, :id2str, "item")
      first_10 = Enum.take(stream2, 10)
      assert length(first_10) == 10

      {:ok, stream3} = ErlangAdapter.prefix_stream(db, :id2str, "item")

      filtered =
        stream3
        |> Enum.filter(fn {_k, v} -> String.ends_with?(v, "0") end)

      # value10, value20, ... value100 (10 items)
      assert length(filtered) == 10
    end

    test "snapshot_stream provides consistent view during iteration", %{db: db} do
      for i <- 1..50 do
        ErlangAdapter.put(db, :spo, "key#{String.pad_leading("#{i}", 2, "0")}", "v#{i}")
      end

      {:ok, snap} = ErlangAdapter.snapshot(db)
      stream = ErlangAdapter.snapshot_stream(db, snap, :spo, "key")

      # Modify data during iteration
      results =
        Enum.map(stream, fn {k, v} ->
          # Write new data during iteration
          ErlangAdapter.put(db, :spo, "new_#{k}", "modified")
          {k, v}
        end)

      # Should have gotten all 50 original items
      assert length(results) == 50

      # But new data should exist
      assert {:ok, "modified"} = ErlangAdapter.get(db, :spo, "new_key01")

      ErlangAdapter.release_snapshot(db, snap)
    end
  end

  describe "concurrent operations" do
    test "multiple concurrent readers and writers", %{db: db} do
      # Start with some data
      for i <- 1..100 do
        ErlangAdapter.put(db, :id2str, "init#{i}", "value#{i}")
      end

      # Writers
      writers =
        for i <- 1..5 do
          Task.async(fn ->
            for j <- 1..20 do
              ErlangAdapter.put(db, :id2str, "writer#{i}_#{j}", "data#{i}_#{j}")
            end

            :ok
          end)
        end

      # Readers
      readers =
        for _ <- 1..10 do
          Task.async(fn ->
            for i <- 1..100 do
              expected = "value#{i}"
              {:ok, ^expected} = ErlangAdapter.get(db, :id2str, "init#{i}")
            end

            :ok
          end)
        end

      # Iterator readers
      iterators =
        for _ <- 1..3 do
          Task.async(fn ->
            {:ok, iter} = ErlangAdapter.prefix_iterator(db, :id2str, "init")
            {:ok, results} = ErlangAdapter.iterator_collect(iter)
            ErlangAdapter.iterator_close(iter)
            assert length(results) == 100
            :ok
          end)
        end

      # Wait for all
      assert Enum.all?(Task.await_many(writers, 10_000), &(&1 == :ok))
      assert Enum.all?(Task.await_many(readers, 10_000), &(&1 == :ok))
      assert Enum.all?(Task.await_many(iterators, 10_000), &(&1 == :ok))
    end

    test "snapshot provides isolation during concurrent writes", %{db: db} do
      # Initial data
      for i <- 1..50 do
        ErlangAdapter.put(db, :spo, "key#{i}", "original#{i}")
      end

      {:ok, snap} = ErlangAdapter.snapshot(db)

      # Concurrent writers modifying original data
      writers =
        for i <- 1..50 do
          Task.async(fn ->
            ErlangAdapter.put(db, :spo, "key#{i}", "modified#{i}")
            :ok
          end)
        end

      # Concurrent snapshot readers
      readers =
        for i <- 1..50 do
          Task.async(fn ->
            expected = "original#{i}"
            {:ok, ^expected} = ErlangAdapter.snapshot_get(db, snap, :spo, "key#{i}")
            :ok
          end)
        end

      Task.await_many(writers, 5000)
      assert Enum.all?(Task.await_many(readers, 5000), &(&1 == :ok))

      ErlangAdapter.release_snapshot(db, snap)
    end
  end

  describe "error handling integration" do
    test "invalid column family errors are consistent across operations", %{db: db} do
      invalid_cf = :nonexistent

      assert {:error, {:invalid_cf, ^invalid_cf}} = ErlangAdapter.get(db, invalid_cf, "key")
      assert {:error, {:invalid_cf, ^invalid_cf}} = ErlangAdapter.put(db, invalid_cf, "key", "value")
      assert {:error, {:invalid_cf, ^invalid_cf}} = ErlangAdapter.delete(db, invalid_cf, "key")
      assert {:error, {:invalid_cf, ^invalid_cf}} = ErlangAdapter.exists(db, invalid_cf, "key")
      assert {:error, {:invalid_cf, ^invalid_cf}} = ErlangAdapter.prefix_iterator(db, invalid_cf, "")

      {:ok, snap} = ErlangAdapter.snapshot(db)
      assert {:error, {:invalid_cf, ^invalid_cf}} = ErlangAdapter.snapshot_get(db, snap, invalid_cf, "key")

      assert {:error, {:invalid_cf, ^invalid_cf}} =
               ErlangAdapter.snapshot_prefix_iterator(db, snap, invalid_cf, "")

      ErlangAdapter.release_snapshot(db, snap)
    end

    test "batch operations validate all entries before writing", %{db: db} do
      # Put some data first
      ErlangAdapter.put(db, :spo, "existing", "value")

      # Batch with invalid CF should fail without writing anything
      operations = [
        {:spo, "key1", "value1"},
        {:nonexistent, "key2", "value2"}
      ]

      assert {:error, {:invalid_cf, :nonexistent}} = ErlangAdapter.write_batch(db, operations, true)

      # key1 should NOT have been written
      assert :not_found = ErlangAdapter.get(db, :spo, "key1")
      # existing data unchanged
      assert {:ok, "value"} = ErlangAdapter.get(db, :spo, "existing")
    end
  end

  describe "resource cleanup" do
    test "iterator resources are properly released", %{db: db} do
      for i <- 1..10 do
        ErlangAdapter.put(db, :spo, "key#{i}", "value#{i}")
      end

      # Create many iterators and close them
      for _ <- 1..100 do
        {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "key")
        ErlangAdapter.iterator_next(iter)
        ErlangAdapter.iterator_close(iter)
      end

      # Should still be able to create more
      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "key")
      {:ok, results} = ErlangAdapter.iterator_collect(iter)
      assert length(results) == 10
      ErlangAdapter.iterator_close(iter)
    end

    test "snapshot resources are properly released", %{db: db} do
      ErlangAdapter.put(db, :spo, "key", "value")

      # Create many snapshots and release them
      for _ <- 1..100 do
        {:ok, snap} = ErlangAdapter.snapshot(db)
        ErlangAdapter.snapshot_get(db, snap, :spo, "key")
        ErlangAdapter.release_snapshot(db, snap)
      end

      # Should still be able to create more
      {:ok, snap} = ErlangAdapter.snapshot(db)
      assert {:ok, "value"} = ErlangAdapter.snapshot_get(db, snap, :spo, "key")
      ErlangAdapter.release_snapshot(db, snap)
    end
  end

  describe "triple store patterns" do
    test "simulates triple index update (add triple)", %{db: db} do
      # Simulate adding triple (s1, p1, o1) to all three indices
      s = "subject1"
      p = "predicate1"
      o = "object1"

      spo_key = "#{s}|#{p}|#{o}"
      pos_key = "#{p}|#{o}|#{s}"
      osp_key = "#{o}|#{s}|#{p}"

      operations = [
        {:spo, spo_key, ""},
        {:pos, pos_key, ""},
        {:osp, osp_key, ""}
      ]

      assert :ok = ErlangAdapter.write_batch(db, operations, true)

      # Verify all indices
      assert {:ok, ""} = ErlangAdapter.get(db, :spo, spo_key)
      assert {:ok, ""} = ErlangAdapter.get(db, :pos, pos_key)
      assert {:ok, ""} = ErlangAdapter.get(db, :osp, osp_key)
    end

    test "simulates dictionary encode/lookup", %{db: db} do
      # Simulate encoding a URI to an ID and storing both directions
      uri = "http://example.org/entity/1"
      id = <<0, 0, 0, 0, 0, 0, 0, 1>>

      operations = [
        {:str2id, uri, id},
        {:id2str, id, uri}
      ]

      assert :ok = ErlangAdapter.write_batch(db, operations, true)

      # Forward lookup (string to ID)
      assert {:ok, ^id} = ErlangAdapter.get(db, :str2id, uri)

      # Reverse lookup (ID to string)
      assert {:ok, ^uri} = ErlangAdapter.get(db, :id2str, id)
    end

    test "simulates query with prefix scan", %{db: db} do
      # Add triples for subject "s1"
      triples = [
        {"s1|p1|o1", ""},
        {"s1|p1|o2", ""},
        {"s1|p2|o3", ""},
        {"s2|p1|o1", ""}
      ]

      operations = Enum.map(triples, fn {k, v} -> {:spo, k, v} end)
      ErlangAdapter.write_batch(db, operations, true)

      # Query: find all triples with subject "s1"
      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "s1|")
      {:ok, results} = ErlangAdapter.iterator_collect(iter)
      ErlangAdapter.iterator_close(iter)

      keys = Enum.map(results, fn {k, _v} -> k end)
      assert length(keys) == 3
      assert "s1|p1|o1" in keys
      assert "s1|p1|o2" in keys
      assert "s1|p2|o3" in keys
      refute "s2|p1|o1" in keys
    end
  end
end
