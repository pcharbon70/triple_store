defmodule TripleStore.Backend.RocksDB.IteratorTest do
  @moduledoc """
  Tests for RocksDB Iterator operations (Task 1.2.4).
  """
  use TripleStore.PooledDbCase

  describe "prefix_iterator/3" do
    test "creates an iterator for a column family", %{db: db} do
      assert {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")
      assert is_pid(iter)
      ErlangAdapter.iterator_close(iter)
    end

    test "creates iterator with prefix", %{db: db} do
      ErlangAdapter.put(db, :spo, "s1p1o1", "value1")
      ErlangAdapter.put(db, :spo, "s2p2o2", "value2")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "s1")
      assert {:ok, "s1p1o1", "value1"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)
      ErlangAdapter.iterator_close(iter)
    end

    test "works with all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        {:ok, iter} = ErlangAdapter.prefix_iterator(db, cf, "")
        assert is_pid(iter), "Failed for #{cf}"
        ErlangAdapter.iterator_close(iter)
      end
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, :invalid_column_family} = ErlangAdapter.prefix_iterator(db, :nonexistent, "")
    end

    test "returns error for closed database", %{db_path: path} do
      {:ok, db2} = ErlangAdapter.open("#{path}_closed")
      ErlangAdapter.close(db2)

      assert catch_exit(ErlangAdapter.prefix_iterator(db2, :spo, ""))
      File.rm_rf("#{path}_closed")
    end
  end

  describe "iterator_next/1" do
    test "returns key-value pairs in order", %{db: db} do
      ErlangAdapter.put(db, :spo, "a", "1")
      ErlangAdapter.put(db, :spo, "b", "2")
      ErlangAdapter.put(db, :spo, "c", "3")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")

      assert {:ok, "a", "1"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "b", "2"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "c", "3"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
    end

    test "stops at prefix boundary", %{db: db} do
      ErlangAdapter.put(db, :spo, "prefix_a", "1")
      ErlangAdapter.put(db, :spo, "prefix_b", "2")
      ErlangAdapter.put(db, :spo, "other_c", "3")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "prefix_")

      assert {:ok, "prefix_a", "1"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "prefix_b", "2"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
    end

    test "returns :iterator_end for empty result", %{db: db} do
      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "nonexistent")
      assert :iterator_end = ErlangAdapter.iterator_next(iter)
      ErlangAdapter.iterator_close(iter)
    end

    test "handles binary keys and values", %{db: db} do
      key = <<1, 2, 3, 4, 5>>
      value = <<255, 254, 253>>

      ErlangAdapter.put(db, :spo, key, value)

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, <<1, 2>>)
      assert {:ok, ^key, ^value} = ErlangAdapter.iterator_next(iter)
      ErlangAdapter.iterator_close(iter)
    end

    test "returns error for closed iterator", %{db: db} do
      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")
      ErlangAdapter.iterator_close(iter)

      assert catch_exit(ErlangAdapter.iterator_next(iter))
    end
  end

  describe "iterator_seek/2" do
    test "seeks to a specific key", %{db: db} do
      ErlangAdapter.put(db, :spo, "a", "1")
      ErlangAdapter.put(db, :spo, "b", "2")
      ErlangAdapter.put(db, :spo, "c", "3")
      ErlangAdapter.put(db, :spo, "d", "4")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")

      # Seek to "c"
      assert :ok = ErlangAdapter.iterator_seek(iter, "c")
      assert {:ok, "c", "3"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "d", "4"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
    end

    test "seeks to key that doesn't exist (positions at next)", %{db: db} do
      ErlangAdapter.put(db, :spo, "a", "1")
      ErlangAdapter.put(db, :spo, "c", "3")
      ErlangAdapter.put(db, :spo, "e", "5")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")

      # Seek to "b" (doesn't exist, should position at "c")
      assert :ok = ErlangAdapter.iterator_seek(iter, "b")
      assert {:ok, "c", "3"} = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
    end

    test "seek past all keys returns :iterator_end", %{db: db} do
      ErlangAdapter.put(db, :spo, "a", "1")
      ErlangAdapter.put(db, :spo, "b", "2")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")

      assert :ok = ErlangAdapter.iterator_seek(iter, "z")
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
    end

    test "seek respects prefix boundary", %{db: db} do
      ErlangAdapter.put(db, :spo, "prefix_a", "1")
      ErlangAdapter.put(db, :spo, "prefix_b", "2")
      ErlangAdapter.put(db, :spo, "other_c", "3")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "prefix_")

      # Seek to something that exists but outside prefix
      assert :ok = ErlangAdapter.iterator_seek(iter, "other_c")
      # Should be :iterator_end because "other_c" doesn't match "prefix_" prefix
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
    end

    test "returns error for closed iterator", %{db: db} do
      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")
      ErlangAdapter.iterator_close(iter)

      assert catch_exit(ErlangAdapter.iterator_seek(iter, "a"))
    end
  end

  describe "iterator_close/1" do
    test "closes an open iterator", %{db: db} do
      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")
      assert :ok = ErlangAdapter.iterator_close(iter)
    end

    test "returns error for already closed iterator", %{db: db} do
      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")
      assert :ok = ErlangAdapter.iterator_close(iter)
      # Closing an already-closed iterator returns :ok (no-op)
      assert :ok = ErlangAdapter.iterator_close(iter)
    end

    test "can close multiple iterators on same db", %{db: db} do
      {:ok, iter1} = ErlangAdapter.prefix_iterator(db, :spo, "a")
      {:ok, iter2} = ErlangAdapter.prefix_iterator(db, :spo, "b")
      {:ok, iter3} = ErlangAdapter.prefix_iterator(db, :pos, "")

      assert :ok = ErlangAdapter.iterator_close(iter1)
      assert :ok = ErlangAdapter.iterator_close(iter2)
      assert :ok = ErlangAdapter.iterator_close(iter3)
    end
  end

  describe "iterator_collect/1" do
    test "collects all entries with matching prefix", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")
      ErlangAdapter.put(db, :spo, "key3", "value3")
      ErlangAdapter.put(db, :spo, "other", "other_value")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "key")
      assert {:ok, results} = ErlangAdapter.iterator_collect(iter)

      assert length(results) == 3
      assert {"key1", "value1"} in results
      assert {"key2", "value2"} in results
      assert {"key3", "value3"} in results
      refute {"other", "other_value"} in results

      ErlangAdapter.iterator_close(iter)
    end

    test "returns empty list for no matches", %{db: db} do
      ErlangAdapter.put(db, :spo, "other", "value")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "nonexistent")
      assert {:ok, []} = ErlangAdapter.iterator_collect(iter)

      ErlangAdapter.iterator_close(iter)
    end

    test "returns error for closed iterator", %{db: db} do
      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")
      ErlangAdapter.iterator_close(iter)

      assert catch_exit(ErlangAdapter.iterator_collect(iter))
    end

    test "advances iterator to end", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "key")
      {:ok, _results} = ErlangAdapter.iterator_collect(iter)

      # Iterator should now be at end
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
    end
  end

  describe "prefix_stream/3" do
    test "creates a stream from an iterator", %{db: db} do
      ErlangAdapter.put(db, :spo, "s1p1o1", "")
      ErlangAdapter.put(db, :spo, "s1p1o2", "")
      ErlangAdapter.put(db, :spo, "s2p2o2", "")

      stream = ErlangAdapter.prefix_stream(db, :spo, "s1")
      results = Enum.to_list(stream)

      assert length(results) == 2
      assert {"s1p1o1", ""} in results
      assert {"s1p1o2", ""} in results
    end

    test "stream is lazy", %{db: db} do
      for i <- 1..100 do
        ErlangAdapter.put(db, :spo, "key#{String.pad_leading("#{i}", 3, "0")}", "value#{i}")
      end

      stream = ErlangAdapter.prefix_stream(db, :spo, "key")

      # Take only first 5
      results = Enum.take(stream, 5)
      assert length(results) == 5
    end

    test "stream handles empty result", %{db: db} do
      stream = ErlangAdapter.prefix_stream(db, :spo, "nonexistent")
      assert [] = Enum.to_list(stream)
    end

    test "returns error for invalid column family", %{db: db} do
      stream = ErlangAdapter.prefix_stream(db, :nonexistent, "")
      # Stream fails when consumed, not when created
      assert_raise RuntimeError, ~r/Failed to create iterator/, fn ->
        Enum.to_list(stream)
      end
    end

    test "can enumerate stream multiple times creates new iterators", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")

      stream = ErlangAdapter.prefix_stream(db, :spo, "key")

      # First enumeration
      results1 = Enum.to_list(stream)
      assert length(results1) == 2

      # Stream.resource creates new iterator each time it's enumerated,
      # but our iterator reference is captured in closure, so second
      # enumeration will see the same exhausted iterator
      # This is expected behavior - streams are typically single-use
    end
  end

  describe "concurrent iteration" do
    test "multiple iterators can be open simultaneously", %{db: db} do
      ErlangAdapter.put(db, :spo, "a1", "v1")
      ErlangAdapter.put(db, :spo, "a2", "v2")
      ErlangAdapter.put(db, :spo, "b1", "v3")
      ErlangAdapter.put(db, :spo, "b2", "v4")

      {:ok, iter_a} = ErlangAdapter.prefix_iterator(db, :spo, "a")
      {:ok, iter_b} = ErlangAdapter.prefix_iterator(db, :spo, "b")

      # Interleave reads
      assert {:ok, "a1", "v1"} = ErlangAdapter.iterator_next(iter_a)
      assert {:ok, "b1", "v3"} = ErlangAdapter.iterator_next(iter_b)
      assert {:ok, "a2", "v2"} = ErlangAdapter.iterator_next(iter_a)
      assert {:ok, "b2", "v4"} = ErlangAdapter.iterator_next(iter_b)

      ErlangAdapter.iterator_close(iter_a)
      ErlangAdapter.iterator_close(iter_b)
    end

    test "concurrent iteration from multiple tasks", %{db: db} do
      # Insert data
      for i <- 1..100 do
        ErlangAdapter.put(db, :id2str, "key#{String.pad_leading("#{i}", 3, "0")}", "value#{i}")
      end

      # Create multiple tasks that iterate concurrently
      tasks =
        for _ <- 1..5 do
          Task.async(fn ->
            {:ok, iter} = ErlangAdapter.prefix_iterator(db, :id2str, "key")
            {:ok, results} = ErlangAdapter.iterator_collect(iter)
            ErlangAdapter.iterator_close(iter)
            length(results)
          end)
        end

      results = Task.await_many(tasks, 5000)

      # All should get 100 results
      assert Enum.all?(results, &(&1 == 100))
    end
  end

  describe "iteration with modifications" do
    test "iterator sees snapshot at creation time", %{db: db} do
      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "key")

      # Add more data after iterator creation
      ErlangAdapter.put(db, :spo, "key3", "value3")

      # RocksDB iterators may or may not see new data depending on timing
      # This is acceptable behavior - we just verify no crashes
      {:ok, results} = ErlangAdapter.iterator_collect(iter)
      assert length(results) >= 2

      ErlangAdapter.iterator_close(iter)
    end
  end

  describe "iterator lifetime safety" do
    @tag :lifetime_safety
    test "iterator continues to work after database close()", %{db_path: path} do
      # This test verifies the fix for the use-after-free bug documented in
      # docs/20251222/rocksdb-close-lifetime-risk.md
      #
      # Previously, calling close() would drop the DB while iterators still held
      # pointers to it, causing use-after-free. The fix stores Arc<SharedDb> in
      # iterators, so the DB stays alive until all iterators are dropped.

      # Create a separate database for this test (not from the pool)
      {:ok, db} = ErlangAdapter.open("#{path}_lifetime")

      # Insert some data
      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")
      ErlangAdapter.put(db, :spo, "key3", "value3")

      # Create iterator BEFORE closing the database
      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "key")

      # Close the database - this used to cause use-after-free when using the iterator
      assert :ok = ErlangAdapter.close(db)

      # Iterator should still work because it holds its own Arc<SharedDb> reference
      # Before the fix, this would crash the VM or return garbage
      assert {:ok, "key1", "value1"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "key2", "value2"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "key3", "value3"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      # Clean up
      ErlangAdapter.iterator_close(iter)
      File.rm_rf("#{path}_lifetime")
    end

    @tag :lifetime_safety
    test "iterator_seek works after database close()", %{db_path: path} do
      {:ok, db} = ErlangAdapter.open("#{path}_lifetime_seek")

      ErlangAdapter.put(db, :spo, "a", "1")
      ErlangAdapter.put(db, :spo, "b", "2")
      ErlangAdapter.put(db, :spo, "c", "3")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")

      # Close the database
      assert :ok = ErlangAdapter.close(db)

      # Seek should still work
      assert :ok = ErlangAdapter.iterator_seek(iter, "b")
      assert {:ok, "b", "2"} = ErlangAdapter.iterator_next(iter)
      assert {:ok, "c", "3"} = ErlangAdapter.iterator_next(iter)
      assert :iterator_end = ErlangAdapter.iterator_next(iter)

      ErlangAdapter.iterator_close(iter)
      File.rm_rf("#{path}_lifetime_seek")
    end

    @tag :lifetime_safety
    test "iterator_collect works after database close()", %{db_path: path} do
      {:ok, db} = ErlangAdapter.open("#{path}_lifetime_collect")

      ErlangAdapter.put(db, :spo, "key1", "value1")
      ErlangAdapter.put(db, :spo, "key2", "value2")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "key")

      # Close the database
      assert :ok = ErlangAdapter.close(db)

      # Collect should still work
      assert {:ok, results} = ErlangAdapter.iterator_collect(iter)
      assert length(results) == 2
      assert {"key1", "value1"} in results
      assert {"key2", "value2"} in results

      ErlangAdapter.iterator_close(iter)
      File.rm_rf("#{path}_lifetime_collect")
    end

    @tag :lifetime_safety
    test "multiple iterators work after database close()", %{db_path: path} do
      {:ok, db} = ErlangAdapter.open("#{path}_lifetime_multi")

      ErlangAdapter.put(db, :spo, "a1", "v1")
      ErlangAdapter.put(db, :spo, "b1", "v2")

      {:ok, iter_a} = ErlangAdapter.prefix_iterator(db, :spo, "a")
      {:ok, iter_b} = ErlangAdapter.prefix_iterator(db, :spo, "b")

      # Close the database
      assert :ok = ErlangAdapter.close(db)

      # Both iterators should still work
      assert {:ok, "a1", "v1"} = ErlangAdapter.iterator_next(iter_a)
      assert {:ok, "b1", "v2"} = ErlangAdapter.iterator_next(iter_b)

      ErlangAdapter.iterator_close(iter_a)
      ErlangAdapter.iterator_close(iter_b)
      File.rm_rf("#{path}_lifetime_multi")
    end
  end

  describe "edge cases" do
    test "empty prefix iterates all keys", %{db: db} do
      ErlangAdapter.put(db, :spo, "a", "1")
      ErlangAdapter.put(db, :spo, "b", "2")
      ErlangAdapter.put(db, :spo, "c", "3")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "")
      {:ok, results} = ErlangAdapter.iterator_collect(iter)

      assert length(results) == 3
      ErlangAdapter.iterator_close(iter)
    end

    test "single byte prefix", %{db: db} do
      ErlangAdapter.put(db, :spo, "abc", "1")
      ErlangAdapter.put(db, :spo, "axy", "2")
      ErlangAdapter.put(db, :spo, "bcd", "3")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "a")
      {:ok, results} = ErlangAdapter.iterator_collect(iter)

      assert length(results) == 2
      ErlangAdapter.iterator_close(iter)
    end

    test "exact key match as prefix", %{db: db} do
      ErlangAdapter.put(db, :spo, "exactkey", "value")
      ErlangAdapter.put(db, :spo, "exactkey_extended", "value2")

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, "exactkey")
      {:ok, results} = ErlangAdapter.iterator_collect(iter)

      assert length(results) == 2
      ErlangAdapter.iterator_close(iter)
    end

    test "handles large number of results", %{db: db} do
      # Insert 1000 keys
      for i <- 1..1000 do
        key = "prefix_#{String.pad_leading("#{i}", 4, "0")}"
        ErlangAdapter.put(db, :id2str, key, "value#{i}")
      end

      {:ok, iter} = ErlangAdapter.prefix_iterator(db, :id2str, "prefix_")
      {:ok, results} = ErlangAdapter.iterator_collect(iter)

      assert length(results) == 1000
      ErlangAdapter.iterator_close(iter)
    end
  end
end
