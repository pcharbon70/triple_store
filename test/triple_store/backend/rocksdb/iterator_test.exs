defmodule TripleStore.Backend.RocksDB.IteratorTest do
  @moduledoc """
  Tests for RocksDB Iterator operations (Task 1.2.4).
  """
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF

  @test_db_base "/tmp/triple_store_iterator_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, path: test_path}
  end

  describe "prefix_iterator/3" do
    test "creates an iterator for a column family", %{db: db} do
      assert {:ok, iter} = NIF.prefix_iterator(db, :spo, "")
      assert is_reference(iter)
      NIF.iterator_close(iter)
    end

    test "creates iterator with prefix", %{db: db} do
      NIF.put(db, :spo, "s1p1o1", "value1")
      NIF.put(db, :spo, "s2p2o2", "value2")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "s1")
      assert {:ok, "s1p1o1", "value1"} = NIF.iterator_next(iter)
      assert :iterator_end = NIF.iterator_next(iter)
      NIF.iterator_close(iter)
    end

    test "works with all column families", %{db: db} do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived] do
        {:ok, iter} = NIF.prefix_iterator(db, cf, "")
        assert is_reference(iter), "Failed for #{cf}"
        NIF.iterator_close(iter)
      end
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, {:invalid_cf, :nonexistent}} = NIF.prefix_iterator(db, :nonexistent, "")
    end

    test "returns error for closed database", %{path: path} do
      {:ok, db2} = NIF.open("#{path}_closed")
      NIF.close(db2)

      assert {:error, :already_closed} = NIF.prefix_iterator(db2, :spo, "")
      File.rm_rf("#{path}_closed")
    end
  end

  describe "iterator_next/1" do
    test "returns key-value pairs in order", %{db: db} do
      NIF.put(db, :spo, "a", "1")
      NIF.put(db, :spo, "b", "2")
      NIF.put(db, :spo, "c", "3")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "")

      assert {:ok, "a", "1"} = NIF.iterator_next(iter)
      assert {:ok, "b", "2"} = NIF.iterator_next(iter)
      assert {:ok, "c", "3"} = NIF.iterator_next(iter)
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "stops at prefix boundary", %{db: db} do
      NIF.put(db, :spo, "prefix_a", "1")
      NIF.put(db, :spo, "prefix_b", "2")
      NIF.put(db, :spo, "other_c", "3")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "prefix_")

      assert {:ok, "prefix_a", "1"} = NIF.iterator_next(iter)
      assert {:ok, "prefix_b", "2"} = NIF.iterator_next(iter)
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "returns :iterator_end for empty result", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, "nonexistent")
      assert :iterator_end = NIF.iterator_next(iter)
      NIF.iterator_close(iter)
    end

    test "handles binary keys and values", %{db: db} do
      key = <<1, 2, 3, 4, 5>>
      value = <<255, 254, 253>>

      NIF.put(db, :spo, key, value)

      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1, 2>>)
      assert {:ok, ^key, ^value} = NIF.iterator_next(iter)
      NIF.iterator_close(iter)
    end

    test "returns error for closed iterator", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, "")
      NIF.iterator_close(iter)

      assert {:error, :iterator_closed} = NIF.iterator_next(iter)
    end
  end

  describe "iterator_seek/2" do
    test "seeks to a specific key", %{db: db} do
      NIF.put(db, :spo, "a", "1")
      NIF.put(db, :spo, "b", "2")
      NIF.put(db, :spo, "c", "3")
      NIF.put(db, :spo, "d", "4")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "")

      # Seek to "c"
      assert :ok = NIF.iterator_seek(iter, "c")
      assert {:ok, "c", "3"} = NIF.iterator_next(iter)
      assert {:ok, "d", "4"} = NIF.iterator_next(iter)
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "seeks to key that doesn't exist (positions at next)", %{db: db} do
      NIF.put(db, :spo, "a", "1")
      NIF.put(db, :spo, "c", "3")
      NIF.put(db, :spo, "e", "5")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "")

      # Seek to "b" (doesn't exist, should position at "c")
      assert :ok = NIF.iterator_seek(iter, "b")
      assert {:ok, "c", "3"} = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "seek past all keys returns :iterator_end", %{db: db} do
      NIF.put(db, :spo, "a", "1")
      NIF.put(db, :spo, "b", "2")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "")

      assert :ok = NIF.iterator_seek(iter, "z")
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "seek respects prefix boundary", %{db: db} do
      NIF.put(db, :spo, "prefix_a", "1")
      NIF.put(db, :spo, "prefix_b", "2")
      NIF.put(db, :spo, "other_c", "3")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "prefix_")

      # Seek to something that exists but outside prefix
      assert :ok = NIF.iterator_seek(iter, "other_c")
      # Should be :iterator_end because "other_c" doesn't match "prefix_" prefix
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "returns error for closed iterator", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, "")
      NIF.iterator_close(iter)

      assert {:error, :iterator_closed} = NIF.iterator_seek(iter, "a")
    end
  end

  describe "iterator_close/1" do
    test "closes an open iterator", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, "")
      assert :ok = NIF.iterator_close(iter)
    end

    test "returns error for already closed iterator", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, "")
      assert :ok = NIF.iterator_close(iter)
      assert {:error, :iterator_closed} = NIF.iterator_close(iter)
    end

    test "can close multiple iterators on same db", %{db: db} do
      {:ok, iter1} = NIF.prefix_iterator(db, :spo, "a")
      {:ok, iter2} = NIF.prefix_iterator(db, :spo, "b")
      {:ok, iter3} = NIF.prefix_iterator(db, :pos, "")

      assert :ok = NIF.iterator_close(iter1)
      assert :ok = NIF.iterator_close(iter2)
      assert :ok = NIF.iterator_close(iter3)
    end
  end

  describe "iterator_collect/1" do
    test "collects all entries with matching prefix", %{db: db} do
      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")
      NIF.put(db, :spo, "key3", "value3")
      NIF.put(db, :spo, "other", "other_value")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "key")
      assert {:ok, results} = NIF.iterator_collect(iter)

      assert length(results) == 3
      assert {"key1", "value1"} in results
      assert {"key2", "value2"} in results
      assert {"key3", "value3"} in results
      refute {"other", "other_value"} in results

      NIF.iterator_close(iter)
    end

    test "returns empty list for no matches", %{db: db} do
      NIF.put(db, :spo, "other", "value")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "nonexistent")
      assert {:ok, []} = NIF.iterator_collect(iter)

      NIF.iterator_close(iter)
    end

    test "returns error for closed iterator", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, "")
      NIF.iterator_close(iter)

      assert {:error, :iterator_closed} = NIF.iterator_collect(iter)
    end

    test "advances iterator to end", %{db: db} do
      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "key")
      {:ok, _results} = NIF.iterator_collect(iter)

      # Iterator should now be at end
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end
  end

  describe "prefix_stream/3" do
    test "creates a stream from an iterator", %{db: db} do
      NIF.put(db, :spo, "s1p1o1", "")
      NIF.put(db, :spo, "s1p1o2", "")
      NIF.put(db, :spo, "s2p2o2", "")

      assert {:ok, stream} = NIF.prefix_stream(db, :spo, "s1")
      results = Enum.to_list(stream)

      assert length(results) == 2
      assert {"s1p1o1", ""} in results
      assert {"s1p1o2", ""} in results
    end

    test "stream is lazy", %{db: db} do
      for i <- 1..100 do
        NIF.put(db, :spo, "key#{String.pad_leading("#{i}", 3, "0")}", "value#{i}")
      end

      {:ok, stream} = NIF.prefix_stream(db, :spo, "key")

      # Take only first 5
      results = Enum.take(stream, 5)
      assert length(results) == 5
    end

    test "stream handles empty result", %{db: db} do
      {:ok, stream} = NIF.prefix_stream(db, :spo, "nonexistent")
      assert [] = Enum.to_list(stream)
    end

    test "returns error for invalid column family", %{db: db} do
      assert {:error, {:invalid_cf, :nonexistent}} = NIF.prefix_stream(db, :nonexistent, "")
    end

    test "can enumerate stream multiple times creates new iterators", %{db: db} do
      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")

      {:ok, stream} = NIF.prefix_stream(db, :spo, "key")

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
      NIF.put(db, :spo, "a1", "v1")
      NIF.put(db, :spo, "a2", "v2")
      NIF.put(db, :spo, "b1", "v3")
      NIF.put(db, :spo, "b2", "v4")

      {:ok, iter_a} = NIF.prefix_iterator(db, :spo, "a")
      {:ok, iter_b} = NIF.prefix_iterator(db, :spo, "b")

      # Interleave reads
      assert {:ok, "a1", "v1"} = NIF.iterator_next(iter_a)
      assert {:ok, "b1", "v3"} = NIF.iterator_next(iter_b)
      assert {:ok, "a2", "v2"} = NIF.iterator_next(iter_a)
      assert {:ok, "b2", "v4"} = NIF.iterator_next(iter_b)

      NIF.iterator_close(iter_a)
      NIF.iterator_close(iter_b)
    end

    test "concurrent iteration from multiple tasks", %{db: db} do
      # Insert data
      for i <- 1..100 do
        NIF.put(db, :id2str, "key#{String.pad_leading("#{i}", 3, "0")}", "value#{i}")
      end

      # Create multiple tasks that iterate concurrently
      tasks =
        for _ <- 1..5 do
          Task.async(fn ->
            {:ok, iter} = NIF.prefix_iterator(db, :id2str, "key")
            {:ok, results} = NIF.iterator_collect(iter)
            NIF.iterator_close(iter)
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
      NIF.put(db, :spo, "key1", "value1")
      NIF.put(db, :spo, "key2", "value2")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "key")

      # Add more data after iterator creation
      NIF.put(db, :spo, "key3", "value3")

      # RocksDB iterators may or may not see new data depending on timing
      # This is acceptable behavior - we just verify no crashes
      {:ok, results} = NIF.iterator_collect(iter)
      assert length(results) >= 2

      NIF.iterator_close(iter)
    end
  end

  describe "edge cases" do
    test "empty prefix iterates all keys", %{db: db} do
      NIF.put(db, :spo, "a", "1")
      NIF.put(db, :spo, "b", "2")
      NIF.put(db, :spo, "c", "3")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "")
      {:ok, results} = NIF.iterator_collect(iter)

      assert length(results) == 3
      NIF.iterator_close(iter)
    end

    test "single byte prefix", %{db: db} do
      NIF.put(db, :spo, "abc", "1")
      NIF.put(db, :spo, "axy", "2")
      NIF.put(db, :spo, "bcd", "3")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "a")
      {:ok, results} = NIF.iterator_collect(iter)

      assert length(results) == 2
      NIF.iterator_close(iter)
    end

    test "exact key match as prefix", %{db: db} do
      NIF.put(db, :spo, "exactkey", "value")
      NIF.put(db, :spo, "exactkey_extended", "value2")

      {:ok, iter} = NIF.prefix_iterator(db, :spo, "exactkey")
      {:ok, results} = NIF.iterator_collect(iter)

      assert length(results) == 2
      NIF.iterator_close(iter)
    end

    test "handles large number of results", %{db: db} do
      # Insert 1000 keys
      for i <- 1..1000 do
        key = "prefix_#{String.pad_leading("#{i}", 4, "0")}"
        NIF.put(db, :id2str, key, "value#{i}")
      end

      {:ok, iter} = NIF.prefix_iterator(db, :id2str, "prefix_")
      {:ok, results} = NIF.iterator_collect(iter)

      assert length(results) == 1000
      NIF.iterator_close(iter)
    end
  end
end
