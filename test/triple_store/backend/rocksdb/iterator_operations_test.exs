defmodule TripleStore.Backend.RocksDB.IteratorOperationsTest do
  @moduledoc """
  Unit tests for Section 2.1: Iterator Operations Migration.

  These tests verify that iterator operations work correctly with erlang-rocksdb.
  """

  use ExUnit.Case, async: false
  alias TripleStore.Backend.RocksDB.NIF

  @moduletag :section_2_1

  # ===========================================================================
  # Setup and Teardown
  # ===========================================================================

  setup do
    # Generate a unique test database path
    unique_id = System.unique_integer([:positive, :monotonic])
    db_path = Path.join([System.tmp_dir!(), "triple_store_test_#{unique_id}"])

    # Ensure clean directory
    File.rm_rf(db_path)
    File.mkdir_p!(db_path)

    # Open the database
    {:ok, db} = NIF.open(db_path)

    # Add some test data to SPO column family
    test_data = [
      {<<1::64-big, 1::64-big, 1::64-big>>, <<>>},
      {<<1::64-big, 1::64-big, 2::64-big>>, <<>>},
      {<<1::64-big, 1::64-big, 3::64-big>>, <<>>},
      {<<1::64-big, 2::64-big, 1::64-big>>, <<>>},
      {<<1::64-big, 2::64-big, 2::64-big>>, <<>>},
      {<<2::64-big, 1::64-big, 1::64-big>>, <<>>},
      {<<2::64-big, 1::64-big, 2::64-big>>, <<>>},
      {<<3::64-big, 1::64-big, 1::64-big>>, <<>>}
    ]

    Enum.each(test_data, fn {key, value} ->
      :ok = NIF.put(db, :spo, key, value)
    end)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf(db_path)
    end)

    {:ok, %{db: db, db_path: db_path}}
  end

  # ===========================================================================
  # 2.1.1 Basic Iterator Creation Tests
  # ===========================================================================

  describe "2.1.1 Basic Iterator Creation" do
    test "2.1.1.1 creates iterator for column family", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)
      assert is_pid(iter)
      NIF.iterator_close(iter)
    end

    test "2.1.1.2 supports fill_cache option", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>, fill_cache: false)
      assert is_pid(iter)
      NIF.iterator_close(iter)
    end

    test "2.1.1.3 handles iterator resource lifecycle", %{db: db} do
      # Create iterator
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)
      assert Process.alive?(iter)

      # Close iterator
      NIF.iterator_close(iter)
      # Give process time to shut down
      Process.sleep(10)
      refute Process.alive?(iter)
    end

    test "2.1.1.4 supports iterator_move operations", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      # Test first
      assert {:ok, <<1::64-big, 1::64-big, 1::64-big>>, <<>>} = NIF.iterator_move(iter, :first)

      # Test next
      assert {:ok, <<1::64-big, 1::64-big, 2::64-big>>, <<>>} = NIF.iterator_move(iter, :next)

      # Test prev
      assert {:ok, <<1::64-big, 1::64-big, 1::64-big>>, <<>>} = NIF.iterator_move(iter, :prev)

      # Test last
      assert {:ok, <<3::64-big, 1::64-big, 1::64-big>>, <<>>} = NIF.iterator_move(iter, :last)

      NIF.iterator_close(iter)
    end

    test "2.1.1.5 implements iterator_close", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)
      assert :ok = NIF.iterator_close(iter)
    end
  end

  # ===========================================================================
  # 2.1.2 Prefix Iterator Tests
  # ===========================================================================

  describe "2.1.2 Prefix Iterator Migration" do
    test "2.1.2.1 prefix_iterator with prefix bounds checking", %{db: db} do
      # Create iterator for subject=1 prefix
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64-big>>)

      # All results should have subject=1
      {:ok, key, _value} = NIF.iterator_move(iter, <<1::64-big>>)
      assert <<1::64-big, _::binary>> = key

      # Collect all entries
      {:ok, entries} = NIF.iterator_collect(iter)

      # All entries should have subject=1
      assert Enum.all?(entries, fn {k, _v} ->
               <<subject::64-big, _::binary>> = k
               subject == 1
             end)

      NIF.iterator_close(iter)
    end

    test "2.1.2.2 respects total_order_seek option", %{db: db} do
      # With total_order_seek, can seek anywhere
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>, total_order_seek: true)

      # Seek to middle of data
      assert {:ok, _key, _value} = NIF.iterator_move(iter, <<2::64-big>>)

      NIF.iterator_close(iter)
    end

    test "2.1.2.3 handles short prefix (< 8 bytes)", %{db: db} do
      # Empty prefix should iterate all data
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      {:ok, entries} = NIF.iterator_collect(iter)
      assert length(entries) == 8

      NIF.iterator_close(iter)
    end

    test "2.1.2.4 handles long prefix (16+ bytes)", %{db: db} do
      # Subject-predicate prefix (16 bytes)
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64-big, 1::64-big>>)

      {:ok, entries} = NIF.iterator_collect(iter)
      # Should have 3 entries with subject=1, predicate=1
      assert length(entries) == 3

      NIF.iterator_close(iter)
    end

    test "2.1.2.5 maintains prefix boundary checking", %{db: db} do
      # Create iterator for subject=1 prefix
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64-big>>)

      # Seek to subject=2 (outside prefix)
      NIF.iterator_seek(iter, <<2::64-big>>)

      # Next should return exhausted since we're past prefix boundary
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end
  end

  # ===========================================================================
  # 2.1.3 Seek Operations Tests
  # ===========================================================================

  describe "2.1.3 Seek Operations Migration" do
    test "2.1.3.1 implements iterator_seek", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      # Seek to specific key
      assert :ok = NIF.iterator_seek(iter, <<1::64-big, 2::64-big, 1::64-big>>)

      # Next should return the key at or after seek position
      assert {:ok, <<1::64-big, 2::64-big, _::binary>>, _} = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "2.1.3.2 handles seek with non-existent keys", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      # Seek to non-existent key (1, 2, 3 doesn't exist, we have 1, 2, 1 and 1, 2, 2)
      # Actually let me use a clearly non-existent key
      assert :ok = NIF.iterator_seek(iter, <<1::64-big, 3::64-big, 0::64-big>>)

      # Should position at next available key
      assert {:ok, <<2::64-big, _::binary>>, _} = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "2.1.3.3 handles seek past end of data", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      # Seek past all data
      assert :ok = NIF.iterator_seek(iter, <<255::64-big>>)

      # Next should return exhausted
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "2.1.3.4 handles seek to first key", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      # Seek to first key
      assert :ok = NIF.iterator_seek(iter, <<0::64-big>>)

      # Should get first entry
      assert {:ok, <<1::64-big, _::binary>>, _} = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "2.1.3.5 validates Leapfrog operations", %{db: db} do
      # Simulate Leapfrog seek pattern: seek to increasing values
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      # Seek to subject=1
      assert :ok = NIF.iterator_seek(iter, <<1::64-big>>)
      assert {:ok, <<1::64-big, _::binary>>, _} = NIF.iterator_next(iter)

      # Seek to subject=2
      assert :ok = NIF.iterator_seek(iter, <<2::64-big>>)
      assert {:ok, <<2::64-big, _::binary>>, _} = NIF.iterator_next(iter)

      # Seek to subject=3
      assert :ok = NIF.iterator_seek(iter, <<3::64-big>>)
      assert {:ok, <<3::64-big, _::binary>>, _} = NIF.iterator_next(iter)

      # Seek past end
      assert :ok = NIF.iterator_seek(iter, <<4::64-big>>)
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end
  end

  # ===========================================================================
  # 2.1.4 Iterator Collect Tests
  # ===========================================================================

  describe "2.1.4 Iterator Collect Operation" do
    test "2.1.4.1 implements iterator_collect", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      {:ok, entries} = NIF.iterator_collect(iter)
      assert is_list(entries)
      assert length(entries) == 8

      NIF.iterator_close(iter)
    end

    test "2.1.4.2 handles prefix boundary in collect", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64-big>>)

      {:ok, entries} = NIF.iterator_collect(iter)
      # All entries with subject=1
      assert length(entries) == 5

      NIF.iterator_close(iter)
    end

    test "2.1.4.3 returns results as list of tuples", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      {:ok, entries} = NIF.iterator_collect(iter)

      # Verify format
      assert Enum.all?(entries, fn
               {k, v} when is_binary(k) and is_binary(v) -> true
               _ -> false
             end)

      NIF.iterator_close(iter)
    end

    test "2.1.4.4 handles exhausted iterator", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<255::64-big>>)

      # Iterator is at a position with no data
      {:ok, entries} = NIF.iterator_collect(iter)
      assert entries == []

      NIF.iterator_close(iter)
    end
  end

  # ===========================================================================
  # 2.1.5 Additional Tests
  # ===========================================================================

  describe "2.1.5 Additional Tests" do
    test "2.1.5.1 iterator_next returns entries sequentially", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      # Get first entry
      {:ok, key1, _} = NIF.iterator_next(iter)

      # Get second entry
      {:ok, key2, _} = NIF.iterator_next(iter)

      # Keys should be in sorted order
      assert key1 < key2

      NIF.iterator_close(iter)
    end

    test "2.1.5.2 iterator_move with binary seek key", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      # Seek using iterator_move with binary key
      {:ok, _key, _value} = NIF.iterator_move(iter, <<2::64-big>>)

      # Verify we're at expected position
      {:ok, <<2::64-big, _::binary>>, _} = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "2.1.5.3 multiple iterators can coexist", %{db: db} do
      {:ok, iter1} = NIF.prefix_iterator(db, :spo, <<1::64-big>>)
      {:ok, iter2} = NIF.prefix_iterator(db, :spo, <<2::64-big>>)

      # Both should be alive
      assert Process.alive?(iter1)
      assert Process.alive?(iter2)

      # iter1 should only see subject=1 entries
      {:ok, entries1} = NIF.iterator_collect(iter1)

      assert Enum.all?(entries1, fn {k, _} ->
               <<s::64-big, _::binary>> = k
               s == 1
             end)

      # iter2 should only see subject=2 entries
      {:ok, entries2} = NIF.iterator_collect(iter2)

      assert Enum.all?(entries2, fn {k, _} ->
               <<s::64-big, _::binary>> = k
               s == 2
             end)

      NIF.iterator_close(iter1)
      NIF.iterator_close(iter2)
    end

    test "2.1.5.4 iterator handles empty column family", %{db: db} do
      # id2str should be empty
      {:ok, iter} = NIF.prefix_iterator(db, :id2str, <<>>)

      {:ok, entries} = NIF.iterator_collect(iter)
      assert entries == []

      NIF.iterator_close(iter)
    end

    test "2.1.5.5 iterator with single entry", %{db: db} do
      # Add a single entry to str2id
      :ok = NIF.put(db, :str2id, <<42::64-big>>, <<"test">>)

      {:ok, iter} = NIF.prefix_iterator(db, :str2id, <<>>)

      {:ok, entries} = NIF.iterator_collect(iter)
      assert length(entries) == 1

      NIF.iterator_close(iter)
    end

    test "2.1.5.6 iterator maintains position across operations", %{db: db} do
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<>>)

      # Move to first
      {:ok, k1, _} = NIF.iterator_move(iter, :first)

      # Move to next
      {:ok, k2, _} = NIF.iterator_move(iter, :next)

      # Move back to prev
      {:ok, k1_again, _} = NIF.iterator_move(iter, :prev)

      # Should be back at first position
      assert k1 == k1_again
      assert k1 < k2

      NIF.iterator_close(iter)
    end
  end

  # ===========================================================================
  # Integration with Index Operations
  # ===========================================================================

  describe "Index Integration" do
    test "prefix scan matches Index.spo_prefix behavior", %{db: db} do
      alias TripleStore.Index

      # Create prefix for subject=1
      prefix = Index.spo_prefix(1)

      {:ok, iter} = NIF.prefix_iterator(db, :spo, prefix)

      {:ok, entries} = NIF.iterator_collect(iter)

      # All entries should have subject=1
      assert Enum.all?(entries, fn {k, _} ->
               <<1::64-big, _::binary>> = k
               true
             end)

      # Should have 5 entries (3 with predicate=1, 2 with predicate=2)
      assert length(entries) == 5

      NIF.iterator_close(iter)
    end
  end
end
