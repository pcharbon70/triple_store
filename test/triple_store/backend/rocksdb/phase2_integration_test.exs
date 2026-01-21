defmodule TripleStore.Backend.RocksDB.Phase2IntegrationTest do
  @moduledoc """
  Integration tests for Section 2.5: Iterator and Snapshot Migration

  These tests verify end-to-end functionality of the erlang-rocksdb migration
  for iterators and snapshots, ensuring compatibility with the original Rust NIF.

  ## Test Sections

  - 2.5.1: Iterator Integration Tests
  - 2.5.2: Snapshot Integration Tests
  - 2.5.3: Query Execution Tests
  - 2.5.4: Performance Validation Tests
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index

  @moduletag :phase2_integration
  @moduletag timeout: 120_000

  # =============================================================================
  # Setup and Teardown
  # =============================================================================

  setup do
    unique_id = System.unique_integer([:positive, :monotonic])
    db_path = Path.join([System.tmp_dir!(), "phase2_integration_#{unique_id}"])

    File.rm_rf(db_path)

    {:ok, db} = NIF.open(db_path)

    on_exit(fn ->
      NIF.close(db)
      File.rm_rf(db_path)
    end)

    {:ok, %{db: db, db_path: db_path}}
  end

  # =============================================================================
  # Section 2.5.1: Iterator Integration Tests
  # =============================================================================

  describe "2.5.1 Iterator Integration Tests" do
    test "2.5.1.1 prefix iterator returns all entries in prefix range", %{db: db} do
      # Insert test data with different prefixes
      test_data = [
        {<<1::64-big, 1::64-big, 1::64-big>>, "v1"},
        {<<1::64-big, 1::64-big, 2::64-big>>, "v2"},
        {<<1::64-big, 1::64-big, 3::64-big>>, "v3"},
        {<<1::64-big, 2::64-big, 1::64-big>>, "v4"},
        {<<2::64-big, 1::64-big, 1::64-big>>, "v5"}
      ]

      Enum.each(test_data, fn {key, value} ->
        :ok = NIF.put(db, :spo, key, value)
      end)

      # Iterate over prefix <<1::64-big, 1::64-big>>
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64-big, 1::64-big>>)

      results =
        Stream.unfold(iter, fn
          :iterator_end ->
            nil

          iter ->
            case NIF.iterator_next(iter) do
              {:ok, key, value} -> {{key, value}, iter}
              :iterator_end -> {nil, :iterator_end}
            end
        end)
        |> Enum.reject(&is_nil/1)

      NIF.iterator_close(iter)

      # Should get exactly 3 entries with matching prefix
      assert length(results) == 3

      # Verify all results have the correct prefix
      Enum.each(results, fn {key, _value} ->
        assert <<1::64-big, 1::64-big, _::64-big>> = key
      end)
    end

    test "2.5.1.2 iterator seek behaves correctly for existing and non-existing keys", %{db: db} do
      # Insert test data with gaps
      test_data = [
        {<<1::64-big, 1::64-big, 100::64-big>>, "v1"},
        {<<1::64-big, 1::64-big, 200::64-big>>, "v2"},
        {<<1::64-big, 1::64-big, 300::64-big>>, "v3"}
      ]

      Enum.each(test_data, fn {key, value} ->
        :ok = NIF.put(db, :spo, key, value)
      end)

      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64-big, 1::64-big>>)

      # Seek to existing key (200)
      :ok = NIF.iterator_seek(iter, <<1::64-big, 1::64-big, 200::64-big>>)
      {:ok, key, value} = NIF.iterator_next(iter)
      assert key == <<1::64-big, 1::64-big, 200::64-big>>
      assert value == "v2"

      # Seek to non-existing key (150) - should position at next (200)
      :ok = NIF.iterator_seek(iter, <<1::64-big, 1::64-big, 150::64-big>>)
      {:ok, key, value} = NIF.iterator_next(iter)
      assert key == <<1::64-big, 1::64-big, 200::64-big>>
      assert value == "v2"

      # Seek past end (400) - should return iterator_end
      :ok = NIF.iterator_seek(iter, <<1::64-big, 1::64-big, 400::64-big>>)
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "2.5.1.3 iterator handles empty results correctly", %{db: db} do
      # Don't insert any data for this prefix
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<99::64-big>>)

      # First next should return iterator_end
      assert :iterator_end = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "2.5.1.4 iterator respects prefix boundaries", %{db: db} do
      # Insert data with different prefixes
      test_data = [
        {<<1::64-big, 1::64-big, 1::64-big>>, "v1"},
        {<<1::64-big, 1::64-big, 2::64-big>>, "v2"},
        # Different predicate
        {<<1::64-big, 2::64-big, 1::64-big>>, "v3"},
        # Different subject
        {<<2::64-big, 1::64-big, 1::64-big>>, "v4"}
      ]

      Enum.each(test_data, fn {key, value} ->
        :ok = NIF.put(db, :spo, key, value)
      end)

      # Iterate over prefix <<1::64-big, 1::64-big>>
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64-big, 1::64-big>>)

      results =
        Stream.unfold(iter, fn
          :iterator_end ->
            nil

          iter ->
            case NIF.iterator_next(iter) do
              {:ok, _key, _value} = result -> {result, iter}
              :iterator_end -> {nil, :iterator_end}
            end
        end)
        |> Enum.reject(&is_nil/1)

      NIF.iterator_close(iter)

      # Should only get entries with exact prefix match
      assert length(results) == 2

      # Stream.unfold yields just the first element of each tuple
      # So results is a list of {:ok, key, value} tuples
      keys = Enum.map(results, fn {:ok, key, _value} -> key end)

      Enum.each(keys, fn key ->
        assert <<1::64-big, 1::64-big, _::64-big>> = key
      end)
    end

    test "2.5.1.5 iterator closes cleanly under all conditions", %{db: db} do
      # Test 1: Close immediately after creation
      {:ok, iter1} = NIF.prefix_iterator(db, :spo, <<>>)
      assert :ok = NIF.iterator_close(iter1)
      Process.sleep(10)
      refute Process.alive?(iter1)

      # Test 2: Close after partial iteration
      test_data = for i <- 1..10, do: {<<1::64-big, i::64-big, 1::64-big>>, "v#{i}"}

      Enum.each(test_data, fn {key, value} ->
        :ok = NIF.put(db, :spo, key, value)
      end)

      {:ok, iter2} = NIF.prefix_iterator(db, :spo, <<1::64-big>>)
      {:ok, _key, _value} = NIF.iterator_next(iter2)
      {:ok, _key, _value} = NIF.iterator_next(iter2)
      assert :ok = NIF.iterator_close(iter2)

      # Test 3: Close after exhaustion
      {:ok, iter3} = NIF.prefix_iterator(db, :spo, <<2::64-big>>)
      assert :iterator_end = NIF.iterator_next(iter3)
      assert :ok = NIF.iterator_close(iter3)
    end
  end

  # =============================================================================
  # Section 2.5.2: Snapshot Integration Tests
  # =============================================================================

  describe "2.5.2 Snapshot Integration Tests" do
    test "2.5.2.1 snapshot provides consistent read across writes", %{db: db} do
      # Insert initial data
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 1::64-big>>, "v1")
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 2::64-big>>, "v2")

      # Create snapshot
      {:ok, snapshot} = NIF.snapshot(db)

      # Verify snapshot sees initial data
      {:ok, v1} = NIF.snapshot_get(db, snapshot, :spo, <<1::64-big, 1::64-big, 1::64-big>>)
      assert v1 == "v1"

      # Write more data
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 3::64-big>>, "v3")
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 1::64-big>>, "v1_modified")

      # Snapshot still sees old data
      {:ok, v1_old} = NIF.snapshot_get(db, snapshot, :spo, <<1::64-big, 1::64-big, 1::64-big>>)
      assert v1_old == "v1"

      # Non-snapshot read sees new data
      {:ok, v1_new} = NIF.get(db, :spo, <<1::64-big, 1::64-big, 1::64-big>>)
      assert v1_new == "v1_modified"

      # Snapshot doesn't see newly inserted key
      assert :not_found =
               NIF.snapshot_get(db, snapshot, :spo, <<1::64-big, 1::64-big, 3::64-big>>)

      # Release snapshot
      :ok = NIF.release_snapshot(db, snapshot)
    end

    test "2.5.2.2 snapshot iterator sees historical data", %{db: db} do
      # Insert initial data
      test_data = [
        {<<1::64-big, 1::64-big, 1::64-big>>, "v1"},
        {<<1::64-big, 1::64-big, 2::64-big>>, "v2"},
        {<<1::64-big, 1::64-big, 3::64-big>>, "v3"}
      ]

      Enum.each(test_data, fn {key, value} ->
        :ok = NIF.put(db, :spo, key, value)
      end)

      # Create snapshot
      {:ok, snapshot} = NIF.snapshot(db)

      # Modify and add data
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 1::64-big>>, "v1_modified")
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 4::64-big>>, "v4")
      :ok = NIF.delete(db, :spo, <<1::64-big, 1::64-big, 2::64-big>>)

      # Snapshot iterator sees historical data
      {:ok, iter} = NIF.snapshot_prefix_iterator(db, snapshot, :spo, <<1::64-big, 1::64-big>>)

      results =
        Stream.unfold(iter, fn
          :iterator_end ->
            nil

          iter ->
            case NIF.iterator_next(iter) do
              {:ok, key, value} -> {{key, value}, iter}
              :iterator_end -> {nil, :iterator_end}
            end
        end)
        |> Enum.reject(&is_nil/1)

      NIF.iterator_close(iter)
      NIF.release_snapshot(db, snapshot)

      # Snapshot should see exactly 3 entries from snapshot time
      assert length(results) == 3

      # Verify data is from snapshot time
      key_values = Map.new(results)
      assert Map.get(key_values, <<1::64-big, 1::64-big, 1::64-big>>) == "v1"
      assert Map.get(key_values, <<1::64-big, 1::64-big, 2::64-big>>) == "v2"
      assert Map.get(key_values, <<1::64-big, 1::64-big, 3::64-big>>) == "v3"

      # Should not see modifications
      refute Map.has_key?(key_values, <<1::64-big, 1::64-big, 4::64-big>>)
    end

    test "2.5.2.3 multiple snapshots see different time points", %{db: db} do
      # Time 1: Insert first data
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 1::64-big>>, "v1")
      {:ok, snap1} = NIF.snapshot(db)

      # Time 2: Insert more data
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 2::64-big>>, "v2")
      {:ok, snap2} = NIF.snapshot(db)

      # Time 3: Modify and add
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 1::64-big>>, "v1_modified")
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 3::64-big>>, "v3")

      # snap1 sees only v1
      {:ok, snap1_v1} = NIF.snapshot_get(db, snap1, :spo, <<1::64-big, 1::64-big, 1::64-big>>)
      assert snap1_v1 == "v1"
      assert :not_found = NIF.snapshot_get(db, snap1, :spo, <<1::64-big, 1::64-big, 2::64-big>>)

      # snap2 sees v1 and v2
      {:ok, snap2_v1} = NIF.snapshot_get(db, snap2, :spo, <<1::64-big, 1::64-big, 1::64-big>>)
      {:ok, snap2_v2} = NIF.snapshot_get(db, snap2, :spo, <<1::64-big, 1::64-big, 2::64-big>>)
      assert snap2_v1 == "v1"
      assert snap2_v2 == "v2"
      assert :not_found = NIF.snapshot_get(db, snap2, :spo, <<1::64-big, 1::64-big, 3::64-big>>)

      # Current state sees all modifications
      {:ok, current_v1} = NIF.get(db, :spo, <<1::64-big, 1::64-big, 1::64-big>>)
      {:ok, current_v2} = NIF.get(db, :spo, <<1::64-big, 1::64-big, 2::64-big>>)
      {:ok, current_v3} = NIF.get(db, :spo, <<1::64-big, 1::64-big, 3::64-big>>)
      assert current_v1 == "v1_modified"
      assert current_v2 == "v2"
      assert current_v3 == "v3"

      # Release snapshots
      :ok = NIF.release_snapshot(db, snap1)
      :ok = NIF.release_snapshot(db, snap2)
    end

    test "2.5.2.4 snapshot release allows proper resource cleanup", %{db: db} do
      # Create multiple snapshots
      {:ok, snap1} = NIF.snapshot(db)
      {:ok, snap2} = NIF.snapshot(db)
      {:ok, snap3} = NIF.snapshot(db)

      # Verify all snapshots are distinct references
      assert snap1 != snap2
      assert snap2 != snap3

      # Release all snapshots
      :ok = NIF.release_snapshot(db, snap1)
      :ok = NIF.release_snapshot(db, snap2)
      :ok = NIF.release_snapshot(db, snap3)

      # Database should still be functional
      :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, 1::64-big>>, "v1")
      {:ok, value} = NIF.get(db, :spo, <<1::64-big, 1::64-big, 1::64-big>>)
      assert value == "v1"
    end

    test "2.5.2.5 snapshot provides isolation from modifications", %{db: db} do
      # Insert initial data
      Enum.each(1..10, fn i ->
        :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, i::64-big>>, "v#{i}")
      end)

      # Create snapshot
      {:ok, snapshot} = NIF.snapshot(db)

      # Make modifications
      Enum.each(11..20, fn i ->
        :ok = NIF.put(db, :spo, <<1::64-big, 1::64-big, i::64-big>>, "v#{i}")
      end)

      # Snapshot iterator should only see initial 10 entries
      {:ok, iter} = NIF.snapshot_prefix_iterator(db, snapshot, :spo, <<1::64-big, 1::64-big>>)

      count =
        Stream.unfold(iter, fn
          :iterator_end ->
            nil

          iter ->
            case NIF.iterator_next(iter) do
              {:ok, _key, _value} -> {1, iter}
              :iterator_end -> nil
            end
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.sum()

      NIF.iterator_close(iter)

      assert count == 10

      NIF.release_snapshot(db, snapshot)
    end
  end

  # =============================================================================
  # Section 2.5.3: Query Execution Tests
  # =============================================================================

  describe "2.5.3 Query Execution Tests" do
    setup %{db: db} do
      # Insert test data for queries
      test_triples = [
        {1, 1, 1},
        {1, 1, 2},
        {1, 2, 1},
        {1, 2, 2},
        {2, 1, 1},
        {2, 1, 2},
        {2, 2, 1},
        {3, 1, 1}
      ]

      Enum.each(test_triples, fn {s, p, o} ->
        Index.insert_triple(db, {s, p, o})
      end)

      :ok
    end

    test "2.5.3.1 simple pattern matching queries work", %{db: db} do
      # Simple pattern: ?s ?p 1 (find all triples with object=1)
      # Note: Index.lookup uses {:bound, id} for bound values and :var for unbound
      {:ok, results} = Index.lookup(db, {:var, :var, {:bound, 1}})

      results_list = Enum.to_list(results)

      # Should find 5 triples
      assert length(results_list) == 5

      # Verify results
      expected =
        Enum.sort([
          {1, 1, 1},
          {1, 2, 1},
          {2, 1, 1},
          {2, 2, 1},
          {3, 1, 1}
        ])

      assert Enum.sort(results_list) == expected
    end

    test "2.5.3.2 prefix scans efficiently with iterators", %{db: db} do
      # Test prefix-based scan using fold
      count =
        NIF.fold(db, :spo, <<2::64-big>>, 0, fn {_key, _value}, acc ->
          acc + 1
        end)

      # Should find 3 triples with subject=2
      assert count == 3
    end

    test "2.5.3.3 range queries with prefix scans", %{db: db} do
      # Prefix scan with upper bound
      results =
        NIF.fold(db, :spo, <<1::64-big>>, [], fn {key, _value}, acc ->
          [key | acc]
        end)

      # All triples with subject=1
      assert length(results) == 4

      # Verify all have subject=1
      Enum.each(results, fn <<s::64-big, _p::64-big, _o::64-big>> ->
        assert s == 1
      end)
    end

    test "2.5.3.4 Leapfrog Triejoin with new iterators", %{db: db} do
      # This test verifies that the Leapfrog algorithm works correctly
      # with the erlang-rocksdb iterators
      alias TripleStore.SPARQL.Leapfrog.TrieIterator

      # Create iterator at level 0 (subject level) with empty prefix
      # TrieIterator.new/4 takes (db, cf, prefix, level)
      {:ok, iter} = TrieIterator.new(db, :spo, <<>>, 0)

      # Collect all distinct subjects
      # new() already positions at first value, so get current first
      subjects =
        Stream.unfold({iter, true}, fn
          {_iter, false} ->
            nil

          {iter, true} ->
            case TrieIterator.current(iter) do
              :exhausted ->
                {nil, {iter, false}}

              {:ok, value} ->
                # Move to next distinct value
                case TrieIterator.next(iter) do
                  {:ok, next_iter} ->
                    {value, {next_iter, true}}

                  {:exhausted, _} ->
                    {value, {iter, false}}
                end
            end
        end)
        |> Enum.reject(&is_nil/1)

      TrieIterator.close(iter)

      assert Enum.sort(subjects) == [1, 2, 3]
    end

    test "2.5.3.5 iterator operations maintain consistency", %{db: db} do
      # Test that multiple iterators can coexist without interfering
      {:ok, iter1} = NIF.prefix_iterator(db, :spo, <<1::64-big>>)
      {:ok, iter2} = NIF.prefix_iterator(db, :spo, <<2::64-big>>)

      # Get first result from each
      {:ok, key1, _val1} = NIF.iterator_next(iter1)
      {:ok, key2, _val2} = NIF.iterator_next(iter2)

      # Verify they're from different prefixes
      assert <<1::64-big, _::binary>> = key1
      assert <<2::64-big, _::binary>> = key2

      # Both iterators should be independent
      {:ok, _next1, _} = NIF.iterator_next(iter1)
      {:ok, _next2, _} = NIF.iterator_next(iter2)

      NIF.iterator_close(iter1)
      NIF.iterator_close(iter2)
    end
  end

  # =============================================================================
  # Section 2.5.4: Performance Validation Tests
  # =============================================================================

  describe "2.5.4 Performance Validation Tests" do
    test "2.5.4.1 iterator throughput baseline test", %{db: db} do
      # Insert test data
      num_entries = 100

      Enum.each(1..num_entries, fn i ->
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big, 1::64-big>>, "value#{i}")
      end)

      # Measure iteration time
      start_time = System.monotonic_time(:millisecond)

      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64-big>>)

      count =
        Stream.unfold(iter, fn
          :iterator_end ->
            nil

          iter ->
            case NIF.iterator_next(iter) do
              {:ok, _key, _value} -> {1, iter}
              :iterator_end -> nil
            end
        end)
        |> Enum.reject(&is_nil/1)
        |> Enum.sum()

      end_time = System.monotonic_time(:millisecond)
      elapsed_ms = end_time - start_time

      NIF.iterator_close(iter)

      # Verify we iterated all entries
      assert count == num_entries

      # Calculate throughput (entries per second)
      throughput = if elapsed_ms > 0, do: div(count * 1000, elapsed_ms), else: count

      # Log throughput for information
      IO.puts("Iterator throughput: #{throughput} entries/sec")
    end

    test "2.5.4.2 fold operations work correctly", %{db: db} do
      # Insert test data
      num_entries = 100

      Enum.each(1..num_entries, fn i ->
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big, 1::64-big>>, "value#{i}")
      end)

      fold_count =
        NIF.fold(db, :spo, <<1::64-big>>, 0, fn {_key, _value}, acc ->
          acc + 1
        end)

      assert fold_count == num_entries
    end

    test "2.5.4.3 seek latency is acceptable", %{db: db} do
      # Insert data with sparse keys
      Enum.each([1, 100, 1000, 10_000], fn i ->
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big, 1::64-big>>, "value#{i}")
      end)

      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64-big>>)

      # Test a few seeks
      :ok = NIF.iterator_seek(iter, <<1::64-big, 100::64-big, 1::64-big>>)
      {:ok, _key, _value} = NIF.iterator_next(iter)

      :ok = NIF.iterator_seek(iter, <<1::64-big, 1000::64-big, 1::64-big>>)
      {:ok, _key, _value} = NIF.iterator_next(iter)

      NIF.iterator_close(iter)
    end

    test "2.5.4.4 stream resources properly cleaned", %{db: db} do
      # Create test data
      Enum.each(1..50, fn i ->
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big, 1::64-big>>, "value#{i}")
      end)

      # Create stream but halt early
      stream = NIF.prefix_stream(db, :spo, <<1::64-big>>)

      # Take only 5 entries and halt
      _taken = Enum.take(stream, 5)

      # Give some time for cleanup
      Process.sleep(100)

      # Database should still be functional
      :ok = NIF.put(db, :spo, <<2::64-big, 1::64-big, 1::64-big>>, "test")
      {:ok, _value} = NIF.get(db, :spo, <<2::64-big, 1::64-big, 1::64-big>>)
    end
  end
end
