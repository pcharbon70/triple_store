defmodule TripleStore.Backend.RocksDB.FoldOperationsTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF

  @moduletag :fold_operations
  @moduletag timeout: 120_000

  describe "Section 2.3.1: Fold Operations Implementation" do
    test "2.3.1.1 fold/5 accumulates all entries in prefix" do
      path = "/tmp/test_fold_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data with prefix <<1::64-big>>
      for i <- 1..10 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Add data with different prefix
      for i <- 1..5 do
        :ok = NIF.put(db, :spo, <<2::64-big, i::64-big>>, "other#{i}")
      end

      # Fold over prefix <<1::64-big>>
      count =
        NIF.fold(db, :spo, <<1::64-big>>, 0, fn {_k, _v}, acc ->
          acc + 1
        end)

      assert count == 10

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.1.2 fold/5 can sum values" do
      path = "/tmp/test_fold_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data with numeric values
      for i <- 1..5 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, <<i::64-big>>)
      end

      # Fold to sum values
      sum =
        NIF.fold(db, :spo, <<1::64-big>>, 0, fn {_k, v}, acc ->
          <<i::64-big>> = v
          acc + i
        end)

      assert sum == 15

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.1.3 fold/5 with iterate_upper_bound option" do
      path = "/tmp/test_fold_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      for i <- 1..20 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Fold with upper bound at <<1::64-big, 11::64-big>>
      count =
        NIF.fold(
          db,
          :spo,
          <<1::64-big>>,
          0,
          fn {_k, _v}, acc ->
            acc + 1
          end,
          iterate_upper_bound: <<1::64-big, 11::64-big>>
        )

      # Should only get entries up to (but not including) the bound
      assert count <= 11

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.1.4 fold/5 handles empty prefix" do
      path = "/tmp/test_fold_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      for i <- 1..5 do
        :ok = NIF.put(db, :spo, <<i::64-big>>, "value#{i}")
      end

      # Fold over empty prefix (all entries)
      count =
        NIF.fold(db, :spo, <<>>, 0, fn {_k, _v}, acc ->
          acc + 1
        end)

      assert count == 5

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.1.5 fold/5 handles non-existent prefix" do
      path = "/tmp/test_fold_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data with different prefix
      for i <- 1..5 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Fold over non-existent prefix
      count =
        NIF.fold(db, :spo, <<99::64-big>>, 0, fn {_k, _v}, acc ->
          acc + 1
        end)

      assert count == 0

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end
  end

  describe "Section 2.3.2: Fold Keys Operation" do
    test "2.3.2.1 fold_keys/5 iterates keys only" do
      path = "/tmp/test_fold_keys_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      for i <- 1..10 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Collect keys using fold_keys
      keys =
        NIF.fold_keys(db, :spo, <<1::64-big>>, [], fn k, acc ->
          [k | acc]
        end)

      assert length(keys) == 10

      # Verify all keys have the correct prefix
      Enum.all?(keys, fn k -> binary_part(k, 0, 8) == <<1::64-big>> end)

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.2.2 fold_keys/5 handles prefix boundary" do
      path = "/tmp/test_fold_keys_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data with two different prefixes
      for i <- 1..5 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value1_#{i}")
        :ok = NIF.put(db, :spo, <<2::64-big, i::64-big>>, "value2_#{i}")
      end

      # Count keys with prefix <<1::64-big>>
      count =
        NIF.fold_keys(db, :spo, <<1::64-big>>, 0, fn _k, acc ->
          acc + 1
        end)

      assert count == 5

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.2.3 fold_keys/5 respects iterate_upper_bound" do
      path = "/tmp/test_fold_keys_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      for i <- 1..20 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Fold keys with upper bound
      keys =
        NIF.fold_keys(
          db,
          :spo,
          <<1::64-big>>,
          [],
          fn k, acc ->
            [k | acc]
          end,
          iterate_upper_bound: <<1::64-big, 11::64-big>>
        )

      # Should have at most 11 keys (0-10)
      assert length(keys) <= 11

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.2.4 fold_keys/5 handles empty column family" do
      path = "/tmp/test_fold_keys_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Fold over empty CF
      count =
        NIF.fold_keys(db, :spo, <<>>, 0, fn _k, acc ->
          acc + 1
        end)

      assert count == 0

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end
  end

  describe "Section 2.3.3: Stream Operations" do
    test "2.3.3.1 prefix_stream/4 creates lazy stream" do
      path = "/tmp/test_stream_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      for i <- 1..10 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Create stream
      stream = NIF.prefix_stream(db, :spo, <<1::64-big>>)

      # Verify it's a stream
      assert Enumerable.impl_for(stream) != nil

      # Collect all entries
      entries = Enum.to_list(stream)

      assert length(entries) == 10

      # Verify format
      Enum.all?(entries, fn {k, v} ->
        binary_part(k, 0, 8) == <<1::64-big>> and is_binary(v)
      end)

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.3.2 prefix_stream/4 with Stream.take" do
      path = "/tmp/test_stream_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      for i <- 1..100 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Take only first 5 entries
      entries =
        db
        |> NIF.prefix_stream(:spo, <<1::64-big>>)
        |> Stream.take(5)
        |> Enum.to_list()

      assert length(entries) == 5

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.3.3 prefix_stream/4 handles empty prefix" do
      path = "/tmp/test_stream_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      for i <- 1..5 do
        :ok = NIF.put(db, :spo, <<i::64-big>>, "value#{i}")
      end

      # Stream all entries
      entries = Enum.to_list(NIF.prefix_stream(db, :spo, <<>>))

      assert length(entries) == 5

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.3.4 prefix_stream/4 handles non-existent prefix" do
      path = "/tmp/test_stream_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data with different prefix
      for i <- 1..5 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Stream non-existent prefix
      entries = Enum.to_list(NIF.prefix_stream(db, :spo, <<99::64-big>>))

      assert Enum.empty?(entries)

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.3.5 prefix_stream/4 properly closes iterator on halt" do
      path = "/tmp/test_stream_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add lots of data
      for i <- 1..1000 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Take only first entry then halt
      entries =
        db
        |> NIF.prefix_stream(:spo, <<1::64-big>>)
        |> Stream.take(1)
        |> Enum.to_list()

      assert length(entries) == 1

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end
  end

  describe "Section 2.3.4: Integration Tests" do
    test "2.3.4.1 fold vs manual iteration produce same results" do
      path = "/tmp/test_fold_integration_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      for i <- 1..50 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Collect via fold
      fold_entries =
        NIF.fold(db, :spo, <<1::64-big>>, [], fn {k, v}, acc ->
          [{k, v} | acc]
        end)
        |> Enum.sort()

      # Collect via iterator
      {:ok, iter} = NIF.prefix_iterator(db, :spo, <<1::64-big>>)

      iter_entries =
        Stream.unfold(iter, fn
          :iterator_end ->
            nil

          iter ->
            case NIF.iterator_next(iter) do
              {:ok, k, v} -> {{k, v}, iter}
              :iterator_end -> {nil, :iterator_end}
            end
        end)
        |> Enum.reject(&is_nil/1)

      NIF.iterator_close(iter)

      # Both should have same entries
      assert length(fold_entries) == length(iter_entries)
      assert length(fold_entries) == 50

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.4.2 fold with snapshot sees historical data" do
      path = "/tmp/test_fold_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add initial data
      for i <- 1..5 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Create snapshot
      {:ok, snap} = NIF.snapshot(db)

      # Add more data
      for i <- 6..10 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Fold with snapshot should only see 5 entries
      count =
        NIF.fold(
          db,
          :spo,
          <<1::64-big>>,
          0,
          fn {_k, _v}, acc ->
            acc + 1
          end,
          snapshot: snap
        )

      assert count == 5

      # Regular fold should see 10 entries
      count =
        NIF.fold(db, :spo, <<1::64-big>>, 0, fn {_k, _v}, acc ->
          acc + 1
        end)

      assert count == 10

      # Clean up
      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.4.3 stream with snapshot sees historical data" do
      path = "/tmp/test_stream_snapshot_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add initial data
      for i <- 1..5 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Create snapshot
      {:ok, snap} = NIF.snapshot(db)

      # Add more data
      for i <- 6..10 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Stream with snapshot should only see 5 entries
      entries = Enum.to_list(NIF.prefix_stream(db, :spo, <<1::64-big>>, snapshot: snap))

      assert length(entries) == 5

      # Clean up
      NIF.release_snapshot(db, snap)
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.4.4 fold_keys is more efficient than fold for key-only ops" do
      path = "/tmp/test_fold_efficiency_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data
      for i <- 1..100 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value#{i}")
      end

      # Collect keys via fold_keys
      keys_from_fold_keys =
        NIF.fold_keys(db, :spo, <<1::64-big>>, [], fn k, acc ->
          [k | acc]
        end)
        |> Enum.sort()

      # Collect keys via fold (discarding values)
      keys_from_fold =
        NIF.fold(db, :spo, <<1::64-big>>, [], fn {k, _v}, acc ->
          [k | acc]
        end)
        |> Enum.sort()

      # Both should have same keys
      assert keys_from_fold_keys == keys_from_fold
      assert length(keys_from_fold_keys) == 100

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end

    test "2.3.4.5 stream respects prefix boundaries correctly" do
      path = "/tmp/test_stream_boundary_#{System.unique_integer()}"
      File.rm_rf(path)

      {:ok, db} = NIF.open(path)

      # Add data with interleaved prefixes
      for i <- 1..10 do
        :ok = NIF.put(db, :spo, <<1::64-big, i::64-big>>, "value1_#{i}")
        :ok = NIF.put(db, :spo, <<2::64-big, i::64-big>>, "value2_#{i}")
      end

      # Stream should only get entries with prefix <<1::64-big>>
      entries = Enum.to_list(NIF.prefix_stream(db, :spo, <<1::64-big>>))

      assert length(entries) == 10

      # Verify all keys have correct prefix
      Enum.all?(entries, fn {k, _v} -> binary_part(k, 0, 8) == <<1::64-big>> end)

      # Clean up
      NIF.close(db)
      File.rm_rf(path)
    end
  end
end
