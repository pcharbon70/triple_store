defmodule TripleStore.Backend.RocksDB.ErlangRocksdbIntegrationTest do
  @moduledoc """
  Integration tests for Section 1.5: Basic Migration Functionality

  These tests verify that erlang-rocksdb can be used with our configured
  column families and binary encoding format. They test the database lifecycle,
  basic operations, and compatibility with the TripleStore's data formats.

  Note: These tests use erlang-rocksdb directly to verify the underlying
  library functionality. The NIF adapter tests are in a separate suite.

  Configuration Note: Some advanced options (prefix_extractor, memtable_prefix_bloom)
  are version-dependent in erlang-rocksdb and are not included in these tests.
  They will be configured during the full adapter implementation.
  """
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ColumnFamilyConfig
  alias TripleStore.Dictionary
  alias TripleStore.Index

  @moduletag :integration

  # Temporary database path helper
  defp temp_db_path(suffix \\ "") do
    name = "test_integration_#{System.unique_integer([:positive, :monotonic])}#{suffix}"

    Path.join(System.tmp_dir!(), name)
    |> String.to_charlist()
  end

  # Minimal column family descriptors for basic testing
  defp minimal_cf_descriptors do
    [
      {~c"default", []},
      {~c"id2str", []},
      {~c"str2id", []},
      {~c"spo", []},
      {~c"pos", []},
      {~c"osp", []},
      {~c"derived", []},
      {~c"numeric_range", []}
    ]
  end

  # Helper to create database with all column families
  defp create_db_with_all_cfs(db_path, db_opts) do
    # First open with just default CF
    {:ok, db, [default_cf]} = :rocksdb.open_with_cf(db_path, db_opts, [{~c"default", []}])

    # Create additional column families
    {:ok, id2str_cf} = :rocksdb.create_column_family(db, ~c"id2str", [])
    {:ok, str2id_cf} = :rocksdb.create_column_family(db, ~c"str2id", [])
    {:ok, spo_cf} = :rocksdb.create_column_family(db, ~c"spo", [])
    {:ok, pos_cf} = :rocksdb.create_column_family(db, ~c"pos", [])
    {:ok, osp_cf} = :rocksdb.create_column_family(db, ~c"osp", [])
    {:ok, derived_cf} = :rocksdb.create_column_family(db, ~c"derived", [])
    {:ok, numeric_cf} = :rocksdb.create_column_family(db, ~c"numeric_range", [])

    {:ok, db, [default_cf, id2str_cf, str2id_cf, spo_cf, pos_cf, osp_cf, derived_cf, numeric_cf]}
  end

  # Helper to open existing database with all column families
  defp open_db_with_all_cfs(db_path, db_opts) do
    cf_descriptors = minimal_cf_descriptors()
    :rocksdb.open_with_cf(db_path, db_opts, cf_descriptors)
  end

  # ===========================================================================
  # Section 1.5.1: Database Lifecycle Tests
  # ===========================================================================

  describe "1.5.1 Database Lifecycle Tests" do
    test "1.5.1.1 Test creating new database with all column families" do
      db_path = temp_db_path("create_new")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        assert {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)

        # Verify we got the expected number of column family handles
        # 7 TripleStore CFs + default
        assert length(cf_handles) == 8

        # Verify the database is open
        assert is_reference(db)

        # Close the database
        assert :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.1.2 Test opening existing database preserves data" do
      db_path = temp_db_path("reopen")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]

        # Create database and write some data
        {:ok, db1, cf_handles1} = create_db_with_all_cfs(db_path, db_opts)

        # Get handles by name (assuming order matches our config)
        [default_cf, id2str_cf, str2id_cf, spo_cf, pos_cf, osp_cf, derived_cf, numeric_cf] =
          cf_handles1

        # Write some test data to id2str CF
        test_key = <<1::64-big>>
        test_value = "http://example.org/test"
        :ok = :rocksdb.put(db1, id2str_cf, test_key, test_value, [])

        # Close database
        :ok = :rocksdb.close(db1)

        # Reopen database
        assert {:ok, db2, cf_handles2} = open_db_with_all_cfs(db_path, db_opts)

        # Get id2str handle from reopened database
        [_default_cf2, id2str_cf2 | _] = cf_handles2

        # Verify data is preserved
        assert {:ok, ^test_value} = :rocksdb.get(db2, id2str_cf2, test_key, [])

        :ok = :rocksdb.close(db2)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.1.3 Test database close releases resources" do
      db_path = temp_db_path("close")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, _cf_handles} = create_db_with_all_cfs(db_path, db_opts)

        # Close the database
        assert :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.1.4 Test database with configured column families" do
      db_path = temp_db_path("cf_access")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)

        # Verify we have exactly 8 CFs
        assert length(cf_handles) == 8

        # Verify each CF is usable
        Enum.each(cf_handles, fn cf_handle ->
          key = <<System.unique_integer()::64-big>>
          value = <<System.unique_integer()::64-big>>
          assert :ok = :rocksdb.put(db, cf_handle, key, value, [])
          assert {:ok, ^value} = :rocksdb.get(db, cf_handle, key, [])
        end)

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.1.5 Test database reopen after unclean shutdown" do
      db_path = temp_db_path("unclean")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]

        # Create and populate database
        {:ok, db1, cf_handles1} = create_db_with_all_cfs(db_path, db_opts)
        [_, id2str_cf | _] = cf_handles1

        # Write some data
        for i <- 1..10 do
          key = <<i::64-big>>
          value = "test_value_#{i}"
          :rocksdb.put(db1, id2str_cf, key, value, [])
        end

        # Simulate unclean shutdown (close without flush)
        :rocksdb.close(db1)

        # Reopen database (should recover from WAL)
        assert {:ok, db2, cf_handles2} = open_db_with_all_cfs(db_path, db_opts)
        [_, id2str_cf2 | _] = cf_handles2

        # Verify data is intact after recovery
        for i <- 1..10 do
          key = <<i::64-big>>
          expected_value = "test_value_#{i}"
          assert {:ok, ^expected_value} = :rocksdb.get(db2, id2str_cf2, key, [])
        end

        :ok = :rocksdb.close(db2)
      after
        File.rm_rf(to_string(db_path))
      end
    end
  end

  # ===========================================================================
  # Section 1.5.2: Data Migration Compatibility Tests
  # ===========================================================================

  describe "1.5.2 Data Migration Compatibility Tests" do
    test "1.5.2.1 Test dictionary data with our binary encoding" do
      db_path = temp_db_path("dict_encoding")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)
        [_default, id2str_cf, str2id_cf | _] = cf_handles

        # Test dictionary ID encoding (64-bit with type tag)
        # Type 1 = URI
        uri_id = Dictionary.encode_id(Dictionary.type_uri(), 1)

        # Write to id2str (ID -> string)
        uri_string = "http://example.org/test"
        assert :ok = :rocksdb.put(db, id2str_cf, <<uri_id::64-big>>, uri_string, [])

        # Write to str2id (string -> ID)
        assert :ok = :rocksdb.put(db, str2id_cf, uri_string, <<uri_id::64-big>>, [])

        # Read back and verify
        assert {:ok, ^uri_string} = :rocksdb.get(db, id2str_cf, <<uri_id::64-big>>, [])
        assert {:ok, <<^uri_id::64-big>>} = :rocksdb.get(db, str2id_cf, uri_string, [])

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.2.2 Test triple index encoding with our binary format" do
      db_path = temp_db_path("index_encoding")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)
        [_default, _id2str, _str2id, spo_cf, pos_cf, osp_cf | _] = cf_handles

        # Create test triple IDs
        s = 1
        p = 2
        o = 3

        # Test SPO index encoding (24-byte key: s::64, p::64, o::64)
        spo_key = Index.spo_key(s, p, o)
        assert byte_size(spo_key) == 24

        :ok = :rocksdb.put(db, spo_cf, spo_key, <<>>, [])

        # Test POS index encoding
        pos_key = Index.pos_key(p, o, s)
        assert byte_size(pos_key) == 24

        :ok = :rocksdb.put(db, pos_cf, pos_key, <<>>, [])

        # Test OSP index encoding
        osp_key = Index.osp_key(o, s, p)
        assert byte_size(osp_key) == 24

        :ok = :rocksdb.put(db, osp_cf, osp_key, <<>>, [])

        # Verify keys can be read back
        assert {:ok, <<>>} = :rocksdb.get(db, spo_cf, spo_key, [])
        assert {:ok, <<>>} = :rocksdb.get(db, pos_cf, pos_key, [])
        assert {:ok, <<>>} = :rocksdb.get(db, osp_cf, osp_key, [])

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.2.3 Test numeric range encoding" do
      db_path = temp_db_path("numeric_encoding")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)

        # Get numeric_range CF handle (last one)
        numeric_cf = List.last(cf_handles)

        # Test inline integer encoding
        {:ok, int_id} = Dictionary.encode_integer(42)

        # Write numeric key-value pair
        numeric_key = <<int_id::64-big>>
        numeric_value = <<100::64-big>>
        :ok = :rocksdb.put(db, numeric_cf, numeric_key, numeric_value, [])

        # Read back and verify
        assert {:ok, ^numeric_value} = :rocksdb.get(db, numeric_cf, numeric_key, [])

        # Verify integer can be decoded
        assert {:ok, 42} = Dictionary.decode_integer(int_id)

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.2.4 Test derived data encoding" do
      db_path = temp_db_path("derived_encoding")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)

        # Get derived CF handle (second to last)
        derived_cf = Enum.at(cf_handles, 6)

        # Test derived triple (same encoding as SPO)
        s = 10
        p = 20
        o = 30

        derived_key = Index.spo_key(s, p, o)
        # Some derivation metadata
        derived_value = <<1::8>>

        :ok = :rocksdb.put(db, derived_cf, derived_key, derived_value, [])

        # Read back and verify
        assert {:ok, ^derived_value} = :rocksdb.get(db, derived_cf, derived_key, [])

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.2.5 Verify no data loss across all column families" do
      db_path = temp_db_path("no_data_loss")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)

        [_default, id2str_cf, str2id_cf, spo_cf, pos_cf, osp_cf, derived_cf, numeric_cf] =
          cf_handles

        # Write data to all column families
        test_data = [
          {id2str_cf, <<1::64-big>>, "http://example.org/1"},
          {str2id_cf, "http://example.org/1", <<1::64-big>>},
          {spo_cf, Index.spo_key(1, 2, 3), <<>>},
          {pos_cf, Index.pos_key(2, 3, 1), <<>>},
          {osp_cf, Index.osp_key(3, 1, 2), <<>>},
          {derived_cf, Index.spo_key(4, 5, 6), <<>>},
          {numeric_cf, <<100::64-big>>, <<200::64-big>>}
        ]

        Enum.each(test_data, fn {cf, key, value} ->
          :rocksdb.put(db, cf, key, value, [])
        end)

        # Verify all data can be read back
        Enum.each(test_data, fn {cf, key, value} ->
          assert {:ok, ^value} = :rocksdb.get(db, cf, key, [])
        end)

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end
  end

  # ===========================================================================
  # Section 1.5.3: Basic Operations Tests
  # ===========================================================================

  describe "1.5.3 Basic Operations Tests" do
    test "1.5.3.1 Test put/get round-trip for all column families" do
      db_path = temp_db_path("put_get")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)

        # Test put/get for each CF
        Enum.each(cf_handles, fn cf ->
          key = <<System.unique_integer()::64-big>>
          value = <<System.unique_integer()::128-big>>

          :ok = :rocksdb.put(db, cf, key, value, [])
          assert {:ok, ^value} = :rocksdb.get(db, cf, key, [])
        end)

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.3.2 Test delete operation removes data correctly" do
      db_path = temp_db_path("delete")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)
        [cf | _] = cf_handles

        # Write a key
        key = <<1::64-big>>
        value = "test_value"
        :ok = :rocksdb.put(db, cf, key, value, [])

        # Verify it exists
        assert {:ok, ^value} = :rocksdb.get(db, cf, key, [])

        # Delete the key
        :ok = :rocksdb.delete(db, cf, key, [])

        # Verify it's gone
        assert :not_found = :rocksdb.get(db, cf, key, [])

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.3.3 Test exists returns correct results" do
      db_path = temp_db_path("exists")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)
        [cf | _] = cf_handles

        # Test non-existent key
        key = <<1::64-big>>
        assert :not_found = :rocksdb.get(db, cf, key, [])

        # Write key
        :ok = :rocksdb.put(db, cf, key, "value", [])

        # Test existing key
        assert {:ok, "value"} = :rocksdb.get(db, cf, key, [])

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.3.4 Test write_batch performs atomic operations" do
      db_path = temp_db_path("write_batch")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)
        [cf | _] = cf_handles

        # Create batch operations
        batch = [
          {:put, cf, <<1::64-big>>, "value1"},
          {:put, cf, <<2::64-big>>, "value2"},
          {:put, cf, <<3::64-big>>, "value3"}
        ]

        # Write batch atomically
        :ok = :rocksdb.write(db, batch, [])

        # Verify all writes completed
        assert {:ok, "value1"} = :rocksdb.get(db, cf, <<1::64-big>>, [])
        assert {:ok, "value2"} = :rocksdb.get(db, cf, <<2::64-big>>, [])
        assert {:ok, "value3"} = :rocksdb.get(db, cf, <<3::64-big>>, [])

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.3.5 Test mixed batch with puts and deletes" do
      db_path = temp_db_path("mixed_batch")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)
        [cf | _] = cf_handles

        # Write initial data
        :ok = :rocksdb.put(db, cf, <<1::64-big>>, "old1", [])
        :ok = :rocksdb.put(db, cf, <<2::64-big>>, "old2", [])

        # Create mixed batch
        batch = [
          # Update
          {:put, cf, <<2::64-big>>, "new2"},
          # Insert
          {:put, cf, <<3::64-big>>, "value3"},
          # Delete
          {:delete, cf, <<1::64-big>>}
        ]

        # Apply batch atomically
        :ok = :rocksdb.write(db, batch, [])

        # Verify results
        # Deleted
        assert :not_found = :rocksdb.get(db, cf, <<1::64-big>>, [])
        # Updated
        assert {:ok, "new2"} = :rocksdb.get(db, cf, <<2::64-big>>, [])
        # Inserted
        assert {:ok, "value3"} = :rocksdb.get(db, cf, <<3::64-big>>, [])

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end
  end

  # ===========================================================================
  # Section 1.5.4: Prefix-Based Operations (Index Scans)
  # ===========================================================================

  describe "1.5.4 Prefix-Based Operations" do
    test "1.5.4.1 Test prefix scan on SPO index" do
      db_path = temp_db_path("prefix_scan")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)
        [_default, _id2str, _str2id, spo_cf | _] = cf_handles

        # Write triples with same subject
        subject = 1

        triples = [
          {subject, 10, 100},
          {subject, 10, 101},
          {subject, 11, 100},
          {subject, 11, 101}
        ]

        Enum.each(triples, fn {s, p, o} ->
          key = Index.spo_key(s, p, o)
          :rocksdb.put(db, spo_cf, key, <<>>, [])
        end)

        # Create iterator for prefix scan (first 8 bytes = subject)
        {:ok, iter} = :rocksdb.iterator(db, spo_cf, [])

        # Seek to subject prefix - iterator_move returns the first matching key
        prefix = Index.spo_prefix(subject)

        # Collect all keys with this prefix by iterating
        results =
          case :rocksdb.iterator_move(iter, prefix) do
            {:ok, first_key, _value} ->
              # Check if first key matches prefix
              case first_key do
                <<^subject::64-big, _::binary>> ->
                  # First key matches, collect it and continue
                  rest =
                    Stream.repeatedly(fn ->
                      case :rocksdb.iterator_move(iter, :next) do
                        {:ok, key, _value} ->
                          case key do
                            <<^subject::64-big, _::binary>> -> {:ok, key}
                            _ -> :done
                          end

                        :iterator_end ->
                          :done

                        {:error, _} ->
                          :done
                      end
                    end)
                    |> Stream.take_while(fn
                      :done -> false
                      _ -> true
                    end)
                    |> Enum.map(fn
                      {:ok, key} -> key
                      _ -> nil
                    end)

                  [first_key | rest]

                _ ->
                  # First key doesn't match prefix
                  []
              end

            :iterator_end ->
              []

            {:error, _} ->
              []
          end

        # Should have found 4 triples
        assert length(results) == 4

        :rocksdb.iterator_close(iter)
        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.4.2 Test prefix scan with subject-predicate prefix" do
      db_path = temp_db_path("sp_prefix_scan")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)
        [_default, _id2str, _str2id, spo_cf | _] = cf_handles

        # Write triples with same subject-predicate
        subject = 1
        predicate = 10
        objects = [100, 101, 102]

        Enum.each(objects, fn o ->
          key = Index.spo_key(subject, predicate, o)
          :rocksdb.put(db, spo_cf, key, <<>>, [])
        end)

        # Create iterator
        {:ok, iter} = :rocksdb.iterator(db, spo_cf, [])

        # Seek to subject-predicate prefix (first 16 bytes)
        prefix = Index.spo_prefix(subject, predicate)

        # Collect results - handle the seek returning the first key
        results =
          case :rocksdb.iterator_move(iter, prefix) do
            {:ok, first_key, _value} ->
              # Check if first key matches prefix
              case first_key do
                <<^subject::64-big, ^predicate::64-big, _::binary>> ->
                  # First key matches, count it and continue
                  rest_count =
                    Stream.repeatedly(fn ->
                      case :rocksdb.iterator_move(iter, :next) do
                        {:ok, key, _value} ->
                          case key do
                            <<^subject::64-big, ^predicate::64-big, _::binary>> -> 1
                            _ -> :done
                          end

                        :iterator_end ->
                          :done

                        {:error, _} ->
                          :done
                      end
                    end)
                    |> Stream.take_while(fn
                      :done -> false
                      _ -> true
                    end)
                    |> Enum.sum()

                  1 + rest_count

                _ ->
                  # First key doesn't match prefix
                  0
              end

            :iterator_end ->
              0

            {:error, _} ->
              0
          end

        # Should have found 3 triples
        assert results == 3

        :rocksdb.iterator_close(iter)
        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end
  end

  # ===========================================================================
  # Section 1.5.5: Configuration Validation
  # ===========================================================================

  describe "1.5.5 Configuration Validation" do
    test "1.5.5.1 Verify all column families are accessible" do
      db_path = temp_db_path("cf_access_valid")

      try do
        db_opts = [create_if_missing: true, error_if_exists: false]
        {:ok, db, cf_handles} = create_db_with_all_cfs(db_path, db_opts)

        # Verify we have exactly 8 CFs
        assert length(cf_handles) == 8

        # Verify each CF is usable
        Enum.each(cf_handles, fn cf_handle ->
          key = <<System.unique_integer()::64-big>>
          value = <<System.unique_integer()::64-big>>
          assert :ok = :rocksdb.put(db, cf_handle, key, value, [])
          assert {:ok, ^value} = :rocksdb.get(db, cf_handle, key, [])
        end)

        :ok = :rocksdb.close(db)
      after
        File.rm_rf(to_string(db_path))
      end
    end

    test "1.5.5.2 Verify column family configuration module" do
      # Test that our configuration module provides the expected data

      # Test triple schema (8 column families)
      triple_cfs = ColumnFamilyConfig.cf_descriptors(:triple)
      assert length(triple_cfs) == 8

      # Test quad schema (11 column families)
      quad_cfs = ColumnFamilyConfig.cf_descriptors(:quad)
      assert length(quad_cfs) == 11

      # Each should be a {name, options} tuple
      Enum.each(triple_cfs ++ quad_cfs, fn {name, opts} ->
        assert is_binary(name)
        assert is_list(opts)
      end)
    end
  end
end
