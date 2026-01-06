# Smoke test to verify erlang-rocksdb dependency works
# This is a temporary test for Phase 1.1 (Dependency Management)

db_name = "test_rocksdb_#{System.unique_integer([:positive])}"
db_path = Path.join(System.tmp_dir!(), db_name) |> String.to_charlist()

IO.puts("Testing erlang-rocksdb dependency...")

# Test 1: Open database
IO.puts("  [1/4] Opening database with column families...")
case :rocksdb.open_with_cf(db_path, [{:create_if_missing, true}], [{~c"default", []}]) do
  {:ok, db, cf_handles} ->
    IO.puts("       Database opened successfully")

    # Get the default column family handle (first in list)
    [cf | _] = cf_handles

    # Test 2: Put operation
    IO.puts("  [2/4] Testing put operation...")
    case :rocksdb.put(db, cf, "test_key", "test_value", []) do
      :ok ->
        IO.puts("       Put successful")

        # Test 3: Get operation
        IO.puts("  [3/4] Testing get operation...")
        case :rocksdb.get(db, cf, "test_key", []) do
          {:ok, "test_value"} ->
            IO.puts("       Get successful - value matches")

            # Test 4: Close operation
            IO.puts("  [4/4] Testing close operation...")
            :rocksdb.close(db)
            File.rm_rf(Path.join(System.tmp_dir!(), db_name))
            IO.puts("")
            IO.puts("  All erlang-rocksdb tests passed!")
            IO.puts("  Dependency management migration complete.")

          {:ok, val} ->
            IO.puts("       ERROR: Get returned wrong value: #{inspect(val)}")
            File.rm_rf(Path.join(System.tmp_dir!(), db_name))

          :not_found ->
            IO.puts("       ERROR: Get returned not_found")
            File.rm_rf(Path.join(System.tmp_dir!(), db_name))
        end

      {:error, reason} ->
        IO.puts("       ERROR: Put failed: #{inspect(reason)}")
        File.rm_rf(Path.join(System.tmp_dir!(), db_name))
    end

  {:error, reason} ->
    IO.puts("       ERROR: Failed to open database: #{inspect(reason)}")
end
