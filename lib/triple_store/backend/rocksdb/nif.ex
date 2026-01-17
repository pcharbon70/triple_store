defmodule TripleStore.Backend.RocksDB.NIF do
  @moduledoc """
  Convenience wrapper for RocksDB operations via ErlangAdapter.

  **DEPRECATED**: This module is deprecated as of Phase 3.3.
  Please use `TripleStore.Backend.RocksDB.ErlangAdapter` directly for new code.

  This module provides backward compatibility by delegating all calls to
  `ErlangAdapter`, which manages the erlang-rocksdb C++ NIF library connection.

  ## Migration Guide

  To migrate from this module to `ErlangAdapter`:

  ```elixir
  # Old way (deprecated)
  {:ok, db} = TripleStore.Backend.RocksDB.NIF.open("/path/to/db")

  # New way (recommended)
  {:ok, db} = TripleStore.Backend.RocksDB.ErlangAdapter.open("/path/to/db")
  ```

  The API is identical - just replace `NIF` with `ErlangAdapter` in your calls.

  ## Column Families

  The database uses the following column families:
  - `:id2str` - Maps 64-bit IDs to string values
  - `:str2id` - Maps string values to 64-bit IDs
  - `:spo` - Subject-Predicate-Object index
  - `:pos` - Predicate-Object-Subject index
  - `:osp` - Object-Subject-Predicate index
  - `:derived` - Stores inferred triples from reasoning
  - `:numeric_range` - Stores numeric range indices for efficient range queries

  ## Architecture

  This module delegates to `ErlangAdapter`, which:
  - Manages database lifecycle via GenServer
  - Translates column family atoms to erlang-rocksdb handles
  - Handles path validation and security
  - Translates error returns

  The `db_ref()` type is a GenServer PID (adapter process).
  """

  @deprecated "Use TripleStore.Backend.RocksDB.ErlangAdapter instead"

  alias TripleStore.Backend.RocksDB.ErlangAdapter

  @type db_ref :: pid()
  @type column_family :: :id2str | :str2id | :spo | :pos | :osp | :derived | :numeric_range
  @type iterator_ref :: reference()
  @type snapshot_ref :: reference()
  @type snapshot_iterator_ref :: reference()

  @type put_operation :: {column_family(), binary(), binary()}
  @type delete_operation :: {column_family(), binary()}
  @type mixed_put :: {:put, column_family(), binary(), binary()}
  @type mixed_delete :: {:delete, column_family(), binary()}

  @doc """
  Indicates the NIF is loaded and using erlang-rocksdb.
  """
  @spec nif_loaded :: String.t()
  def nif_loaded, do: "erlang-rocksdb"

  @doc """
  Opens a RocksDB database at the given path.

  ## Parameters

  - `path`: Path to the database directory

  ## Returns

  - `{:ok, db_ref}` - Database opened successfully
  - `{:error, reason}` - Failed to open database

  """
  @spec open(String.t()) :: {:ok, db_ref()} | {:error, term()}
  def open(path) when is_binary(path) do
    ErlangAdapter.open(path)
  end

  @doc """
  Opens a RocksDB database at the given path with options.

  ## Parameters

  - `path`: Path to the database directory
  - `opts`: Keyword list of options
    - `:schema` - Schema type: `:triple` (default) or `:quad`

  ## Returns

  - `{:ok, db_ref}` - Database opened successfully
  - `{:error, reason}` - Failed to open database

  """
  @spec open(String.t(), keyword()) :: {:ok, db_ref()} | {:error, term()}
  def open(path, opts) when is_binary(path) and is_list(opts) do
    ErlangAdapter.open(path, opts)
  end

  @doc """
  Closes the database and releases all resources.

  ## Parameters

  - `db_ref`: The database reference (adapter PID)

  ## Returns

  - `:ok` - Database closed successfully
  - `{:error, reason}` - Failed to close database

  """
  @spec close(db_ref()) :: :ok | {:error, term()}
  def close(db_ref) when is_pid(db_ref) do
    ErlangAdapter.close(db_ref)
  end

  @doc """
  Flushes the Write-Ahead Log to durable storage.

  ## Parameters

  - `db_ref`: The database reference
  - `sync`: If true, synchronously flush the WAL

  ## Returns

  - `:ok` - WAL flushed successfully
  - `{:error, reason}` - Failed to flush WAL

  """
  @spec flush_wal(db_ref(), boolean()) :: :ok | {:error, term()}
  def flush_wal(db_ref, sync) when is_pid(db_ref) and is_boolean(sync) do
    ErlangAdapter.flush_wal(db_ref, sync)
  end

  @doc """
  Sets runtime options for the database.

  ## Parameters

  - `db_ref`: The database reference
  - `options`: List of {option_name, option_value} tuples

  ## Returns

  - `:ok` - Options set successfully
  - `{:error, reason}` - Failed to set options

  """
  @spec set_options(db_ref(), [{String.t(), String.t()}]) :: :ok | {:error, term()}
  def set_options(db_ref, options) when is_pid(db_ref) and is_list(options) do
    ErlangAdapter.set_options(db_ref, options)
  end

  @doc """
  Gets the path of the database.

  ## Parameters

  - `db_ref`: The database reference

  ## Returns

  - `{:ok, path}` - Path retrieved successfully
  - `{:error, reason}` - Failed to get path

  """
  @spec get_path(db_ref()) :: {:ok, String.t()}
  def get_path(db_ref) when is_pid(db_ref) do
    ErlangAdapter.get_path(db_ref)
  end

  @doc """
  Lists all column families in an existing database.

  ## Parameters

  - `path`: Path to the database directory

  ## Returns

  - List of column family name strings

  """
  @spec list_column_families(String.t()) :: [String.t()]
  def list_column_families(path) when is_binary(path) do
    ErlangAdapter.list_column_families(path)
  end

  @doc """
  Checks if the database is currently open.

  ## Parameters

  - `db_ref`: The database reference

  ## Returns

  - `true` - Database is open
  - `false` - Database is closed

  """
  @spec is_open(db_ref()) :: boolean()
  def is_open(db_ref) when is_pid(db_ref) do
    ErlangAdapter.is_open(db_ref)
  end

  @doc """
  Gets a value from the database.

  ## Parameters

  - `db_ref`: The database reference
  - `cf`: Column family atom
  - `key`: Binary key to look up

  ## Returns

  - `{:ok, value}` - Key found, returns the value
  - `:not_found` - Key does not exist
  - `{:error, reason}` - Error occurred

  """
  @spec get(db_ref(), column_family(), binary()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def get(db_ref, cf, key) when is_pid(db_ref) and is_atom(cf) and is_binary(key) do
    ErlangAdapter.get(db_ref, cf, key)
  end

  @doc """
  Puts a key-value pair into the database.

  ## Parameters

  - `db_ref`: The database reference
  - `cf`: Column family atom
  - `key`: Binary key
  - `value`: Binary value

  ## Returns

  - `:ok` - Value written successfully
  - `{:error, reason}` - Error occurred

  """
  @spec put(db_ref(), column_family(), binary(), binary()) :: :ok | {:error, term()}
  def put(db_ref, cf, key, value) when is_pid(db_ref) and is_atom(cf) and is_binary(key) do
    ErlangAdapter.put(db_ref, cf, key, value)
  end

  @doc """
  Deletes a key from the database.

  ## Parameters

  - `db_ref`: The database reference
  - `cf`: Column family atom
  - `key`: Binary key to delete

  ## Returns

  - `:ok` - Key deleted successfully
  - `{:error, reason}` - Error occurred

  """
  @spec delete(db_ref(), column_family(), binary()) :: :ok | {:error, term()}
  def delete(db_ref, cf, key) when is_pid(db_ref) and is_atom(cf) and is_binary(key) do
    ErlangAdapter.delete(db_ref, cf, key)
  end

  @doc """
  Checks if a key exists in the database.

  ## Parameters

  - `db_ref`: The database reference
  - `cf`: Column family atom
  - `key`: Binary key to check

  ## Returns

  - `{:ok, true}` - Key exists
  - `{:ok, false}` - Key does not exist
  - `{:error, reason}` - Error occurred

  """
  @spec exists(db_ref(), column_family(), binary()) :: {:ok, boolean()} | {:error, term()}
  def exists(db_ref, cf, key) when is_pid(db_ref) and is_atom(cf) and is_binary(key) do
    ErlangAdapter.exists(db_ref, cf, key)
  end

  @doc """
  Writes multiple put operations atomically in a batch.

  ## Parameters

  - `db_ref`: The database reference
  - `operations`: List of {cf, key, value} tuples
  - `sync`: If true, synchronously write to WAL

  ## Returns

  - `:ok` - Batch written successfully
  - `{:error, reason}` - Error occurred

  """
  @spec write_batch(db_ref(), [put_operation()], boolean()) :: :ok | {:error, term()}
  def write_batch(db_ref, operations, sync \\ false)
      when is_pid(db_ref) and is_list(operations) and is_boolean(sync) do
    ErlangAdapter.write_batch(db_ref, operations, sync)
  end

  @doc """
  Deletes multiple keys atomically in a batch.

  ## Parameters

  - `db_ref`: The database reference
  - `operations`: List of {cf, key} tuples
  - `sync`: If true, synchronously write to WAL

  ## Returns

  - `:ok` - Batch deleted successfully
  - `{:error, reason}` - Error occurred

  """
  @spec delete_batch(db_ref(), [delete_operation()], boolean()) :: :ok | {:error, term()}
  def delete_batch(db_ref, operations, sync \\ false)
      when is_pid(db_ref) and is_list(operations) and is_boolean(sync) do
    ErlangAdapter.delete_batch(db_ref, operations, sync)
  end

  @doc """
  Writes a mixed batch of put and delete operations atomically.

  ## Parameters

  - `db_ref`: The database reference
  - `operations`: List of put/delete operations
    - `{:put, cf, key, value}` - Put operation
    - `{:delete, cf, key}` - Delete operation
  - `sync`: If true, synchronously write to WAL

  ## Returns

  - `:ok` - Batch written successfully
  - `{:error, reason}` - Error occurred

  """
  @spec mixed_batch(db_ref(), [mixed_put() | mixed_delete()], boolean()) :: :ok | {:error, term()}
  def mixed_batch(db_ref, operations, sync \\ false)
      when is_pid(db_ref) and is_list(operations) and is_boolean(sync) do
    ErlangAdapter.mixed_batch(db_ref, operations, sync)
  end

  # ===========================================================================
  # Iterator Operations (Phase 2 - Section 2.1 Implemented)
  # ===========================================================================

  @doc """
  Creates a prefix iterator for a column family.

  The iterator_ref returned is a PID of the iterator wrapper process.

  ## Parameters

  - `db_ref` - The database adapter PID
  - `cf` - Column family atom
  - `prefix` - Binary prefix to iterate within

  ## Returns

  - `{:ok, iterator_ref}` - Iterator created successfully (iterator_ref is a PID)
  - `{:error, reason}` - Failed to create iterator

  """
  @spec prefix_iterator(db_ref(), column_family(), binary()) ::
          {:ok, iterator_ref()} | {:error, term()}
  def prefix_iterator(db_ref, cf, prefix)
      when is_pid(db_ref) and is_atom(cf) and is_binary(prefix) do
    ErlangAdapter.prefix_iterator(db_ref, cf, prefix)
  end

  @doc """
  Creates a prefix iterator with options.

  ## Parameters

  - `db_ref` - The database adapter PID
  - `cf` - Column family atom
  - `prefix` - Binary prefix to iterate within
  - `opts` - Iterator options

  ## Options

  - `fill_cache` - Whether to fill block cache (default: true)
  - `total_order_seek` - Use total order seek (default: false)
  - `prefix_same_as_start` - Optimize for prefix iteration (default: false)
  - `snapshot` - Use a specific snapshot (placeholder for Section 2.2)

  ## Returns

  - `{:ok, iterator_ref}` - Iterator created successfully
  - `{:error, reason}` - Failed to create iterator

  """
  @spec prefix_iterator(db_ref(), column_family(), binary(), keyword()) ::
          {:ok, iterator_ref()} | {:error, term()}
  def prefix_iterator(db_ref, cf, prefix, opts)
      when is_pid(db_ref) and is_atom(cf) and is_binary(prefix) do
    ErlangAdapter.prefix_iterator(db_ref, cf, prefix, opts)
  end

  @doc """
  Gets the next entry from an iterator.

  ## Parameters

  - `iterator_ref` - The iterator PID

  ## Returns

  - `{:ok, key, value}` - Entry found
  - `:iterator_end` - Iterator exhausted
  - `{:error, reason}` - Error occurred

  """
  @spec iterator_next(iterator_ref()) ::
          {:ok, binary(), binary()} | :iterator_end | {:error, term()}
  def iterator_next(iterator_ref) when is_pid(iterator_ref) do
    ErlangAdapter.iterator_next(iterator_ref)
  end

  @doc """
  Moves an iterator to a new position.

  ## Parameters

  - `iterator_ref` - The iterator PID
  - `action` - Movement action (:first, :last, :next, :prev, or binary seek key)

  ## Returns

  - `{:ok, key, value}` - Entry found
  - `:iterator_end` - Iterator exhausted
  - `{:error, reason}` - Error occurred

  """
  @spec iterator_move(iterator_ref(), :first | :last | :next | :prev | binary()) ::
          {:ok, binary(), binary()} | :iterator_end | {:error, term()}
  def iterator_move(iterator_ref, action) when is_pid(iterator_ref) do
    ErlangAdapter.iterator_move(iterator_ref, action)
  end

  @doc """
  Seeks an iterator to a target key.

  ## Parameters

  - `iterator_ref` - The iterator PID
  - `target` - Binary key to seek to

  ## Returns

  - `:ok` - Seek successful
  - `{:error, reason}` - Error occurred

  """
  @spec iterator_seek(iterator_ref(), binary()) :: :ok | {:error, term()}
  def iterator_seek(iterator_ref, target) when is_pid(iterator_ref) and is_binary(target) do
    ErlangAdapter.iterator_seek(iterator_ref, target)
  end

  @doc """
  Closes an iterator and releases resources.

  ## Parameters

  - `iterator_ref` - The iterator PID

  ## Returns

  - `:ok`

  """
  @spec iterator_close(iterator_ref()) :: :ok
  def iterator_close(iterator_ref) when is_pid(iterator_ref) do
    ErlangAdapter.iterator_close(iterator_ref)
  end

  @doc """
  Collects all remaining entries from an iterator.

  ## Parameters

  - `iterator_ref` - The iterator PID

  ## Returns

  - `{:ok, [{key, value}]}` - List of entries
  - `{:error, reason}` - Error occurred

  """
  @spec iterator_collect(iterator_ref()) :: {:ok, [{binary(), binary()}]} | {:error, term()}
  def iterator_collect(iterator_ref) when is_pid(iterator_ref) do
    ErlangAdapter.iterator_collect(iterator_ref)
  end

  # ===========================================================================
  # Fold Operations (Phase 2 - Section 2.3)
  # ===========================================================================

  @doc """
  Folds over a prefix range in a column family.

  This function efficiently iterates over all key-value pairs within a prefix
  range, reducing BEAM-NIF boundary crossings for better performance.

  ## Parameters

  - `db_ref`: The database reference (adapter PID)
  - `cf`: Column family atom
  - `prefix`: Binary prefix to limit the fold to
  - `acc`: Initial accumulator value
  - `fun`: Fold function: `({key, value}, acc) -> new_acc`

  ## Returns

  - `acc` - Final accumulator value

  ## Examples

      # Count all entries with a prefix
      count = NIF.fold(db, :spo, <<subject_id::64-big>>, 0, fn {_k, _v}, acc -> acc + 1 end)

      # Sum all values
      sum = NIF.fold(db, :spo, <<prefix::binary>>, 0, fn {_k, v}, acc -> acc + parse_value(v) end)

  """
  @spec fold(db_ref(), column_family(), binary(), term(), fold_fun()) :: term()
  def fold(db_ref, cf, prefix, acc, fun)
      when is_pid(db_ref) and is_atom(cf) and is_binary(prefix) and is_function(fun, 2) do
    ErlangAdapter.fold(db_ref, cf, prefix, acc, fun)
  end

  @doc """
  Folds over a prefix range with options.

  ## Options

  - `iterate_upper_bound` - Upper bound for iteration (binary)
  - `fill_cache` - Whether to fill block cache (default: true)
  - `snapshot` - Use a specific snapshot

  """
  @spec fold(db_ref(), column_family(), binary(), term(), fold_fun(), keyword()) :: term()
  def fold(db_ref, cf, prefix, acc, fun, opts)
      when is_pid(db_ref) and is_atom(cf) and is_binary(prefix) and is_function(fun, 2) do
    ErlangAdapter.fold(db_ref, cf, prefix, acc, fun, opts)
  end

  @doc """
  Folds over keys only in a prefix range.

  More efficient than `fold/5` when values are not needed.

  ## Parameters

  - `db_ref`: The database reference (adapter PID)
  - `cf`: Column family atom
  - `prefix`: Binary prefix to limit the fold to
  - `acc`: Initial accumulator value
  - `fun`: Fold function: `(key, acc) -> new_acc`

  ## Returns

  - `acc` - Final accumulator value

  ## Examples

      # Collect all keys with a prefix
      keys = NIF.fold_keys(db, :spo, <<subject_id::64-big>>, [], fn k, acc -> [k | acc] end)

  """
  @spec fold_keys(db_ref(), column_family(), binary(), term(), fold_keys_fun()) :: term()
  def fold_keys(db_ref, cf, prefix, acc, fun)
      when is_pid(db_ref) and is_atom(cf) and is_binary(prefix) and is_function(fun, 2) do
    ErlangAdapter.fold_keys(db_ref, cf, prefix, acc, fun)
  end

  @doc """
  Folds over keys only in a prefix range with options.

  ## Options

  - `iterate_upper_bound` - Upper bound for iteration (binary)
  - `fill_cache` - Whether to fill block cache (default: true)
  - `snapshot` - Use a specific snapshot

  """
  @spec fold_keys(db_ref(), column_family(), binary(), term(), fold_keys_fun(), keyword()) ::
          term()
  def fold_keys(db_ref, cf, prefix, acc, fun, opts)
      when is_pid(db_ref) and is_atom(cf) and is_binary(prefix) and is_function(fun, 2) do
    ErlangAdapter.fold_keys(db_ref, cf, prefix, acc, fun, opts)
  end

  @type fold_fun :: ({{binary(), binary()}, term()} -> term())
  @type fold_keys_fun :: (binary(), term() -> term())

  # ===========================================================================
  # Stream Operations (Phase 2 - Section 2.3)
  # ===========================================================================

  @doc """
  Creates a lazy stream over a prefix range.

  The stream properly manages resources and will close the underlying iterator
  when the stream is terminated or if an error occurs.

  ## Parameters

  - `db_ref`: The database reference (adapter PID)
  - `cf`: Column family atom
  - `prefix`: Binary prefix to iterate over

  ## Returns

  - `Enumerable.t()` - A stream of `{key, value}` tuples

  ## Examples

      # Stream all entries with a prefix
      db |> NIF.prefix_stream(:spo, <<subject_id::64-big>>) |> Enum.to_list()

      # Use with Stream functions for lazy evaluation
      db
      |> NIF.prefix_stream(:spo, <<subject_id::64-big>>)
      |> Stream.take(100)
      |> Enum.to_list()

  """
  @spec prefix_stream(db_ref(), column_family(), binary()) :: Enumerable.t()
  def prefix_stream(db_ref, cf, prefix)
      when is_pid(db_ref) and is_atom(cf) and is_binary(prefix) do
    ErlangAdapter.prefix_stream(db_ref, cf, prefix)
  end

  @doc """
  Creates a lazy stream over a prefix range with options.

  ## Options

  - `fill_cache` - Whether to fill block cache (default: true)
  - `snapshot` - Use a specific snapshot

  """
  @spec prefix_stream(db_ref(), column_family(), binary(), keyword()) :: Enumerable.t()
  def prefix_stream(db_ref, cf, prefix, opts)
      when is_pid(db_ref) and is_atom(cf) and is_binary(prefix) do
    ErlangAdapter.prefix_stream(db_ref, cf, prefix, opts)
  end

  # ===========================================================================
  # Snapshot Operations (Phase 2 - Section 2.2)
  # ===========================================================================

  @doc """
  Creates a point-in-time snapshot of the database.

  Snapshots provide consistent read-only views over the entire state of the key-value store.

  ## Parameters

  - `db_ref`: The database reference (adapter PID)

  ## Returns

  - `{:ok, snapshot_ref}` - Snapshot created successfully
  - `{:error, reason}` - Failed to create snapshot

  """
  @spec snapshot(db_ref()) :: {:ok, snapshot_ref()} | {:error, term()}
  def snapshot(db_ref) when is_pid(db_ref) do
    ErlangAdapter.snapshot(db_ref)
  end

  @doc """
  Gets a value from a snapshot.

  Reads the value as it existed when the snapshot was created.

  ## Parameters

  - `db_ref`: The database reference (adapter PID)
  - `snapshot_ref`: The snapshot reference
  - `cf`: Column family atom
  - `key`: Binary key to look up

  ## Returns

  - `{:ok, value}` - Key found in snapshot
  - `:not_found` - Key does not exist in snapshot
  - `{:error, reason}` - Error occurred

  """
  @spec snapshot_get(db_ref(), snapshot_ref(), column_family(), binary()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def snapshot_get(db_ref, snapshot_ref, cf, key)
      when is_pid(db_ref) and is_reference(snapshot_ref) and is_atom(cf) and is_binary(key) do
    ErlangAdapter.snapshot_get(db_ref, snapshot_ref, cf, key)
  end

  @doc """
  Creates a prefix iterator on a snapshot.

  The iterator will see the database state as of the snapshot creation time.

  ## Parameters

  - `db_ref`: The database reference (adapter PID)
  - `snapshot_ref`: The snapshot reference
  - `cf`: Column family atom
  - `prefix`: Binary prefix to iterate over

  ## Returns

  - `{:ok, iterator_ref}` - Iterator created successfully
  - `{:error, reason}` - Failed to create iterator

  """
  @spec snapshot_prefix_iterator(db_ref(), snapshot_ref(), column_family(), binary()) ::
          {:ok, iterator_ref()} | {:error, term()}
  def snapshot_prefix_iterator(db_ref, snapshot_ref, cf, prefix)
      when is_pid(db_ref) and is_reference(snapshot_ref) and is_atom(cf) and is_binary(prefix) do
    ErlangAdapter.snapshot_prefix_iterator(db_ref, snapshot_ref, cf, prefix)
  end

  @doc """
  Creates a prefix iterator on a snapshot with options.

  ## Parameters

  - `db_ref`: The database reference (adapter PID)
  - `snapshot_ref`: The snapshot reference
  - `cf`: Column family atom
  - `prefix`: Binary prefix to iterate over
  - `opts`: Iterator options

  ## Returns

  - `{:ok, iterator_ref}` - Iterator created successfully
  - `{:error, reason}` - Failed to create iterator

  """
  @spec snapshot_prefix_iterator(db_ref(), snapshot_ref(), column_family(), binary(), keyword()) ::
          {:ok, iterator_ref()} | {:error, term()}
  def snapshot_prefix_iterator(db_ref, snapshot_ref, cf, prefix, opts)
      when is_pid(db_ref) and is_reference(snapshot_ref) and is_atom(cf) and is_binary(prefix) do
    ErlangAdapter.snapshot_prefix_iterator(db_ref, snapshot_ref, cf, prefix, opts)
  end

  @doc """
  Gets the next entry from a snapshot iterator.

  This is the same as `iterator_next/1` - snapshot iterators use the same iterator process.

  ## Parameters

  - `iter_ref`: The iterator PID

  ## Returns

  - `{:ok, key, value}` - Entry found
  - `:iterator_end` - Iterator exhausted
  - `{:error, reason}` - Error occurred

  """
  @spec snapshot_iterator_next(iterator_ref()) ::
          {:ok, binary(), binary()} | :iterator_end | {:error, term()}
  def snapshot_iterator_next(iter_ref) when is_pid(iter_ref) do
    iterator_next(iter_ref)
  end

  @doc """
  Closes a snapshot iterator.

  This is the same as `iterator_close/1` - snapshot iterators use the same iterator process.

  ## Parameters

  - `iter_ref`: The iterator PID

  ## Returns

  - `:ok`

  """
  @spec snapshot_iterator_close(iterator_ref()) :: :ok
  def snapshot_iterator_close(iter_ref) when is_pid(iter_ref) do
    iterator_close(iter_ref)
  end

  @doc """
  Collects all remaining entries from a snapshot iterator.

  This is the same as `iterator_collect/1` - snapshot iterators use the same iterator process.

  ## Parameters

  - `iter_ref`: The iterator PID

  ## Returns

  - `{:ok, [{key, value}]}` - List of entries
  - `{:error, reason}` - Error occurred

  """
  @spec snapshot_iterator_collect(iterator_ref()) ::
          {:ok, [{binary(), binary()}]} | {:error, term()}
  def snapshot_iterator_collect(iter_ref) when is_pid(iter_ref) do
    iterator_collect(iter_ref)
  end

  @doc """
  Releases a snapshot, freeing its resources.

  ## Parameters

  - `db_ref`: The database reference (adapter PID)
  - `snapshot_ref`: The snapshot reference to release

  ## Returns

  - `:ok` - Snapshot released successfully
  - `{:error, reason}` - Failed to release snapshot

  """
  @spec release_snapshot(db_ref(), snapshot_ref()) :: :ok | {:error, term()}
  def release_snapshot(db_ref, snapshot_ref) when is_pid(db_ref) and is_reference(snapshot_ref) do
    ErlangAdapter.release_snapshot(db_ref, snapshot_ref)
  end

  @doc """
  Creates a prefix stream from a snapshot - NOT YET IMPLEMENTED.

  This will be implemented in Phase 2.3 (Fold-Based Iteration).

  """
  @spec snapshot_stream(db_ref(), snapshot_ref(), column_family(), binary()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def snapshot_stream(_db_ref, _snapshot_ref, _cf, _prefix) do
    raise("Stream operations not yet implemented - see Phase 2.3 migration plan")
  end
end
