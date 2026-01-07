defmodule TripleStore.Backend.RocksDB.NIF do
  @moduledoc """
  NIF bindings for RocksDB operations.

  This module provides the interface to RocksDB storage, implemented via
  the erlang-rocksdb C++ NIF library. The NIF module delegates to the
  ErlangAdapter GenServer which manages the database connection and
  column family handles.

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

  The NIF module is a thin wrapper around `ErlangAdapter` which:
  - Manages database lifecycle via GenServer
  - Translates column family atoms to erlang-rocksdb handles
  - Handles path validation and security
  - Translates error returns

  The `db_ref()` type is now a GenServer PID (adapter process).
  """

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
  @spec get(db_ref(), column_family(), binary()) :: {:ok, binary()} | :not_found | {:error, term()}
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
  @spec prefix_iterator(db_ref(), column_family(), binary()) :: {:ok, iterator_ref()} | {:error, term()}
  def prefix_iterator(db_ref, cf, prefix) when is_pid(db_ref) and is_atom(cf) and is_binary(prefix) do
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
  @spec prefix_iterator(db_ref(), column_family(), binary(), keyword()) :: {:ok, iterator_ref()} | {:error, term()}
  def prefix_iterator(db_ref, cf, prefix, opts) when is_pid(db_ref) and is_atom(cf) and is_binary(prefix) do
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
  @spec iterator_next(iterator_ref()) :: {:ok, binary(), binary()} | :iterator_end | {:error, term()}
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
  @spec iterator_move(iterator_ref(), :first | :last | :next | :prev | binary()) :: {:ok, binary(), binary()} | :iterator_end | {:error, term()}
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

  @doc """
  Creates a prefix stream - NOT YET IMPLEMENTED.

  This will be implemented in Phase 2.3 (Fold-Based Iteration).
  """
  @spec prefix_stream(db_ref(), column_family(), binary()) :: {:ok, Enumerable.t()} | {:error, term()}
  def prefix_stream(_db_ref, _cf, _prefix) do
    raise("Stream operations not yet implemented - see Phase 2.3 migration plan")
  end

  # ===========================================================================
  # Snapshot Operations (Phase 2 - Not Yet Implemented)
  # ===========================================================================

  @doc """
  Creates a snapshot - NOT YET IMPLEMENTED.

  This will be implemented in Phase 2 of the migration.
  """
  @spec snapshot(db_ref()) :: {:ok, snapshot_ref()} | {:error, term()}
  def snapshot(_db_ref) do
    raise("Snapshot operations not yet implemented - see Phase 2 migration plan")
  end

  @doc """
  Gets a value from a snapshot - NOT YET IMPLEMENTED.

  This will be implemented in Phase 2 of the migration.
  """
  @spec snapshot_get(snapshot_ref(), column_family(), binary()) :: {:ok, binary()} | :not_found | {:error, term()}
  def snapshot_get(_snapshot_ref, _cf, _key) do
    raise("Snapshot operations not yet implemented - see Phase 2 migration plan")
  end

  @doc """
  Creates a prefix iterator on a snapshot - NOT YET IMPLEMENTED.

  This will be implemented in Phase 2 of the migration.
  """
  @spec snapshot_prefix_iterator(snapshot_ref(), column_family(), binary()) ::
          {:ok, snapshot_iterator_ref()} | {:error, term()}
  def snapshot_prefix_iterator(_snapshot_ref, _cf, _prefix) do
    raise("Snapshot operations not yet implemented - see Phase 2 migration plan")
  end

  @doc """
  Gets the next entry from a snapshot iterator - NOT YET IMPLEMENTED.

  This will be implemented in Phase 2 of the migration.
  """
  @spec snapshot_iterator_next(snapshot_iterator_ref()) ::
          {:ok, binary(), binary()} | :iterator_end | {:error, term()}
  def snapshot_iterator_next(_iter_ref) do
    raise("Snapshot operations not yet implemented - see Phase 2 migration plan")
  end

  @doc """
  Closes a snapshot iterator - NOT YET IMPLEMENTED.

  This will be implemented in Phase 2 of the migration.
  """
  @spec snapshot_iterator_close(snapshot_iterator_ref()) :: :ok | {:error, term()}
  def snapshot_iterator_close(_iter_ref) do
    raise("Snapshot operations not yet implemented - see Phase 2 migration plan")
  end

  @doc """
  Collects all remaining entries from a snapshot iterator - NOT YET IMPLEMENTED.

  This will be implemented in Phase 2 of the migration.
  """
  @spec snapshot_iterator_collect(snapshot_iterator_ref()) :: {:ok, [{binary(), binary()}]} | {:error, term()}
  def snapshot_iterator_collect(_iter_ref) do
    raise("Snapshot operations not yet implemented - see Phase 2 migration plan")
  end

  @doc """
  Releases a snapshot - NOT YET IMPLEMENTED.

  This will be implemented in Phase 2 of the migration.
  """
  @spec release_snapshot(snapshot_ref()) :: :ok | {:error, term()}
  def release_snapshot(_snapshot_ref) do
    raise("Snapshot operations not yet implemented - see Phase 2 migration plan")
  end

  @doc """
  Creates a prefix stream from a snapshot - NOT YET IMPLEMENTED.

  This will be implemented in Phase 2 of the migration.
  """
  @spec snapshot_stream(snapshot_ref(), column_family(), binary()) :: {:ok, Enumerable.t()} | {:error, term()}
  def snapshot_stream(_snapshot_ref, _cf, _prefix) do
    raise("Snapshot operations not yet implemented - see Phase 2 migration plan")
  end
end
