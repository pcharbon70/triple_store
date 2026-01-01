defmodule TripleStore.Backend.RocksDB.NIF do
  @moduledoc """
  NIF bindings for RocksDB operations.

  This module contains the low-level NIF function declarations that interface
  with the Rust implementation in `native/rocksdb_nif`.

  ## Usage

  These functions should not be called directly. Use the higher-level
  `TripleStore.Backend.RocksDB` module instead.

  ## Configuration

  To skip NIF compilation during development (when Rust is not installed),
  set the environment variable `RUSTLER_SKIP_COMPILATION=1`.

  ## Column Families

  The database uses the following column families:
  - `:id2str` - Maps 64-bit IDs to string values
  - `:str2id` - Maps string values to 64-bit IDs
  - `:spo` - Subject-Predicate-Object index
  - `:pos` - Predicate-Object-Subject index
  - `:osp` - Object-Subject-Predicate index
  - `:derived` - Stores inferred triples from reasoning
  """

  @skip_compilation System.get_env("RUSTLER_SKIP_COMPILATION") == "1"

  use Rustler,
    otp_app: :triple_store,
    crate: "rocksdb_nif",
    skip_compilation?: @skip_compilation

  @type db_ref :: reference()
  @type column_family :: :id2str | :str2id | :spo | :pos | :osp | :derived

  @doc """
  Verifies that the NIF is loaded correctly.

  Returns `"rocksdb_nif"` if the NIF is operational.

  ## Examples

      iex> TripleStore.Backend.RocksDB.NIF.nif_loaded()
      "rocksdb_nif"

  """
  @spec nif_loaded :: String.t()
  def nif_loaded, do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Opens a RocksDB database at the given path.

  Creates the database and all required column families if they don't exist.
  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `path` - Path to the database directory

  ## Returns
  - `{:ok, db_ref}` on success
  - `{:error, {:open_failed, reason}}` on failure

  ## Examples

      iex> {:ok, db} = TripleStore.Backend.RocksDB.NIF.open("/tmp/test_db")
      iex> is_reference(db)
      true

  """
  @spec open(String.t()) :: {:ok, db_ref()} | {:error, {:open_failed, String.t()}}
  def open(_path), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the database and releases all resources.

  After calling close, the database handle is no longer valid.
  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `db_ref` - The database reference to close

  ## Returns
  - `:ok` on success
  - `{:error, :already_closed}` if already closed

  ## Examples

      iex> {:ok, db} = TripleStore.Backend.RocksDB.NIF.open("/tmp/test_db")
      iex> TripleStore.Backend.RocksDB.NIF.close(db)
      :ok

  """
  @spec close(db_ref()) :: :ok | {:error, :already_closed}
  def close(_db_ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Flushes the Write-Ahead Log (WAL) to disk.

  This ensures all buffered writes are persisted to the WAL. When `sync` is `true`,
  it also calls fsync to ensure data is physically written to storage.

  Use this after bulk loading with `sync: false` to ensure all data is durable
  before considering the load complete.

  ## Arguments
  - `db_ref` - The database reference
  - `sync` - When `true`, calls fsync after flushing (fully durable). When `false`,
    only flushes to OS buffer cache.

  ## Returns
  - `:ok` on success
  - `{:error, :already_closed}` if database is closed
  - `{:error, {:flush_failed, reason}}` on failure

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> NIF.write_batch(db, [{:spo, "key", "value"}], false)
      :ok
      iex> NIF.flush_wal(db, true)
      :ok

  """
  @spec flush_wal(db_ref(), boolean()) :: :ok | {:error, term()}
  def flush_wal(_db_ref, _sync), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Returns the path of the database.

  ## Arguments
  - `db_ref` - The database reference

  ## Returns
  - `{:ok, path}` with the database path
  """
  @spec get_path(db_ref()) :: {:ok, String.t()}
  def get_path(_db_ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Lists all column families in the database.

  ## Returns
  - List of column family atoms: `[:id2str, :str2id, :spo, :pos, :osp, :derived]`
  """
  @spec list_column_families :: [column_family()]
  def list_column_families, do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Checks if the database is open.

  ## Arguments
  - `db_ref` - The database reference

  ## Returns
  - `true` if open, `false` if closed
  """
  @spec is_open(db_ref()) :: boolean()
  # credo:disable-for-next-line Credo.Check.Readability.PredicateFunctionNames
  def is_open(_db_ref), do: :erlang.nif_error(:nif_not_loaded)

  # ============================================================================
  # Key-Value Operations
  # ============================================================================

  @doc """
  Gets a value from a column family.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `db_ref` - The database reference
  - `cf` - The column family atom
  - `key` - The key as a binary

  ## Returns
  - `{:ok, value}` if found
  - `:not_found` if key doesn't exist
  - `{:error, :already_closed}` if database is closed
  - `{:error, {:invalid_cf, cf}}` if column family is invalid
  - `{:error, {:get_failed, reason}}` on other errors

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> NIF.put(db, :id2str, "key1", "value1")
      :ok
      iex> NIF.get(db, :id2str, "key1")
      {:ok, "value1"}
      iex> NIF.get(db, :id2str, "nonexistent")
      :not_found

  """
  @spec get(db_ref(), column_family(), binary()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def get(_db_ref, _cf, _key), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Puts a key-value pair into a column family.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `db_ref` - The database reference
  - `cf` - The column family atom
  - `key` - The key as a binary
  - `value` - The value as a binary

  ## Returns
  - `:ok` on success
  - `{:error, :already_closed}` if database is closed
  - `{:error, {:invalid_cf, cf}}` if column family is invalid
  - `{:error, {:put_failed, reason}}` on other errors

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> NIF.put(db, :id2str, "key1", "value1")
      :ok

  """
  @spec put(db_ref(), column_family(), binary(), binary()) :: :ok | {:error, term()}
  def put(_db_ref, _cf, _key, _value), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Deletes a key from a column family.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `db_ref` - The database reference
  - `cf` - The column family atom
  - `key` - The key to delete

  ## Returns
  - `:ok` on success (even if key didn't exist)
  - `{:error, :already_closed}` if database is closed
  - `{:error, {:invalid_cf, cf}}` if column family is invalid
  - `{:error, {:delete_failed, reason}}` on other errors

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> NIF.put(db, :id2str, "key1", "value1")
      :ok
      iex> NIF.delete(db, :id2str, "key1")
      :ok
      iex> NIF.get(db, :id2str, "key1")
      :not_found

  """
  @spec delete(db_ref(), column_family(), binary()) :: :ok | {:error, term()}
  def delete(_db_ref, _cf, _key), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Checks if a key exists in a column family.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.
  More efficient than `get/3` when you only need to check existence.

  ## Arguments
  - `db_ref` - The database reference
  - `cf` - The column family atom
  - `key` - The key to check

  ## Returns
  - `{:ok, true}` if key exists
  - `{:ok, false}` if key doesn't exist
  - `{:error, :already_closed}` if database is closed
  - `{:error, {:invalid_cf, cf}}` if column family is invalid
  - `{:error, {:get_failed, reason}}` on other errors

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> NIF.put(db, :id2str, "key1", "value1")
      :ok
      iex> NIF.exists(db, :id2str, "key1")
      {:ok, true}
      iex> NIF.exists(db, :id2str, "nonexistent")
      {:ok, false}

  """
  @spec exists(db_ref(), column_family(), binary()) :: {:ok, boolean()} | {:error, term()}
  def exists(_db_ref, _cf, _key), do: :erlang.nif_error(:nif_not_loaded)

  # ============================================================================
  # Batch Operations
  # ============================================================================

  @type put_operation :: {column_family(), binary(), binary()}
  @type delete_operation :: {column_family(), binary()}
  @type mixed_put :: {:put, column_family(), binary(), binary()}
  @type mixed_delete :: {:delete, column_family(), binary()}

  @doc """
  Atomically writes multiple key-value pairs to column families.

  Uses RocksDB WriteBatch for atomic commit - either all operations succeed
  or none do. Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `db_ref` - The database reference
  - `operations` - List of `{cf, key, value}` tuples
  - `sync` - When `true`, forces an fsync after the write. When `false`,
    the write is buffered in the OS (WAL still provides durability).
    Use `false` for bulk loading to improve performance.

  ## Returns
  - `:ok` on success
  - `{:error, :already_closed}` if database is closed
  - `{:error, {:invalid_cf, cf}}` if column family is invalid
  - `{:error, {:batch_failed, reason}}` on other errors

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> operations = [
      ...>   {:id2str, "key1", "value1"},
      ...>   {:id2str, "key2", "value2"},
      ...>   {:str2id, "value1", "key1"}
      ...> ]
      iex> NIF.write_batch(db, operations, true)
      :ok

  """
  @spec write_batch(db_ref(), [put_operation()], boolean()) :: :ok | {:error, term()}
  def write_batch(_db_ref, _operations, _sync), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Atomically deletes multiple keys from column families.

  Uses RocksDB WriteBatch for atomic commit - either all operations succeed
  or none do. Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `db_ref` - The database reference
  - `operations` - List of `{cf, key}` tuples
  - `sync` - When `true`, forces an fsync after the write. When `false`,
    the write is buffered in the OS (WAL still provides durability).
    Use `false` for bulk loading to improve performance.

  ## Returns
  - `:ok` on success
  - `{:error, :already_closed}` if database is closed
  - `{:error, {:invalid_cf, cf}}` if column family is invalid
  - `{:error, {:batch_failed, reason}}` on other errors

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> NIF.write_batch(db, [{:id2str, "key1", "value1"}, {:id2str, "key2", "value2"}], true)
      :ok
      iex> NIF.delete_batch(db, [{:id2str, "key1"}, {:id2str, "key2"}], true)
      :ok

  """
  @spec delete_batch(db_ref(), [delete_operation()], boolean()) :: :ok | {:error, term()}
  def delete_batch(_db_ref, _operations, _sync), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Atomically performs mixed put and delete operations.

  Uses RocksDB WriteBatch for atomic commit - either all operations succeed
  or none do. This is essential for maintaining consistency when updating
  multiple indices (SPO, POS, OSP) for a single triple.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `db_ref` - The database reference
  - `operations` - List of operations:
    - `{:put, cf, key, value}` for puts
    - `{:delete, cf, key}` for deletes
  - `sync` - When `true`, forces an fsync after the write. When `false`,
    the write is buffered in the OS (WAL still provides durability).
    Use `false` for bulk loading to improve performance.

  ## Returns
  - `:ok` on success
  - `{:error, :already_closed}` if database is closed
  - `{:error, {:invalid_cf, cf}}` if column family is invalid
  - `{:error, {:invalid_operation, op}}` if operation type is invalid
  - `{:error, {:batch_failed, reason}}` on other errors

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> operations = [
      ...>   {:put, :spo, "s1p1o1", ""},
      ...>   {:put, :pos, "p1o1s1", ""},
      ...>   {:delete, :spo, "old_key"},
      ...>   {:delete, :pos, "old_key2"}
      ...> ]
      iex> NIF.mixed_batch(db, operations, true)
      :ok

  """
  @spec mixed_batch(db_ref(), [mixed_put() | mixed_delete()], boolean()) :: :ok | {:error, term()}
  def mixed_batch(_db_ref, _operations, _sync), do: :erlang.nif_error(:nif_not_loaded)

  # ============================================================================
  # Iterator Operations
  # ============================================================================

  @type iterator_ref :: reference()

  @doc """
  Creates a prefix iterator for a column family.

  The iterator returns all key-value pairs where the key starts with the given prefix.
  The iterator must be closed with `iterator_close/1` when done, or it will be
  automatically closed when garbage collected.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `db_ref` - The database reference
  - `cf` - The column family atom
  - `prefix` - The prefix to iterate over (can be empty for full scan)

  ## Returns
  - `{:ok, iterator_ref}` on success
  - `{:error, :already_closed}` if database is closed
  - `{:error, {:invalid_cf, cf}}` if column family is invalid

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> NIF.put(db, :spo, "s1p1o1", "")
      iex> {:ok, iter} = NIF.prefix_iterator(db, :spo, "s1")
      iex> {:ok, key, _value} = NIF.iterator_next(iter)
      iex> key
      "s1p1o1"

  """
  @spec prefix_iterator(db_ref(), column_family(), binary()) ::
          {:ok, iterator_ref()} | {:error, term()}
  def prefix_iterator(_db_ref, _cf, _prefix), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Gets the next key-value pair from the iterator.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `iter_ref` - The iterator reference

  ## Returns
  - `{:ok, key, value}` if there's a next item with matching prefix
  - `:iterator_end` if the iterator is exhausted or prefix no longer matches
  - `{:error, :iterator_closed}` if iterator was closed
  - `{:error, {:iterator_failed, reason}}` on error

  ## Examples

      iex> {:ok, iter} = NIF.prefix_iterator(db, :spo, "s1")
      iex> {:ok, key, value} = NIF.iterator_next(iter)
      iex> NIF.iterator_next(iter)
      :iterator_end

  """
  @spec iterator_next(iterator_ref()) ::
          {:ok, binary(), binary()} | :iterator_end | {:error, term()}
  def iterator_next(_iter_ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Seeks the iterator to a specific key.

  After seeking, the iterator will return keys >= target that match the prefix.
  This is essential for Leapfrog Triejoin in Phase 3.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `iter_ref` - The iterator reference
  - `target` - The key to seek to

  ## Returns
  - `:ok` on success
  - `{:error, :iterator_closed}` if iterator was closed
  - `{:error, :already_closed}` if database was closed

  ## Examples

      iex> {:ok, iter} = NIF.prefix_iterator(db, :spo, "")
      iex> NIF.iterator_seek(iter, "s2")
      :ok
      iex> {:ok, key, _value} = NIF.iterator_next(iter)
      iex> key >= "s2"
      true

  """
  @spec iterator_seek(iterator_ref(), binary()) :: :ok | {:error, term()}
  def iterator_seek(_iter_ref, _target), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes the iterator and releases resources.

  ## Arguments
  - `iter_ref` - The iterator reference

  ## Returns
  - `:ok` on success
  - `{:error, :iterator_closed}` if already closed

  ## Examples

      iex> {:ok, iter} = NIF.prefix_iterator(db, :spo, "s1")
      iex> NIF.iterator_close(iter)
      :ok
      iex> NIF.iterator_close(iter)
      {:error, :iterator_closed}

  """
  @spec iterator_close(iterator_ref()) :: :ok | {:error, :iterator_closed}
  def iterator_close(_iter_ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Collects all remaining key-value pairs from an iterator into a list.

  This is a convenience function that returns all matching entries.
  Useful for small result sets where streaming isn't needed.
  The iterator position advances to the end after this call.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `iter_ref` - The iterator reference

  ## Returns
  - `{:ok, [{key, value}, ...]}` with all remaining entries
  - `{:error, :iterator_closed}` if iterator was closed
  - `{:error, {:iterator_failed, reason}}` on error

  ## Examples

      iex> {:ok, iter} = NIF.prefix_iterator(db, :spo, "s1")
      iex> {:ok, results} = NIF.iterator_collect(iter)
      iex> length(results)
      3

  """
  @spec iterator_collect(iterator_ref()) ::
          {:ok, [{binary(), binary()}]} | {:error, term()}
  def iterator_collect(_iter_ref), do: :erlang.nif_error(:nif_not_loaded)

  # ============================================================================
  # Stream Wrapper
  # ============================================================================

  @doc """
  Creates an Elixir Stream from a prefix iterator.

  This wraps the iterator in a lazy Stream that automatically handles
  iteration and cleanup. The iterator is closed when the stream is
  fully consumed or when the stream is garbage collected.

  ## Arguments
  - `db_ref` - The database reference
  - `cf` - The column family atom
  - `prefix` - The prefix to iterate over (can be empty for full scan)

  ## Returns
  - `{:ok, Stream.t()}` on success
  - `{:error, term()}` on failure

  ## Examples

      iex> {:ok, stream} = NIF.prefix_stream(db, :spo, "s1")
      iex> Enum.take(stream, 5)
      [{"s1p1o1", ""}, {"s1p1o2", ""}, ...]

  """
  @spec prefix_stream(db_ref(), column_family(), binary()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def prefix_stream(db_ref, cf, prefix) do
    case prefix_iterator(db_ref, cf, prefix) do
      {:ok, iter} ->
        stream =
          Stream.resource(
            fn -> iter end,
            &stream_next/1,
            fn iter -> iterator_close(iter) end
          )

        {:ok, stream}

      error ->
        error
    end
  end

  defp stream_next(iter) do
    case iterator_next(iter) do
      {:ok, key, value} -> {[{key, value}], iter}
      :iterator_end -> {:halt, iter}
      {:error, _} -> {:halt, iter}
    end
  end

  # ============================================================================
  # Snapshot Operations
  # ============================================================================

  @type snapshot_ref :: reference()
  @type snapshot_iterator_ref :: reference()

  @doc """
  Creates a snapshot of the database for consistent point-in-time reads.

  A snapshot provides a consistent view of the database at the time of creation.
  All reads using the snapshot will see the same data, regardless of subsequent
  writes to the database. This is essential for transaction isolation.

  The snapshot must be released with `release_snapshot/1` when done, or it will
  be automatically released when garbage collected.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `db_ref` - The database reference

  ## Returns
  - `{:ok, snapshot_ref}` on success
  - `{:error, :already_closed}` if database is closed

  ## Examples

      iex> {:ok, db} = NIF.open("/tmp/test_db")
      iex> NIF.put(db, :spo, "key1", "value1")
      iex> {:ok, snap} = NIF.snapshot(db)
      iex> NIF.put(db, :spo, "key1", "value2")  # Update after snapshot
      iex> NIF.snapshot_get(snap, :spo, "key1")
      {:ok, "value1"}  # Still sees old value

  """
  @spec snapshot(db_ref()) :: {:ok, snapshot_ref()} | {:error, term()}
  def snapshot(_db_ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Gets a value from a column family using a snapshot.

  This provides point-in-time consistent reads - the value returned
  is what existed at the time the snapshot was created.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `snapshot_ref` - The snapshot reference
  - `cf` - The column family atom
  - `key` - The key as a binary

  ## Returns
  - `{:ok, value}` if found
  - `:not_found` if key doesn't exist at snapshot time
  - `{:error, :snapshot_released}` if snapshot was released
  - `{:error, {:invalid_cf, cf}}` if column family is invalid
  - `{:error, {:get_failed, reason}}` on other errors

  ## Examples

      iex> {:ok, snap} = NIF.snapshot(db)
      iex> NIF.snapshot_get(snap, :spo, "key1")
      {:ok, "value1"}

  """
  @spec snapshot_get(snapshot_ref(), column_family(), binary()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def snapshot_get(_snapshot_ref, _cf, _key), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Creates a prefix iterator over a snapshot.

  The iterator returns all key-value pairs where the key starts with the given prefix,
  using the consistent view from the snapshot.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `snapshot_ref` - The snapshot reference
  - `cf` - The column family atom
  - `prefix` - The prefix to iterate over (can be empty for full scan)

  ## Returns
  - `{:ok, iterator_ref}` on success
  - `{:error, :snapshot_released}` if snapshot was released
  - `{:error, {:invalid_cf, cf}}` if column family is invalid

  ## Examples

      iex> {:ok, snap} = NIF.snapshot(db)
      iex> {:ok, iter} = NIF.snapshot_prefix_iterator(snap, :spo, "s1")
      iex> {:ok, key, value} = NIF.snapshot_iterator_next(iter)

  """
  @spec snapshot_prefix_iterator(snapshot_ref(), column_family(), binary()) ::
          {:ok, snapshot_iterator_ref()} | {:error, term()}
  def snapshot_prefix_iterator(_snapshot_ref, _cf, _prefix),
    do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Gets the next key-value pair from a snapshot iterator.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `iter_ref` - The snapshot iterator reference

  ## Returns
  - `{:ok, key, value}` if there's a next item with matching prefix
  - `:iterator_end` if the iterator is exhausted or prefix no longer matches
  - `{:error, :iterator_closed}` if iterator was closed
  - `{:error, {:iterator_failed, reason}}` on error

  """
  @spec snapshot_iterator_next(snapshot_iterator_ref()) ::
          {:ok, binary(), binary()} | :iterator_end | {:error, term()}
  def snapshot_iterator_next(_iter_ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Closes a snapshot iterator and releases resources.

  ## Arguments
  - `iter_ref` - The snapshot iterator reference

  ## Returns
  - `:ok` on success
  - `{:error, :iterator_closed}` if already closed

  """
  @spec snapshot_iterator_close(snapshot_iterator_ref()) :: :ok | {:error, :iterator_closed}
  def snapshot_iterator_close(_iter_ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Collects all remaining key-value pairs from a snapshot iterator into a list.

  Uses dirty CPU scheduler to prevent blocking BEAM schedulers.

  ## Arguments
  - `iter_ref` - The snapshot iterator reference

  ## Returns
  - `{:ok, [{key, value}, ...]}` with all remaining entries
  - `{:error, :iterator_closed}` if iterator was closed
  - `{:error, {:iterator_failed, reason}}` on error

  """
  @spec snapshot_iterator_collect(snapshot_iterator_ref()) ::
          {:ok, [{binary(), binary()}]} | {:error, term()}
  def snapshot_iterator_collect(_iter_ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Releases a snapshot and frees resources.

  After calling release, the snapshot handle is no longer valid.

  ## Arguments
  - `snapshot_ref` - The snapshot reference

  ## Returns
  - `:ok` on success
  - `{:error, :snapshot_released}` if already released

  ## Examples

      iex> {:ok, snap} = NIF.snapshot(db)
      iex> NIF.release_snapshot(snap)
      :ok
      iex> NIF.release_snapshot(snap)
      {:error, :snapshot_released}

  """
  @spec release_snapshot(snapshot_ref()) :: :ok | {:error, :snapshot_released}
  def release_snapshot(_snapshot_ref), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Creates an Elixir Stream from a snapshot prefix iterator.

  This wraps the snapshot iterator in a lazy Stream that automatically handles
  iteration and cleanup. The iterator is closed when the stream is fully consumed.

  ## Arguments
  - `snapshot_ref` - The snapshot reference
  - `cf` - The column family atom
  - `prefix` - The prefix to iterate over (can be empty for full scan)

  ## Returns
  - `{:ok, Stream.t()}` on success
  - `{:error, term()}` on failure

  ## Examples

      iex> {:ok, snap} = NIF.snapshot(db)
      iex> {:ok, stream} = NIF.snapshot_stream(snap, :spo, "s1")
      iex> Enum.to_list(stream)
      [{"s1p1o1", ""}, {"s1p1o2", ""}]

  """
  @spec snapshot_stream(snapshot_ref(), column_family(), binary()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def snapshot_stream(snapshot_ref, cf, prefix) do
    case snapshot_prefix_iterator(snapshot_ref, cf, prefix) do
      {:ok, iter} ->
        stream =
          Stream.resource(
            fn -> iter end,
            &snapshot_stream_next/1,
            fn iter -> snapshot_iterator_close(iter) end
          )

        {:ok, stream}

      error ->
        error
    end
  end

  defp snapshot_stream_next(iter) do
    case snapshot_iterator_next(iter) do
      {:ok, key, value} -> {[{key, value}], iter}
      :iterator_end -> {:halt, iter}
      {:error, _} -> {:halt, iter}
    end
  end
end
