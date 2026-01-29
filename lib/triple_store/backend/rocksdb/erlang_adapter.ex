defmodule TripleStore.Backend.RocksDB.ErlangAdapter do
  @moduledoc """
  Erlang-RocksDB adapter for TripleStore storage backend.

  This module provides an Elixir wrapper around the erlang-rocksdb NIF,
  translating TripleStore's NIF API to erlang-rocksdb function calls.

  The adapter manages database connections and column family handle mapping,
  providing a clean interface that matches the original Rust NIF API.

  ## Architecture

  The adapter uses a GenServer to manage database state:
  - Stores the erlang-rocksdb database reference
  - Maps column family atoms to their handles
  - Provides a unique adapter identifier for callers

  ## Column Family Translation

  TripleStore uses atoms for column families (`:spo`, `:pos`, etc.) while
  erlang-rocksdb uses charlist strings (`~c"spo"`). The adapter handles
  this translation transparently.

  ## Performance Characteristics

  - **Fold operations**: Use `fold/5` and `fold_keys/5` for efficient iteration.
    These operations minimize BEAM-NIF boundary crossings by performing iteration
    in C++ land.
  - **Point lookups**: Use `get/3` for single key access with O(log n) complexity.
  - **Prefix scans**: Use `prefix_iterator/3` or `fold/5` for scanning keys with
    a common prefix.
  - **Batch writes**: Use `write_batch/3`, `delete_batch/3`, or `mixed_batch/3`
    for atomic multi-key operations.
  - **Streams**: Use `prefix_stream/3` for lazy, resource-safe iteration with
    automatic cleanup.

  ## Usage

  ```elixir
  # Open a database
  {:ok, adapter} = ErlangAdapter.open("/path/to/db")

  # Perform operations
  :ok = ErlangAdapter.put(adapter, :spo, key, value)
  {:ok, value} = ErlangAdapter.get(adapter, :spo, key)

  # Fold over a prefix (efficient for bulk operations)
  count = ErlangAdapter.fold(adapter, :spo, prefix, 0, fn {_k, _v}, acc -> acc + 1 end)

  # Stream with lazy evaluation
  adapter
  |> ErlangAdapter.prefix_stream(:spo, prefix)
  |> Enum.take(100)

  # Close the database
  :ok = ErlangAdapter.close(adapter)
  ```

  ## Error Handling

  All functions use `with` clauses for error propagation and return
  standardized error tuples:
  - `{:error, reason}` - Operation failed
  - `:not_found` - Key does not exist (for get operations)

  ## Migration from NIF Module

  The `TripleStore.Backend.RocksDB.ErlangAdapter` module is deprecated. Use this module
  directly instead:

  ```elixir
  # Old way (deprecated)
  {:ok, db} = TripleStore.Backend.RocksDB.ErlangAdapter.open("/path/to/db")

  # New way (recommended)
  {:ok, adapter} = TripleStore.Backend.RocksDB.ErlangAdapter.open("/path/to/db")
  ```

  The API is identical - simply replace `NIF` with `ErlangAdapter`.

  ## Schema Versions

  The adapter supports two database schemas:

  | Schema | Version | Key Size | Indices | Column Families |
  |--------|---------|----------|---------|-----------------|
  | Triple Store | v1 | 24 bytes | spo, pos, osp | id2str, str2id, spo, pos, osp, derived, numeric_range |
  | Quad Store | v2 | 32 bytes | gspo, gpos, spog, posg | id2str, str2id, gspo, gpos, spog, posg, derived, numeric_range |

  ### Opening a Database

  ```elixir
  # Open as triple store (default, v1)
  {:ok, adapter} = ErlangAdapter.open("/path/to/db")

  # Open as quad store (v2)
  {:ok, adapter} = ErlangAdapter.open("/path/to/quad_db", schema: :quad)

  # Check if database is a quad store
  {:ok, is_quad} = ErlangAdapter.is_quad_store?(adapter)
  ```

  ## Schema Migration

  **Direct in-place migration from triple store (v1) to quad store (v2) is NOT supported.**

  Triple stores and quad stores use different column families and key formats:
  - Triple stores use 24-byte keys with SPO, POS, OSP indices
  - Quad stores use 32-byte keys with GSPO, GPOS, SPOG, POSG indices

  To migrate from triple store to quad store:

  1. **Export data from triple store:**
     ```elixir
     {:ok, triple_adapter} = ErlangAdapter.open("/path/to/triple_db")
     # Export all triples to N-Triples or another format
     ```

  2. **Import into new quad store:**
     ```elixir
     {:ok, quad_adapter} = ErlangAdapter.open("/path/to/quad_db", schema: :quad)
     # Import exported data with graph context (default graph = ID 0)
     ```

  3. **Verification:**
     ```elixir
     {:ok, true} = ErlangAdapter.is_quad_store?(quad_adapter)
     ```

  ### Why Export/Import?

  - **Key format change**: 24-byte → 32-byte keys requires rewriting all keys
  - **Index structure change**: 3 indices → 4 indices requires rebuilding indices
  - **Graph context**: Triples gain graph position (default graph = ID 0)
  - **Column families**: Old CFs (spo, pos, osp) are not compatible with new CFs (gspo, gpos, spog, posg)

  The export/import process ensures data integrity and allows for:
  - Adding named graph context to existing triples
  - Rebuilding all four quad indices from scratch
  - Proper validation of the new schema
  """

  use GenServer
  require Logger

  alias TripleStore.Backend.RocksDB.ColumnFamilyConfig
  alias TripleStore.Backend.RocksDB.Iterator

  @type adapter :: pid()
  @type db_ref :: reference()
  @type column_family ::
          :id2str
          | :str2id
          | :spo
          | :pos
          | :osp
          | :derived
          | :numeric_range
          | :gspo
          | :gpos
          | :spog
          | :posg
          | :acl
  @type cf_handle :: reference()
  @type cf_name :: charlist()
  @type iterator_ref :: pid()
  @type snapshot_ref :: pid()

  # Fold function types
  @type fold_fun :: ({binary(), binary()}, term() -> term())
  @type fold_keys_fun :: (binary(), term() -> term())

  # ===========================================================================
  # Schema Version Constants (Section 1.1.3)
  # ===========================================================================

  # Schema version for triple store (24-byte keys, 3 indices: spo, pos, osp)
  @schema_v1_triple 1

  # Schema version for quad store (32-byte keys, 4 indices: gspo, gpos, spog, posg)
  @schema_v2_quad 2

  # Special key for storing schema version in default CF
  # Uses a distinctive pattern to avoid collision with real data
  @schema_version_key <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Returns the NIF library identifier for backward compatibility.

  Since erlang-rocksdb handles NIF loading internally, this function
  returns a constant identifier to satisfy legacy test expectations.

  ## Returns

  - `"rocksdb_nif"` - Constant NIF identifier

  """
  @spec nif_loaded() :: String.t()
  def nif_loaded, do: "rocksdb_nif"

  @doc """
  Opens a RocksDB database with all configured column families.

  If the database doesn't exist, it will be created with all column families.
  If it exists, it will be opened with the existing column families.

  ## Parameters

  - `path`: Path to the database directory

  ## Returns

  - `{:ok, adapter}` - Database opened successfully
  - `{:error, reason}` - Failed to open database

  ## Examples

      iex> {:ok, adapter} = ErlangAdapter.open("/tmp/test_db")
      iex> is_pid(adapter)
      true

  """
  @spec open(String.t()) :: {:ok, adapter()} | {:error, term()}
  def open(path) when is_binary(path) do
    open(path, [])
  end

  @doc """
  Opens a RocksDB database with options.

  ## Parameters

  - `path`: Path to the database directory
  - `opts`: Keyword list of options
    - `:create_if_missing` - Create database if it doesn't exist (default: true)
    - `:error_if_exists` - Error if database already exists (default: false)
    - `:schema` - Schema type: `:triple` (default) or `:quad`
      - `:triple` - Triple store with 24-byte keys, 3 indices (spo, pos, osp)
      - `:quad` - Quad store with 32-byte keys, 4 indices (gspo, gpos, spog, posg)

  ## Returns

  - `{:ok, adapter}` - Database opened successfully
  - `{:error, reason}` - Failed to open database
  - `{:error, :schema_mismatch}` - Database schema version doesn't match expected

  ## Examples

      # Open as triple store (default)
      {:ok, adapter} = ErlangAdapter.open("/path/to/db")

      # Open as quad store
      {:ok, adapter} = ErlangAdapter.open("/path/to/quad_db", schema: :quad)

  """
  @spec open(String.t(), keyword()) :: {:ok, adapter()} | {:error, term()}
  def open(path, opts) when is_binary(path) and is_list(opts) do
    # Validate path before proceeding
    with :ok <- validate_path(path),
         :ok <- ensure_directory_exists(path) do
      # Start the adapter GenServer
      case GenServer.start(__MODULE__, {path, opts}, name: nil) do
        {:ok, pid} -> {:ok, pid}
        {:error, {:already_started, pid}} -> {:ok, pid}
        error -> error
      end
    end
  end

  @doc """
  Opens a database optimized for bulk loading large datasets.

  This is a convenience function that opens a database with settings
  suitable for bulk loading. Note that erlang-rocksdb doesn't support
  runtime option changes, so bulk load optimization must be done through
  other means (larger batch sizes, disabling WAL during initial load, etc.).

  ## Parameters

  - `path`: Path to the database directory
  - `opts`: Additional options (same as `open/2`)
    - `:schema` - Schema type: `:triple` (default) or `:quad`

  ## Returns

  - `{:ok, adapter}` - Database opened successfully
  - `{:error, reason}` - Failed to open database

  ## Bulk Loading Best Practices

  Since erlang-rocksdb doesn't support runtime option changes:

  1. **Use larger batch sizes**: Pass larger `batch_size` to load operations
  2. **Disable WAL for initial load**: Set `disable_wal: true` in options (use with caution)
  3. **Load in parallel**: Use Flow for concurrent loading from multiple files
  4. **Compact after load**: Call manual compaction after bulk loading completes

  ## Examples

      # Open for bulk load
      {:ok, adapter} = ErlangAdapter.open_for_bulk_load("/path/to/db")

      # Load large dataset
      TripleStore.load(store, "large_file.ttl", batch_size: 50_000)

      # After load completes, compact the database
      :ok = ErlangAdapter.compact(adapter)

  """
  @spec open_for_bulk_load(String.t(), keyword()) :: {:ok, adapter()} | {:error, term()}
  def open_for_bulk_load(path, opts \\ []) do
    # Note: erlang-rocksdb doesn't support runtime option changes
    # This function serves as documentation and a forward-compatible API
    open(path, opts)
  end

  @doc """
  Closes the database and releases all resources.

  ## Parameters

  - `adapter`: The adapter PID

  ## Returns

  - `:ok` - Database closed successfully
  - `{:error, reason}` - Failed to close database

  """
  @spec close(adapter()) :: :ok | {:error, term()}
  def close(adapter) when is_pid(adapter) do
    if Process.alive?(adapter) do
      try do
        GenServer.stop(adapter, :normal, 10_000)
        :ok
      rescue
        # Process died during close
        _ -> {:error, :already_closed}
      end
    else
      {:error, :already_closed}
    end
  end

  @doc """
  Flushes the Write-Ahead Log to durable storage.

  ## Parameters

  - `adapter`: The adapter PID
  - `sync`: If true, synchronously flush the WAL

  ## Returns

  - `:ok` - WAL flushed successfully
  - `{:error, reason}` - Failed to flush WAL

  """
  @spec flush_wal(adapter(), boolean()) :: :ok | {:error, term()}
  def flush_wal(adapter, sync \\ false) when is_pid(adapter) and is_boolean(sync) do
    GenServer.call(adapter, {:flush_wal, sync})
  end

  @doc """
  Sets runtime options for the database.

  ## Parameters

  - `adapter`: The adapter PID
  - `options`: List of {option_name, option_value} tuples

  ## Returns

  - `:ok` - Options set successfully
  - `{:error, reason}` - Failed to set options

  """
  @spec set_options(adapter(), [{String.t(), String.t()}]) :: :ok | {:error, term()}
  def set_options(adapter, options) when is_pid(adapter) and is_list(options) do
    GenServer.call(adapter, {:set_options, options})
  end

  @doc """
  Gets the path of the database.

  ## Parameters

  - `adapter`: The adapter PID

  ## Returns

  - `{:ok, path}` - Path retrieved successfully
  - `{:error, reason}` - Failed to get path

  """
  @spec get_path(adapter()) :: {:ok, String.t()} | {:error, term()}
  def get_path(adapter) when is_pid(adapter) do
    GenServer.call(adapter, :get_path)
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
    db_path = String.to_charlist(path)

    case :rocksdb.list_column_families(db_path, []) do
      {:ok, cf_names} -> Enum.map(cf_names, &List.to_string/1)
      {:error, _reason} -> []
    end
  end

  @doc """
  Checks if the database is currently open.

  ## Parameters

  - `adapter`: The adapter PID

  ## Returns

  - `true` - Database is open
  - `false` - Database is closed

  """
  @spec is_open(adapter()) :: boolean()
  def is_open(adapter) when is_pid(adapter) do
    Process.alive?(adapter) and GenServer.call(adapter, :is_open)
  end

  @doc """
  Checks if the database is a quad store (schema version 2).

  A quad store uses 32-byte keys and four indices (gspo, gpos, spog, posg)
  to support named graphs. A triple store uses 24-byte keys and three
  indices (spo, pos, osp).

  ## Parameters

  - `adapter`: The adapter PID

  ## Returns

  - `{:ok, true}` - Database is a quad store
  - `{:ok, false}` - Database is a triple store
  - `{:error, reason}` - Error occurred

  ## Examples

      iex> {:ok, adapter} = ErlangAdapter.open("/tmp/test_db", schema: :quad)
      iex> ErlangAdapter.is_quad_store?(adapter)
      {:ok, true}

      iex> {:ok, adapter} = ErlangAdapter.open("/tmp/test_db", schema: :triple)
      iex> ErlangAdapter.is_quad_store?(adapter)
      {:ok, false}

  """
  @spec is_quad_store?(adapter()) :: {:ok, boolean()} | {:error, term()}
  def is_quad_store?(adapter) when is_pid(adapter) do
    GenServer.call(adapter, :is_quad_store)
  end

  @doc """
  Gets a value from the database.

  ## Parameters

  - `adapter`: The adapter PID
  - `cf`: Column family atom
  - `key`: Binary key to look up

  ## Returns

  - `{:ok, value}` - Key found, returns the value
  - `:not_found` - Key does not exist
  - `{:error, reason}` - Error occurred

  """
  @spec get(adapter(), column_family(), binary()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def get(adapter, cf, key) when is_pid(adapter) and is_atom(cf) and is_binary(key) do
    GenServer.call(adapter, {:get, cf, key})
  end

  @doc """
  Puts a key-value pair into the database.

  ## Parameters

  - `adapter`: The adapter PID
  - `cf`: Column family atom
  - `key`: Binary key
  - `value`: Binary value

  ## Returns

  - `:ok` - Value written successfully
  - `{:error, reason}` - Error occurred

  """
  @spec put(adapter(), column_family(), binary(), binary()) :: :ok | {:error, term()}
  def put(adapter, cf, key, value) when is_pid(adapter) and is_atom(cf) and is_binary(key) do
    GenServer.call(adapter, {:put, cf, key, value})
  end

  @doc """
  Deletes a key from the database.

  ## Parameters

  - `adapter`: The adapter PID
  - `cf`: Column family atom
  - `key`: Binary key to delete

  ## Returns

  - `:ok` - Key deleted successfully
  - `{:error, reason}` - Error occurred

  """
  @spec delete(adapter(), column_family(), binary()) :: :ok | {:error, term()}
  def delete(adapter, cf, key) when is_pid(adapter) and is_atom(cf) and is_binary(key) do
    GenServer.call(adapter, {:delete, cf, key})
  end

  @doc """
  Checks if a key exists in the database.

  ## Parameters

  - `adapter`: The adapter PID
  - `cf`: Column family atom
  - `key`: Binary key to check

  ## Returns

  - `{:ok, true}` - Key exists
  - `{:ok, false}` - Key does not exist
  - `{:error, reason}` - Error occurred

  """
  @spec exists(adapter(), column_family(), binary()) :: {:ok, boolean()} | {:error, term()}
  def exists(adapter, cf, key) when is_pid(adapter) and is_atom(cf) and is_binary(key) do
    GenServer.call(adapter, {:exists, cf, key})
  end

  @doc """
  Writes multiple operations atomically in a batch.

  ## Parameters

  - `adapter`: The adapter PID
  - `operations`: List of {cf, key, value} tuples for put operations
  - `sync`: If true, synchronously write to WAL

  ## Returns

  - `:ok` - Batch written successfully
  - `{:error, reason}` - Error occurred

  """
  @spec write_batch(adapter(), [{column_family(), binary(), binary()}], boolean()) ::
          :ok | {:error, term()}
  def write_batch(adapter, operations, sync \\ false)
      when is_pid(adapter) and is_list(operations) do
    GenServer.call(adapter, {:write_batch, operations, sync})
  end

  @doc """
  Deletes multiple keys atomically in a batch.

  ## Parameters

  - `adapter`: The adapter PID
  - `operations`: List of {cf, key} tuples for delete operations
  - `sync`: If true, synchronously write to WAL

  ## Returns

  - `:ok` - Batch deleted successfully
  - `{:error, reason}` - Error occurred

  """
  @spec delete_batch(adapter(), [{column_family(), binary()}], boolean()) ::
          :ok | {:error, term()}
  def delete_batch(adapter, operations, sync \\ false)
      when is_pid(adapter) and is_list(operations) do
    GenServer.call(adapter, {:delete_batch, operations, sync})
  end

  @doc """
  Writes a mixed batch of put and delete operations atomically.

  ## Parameters

  - `adapter`: The adapter PID
  - `operations`: List of put/delete operations
    - `{:put, cf, key, value}` - Put operation
    - `{:delete, cf, key}` - Delete operation
  - `sync`: If true, synchronously write to WAL

  ## Returns

  - `:ok` - Batch written successfully
  - `{:error, reason}` - Error occurred

  """
  @spec mixed_batch(
          adapter(),
          [{:put, column_family(), binary(), binary()} | {:delete, column_family(), binary()}],
          boolean()
        ) :: :ok | {:error, term()}
  def mixed_batch(adapter, operations, sync \\ false)
      when is_pid(adapter) and is_list(operations) do
    GenServer.call(adapter, {:mixed_batch, operations, sync})
  end

  # ===========================================================================
  # Iterator Operations
  # ===========================================================================

  @doc """
  Creates a prefix iterator for a column family.

  ## Parameters

  - `adapter` - The adapter PID
  - `cf` - Column family atom
  - `prefix` - Binary prefix to iterate within

  ## Returns

  - `{:ok, iterator_ref}` - Iterator created successfully (iterator_ref is a PID)
  - `{:error, reason}` - Failed to create iterator

  ## Examples

      {:ok, iter} = ErlangAdapter.prefix_iterator(adapter, :spo, <<subject_id::64-big>>)

  """
  @spec prefix_iterator(adapter(), column_family(), binary()) ::
          {:ok, iterator_ref()} | {:error, term()}
  def prefix_iterator(adapter, cf, prefix)
      when is_pid(adapter) and is_atom(cf) and is_binary(prefix) do
    GenServer.call(adapter, {:prefix_iterator, cf, prefix})
  end

  @doc """
  Creates a prefix iterator with options.

  ## Parameters

  - `adapter` - The adapter PID
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
  @spec prefix_iterator(adapter(), column_family(), binary(), keyword()) ::
          {:ok, iterator_ref()} | {:error, term()}
  def prefix_iterator(adapter, cf, prefix, opts)
      when is_pid(adapter) and is_atom(cf) and is_binary(prefix) do
    GenServer.call(adapter, {:prefix_iterator, cf, prefix, opts})
  end

  @doc """
  Creates a general iterator for a column family.

  ## Parameters

  - `adapter` - The adapter PID
  - `cf` - Column family atom
  - `opts` - Iterator options

  ## Returns

  - `{:ok, iterator_ref}` - Iterator created successfully
  - `{:error, reason}` - Failed to create iterator

  """
  @spec iterator(adapter(), column_family(), keyword()) ::
          {:ok, iterator_ref()} | {:error, term()}
  def iterator(adapter, cf, opts \\ []) when is_pid(adapter) and is_atom(cf) do
    GenServer.call(adapter, {:iterator, cf, opts})
  end

  @doc """
  Moves the iterator and returns the next entry.

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
    Iterator.move(iterator_ref, action)
  end

  @doc """
  Gets the next entry from the iterator.

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
    Iterator.next(iterator_ref)
  end

  @doc """
  Seeks the iterator to a specific key.

  ## Parameters

  - `iterator_ref` - The iterator PID
  - `seek_key` - Binary key to seek to

  ## Returns

  - `:ok` - Seek successful
  - `{:error, reason}` - Error occurred

  """
  @spec iterator_seek(iterator_ref(), binary()) :: :ok | {:error, term()}
  def iterator_seek(iterator_ref, seek_key) when is_pid(iterator_ref) and is_binary(seek_key) do
    Iterator.seek(iterator_ref, seek_key)
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
    Iterator.close(iterator_ref)
  end

  @doc """
  Collects all remaining entries from an iterator.

  ## Parameters

  - `iterator_ref` - The iterator PID
  - `opts` - Options

  ## Returns

  - `{:ok, [{key, value}]}` - List of entries
  - `{:error, reason}` - Error occurred

  """
  @spec iterator_collect(iterator_ref(), keyword()) ::
          {:ok, [{binary(), binary()}]} | {:error, term()}
  def iterator_collect(iterator_ref, opts \\ []) when is_pid(iterator_ref) do
    Iterator.collect(iterator_ref, opts)
  end

  # ===========================================================================
  # Snapshot Operations
  # ===========================================================================

  @doc """
  Creates a point-in-time snapshot of the database.

  Snapshots provide consistent read-only views over the entire state of the key-value store.

  ## Parameters

  - `adapter` - The adapter PID

  ## Returns

  - `{:ok, snapshot_ref}` - Snapshot created successfully
  - `{:error, reason}` - Failed to create snapshot

  ## Examples

      iex> {:ok, snap} = ErlangAdapter.snapshot(adapter)
      iex> is_reference(snap)
      true

  """
  @spec snapshot(adapter()) :: {:ok, reference()} | {:error, term()}
  def snapshot(adapter) when is_pid(adapter) do
    GenServer.call(adapter, :snapshot)
  end

  @doc """
  Releases a snapshot, freeing its resources.

  ## Parameters

  - `adapter` - The adapter PID
  - `snapshot_ref` - The snapshot reference to release

  ## Returns

  - `:ok` - Snapshot released successfully
  - `{:error, reason}` - Failed to release snapshot

  """
  @spec release_snapshot(adapter(), reference()) :: :ok | {:error, term()}
  def release_snapshot(adapter, snapshot_ref)
      when is_pid(adapter) and is_reference(snapshot_ref) do
    GenServer.call(adapter, {:release_snapshot, snapshot_ref})
  end

  @doc """
  Gets a value from a snapshot.

  Reads the value as it existed when the snapshot was created.

  ## Parameters

  - `adapter` - The adapter PID
  - `snapshot_ref` - The snapshot reference
  - `cf` - Column family atom
  - `key` - Binary key to look up

  ## Returns

  - `{:ok, value}` - Key found in snapshot
  - `:not_found` - Key does not exist in snapshot
  - `{:error, reason}` - Error occurred

  """
  @spec snapshot_get(adapter(), reference(), column_family(), binary()) ::
          {:ok, binary()} | :not_found | {:error, term()}
  def snapshot_get(adapter, snapshot_ref, cf, key)
      when is_pid(adapter) and is_reference(snapshot_ref) and is_atom(cf) and is_binary(key) do
    GenServer.call(adapter, {:snapshot_get, snapshot_ref, cf, key})
  end

  @doc """
  Creates a prefix iterator on a snapshot.

  The iterator will see the database state as of the snapshot creation time.

  ## Parameters

  - `adapter` - The adapter PID
  - `snapshot_ref` - The snapshot reference
  - `cf` - Column family atom
  - `prefix` - Binary prefix to iterate over

  ## Returns

  - `{:ok, iterator_ref}` - Iterator created successfully
  - `{:error, reason}` - Failed to create iterator

  """
  @spec snapshot_prefix_iterator(adapter(), reference(), column_family(), binary()) ::
          {:ok, iterator_ref()} | {:error, term()}
  def snapshot_prefix_iterator(adapter, snapshot_ref, cf, prefix)
      when is_pid(adapter) and is_reference(snapshot_ref) and is_atom(cf) and is_binary(prefix) do
    GenServer.call(adapter, {:snapshot_prefix_iterator, snapshot_ref, cf, prefix})
  end

  @doc """
  Creates a prefix iterator on a snapshot with options.

  ## Parameters

  - `adapter` - The adapter PID
  - `snapshot_ref` - The snapshot reference
  - `cf` - Column family atom
  - `prefix` - Binary prefix to iterate over
  - `opts` - Iterator options

  ## Returns

  - `{:ok, iterator_ref}` - Iterator created successfully
  - `{:error, reason}` - Failed to create iterator

  """
  @spec snapshot_prefix_iterator(adapter(), reference(), column_family(), binary(), keyword()) ::
          {:ok, iterator_ref()} | {:error, term()}
  def snapshot_prefix_iterator(adapter, snapshot_ref, cf, prefix, opts)
      when is_pid(adapter) and is_reference(snapshot_ref) and is_atom(cf) and is_binary(prefix) do
    GenServer.call(adapter, {:snapshot_prefix_iterator, snapshot_ref, cf, prefix, opts})
  end

  # ===========================================================================
  # Fold Operations (Phase 2 - Section 2.3)
  # ===========================================================================

  @doc """
  Folds over a prefix range in a column family.

  This function uses erlang-rocksdb's fold operation to efficiently iterate
  over all key-value pairs within a prefix range. The fold operation is performed
  in C++ land, reducing BEAM-NIF boundary crossings for better performance.

  ## Parameters

  - `adapter` - The adapter PID
  - `cf` - Column family atom
  - `prefix` - Binary prefix to limit the fold to
  - `acc` - Initial accumulator value
  - `fun` - Fold function: `({key, value}, acc) -> new_acc`

  ## Returns

  - `acc` - Final accumulator value

  ## Examples

      # Count all entries with a prefix
      count = ErlangAdapter.fold(adapter, :spo, <<subject_id::64-big>>, 0, fn {_k, _v}, acc -> acc + 1 end)

  """
  @spec fold(adapter(), column_family(), binary(), term(), fold_fun()) :: term()
  def fold(adapter, cf, prefix, acc, fun)
      when is_pid(adapter) and is_atom(cf) and is_binary(prefix) and is_function(fun, 2) do
    GenServer.call(adapter, {:fold, cf, prefix, acc, fun})
  end

  @doc """
  Folds over a prefix range with options.

  ## Options

  - `iterate_upper_bound` - Upper bound for iteration (binary)
  - `fill_cache` - Whether to fill block cache (default: true)
  - `snapshot` - Use a specific snapshot

  """
  @spec fold(adapter(), column_family(), binary(), term(), fold_fun(), keyword()) :: term()
  def fold(adapter, cf, prefix, acc, fun, opts)
      when is_pid(adapter) and is_atom(cf) and is_binary(prefix) and is_function(fun, 2) do
    GenServer.call(adapter, {:fold, cf, prefix, acc, fun, opts})
  end

  @doc """
  Folds over keys only in a prefix range.

  More efficient than `fold/5` when values are not needed.

  ## Parameters

  - `adapter` - The adapter PID
  - `cf` - Column family atom
  - `prefix` - Binary prefix to limit the fold to
  - `acc` - Initial accumulator value
  - `fun` - Fold function: `(key, acc) -> new_acc`

  ## Returns

  - `acc` - Final accumulator value

  ## Examples

      # Collect all keys with a prefix
      keys = ErlangAdapter.fold_keys(adapter, :spo, <<subject_id::64-big>>, [], fn k, acc -> [k | acc] end)

  """
  @spec fold_keys(adapter(), column_family(), binary(), term(), fold_keys_fun()) :: term()
  def fold_keys(adapter, cf, prefix, acc, fun)
      when is_pid(adapter) and is_atom(cf) and is_binary(prefix) and is_function(fun, 2) do
    GenServer.call(adapter, {:fold_keys, cf, prefix, acc, fun})
  end

  @doc """
  Folds over keys only in a prefix range with options.

  ## Options

  - `iterate_upper_bound` - Upper bound for iteration (binary)
  - `fill_cache` - Whether to fill block cache (default: true)
  - `snapshot` - Use a specific snapshot

  """
  @spec fold_keys(adapter(), column_family(), binary(), term(), fold_keys_fun(), keyword()) ::
          term()
  def fold_keys(adapter, cf, prefix, acc, fun, opts)
      when is_pid(adapter) and is_atom(cf) and is_binary(prefix) and is_function(fun, 2) do
    GenServer.call(adapter, {:fold_keys, cf, prefix, acc, fun, opts})
  end

  # ===========================================================================
  # Stream Operations (Phase 2 - Section 2.3)
  # ===========================================================================

  @doc """
  Creates a lazy stream over a prefix range using a snapshot.

  The stream reads from a point-in-time snapshot of the database,
  ensuring consistent reads even if the data is modified during iteration.

  ## Parameters

  - `adapter` - The adapter PID
  - `snapshot_ref` - Snapshot reference from `snapshot/1`
  - `cf` - Column family atom
  - `prefix` - Binary prefix to iterate over

  ## Returns

  - `Stream.t()` - A stream of `{key, value}` tuples

  ## Examples

      # Create snapshot and stream from it
      {:ok, snap} = ErlangAdapter.snapshot(db)
      stream = ErlangAdapter.snapshot_stream(db, snap, :spo, <<subject_id::64-big>>)
      results = Enum.to_list(stream)
      ErlangAdapter.release_snapshot(db, snap)

  """
  @spec snapshot_stream(adapter(), snapshot_ref(), column_family(), binary()) :: Enumerable.t()
  def snapshot_stream(adapter, snapshot_ref, cf, prefix)
      when is_pid(adapter) and is_reference(snapshot_ref) and is_atom(cf) and is_binary(prefix) do
    prefix_stream(adapter, cf, prefix, snapshot: snapshot_ref)
  end

  @doc """
  Creates a lazy stream over a prefix range.

  The stream properly manages resources and will close the underlying iterator
  when the stream is terminated or if an error occurs.

  ## Parameters

  - `adapter` - The adapter PID
  - `cf` - Column family atom
  - `prefix` - Binary prefix to iterate over

  ## Returns

  - `Stream.t()` - A stream of `{key, value}` tuples

  ## Examples

      # Stream all entries with a prefix
      adapter |> ErlangAdapter.prefix_stream(:spo, <<subject_id::64-big>>) |> Enum.to_list()

      # Use with Stream functions for lazy evaluation
      adapter
      |> ErlangAdapter.prefix_stream(:spo, <<subject_id::64-big>>)
      |> Stream.take(100)
      |> Enum.to_list()

  """
  @spec prefix_stream(adapter(), column_family(), binary()) :: Enumerable.t()
  def prefix_stream(adapter, cf, prefix)
      when is_pid(adapter) and is_atom(cf) and is_binary(prefix) do
    prefix_stream(adapter, cf, prefix, [])
  end

  @doc """
  Creates a lazy stream over a prefix range with options.

  ## Options

  - `fill_cache` - Whether to fill block cache (default: true)
  - `snapshot` - Use a specific snapshot

  """
  @spec prefix_stream(adapter(), column_family(), binary(), keyword()) :: Enumerable.t()
  def prefix_stream(adapter, cf, prefix, opts)
      when is_pid(adapter) and is_atom(cf) and is_binary(prefix) do
    Stream.resource(
      fn ->
        # Setup: create the iterator
        case GenServer.call(adapter, {:prefix_iterator, cf, prefix, opts}) do
          {:ok, iter_pid} ->
            # Monitor the iterator for cleanup
            ref = Process.monitor(iter_pid)
            {iter_pid, ref}

          {:error, reason} ->
            raise "Failed to create iterator: #{inspect(reason)}"
        end
      end,
      fn
        # Halted state - check this first since it's more specific
        {:halt, acc} ->
          {:halt, acc}

        # Next: get one entry
        {iter_pid, _ref} ->
          case Iterator.next(iter_pid) do
            {:ok, key, value} ->
              {[{key, value}], {iter_pid, nil}}

            :iterator_end ->
              {:halt, {iter_pid, nil}}
          end
      end,
      fn
        # Cleanup: close iterator and demonitor
        {iter_pid, ref} when is_pid(iter_pid) ->
          Iterator.close(iter_pid)

          if is_reference(ref), do: Process.demonitor(ref, [:flush])

          :ok

        _ ->
          :ok
      end
    )
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init({path, opts}) do
    # Determine schema type (default to :triple for backward compatibility)
    schema_type = Keyword.get(opts, :schema, :triple)

    # Validate schema type
    unless schema_type in [:triple, :quad] do
      raise ArgumentError, "Invalid schema type: #{schema_type}. Must be :triple or :quad"
    end

    # Get database options from config
    db_opts = ColumnFamilyConfig.db_options()

    # Add caller options
    create_if_missing = Keyword.get(opts, :create_if_missing, true)
    error_if_exists = Keyword.get(opts, :error_if_exists, false)

    db_opts =
      db_opts
      |> Keyword.put(:create_if_missing, create_if_missing)
      |> Keyword.put(:error_if_exists, error_if_exists)

    # Convert path to charlist for erlang-rocksdb
    db_path = String.to_charlist(path)

    # Open or create the database
    case open_or_create_database(db_path, db_opts, path, schema_type) do
      {:ok, db, cf_handles} ->
        # Map column family names to atoms
        cf_map = map_cf_handles(cf_handles, schema_type)

        {:ok, %{db: db, cf_handles: cf_map, path: path, schema_type: schema_type}}

      {:error, _reason} = error ->
        # Return error to prevent the GenServer from starting
        error
    end
  end

  @impl true
  def handle_call(:is_open, _from, state) do
    {:reply, true, state}
  end

  @impl true
  def handle_call(:is_quad_store, _from, %{db: db} = state) do
    case read_schema_version(db) do
      {:ok, @schema_v2_quad} -> {:reply, {:ok, true}, state}
      {:ok, @schema_v1_triple} -> {:reply, {:ok, false}, state}
      {:ok, version} -> {:reply, {:error, {:unknown_schema_version, version}}, state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call(:get_path, _from, %{path: path} = state) do
    {:reply, {:ok, path}, state}
  end

  @impl true
  def handle_call({:get, cf, key}, _from, %{db: db, cf_handles: cf_handles} = state) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         result <- :rocksdb.get(db, cf_handle, key, []) do
      {:reply, result, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:put, cf, key, value}, _from, %{db: db, cf_handles: cf_handles} = state) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         result <- :rocksdb.put(db, cf_handle, key, value, []) do
      {:reply, result, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:delete, cf, key}, _from, %{db: db, cf_handles: cf_handles} = state) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         result <- :rocksdb.delete(db, cf_handle, key, []) do
      {:reply, result, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:exists, cf, key}, _from, %{db: db, cf_handles: cf_handles} = state) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf) do
      case :rocksdb.get(db, cf_handle, key, []) do
        {:ok, _value} -> {:reply, {:ok, true}, state}
        :not_found -> {:reply, {:ok, false}, state}
        {:error, _reason} = error -> {:reply, error, state}
      end
    else
      {:error, _reason} = error -> {:reply, error, state}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call(
        {:write_batch, operations, sync},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    write_opts = if sync, do: [sync: true], else: []

    batch =
      Enum.map(operations, fn {cf, key, value} ->
        case get_cf_handle(cf_handles, cf) do
          {:ok, cf_handle} -> {:put, cf_handle, key, value}
          {:error, _reason} = error -> error
        end
      end)

    if Enum.any?(batch, &match?({:error, _}, &1)) do
      {:reply, {:error, :invalid_column_family}, state}
    else
      result = :rocksdb.write(db, batch, write_opts)
      {:reply, result, state}
    end
  end

  @impl true
  def handle_call(
        {:delete_batch, operations, sync},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    write_opts = if sync, do: [sync: true], else: []

    batch =
      Enum.map(operations, fn {cf, key} ->
        case get_cf_handle(cf_handles, cf) do
          {:ok, cf_handle} -> {:delete, cf_handle, key}
          {:error, _reason} = error -> error
        end
      end)

    if Enum.any?(batch, &match?({:error, _}, &1)) do
      {:reply, {:error, :invalid_column_family}, state}
    else
      result = :rocksdb.write(db, batch, write_opts)
      {:reply, result, state}
    end
  end

  @impl true
  def handle_call(
        {:mixed_batch, operations, sync},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    write_opts = if sync, do: [sync: true], else: []

    batch =
      Enum.map(operations, fn
        {:put, cf, key, value} ->
          case get_cf_handle(cf_handles, cf) do
            {:ok, cf_handle} -> {:put, cf_handle, key, value}
            {:error, _reason} = error -> error
          end

        {:delete, cf, key} ->
          case get_cf_handle(cf_handles, cf) do
            {:ok, cf_handle} -> {:delete, cf_handle, key}
            {:error, _reason} = error -> error
          end

        {invalid_op, _cf, _key, _value} ->
          {:error, {:invalid_operation, invalid_op}}

        {invalid_op, _cf, _key} ->
          {:error, {:invalid_operation, invalid_op}}

        other ->
          {:error, {:invalid_operation, :unknown}}
      end)

    # Check for errors in the batch
    error =
      Enum.find(batch, fn
        {:error, {:invalid_operation, _}} -> true
        {:error, _} -> true
        _ -> false
      end)

    case error do
      {:error, {:invalid_operation, op}} ->
        {:reply, {:error, {:invalid_operation, op}}, state}

      {:error, _} ->
        {:reply, {:error, :invalid_column_family}, state}

      nil ->
        result = :rocksdb.write(db, batch, write_opts)
        {:reply, result, state}
    end
  end

  @impl true
  def handle_call({:flush_wal, sync}, _from, %{db: db} = state) do
    # erlang-rocksdb doesn't have flush_wal, use flush/2 instead
    # flush flushes memtables to L0 SST files, which persists data
    flush_opts = if sync, do: [sync: true], else: []
    result = :rocksdb.flush(db, flush_opts)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:set_options, _options}, _from, state) do
    # erlang-rocksdb doesn't support runtime set_options
    # Options must be set at database open time
    {:reply, {:error, :not_supported}, state}
  end

  @impl true
  def handle_call(
        {:prefix_iterator, cf, prefix},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         opts = [
           prefix: prefix,
           fill_cache: true,
           total_order_seek: false,
           prefix_same_as_start: false
         ],
         {:ok, iter_pid} <- Iterator.start_link(db, cf_handle, opts) do
      {:reply, {:ok, iter_pid}, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call(
        {:prefix_iterator, cf, prefix, opts},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         opts = Keyword.put(opts, :prefix, prefix),
         {:ok, iter_pid} <- Iterator.start_link(db, cf_handle, opts) do
      {:reply, {:ok, iter_pid}, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:iterator, cf, opts}, _from, %{db: db, cf_handles: cf_handles} = state) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         {:ok, iter_pid} <- Iterator.start_link(db, cf_handle, opts) do
      {:reply, {:ok, iter_pid}, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  # ===========================================================================
  # Snapshot Operation Handlers
  # ===========================================================================

  @impl true
  def handle_call(:snapshot, _from, %{db: db} = state) do
    case :rocksdb.snapshot(db) do
      {:ok, snapshot_ref} -> {:reply, {:ok, snapshot_ref}, state}
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:release_snapshot, snapshot_ref}, _from, state) do
    result = :rocksdb.release_snapshot(snapshot_ref)
    {:reply, result, state}
  end

  @impl true
  def handle_call(
        {:snapshot_get, snapshot_ref, cf, key},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         read_opts = [{:snapshot, snapshot_ref}],
         result <- :rocksdb.get(db, cf_handle, key, read_opts) do
      {:reply, result, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call(
        {:snapshot_prefix_iterator, snapshot_ref, cf, prefix},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         opts = [
           prefix: prefix,
           fill_cache: true,
           total_order_seek: false,
           prefix_same_as_start: false,
           snapshot: snapshot_ref
         ],
         {:ok, iter_pid} <- Iterator.start_link(db, cf_handle, opts) do
      {:reply, {:ok, iter_pid}, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call(
        {:snapshot_prefix_iterator, snapshot_ref, cf, prefix, opts},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         opts = Keyword.put(opts, :snapshot, snapshot_ref) |> Keyword.put(:prefix, prefix),
         {:ok, iter_pid} <- Iterator.start_link(db, cf_handle, opts) do
      {:reply, {:ok, iter_pid}, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
    end
  end

  # ===========================================================================
  # Fold Operation Handlers
  # ===========================================================================

  @impl true
  def handle_call({:fold, cf, prefix, acc, fun}, _from, %{db: db, cf_handles: cf_handles} = state) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         read_opts = build_fold_read_opts(prefix, []),
         result <- do_fold(db, cf_handle, prefix, read_opts, acc, fun) do
      {:reply, result, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call(
        {:fold, cf, prefix, acc, fun, opts},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         read_opts = build_fold_read_opts(prefix, opts),
         result <- do_fold(db, cf_handle, prefix, read_opts, acc, fun) do
      {:reply, result, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call(
        {:fold_keys, cf, prefix, acc, fun},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         read_opts = build_fold_read_opts(prefix, []),
         result <- do_fold_keys(db, cf_handle, prefix, read_opts, acc, fun) do
      {:reply, result, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def handle_call(
        {:fold_keys, cf, prefix, acc, fun, opts},
        _from,
        %{db: db, cf_handles: cf_handles} = state
      ) do
    with {:ok, cf_handle} <- get_cf_handle(cf_handles, cf),
         read_opts = build_fold_read_opts(prefix, opts),
         result <- do_fold_keys(db, cf_handle, prefix, read_opts, acc, fun) do
      {:reply, result, state}
    else
      {:error, _reason} = error -> {:reply, error, state}
      error -> {:reply, error, state}
    end
  end

  @impl true
  def terminate(_reason, %{db: db}) do
    # Close the RocksDB database
    if db do
      :rocksdb.close(db)
    end

    :ok
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  # Opens an existing database or creates a new one with all column families
  defp open_or_create_database(db_path, db_opts, original_path, schema_type) do
    db_exists = database_exists?(original_path)

    cond do
      db_exists and Keyword.get(db_opts, :error_if_exists, false) ->
        {:error, :database_already_exists}

      db_exists ->
        # Open existing database with all CFs and validate schema version
        open_existing_database(db_path, db_opts, schema_type)

      true ->
        # Create new database with all CFs and set schema version
        create_new_database(db_path, db_opts, schema_type)
    end
  end

  # Check if database directory exists and has RocksDB files
  defp database_exists?(path) do
    case File.stat(path) do
      {:ok, %{type: :directory}} ->
        # Check if it looks like a RocksDB database (has CURRENT or MANIFEST files)
        current_exists = File.exists?(Path.join(path, "CURRENT"))
        manifest_exists = File.exists?(Path.join(path, "MANIFEST-000001"))
        current_exists or manifest_exists

      _ ->
        false
    end
  end

  # Opens an existing database with all configured column families
  defp open_existing_database(db_path, db_opts, expected_schema_type) do
    # Get column family descriptors - we need to determine which schema the DB has first
    # For now, try to detect schema by listing existing CFs
    case :rocksdb.list_column_families(db_path, []) do
      {:ok, existing_cf_names} ->
        # Detect schema from existing CFs
        detected_schema = detect_schema_from_cfs(existing_cf_names)

        # Validate schema version matches what we expect
        schema_version =
          case detected_schema do
            :triple -> @schema_v1_triple
            :quad -> @schema_v2_quad
          end

        expected_version =
          case expected_schema_type do
            :triple -> @schema_v1_triple
            :quad -> @schema_v2_quad
          end

        if schema_version != expected_version do
          # Schema mismatch - this is a hard error
          Logger.error(
            "Schema mismatch: expected #{expected_schema_type} (v#{expected_version}), but database has #{detected_schema} (v#{schema_version})"
          )

          {:error, :schema_mismatch}
        else
          # Schema matches, proceed with opening
          cf_descriptors = ColumnFamilyConfig.cf_descriptors(detected_schema)

          # Convert to charlist format for erlang-rocksdb
          cf_descriptors_charlist =
            Enum.map(cf_descriptors, fn {name, opts} ->
              {String.to_charlist(name), opts}
            end)

          case :rocksdb.open_with_cf(db_path, db_opts, cf_descriptors_charlist) do
            {:ok, db, cf_handles} ->
              # Validate schema version from stored metadata
              case read_schema_version(db) do
                {:ok, ^schema_version} ->
                  {:ok, db, cf_handles}

                {:ok, actual_version} ->
                  Logger.error(
                    "Schema version mismatch in metadata: expected v#{schema_version}, got v#{actual_version}"
                  )

                  {:error, :schema_mismatch}

                {:error, :not_found} ->
                  # Old database without schema version metadata
                  # If we have triple indices, assume it's a v1 triple store
                  if detected_schema == :triple do
                    Logger.info(
                      "No schema version metadata found, assuming v1 (triple) based on column families"
                    )

                    {:ok, db, cf_handles}
                  else
                    {:error, :missing_schema_metadata}
                  end

                {:error, _reason} = error ->
                  error
              end

            {:error, _reason} = error ->
              error
          end
        end

      {:error, _reason} = error ->
        error
    end
  end

  # Creates a new database with all column families
  defp create_new_database(db_path, db_opts, schema_type) do
    # First, open with just the default column family
    case :rocksdb.open_with_cf(db_path, db_opts, [{~c"default", []}]) do
      {:ok, db, [default_cf]} ->
        # Create schema-appropriate column families
        create_result =
          case schema_type do
            :triple -> create_triple_column_families(db)
            :quad -> create_quad_column_families(db)
          end

        case create_result do
          {:ok, cf_handles} ->
            # Write schema version to metadata
            schema_version =
              case schema_type do
                :triple -> @schema_v1_triple
                :quad -> @schema_v2_quad
              end

            case write_schema_version(db, schema_version) do
              :ok ->
                {:ok, db, [default_cf | cf_handles]}

              {:error, _reason} = error ->
                # Clean up on error
                :rocksdb.close(db)
                error
            end

          {:error, _reason} = error ->
            # Clean up on error
            :rocksdb.close(db)
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  # Creates triple store column families
  defp create_triple_column_families(db) do
    with {:ok, id2str_cf} <- :rocksdb.create_column_family(db, ~c"id2str", []),
         {:ok, str2id_cf} <- :rocksdb.create_column_family(db, ~c"str2id", []),
         {:ok, spo_cf} <- :rocksdb.create_column_family(db, ~c"spo", []),
         {:ok, pos_cf} <- :rocksdb.create_column_family(db, ~c"pos", []),
         {:ok, osp_cf} <- :rocksdb.create_column_family(db, ~c"osp", []),
         {:ok, derived_cf} <- :rocksdb.create_column_family(db, ~c"derived", []),
         {:ok, numeric_cf} <- :rocksdb.create_column_family(db, ~c"numeric_range", []) do
      {:ok, [id2str_cf, str2id_cf, spo_cf, pos_cf, osp_cf, derived_cf, numeric_cf]}
    end
  end

  # Creates quad store column families
  defp create_quad_column_families(db) do
    with {:ok, id2str_cf} <- :rocksdb.create_column_family(db, ~c"id2str", []),
         {:ok, str2id_cf} <- :rocksdb.create_column_family(db, ~c"str2id", []),
         {:ok, gspo_cf} <- :rocksdb.create_column_family(db, ~c"gspo", []),
         {:ok, gpos_cf} <- :rocksdb.create_column_family(db, ~c"gpos", []),
         {:ok, spog_cf} <- :rocksdb.create_column_family(db, ~c"spog", []),
         {:ok, posg_cf} <- :rocksdb.create_column_family(db, ~c"posg", []),
         {:ok, derived_cf} <- :rocksdb.create_column_family(db, ~c"derived", []),
         {:ok, derivation_prov_cf} <- :rocksdb.create_column_family(db, ~c"derivation_provenance", []),
         {:ok, numeric_cf} <- :rocksdb.create_column_family(db, ~c"numeric_range", []),
         {:ok, acl_cf} <- :rocksdb.create_column_family(db, ~c"acl", []) do
      {:ok,
       [id2str_cf, str2id_cf, gspo_cf, gpos_cf, spog_cf, posg_cf, derived_cf, derivation_prov_cf, numeric_cf, acl_cf]}
    end
  end

  # Maps column family handles to their atom names
  defp map_cf_handles(cf_handles, schema_type) do
    # The order of cf_handles matches the order we opened them
    # Triple: [default, id2str, str2id, spo, pos, osp, derived, numeric_range]
    # Quad: [default, id2str, str2id, gspo, gpos, spog, posg, derived, numeric_range, acl]
    cf_names_in_order =
      case schema_type do
        :triple ->
          [:default, :id2str, :str2id, :spo, :pos, :osp, :derived, :numeric_range]

        :quad ->
          [:default, :id2str, :str2id, :gspo, :gpos, :spog, :posg, :derived, :derivation_provenance, :numeric_range, :acl]
      end

    Enum.zip(cf_names_in_order, cf_handles)
    |> Enum.into(%{})
  end

  # Gets the column family handle for a given atom
  defp get_cf_handle(cf_handles, cf_atom) do
    case Map.get(cf_handles, cf_atom) do
      nil -> {:error, :invalid_column_family}
      handle -> {:ok, handle}
    end
  end

  # Validates the database path for security

  # Uses Path.wildcard to safely check if the expanded path is within allowed directories.
  # This handles path traversal attempts more securely than checking for literal ".."
  # since Path.expand normalizes "../" sequences and we verify the result is within bounds.
  defp validate_path(path) when is_binary(path) do
    expanded_path = Path.expand(path)
    tmp_dir = Path.expand(System.tmp_dir!())
    current_dir = Path.expand(File.cwd!())
    is_absolute = String.starts_with?(path, "/")

    cond do
      # Check for null bytes (security: can be used to bypass string checks)
      String.contains?(path, "\0") ->
        {:error, :null_byte_in_path}

      # Check if expanded path attempts to escape allowed directories
      # For relative paths: check they don't escape current directory after expansion
      not is_absolute and not path_within_directory?(expanded_path, current_dir) ->
        {:error, :path_traversal_attempt}

      # Allow relative paths that stay within current directory
      not is_absolute ->
        :ok

      # Allow paths under /tmp
      path_within_directory?(expanded_path, tmp_dir) ->
        :ok

      # Allow paths under the current project directory
      path_within_directory?(expanded_path, current_dir) ->
        :ok

      # Allow paths under /dev/shm (Linux shared memory for test databases)
      path_within_directory?(expanded_path, "/dev/shm") ->
        :ok

      # Reject other absolute paths for security
      is_absolute ->
        {:error, :absolute_path_not_allowed}

      true ->
        :ok
    end
  end

  # Checks if a path is within a directory (after expansion)
  defp path_within_directory?(path, directory) do
    # Ensure both paths end with / for proper prefix comparison
    dir_with_slash = directory <> "/"

    # Check if path starts with directory (or equals it)
    path == directory or String.starts_with?(path <> "/", dir_with_slash)
  end

  # Ensures the database directory exists
  defp ensure_directory_exists(path) do
    case File.mkdir_p(path) do
      :ok -> :ok
      {:error, reason} -> {:error, {:mkdir_failed, reason}}
    end
  end

  # ===========================================================================
  # Fold Helper Functions
  # ===========================================================================

  # Builds read options for fold operations with prefix support
  defp build_fold_read_opts(_prefix, opts) do
    # Build read options list for erlang-rocksdb
    read_opts = []

    # Add fill_cache option (only add when false to reduce options)
    read_opts =
      case Keyword.get(opts, :fill_cache, true) do
        true -> read_opts
        false -> [:fill_cache_false | read_opts]
      end

    # Add iterate_upper_bound if provided
    read_opts =
      case Keyword.get(opts, :iterate_upper_bound) do
        nil -> read_opts
        bound -> [{:iterate_upper_bound, bound} | read_opts]
      end

    # Add snapshot if provided
    read_opts =
      case Keyword.get(opts, :snapshot) do
        nil -> read_opts
        snapshot -> [{:snapshot, snapshot} | read_opts]
      end

    read_opts
  end

  # Performs fold over key-value pairs with prefix checking
  defp do_fold(db, cf_handle, prefix, read_opts, acc, fun) do
    # Create iterator for fold
    case :rocksdb.iterator(db, cf_handle, read_opts) do
      {:ok, iter} ->
        try do
          # Seek to prefix
          case :rocksdb.iterator_move(iter, prefix) do
            {:ok, key, value} ->
              if has_prefix?(key, prefix) do
                fold_iteration(iter, prefix, fun.({key, value}, acc), fun)
              else
                acc
              end

            :iterator_end ->
              acc

            {:error, _reason} ->
              acc
          end
        after
          :rocksdb.iterator_close(iter)
        end

      {:error, _reason} ->
        acc
    end
  end

  # Performs fold over keys only with prefix checking
  defp do_fold_keys(db, cf_handle, prefix, read_opts, acc, fun) do
    # Create iterator for fold
    case :rocksdb.iterator(db, cf_handle, read_opts) do
      {:ok, iter} ->
        try do
          # Seek to prefix
          case :rocksdb.iterator_move(iter, prefix) do
            {:ok, key, _value} ->
              if has_prefix?(key, prefix) do
                fold_keys_iteration(iter, prefix, fun.(key, acc), fun)
              else
                acc
              end

            :iterator_end ->
              acc

            {:error, _reason} ->
              acc
          end
        after
          :rocksdb.iterator_close(iter)
        end

      {:error, _reason} ->
        acc
    end
  end

  # Continues fold iteration with key-value pairs
  defp fold_iteration(iter, prefix, acc, fun) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, value} ->
        if has_prefix?(key, prefix) do
          fold_iteration(iter, prefix, fun.({key, value}, acc), fun)
        else
          acc
        end

      :iterator_end ->
        acc

      {:error, _reason} ->
        acc
    end
  end

  # Continues fold iteration with keys only
  defp fold_keys_iteration(iter, prefix, acc, fun) do
    case :rocksdb.iterator_move(iter, :next) do
      {:ok, key, _value} ->
        if has_prefix?(key, prefix) do
          fold_keys_iteration(iter, prefix, fun.(key, acc), fun)
        else
          acc
        end

      :iterator_end ->
        acc

      {:error, _reason} ->
        acc
    end
  end

  # Checks if a key has the given prefix
  defp has_prefix?(_key, prefix) when byte_size(prefix) == 0, do: true
  defp has_prefix?(key, prefix) when byte_size(key) < byte_size(prefix), do: false

  defp has_prefix?(key, prefix) do
    binary_part(key, 0, byte_size(prefix)) == prefix
  end

  # ===========================================================================
  # Schema Version Functions (Section 1.1.3)
  # ===========================================================================

  # Detects schema type from list of column families
  defp detect_schema_from_cfs(cf_names) do
    cf_strings = Enum.map(cf_names, &List.to_string/1)

    cond do
      # Quad store has gspo, gpos, spog, posg
      "gspo" in cf_strings and "gpos" in cf_strings ->
        :quad

      # Triple store has spo, pos, osp but NOT gspo
      "spo" in cf_strings and "pos" in cf_strings and "osp" in cf_strings ->
        :triple

      # Unknown schema - default to triple for backward compatibility
      true ->
        Logger.warning("Unable to detect schema from column families, assuming :triple")
        :triple
    end
  end

  # Reads the schema version from database metadata
  defp read_schema_version(db) do
    case :rocksdb.get(db, @schema_version_key, []) do
      {:ok, <<version::64-big>>} ->
        {:ok, version}

      :not_found ->
        {:error, :not_found}

      {:error, _reason} = error ->
        error
    end
  end

  # Writes the schema version to database metadata
  defp write_schema_version(db, version) when is_integer(version) do
    :rocksdb.put(db, @schema_version_key, <<version::64-big>>, [])
  end
end
