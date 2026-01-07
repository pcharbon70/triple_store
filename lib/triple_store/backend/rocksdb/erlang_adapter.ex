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

  ## Usage

  ```elixir
  # Open a database
  {:ok, adapter} = ErlangAdapter.open("/path/to/db")

  # Perform operations
  :ok = ErlangAdapter.put(adapter, :spo, key, value)
  {:ok, value} = ErlangAdapter.get(adapter, :spo, key)

  # Close the database
  :ok = ErlangAdapter.close(adapter)
  ```

  ## Error Handling

  All functions use `with` clauses for error propagation and return
  standardized error tuples:
  - `{:error, reason}` - Operation failed
  - `:not_found` - Key does not exist (for get operations)
  """

  use GenServer
  require Logger

  alias TripleStore.Backend.RocksDB.ColumnFamilyConfig

  @type adapter :: pid()
  @type db_ref :: reference()
  @type column_family :: :id2str | :str2id | :spo | :pos | :osp | :derived | :numeric_range
  @type cf_handle :: reference()
  @type cf_name :: charlist()

  # ===========================================================================
  # Client API
  # ===========================================================================

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

  ## Returns

  - `{:ok, adapter}` - Database opened successfully
  - `{:error, reason}` - Failed to open database

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
  @spec get(adapter(), column_family(), binary()) :: {:ok, binary()} | :not_found | {:error, term()}
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
  @spec delete_batch(adapter(), [{column_family(), binary()}], boolean()) :: :ok | {:error, term()}
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
  @spec mixed_batch(adapter(), [{:put, column_family(), binary(), binary()} | {:delete, column_family(), binary()}], boolean()) :: :ok | {:error, term()}
  def mixed_batch(adapter, operations, sync \\ false)
      when is_pid(adapter) and is_list(operations) do
    GenServer.call(adapter, {:mixed_batch, operations, sync})
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init({path, opts}) do
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
    case open_or_create_database(db_path, db_opts, path) do
      {:ok, db, cf_handles} ->
        # Map column family names to atoms
        cf_map = map_cf_handles(cf_handles)

        {:ok, %{db: db, cf_handles: cf_map, path: path}}

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
  def handle_call({:write_batch, operations, sync}, _from, %{db: db, cf_handles: cf_handles} = state) do
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
  def handle_call({:delete_batch, operations, sync}, _from, %{db: db, cf_handles: cf_handles} = state) do
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
  def handle_call({:mixed_batch, operations, sync}, _from, %{db: db, cf_handles: cf_handles} = state) do
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
      end)

    if Enum.any?(batch, &match?({:error, _}, &1)) do
      {:reply, {:error, :invalid_column_family}, state}
    else
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
  defp open_or_create_database(db_path, db_opts, original_path) do
    db_exists = database_exists?(original_path)

    cond do
      db_exists and Keyword.get(db_opts, :error_if_exists, false) ->
        {:error, :database_already_exists}

      db_exists ->
        # Open existing database with all CFs
        open_existing_database(db_path, db_opts)

      true ->
        # Create new database with all CFs
        create_new_database(db_path, db_opts)
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
  defp open_existing_database(db_path, db_opts) do
    # Get all column family descriptors
    cf_descriptors = ColumnFamilyConfig.cf_descriptors()

    # Convert to charlist format for erlang-rocksdb
    cf_descriptors_charlist = Enum.map(cf_descriptors, fn {name, opts} -> {String.to_charlist(name), opts} end)

    case :rocksdb.open_with_cf(db_path, db_opts, cf_descriptors_charlist) do
      {:ok, db, cf_handles} ->
        {:ok, db, cf_handles}

      {:error, _reason} = error ->
        error
    end
  end

  # Creates a new database with all column families
  defp create_new_database(db_path, db_opts) do
    # First, open with just the default column family
    case :rocksdb.open_with_cf(db_path, db_opts, [{~c"default", []}]) do
      {:ok, db, [default_cf]} ->
        # Create additional column families
        with {:ok, id2str_cf} <- :rocksdb.create_column_family(db, ~c"id2str", []),
             {:ok, str2id_cf} <- :rocksdb.create_column_family(db, ~c"str2id", []),
             {:ok, spo_cf} <- :rocksdb.create_column_family(db, ~c"spo", []),
             {:ok, pos_cf} <- :rocksdb.create_column_family(db, ~c"pos", []),
             {:ok, osp_cf} <- :rocksdb.create_column_family(db, ~c"osp", []),
             {:ok, derived_cf} <- :rocksdb.create_column_family(db, ~c"derived", []),
             {:ok, numeric_cf} <- :rocksdb.create_column_family(db, ~c"numeric_range", []) do
          {:ok, db, [default_cf, id2str_cf, str2id_cf, spo_cf, pos_cf, osp_cf, derived_cf, numeric_cf]}
        else
          {:error, _reason} = error ->
            # Clean up on error
            :rocksdb.close(db)
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  # Maps column family handles to their atom names
  defp map_cf_handles(cf_handles) do
    # The order of cf_handles matches the order we opened them
    # [default, id2str, str2id, spo, pos, osp, derived, numeric_range]
    cf_names_in_order = [:default, :id2str, :str2id, :spo, :pos, :osp, :derived, :numeric_range]

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
  defp validate_path(path) when is_binary(path) do
    expanded_path = Path.expand(path)
    tmp_dir = Path.expand(System.tmp_dir!())
    is_absolute = String.starts_with?(path, "/")

    cond do
      # Check for path traversal attempts
      String.contains?(path, "..") ->
        {:error, :path_traversal_attempt}

      # Check for suspicious characters
      String.contains?(path, "\0") ->
        {:error, :null_byte_in_path}

      # Allow relative paths (will be expanded to current working directory)
      not is_absolute ->
        :ok

      # Allow paths under /tmp
      String.starts_with?(expanded_path, tmp_dir) ->
        :ok

      # Allow paths under the current project directory
      String.starts_with?(expanded_path, Path.expand(File.cwd!())) ->
        :ok

      # Reject other absolute paths for security
      is_absolute ->
        {:error, :absolute_path_not_allowed}

      true ->
        :ok
    end
  end

  # Ensures the database directory exists
  defp ensure_directory_exists(path) do
    case File.mkdir_p(path) do
      :ok -> :ok
      {:error, reason} -> {:error, {:mkdir_failed, reason}}
    end
  end
end
