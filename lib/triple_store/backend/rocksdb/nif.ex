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
end
