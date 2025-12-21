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
  """

  # Skip compilation if RUSTLER_SKIP_COMPILATION is set
  # This allows development without Rust installed
  @skip_compilation System.get_env("RUSTLER_SKIP_COMPILATION") == "1"

  use Rustler,
    otp_app: :triple_store,
    crate: "rocksdb_nif",
    skip_compilation?: @skip_compilation

  @doc """
  Verifies that the NIF is loaded correctly.

  Returns `"rocksdb_nif"` if the NIF is operational.

  ## Examples

      iex> TripleStore.Backend.RocksDB.NIF.nif_loaded()
      "rocksdb_nif"

  """
  @spec nif_loaded :: String.t()
  def nif_loaded, do: :erlang.nif_error(:nif_not_loaded)
end
