defmodule TripleStore.SPARQL.Parser.NIF do
  @moduledoc """
  NIF bindings for SPARQL parsing using spargebra.

  This module contains the low-level NIF function declarations that interface
  with the Rust implementation in `native/sparql_parser_nif`.

  ## Usage

  These functions should not be called directly. Use the higher-level
  `TripleStore.SPARQL.Parser` module instead.

  ## Configuration

  To skip NIF compilation during development (when Rust is not installed),
  set the environment variable `RUSTLER_SKIP_COMPILATION=1`.
  """

  @skip_compilation System.get_env("RUSTLER_SKIP_COMPILATION") == "1"

  use Rustler,
    otp_app: :triple_store,
    crate: "sparql_parser_nif",
    skip_compilation?: @skip_compilation

  @doc """
  Verifies that the NIF is loaded correctly.

  Returns `"sparql_parser_nif"` if the NIF is operational.

  ## Examples

      iex> TripleStore.SPARQL.Parser.NIF.nif_loaded()
      "sparql_parser_nif"

  """
  @spec nif_loaded :: String.t()
  def nif_loaded, do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Parses a SPARQL query string into an AST.

  ## Arguments
  - `sparql` - The SPARQL query string to parse

  ## Returns
  - `{:ok, ast}` on success where ast is the Elixir representation
  - `{:error, {:parse_error, message}}` on parse failure

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.NIF.parse_query("SELECT ?s WHERE { ?s ?p ?o }")
      iex> elem(ast, 0)
      :select

  """
  @spec parse_query(String.t()) :: {:ok, term()} | {:error, {:parse_error, String.t()}}
  def parse_query(_sparql), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Parses a SPARQL UPDATE string into an AST.

  ## Arguments
  - `sparql` - The SPARQL UPDATE string to parse

  ## Returns
  - `{:ok, ast}` on success where ast is the Elixir representation
  - `{:error, {:parse_error, message}}` on parse failure

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.NIF.parse_update("INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }")
      iex> elem(ast, 0)
      :update

  """
  @spec parse_update(String.t()) :: {:ok, term()} | {:error, {:parse_error, String.t()}}
  def parse_update(_sparql), do: :erlang.nif_error(:nif_not_loaded)
end
