# Query Helpers - Shared utilities for codebase insight scripts
#
# This module provides common functions used across all query scripts.
# It is automatically loaded when you run any of the example scripts.

defmodule QueryHelpers do
  @moduledoc """
  Shared utility functions for RDF query scripts.

  Provides:
  - RDF term extraction
  - Store connection management
  - Output formatting helpers
  """

  @default_data_path "./tmp/ash_data"

  @doc """
  Extract a readable value from RDF term tuples.

  RDF terms come back from queries in tuple format:
  - `{:literal, :simple, "value"}` - plain string literal
  - `{:literal, :typed, "42", "xsd:integer"}` - typed literal
  - `{:named_node, "http://..."}` - URI/IRI

  This function extracts the human-readable value.
  """
  def extract({:literal, :simple, val}), do: val
  def extract({:literal, :typed, val, _type}), do: val
  def extract({:named_node, url}), do: url
  def extract(nil), do: nil
  def extract(other), do: inspect(other)

  @doc """
  Get the short name from a fully qualified module name.

  ## Examples

      iex> short_name("Ash.Resource.Info")
      "Info"
  """
  def short_name(full_name) when is_binary(full_name) do
    full_name |> String.split(".") |> List.last()
  end
  def short_name(other), do: extract(other)

  @doc """
  Extract caller module from a call site URI.

  Call sites are encoded as URIs like:
  `https://example.org/code#call/Ash.Resource.Change/module/0/141`

  This extracts "Ash.Resource.Change" from that URI.
  """
  def extract_caller_module(callsite_uri) when is_binary(callsite_uri) do
    case String.split(callsite_uri, ["#call/", "/module"]) do
      [_, mod | _] -> mod
      _ -> "unknown"
    end
  end
  def extract_caller_module(_), do: "unknown"

  @doc """
  Open the triple store and run a function with it.

  Handles opening, error reporting, and closing automatically.

  ## Options

  - `:data_path` - Path to the RocksDB data directory (default: "./tmp/ash_data")

  ## Examples

      QueryHelpers.with_store(fn store ->
        {:ok, results} = TripleStore.query(store, "SELECT ...")
        process(results)
      end)
  """
  def with_store(opts \\ [], func) when is_function(func, 1) do
    data_path = Keyword.get(opts, :data_path, @default_data_path)

    case TripleStore.open(data_path) do
      {:ok, store} ->
        try do
          func.(store)
        after
          TripleStore.close(store)
        end

      {:error, reason} ->
        IO.puts("Failed to open store at #{data_path}: #{inspect(reason)}")
        IO.puts("\nMake sure you've loaded the data first:")
        IO.puts("  mix run -e '{:ok, s} = TripleStore.open(\"#{data_path}\"); TripleStore.load(s, \"examples/ash.ttl\"); TripleStore.close(s)'")
        {:error, reason}
    end
  end

  @doc """
  Print a section header with title and description.
  """
  def header(title, description \\ nil) do
    IO.puts(String.duplicate("=", 70))
    IO.puts(title)
    if description, do: IO.puts(description)
    IO.puts(String.duplicate("=", 70))
    IO.puts("")
  end

  @doc """
  Print a sub-section separator.
  """
  def separator do
    IO.puts("  " <> String.duplicate("-", 60))
  end

  @doc """
  Pad a number for aligned output.
  """
  def pad_num(n, width \\ 5) do
    String.pad_leading(Integer.to_string(n), width)
  end

  @doc """
  Create a simple bar chart string.
  """
  def bar(count, scale \\ 1, max_width \\ 30) do
    width = min(trunc(count / scale), max_width)
    String.duplicate("█", width)
  end
end
