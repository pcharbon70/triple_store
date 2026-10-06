defmodule TripleStore.Benchmark.Artifact do
  @moduledoc """
  Suite-neutral file provenance helpers for benchmark artifacts.

  The module centralizes streaming checksums, line counts, and atomic copies so
  benchmark suites do not need to duplicate Wikidata-specific implementations.
  """

  @chunk_size 1_048_576

  @doc "Computes a lowercase SHA-256 checksum without loading the file into memory."
  @spec checksum(Path.t()) :: {:ok, String.t()} | {:error, term()}
  def checksum(path) when is_binary(path) do
    with {:ok, file} <- File.open(path, [:read, :binary]) do
      digest =
        try do
          file
          |> IO.binstream(@chunk_size)
          |> Enum.reduce(:crypto.hash_init(:sha256), &:crypto.hash_update(&2, &1))
          |> :crypto.hash_final()
          |> Base.encode16(case: :lower)
        after
          File.close(file)
        end

      {:ok, "sha256:#{digest}"}
    end
  end

  @doc "Checks a file against a canonical `sha256:<hex>` digest."
  @spec verify_checksum(Path.t(), String.t()) :: :ok | {:error, term()}
  def verify_checksum(path, "sha256:" <> digest = expected) when byte_size(digest) == 64 do
    case checksum(path) do
      {:ok, ^expected} -> :ok
      {:ok, actual} -> {:error, {:checksum_mismatch, expected, actual}}
      {:error, reason} -> {:error, reason}
    end
  end

  def verify_checksum(_path, checksum), do: {:error, {:invalid_checksum, checksum}}

  @doc "Counts non-empty, non-comment lines using bounded memory."
  @spec statement_count(Path.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def statement_count(path) when is_binary(path) do
    with {:ok, file} <- File.open(path, [:read]) do
      count =
        try do
          file
          |> IO.stream(:line)
          |> Enum.count(&statement_line?/1)
        after
          File.close(file)
        end

      {:ok, count}
    end
  end

  @doc "Returns whether a line contains a statement rather than whitespace or a comment."
  @spec statement_line?(String.t()) :: boolean()
  def statement_line?(line) when is_binary(line) do
    trimmed = String.trim(line)
    trimmed != "" and not String.starts_with?(trimmed, "#")
  end

  @doc "Copies a file through a sibling temporary path and atomically promotes it."
  @spec atomic_copy(Path.t(), Path.t()) :: :ok | {:error, term()}
  def atomic_copy(source, destination) when is_binary(source) and is_binary(destination) do
    temporary = destination <> ".partial.#{System.unique_integer([:positive])}"

    with :ok <- File.mkdir_p(Path.dirname(destination)),
         {:ok, _bytes} <- File.copy(source, temporary),
         :ok <- File.rename(temporary, destination) do
      :ok
    else
      {:error, reason} = error ->
        File.rm(temporary)
        if reason == :eexist, do: {:error, :destination_exists}, else: error
    end
  end

  @doc "Returns the recursive byte size of a file or directory."
  @spec size(Path.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def size(path) when is_binary(path) do
    cond do
      File.regular?(path) ->
        case File.stat(path) do
          {:ok, stat} -> {:ok, stat.size}
          {:error, _} = error -> error
        end

      File.dir?(path) ->
        directory_size(path)

      true ->
        {:error, :not_found}
    end
  end

  defp directory_size(path) do
    path
    |> Path.join("**/*")
    |> Path.wildcard(match_dot: true)
    |> Enum.reduce_while({:ok, 0}, &add_entry_size/2)
  end

  defp add_entry_size(entry, {:ok, total}) do
    case File.stat(entry) do
      {:ok, %{type: :regular, size: bytes}} -> {:cont, {:ok, total + bytes}}
      {:ok, _stat} -> {:cont, {:ok, total}}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  @doc "Infers a supported RDF syntax from a file extension."
  @spec infer_rdf_format(Path.t()) :: {:ok, atom()} | {:error, :unknown_format}
  def infer_rdf_format(path) when is_binary(path) do
    case Path.extname(path) do
      ".nt" -> {:ok, :ntriples}
      ".nq" -> {:ok, :nquads}
      ".ttl" -> {:ok, :turtle}
      ".trig" -> {:ok, :trig}
      ".rdf" -> {:ok, :rdfxml}
      _ -> {:error, :unknown_format}
    end
  end
end
