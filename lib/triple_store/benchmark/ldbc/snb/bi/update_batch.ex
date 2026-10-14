defmodule TripleStore.Benchmark.LDBC.SNB.BI.UpdateBatch do
  @moduledoc """
  Ordered and atomic application of one canonical SNB BI microbatch.

  All records are decoded and dictionary-encoded before a single mixed RocksDB
  batch is submitted. Cache and statistics callbacks run only after commit.
  The returned receipt binds the batch position to the source checksum.
  """

  alias TripleStore.Adapter
  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.LDBC.SNB.BI.UpdateSchema
  alias TripleStore.Benchmark.LDBC.SNB.UpdateStream
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Update.Helpers
  alias TripleStore.Statistics

  @doc "Applies one update stream as an all-or-nothing storage mutation."
  @spec apply(TripleStore.store(), Path.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def apply(store, path, opts \\ []) do
    with {:ok, checksum} <- Artifact.checksum(path),
         {:ok, records} <- read_records(path),
         {:ok, mutations} <- encode_records(store, records),
         :ok <- write(store.db, mutations, opts),
         :ok <- after_commit(store, opts) do
      {:ok,
       %{
         checksum: checksum,
         records: length(records),
         mutations: length(mutations),
         first_sequence: records |> List.first() |> Map.fetch!(:sequence),
         last_sequence: records |> List.last() |> Map.fetch!(:sequence)
       }}
    end
  rescue
    error in File.Error -> {:error, {:update_stream_failed, error.reason}}
  end

  defp read_records(path) do
    path
    |> UpdateStream.stream()
    |> Enum.reduce_while({:ok, []}, fn
      %{sequence: _sequence, type: _type, fields: _fields} = record, {:ok, records} ->
        {:cont, {:ok, [record | records]}}

      %{sequence: _sequence, operation: operation, quads: quads} = record, {:ok, records}
      when operation in [:insert, :delete] and is_list(quads) ->
        {:cont, {:ok, [record | records]}}

      {:error, reason}, _acc ->
        {:halt, {:error, reason}}

      record, _acc ->
        {:halt, {:error, {:invalid_or_reordered_update, record}}}
    end)
    |> case do
      {:ok, []} ->
        {:error, :empty_update_batch}

      {:ok, records} ->
        records = Enum.reverse(records)

        if Enum.all?(records, &match?(%{type: _, fields: _}, &1)) do
          with :ok <- UpdateSchema.validate_records(records), do: {:ok, records}
        else
          with :ok <- validate_legacy_records(records), do: {:ok, records}
        end

      {:error, _} = error ->
        error
    end
  end

  defp encode_records(store, records) do
    if Enum.all?(records, &match?(%{type: _, fields: _}, &1)) do
      encode_canonical_records(store, records)
    else
      encode_legacy_records(store, records)
    end
  end

  defp encode_canonical_records(store, records) do
    with {:ok, translated} <- UpdateSchema.translate_records(store, records) do
      Enum.reduce_while(translated, {:ok, []}, fn mutation, {:ok, mutations} ->
        case Adapter.from_rdf_quads(store.dict_manager, mutation.quads) do
          {:ok, encoded} ->
            tagged = Enum.map(encoded, &{mutation.operation, &1})
            {:cont, {:ok, mutations ++ tagged}}

          {:error, reason} ->
            {:halt, {:error, {:update_encoding_failed, mutation.sequence, reason}}}
        end
      end)
    end
  end

  defp encode_legacy_records(store, records) do
    Enum.reduce_while(records, {:ok, []}, fn record, {:ok, mutations} ->
      case Adapter.from_rdf_quads(store.dict_manager, record.quads) do
        {:ok, encoded} ->
          tagged = Enum.map(encoded, &{record.operation, &1})
          {:cont, {:ok, mutations ++ tagged}}

        {:error, reason} ->
          {:halt, {:error, {:update_encoding_failed, record.sequence, reason}}}
      end
    end)
  end

  defp validate_legacy_records(records) do
    records
    |> Enum.reduce_while(-1, fn record, previous ->
      if is_integer(record.sequence) and record.sequence > previous,
        do: {:cont, record.sequence},
        else: {:halt, {:error, {:invalid_or_reordered_update, record}}}
    end)
    |> case do
      {:error, _} = error -> error
      _sequence -> :ok
    end
  end

  defp write(db, mutations, opts) do
    writer = Keyword.get(opts, :writer, &QuadOperations.apply_mutations/3)

    case writer.(db, mutations, sync: true) do
      :ok -> :ok
      {:error, reason} -> {:error, {:atomic_update_failed, reason}}
    end
  end

  defp after_commit(store, opts) do
    cache_invalidator = Keyword.get(opts, :cache_invalidator, &Helpers.invalidate_result_caches/1)

    statistics_invalidator =
      Keyword.get(opts, :statistics_invalidator, &Statistics.invalidate_all_quad_cache/1)

    with :ok <- normalize_post_commit(cache_invalidator.(store.db)),
         :ok <- normalize_post_commit(statistics_invalidator.(store.db)) do
      opts
      |> Keyword.get(:after_commit, fn _store -> :ok end)
      |> then(& &1.(store))
      |> normalize_post_commit()
    end
  end

  defp normalize_post_commit(result) do
    case result do
      :ok -> :ok
      {:ok, _value} -> :ok
      {:error, reason} -> {:error, {:post_commit_refresh_failed, reason}}
      other -> {:error, {:invalid_post_commit_result, other}}
    end
  end
end
