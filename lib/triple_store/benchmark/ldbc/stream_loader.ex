defmodule TripleStore.Benchmark.LDBC.StreamLoader do
  @moduledoc """
  Bounded-memory RDF loading and verification for LDBC benchmark fixtures.

  Parsed RDF terms pass through `TripleStore.Adapter` and every encoded batch
  passes through `TripleStore.QuadOperations`, preserving dictionary and atomic
  four-index write invariants. Timing buckets deliberately separate parsing,
  RDF shape mapping, dictionary allocation, and RocksDB writes.
  """

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.LDBC.{DatasetManifest, RDFStream}
  alias TripleStore.QuadOperations

  @quad_indices [:gspo, :gpos, :spog, :posg]

  @type metrics :: %{
          count: non_neg_integer(),
          elapsed_us: non_neg_integer(),
          parse_us: non_neg_integer(),
          mapping_us: non_neg_integer(),
          dictionary_us: non_neg_integer(),
          write_us: non_neg_integer(),
          throughput_statements_per_second: float(),
          warnings: [String.t()],
          memory_high_water_bytes: non_neg_integer(),
          store_size_bytes: non_neg_integer() | nil
        }

  @doc "Loads the manifest's initial RDF component into an open quad store."
  @spec load(TripleStore.store(), DatasetManifest.t(), keyword()) ::
          {:ok, metrics()} | {:error, term()}
  def load(store, manifest, opts \\ [])

  def load(%{schema: :quad} = store, %DatasetManifest{} = manifest, opts) do
    batch_size =
      Keyword.get(opts, :batch_size, manifest.store.loader_settings[:batch_size] || 10_000)

    cancel? = Keyword.get(opts, :cancel?, fn -> false end)

    with :ok <- validate_options(batch_size, cancel?),
         :ok <- DatasetManifest.validate(manifest),
         {:ok, component} <- initial_component(manifest),
         :ok <- Artifact.verify_checksum(component.path, component.checksum),
         {:ok, format} <- rdf_format(component.path) do
      stream_batches(store, component, format, batch_size, cancel?)
    end
  end

  def load(%{schema: schema}, %DatasetManifest{}, _opts),
    do: {:error, {:unsupported_store_schema, schema}}

  @doc "Verifies quad schema metadata and equal statement counts in all explicit indices."
  @spec verify(TripleStore.store(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def verify(%{db: db, schema: :quad}, expected_count) when is_integer(expected_count) do
    with {:ok, true} <- ErlangAdapter.is_quad_store?(db),
         {:ok, counts} <- index_counts(db),
         true <- Enum.all?(counts, fn {_index, count} -> count == expected_count end) do
      {:ok, %{schema: :quad, statement_count: expected_count, index_counts: counts}}
    else
      {:ok, false} -> {:error, :quad_schema_metadata_mismatch}
      false -> {:error, {:quad_index_count_mismatch, expected_count}}
      {:error, _} = error -> error
    end
  end

  def verify(%{schema: schema}, _expected_count),
    do: {:error, {:unsupported_store_schema, schema}}

  defp stream_batches(store, component, format, batch_size, cancel?) do
    started = System.monotonic_time(:microsecond)
    memory = :erlang.memory(:total)

    initial = %{
      count: 0,
      parse_us: 0,
      mapping_us: 0,
      dictionary_us: 0,
      write_us: 0,
      memory_high_water_bytes: memory
    }

    result =
      component.path
      |> File.stream!(:line)
      |> Stream.with_index(1)
      |> Stream.chunk_every(batch_size)
      |> Enum.reduce_while(
        {:ok, initial},
        &reduce_batch(&1, &2, store, format, cancel?, started)
      )

    case result do
      {:ok, %{count: count} = metrics} when count == component.count ->
        {:ok, finalize(metrics, started, store.path)}

      {:ok, metrics} ->
        {:error,
         {:statement_count_mismatch, component.count, metrics.count,
          finalize(metrics, started, store.path)}}

      {:error, _} = error ->
        error
    end
  rescue
    error in File.Error -> {:error, {:file_error, error.reason}}
  end

  defp reduce_batch(lines, {:ok, metrics}, store, format, cancel?, started) do
    if cancel?.() do
      {:halt, {:error, {:load_cancelled, finalize(metrics, started, store.path)}}}
    else
      continue_load(load_batch(store, lines, format, metrics), store.path, started)
    end
  end

  defp continue_load({:ok, updated}, _path, _started), do: {:cont, {:ok, updated}}

  defp continue_load({:error, reason, updated}, path, started) do
    {:halt, {:error, {:load_failed, reason, finalize(updated, started, path)}}}
  end

  defp load_batch(store, lines, format, metrics) do
    {parse_us, parsed} = :timer.tc(fn -> parse_lines(lines, format) end)
    metrics = add_time(metrics, :parse_us, parse_us)

    case parsed do
      {:ok, statements} -> load_statements(store, statements, metrics)
      {:error, reason} -> {:error, reason, metrics}
    end
  end

  defp load_statements(store, statements, metrics) do
    {mapping_us, quads} = :timer.tc(fn -> Enum.map(statements, &as_quad/1) end)
    metrics = add_time(metrics, :mapping_us, mapping_us)

    {dictionary_us, encoded} =
      :timer.tc(fn -> Adapter.from_rdf_quads(store.dict_manager, quads) end)

    metrics = add_time(metrics, :dictionary_us, dictionary_us)

    case encoded do
      {:ok, encoded_quads} -> write_encoded(store, encoded_quads, metrics)
      {:error, reason} -> {:error, {:dictionary_failed, reason}, metrics}
    end
  end

  defp write_encoded(store, encoded_quads, metrics) do
    {write_us, result} =
      :timer.tc(fn -> QuadOperations.insert_quads(store.db, encoded_quads, sync: false) end)

    updated =
      metrics
      |> add_time(:write_us, write_us)
      |> Map.update!(:count, &(&1 + length(encoded_quads)))
      |> sample_memory()

    case result do
      :ok -> {:ok, updated}
      {:error, reason} -> {:error, {:write_failed, reason}, updated}
    end
  end

  defp parse_lines(lines, format) do
    Enum.reduce_while(lines, {:ok, []}, &parse_indexed_line(&1, &2, format))
    |> case do
      {:ok, statements} -> {:ok, Enum.reverse(statements)}
      error -> error
    end
  end

  defp parse_indexed_line({line, line_number}, {:ok, statements}, format) do
    if Artifact.statement_line?(line) do
      parse_statement(line, line_number, statements, format)
    else
      {:cont, {:ok, statements}}
    end
  end

  defp parse_statement(line, line_number, statements, format) do
    case RDFStream.parse_line(line, format) do
      {:ok, statement} -> {:cont, {:ok, [statement | statements]}}
      {:error, reason} -> {:halt, {:error, {:rdf_parse_error, line_number, reason}}}
    end
  end

  defp as_quad({subject, predicate, object}), do: {subject, predicate, object, nil}
  defp as_quad({_subject, _predicate, _object, _graph} = quad), do: quad

  defp index_counts(db) do
    Enum.reduce_while(@quad_indices, {:ok, %{}}, &count_index(&1, &2, db))
  end

  defp count_index(index, {:ok, counts}, db) do
    case ErlangAdapter.fold_keys(db, index, <<>>, 0, fn _key, count -> count + 1 end,
           fill_cache: false
         ) do
      count when is_integer(count) -> {:cont, {:ok, Map.put(counts, index, count)}}
      {:error, reason} -> {:halt, {:error, {:index_verification_failed, index, reason}}}
    end
  end

  defp finalize(metrics, started, path) do
    elapsed_us = max(System.monotonic_time(:microsecond) - started, 0)

    size =
      case Artifact.size(path) do
        {:ok, bytes} -> bytes
        _ -> nil
      end

    throughput = if elapsed_us == 0, do: 0.0, else: metrics.count * 1_000_000 / elapsed_us

    metrics
    |> Map.put(:elapsed_us, elapsed_us)
    |> Map.put(:throughput_statements_per_second, throughput)
    |> Map.put(:warnings, [])
    |> Map.put(:store_size_bytes, size)
  end

  defp add_time(metrics, key, duration), do: Map.update!(metrics, key, &(&1 + duration))

  defp sample_memory(metrics) do
    Map.update!(metrics, :memory_high_water_bytes, &max(&1, :erlang.memory(:total)))
  end

  defp initial_component(manifest) do
    case Enum.filter(manifest.components, &(&1.role == :initial)) do
      [component] -> {:ok, component}
      [] -> {:error, :missing_initial_component}
      _ -> {:error, :multiple_initial_components}
    end
  end

  defp rdf_format(path) do
    case Artifact.infer_rdf_format(path) do
      {:ok, format} when format in [:ntriples, :nquads] -> {:ok, format}
      {:ok, format} -> {:error, {:unsupported_rdf_format, format}}
      {:error, _} = error -> error
    end
  end

  defp validate_options(batch_size, cancel?)
       when is_integer(batch_size) and batch_size > 0 and is_function(cancel?, 0),
       do: :ok

  defp validate_options(_batch_size, _cancel?), do: {:error, :invalid_load_options}
end
