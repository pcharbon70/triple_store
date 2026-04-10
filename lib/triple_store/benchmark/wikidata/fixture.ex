defmodule TripleStore.Benchmark.Wikidata.Fixture do
  @moduledoc """
  Local fixture registration and subset generation for Wikidata-style benchmarks.

  Expected fixture layout under the chosen root directory:

      <root>/
        datasets/
          <dataset_id>/
            manifest.term
            data.<ext>
        stores/
          <dataset_id>/

  Dataset manifests are persisted as Erlang terms to keep the foundation layer
  dependency-free while remaining structured and machine-readable.
  """

  alias TripleStore.Benchmark.Wikidata.{Contract, DatasetManifest}

  @manifest_filename "manifest.term"

  @doc """
  Creates the benchmark fixture directory structure.
  """
  @spec ensure_root(Path.t()) :: :ok | {:error, term()}
  def ensure_root(root_dir) when is_binary(root_dir) do
    with :ok <- File.mkdir_p(Path.join(root_dir, "datasets")),
         :ok <- File.mkdir_p(Path.join(root_dir, "stores")) do
      :ok
    end
  end

  @doc """
  Returns the expected paths for a dataset fixture.
  """
  @spec layout_paths(Path.t(), String.t(), atom()) :: map()
  def layout_paths(root_dir, dataset_id, format) do
    dataset_dir = Path.join([root_dir, "datasets", dataset_id])
    store_path = Path.join([root_dir, "stores", dataset_id])
    extension = DatasetManifest.extension_for_format(format)

    %{
      dataset_dir: dataset_dir,
      manifest_path: Path.join(dataset_dir, @manifest_filename),
      data_path: Path.join(dataset_dir, "data#{extension}"),
      store_path: store_path
    }
  end

  @doc """
  Registers a prepared dataset fixture under the benchmark root.
  """
  @spec register_dataset(Path.t(), DatasetManifest.t(), Path.t()) ::
          {:ok, DatasetManifest.t()} | {:error, term()}
  def register_dataset(root_dir, %DatasetManifest{} = manifest, source_path)
      when is_binary(root_dir) and is_binary(source_path) do
    with :ok <- ensure_root(root_dir),
         :ok <-
           File.mkdir_p(layout_paths(root_dir, manifest.dataset_id, manifest.format).dataset_dir),
         {:ok, updated_manifest} <-
           copy_and_persist_manifest(root_dir, manifest, source_path, manifest.dataset_id) do
      {:ok, updated_manifest}
    end
  end

  @doc """
  Loads a persisted dataset manifest from disk.
  """
  @spec load_manifest(Path.t(), String.t()) :: {:ok, DatasetManifest.t()} | {:error, term()}
  def load_manifest(root_dir, dataset_id) when is_binary(root_dir) and is_binary(dataset_id) do
    manifest_path = Path.join([root_dir, "datasets", dataset_id, @manifest_filename])

    with {:ok, binary} <- File.read(manifest_path),
         manifest_map <- :erlang.binary_to_term(binary),
         {:ok, manifest} <- DatasetManifest.new(manifest_map) do
      {:ok, manifest}
    else
      {:error, _} = error -> error
      error -> {:error, error}
    end
  end

  @doc """
  Validates a registered fixture against its on-disk contents.
  """
  @spec validate_registered_dataset(Path.t(), String.t()) :: :ok | {:error, term()}
  def validate_registered_dataset(root_dir, dataset_id)
      when is_binary(root_dir) and is_binary(dataset_id) do
    with {:ok, manifest} <- load_manifest(root_dir, dataset_id),
         true <- File.exists?(manifest.local_data_path) or {:error, :data_file_missing},
         {:ok, checksum} <- DatasetManifest.checksum(manifest.local_data_path),
         {:ok, triple_count} <- DatasetManifest.statement_count(manifest.local_data_path),
         true <- checksum == manifest.checksum or {:error, :checksum_mismatch},
         true <- triple_count == manifest.triple_count or {:error, :triple_count_mismatch} do
      :ok
    end
  end

  @doc """
  Creates a deterministic subset fixture from a registered source dataset.
  """
  @spec create_subset(Path.t(), DatasetManifest.t(), Contract.dataset_tier(), keyword()) ::
          {:ok, DatasetManifest.t()} | {:error, term()}
  def create_subset(root_dir, %DatasetManifest{} = source_manifest, tier, opts \\ [])
      when is_binary(root_dir) and is_list(opts) do
    with true <- tier in [:smoke, :medium] or {:error, :unsupported_subset_tier},
         true <- source_manifest.local_data_path != nil or {:error, :missing_local_data_path},
         :ok <- ensure_line_oriented_format(source_manifest.format),
         {:ok, _tier_meta} <- Contract.dataset_tier(tier) do
      seed = Keyword.get(opts, :seed, 42)

      target =
        min(
          Keyword.get(opts, :target_statements, default_target_statements(tier)),
          source_manifest.triple_count
        )

      subset_id =
        Keyword.get(opts, :dataset_id, "#{source_manifest.dataset_id}-#{tier}-seed#{seed}")

      layout = layout_paths(root_dir, subset_id, source_manifest.format)
      :ok = File.mkdir_p(layout.dataset_dir)

      plan = selection_plan(source_manifest.triple_count, target, seed)
      :ok = write_subset(source_manifest.local_data_path, layout.data_path, plan)

      DatasetManifest.from_source(
        layout.data_path,
        dataset_id: subset_id,
        tier: tier,
        source_url: source_manifest.source_url,
        source_date: source_manifest.source_date,
        dump_version: source_manifest.dump_version,
        format: source_manifest.format,
        normalization_flags: source_manifest.normalization_flags,
        subset_seed: seed,
        subset_strategy: %{
          type: :deterministic_stride,
          seed: seed,
          target_statements: target,
          stride: plan.stride,
          offset: plan.offset
        },
        generated_from: %{
          dataset_id: source_manifest.dataset_id,
          checksum: source_manifest.checksum,
          triple_count: source_manifest.triple_count
        },
        local_data_path: layout.data_path,
        manifest_path: layout.manifest_path
      )
      |> case do
        {:ok, subset_manifest} ->
          persist_manifest(subset_manifest)
          {:ok, subset_manifest}

        {:error, _} = error ->
          error
      end
    end
  end

  @doc """
  Returns the default subset size for a dataset tier.
  """
  @spec default_target_statements(Contract.dataset_tier()) :: pos_integer()
  def default_target_statements(:smoke), do: 1_000
  def default_target_statements(:medium), do: 50_000
  def default_target_statements(:large), do: 1_000_000
  def default_target_statements(:full_dump), do: 10_000_000

  @doc """
  Computes a deterministic subset-selection plan.
  """
  @spec selection_plan(non_neg_integer(), non_neg_integer(), integer()) :: map()
  def selection_plan(total_statements, target_statements, seed)
      when total_statements >= 0 and target_statements >= 0 and is_integer(seed) do
    target =
      cond do
        total_statements == 0 -> 0
        target_statements <= 0 -> 0
        true -> min(total_statements, target_statements)
      end

    stride =
      cond do
        target <= 1 -> max(total_statements, 1)
        total_statements <= target -> 1
        true -> max(div(total_statements, target), 1)
      end

    offset =
      cond do
        stride <= 1 -> 0
        true -> rem(abs(seed), stride)
      end

    %{
      total_statements: total_statements,
      target_statements: target,
      stride: stride,
      offset: offset
    }
  end

  @doc """
  Persists a dataset manifest to disk.
  """
  @spec persist_manifest(DatasetManifest.t()) :: :ok | {:error, term()}
  def persist_manifest(%DatasetManifest{} = manifest) do
    File.write(manifest.manifest_path, :erlang.term_to_binary(DatasetManifest.to_map(manifest)))
  end

  defp copy_and_persist_manifest(root_dir, manifest, source_path, dataset_id) do
    layout = layout_paths(root_dir, dataset_id, manifest.format)

    with :ok <- File.cp(source_path, layout.data_path),
         {:ok, updated_manifest} <-
           DatasetManifest.new(
             DatasetManifest.to_map(manifest)
             |> Map.merge(%{
               local_data_path: layout.data_path,
               manifest_path: layout.manifest_path
             })
           ),
         :ok <- persist_manifest(updated_manifest) do
      {:ok, updated_manifest}
    end
  end

  defp ensure_line_oriented_format(format) when format in [:ntriples, :nquads], do: :ok
  defp ensure_line_oriented_format(_format), do: {:error, :unsupported_subset_format}

  defp write_subset(source_path, destination_path, plan) do
    selection =
      source_path
      |> File.stream!([], :line)
      |> Stream.filter(&DatasetManifest.statement_line?/1)
      |> Stream.with_index()
      |> Stream.filter(fn {_line, index} ->
        keep_statement?(index, plan)
      end)
      |> Stream.map(fn {line, _index} -> line end)
      |> Enum.take(plan.target_statements)

    File.write!(destination_path, selection)
    :ok
  end

  defp keep_statement?(_index, %{target_statements: 0}), do: false
  defp keep_statement?(_index, %{stride: 1}), do: true
  defp keep_statement?(index, %{stride: stride, offset: offset}), do: rem(index, stride) == offset
end
