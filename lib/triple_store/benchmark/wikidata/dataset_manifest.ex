defmodule TripleStore.Benchmark.Wikidata.DatasetManifest do
  @moduledoc """
  Metadata for a benchmark dataset fixture.

  Dataset manifests track the provenance and local registration details needed
  to reproduce benchmark fixtures:

  - source URL and dump version
  - checksum and statement count
  - dataset tier and RDF format
  - normalization flags
  - subset-generation metadata for derived fixtures
  """

  alias TripleStore.Benchmark.Wikidata.Contract

  @enforce_keys [
    :dataset_id,
    :tier,
    :source_url,
    :dump_version,
    :checksum,
    :triple_count,
    :format
  ]
  defstruct dataset_id: nil,
            tier: nil,
            source_url: nil,
            source_date: nil,
            dump_version: nil,
            checksum: nil,
            triple_count: nil,
            format: nil,
            normalization_flags: [],
            subset_seed: nil,
            subset_strategy: nil,
            generated_from: nil,
            local_data_path: nil,
            manifest_path: nil

  @type t :: %__MODULE__{
          dataset_id: String.t(),
          tier: Contract.dataset_tier(),
          source_url: String.t(),
          source_date: Date.t() | String.t() | nil,
          dump_version: String.t(),
          checksum: String.t(),
          triple_count: non_neg_integer(),
          format: atom(),
          normalization_flags: [atom() | String.t()],
          subset_seed: integer() | nil,
          subset_strategy: map() | nil,
          generated_from: map() | nil,
          local_data_path: String.t() | nil,
          manifest_path: String.t() | nil
        }

  @type validation_error ::
          {:dataset_id, String.t()}
          | {:tier, String.t()}
          | {:source_url, String.t()}
          | {:dump_version, String.t()}
          | {:checksum, String.t()}
          | {:triple_count, String.t()}
          | {:format, String.t()}
          | {:normalization_flags, String.t()}

  @doc """
  Builds and validates a dataset manifest.
  """
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, [validation_error()]}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})

    manifest = %__MODULE__{
      dataset_id: Map.get(attrs, :dataset_id),
      tier: Map.get(attrs, :tier),
      source_url: Map.get(attrs, :source_url),
      source_date: Map.get(attrs, :source_date),
      dump_version: Map.get(attrs, :dump_version),
      checksum: Map.get(attrs, :checksum),
      triple_count: Map.get(attrs, :triple_count),
      format: Map.get(attrs, :format),
      normalization_flags: Map.get(attrs, :normalization_flags, []),
      subset_seed: Map.get(attrs, :subset_seed),
      subset_strategy: Map.get(attrs, :subset_strategy),
      generated_from: Map.get(attrs, :generated_from),
      local_data_path: Map.get(attrs, :local_data_path),
      manifest_path: Map.get(attrs, :manifest_path)
    }

    case validate(manifest) do
      :ok -> {:ok, manifest}
      {:error, errors} -> {:error, errors}
    end
  end

  @doc """
  Builds a dataset manifest directly from an RDF source file.
  """
  @spec from_source(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def from_source(source_path, opts) when is_binary(source_path) and is_list(opts) do
    with :ok <- ensure_source_exists(source_path),
         {:ok, dataset_format} <-
           infer_or_validate_format(source_path, Keyword.get(opts, :format)),
         {:ok, checksum} <- checksum(source_path),
         {:ok, triple_count} <- statement_count(source_path) do
      new(
        dataset_id: Keyword.fetch!(opts, :dataset_id),
        tier: Keyword.fetch!(opts, :tier),
        source_url: Keyword.fetch!(opts, :source_url),
        source_date: Keyword.get(opts, :source_date),
        dump_version: Keyword.fetch!(opts, :dump_version),
        checksum: checksum,
        triple_count: triple_count,
        format: dataset_format,
        normalization_flags: Keyword.get(opts, :normalization_flags, []),
        subset_seed: Keyword.get(opts, :subset_seed),
        subset_strategy: Keyword.get(opts, :subset_strategy),
        generated_from: Keyword.get(opts, :generated_from),
        local_data_path: Keyword.get(opts, :local_data_path),
        manifest_path: Keyword.get(opts, :manifest_path)
      )
    end
  end

  @doc """
  Validates a dataset manifest struct.
  """
  @spec validate(t()) :: :ok | {:error, [validation_error()]}
  def validate(%__MODULE__{} = manifest) do
    errors =
      []
      |> validate_dataset_id(manifest)
      |> validate_tier(manifest)
      |> validate_source_url(manifest)
      |> validate_dump_version(manifest)
      |> validate_checksum(manifest)
      |> validate_triple_count(manifest)
      |> validate_format(manifest)
      |> validate_normalization_flags(manifest)

    case errors do
      [] -> :ok
      _ -> {:error, Enum.reverse(errors)}
    end
  end

  @doc """
  Converts the manifest into a plain map for persistence.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = manifest) do
    %{
      dataset_id: manifest.dataset_id,
      tier: manifest.tier,
      source_url: manifest.source_url,
      source_date: manifest.source_date,
      dump_version: manifest.dump_version,
      checksum: manifest.checksum,
      triple_count: manifest.triple_count,
      format: manifest.format,
      normalization_flags: manifest.normalization_flags,
      subset_seed: manifest.subset_seed,
      subset_strategy: manifest.subset_strategy,
      generated_from: manifest.generated_from,
      local_data_path: manifest.local_data_path,
      manifest_path: manifest.manifest_path
    }
  end

  @doc """
  Returns the file extension expected for the RDF format.
  """
  @spec extension_for_format(atom()) :: String.t()
  def extension_for_format(:ntriples), do: ".nt"
  def extension_for_format(:nquads), do: ".nq"
  def extension_for_format(:turtle), do: ".ttl"
  def extension_for_format(:trig), do: ".trig"
  def extension_for_format(:rdfxml), do: ".rdf"
  def extension_for_format(format), do: ".#{format}"

  @doc """
  Counts non-empty RDF statements in a line-oriented source file.
  """
  @spec statement_count(Path.t()) :: {:ok, non_neg_integer()} | {:error, term()}
  def statement_count(path) when is_binary(path) do
    with {:ok, file} <- File.open(path, [:read]) do
      count =
        try do
          file
          |> IO.stream(:line)
          |> Enum.reduce(0, fn line, acc ->
            if statement_line?(line), do: acc + 1, else: acc
          end)
        after
          File.close(file)
        end

      {:ok, count}
    end
  end

  @doc """
  Computes a sha256 checksum for a source file.
  """
  @spec checksum(Path.t()) :: {:ok, String.t()} | {:error, term()}
  def checksum(path) when is_binary(path) do
    with {:ok, file} <- File.open(path, [:read]) do
      digest =
        try do
          file
          |> IO.binstream(2048)
          |> Enum.reduce(:crypto.hash_init(:sha256), fn chunk, acc ->
            :crypto.hash_update(acc, chunk)
          end)
          |> :crypto.hash_final()
          |> Base.encode16(case: :lower)
        after
          File.close(file)
        end

      {:ok, "sha256:#{digest}"}
    end
  end

  @doc """
  Infers the RDF format from a path.
  """
  @spec infer_format(Path.t()) :: {:ok, atom()} | {:error, :unknown_format}
  def infer_format(path) when is_binary(path) do
    case Path.extname(path) do
      ".nt" -> {:ok, :ntriples}
      ".nq" -> {:ok, :nquads}
      ".ttl" -> {:ok, :turtle}
      ".trig" -> {:ok, :trig}
      ".rdf" -> {:ok, :rdfxml}
      _ -> {:error, :unknown_format}
    end
  end

  @doc """
  Returns true when the line should count as an RDF statement in a line-oriented fixture.
  """
  @spec statement_line?(String.t()) :: boolean()
  def statement_line?(line) when is_binary(line) do
    trimmed = String.trim(line)
    trimmed != "" and not String.starts_with?(trimmed, "#")
  end

  defp ensure_source_exists(source_path) do
    if File.exists?(source_path), do: :ok, else: {:error, :source_not_found}
  end

  defp infer_or_validate_format(_source_path, format) when is_atom(format) and not is_nil(format),
    do: {:ok, format}

  defp infer_or_validate_format(source_path, nil), do: infer_format(source_path)

  defp validate_dataset_id(errors, %__MODULE__{dataset_id: dataset_id})
       when is_binary(dataset_id) and dataset_id != "",
       do: errors

  defp validate_dataset_id(errors, _manifest),
    do: [{:dataset_id, "must be a non-empty string"} | errors]

  defp validate_tier(errors, %__MODULE__{tier: tier}) do
    if Contract.dataset_tier?(tier) do
      errors
    else
      [{:tier, "must be one of #{inspect(Enum.map(Contract.dataset_tiers(), & &1.id))}"} | errors]
    end
  end

  defp validate_source_url(errors, %__MODULE__{source_url: source_url})
       when is_binary(source_url) and source_url != "",
       do: errors

  defp validate_source_url(errors, _manifest),
    do: [{:source_url, "must be a non-empty string"} | errors]

  defp validate_dump_version(errors, %__MODULE__{dump_version: dump_version})
       when is_binary(dump_version) and dump_version != "",
       do: errors

  defp validate_dump_version(errors, _manifest),
    do: [{:dump_version, "must be a non-empty string"} | errors]

  defp validate_checksum(errors, %__MODULE__{checksum: checksum})
       when is_binary(checksum) and checksum != "",
       do: errors

  defp validate_checksum(errors, _manifest),
    do: [{:checksum, "must be a non-empty string"} | errors]

  defp validate_triple_count(errors, %__MODULE__{triple_count: triple_count})
       when is_integer(triple_count) and triple_count >= 0,
       do: errors

  defp validate_triple_count(errors, _manifest),
    do: [{:triple_count, "must be a non-negative integer"} | errors]

  defp validate_format(errors, %__MODULE__{format: format}) when is_atom(format), do: errors

  defp validate_format(errors, _manifest),
    do: [{:format, "must be an RDF format atom"} | errors]

  defp validate_normalization_flags(errors, %__MODULE__{normalization_flags: flags})
       when is_list(flags) do
    if Enum.all?(flags, &(is_atom(&1) or is_binary(&1))) do
      errors
    else
      [{:normalization_flags, "must contain only atoms or strings"} | errors]
    end
  end

  defp validate_normalization_flags(errors, _manifest),
    do: [{:normalization_flags, "must be a list"} | errors]
end
