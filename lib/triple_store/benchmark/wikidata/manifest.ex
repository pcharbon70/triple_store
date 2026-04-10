defmodule TripleStore.Benchmark.Wikidata.Manifest do
  @moduledoc """
  Validated manifest representation for Wikidata-style benchmark queries.

  A manifest describes a benchmark query or query template together with the
  dataset tier, provenance, required output artifacts, and run metadata that
  must be captured when the benchmark is executed.
  """

  alias TripleStore.Benchmark.Wikidata.Contract

  @enforce_keys [:benchmark_id, :suite, :category, :source, :dataset]
  defstruct version: 1,
            benchmark_id: nil,
            suite: nil,
            category: nil,
            execution_variant: :raw,
            source: nil,
            tags: [],
            dataset: nil,
            required_artifacts: Contract.required_result_artifacts(),
            success_criteria: Contract.success_criteria(),
            required_run_metadata: Contract.required_run_metadata_fields()

  @type t :: %__MODULE__{
          version: pos_integer(),
          benchmark_id: String.t(),
          suite: Contract.workload_family(),
          category: atom() | String.t(),
          execution_variant: Contract.execution_variant(),
          source: map(),
          tags: [atom() | String.t()],
          dataset: map(),
          required_artifacts: [Contract.result_artifact()],
          success_criteria: map(),
          required_run_metadata: [atom()]
        }

  @type validation_error ::
          {:benchmark_id, String.t()}
          | {:suite, String.t()}
          | {:execution_variant, String.t()}
          | {:source, String.t()}
          | {:dataset, String.t()}
          | {:required_artifacts, String.t()}
          | {:required_run_metadata, String.t()}
          | {:version, String.t()}
          | {:tags, String.t()}

  @doc """
  Builds and validates a manifest.
  """
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, [validation_error()]}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})

    manifest = %__MODULE__{
      version: Map.get(attrs, :version, 1),
      benchmark_id: Map.get(attrs, :benchmark_id),
      suite: Map.get(attrs, :suite),
      category: Map.get(attrs, :category),
      execution_variant: Map.get(attrs, :execution_variant, :raw),
      source: Map.get(attrs, :source),
      tags: Map.get(attrs, :tags, []),
      dataset: Map.get(attrs, :dataset),
      required_artifacts:
        Map.get(attrs, :required_artifacts, Contract.required_result_artifacts()),
      success_criteria: Map.get(attrs, :success_criteria, Contract.success_criteria()),
      required_run_metadata:
        Map.get(attrs, :required_run_metadata, Contract.required_run_metadata_fields())
    }

    case validate(manifest) do
      :ok -> {:ok, manifest}
      {:error, errors} -> {:error, errors}
    end
  end

  @doc """
  Validates a manifest struct.
  """
  @spec validate(t()) :: :ok | {:error, [validation_error()]}
  def validate(%__MODULE__{} = manifest) do
    errors =
      []
      |> validate_version(manifest)
      |> validate_benchmark_id(manifest)
      |> validate_suite(manifest)
      |> validate_execution_variant(manifest)
      |> validate_source(manifest)
      |> validate_tags(manifest)
      |> validate_dataset(manifest)
      |> validate_required_artifacts(manifest)
      |> validate_required_run_metadata(manifest)

    case errors do
      [] -> :ok
      _ -> {:error, Enum.reverse(errors)}
    end
  end

  @doc """
  Returns a plain map representation suitable for serialization.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = manifest) do
    %{
      version: manifest.version,
      benchmark_id: manifest.benchmark_id,
      suite: manifest.suite,
      category: manifest.category,
      execution_variant: manifest.execution_variant,
      source: manifest.source,
      tags: manifest.tags,
      dataset: manifest.dataset,
      required_artifacts: manifest.required_artifacts,
      success_criteria: manifest.success_criteria,
      required_run_metadata: manifest.required_run_metadata
    }
  end

  defp validate_version(errors, %__MODULE__{version: version})
       when is_integer(version) and version > 0,
       do: errors

  defp validate_version(errors, _manifest),
    do: [{:version, "must be a positive integer"} | errors]

  defp validate_benchmark_id(errors, %__MODULE__{benchmark_id: benchmark_id})
       when is_binary(benchmark_id) and benchmark_id != "",
       do: errors

  defp validate_benchmark_id(errors, _manifest),
    do: [{:benchmark_id, "must be a non-empty string"} | errors]

  defp validate_suite(errors, %__MODULE__{suite: suite}) do
    if Contract.workload_family?(suite) do
      errors
    else
      [{:suite, "must be one of #{inspect(Contract.workload_families())}"} | errors]
    end
  end

  defp validate_execution_variant(errors, %__MODULE__{execution_variant: execution_variant}) do
    if Contract.execution_variant?(execution_variant) do
      errors
    else
      [{:execution_variant, "must be one of #{inspect(Contract.execution_variants())}"} | errors]
    end
  end

  defp validate_source(errors, %__MODULE__{source: source}) when is_map(source) do
    if Map.has_key?(source, :kind) and
         (Map.has_key?(source, :location) or Map.has_key?(source, :label)) do
      errors
    else
      [{:source, "must include :kind and either :location or :label"} | errors]
    end
  end

  defp validate_source(errors, _manifest),
    do: [{:source, "must be a map"} | errors]

  defp validate_tags(errors, %__MODULE__{tags: tags}) when is_list(tags) do
    if Enum.all?(tags, &(is_atom(&1) or is_binary(&1))) do
      errors
    else
      [{:tags, "must contain only atoms or strings"} | errors]
    end
  end

  defp validate_tags(errors, _manifest),
    do: [{:tags, "must be a list"} | errors]

  defp validate_dataset(errors, %__MODULE__{dataset: dataset}) when is_map(dataset) do
    tier = Map.get(dataset, :tier)
    dump_version = Map.get(dataset, :dump_version)
    checksum = Map.get(dataset, :checksum)
    format = Map.get(dataset, :format)
    normalization_flags = Map.get(dataset, :normalization_flags, [])

    cond do
      not Contract.dataset_tier?(tier) ->
        [{:dataset, "must include a valid :tier"} | errors]

      not is_binary(dump_version) or dump_version == "" ->
        [{:dataset, "must include a non-empty :dump_version"} | errors]

      not is_binary(checksum) or checksum == "" ->
        [{:dataset, "must include a non-empty :checksum"} | errors]

      not is_atom(format) ->
        [{:dataset, "must include an RDF :format atom"} | errors]

      not is_list(normalization_flags) ->
        [{:dataset, ":normalization_flags must be a list"} | errors]

      true ->
        errors
    end
  end

  defp validate_dataset(errors, _manifest),
    do: [{:dataset, "must be a map"} | errors]

  defp validate_required_artifacts(errors, %__MODULE__{required_artifacts: artifacts})
       when is_list(artifacts) do
    if Enum.all?(artifacts, &Contract.result_artifact?/1) do
      errors
    else
      [{:required_artifacts, "must contain only known artifact types"} | errors]
    end
  end

  defp validate_required_artifacts(errors, _manifest),
    do: [{:required_artifacts, "must be a list"} | errors]

  defp validate_required_run_metadata(
         errors,
         %__MODULE__{required_run_metadata: required_run_metadata}
       )
       when is_list(required_run_metadata) do
    allowed = MapSet.new(Contract.required_run_metadata_fields())

    if Enum.all?(required_run_metadata, &MapSet.member?(allowed, &1)) do
      errors
    else
      [{:required_run_metadata, "must contain only defined runtime metadata fields"} | errors]
    end
  end

  defp validate_required_run_metadata(errors, _manifest),
    do: [{:required_run_metadata, "must be a list"} | errors]
end
