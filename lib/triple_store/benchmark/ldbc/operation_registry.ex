defmodule TripleStore.Benchmark.LDBC.OperationRegistry do
  @moduledoc """
  Loads executable LDBC operation definitions and links them to pinned metadata.

  The checked-in executable registry remains separate from the upstream catalog:
  catalogs state benchmark semantics while this registry states the exact local
  translation and execution strategy.
  """

  alias TripleStore.Benchmark.LDBC.{Catalog, Operation, Profile, SourceManifest}

  @doc "Returns the repository-owned executable operation manifest path."
  @spec default_path() :: Path.t()
  def default_path do
    case :code.priv_dir(:triple_store) do
      {:error, _reason} -> Path.expand("../../../../priv/benchmarks/ldbc/operations.exs", __DIR__)
      priv_dir -> Path.join(to_string(priv_dir), "benchmarks/ldbc/operations.exs")
    end
  end

  @doc "Loads and validates executable definitions against catalogs, profiles, and sources."
  @spec load(Path.t()) :: {:ok, [Operation.t()]} | {:error, term()}
  def load(path \\ default_path()) do
    with {entries, _binding} <- Code.eval_file(path),
         operations <- Enum.map(entries, &struct!(Operation, &1)),
         {:ok, catalogs} <- Catalog.load_all(),
         {:ok, profiles} <- Profile.load(),
         {:ok, sources} <- SourceManifest.load(),
         :ok <- validate(operations, catalogs, profiles, sources) do
      {:ok, operations}
    end
  rescue
    error -> {:error, {:invalid_operation_manifest, path, Exception.message(error)}}
  end

  @doc "Validates registry uniqueness and all semantic metadata links."
  @spec validate([Operation.t()], [map()], [map()], [map()]) :: :ok | {:error, [term()]}
  def validate(operations, catalogs, profiles, sources) when is_list(operations) do
    catalog_operations = Catalog.operations(catalogs) |> Map.new(&{&1.id, &1})
    profiles_by_id = Map.new(profiles, &{&1.id, &1})
    sources_by_id = Map.new(sources, &{&1.id, &1})

    errors =
      duplicate_errors(operations) ++
        Enum.flat_map(operations, fn operation ->
          validation_errors(operation) ++
            link_errors(operation, catalog_operations, profiles_by_id, sources_by_id)
        end)

    if errors == [], do: :ok, else: {:error, errors}
  end

  def validate(_operations, _catalogs, _profiles, _sources), do: {:error, [{:registry, :invalid}]}

  @doc "Returns a deterministic checksum for bridge handshakes and artifacts."
  @spec checksum([Operation.t()]) :: String.t()
  def checksum(operations) do
    operations
    |> Enum.sort_by(& &1.id)
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp validation_errors(operation) do
    case Operation.validate(operation) do
      :ok -> []
      {:error, errors} -> Enum.map(errors, &{operation.id, &1})
    end
  end

  defp link_errors(operation, catalog_operations, profiles, sources) do
    catalog_operation = Map.get(catalog_operations, operation.catalog_id)
    profile = Map.get(profiles, operation.profile_id)
    source = Map.get(sources, operation.source.source_id)

    []
    |> require(not is_nil(catalog_operation), {operation.id, :unknown_catalog_operation})
    |> require(
      not is_nil(profile) and profile.benchmark == operation.suite,
      {operation.id, :unknown_or_mismatched_profile}
    )
    |> require(
      not is_nil(catalog_operation) and catalog_operation.upstream_id == operation.upstream_id,
      {operation.id, :upstream_id_mismatch}
    )
    |> require(
      not is_nil(source) and source.benchmark == operation.suite,
      {operation.id, :unknown_or_mismatched_source}
    )
    |> require(
      not is_nil(source) and source.checksum.value == operation.source.checksum,
      {operation.id, :source_checksum_mismatch}
    )
  end

  defp duplicate_errors(operations) do
    operations
    |> Enum.frequencies_by(& &1.id)
    |> Enum.flat_map(fn
      {id, count} when count > 1 -> [{id, :duplicate_id}]
      _ -> []
    end)
  end

  defp require(errors, true, _error), do: errors
  defp require(errors, false, error), do: [error | errors]
end
