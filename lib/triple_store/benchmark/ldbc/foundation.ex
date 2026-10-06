defmodule TripleStore.Benchmark.LDBC.Foundation do
  @moduledoc """
  Executable Phase 1 gate for the LDBC benchmark foundation.

  The gate validates immutable sources, profile links, complete operation
  catalogs, pinned catalog digests, and the capability matrix as one unit.
  Runtime parser and store probes remain integration tests because they allocate
  native resources.
  """

  alias TripleStore.Benchmark.LDBC.{CapabilityMatrix, Catalog, Profile, SourceManifest}

  @doc "Returns the path containing accepted deterministic catalog digests."
  @spec digest_path() :: Path.t()
  def digest_path do
    case :code.priv_dir(:triple_store) do
      {:error, _reason} ->
        Path.expand("../../../../priv/benchmarks/ldbc/catalog_digests.exs", __DIR__)

      priv_dir ->
        Path.join(to_string(priv_dir), "benchmarks/ldbc/catalog_digests.exs")
    end
  end

  @doc "Runs the complete metadata and architecture-link phase gate."
  @spec validate() :: {:ok, map()} | {:error, [term()] | term()}
  def validate do
    with {:ok, sources} <- SourceManifest.load(),
         {:ok, profiles} <- Profile.load(),
         {:ok, catalogs} <- Catalog.load_all(),
         {:ok, matrix} <- CapabilityMatrix.load(),
         {:ok, accepted_digests} <- load_digests(),
         :ok <- validate_links(sources, profiles, catalogs),
         :ok <- validate_digests(catalogs, accepted_digests) do
      {:ok,
       %{
         source_count: length(sources),
         profile_count: length(profiles),
         catalog_count: length(catalogs),
         operation_count: length(Catalog.operations(catalogs)),
         finding_count: length(matrix.system_findings),
         capability_summary: CapabilityMatrix.summary(matrix),
         catalog_digests: accepted_digests,
         architecture_decision: matrix.architecture_decision
       }}
    end
  end

  @doc "Validates that every profile references existing pinned sources and a catalog."
  @spec validate_links([map()], [map()], [map()]) :: :ok | {:error, [term()]}
  def validate_links(sources, profiles, catalogs) do
    source_ids = MapSet.new(Enum.map(sources, & &1.id))
    catalogs_by_id = Map.new(catalogs, &{&1.id, &1})

    errors =
      Enum.flat_map(profiles, fn profile ->
        source_errors =
          profile.source_ids
          |> Enum.reject(&MapSet.member?(source_ids, &1))
          |> Enum.map(&{profile.id, :unknown_source, &1})

        catalog_errors =
          case Map.fetch(catalogs_by_id, profile.catalog) do
            {:ok, %{version: version}} when version == profile.version ->
              []

            {:ok, %{version: version}} ->
              [{profile.id, :catalog_version, version, profile.version}]

            :error ->
              [{profile.id, :unknown_catalog, profile.catalog}]
          end

        source_errors ++ catalog_errors
      end)

    if errors == [], do: :ok, else: {:error, errors}
  end

  @doc "Validates catalogs against the accepted deterministic digests."
  @spec validate_digests([map()], map()) :: :ok | {:error, [term()]}
  def validate_digests(catalogs, accepted_digests) do
    actual = Map.new(catalogs, &{&1.id, Catalog.digest(&1)})

    errors =
      (Map.keys(actual) ++ Map.keys(accepted_digests))
      |> Enum.uniq()
      |> Enum.sort()
      |> Enum.flat_map(fn id ->
        case {Map.fetch(actual, id), Map.fetch(accepted_digests, id)} do
          {{:ok, digest}, {:ok, digest}} -> []
          {{:ok, digest}, {:ok, accepted}} -> [{id, :digest_mismatch, accepted, digest}]
          {:error, {:ok, _accepted}} -> [{id, :catalog_missing}]
          {{:ok, _digest}, :error} -> [{id, :accepted_digest_missing}]
        end
      end)

    if errors == [], do: :ok, else: {:error, errors}
  end

  defp load_digests do
    path = digest_path()

    if File.regular?(path) do
      {digests, _binding} = Code.eval_file(path)
      {:ok, digests}
    else
      {:error, {:catalog_digest_file_missing, path}}
    end
  rescue
    error ->
      {:error, {:invalid_catalog_digest_file, digest_path(), Exception.message(error)}}
  end
end
