defmodule TripleStore.Benchmark.LDBC.Catalog do
  @moduledoc """
  Loads and validates canonical operation metadata for pinned LDBC profiles.

  Catalogs describe upstream semantics and coverage. They intentionally contain
  no TripleStore query translations; capability and implementation status live in
  the separate capability matrix.
  """

  alias TripleStore.Benchmark.LDBC.SourceManifest

  @benchmarks [:spb, :snb_bi, :snb_interactive]
  @families [
    :aggregation,
    :editorial,
    :validation,
    :conformance,
    :lifecycle,
    :resilience,
    :read,
    :update_batch,
    :complex_read,
    :short_read,
    :insert
  ]
  @availability [:mandatory, :optional, :audit_only, :version_specific]
  @required_operation_keys [
    :id,
    :upstream_id,
    :family,
    :availability,
    :title,
    :parameters,
    :result,
    :ordering,
    :limit,
    :variants,
    :choke_points,
    :frequency,
    :dependencies,
    :source_path
  ]

  @type catalog :: map()
  @type operation :: map()

  @doc "Returns all repository-owned catalog paths in stable order."
  @spec default_paths() :: [Path.t()]
  def default_paths do
    root =
      case :code.priv_dir(:triple_store) do
        {:error, _reason} -> Path.expand("../../../../priv/benchmarks/ldbc/catalogs", __DIR__)
        priv_dir -> Path.join(to_string(priv_dir), "benchmarks/ldbc/catalogs")
      end

    root |> Path.join("*.exs") |> Path.wildcard() |> Enum.sort()
  end

  @doc "Loads and validates every local operation catalog."
  @spec load_all([Path.t()]) :: {:ok, [catalog()]} | {:error, term()}
  def load_all(paths \\ default_paths()) do
    with {:ok, source_manifest} <- SourceManifest.load(),
         {:ok, catalogs} <- load_files(paths),
         :ok <- validate(catalogs, source_manifest) do
      {:ok, catalogs}
    end
  end

  @doc "Validates catalog completeness, uniqueness, source ownership, and operation schemas."
  @spec validate(term(), [SourceManifest.source()]) :: :ok | {:error, [term()]}
  def validate(catalogs, sources) when is_list(catalogs) and is_list(sources) do
    source_ids = MapSet.new(Enum.map(sources, & &1.id))

    errors =
      Enum.flat_map(catalogs, &validate_catalog(&1, source_ids)) ++ cross_catalog_errors(catalogs)

    if errors == [], do: :ok, else: {:error, errors}
  end

  def validate(_catalogs, _sources), do: {:error, [{:catalogs, :shape, "must be a list"}]}

  @doc "Returns all operations from all catalogs in stable local-ID order."
  @spec operations([catalog()]) :: [operation()]
  def operations(catalogs) do
    catalogs
    |> Enum.flat_map(& &1.operations)
    |> Enum.sort_by(& &1.id)
  end

  @doc "Produces a deterministic SHA-256 digest for catalog regeneration checks."
  @spec digest(catalog()) :: String.t()
  def digest(catalog) do
    catalog
    |> canonicalize()
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp load_files(paths) do
    Enum.reduce_while(paths, {:ok, []}, fn path, {:ok, catalogs} ->
      try do
        {catalog, _binding} = Code.eval_file(path)
        {:cont, {:ok, [catalog | catalogs]}}
      rescue
        error -> {:halt, {:error, {:invalid_catalog_file, path, Exception.message(error)}}}
      end
    end)
    |> case do
      {:ok, catalogs} -> {:ok, Enum.reverse(catalogs)}
      error -> error
    end
  end

  defp validate_catalog(catalog, source_ids) when is_map(catalog) do
    id = Map.get(catalog, :id, "<missing-catalog-id>")
    operations = Map.get(catalog, :operations, [])
    expected_ids = Map.get(catalog, :expected_operation_ids, [])

    []
    |> require_catalog_field(id, catalog, :id, &is_binary/1)
    |> require_catalog_field(id, catalog, :version, &is_binary/1)
    |> require_catalog_field(id, catalog, :benchmark, &(&1 in @benchmarks))
    |> require_catalog_field(id, catalog, :source_id, &MapSet.member?(source_ids, &1))
    |> validate_operations(id, operations)
    |> validate_expected_ids(id, operations, expected_ids)
  end

  defp validate_catalog(_catalog, _source_ids),
    do: [{"<invalid-catalog>", :shape, "must be a map"}]

  defp require_catalog_field(errors, id, catalog, field, validator) do
    if validator.(Map.get(catalog, field)), do: errors, else: [{id, field, "is invalid"} | errors]
  end

  defp validate_operations(errors, id, operations)
       when is_list(operations) and operations != [] do
    operation_errors = Enum.flat_map(operations, &validate_operation(&1, id))
    operation_ids = Enum.map(operations, &Map.get(&1, :id))
    duplicate_ids = duplicate_errors(operation_ids, id)
    errors ++ operation_errors ++ duplicate_ids
  end

  defp validate_operations(errors, id, _operations),
    do: [{id, :operations, "must be a non-empty list"} | errors]

  defp validate_operation(operation, catalog_id) when is_map(operation) do
    missing = Enum.reject(@required_operation_keys, &Map.has_key?(operation, &1))

    errors = Enum.map(missing, &{catalog_id, Map.get(operation, :id), &1, "is required"})
    op_id = Map.get(operation, :id)

    errors
    |> maybe_error(
      is_binary(op_id) and Regex.match?(~r/\Aldbc\/[a-z0-9-]+\/[a-z0-9.-]+@[^\s]+\z/, op_id),
      {catalog_id, op_id, :id, "must be a stable versioned LDBC ID"}
    )
    |> maybe_error(
      Map.get(operation, :family) in @families,
      {catalog_id, op_id, :family, "is unknown"}
    )
    |> maybe_error(
      Map.get(operation, :availability) in @availability,
      {catalog_id, op_id, :availability, "is unknown"}
    )
    |> maybe_error(
      is_list(Map.get(operation, :parameters)),
      {catalog_id, op_id, :parameters, "must be a list"}
    )
    |> maybe_error(
      is_list(Map.get(operation, :result)),
      {catalog_id, op_id, :result, "must be a list"}
    )
  end

  defp validate_operation(_operation, catalog_id),
    do: [{catalog_id, nil, :shape, "operation must be a map"}]

  defp maybe_error(errors, true, _error), do: errors
  defp maybe_error(errors, false, error), do: [error | errors]

  defp validate_expected_ids(errors, id, operations, expected_ids) when is_list(expected_ids) do
    actual = operations |> Enum.map(& &1.id) |> Enum.sort()
    expected = Enum.sort(expected_ids)

    if actual == expected,
      do: errors,
      else: [{id, :expected_operation_ids, "does not exactly match catalog operations"} | errors]
  end

  defp validate_expected_ids(errors, id, _operations, _expected_ids),
    do: [{id, :expected_operation_ids, "must be a list"} | errors]

  defp cross_catalog_errors(catalogs) do
    operation_ids =
      catalogs
      |> Enum.filter(&is_map/1)
      |> Enum.flat_map(&Map.get(&1, :operations, []))
      |> Enum.filter(&is_map/1)
      |> Enum.map(&Map.get(&1, :id))

    duplicate_errors(operation_ids, :catalogs)
  end

  defp duplicate_errors(ids, owner) do
    ids
    |> Enum.frequencies()
    |> Enum.flat_map(fn
      {id, count} when count > 1 -> [{owner, id, :id, "is duplicated"}]
      _ -> []
    end)
  end

  defp canonicalize(value) when is_map(value) do
    value
    |> Enum.map(fn {key, item} -> {key, canonicalize(item)} end)
    |> Enum.sort_by(fn {key, _item} -> key end)
  end

  defp canonicalize(value) when is_list(value), do: Enum.map(value, &canonicalize/1)
  defp canonicalize(value), do: value
end
