defmodule TripleStore.Benchmark.LDBC.CapabilityMatrix do
  @moduledoc """
  Machine-readable Phase 1 capability baseline for the pinned LDBC catalogs.

  Parser support, end-to-end execution support, and benchmark-profile inclusion
  are separate fields. This prevents syntax acceptance from being reported as a
  correct implementation of an operation.
  """

  alias TripleStore.Benchmark.LDBC.Catalog

  @states [:supported, :requires_fix, :requires_extension, :profile_exclusion]
  @parse_states [:verified, :template_requires_binding, :translation_required, :not_applicable]
  @execution_states [:verified, :unverified, :unsupported, :not_applicable]
  @required_entry_keys [
    :operation_id,
    :status,
    :parse_support,
    :execution_support,
    :features,
    :owner,
    :implementation_phase,
    :rationale,
    :evidence
  ]
  @required_finding_keys [
    :id,
    :area,
    :status,
    :observed_behavior,
    :required_behavior,
    :owner,
    :implementation_phase,
    :evidence
  ]

  @type matrix :: map()

  @doc "Returns the repository-owned capability matrix path."
  @spec default_path() :: Path.t()
  def default_path do
    case :code.priv_dir(:triple_store) do
      {:error, _reason} ->
        Path.expand("../../../../priv/benchmarks/ldbc/capabilities.exs", __DIR__)

      priv_dir ->
        Path.join(to_string(priv_dir), "benchmarks/ldbc/capabilities.exs")
    end
  end

  @doc "Loads and validates the capability matrix against the current catalogs."
  @spec load(Path.t()) :: {:ok, matrix()} | {:error, term()}
  def load(path \\ default_path()) do
    with true <- File.regular?(path) or {:error, {:capability_matrix_not_found, path}},
         {matrix, _binding} <- Code.eval_file(path),
         {:ok, catalogs} <- Catalog.load_all(),
         :ok <- validate(matrix, catalogs) do
      {:ok, matrix}
    end
  rescue
    error -> {:error, {:invalid_capability_matrix, path, Exception.message(error)}}
  end

  @doc "Validates complete operation coverage and all system-level findings."
  @spec validate(term(), [Catalog.catalog()]) :: :ok | {:error, [term()]}
  def validate(matrix, catalogs) when is_map(matrix) and is_list(catalogs) do
    entries = Map.get(matrix, :operations, [])
    findings = Map.get(matrix, :system_findings, [])
    expected_ids = catalogs |> Catalog.operations() |> Enum.map(& &1.id) |> Enum.sort()

    actual_ids =
      entries |> Enum.filter(&is_map/1) |> Enum.map(&Map.get(&1, :operation_id)) |> Enum.sort()

    errors =
      []
      |> validate_header(matrix)
      |> validate_exact_coverage(expected_ids, actual_ids)
      |> Kernel.++(Enum.flat_map(entries, &validate_entry/1))
      |> Kernel.++(duplicate_errors(actual_ids, :operation_id))
      |> Kernel.++(Enum.flat_map(findings, &validate_finding/1))
      |> Kernel.++(duplicate_errors(Enum.map(findings, &Map.get(&1, :id)), :finding_id))

    if errors == [], do: :ok, else: {:error, errors}
  end

  def validate(_matrix, _catalogs),
    do: {:error, [{:matrix, :shape, "must be a map and receive catalog maps"}]}

  @doc "Returns operation counts grouped by implementation state."
  @spec summary(matrix()) :: map()
  def summary(matrix) do
    counts = Enum.frequencies_by(matrix.operations, & &1.status)
    Map.new(@states, &{&1, Map.get(counts, &1, 0)})
  end

  @doc "Representative parser probes for the three suites and update surface."
  @spec parser_probes() :: [map()]
  def parser_probes do
    [
      %{
        id: "spb/aggregate-context",
        benchmark: :spb,
        parser: :query,
        sparql: """
        SELECT ?type (COUNT(?work) AS ?count)
        WHERE {
          GRAPH ?graph { ?work a ?type . OPTIONAL { ?work <urn:tag> ?tag } }
        }
        GROUP BY ?type ORDER BY DESC(?count) LIMIT 10
        """
      },
      %{
        id: "snb-bi/group-subquery",
        benchmark: :snb_bi,
        parser: :query,
        sparql: """
        SELECT ?country (SUM(?messages) AS ?total)
        WHERE {
          { SELECT ?person ?country (COUNT(?message) AS ?messages)
            WHERE { ?person <urn:country> ?country . ?message <urn:creator> ?person }
            GROUP BY ?person ?country }
        }
        GROUP BY ?country ORDER BY DESC(?total)
        """
      },
      %{
        id: "snb-interactive/property-path",
        benchmark: :snb_interactive,
        parser: :query,
        sparql: """
        SELECT ?friend WHERE {
          <urn:person:1> <urn:knows>+ ?friend .
          OPTIONAL { ?friend <urn:firstName> ?name }
        }
        ORDER BY ?friend LIMIT 20
        """
      },
      %{
        id: "shared/update",
        benchmark: :spb,
        parser: :update,
        sparql: """
        DELETE { GRAPH <urn:g> { ?s <urn:status> ?old } }
        INSERT { GRAPH <urn:g> { ?s <urn:status> "published" } }
        WHERE  { GRAPH <urn:g> { ?s <urn:status> ?old } }
        """
      }
    ]
  end

  defp validate_header(errors, matrix) do
    errors
    |> maybe_error(Map.get(matrix, :version) == 1, {:matrix, :version, "must be 1"})
    |> maybe_error(
      Map.get(matrix, :architecture_decision) == "ADR-0002",
      {:matrix, :architecture_decision, "must reference ADR-0002"}
    )
  end

  defp validate_exact_coverage(errors, expected_ids, actual_ids) do
    if expected_ids == actual_ids,
      do: errors,
      else: [
        {:matrix, :operations, "must classify every catalog operation exactly once"} | errors
      ]
  end

  defp validate_entry(entry) when is_map(entry) do
    missing = Enum.reject(@required_entry_keys, &Map.has_key?(entry, &1))
    id = Map.get(entry, :operation_id)

    Enum.map(missing, &{id, &1, "is required"})
    |> maybe_error(Map.get(entry, :status) in @states, {id, :status, "is invalid"})
    |> maybe_error(
      Map.get(entry, :parse_support) in @parse_states,
      {id, :parse_support, "is invalid"}
    )
    |> maybe_error(
      Map.get(entry, :execution_support) in @execution_states,
      {id, :execution_support, "is invalid"}
    )
    |> maybe_error(is_list(Map.get(entry, :features)), {id, :features, "must be a list"})
    |> maybe_error(is_binary(Map.get(entry, :owner)), {id, :owner, "must be a module name"})
    |> maybe_error(
      is_integer(Map.get(entry, :implementation_phase)),
      {id, :implementation_phase, "must be an integer"}
    )
  end

  defp validate_entry(_entry), do: [{nil, :shape, "capability entry must be a map"}]

  defp validate_finding(finding) when is_map(finding) do
    missing = Enum.reject(@required_finding_keys, &Map.has_key?(finding, &1))
    id = Map.get(finding, :id)

    Enum.map(missing, &{id, &1, "is required"})
    |> maybe_error(Map.get(finding, :status) in @states, {id, :status, "is invalid"})
    |> maybe_error(
      is_integer(Map.get(finding, :implementation_phase)),
      {id, :implementation_phase, "must be an integer"}
    )
  end

  defp validate_finding(_finding), do: [{nil, :shape, "system finding must be a map"}]

  defp duplicate_errors(ids, field) do
    ids
    |> Enum.frequencies()
    |> Enum.flat_map(fn
      {id, count} when count > 1 -> [{id, field, "is duplicated"}]
      _ -> []
    end)
  end

  defp maybe_error(errors, true, _error), do: errors
  defp maybe_error(errors, false, error), do: [error | errors]
end
