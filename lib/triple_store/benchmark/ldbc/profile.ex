defmodule TripleStore.Benchmark.LDBC.Profile do
  @moduledoc """
  Validated execution and reporting profiles for the LDBC benchmark family.

  A profile states which parts of an upstream protocol are preserved and which
  claims a report may make. Audit terminology is validated separately from
  workload completeness so an internally comparable run cannot present itself as
  externally audited.
  """

  @claim_levels [:smoke, :development, :comparable, :audit_preparation]
  @benchmarks [:spb, :snb_bi, :snb_interactive]
  @protected_claims ["official", "certified", "audited"]
  @canonical_flags [
    :complete_operation_mix,
    :canonical_parameters,
    :canonical_schedule,
    :canonical_validation,
    :canonical_scoring
  ]
  @required_keys [
    :id,
    :benchmark,
    :version,
    :claim_level,
    :source_ids,
    :catalog,
    :protocol,
    :score_namespace,
    :required_evidence,
    :description
  ]

  @type profile :: map()
  @type validation_error :: {String.t() | :profiles, atom(), String.t()}

  @doc "Returns the repository-owned profile manifest path."
  @spec default_path() :: Path.t()
  def default_path do
    case :code.priv_dir(:triple_store) do
      {:error, _reason} -> Path.expand("../../../../priv/benchmarks/ldbc/profiles.exs", __DIR__)
      priv_dir -> Path.join(to_string(priv_dir), "benchmarks/ldbc/profiles.exs")
    end
  end

  @doc "Loads and validates all benchmark profiles."
  @spec load(Path.t()) :: {:ok, [profile()]} | {:error, term()}
  def load(path \\ default_path()) do
    with true <- File.regular?(path) or {:error, {:profile_manifest_not_found, path}},
         {profiles, _binding} <- Code.eval_file(path),
         :ok <- validate(profiles) do
      {:ok, profiles}
    end
  rescue
    error -> {:error, {:invalid_profile_manifest, path, Exception.message(error)}}
  end

  @doc "Validates profile structure and claim-level invariants."
  @spec validate(term()) :: :ok | {:error, [validation_error()]}
  def validate(profiles) when is_list(profiles) do
    errors = Enum.flat_map(profiles, &validate_profile/1) ++ duplicate_id_errors(profiles)
    if errors == [], do: :ok, else: {:error, errors}
  end

  def validate(_profiles), do: {:error, [{:profiles, :shape, "must be a list"}]}

  @doc "Returns a profile by its stable ID."
  @spec fetch([profile()], String.t()) ::
          {:ok, profile()} | {:error, {:unknown_profile, String.t()}}
  def fetch(profiles, id) do
    case Enum.find(profiles, &(&1.id == id)) do
      nil -> {:error, {:unknown_profile, id}}
      profile -> {:ok, profile}
    end
  end

  @doc "Validates report terminology against profile and external audit evidence."
  @spec validate_report_claim(profile(), String.t(), map() | nil) ::
          :ok | {:error, {:protected_claim_requires_completed_audit, String.t()}}
  def validate_report_claim(profile, claim, audit_metadata \\ nil)
      when is_map(profile) and is_binary(claim) do
    protected? =
      claim
      |> String.downcase()
      |> then(fn normalized ->
        Enum.any?(@protected_claims, &String.contains?(normalized, &1))
      end)

    if protected? and not completed_audit?(profile, audit_metadata) do
      {:error, {:protected_claim_requires_completed_audit, claim}}
    else
      :ok
    end
  end

  defp validate_profile(profile) when is_map(profile) do
    id = Map.get(profile, :id, "<missing-id>")

    []
    |> require_keys(id, profile)
    |> validate_member(id, :benchmark, Map.get(profile, :benchmark), @benchmarks)
    |> validate_member(id, :claim_level, Map.get(profile, :claim_level), @claim_levels)
    |> validate_non_empty_list(id, :source_ids, Map.get(profile, :source_ids))
    |> validate_protocol(id, profile)
    |> validate_score_namespace(id, profile)
    |> validate_audit_evidence(id, profile)
  end

  defp validate_profile(_profile), do: [{"<invalid-profile>", :shape, "must be a map"}]

  defp require_keys(errors, id, profile) do
    Enum.reduce(@required_keys, errors, fn key, acc ->
      if Map.has_key?(profile, key), do: acc, else: [{id, key, "is required"} | acc]
    end)
  end

  defp validate_member(errors, id, field, value, allowed) do
    if value in allowed,
      do: errors,
      else: [{id, field, "must be one of #{inspect(allowed)}"} | errors]
  end

  defp validate_non_empty_list(errors, _id, _field, values)
       when is_list(values) and values != [] do
    if Enum.all?(values, &(is_binary(&1) and &1 != "")), do: errors, else: :invalid
  end

  defp validate_non_empty_list(_errors, _id, _field, _values), do: :invalid

  defp validate_protocol(:invalid, id, _profile),
    do: [{id, :source_ids, "must be a non-empty list of strings"}]

  defp validate_protocol(errors, id, %{protocol: protocol, claim_level: level})
       when is_map(protocol) do
    valid_flags? = Enum.all?(@canonical_flags, &is_boolean(Map.get(protocol, &1)))
    comparable? = level in [:comparable, :audit_preparation]
    canonical? = Enum.all?(@canonical_flags, &Map.get(protocol, &1))

    cond do
      not valid_flags? ->
        [{id, :protocol, "must define every canonical protocol flag"} | errors]

      comparable? and not canonical? ->
        [{id, :protocol, "comparable profiles must preserve the complete protocol"} | errors]

      true ->
        errors
    end
  end

  defp validate_protocol(errors, id, _profile),
    do: [{id, :protocol, "must be a protocol map"} | errors]

  defp validate_score_namespace(errors, id, profile) do
    namespace = Map.get(profile, :score_namespace)
    level = Map.get(profile, :claim_level)

    cond do
      namespace not in [:diagnostic, :canonical] ->
        [{id, :score_namespace, "must be :diagnostic or :canonical"} | errors]

      level in [:smoke, :development] and namespace != :diagnostic ->
        [{id, :score_namespace, "partial profiles may emit diagnostic scores only"} | errors]

      level in [:comparable, :audit_preparation] and namespace != :canonical ->
        [{id, :score_namespace, "complete profiles must use canonical scoring"} | errors]

      true ->
        errors
    end
  end

  defp validate_audit_evidence(errors, id, %{claim_level: :audit_preparation} = profile) do
    required = Map.get(profile, :required_evidence, [])
    missing = [:configuration, :pricing, :provenance, :full_disclosure] -- required

    if missing == [],
      do: errors,
      else: [{id, :required_evidence, "missing #{inspect(missing)}"} | errors]
  end

  defp validate_audit_evidence(errors, _id, _profile), do: errors

  defp duplicate_id_errors(profiles) do
    profiles
    |> Enum.filter(&is_map/1)
    |> Enum.map(&Map.get(&1, :id))
    |> Enum.reject(&is_nil/1)
    |> Enum.frequencies()
    |> Enum.flat_map(fn
      {id, count} when count > 1 -> [{id, :id, "is duplicated"}]
      _ -> []
    end)
  end

  defp completed_audit?(%{claim_level: :audit_preparation}, %{
         status: :completed,
         auditor: auditor,
         report_url: report_url
       })
       when is_binary(auditor) and auditor != "" and is_binary(report_url) and report_url != "",
       do: true

  defp completed_audit?(_profile, _audit_metadata), do: false
end
