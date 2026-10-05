defmodule TripleStore.Benchmark.LDBC.SourceManifest do
  @moduledoc """
  Loads and validates the immutable upstream-source manifest for LDBC tooling.

  The manifest is repository metadata, not a downloader. Network acquisition is
  deliberately left to later benchmark phases; this module makes source identity,
  licensing, runtime requirements, and offline behavior explicit first.
  """

  @benchmarks [:spb, :snb_bi, :snb_interactive]
  @readiness_levels [:stable, :audited_stable, :work_in_progress]
  @asset_policies [:metadata_only, :generate_or_download]
  @required_keys [
    :id,
    :benchmark,
    :profiles,
    :roles,
    :repository,
    :release,
    :commit,
    :checksum,
    :license,
    :notice,
    :runtimes,
    :platforms,
    :readiness,
    :asset_policy
  ]

  @type source :: %{
          required(:id) => String.t(),
          required(:benchmark) => atom(),
          required(:profiles) => [String.t()],
          required(:roles) => [atom()],
          required(:repository) => String.t(),
          required(:release) => String.t(),
          required(:commit) => String.t(),
          required(:checksum) => %{algorithm: :git_sha1, value: String.t()},
          required(:license) => String.t(),
          required(:notice) => String.t(),
          required(:runtimes) => [String.t()],
          required(:platforms) => [String.t()],
          required(:readiness) => atom(),
          required(:asset_policy) => atom()
        }

  @type validation_error :: {String.t() | :manifest, atom(), String.t()}

  @doc "Returns the repository-owned source manifest path."
  @spec default_path() :: Path.t()
  def default_path do
    case :code.priv_dir(:triple_store) do
      {:error, _reason} -> Path.expand("../../../../priv/benchmarks/ldbc/sources.exs", __DIR__)
      priv_dir -> Path.join(to_string(priv_dir), "benchmarks/ldbc/sources.exs")
    end
  end

  @doc "Loads and validates an LDBC source manifest."
  @spec load(Path.t()) :: {:ok, [source()]} | {:error, term()}
  def load(path \\ default_path()) do
    with {:ok, sources} <- read(path),
         :ok <- validate(sources) do
      {:ok, sources}
    end
  end

  @doc "Validates all source entries and rejects duplicate identifiers."
  @spec validate(term()) :: :ok | {:error, [validation_error()]}
  def validate(sources) when is_list(sources) do
    errors =
      sources
      |> Enum.flat_map(&validate_source/1)
      |> Kernel.++(duplicate_id_errors(sources))

    case errors do
      [] -> :ok
      _ -> {:error, errors}
    end
  end

  def validate(_sources), do: {:error, [{:manifest, :shape, "must be a list of source maps"}]}

  @doc "Returns a source entry by its stable manifest identifier."
  @spec fetch([source()], String.t()) :: {:ok, source()} | {:error, {:unknown_source, String.t()}}
  def fetch(sources, id) when is_list(sources) and is_binary(id) do
    case Enum.find(sources, &(&1.id == id)) do
      nil -> {:error, {:unknown_source, id}}
      source -> {:ok, source}
    end
  end

  @doc "Checks an externally acquired source without attempting network access."
  @spec require_external_asset(source(), Path.t()) ::
          :ok | {:error, {:external_asset_missing, String.t(), Path.t()}}
  def require_external_asset(%{id: id}, path) when is_binary(path) do
    if File.exists?(path) do
      :ok
    else
      {:error, {:external_asset_missing, id, path}}
    end
  end

  defp read(path) do
    if File.regular?(path) do
      {value, _binding} = Code.eval_file(path)
      {:ok, value}
    else
      {:error, {:manifest_not_found, path}}
    end
  rescue
    error -> {:error, {:invalid_manifest_file, path, Exception.message(error)}}
  end

  defp validate_source(source) when is_map(source) do
    id = Map.get(source, :id, "<missing-id>")

    []
    |> require_keys(id, source)
    |> validate_id(id)
    |> validate_enum(id, :benchmark, Map.get(source, :benchmark), @benchmarks)
    |> validate_non_empty_list(id, :profiles, Map.get(source, :profiles), &is_binary/1)
    |> validate_non_empty_list(id, :roles, Map.get(source, :roles), &is_atom/1)
    |> validate_https_url(id, Map.get(source, :repository))
    |> validate_release(id, Map.get(source, :release))
    |> validate_commit(id, Map.get(source, :commit))
    |> validate_checksum(id, Map.get(source, :checksum), Map.get(source, :commit))
    |> validate_non_empty_binary(id, :license, Map.get(source, :license))
    |> validate_non_empty_binary(id, :notice, Map.get(source, :notice))
    |> validate_non_empty_list(id, :runtimes, Map.get(source, :runtimes), &is_binary/1)
    |> validate_non_empty_list(id, :platforms, Map.get(source, :platforms), &is_binary/1)
    |> validate_enum(id, :readiness, Map.get(source, :readiness), @readiness_levels)
    |> validate_enum(id, :asset_policy, Map.get(source, :asset_policy), @asset_policies)
  end

  defp validate_source(_source), do: [{"<invalid-entry>", :shape, "must be a map"}]

  defp require_keys(errors, id, source) do
    Enum.reduce(@required_keys, errors, fn key, acc ->
      if Map.has_key?(source, key), do: acc, else: [{id, key, "is required"} | acc]
    end)
  end

  defp validate_id(errors, id) when is_binary(id) and id != "", do: errors
  defp validate_id(errors, id), do: [{inspect(id), :id, "must be a non-empty string"} | errors]

  defp validate_enum(errors, id, field, value, allowed) do
    if value in allowed do
      errors
    else
      [{id, field, "must be one of #{inspect(allowed)}"} | errors]
    end
  end

  defp validate_non_empty_binary(errors, _id, _field, value)
       when is_binary(value) and value != "",
       do: errors

  defp validate_non_empty_binary(errors, id, field, _value),
    do: [{id, field, "must be a non-empty string"} | errors]

  defp validate_non_empty_list(errors, id, field, values, item_validator)
       when is_list(values) do
    if values != [] and Enum.all?(values, item_validator) do
      errors
    else
      [{id, field, "must be a non-empty homogeneous list"} | errors]
    end
  end

  defp validate_non_empty_list(errors, id, field, _values, _item_validator),
    do: [{id, field, "must be a non-empty list"} | errors]

  defp validate_https_url(errors, id, repository) do
    case URI.parse(repository || "") do
      %URI{scheme: "https", host: host, path: path}
      when is_binary(host) and host != "" and is_binary(path) and path != "" ->
        errors

      _ ->
        [{id, :repository, "must be an HTTPS repository URL"} | errors]
    end
  end

  defp validate_release(errors, id, release) when is_binary(release) and release != "" do
    if release in ["main", "master", "HEAD", "latest"] do
      [{id, :release, "must not name a moving ref"} | errors]
    else
      errors
    end
  end

  defp validate_release(errors, id, _release),
    do: [{id, :release, "must be a release tag or exact commit label"} | errors]

  defp validate_commit(errors, id, commit) when is_binary(commit) do
    if Regex.match?(~r/\A[0-9a-f]{40}\z/, commit) do
      errors
    else
      [{id, :commit, "must be a full 40-character lowercase Git commit"} | errors]
    end
  end

  defp validate_commit(errors, id, _commit),
    do: [{id, :commit, "must be a full 40-character lowercase Git commit"} | errors]

  defp validate_checksum(errors, id, checksum, commit) do
    case checksum do
      %{algorithm: :git_sha1, value: ^commit} when is_binary(commit) -> errors
      _ -> [{id, :checksum, "must be a git_sha1 checksum matching :commit"} | errors]
    end
  end

  defp duplicate_id_errors(sources) do
    sources
    |> Enum.filter(&is_map/1)
    |> Enum.map(&Map.get(&1, :id))
    |> Enum.reject(&is_nil/1)
    |> Enum.frequencies()
    |> Enum.flat_map(fn
      {id, count} when count > 1 -> [{id, :id, "is duplicated"}]
      _ -> []
    end)
  end
end
