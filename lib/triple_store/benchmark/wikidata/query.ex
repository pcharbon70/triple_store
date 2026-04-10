defmodule TripleStore.Benchmark.Wikidata.Query do
  @moduledoc """
  Normalized benchmark-query representation for Wikidata workloads.

  Every imported or generated benchmark query carries a validated benchmark
  manifest plus the query text and the metadata needed for reporting,
  classification, and later correctness analysis.
  """

  alias TripleStore.Benchmark.Wikidata.{Manifest, Normalizer}

  @enforce_keys [:manifest, :name, :description, :sparql]
  defstruct manifest: nil,
            name: nil,
            description: nil,
            raw_sparql: nil,
            sparql: nil,
            params: [],
            group: nil,
            shape: nil,
            feature_tags: [],
            answer_size_class: :unknown,
            source_id: nil,
            preprocessing: [],
            stress_points: [],
            template_metadata: nil

  @type shape ::
          :single_bgp
          | :path_bgp
          | :star_bgp
          | :multi_bgp
          | :optional
          | :property_path
          | :mixed

  @type answer_size_class :: :tiny | :small | :medium | :large | :huge | :unknown

  @type t :: %__MODULE__{
          manifest: Manifest.t(),
          name: String.t(),
          description: String.t(),
          raw_sparql: String.t(),
          sparql: String.t(),
          params: [atom()],
          group: atom() | String.t() | nil,
          shape: shape() | nil,
          feature_tags: [atom() | String.t()],
          answer_size_class: answer_size_class(),
          source_id: String.t() | nil,
          preprocessing: [atom() | tuple()],
          stress_points: [atom() | String.t()],
          template_metadata: map() | nil
        }

  @type validation_error ::
          {:manifest, String.t()}
          | {:name, String.t()}
          | {:description, String.t()}
          | {:sparql, String.t()}
          | {:params, String.t()}
          | {:feature_tags, String.t()}
          | {:answer_size_class, String.t()}
          | {:stress_points, String.t()}

  @answer_size_classes [:tiny, :small, :medium, :large, :huge, :unknown]

  @doc """
  Builds and validates a normalized benchmark query.
  """
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, [validation_error()]}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})

    with {:ok, manifest} <- normalize_manifest(Map.get(attrs, :manifest)) do
      sparql = Map.get(attrs, :sparql)
      raw_sparql = Map.get(attrs, :raw_sparql, sparql)

      query = %__MODULE__{
        manifest: manifest,
        name: Map.get(attrs, :name),
        description: Map.get(attrs, :description),
        raw_sparql: raw_sparql,
        sparql: sparql,
        params: Map.get(attrs, :params, []),
        group: Map.get(attrs, :group),
        shape: Map.get(attrs, :shape),
        feature_tags: Map.get(attrs, :feature_tags, []),
        answer_size_class: Map.get(attrs, :answer_size_class, :unknown),
        source_id: Map.get(attrs, :source_id),
        preprocessing: Map.get(attrs, :preprocessing, []),
        stress_points: Map.get(attrs, :stress_points, []),
        template_metadata: Map.get(attrs, :template_metadata)
      }

      case validate(query) do
        :ok -> {:ok, query}
        {:error, errors} -> {:error, errors}
      end
    end
  end

  @doc """
  Validates a benchmark query struct.
  """
  @spec validate(t()) :: :ok | {:error, [validation_error()]}
  def validate(%__MODULE__{} = query) do
    errors =
      []
      |> validate_name(query)
      |> validate_description(query)
      |> validate_sparql(query)
      |> validate_params(query)
      |> validate_feature_tags(query)
      |> validate_answer_size_class(query)
      |> validate_stress_points(query)

    case errors do
      [] -> :ok
      _ -> {:error, Enum.reverse(errors)}
    end
  end

  @doc """
  Returns the benchmark ID for the query.
  """
  @spec benchmark_id(t()) :: String.t()
  def benchmark_id(%__MODULE__{manifest: manifest}), do: manifest.benchmark_id

  @doc """
  Applies a benchmark execution variant to the query text.
  """
  @spec with_variant(t(), atom()) :: {:ok, t()} | {:error, [validation_error()]}
  def with_variant(%__MODULE__{} = query, execution_variant) do
    manifest_attrs =
      query.manifest
      |> Manifest.to_map()
      |> Map.put(:execution_variant, execution_variant)

    with {:ok, manifest} <- Manifest.new(manifest_attrs) do
      new(%{
        manifest: manifest,
        name: query.name,
        description: query.description,
        raw_sparql: query.raw_sparql,
        sparql: Normalizer.apply_execution_variant(query.sparql, execution_variant),
        params: query.params,
        group: query.group,
        shape: query.shape,
        feature_tags: query.feature_tags,
        answer_size_class: query.answer_size_class,
        source_id: query.source_id,
        preprocessing: query.preprocessing,
        stress_points: query.stress_points,
        template_metadata: query.template_metadata
      })
    end
  end

  @doc """
  Applies a limit policy to a normalized query.
  """
  @spec with_limit_policy(t(), Normalizer.limit_policy()) ::
          {:ok, t()} | {:error, [validation_error()]}
  def with_limit_policy(%__MODULE__{} = query, limit_policy) do
    new(%{
      manifest: query.manifest,
      name: query.name,
      description: query.description,
      raw_sparql: query.raw_sparql,
      sparql: Normalizer.apply_limit_policy(query.sparql, limit_policy),
      params: query.params,
      group: query.group,
      shape: query.shape,
      feature_tags: query.feature_tags,
      answer_size_class: query.answer_size_class,
      source_id: query.source_id,
      preprocessing: query.preprocessing,
      stress_points: query.stress_points,
      template_metadata: query.template_metadata
    })
  end

  defp normalize_manifest(%Manifest{} = manifest), do: {:ok, manifest}

  defp normalize_manifest(attrs) when is_map(attrs) or is_list(attrs) do
    Manifest.new(attrs)
  end

  defp normalize_manifest(_attrs),
    do: {:error, [{:manifest, "must be a valid benchmark manifest"}]}

  defp validate_name(errors, %__MODULE__{name: name}) when is_binary(name) and name != "",
    do: errors

  defp validate_name(errors, _query), do: [{:name, "must be a non-empty string"} | errors]

  defp validate_description(errors, %__MODULE__{description: description})
       when is_binary(description) and description != "",
       do: errors

  defp validate_description(errors, _query),
    do: [{:description, "must be a non-empty string"} | errors]

  defp validate_sparql(errors, %__MODULE__{sparql: sparql, raw_sparql: raw_sparql})
       when is_binary(sparql) and sparql != "" and is_binary(raw_sparql) and raw_sparql != "",
       do: errors

  defp validate_sparql(errors, _query),
    do: [{:sparql, "must include non-empty raw and normalized query text"} | errors]

  defp validate_params(errors, %__MODULE__{params: params}) when is_list(params) do
    if Enum.all?(params, &is_atom/1) do
      errors
    else
      [{:params, "must contain only atoms"} | errors]
    end
  end

  defp validate_params(errors, _query), do: [{:params, "must be a list"} | errors]

  defp validate_feature_tags(errors, %__MODULE__{feature_tags: feature_tags})
       when is_list(feature_tags) do
    if Enum.all?(feature_tags, &(is_atom(&1) or is_binary(&1))) do
      errors
    else
      [{:feature_tags, "must contain only atoms or strings"} | errors]
    end
  end

  defp validate_feature_tags(errors, _query),
    do: [{:feature_tags, "must be a list"} | errors]

  defp validate_answer_size_class(errors, %__MODULE__{answer_size_class: answer_size_class}) do
    if answer_size_class in @answer_size_classes do
      errors
    else
      [{:answer_size_class, "must be one of #{inspect(@answer_size_classes)}"} | errors]
    end
  end

  defp validate_stress_points(errors, %__MODULE__{stress_points: stress_points})
       when is_list(stress_points) do
    if Enum.all?(stress_points, &(is_atom(&1) or is_binary(&1))) do
      errors
    else
      [{:stress_points, "must contain only atoms or strings"} | errors]
    end
  end

  defp validate_stress_points(errors, _query),
    do: [{:stress_points, "must be a list"} | errors]
end
