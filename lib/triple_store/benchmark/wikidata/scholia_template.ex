defmodule TripleStore.Benchmark.Wikidata.ScholiaTemplate do
  @moduledoc """
  Normalized Scholia-style query template representation.

  Templates are loaded from local benchmark assets, normalized once, and then
  instantiated into benchmark queries for specific representative items and
  execution variants.
  """

  alias TripleStore.Benchmark.Wikidata.{Contract, Normalizer}

  @complexities [:simple, :medium, :complex]

  @enforce_keys [:template_id, :class_id, :class_label, :name, :description, :raw_sparql, :sparql]
  defstruct template_id: nil,
            class_id: nil,
            class_label: nil,
            name: nil,
            description: nil,
            shape: nil,
            raw_sparql: nil,
            sparql: nil,
            params: [],
            defaults: %{},
            feature_tags: [],
            complexity: :medium,
            stress_points: [],
            rewrites: [],
            variant_support: [:raw],
            source: %{}

  @type t :: %__MODULE__{
          template_id: String.t(),
          class_id: atom(),
          class_label: String.t(),
          name: String.t(),
          description: String.t(),
          shape: atom() | nil,
          raw_sparql: String.t(),
          sparql: String.t(),
          params: [atom()],
          defaults: %{optional(atom()) => String.t()},
          feature_tags: [atom() | String.t()],
          complexity: :simple | :medium | :complex,
          stress_points: [atom() | String.t()],
          rewrites: [Normalizer.rewrite()],
          variant_support: [Contract.execution_variant()],
          source: map()
        }

  @type validation_error ::
          {:template_id, String.t()}
          | {:class_id, String.t()}
          | {:class_label, String.t()}
          | {:name, String.t()}
          | {:description, String.t()}
          | {:sparql, String.t()}
          | {:params, String.t()}
          | {:defaults, String.t()}
          | {:feature_tags, String.t()}
          | {:complexity, String.t()}
          | {:stress_points, String.t()}
          | {:variant_support, String.t()}
          | {:source, String.t()}

  @doc """
  Builds and validates a normalized Scholia template.
  """
  @spec new(map() | keyword()) :: {:ok, t()} | {:error, [validation_error()]}
  def new(attrs) when is_map(attrs) or is_list(attrs) do
    attrs = Enum.into(attrs, %{})
    raw_sparql = Map.get(attrs, :raw_sparql)
    rewrites = Map.get(attrs, :rewrites, [])

    template = %__MODULE__{
      template_id: Map.get(attrs, :template_id),
      class_id: Map.get(attrs, :class_id),
      class_label: Map.get(attrs, :class_label),
      name: Map.get(attrs, :name),
      description: Map.get(attrs, :description),
      shape: Map.get(attrs, :shape),
      raw_sparql: raw_sparql,
      sparql: Normalizer.normalize(raw_sparql, rewrites: rewrites),
      params: Map.get(attrs, :params, []),
      defaults: Map.get(attrs, :defaults, %{}),
      feature_tags: Map.get(attrs, :feature_tags, []),
      complexity: Map.get(attrs, :complexity, :medium),
      stress_points: Map.get(attrs, :stress_points, []),
      rewrites: rewrites,
      variant_support: Map.get(attrs, :variant_support, [:raw]),
      source: Map.get(attrs, :source, %{})
    }

    case validate(template) do
      :ok -> {:ok, template}
      {:error, errors} -> {:error, errors}
    end
  end

  @doc """
  Validates a Scholia template struct.
  """
  @spec validate(t()) :: :ok | {:error, [validation_error()]}
  def validate(%__MODULE__{} = template) do
    errors =
      []
      |> validate_template_id(template)
      |> validate_class_id(template)
      |> validate_class_label(template)
      |> validate_name(template)
      |> validate_description(template)
      |> validate_sparql(template)
      |> validate_params(template)
      |> validate_defaults(template)
      |> validate_feature_tags(template)
      |> validate_complexity(template)
      |> validate_stress_points(template)
      |> validate_variant_support(template)
      |> validate_source(template)

    case errors do
      [] -> :ok
      _ -> {:error, Enum.reverse(errors)}
    end
  end

  defp validate_template_id(errors, %__MODULE__{template_id: template_id})
       when is_binary(template_id) and template_id != "",
       do: errors

  defp validate_template_id(errors, _template),
    do: [{:template_id, "must be a non-empty string"} | errors]

  defp validate_class_id(errors, %__MODULE__{class_id: class_id}) when is_atom(class_id),
    do: errors

  defp validate_class_id(errors, _template), do: [{:class_id, "must be an atom"} | errors]

  defp validate_class_label(errors, %__MODULE__{class_label: class_label})
       when is_binary(class_label) and class_label != "",
       do: errors

  defp validate_class_label(errors, _template),
    do: [{:class_label, "must be a non-empty string"} | errors]

  defp validate_name(errors, %__MODULE__{name: name}) when is_binary(name) and name != "",
    do: errors

  defp validate_name(errors, _template), do: [{:name, "must be a non-empty string"} | errors]

  defp validate_description(errors, %__MODULE__{description: description})
       when is_binary(description) and description != "",
       do: errors

  defp validate_description(errors, _template),
    do: [{:description, "must be a non-empty string"} | errors]

  defp validate_sparql(errors, %__MODULE__{raw_sparql: raw_sparql, sparql: sparql})
       when is_binary(raw_sparql) and raw_sparql != "" and is_binary(sparql) and sparql != "",
       do: errors

  defp validate_sparql(errors, _template),
    do: [{:sparql, "must include non-empty raw and normalized query text"} | errors]

  defp validate_params(errors, %__MODULE__{params: params}) when is_list(params) do
    if Enum.all?(params, &is_atom/1) do
      errors
    else
      [{:params, "must contain only atoms"} | errors]
    end
  end

  defp validate_params(errors, _template), do: [{:params, "must be a list"} | errors]

  defp validate_defaults(errors, %__MODULE__{defaults: defaults}) when is_map(defaults) do
    if Enum.all?(defaults, fn {key, value} -> is_atom(key) and is_binary(value) end) do
      errors
    else
      [{:defaults, "must map atom keys to string defaults"} | errors]
    end
  end

  defp validate_defaults(errors, _template), do: [{:defaults, "must be a map"} | errors]

  defp validate_feature_tags(errors, %__MODULE__{feature_tags: feature_tags})
       when is_list(feature_tags) do
    if Enum.all?(feature_tags, &(is_atom(&1) or is_binary(&1))) do
      errors
    else
      [{:feature_tags, "must contain only atoms or strings"} | errors]
    end
  end

  defp validate_feature_tags(errors, _template),
    do: [{:feature_tags, "must be a list"} | errors]

  defp validate_complexity(errors, %__MODULE__{complexity: complexity}) do
    if complexity in @complexities do
      errors
    else
      [{:complexity, "must be one of #{inspect(@complexities)}"} | errors]
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

  defp validate_stress_points(errors, _template),
    do: [{:stress_points, "must be a list"} | errors]

  defp validate_variant_support(errors, %__MODULE__{variant_support: variant_support})
       when is_list(variant_support) do
    if Enum.all?(variant_support, &Contract.execution_variant?/1) do
      errors
    else
      [{:variant_support, "must contain only benchmark execution variants"} | errors]
    end
  end

  defp validate_variant_support(errors, _template),
    do: [{:variant_support, "must be a list"} | errors]

  defp validate_source(errors, %__MODULE__{source: source}) when is_map(source) do
    if Map.has_key?(source, :kind) and Map.has_key?(source, :location) do
      errors
    else
      [{:source, "must include :kind and :location"} | errors]
    end
  end

  defp validate_source(errors, _template), do: [{:source, "must be a map"} | errors]
end
