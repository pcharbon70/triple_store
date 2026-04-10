defmodule TripleStore.Benchmark.Wikidata.Scholia do
  @moduledoc """
  Scholia-derived benchmark corpus support for TripleStore.

  Templates and representative items are stored as local benchmark assets so
  the benchmark remains reproducible while still allowing per-tier
  instantiation, execution variants, and explicit fallback reporting.
  """

  alias TripleStore.Benchmark.Wikidata.{
    Corpus,
    Manifest,
    PublicWorkloads,
    Query,
    ScholiaTemplate
  }

  @class_iris %{
    human: "http://www.wikidata.org/entity/Q5",
    scholarly_article: "http://www.wikidata.org/entity/Q13442814",
    organization: "http://www.wikidata.org/entity/Q43229",
    field_of_work: "http://www.wikidata.org/entity/Q4936952"
  }
  @default_dump_version "2024-10"
  @default_checksum "wikidata:2024-10:scholia-corpus"

  @doc """
  Returns the absolute asset path for a Scholia benchmark asset.
  """
  @spec asset_path(:templates | :representatives) :: String.t()
  def asset_path(:templates),
    do: Application.app_dir(:triple_store, "priv/benchmarks/wikidata/scholia_templates.exs")

  def asset_path(:representatives),
    do: Application.app_dir(:triple_store, "priv/benchmarks/wikidata/scholia_representatives.exs")

  @doc """
  Returns the normalized Scholia templates loaded from the local asset file.
  """
  @spec templates() :: [ScholiaTemplate.t()]
  def templates do
    :templates
    |> load_asset()
    |> Enum.map(fn attrs ->
      {:ok, template} = ScholiaTemplate.new(attrs)
      template
    end)
  end

  @doc """
  Returns the known Scholia template classes and labels.
  """
  @spec template_classes() :: [%{class_id: atom(), class_label: String.t()}]
  def template_classes do
    templates()
    |> Enum.map(&%{class_id: &1.class_id, class_label: &1.class_label})
    |> Enum.uniq()
  end

  @doc """
  Returns representative items for the given dataset tier.
  """
  @spec representatives(atom()) :: %{optional(atom()) => [map()]}
  def representatives(tier) when is_atom(tier) do
    load_asset(:representatives)
    |> Map.get(tier, %{})
  end

  @doc """
  Builds the instantiated Scholia benchmark corpus for a dataset tier.
  """
  @spec corpus(atom(), keyword()) :: {:ok, Corpus.t()} | {:error, term()}
  def corpus(tier, opts \\ []) when is_atom(tier) and is_list(opts) do
    requested_variants = Keyword.get(opts, :variants, [:raw, :count_only, :distinct_only])
    limit_policy = Keyword.get(opts, :limit_policy, PublicWorkloads.limit_policy_for_tier(tier))

    {queries, exclusions} =
      Enum.reduce(templates(), {[], []}, fn template, {queries, exclusions} ->
        items = Map.get(representatives(tier), template.class_id, [])

        if items == [] do
          {queries, [missing_instantiation_exclusion(template, tier) | exclusions]}
        else
          instantiated =
            Enum.flat_map(items, fn item ->
              instantiate(template, item,
                tier: tier,
                requested_variants: requested_variants,
                limit_policy: limit_policy
              )
            end)

          {queries ++ instantiated, exclusions}
        end
      end)

    Corpus.new(
      suite: :scholia,
      name: "Scholia Derived Benchmark",
      description:
        "Instantiated Scholia-style templates with per-tier representatives and variants.",
      metadata: corpus_metadata(tier, limit_policy),
      queries: queries,
      exclusions: Enum.reverse(exclusions)
    )
  end

  @doc """
  Instantiates a template for a representative item and returns all requested variants.
  """
  @spec instantiate(ScholiaTemplate.t(), map(), keyword()) :: [Query.t()]
  def instantiate(%ScholiaTemplate{} = template, representative, opts \\ [])
      when is_map(representative) do
    tier = Keyword.get(opts, :tier, :full_dump)
    requested_variants = Keyword.get(opts, :requested_variants, [:raw])
    limit_policy = Keyword.get(opts, :limit_policy, PublicWorkloads.limit_policy_for_tier(tier))
    bindings = parameter_bindings(template, representative, opts)

    dataset = %{
      tier: tier,
      dump_version: @default_dump_version,
      checksum: @default_checksum,
      format: :ntriples,
      normalization_flags: [:truthy_only, :scholia_normalized]
    }

    tags =
      template.feature_tags
      |> Enum.map(&to_string/1)
      |> Kernel.++([to_string(template.class_id), to_string(template.complexity)])
      |> Enum.uniq()

    {:ok, manifest} =
      Manifest.new(
        benchmark_id: benchmark_id(template, representative, :raw),
        suite: :scholia,
        category: template.class_id,
        execution_variant: :raw,
        source: template.source,
        tags: tags,
        dataset: dataset
      )

    {:ok, base_query} =
      Query.new(
        manifest: manifest,
        name: "#{template.name} for #{representative.label}",
        description: template.description,
        raw_sparql: substitute(template.raw_sparql, bindings),
        sparql:
          template.sparql
          |> substitute(bindings)
          |> TripleStore.Benchmark.Wikidata.Normalizer.apply_limit_policy(limit_policy),
        params: template.params,
        group: template.class_id,
        shape: template.shape,
        feature_tags: template.feature_tags,
        answer_size_class: :small,
        source_id: representative.id,
        preprocessing: template.rewrites,
        stress_points: template.stress_points,
        template_metadata: %{
          template_id: template.template_id,
          class_id: template.class_id,
          class_label: template.class_label,
          representative_id: representative.id,
          representative_label: representative.label,
          representative_iri: representative.iri,
          tier: tier,
          bindings: bindings,
          fallback_behavior: :skip_when_missing
        }
      )

    supported_variants =
      requested_variants
      |> Enum.uniq()
      |> Enum.filter(&(&1 in template.variant_support))

    Enum.map(supported_variants, fn execution_variant ->
      if execution_variant == :raw do
        base_query
      else
        {:ok, query} = Query.with_variant(base_query, execution_variant)
        update_variant_benchmark_id(query, template, representative, execution_variant)
      end
    end)
  end

  @doc """
  Returns merged template bindings for an item, its class, and common variables.
  """
  @spec parameter_bindings(ScholiaTemplate.t(), map(), keyword()) :: %{
          optional(atom()) => String.t()
        }
  def parameter_bindings(%ScholiaTemplate{} = template, representative, opts \\ [])
      when is_map(representative) and is_list(opts) do
    language = Keyword.get(opts, :language, Map.get(template.defaults, :language, "en"))
    limit = Keyword.get(opts, :limit, Map.get(template.defaults, :limit, "100"))
    class = Map.get(template.defaults, :class, class_iri(template.class_id))

    %{
      entity: representative.iri,
      entity_id: representative.id,
      entity_label: representative.label,
      class: class,
      class_id: Atom.to_string(template.class_id),
      class_label: template.class_label,
      language: to_string(language),
      limit: to_string(limit)
    }
  end

  defp corpus_metadata(tier, limit_policy) do
    %{
      source_origin: %{
        suite: :scholia,
        family: :scholia_templates,
        asset: asset_path(:templates)
      },
      license_notes: [
        "Scholia template metadata is stored as local benchmark assets under priv/benchmarks.",
        "Engine-specific template constructs are normalized before instantiation."
      ],
      preprocessing_history: [
        "Loaded Scholia template assets from the repository.",
        "Normalized label-service and Blazegraph-hint constructs.",
        "Instantiated representative items by dataset tier.",
        "Generated count-only and distinct-only variants when the template supports them."
      ],
      dataset_tier: tier,
      limit_policy: limit_policy,
      template_classes: template_classes()
    }
  end

  defp missing_instantiation_exclusion(template, tier) do
    %{
      benchmark_id: "#{template.template_id}-#{tier}-excluded",
      family: template.class_id,
      reason: :no_valid_instantiation,
      classification: :tier_fallback,
      fragment: template.raw_sparql,
      notes:
        "No representative items were defined for #{template.class_id} on tier #{tier}; the template is skipped explicitly."
    }
  end

  defp benchmark_id(template, representative, execution_variant) do
    "#{template.template_id}-#{String.downcase(representative.id)}-#{execution_variant}"
  end

  defp update_variant_benchmark_id(query, template, representative, execution_variant) do
    {:ok, manifest} =
      query.manifest
      |> Manifest.to_map()
      |> Map.put(:benchmark_id, benchmark_id(template, representative, execution_variant))
      |> Manifest.new()

    %{query | manifest: manifest}
  end

  defp class_iri(class_id), do: Map.fetch!(@class_iris, class_id)

  defp substitute(template_text, bindings) do
    Enum.reduce(bindings, template_text, fn {key, value}, acc ->
      key = to_string(key)
      value = to_string(value)

      acc
      |> String.replace("<%#{key}%>", "<#{value}>")
      |> String.replace("%#{key}%", value)
    end)
  end

  defp load_asset(kind) do
    {data, _bindings} = Code.eval_file(asset_path(kind))
    data
  end
end
