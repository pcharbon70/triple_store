defmodule TripleStore.Benchmark.Wikidata.PublicWorkloads do
  @moduledoc """
  Public-query workload importers for Wikidata-style benchmarks.

  This module packages three public workload families into a consistent local
  representation:

  - `:wgpb` graph-pattern workloads
  - `:wdqs` user-facing query workloads
  - `:wdbench` fragment-oriented workloads expanded to runnable queries
  """

  alias TripleStore.Benchmark.Wikidata.{Corpus, Manifest, Normalizer, Query}

  @wd "http://www.wikidata.org/entity/"
  @wdt "http://www.wikidata.org/prop/direct/"
  @rdfs "http://www.w3.org/2000/01/rdf-schema#"
  @default_dump_version "2024-10"
  @default_checksum "wikidata:2024-10:truthy-public-corpus"

  @type suite :: :wgpb | :wdqs | :wdbench

  @doc """
  Returns all public workload corpora keyed by suite.
  """
  @spec all_corpora(keyword()) :: %{suite() => Corpus.t()}
  def all_corpora(opts \\ []) when is_list(opts) do
    Enum.into([:wgpb, :wdqs, :wdbench], %{}, fn suite ->
      {:ok, corpus} = corpus(suite, opts)
      {suite, corpus}
    end)
  end

  @doc """
  Returns all public workload queries across all suites.
  """
  @spec all_queries(keyword()) :: [Query.t()]
  def all_queries(opts \\ []) when is_list(opts) do
    opts
    |> all_corpora()
    |> Map.values()
    |> Enum.flat_map(& &1.queries)
  end

  @doc """
  Returns a normalized corpus for a specific public workload family.
  """
  @spec corpus(suite(), keyword()) :: {:ok, Corpus.t()} | {:error, term()}
  def corpus(suite, opts \\ [])

  def corpus(:wgpb, opts),
    do: build_corpus(:wgpb, "Wikidata Graph Pattern Benchmark", wgpb_defs(), opts)

  def corpus(:wdqs, opts),
    do: build_corpus(:wdqs, "Wikidata Query Service Workload", wdqs_defs(), opts)

  def corpus(:wdbench, opts) do
    build_corpus(
      :wdbench,
      "Wikidata Fragment Benchmark",
      expand_wdbench_fragments(opts),
      opts,
      exclusions: wdbench_exclusions()
    )
  end

  @doc """
  Finds a query inside one of the public workload suites.
  """
  @spec get(suite(), String.t(), keyword()) :: {:ok, Query.t()} | {:error, term()}
  def get(suite, benchmark_id, opts \\ []) when is_binary(benchmark_id) and is_list(opts) do
    with {:ok, corpus} <- corpus(suite, opts) do
      Corpus.get(corpus, benchmark_id)
    end
  end

  @doc """
  Returns the default LIMIT policy for a dataset tier.
  """
  @spec limit_policy_for_tier(atom()) :: Normalizer.limit_policy()
  def limit_policy_for_tier(:smoke), do: {:cap, 25}
  def limit_policy_for_tier(:medium), do: {:cap, 100}
  def limit_policy_for_tier(:large), do: {:cap, 1_000}
  def limit_policy_for_tier(:full_dump), do: :preserve
  def limit_policy_for_tier(_tier), do: :preserve

  defp build_corpus(suite, name, query_defs, opts, extra_attrs \\ []) do
    tier = Keyword.get(opts, :tier, :full_dump)
    limit_policy = Keyword.get(opts, :limit_policy, limit_policy_for_tier(tier))

    queries =
      Enum.map(query_defs, fn query_def ->
        build_query(suite, query_def, tier, limit_policy)
      end)

    Corpus.new(
      suite: suite,
      name: name,
      description: corpus_description(suite),
      metadata: corpus_metadata(suite, tier, limit_policy),
      queries: queries,
      exclusions: Keyword.get(extra_attrs, :exclusions, [])
    )
  end

  defp build_query(suite, query_def, tier, limit_policy) do
    source = %{
      kind: :public_workload,
      location: Map.fetch!(query_def, :source_location),
      label: Map.fetch!(query_def, :source_label)
    }

    dataset = %{
      tier: tier,
      dump_version: @default_dump_version,
      checksum: @default_checksum,
      format: :ntriples,
      normalization_flags: [:truthy_only]
    }

    tags =
      query_def
      |> Map.get(:feature_tags, [])
      |> Enum.map(&to_string/1)
      |> Kernel.++([
        to_string(Map.fetch!(query_def, :group)),
        to_string(Map.fetch!(query_def, :shape))
      ])
      |> Enum.uniq()

    normalized_sparql =
      Normalizer.normalize(Map.fetch!(query_def, :sparql),
        rewrites: Map.get(query_def, :rewrites, []),
        limit_policy: limit_policy
      )

    {:ok, manifest} =
      Manifest.new(
        benchmark_id: Map.fetch!(query_def, :benchmark_id),
        suite: suite,
        category: Map.fetch!(query_def, :group),
        execution_variant: :raw,
        source: source,
        tags: tags,
        dataset: dataset
      )

    {:ok, query} =
      Query.new(
        manifest: manifest,
        name: Map.fetch!(query_def, :name),
        description: Map.fetch!(query_def, :description),
        raw_sparql: Map.fetch!(query_def, :sparql),
        sparql: normalized_sparql,
        params: Map.get(query_def, :params, []),
        group: Map.fetch!(query_def, :group),
        shape: Map.fetch!(query_def, :shape),
        feature_tags: Map.get(query_def, :feature_tags, []),
        answer_size_class: Map.get(query_def, :answer_size_class, :unknown),
        source_id: Map.get(query_def, :source_id),
        preprocessing: Map.get(query_def, :rewrites, []),
        stress_points: Map.get(query_def, :stress_points, [])
      )

    query
  end

  defp corpus_metadata(suite, tier, limit_policy) do
    %{
      source_origin: %{
        suite: suite,
        family: suite_origin_family(suite),
        paper: "Wikidata Workshop 2025 benchmarking study",
        location: "phase-02-public-workloads"
      },
      license_notes: [
        "Imported workload definitions are maintained as local benchmark assets.",
        "Query text was normalized for TripleStore compatibility where necessary."
      ],
      preprocessing_history: preprocessing_history(suite),
      dataset_tier: tier,
      limit_policy: limit_policy
    }
  end

  defp corpus_description(:wgpb),
    do: "Normalized graph-pattern workloads grouped by original WGPB query shape."

  defp corpus_description(:wdqs),
    do: "User-facing query workloads with stable IDs, metadata, and compatibility rewrites."

  defp corpus_description(:wdbench),
    do:
      "Fragment-derived workloads expanded into runnable benchmark queries with explicit exclusions."

  defp suite_origin_family(:wgpb), do: :graph_patterns
  defp suite_origin_family(:wdqs), do: :query_service_logs
  defp suite_origin_family(:wdbench), do: :fragment_logs

  defp preprocessing_history(:wgpb) do
    [
      "Imported representative graph-pattern queries with stable benchmark IDs.",
      "Applied tier-specific LIMIT policies without changing graph shape."
    ]
  end

  defp preprocessing_history(:wdqs) do
    [
      "Imported representative user-facing queries with stable IDs and names.",
      "Removed Blazegraph-specific hints when they were not standards compliant.",
      "Applied tier-specific LIMIT policies."
    ]
  end

  defp preprocessing_history(:wdbench) do
    [
      "Wrapped query fragments in full SELECT forms with stable identifiers.",
      "Applied tier-specific LIMIT policies by dataset tier.",
      "Tracked unconvertible fragments as explicit exclusions."
    ]
  end

  defp wgpb_defs do
    [
      %{
        benchmark_id: "wgpb-single-bgp-001",
        source_id: "WGPB-1",
        source_label: "WGPB Single BGP 1",
        source_location: "wikidata:wgpb:single-bgp:1",
        name: "Humans by instance",
        description: "Find entities that are directly typed as human.",
        group: :single_bgp,
        shape: :single_bgp,
        feature_tags: [:bgp, :instance_of],
        answer_size_class: :medium,
        sparql:
          prefixes() <>
            """
            SELECT ?person WHERE {
              ?person wdt:P31 wd:Q5 .
            }
            LIMIT 1000
            """,
        stress_points: [:high_fanout]
      },
      %{
        benchmark_id: "wgpb-path-bgp-001",
        source_id: "WGPB-2",
        source_label: "WGPB Path BGP 1",
        source_location: "wikidata:wgpb:path-bgp:1",
        name: "Humans with country",
        description: "Find humans together with their country of citizenship.",
        group: :path_bgp,
        shape: :path_bgp,
        feature_tags: [:bgp, :join],
        answer_size_class: :medium,
        sparql:
          prefixes() <>
            """
            SELECT ?person ?country WHERE {
              ?person wdt:P31 wd:Q5 .
              ?person wdt:P27 ?country .
            }
            LIMIT 1000
            """,
        stress_points: [:join_selectivity]
      },
      %{
        benchmark_id: "wgpb-star-bgp-001",
        source_id: "WGPB-3",
        source_label: "WGPB Star BGP 1",
        source_location: "wikidata:wgpb:star-bgp:1",
        name: "Humans with occupation and gender",
        description: "Find humans with multiple direct properties from the same subject.",
        group: :star_bgp,
        shape: :star_bgp,
        feature_tags: [:bgp, :star_join],
        answer_size_class: :small,
        sparql:
          prefixes() <>
            """
            SELECT ?person ?occupation ?gender WHERE {
              ?person wdt:P31 wd:Q5 ;
                      wdt:P106 ?occupation ;
                      wdt:P21 ?gender .
            }
            LIMIT 500
            """,
        stress_points: [:wide_subject_lookup]
      },
      %{
        benchmark_id: "wgpb-multi-bgp-001",
        source_id: "WGPB-4",
        source_label: "WGPB Multi BGP 1",
        source_location: "wikidata:wgpb:multi-bgp:1",
        name: "Humans linked to occupations in the subclass tree",
        description: "Join entity facts with a short subclass traversal anchor.",
        group: :multi_bgp,
        shape: :multi_bgp,
        feature_tags: [:bgp, :join, :taxonomy],
        answer_size_class: :small,
        sparql:
          prefixes() <>
            """
            SELECT ?person ?occupation WHERE {
              ?person wdt:P31 wd:Q5 ;
                      wdt:P106 ?occupation .
              ?occupation wdt:P279 wd:Q12737077 .
            }
            LIMIT 250
            """,
        stress_points: [:join_ordering]
      }
    ]
  end

  defp wdqs_defs do
    [
      %{
        benchmark_id: "wdqs-people-occupation-001",
        source_id: "WDQS-1",
        source_label: "WDQS Occupation Query",
        source_location: "wikidata:wdqs:people:occupation:1",
        name: "Humans with writer occupation",
        description:
          "Find human entities with a writer occupation using a user-facing query pattern.",
        group: :people,
        shape: :star_bgp,
        feature_tags: [:user_query, :bgp, :label_projection],
        answer_size_class: :small,
        rewrites: [:strip_blazegraph_hints],
        sparql: """
        PREFIX wd: <#{@wd}>
        PREFIX wdt: <#{@wdt}>
        PREFIX rdfs: <#{@rdfs}>
        PREFIX hint: <http://www.bigdata.com/queryHints#>

        SELECT ?person ?personLabel WHERE {
          hint:Query hint:optimizer "None" .
          ?person wdt:P31 wd:Q5 ;
                  wdt:P106 wd:Q36180 .
          OPTIONAL {
            ?person rdfs:label ?personLabel .
            FILTER(LANG(?personLabel) = "en")
          }
        }
        LIMIT 500
        """,
        stress_points: [:optimizer_sensitivity]
      },
      %{
        benchmark_id: "wdqs-country-citizenship-001",
        source_id: "WDQS-2",
        source_label: "WDQS Citizenship Query",
        source_location: "wikidata:wdqs:country:citizenship:1",
        name: "Humans in the United States",
        description: "Find humans with United States citizenship and optional labels.",
        group: :geography,
        shape: :optional,
        feature_tags: [:user_query, :optional],
        answer_size_class: :small,
        sparql:
          prefixes() <>
            """
            SELECT ?person ?personLabel WHERE {
              ?person wdt:P31 wd:Q5 ;
                      wdt:P27 wd:Q30 .
              OPTIONAL {
                ?person rdfs:label ?personLabel .
                FILTER(LANG(?personLabel) = "en")
              }
            }
            LIMIT 500
            """,
        stress_points: [:optional_join]
      },
      %{
        benchmark_id: "wdqs-subclass-tree-001",
        source_id: "WDQS-3",
        source_label: "WDQS Subclass Tree Query",
        source_location: "wikidata:wdqs:subclass-tree:1",
        name: "Classes under creative work",
        description: "Traverse a subclass hierarchy with a user-facing query pattern.",
        group: :taxonomy,
        shape: :property_path,
        feature_tags: [:user_query, :property_path, :taxonomy],
        answer_size_class: :small,
        sparql:
          prefixes() <>
            """
            SELECT ?class WHERE {
              ?class wdt:P279* wd:Q17537576 .
            }
            LIMIT 250
            """,
        stress_points: [:recursive_path]
      }
    ]
  end

  defp expand_wdbench_fragments(opts) do
    tier = Keyword.get(opts, :tier, :full_dump)
    limit_policy = Keyword.get(opts, :limit_policy, limit_policy_for_tier(tier))

    wdbench_fragment_defs()
    |> Enum.map(fn fragment_def ->
      fragment_def
      |> Map.put(
        :source_location,
        "wikidata:wdbench:#{fragment_def.family}:#{fragment_def.source_id}"
      )
      |> Map.put(:source_label, "WDBench #{fragment_def.family} #{fragment_def.source_id}")
      |> Map.put(:group, fragment_def.family)
      |> Map.put(:shape, Map.fetch!(fragment_def, :shape))
      |> Map.put(:feature_tags, [:fragment_expansion | Map.get(fragment_def, :feature_tags, [])])
      |> Map.put(:sparql, wrap_fragment(fragment_def.select, fragment_def.fragment, limit_policy))
    end)
  end

  defp wdbench_fragment_defs do
    [
      %{
        benchmark_id: "wdbench-single-bgp-001",
        source_id: "1",
        name: "Fragment: humans by instance",
        description: "Expand a single triple-pattern fragment into a runnable query.",
        family: :single_bgp,
        shape: :single_bgp,
        select: ["?person"],
        fragment: "?person wdt:P31 wd:Q5 .",
        feature_tags: [:bgp],
        answer_size_class: :medium,
        stress_points: [:high_fanout]
      },
      %{
        benchmark_id: "wdbench-multiple-bgps-001",
        source_id: "1",
        name: "Fragment: humans with country and occupation",
        description: "Expand a multi-BGP fragment into a runnable query.",
        family: :multiple_bgps,
        shape: :multi_bgp,
        select: ["?person", "?country", "?occupation"],
        fragment: """
        ?person wdt:P31 wd:Q5 .
        ?person wdt:P27 ?country .
        ?person wdt:P106 ?occupation .
        """,
        feature_tags: [:bgp, :join],
        answer_size_class: :small,
        stress_points: [:join_selectivity]
      },
      %{
        benchmark_id: "wdbench-optional-001",
        source_id: "1",
        name: "Fragment: humans with optional label",
        description: "Expand a fragment with OPTIONAL into a runnable query.",
        family: :optional,
        shape: :optional,
        select: ["?person", "?personLabel"],
        fragment: """
        ?person wdt:P31 wd:Q5 .
        OPTIONAL {
          ?person rdfs:label ?personLabel .
          FILTER(LANG(?personLabel) = "en")
        }
        """,
        feature_tags: [:optional],
        answer_size_class: :medium,
        stress_points: [:optional_join]
      },
      %{
        benchmark_id: "wdbench-property-paths-001",
        source_id: "1",
        name: "Fragment: subclass traversal",
        description: "Expand a property-path fragment into a runnable query.",
        family: :property_paths,
        shape: :property_path,
        select: ["?class"],
        fragment: "?class wdt:P279* wd:Q17537576 .",
        feature_tags: [:property_path],
        answer_size_class: :small,
        stress_points: [:recursive_path]
      },
      %{
        benchmark_id: "wdbench-other-001",
        source_id: "1",
        name: "Fragment: mixed union query",
        description: "Expand an 'other' fragment with a UNION into a runnable query.",
        family: :other,
        shape: :mixed,
        select: ["?person"],
        fragment: """
        {
          ?person wdt:P31 wd:Q5 .
          ?person wdt:P27 wd:Q30 .
        }
        UNION
        {
          ?person wdt:P31 wd:Q5 .
          ?person wdt:P106 wd:Q36180 .
        }
        """,
        feature_tags: [:union, :mixed],
        answer_size_class: :small,
        stress_points: [:union_branching]
      }
    ]
  end

  defp wdbench_exclusions do
    [
      %{
        benchmark_id: "wdbench-other-excluded-001",
        family: :other,
        reason: :engine_specific_fragment,
        classification: :requires_manual_rewrite,
        fragment:
          "SERVICE wikibase:label { bd:serviceParam wikibase:language \"[AUTO_LANGUAGE],en\". }",
        notes:
          "This fragment was kept as an explicit exclusion because it requires a label-service rewrite rather than direct fragment expansion."
      }
    ]
  end

  defp wrap_fragment(select_vars, fragment, limit_policy) do
    (prefixes() <>
       """
       SELECT #{Enum.join(select_vars, " ")} WHERE {
       #{indent_block(fragment)}
       }
       """)
    |> Normalizer.apply_limit_policy(limit_policy)
  end

  defp indent_block(block) do
    block
    |> String.trim()
    |> String.split("\n")
    |> Enum.map_join("\n", &"  #{String.trim_trailing(&1)}")
  end

  defp prefixes do
    """
    PREFIX wd: <#{@wd}>
    PREFIX wdt: <#{@wdt}>
    PREFIX rdfs: <#{@rdfs}>

    """
  end
end
