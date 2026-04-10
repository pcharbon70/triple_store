defmodule TripleStore.Benchmark.Wikidata.QueryCorpusIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.Wikidata.{
    Corpus,
    DatasetManifest,
    Fixture,
    PublicWorkloads,
    Query,
    Scholia,
    StoreFixture
  }

  alias TripleStore.SPARQL.Parser

  @moduletag :integration

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "wikidata_query_corpus_integration_#{System.unique_integer([:positive])}"
      )

    source_path = Path.join(root, "smoke.nt")
    File.mkdir_p!(root)
    File.write!(source_path, smoke_dataset())

    {:ok, manifest} =
      DatasetManifest.from_source(
        source_path,
        dataset_id: "wikidata-phase-2-smoke",
        tier: :smoke,
        source_url: "https://example.org/wikidata-phase-2-smoke.nt",
        dump_version: "2024-10",
        source_date: ~D[2024-10-01],
        normalization_flags: [:truthy_only]
      )

    {:ok, registered_manifest} = Fixture.register_dataset(root, manifest, source_path)
    public_corpora = PublicWorkloads.all_corpora(tier: :smoke)
    {:ok, scholia_smoke_corpus} = Scholia.corpus(:smoke)
    {:ok, scholia_medium_corpus} = Scholia.corpus(:medium)

    on_exit(fn -> File.rm_rf(root) end)

    {:ok,
     root: root,
     manifest: registered_manifest,
     public_corpora: public_corpora,
     scholia_smoke_corpus: scholia_smoke_corpus,
     scholia_medium_corpus: scholia_medium_corpus}
  end

  describe "2.3.1 corpus validation integration" do
    test "every normalized benchmark query parses and belongs to a suite/category", %{
      public_corpora: public_corpora,
      scholia_smoke_corpus: scholia_smoke_corpus,
      scholia_medium_corpus: scholia_medium_corpus
    } do
      all_queries =
        public_corpora
        |> Map.values()
        |> Enum.flat_map(& &1.queries)
        |> Kernel.++(scholia_smoke_corpus.queries)
        |> Kernel.++(scholia_medium_corpus.queries)

      Enum.each(all_queries, fn query ->
        assert {:ok, _ast} = Parser.parse(query.sparql)
        assert query.manifest.suite in [:wgpb, :wdbench, :wdqs, :scholia]
        refute is_nil(query.manifest.category)
        refute is_nil(query.group)
      end)
    end

    test "instantiated templates remain parseable and explicit exclusions are preserved", %{
      public_corpora: public_corpora,
      scholia_smoke_corpus: scholia_smoke_corpus,
      scholia_medium_corpus: scholia_medium_corpus
    } do
      Enum.each(scholia_smoke_corpus.queries ++ scholia_medium_corpus.queries, fn query ->
        assert {:ok, _ast} = Parser.parse(query.sparql)
        assert query.template_metadata.template_id != nil
        assert query.template_metadata.representative_id != nil
      end)

      wdbench_exclusions = public_corpora.wdbench.exclusions
      scholia_exclusions = scholia_smoke_corpus.exclusions

      refute wdbench_exclusions == []
      refute scholia_exclusions == []

      assert Enum.all?(wdbench_exclusions ++ scholia_exclusions, fn exclusion ->
               is_binary(exclusion.benchmark_id) and exclusion.benchmark_id != "" and
                 is_atom(exclusion.reason) and is_binary(exclusion.fragment)
             end)

      refute Enum.any?(
               wdbench_exclusions,
               &(&1.benchmark_id in Corpus.query_ids(public_corpora.wdbench))
             )

      refute Enum.any?(
               scholia_exclusions,
               &(&1.benchmark_id in Corpus.query_ids(scholia_smoke_corpus))
             )
    end
  end

  describe "2.3.2 query execution smoke integration" do
    test "representative queries from every workload family execute successfully", %{
      root: root,
      manifest: manifest,
      public_corpora: public_corpora,
      scholia_smoke_corpus: scholia_smoke_corpus
    } do
      {:ok, fixture_state} =
        StoreFixture.setup(root, manifest,
          store_id: "wikidata-phase-2-store",
          load_preset: :truthy_only,
          warmup: fn store -> TripleStore.stats(store) end
        )

      try do
        sample_queries = [
          hd(public_corpora.wgpb.queries),
          hd(public_corpora.wdbench.queries),
          hd(public_corpora.wdqs.queries),
          hd(scholia_smoke_corpus.queries)
        ]

        Enum.each(sample_queries, fn query ->
          assert {:ok, results} = TripleStore.query(fixture_state.store, query.sparql)
          assert length(results) >= 1
        end)
      after
        assert :ok = StoreFixture.teardown(fixture_state, delete_store: true)
      end
    end

    test "count-only and distinct-only variants execute and query metadata survives into runtime records",
         %{
           root: root,
           manifest: manifest,
           public_corpora: public_corpora,
           scholia_smoke_corpus: scholia_smoke_corpus
         } do
      {:ok, fixture_state} =
        StoreFixture.setup(root, manifest,
          store_id: "wikidata-phase-2-variant-store",
          load_preset: :truthy_only,
          warmup: fn store -> TripleStore.stats(store) end
        )

      try do
        {:ok, wdbench_distinct} =
          public_corpora.wdbench
          |> Corpus.get("wdbench-single-bgp-001")
          |> then(fn {:ok, query} -> Query.with_variant(query, :distinct_only) end)

        {:ok, scholia_count} =
          Corpus.get(scholia_smoke_corpus, "scholia-author-works-q42-count_only")

        assert {:ok, distinct_results} =
                 TripleStore.query(fixture_state.store, wdbench_distinct.sparql)

        assert {:ok, count_results} = TripleStore.query(fixture_state.store, scholia_count.sparql)

        assert length(distinct_results) >= 1
        assert length(count_results) == 1

        runtime_record = %{
          benchmark_id: Query.benchmark_id(scholia_count),
          suite: scholia_count.manifest.suite,
          category: scholia_count.manifest.category,
          group: scholia_count.group,
          template_id: scholia_count.template_metadata.template_id,
          representative_id: scholia_count.template_metadata.representative_id,
          result_count: length(count_results)
        }

        assert runtime_record.benchmark_id == "scholia-author-works-q42-count_only"
        assert runtime_record.suite == :scholia
        assert runtime_record.category == :human
        assert runtime_record.group == :human
        assert runtime_record.template_id == "scholia-author-works"
        assert runtime_record.representative_id == "Q42"
        assert runtime_record.result_count == 1
      after
        assert :ok = StoreFixture.teardown(fixture_state, delete_store: true)
      end
    end
  end

  defp smoke_dataset do
    Enum.join(
      [
        triple("Q42", "P31", "Q5"),
        triple("Q42", "P106", "Q36180"),
        triple("Q42", "P21", "Q6581097"),
        triple("Q42", "P27", "Q30"),
        triple("Q42", "P101", "QFOW1"),
        triple("Q42", "P108", "QOrg1"),
        label("Q42", "Douglas Adams"),
        triple("Q80", "P31", "Q5"),
        triple("Q80", "P106", "Q36180"),
        triple("Q80", "P21", "Q6581097"),
        triple("Q80", "P27", "Q30"),
        triple("Q80", "P101", "QFOW1"),
        triple("Q80", "P108", "QOrg1"),
        label("Q80", "Tim Berners-Lee"),
        triple("Q36180", "P279", "Q12737077"),
        label("Q36180", "writer"),
        label("Q12737077", "occupation"),
        triple("QOrg1", "P31", "Q43229"),
        label("QOrg1", "Example Research Org"),
        triple("QWork1", "P50", "Q42"),
        label("QWork1", "The Hitchhiker's Guide"),
        triple("QWork2", "P50", "Q42"),
        label("QWork2", "Dirk Gently"),
        triple("QPaper1", "P31", "Q13442814"),
        label("QPaper1", "Example Article One"),
        triple("QCiting1", "P2860", "QPaper1"),
        label("QCiting1", "Citing Article One"),
        triple("QNovel", "P279", "Q17537576"),
        label("QNovel", "Novel"),
        triple("QEssay", "P279", "Q17537576"),
        label("QEssay", "Essay"),
        label("Q17537576", "creative work"),
        label("QFOW1", "Astrophysics")
      ],
      "\n"
    )
  end

  defp triple(subject_id, predicate_id, object_id) do
    """
    <http://www.wikidata.org/entity/#{subject_id}> <http://www.wikidata.org/prop/direct/#{predicate_id}> <http://www.wikidata.org/entity/#{object_id}> .
    """
    |> String.trim()
  end

  defp label(subject_id, value) do
    """
    <http://www.wikidata.org/entity/#{subject_id}> <http://www.w3.org/2000/01/rdf-schema#label> "#{value}"@en .
    """
    |> String.trim()
  end
end
