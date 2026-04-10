defmodule TripleStore.Benchmark.Wikidata.RunnerAndReportIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.Wikidata.{
    DatasetManifest,
    Fixture,
    Metrics,
    PublicWorkloads,
    Query,
    Report,
    Runner,
    Scholia,
    StoreFixture
  }

  @moduletag :integration

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "wikidata_runner_report_integration_#{System.unique_integer([:positive])}"
      )

    source_path = Path.join(root, "smoke.nt")
    File.mkdir_p!(root)
    File.write!(source_path, smoke_dataset())

    {:ok, manifest} =
      DatasetManifest.from_source(
        source_path,
        dataset_id: "wikidata-phase-3-smoke",
        tier: :smoke,
        source_url: "https://example.org/wikidata-phase-3-smoke.nt",
        dump_version: "2024-10",
        source_date: ~D[2024-10-01],
        normalization_flags: [:truthy_only]
      )

    {:ok, registered_manifest} = Fixture.register_dataset(root, manifest, source_path)
    public_corpora = PublicWorkloads.all_corpora(tier: :smoke)
    {:ok, scholia_corpus} = Scholia.corpus(:smoke)

    {:ok, fixture_state} =
      StoreFixture.setup(root, registered_manifest,
        store_id: "wikidata-phase-3-store",
        load_preset: :truthy_only,
        warmup: fn store -> TripleStore.stats(store) end
      )

    on_exit(fn ->
      if File.exists?(fixture_state.store_path) do
        _ = StoreFixture.teardown(fixture_state, delete_store: true)
      end

      File.rm_rf(root)
    end)

    {:ok,
     root: root,
     manifest: registered_manifest,
     public_corpora: public_corpora,
     scholia_corpus: scholia_corpus,
     fixture_state: fixture_state}
  end

  describe "3.3.1 runner integration" do
    test "warmup runs stay out of measured timings and single, suite, and matrix interfaces all complete",
         %{fixture_state: fixture_state, public_corpora: public_corpora} do
      query = hd(public_corpora.wgpb.queries)
      wgpb_id = Query.benchmark_id(query)
      wdqs_id = public_corpora.wdqs.queries |> hd() |> Query.benchmark_id()
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      counting_executor = fn store, sparql, opts ->
        Agent.update(counter, &(&1 + 1))
        TripleStore.query(store, sparql, opts)
      end

      assert {:ok, single_result} =
               Runner.run(fixture_state, query,
                 warmup_iterations: 2,
                 measurement_iterations: 3,
                 timeout_ms: 500,
                 executor: counting_executor
               )

      assert Agent.get(counter, & &1) == 5
      [single_query_run] = single_result.query_runs
      assert length(single_query_run.iterations) == 3
      assert length(single_query_run.adjusted_timings_us) == 3

      assert {:ok, suite_result} =
               Runner.run(fixture_state, public_corpora.wgpb,
                 query_ids: [wgpb_id],
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 timeout_ms: 500
               )

      assert suite_result.run_kind == :suite
      assert Enum.map(suite_result.query_runs, & &1.benchmark_id) == [wgpb_id]

      assert {:ok, matrix_result} =
               Runner.run(fixture_state, [:wgpb, :wdqs],
                 dataset_tier: :smoke,
                 query_ids: [wgpb_id, wdqs_id],
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 timeout_ms: 500
               )

      assert matrix_result.run_kind == :matrix

      assert Enum.sort(Enum.uniq(Enum.map(matrix_result.query_runs, & &1.suite))) == [
               :wdqs,
               :wgpb
             ]
    end

    test "timeout and parse failures produce adjusted timings and structured errors", %{
      fixture_state: fixture_state,
      public_corpora: public_corpora
    } do
      template = hd(public_corpora.wgpb.queries)

      {:ok, parse_query} =
        Query.new(%{
          manifest:
            template.manifest
            |> TripleStore.Benchmark.Wikidata.Manifest.to_map()
            |> Map.put(:benchmark_id, "phase-3-parse-error"),
          name: "Invalid parse query",
          description: "Forces a parser failure",
          raw_sparql: "INVALID SPARQL",
          sparql: "INVALID SPARQL",
          group: template.group,
          shape: template.shape,
          feature_tags: template.feature_tags,
          stress_points: [:parser]
        })

      assert {:ok, parse_result} =
               Runner.run(fixture_state, parse_query,
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 timeout_ms: 100,
                 penalty_us: 50_000,
                 long_running_threshold_us: 10_000
               )

      [parse_query_run] = parse_result.query_runs
      [parse_iteration] = parse_query_run.iterations
      assert parse_iteration.status == :error
      assert parse_iteration.error_class == :parse_error
      assert parse_iteration.penalty_reason == :failure
      assert hd(parse_query_run.adjusted_timings_us) >= 50_000

      timeout_executor = fn _store, _sparql, _opts -> {:error, :timeout} end

      assert {:ok, timeout_result} =
               Runner.run(fixture_state, template,
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 timeout_ms: 25,
                 penalty_us: 75_000,
                 long_running_threshold_us: 10_000,
                 executor: timeout_executor
               )

      [timeout_query_run] = timeout_result.query_runs
      [timeout_iteration] = timeout_query_run.iterations
      assert timeout_iteration.status == :error
      assert timeout_iteration.error_class == :timeout
      assert timeout_query_run.partial_failure_class == :hard_incompatibility
      assert hd(timeout_query_run.adjusted_timings_us) >= 75_000
    end
  end

  describe "3.3.2 artifact integration" do
    test "metrics and artifacts stay consistent with the executed run", %{
      root: root,
      manifest: manifest,
      fixture_state: fixture_state,
      public_corpora: public_corpora,
      scholia_corpus: scholia_corpus
    } do
      wgpb_id = public_corpora.wgpb.queries |> hd() |> Query.benchmark_id()
      scholia_id = scholia_corpus.queries |> hd() |> Query.benchmark_id()

      assert {:ok, run_result} =
               Runner.run(fixture_state, [:wgpb, scholia_corpus],
                 query_ids: [wgpb_id, scholia_id],
                 warmup_iterations: 0,
                 measurement_iterations: 2,
                 timeout_ms: 500
               )

      assert {:ok, report} =
               Metrics.summarize(run_result,
                 report_id: "wikidata-phase-3-summary",
                 generated_at: ~U[2026-04-10 17:00:00Z]
               )

      assert report.dataset_manifest.dataset_id == manifest.dataset_id
      assert report.runtime_config.dataset_tier == :smoke

      assert report.overall_summary.total_successes ==
               Enum.sum(Enum.map(report.query_summaries, & &1.success_count))

      assert report.overall_summary.total_failures ==
               Enum.sum(Enum.map(report.query_summaries, & &1.failure_count))

      artifact_root = Path.join(root, "artifacts")

      assert {:ok, bundle} = Report.write(report, artifact_root)
      assert File.exists?(bundle.paths.json)
      assert File.exists?(bundle.paths.csv)
      assert File.exists?(bundle.paths.markdown)

      decoded = bundle.paths.json |> File.read!() |> Jason.decode!()
      csv = File.read!(bundle.paths.csv)
      markdown = File.read!(bundle.paths.markdown)

      assert decoded["runtime_config"]["dataset_tier"] == "smoke"
      assert decoded["dataset_manifest"]["dataset_id"] == manifest.dataset_id
      assert String.contains?(csv, wgpb_id)
      assert String.contains?(csv, scholia_id)
      assert String.contains?(markdown, "## Suite Summaries")
      assert String.contains?(markdown, "## Query Shape Aggregates")
    end

    test "reruns version artifacts and fixture teardown leaves no persisted benchmark store", %{
      root: root,
      fixture_state: fixture_state,
      public_corpora: public_corpora
    } do
      query = hd(public_corpora.wgpb.queries)

      assert {:ok, first_run} =
               Runner.run(fixture_state, query,
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 timeout_ms: 500
               )

      assert {:ok, second_run} =
               Runner.run(fixture_state, query,
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 timeout_ms: 500
               )

      assert {:ok, first_report} =
               Metrics.summarize(first_run,
                 report_id: "wikidata-phase-3-versioned",
                 generated_at: ~U[2026-04-10 17:05:00Z]
               )

      assert {:ok, second_report} =
               Metrics.summarize(second_run,
                 report_id: "wikidata-phase-3-versioned",
                 generated_at: ~U[2026-04-10 17:06:00Z]
               )

      artifact_root = Path.join(root, "versioned-artifacts")

      assert {:ok, first_bundle} = Report.write(first_report, artifact_root)
      assert {:ok, second_bundle} = Report.write(second_report, artifact_root)

      assert first_bundle.report.report_version == 1
      assert second_bundle.report.report_version == 2
      assert first_bundle.output_dir != second_bundle.output_dir
      assert File.exists?(fixture_state.store_path)

      assert :ok = StoreFixture.teardown(fixture_state, delete_store: true)
      refute File.exists?(fixture_state.store_path)
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
