defmodule TripleStore.Benchmark.Wikidata.CorrectnessAndAutomationIntegrationTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias TripleStore.Benchmark.Wikidata.{
    AcceptedDivergence,
    Baseline,
    Correctness,
    Corpus,
    DatasetManifest,
    Manifest,
    Metrics,
    Operations,
    Query,
    Report,
    Runner
  }

  @moduletag :integration
  @xsd_integer "http://www.w3.org/2001/XMLSchema#integer"

  describe "4.3.1 correctness integration" do
    test "stable answers, count and distinct validation, and accepted divergences work end to end" do
      corpus = integration_corpus()
      manifest = dataset_manifest()

      assert {:ok, reference_run} =
               Runner.run(fake_store(), corpus,
                 dataset_manifest: manifest,
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 timeout_ms: 250,
                 executor: &reference_executor/3
               )

      assert {:ok, repeated_run} =
               Runner.run(fake_store(), corpus,
                 dataset_manifest: manifest,
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 timeout_ms: 250,
                 executor: &reference_executor/3
               )

      assert fingerprints_by_query(reference_run) == fingerprints_by_query(repeated_run)

      assert {:ok, baseline} =
               Baseline.from_run_result(reference_run,
                 baseline_id: "wikidata-phase-4-integration",
                 generated_at: ~U[2026-04-10 18:40:00Z]
               )

      assert {:ok, divergent_run} =
               Runner.run(fake_store(), corpus,
                 dataset_manifest: manifest,
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 timeout_ms: 250,
                 executor: &divergent_executor/3
               )

      path_reference = find_query_run(reference_run, "phase4-paths", :raw)
      path_actual = find_query_run(divergent_run, "phase4-paths", :raw)

      assert {:ok, accepted_divergence} =
               AcceptedDivergence.new(
                 benchmark_id: "phase4-paths",
                 execution_variant: :raw,
                 classification: :paths,
                 reference_fingerprint: path_reference.answer_record.fingerprint,
                 actual_fingerprint: path_actual.answer_record.fingerprint,
                 notes: "Accepted integration-test property-path divergence"
               )

      assert {:ok, correctness_run} =
               Correctness.attach(divergent_run,
                 baseline: baseline,
                 accepted_divergences: [accepted_divergence]
               )

      duplicate = find_query_run(correctness_run, "phase4-duplicates", :raw).correctness

      count_only =
        find_query_run(correctness_run, "phase4-semantics-count", :count_only).correctness

      distinct_only =
        find_query_run(correctness_run, "phase4-semantics-distinct", :distinct_only).correctness

      paths = find_query_run(correctness_run, "phase4-paths", :raw).correctness

      assert duplicate.status == :divergent
      assert duplicate.classification == :duplicates
      assert duplicate.divergence_count == 1

      assert count_only.status == :divergent
      assert count_only.classification == :limit_or_distinct_semantics
      assert count_only.divergence_count >= 1

      assert distinct_only.status == :divergent
      assert distinct_only.classification == :limit_or_distinct_semantics
      assert distinct_only.divergence_count >= 1

      assert paths.status == :accepted_divergence
      assert paths.classification == :paths
      assert paths.accepted

      assert {:ok, report} =
               Metrics.summarize(correctness_run,
                 report_id: "wikidata-phase-4-correctness",
                 generated_at: ~U[2026-04-10 18:45:00Z]
               )

      artifact_root = unique_tmp_dir!("wikidata_phase4_correctness_artifacts")

      assert {:ok, bundle} = Report.write(report, artifact_root)

      decoded = Jason.decode!(File.read!(bundle.paths.json))

      assert Enum.any?(decoded["query_summaries"], fn query_summary ->
               query_summary["benchmark_id"] == "phase4-duplicates" and
                 query_summary["divergence_classification"] == "duplicates"
             end)

      assert Enum.any?(decoded["query_summaries"], fn query_summary ->
               query_summary["benchmark_id"] == "phase4-semantics-count" and
                 query_summary["divergence_classification"] == "limit_or_distinct_semantics"
             end)

      assert Enum.any?(decoded["query_summaries"], fn query_summary ->
               query_summary["benchmark_id"] == "phase4-paths" and
                 query_summary["divergence_status"] == "accepted_divergence"
             end)
    end
  end

  describe "4.3.2 automation integration" do
    test "smoke workflows emit provenance, preserve accepted baselines, and fail thresholds clearly" do
      fixture_root = unique_tmp_dir!("wikidata_phase4_ops_fixture")
      output_root = unique_tmp_dir!("wikidata_phase4_ops_output")
      baseline_path = Path.join(output_root, "reference_answers.json")
      accepted_divergences_path = Path.join(output_root, "accepted_divergences.json")

      File.write!(accepted_divergences_path, "[]\n")

      assert {:ok, seed_run} =
               Operations.run(:smoke,
                 fixture_root: fixture_root,
                 output_root: output_root,
                 report_id: "wikidata-phase4-seed",
                 write_answer_baseline: baseline_path
               )

      seed_baseline = File.read!(baseline_path)

      assert {:ok, first_comparison} =
               Operations.run(:smoke,
                 fixture_root: fixture_root,
                 output_root: output_root,
                 report_id: "wikidata-phase4-history",
                 answer_baseline: baseline_path,
                 accepted_divergences: accepted_divergences_path
               )

      assert {:ok, second_comparison} =
               Operations.run(:smoke,
                 fixture_root: fixture_root,
                 output_root: output_root,
                 report_id: "wikidata-phase4-history",
                 answer_baseline: baseline_path,
                 accepted_divergences: accepted_divergences_path
               )

      assert File.read!(baseline_path) == seed_baseline

      assert first_comparison.artifact_bundle.output_dir !=
               second_comparison.artifact_bundle.output_dir

      assert String.ends_with?(second_comparison.artifact_bundle.output_dir, "-v2")

      Mix.Task.reenable("benchmark.wikidata")

      task_output =
        capture_io(fn ->
          Mix.Tasks.Benchmark.Wikidata.run([
            "smoke",
            "--fixture-root",
            unique_tmp_dir!("wikidata_phase4_mix_fixture"),
            "--output-root",
            output_root,
            "--report-id",
            "wikidata-phase4-mix",
            "--answer-baseline",
            baseline_path,
            "--accepted-divergences",
            accepted_divergences_path
          ])
        end)

      assert task_output =~ "Benchmark run passed"
      assert task_output =~ "wikidata-phase4-mix"

      summary_path =
        Path.join([
          output_root,
          "smoke",
          "wikidata-phase4-mix",
          "summary.json"
        ])

      summary = Jason.decode!(File.read!(summary_path))

      assert summary["dataset_manifest"]["dataset_id"] == "wikidata-built-in-smoke"

      assert summary["dataset_manifest"]["source_url"] ==
               "repo://priv/benchmarks/wikidata/fixtures/smoke.nt"

      assert summary["runtime_config"]["dataset_tier"] == "smoke"
      assert summary["runtime_metadata"]["hostname"] not in [nil, ""]

      ci_workflow = File.read!(".github/workflows/ci.yml")
      scheduled_workflow = File.read!(".github/workflows/wikidata-benchmarks.yml")

      assert ci_workflow =~ "Upload smoke benchmark artifacts"
      assert ci_workflow =~ "path: tmp/wikidata_ci_runs"
      assert scheduled_workflow =~ "Upload benchmark artifacts"
      assert scheduled_workflow =~ "path: tmp/wikidata_scheduled_runs"

      assert {:error, {:threshold_failed, failures}} =
               Operations.run(:smoke,
                 fixture_root: unique_tmp_dir!("wikidata_phase4_threshold_fixture"),
                 output_root: unique_tmp_dir!("wikidata_phase4_threshold_output"),
                 report_id: "wikidata-phase4-threshold",
                 max_adjusted_p95_us: 1
               )

      assert Enum.any?(failures, &String.contains?(&1, "wgpb/"))
      assert Enum.any?(failures, &String.contains?(&1, "wgpb-single-bgp-001"))
      assert seed_run.report.dataset_manifest.dataset_id == "wikidata-built-in-smoke"
    end
  end

  defp integration_corpus do
    {:ok, duplicates_query} =
      build_query(
        "phase4-duplicates",
        """
        SELECT ?person WHERE {
          VALUES ?person { <http://example.org/integration/duplicates> }
        }
        """,
        shape: :single_bgp,
        feature_tags: [:bgp],
        group: :correctness
      )

    {:ok, path_query} =
      build_query(
        "phase4-paths",
        """
        SELECT ?work WHERE {
          VALUES ?work { <http://example.org/integration/path> }
        }
        """,
        shape: :property_path,
        feature_tags: [:property_path],
        stress_points: [:property_path],
        group: :correctness
      )

    {:ok, count_base_query} =
      build_query(
        "phase4-semantics-count",
        """
        SELECT ?person WHERE {
          VALUES ?person {
            <http://example.org/integration/semantics-count-a>
            <http://example.org/integration/semantics-count-b>
          }
        }
        """,
        shape: :single_bgp,
        feature_tags: [:bgp],
        group: :correctness
      )

    {:ok, count_query} = Query.with_variant(count_base_query, :count_only)

    {:ok, distinct_base_query} =
      build_query(
        "phase4-semantics-distinct",
        """
        SELECT ?person WHERE {
          VALUES ?person {
            <http://example.org/integration/semantics-distinct-a>
            <http://example.org/integration/semantics-distinct-b>
          }
        }
        """,
        shape: :single_bgp,
        feature_tags: [:bgp],
        group: :correctness
      )

    {:ok, distinct_query} = Query.with_variant(distinct_base_query, :distinct_only)

    {:ok, corpus} =
      Corpus.new(
        suite: :wdqs,
        name: "Phase 4 Correctness Integration",
        description: "Controlled end-to-end correctness integration workload",
        metadata: %{
          source_origin: "integration-test",
          license_notes: "internal test fixture",
          preprocessing_history: [:handcrafted]
        },
        queries: [duplicates_query, path_query, count_query, distinct_query],
        exclusions: []
      )

    corpus
  end

  defp build_query(benchmark_id, sparql, opts) do
    {:ok, manifest} =
      Manifest.new(
        benchmark_id: benchmark_id,
        suite: :wdqs,
        category: :integration,
        execution_variant: Keyword.get(opts, :execution_variant, :raw),
        source: %{kind: :integration_test, label: "Phase 4 integration"},
        tags: [:integration, :correctness],
        dataset: %{
          tier: :smoke,
          dump_version: "2024-10",
          checksum: "sha256:phase4-integration",
          format: :ntriples,
          normalization_flags: [:truthy_only]
        }
      )

    Query.new(%{
      manifest: manifest,
      name: benchmark_id,
      description: "Phase 4 integration query #{benchmark_id}",
      raw_sparql: sparql,
      sparql: sparql,
      group: Keyword.get(opts, :group),
      shape: Keyword.get(opts, :shape, :mixed),
      feature_tags: Keyword.get(opts, :feature_tags, []),
      answer_size_class: :small,
      source_id: benchmark_id,
      stress_points: Keyword.get(opts, :stress_points, []),
      template_metadata: %{integration_test: true}
    })
  end

  defp reference_executor(_store, sparql, _opts) do
    cond do
      String.contains?(sparql, "integration/duplicates") ->
        {:ok, [%{"person" => named_node("Q42")}]}

      String.contains?(sparql, "integration/path") ->
        {:ok, [%{"work" => named_node("QWork1")}]}

      String.contains?(sparql, "integration/semantics-count") ->
        {:ok, [%{"count" => typed_literal("2", @xsd_integer)}]}

      String.contains?(sparql, "integration/semantics-distinct") ->
        {:ok,
         [
           %{"person" => named_node("Q42")},
           %{"person" => named_node("Q80")}
         ]}

      true ->
        {:error, {:unsupported_query, sparql}}
    end
  end

  defp divergent_executor(_store, sparql, _opts) do
    cond do
      String.contains?(sparql, "integration/duplicates") ->
        {:ok,
         [
           %{"person" => named_node("Q42")},
           %{"person" => named_node("Q42")}
         ]}

      String.contains?(sparql, "integration/path") ->
        {:ok, [%{"work" => named_node("QWork2")}]}

      String.contains?(sparql, "integration/semantics-count") ->
        {:ok, [%{"count" => typed_literal("1", @xsd_integer)}]}

      String.contains?(sparql, "integration/semantics-distinct") ->
        {:ok, [%{"person" => named_node("Q42")}]}

      true ->
        {:error, {:unsupported_query, sparql}}
    end
  end

  defp fingerprints_by_query(run_result) do
    Map.new(run_result.query_runs, fn query_run ->
      {{query_run.benchmark_id, query_run.execution_variant}, query_run.answer_record.fingerprint}
    end)
  end

  defp find_query_run(run_result, benchmark_id, execution_variant) do
    Enum.find(run_result.query_runs, fn query_run ->
      query_run.benchmark_id == benchmark_id and query_run.execution_variant == execution_variant
    end)
  end

  defp fake_store do
    %{
      db: self(),
      dict_manager: self(),
      transaction: nil,
      path: "/tmp/wikidata_phase4_integration",
      schema: :triple
    }
  end

  defp dataset_manifest do
    {:ok, manifest} =
      DatasetManifest.new(
        dataset_id: "wikidata-phase-4-integration",
        tier: :smoke,
        source_url: "https://example.org/wikidata-phase-4-integration.nt",
        dump_version: "2024-10",
        checksum: "sha256:phase4-integration",
        triple_count: 4,
        format: :ntriples,
        normalization_flags: [:truthy_only]
      )

    manifest
  end

  defp named_node(id), do: {:named_node, "http://www.wikidata.org/entity/#{id}"}
  defp typed_literal(value, datatype), do: {:literal, :typed, value, datatype}

  defp unique_tmp_dir!(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.rm_rf!(path)
    File.mkdir_p!(path)
    path
  end
end
