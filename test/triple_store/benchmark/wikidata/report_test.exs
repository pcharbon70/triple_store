defmodule TripleStore.Benchmark.Wikidata.ReportTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.{Metrics, Report}

  describe "artifact rendering" do
    test "renders JSON, CSV, and Markdown outputs" do
      {:ok, report} =
        Metrics.summarize(sample_run_result(),
          report_id: "wikidata-smoke-suite-report",
          generated_at: ~U[2026-04-10 16:35:00Z]
        )

      json = Report.to_json(report)
      csv = Report.to_csv(report)
      markdown = Report.to_markdown(report)

      decoded = Jason.decode!(json)

      assert decoded["report_id"] == "wikidata-smoke-suite-report"
      assert decoded["overall_summary"]["group_key"] == "suite"
      assert String.contains?(csv, "benchmark_id,query_name,suite")
      assert String.contains?(csv, "wgpb-1")
      assert String.contains?(csv, "divergence_classification")
      assert String.contains?(csv, "accepted_divergence")
      assert String.contains?(markdown, "# Wikidata Benchmark Report")
      assert String.contains?(markdown, "## Dataset Provenance")
      assert String.contains?(markdown, "## Hardware Metadata")
      assert String.contains?(markdown, "Correctness")
    end

    test "writes versioned report bundles without clobbering prior artifacts" do
      {:ok, report} =
        Metrics.summarize(sample_run_result(),
          report_id: "wikidata-fixed-report",
          generated_at: ~U[2026-04-10 16:40:00Z]
        )

      output_root = unique_tmp_dir!("wikidata_report_test")

      assert {:ok, first_bundle} = Report.write(report, output_root)
      assert File.exists?(first_bundle.paths.json)
      assert File.exists?(first_bundle.paths.csv)
      assert File.exists?(first_bundle.paths.markdown)
      assert first_bundle.report.report_version == 1

      assert {:ok, second_bundle} = Report.write(report, output_root)
      assert second_bundle.report.report_version == 2
      assert second_bundle.output_dir != first_bundle.output_dir
      assert String.ends_with?(second_bundle.output_dir, "-v2")
    end
  end

  defp sample_run_result do
    %{
      schema_version: 1,
      run_kind: :suite,
      started_at: ~U[2026-04-10 16:34:00Z],
      completed_at: ~U[2026-04-10 16:34:01Z],
      duration_ms: 1_000,
      target: %{suite: :wgpb, query_count: 2},
      dataset_manifest: %{
        dataset_id: "wikidata-smoke",
        tier: :smoke,
        dump_version: "2024-10",
        triple_count: 10,
        checksum: "sha256:test",
        source_url: "https://example.org/wikidata-smoke.nt"
      },
      runtime_config: %{
        dataset_tier: :smoke,
        warmup_iterations: 1,
        measurement_iterations: 2,
        timeout_ms: 250,
        penalty_us: 1_000,
        long_running_threshold_us: 600
      },
      runtime_metadata: %{
        captured_at: ~U[2026-04-10 16:33:59Z],
        hostname: "benchmark-host",
        elixir_version: "1.19.5",
        otp_release: "28",
        system_architecture: "x86_64-apple-darwin",
        logical_processors: 8,
        logical_processors_available: 8,
        schedulers: 8,
        schedulers_online: 8,
        dirty_cpu_schedulers: 8,
        dirty_io_schedulers: 10
      },
      git_sha: "abc123def456",
      query_runs: [
        %{
          benchmark_id: "wgpb-1",
          query_name: "WGPB 1",
          suite: :wgpb,
          category: :single_bgp,
          group: :core,
          shape: :single_bgp,
          execution_variant: :raw,
          source_id: "wgpb-source-1",
          feature_tags: [:bgp],
          answer_size_class: :tiny,
          raw_query_text: "SELECT ?s WHERE { ?s ?p ?o }",
          normalized_query_text: "SELECT ?s WHERE { ?s ?p ?o }",
          stress_points: [:lookup],
          measurement_iterations: 2,
          warmup_iterations: 1,
          timeout_ms: 250,
          raw_timings_us: [100, 300],
          adjusted_timings_us: [100, 300],
          iterations: [],
          completion_rate: 1.0,
          success_count: 2,
          timeout_count: 0,
          parser_error_count: 0,
          execution_error_count: 0,
          cancellation_count: 0,
          out_of_memory_count: 0,
          result_count: 1,
          failure_count: 0,
          penalty_count: 0,
          answer_record: %{fingerprint: "answer-wgpb-1", row_count: 1},
          correctness: %{
            status: :match,
            classification: nil,
            accepted: false,
            answer_fingerprint: "answer-wgpb-1",
            reference_fingerprint: "reference-wgpb-1",
            actual_row_count: 1,
            reference_row_count: 1,
            divergence_count: 0,
            exemplars: []
          },
          partial_failure_class: :none,
          failures: [],
          template_metadata: nil
        },
        %{
          benchmark_id: "wgpb-2",
          query_name: "WGPB 2",
          suite: :wgpb,
          category: :optional,
          group: :core,
          shape: :optional,
          execution_variant: :raw,
          source_id: "wgpb-source-2",
          feature_tags: [:optional],
          answer_size_class: :small,
          raw_query_text: "SELECT ?s WHERE { ?s ?p ?o OPTIONAL { ?s ?p2 ?o2 } }",
          normalized_query_text: "SELECT ?s WHERE { ?s ?p ?o OPTIONAL { ?s ?p2 ?o2 } }",
          stress_points: [:optional_join],
          measurement_iterations: 2,
          warmup_iterations: 1,
          timeout_ms: 250,
          raw_timings_us: [400],
          adjusted_timings_us: [400, 1_000],
          iterations: [],
          completion_rate: 0.5,
          success_count: 1,
          timeout_count: 1,
          parser_error_count: 0,
          execution_error_count: 0,
          cancellation_count: 0,
          out_of_memory_count: 0,
          result_count: 2,
          failure_count: 1,
          penalty_count: 1,
          answer_record: %{fingerprint: "answer-wgpb-2", row_count: 2},
          correctness: %{
            status: :accepted_divergence,
            classification: :paths,
            accepted: true,
            answer_fingerprint: "answer-wgpb-2",
            reference_fingerprint: "reference-wgpb-2",
            actual_row_count: 2,
            reference_row_count: 1,
            divergence_count: 2,
            exemplars: [%{type: :unexpected, row: "row-1"}]
          },
          partial_failure_class: :flaky_run,
          failures: [%{iteration: 2, class: :timeout}],
          template_metadata: nil
        }
      ]
    }
  end

  defp unique_tmp_dir!(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.rm_rf!(path)
    File.mkdir_p!(path)
    path
  end
end
