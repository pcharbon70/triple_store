defmodule TripleStore.Benchmark.Wikidata.MetricsTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.Metrics

  describe "summarize/2" do
    test "computes query, suite, and shape summaries from runner output" do
      assert {:ok, report} =
               Metrics.summarize(sample_run_result(),
                 report_id: "wikidata-smoke-suite-test",
                 generated_at: ~U[2026-04-10 16:30:00Z]
               )

      assert report.schema_version == 1
      assert report.report_version == 1
      assert report.report_id == "wikidata-smoke-suite-test"
      assert report.run.run_kind == :suite

      assert report.overall_summary.query_count == 3
      assert report.overall_summary.total_measurement_iterations == 6
      assert report.overall_summary.total_successes == 3
      assert report.overall_summary.total_failures == 3
      assert report.overall_summary.completion_rate == 0.5
      assert report.overall_summary.divergence_status == :not_evaluated

      first_query = Enum.find(report.query_summaries, &(&1.benchmark_id == "wgpb-1"))
      assert first_query.raw_timing_summary.mean_us == 200.0
      assert first_query.adjusted_timing_summary.median_us == 200.0
      assert first_query.throughput.raw_queries_per_sec == 5_000.0
      assert first_query.error_totals.timeout == 0

      timeout_query = Enum.find(report.query_summaries, &(&1.benchmark_id == "wgpb-2"))
      assert timeout_query.error_totals.timeout == 1
      assert timeout_query.partial_failure_class == :flaky_run
      assert timeout_query.adjusted_timing_summary.mean_us == 700.0

      wgpb_summary = Enum.find(report.suite_summaries, &(&1.group_key == :wgpb))
      assert wgpb_summary.query_count == 2
      assert wgpb_summary.total_successes == 3
      assert wgpb_summary.total_failures == 1
      assert wgpb_summary.completion_rate == 0.75
      assert wgpb_summary.error_totals.timeout == 1

      shape_summary = Enum.find(report.aggregates.by_query_shape, &(&1.group_key == :unknown))
      assert shape_summary.query_count == 1
      assert shape_summary.error_totals.parse_error == 2
      assert shape_summary.partial_failure_breakdown.hard_incompatibility == 1
    end

    test "rejects invalid run results" do
      assert {:error, :invalid_run_result} = Metrics.summarize(%{})
    end
  end

  defp sample_run_result do
    %{
      schema_version: 1,
      run_kind: :suite,
      started_at: ~U[2026-04-10 16:25:00Z],
      completed_at: ~U[2026-04-10 16:25:02Z],
      duration_ms: 2_000,
      target: %{suite: :wgpb, query_count: 3},
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
        captured_at: ~U[2026-04-10 16:24:59Z],
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
          partial_failure_class: :flaky_run,
          failures: [%{iteration: 2, class: :timeout}],
          template_metadata: nil
        },
        %{
          benchmark_id: "wdqs-1",
          query_name: "WDQS 1",
          suite: :wdqs,
          category: :mixed,
          group: :long_tail,
          shape: nil,
          execution_variant: :count_only,
          source_id: "wdqs-source-1",
          feature_tags: [:count],
          answer_size_class: :unknown,
          raw_query_text: "INVALID",
          normalized_query_text: "INVALID",
          stress_points: [:parser],
          measurement_iterations: 2,
          warmup_iterations: 1,
          timeout_ms: 250,
          raw_timings_us: [],
          adjusted_timings_us: [1_000, 1_000],
          iterations: [],
          completion_rate: 0.0,
          success_count: 0,
          timeout_count: 0,
          parser_error_count: 2,
          execution_error_count: 0,
          cancellation_count: 0,
          out_of_memory_count: 0,
          result_count: nil,
          failure_count: 2,
          penalty_count: 2,
          partial_failure_class: :hard_incompatibility,
          failures: [%{iteration: 1, class: :parse_error}, %{iteration: 2, class: :parse_error}],
          template_metadata: nil
        }
      ]
    }
  end
end
