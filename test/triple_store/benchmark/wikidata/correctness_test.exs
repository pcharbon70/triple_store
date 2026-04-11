defmodule TripleStore.Benchmark.Wikidata.CorrectnessTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.{
    AcceptedDivergence,
    AnswerNormalizer,
    Baseline,
    Correctness
  }

  describe "attach/2" do
    test "classifies datatype, duplicate, path, and parser divergences and honors accepted divergences" do
      datatype_actual =
        answer_record([
          %{"count" => {:literal, :typed, "1", "http://www.w3.org/2001/XMLSchema#boolean"}}
        ])

      datatype_reference =
        answer_record([
          %{"count" => {:literal, :typed, "true", "http://www.w3.org/2001/XMLSchema#boolean"}}
        ])

      duplicates_actual =
        answer_record([
          %{"person" => {:named_node, "http://example.org/Q42"}},
          %{"person" => {:named_node, "http://example.org/Q42"}}
        ])

      duplicates_reference =
        answer_record([
          %{"person" => {:named_node, "http://example.org/Q42"}}
        ])

      paths_actual =
        answer_record([
          %{"work" => {:named_node, "http://example.org/Q1"}}
        ])

      paths_reference =
        answer_record([
          %{"work" => {:named_node, "http://example.org/Q2"}}
        ])

      assert {:ok, baseline} =
               Baseline.from_run_result(%{
                 query_runs: [
                   query_run("datatype-q", :wgpb, :raw, datatype_reference),
                   query_run("duplicate-q", :wgpb, :raw, duplicates_reference),
                   query_run("path-q", :wdqs, :raw, paths_reference)
                 ],
                 runtime_config: %{dataset_tier: :smoke}
               })

      assert {:ok, accepted_divergence} =
               AcceptedDivergence.new(
                 benchmark_id: "path-q",
                 classification: :paths,
                 execution_variant: :raw,
                 reference_fingerprint: paths_reference.fingerprint,
                 actual_fingerprint: paths_actual.fingerprint,
                 notes: "Accepted path semantics difference"
               )

      run_result = %{
        query_runs: [
          query_run("datatype-q", :wgpb, :raw, datatype_actual),
          query_run("duplicate-q", :wgpb, :raw, duplicates_actual),
          query_run("path-q", :wdqs, :raw, paths_actual,
            shape: :property_path,
            feature_tags: [:property_path]
          ),
          parser_failure_query_run("parser-q", :wdqs, :count_only)
        ]
      }

      assert {:ok, enriched} =
               Correctness.attach(run_result,
                 baseline: baseline,
                 accepted_divergences: [accepted_divergence]
               )

      datatype = find_query(enriched.query_runs, "datatype-q").correctness
      assert datatype.status == :divergent
      assert datatype.classification == :datatype_handling

      duplicates = find_query(enriched.query_runs, "duplicate-q").correctness
      assert duplicates.status == :divergent
      assert duplicates.classification == :duplicates

      paths = find_query(enriched.query_runs, "path-q").correctness
      assert paths.status == :accepted_divergence
      assert paths.classification == :paths
      assert paths.accepted

      parser = find_query(enriched.query_runs, "parser-q").correctness
      assert parser.status == :missing_reference
      assert parser.classification == nil
    end

    test "marks parser failures as not comparable when a baseline exists" do
      reference =
        answer_record([
          %{
            "count" => {:literal, :typed, "1", "http://www.w3.org/2001/XMLSchema#integer"}
          }
        ])

      assert {:ok, baseline} =
               Baseline.from_run_result(%{
                 query_runs: [query_run("parser-q", :wdqs, :count_only, reference)],
                 runtime_config: %{dataset_tier: :smoke}
               })

      assert {:ok, enriched} =
               Correctness.attach(
                 %{query_runs: [parser_failure_query_run("parser-q", :wdqs, :count_only)]},
                 baseline: baseline
               )

      parser = find_query(enriched.query_runs, "parser-q").correctness
      assert parser.status == :not_comparable
      assert parser.classification == :parser
      assert parser.divergence_count == 1
    end
  end

  defp answer_record(results) do
    {:ok, record} =
      AnswerNormalizer.normalize(results,
        execution_variant: :raw,
        ordering: :unordered,
        blank_node_policy: :anonymous
      )

    record
  end

  defp query_run(benchmark_id, suite, execution_variant, answer_record, opts \\ []) do
    %{
      benchmark_id: benchmark_id,
      query_name: benchmark_id,
      suite: suite,
      category: :mixed,
      group: :test,
      shape: Keyword.get(opts, :shape, :mixed),
      execution_variant: execution_variant,
      source_id: "#{benchmark_id}-source",
      feature_tags: Keyword.get(opts, :feature_tags, []),
      answer_size_class: :small,
      raw_query_text: "SELECT * WHERE { ?s ?p ?o }",
      normalized_query_text: "SELECT * WHERE { ?s ?p ?o }",
      stress_points: [],
      measurement_iterations: 1,
      warmup_iterations: 0,
      timeout_ms: 250,
      raw_timings_us: [100],
      adjusted_timings_us: [100],
      iterations: [],
      completion_rate: 1.0,
      success_count: 1,
      timeout_count: 0,
      parser_error_count: 0,
      execution_error_count: 0,
      cancellation_count: 0,
      out_of_memory_count: 0,
      result_count: answer_record.row_count,
      failure_count: 0,
      penalty_count: 0,
      answer_record: answer_record,
      correctness: nil,
      partial_failure_class: :none,
      failures: [],
      template_metadata: nil
    }
  end

  defp parser_failure_query_run(benchmark_id, suite, execution_variant) do
    %{
      benchmark_id: benchmark_id,
      query_name: benchmark_id,
      suite: suite,
      category: :mixed,
      group: :test,
      shape: :mixed,
      execution_variant: execution_variant,
      source_id: "#{benchmark_id}-source",
      feature_tags: [],
      answer_size_class: :unknown,
      raw_query_text: "INVALID",
      normalized_query_text: "INVALID",
      stress_points: [:parser],
      measurement_iterations: 1,
      warmup_iterations: 0,
      timeout_ms: 250,
      raw_timings_us: [],
      adjusted_timings_us: [100_000],
      iterations: [],
      completion_rate: 0.0,
      success_count: 0,
      timeout_count: 0,
      parser_error_count: 1,
      execution_error_count: 0,
      cancellation_count: 0,
      out_of_memory_count: 0,
      result_count: nil,
      failure_count: 1,
      penalty_count: 1,
      answer_record: nil,
      correctness: nil,
      partial_failure_class: :hard_incompatibility,
      failures: [%{iteration: 1, class: :parse_error, message: "Parse error"}],
      template_metadata: nil
    }
  end

  defp find_query(query_runs, benchmark_id) do
    Enum.find(query_runs, &(&1.benchmark_id == benchmark_id))
  end
end
