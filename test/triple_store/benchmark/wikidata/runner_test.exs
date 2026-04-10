defmodule TripleStore.Benchmark.Wikidata.RunnerTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.{DatasetManifest, PublicWorkloads, Query, Runner}

  describe "defaults_for_tier/1" do
    test "returns per-tier defaults for supported tiers" do
      assert {:ok, smoke} = Runner.defaults_for_tier(:smoke)
      assert smoke.warmup_iterations == 1
      assert smoke.measurement_iterations == 2
      assert smoke.timeout_ms == 250

      assert {:ok, full_dump} = Runner.defaults_for_tier(:full_dump)
      assert full_dump.measurement_iterations == 10
      assert full_dump.timeout_ms == 10_000
    end
  end

  describe "run/3" do
    test "runs a single query with warmup and provenance capture" do
      {:ok, corpus} = PublicWorkloads.corpus(:wgpb, tier: :smoke)
      query = hd(corpus.queries)

      assert {:ok, result} =
               Runner.run(fake_store(), query,
                 dataset_manifest: dataset_manifest(),
                 warmup_iterations: 2,
                 measurement_iterations: 3,
                 timeout_ms: 500,
                 penalty_us: 1_000_000,
                 long_running_threshold_us: 750_000,
                 executor: &successful_executor/3
               )

      assert result.run_kind == :query
      assert result.dataset_manifest.dataset_id == "wikidata-runner-test"
      assert result.runtime_config.warmup_iterations == 2
      assert result.runtime_config.measurement_iterations == 3
      assert result.runtime_config.timeout_ms == 500
      assert is_binary(result.git_sha)

      [query_run] = result.query_runs
      assert query_run.benchmark_id == "wgpb-single-bgp-001"
      assert query_run.success_count == 3
      assert query_run.failure_count == 0
      assert query_run.completion_rate == 1.0
      assert length(query_run.iterations) == 3
      assert length(query_run.raw_timings_us) == 3
      assert Enum.all?(query_run.iterations, &(&1.status == :ok))
      assert is_binary(query_run.answer_record.fingerprint)
      assert Enum.all?(query_run.iterations, &is_binary(&1.answer_fingerprint))
    end

    test "runs suite and matrix targets through the same interface" do
      {:ok, wgpb} = PublicWorkloads.corpus(:wgpb, tier: :smoke)

      assert {:ok, suite_result} =
               Runner.run(fake_store(), wgpb,
                 dataset_manifest: dataset_manifest(),
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 execution_variants: [:raw, :count_only],
                 executor: &successful_executor/3
               )

      assert suite_result.run_kind == :suite
      assert suite_result.target.suite == :wgpb
      assert length(suite_result.query_runs) == 8

      assert {:ok, matrix_result} =
               Runner.run(fake_store(), [:wgpb, :wdqs],
                 dataset_manifest: dataset_manifest(),
                 dataset_tier: :smoke,
                 warmup_iterations: 0,
                 measurement_iterations: 1,
                 execution_variants: [:raw],
                 executor: &successful_executor/3
               )

      assert matrix_result.run_kind == :matrix
      assert Enum.sort(matrix_result.target.suites) == [:wdqs, :wgpb]

      assert Enum.sort(Enum.uniq(Enum.map(matrix_result.query_runs, & &1.suite))) == [
               :wdqs,
               :wgpb
             ]
    end

    test "captures structured parse, timeout, cancellation, out-of-memory, and execution failures" do
      assert_failure_class(
        build_query("runner-parse-error", "INVALID SPARQL"),
        fn _store, _sparql, _opts -> {:error, {:parse_error, "bad syntax"}} end,
        :parse_error,
        :hard_incompatibility
      )

      assert_failure_class(
        build_query("runner-timeout", "SELECT ?s WHERE { ?s ?p ?o }"),
        fn _store, _sparql, _opts -> {:error, :timeout} end,
        :timeout,
        :hard_incompatibility
      )

      assert_failure_class(
        build_query("runner-cancelled", "SELECT ?s WHERE { ?s ?p ?o }"),
        fn _store, _sparql, _opts -> {:error, :cancelled} end,
        :cancelled,
        :resource_exhaustion
      )

      assert_failure_class(
        build_query("runner-oom", "SELECT ?s WHERE { ?s ?p ?o }"),
        fn _store, _sparql, _opts ->
          {:error, TripleStore.Error.new(:system_resource_exhausted, "out of memory")}
        end,
        :out_of_memory,
        :resource_exhaustion
      )

      assert_failure_class(
        build_query("runner-exec", "SELECT ?s WHERE { ?s ?p ?o }"),
        fn _store, _sparql, _opts -> {:error, {:unsupported_pattern, :values}} end,
        :execution_error,
        :hard_incompatibility
      )
    end
  end

  defp assert_failure_class(query, executor, expected_error_class, expected_partial_failure_class) do
    assert {:ok, result} =
             Runner.run(fake_store(), query,
               dataset_manifest: dataset_manifest(),
               warmup_iterations: 0,
               measurement_iterations: 1,
               timeout_ms: 50,
               penalty_us: 100_000,
               long_running_threshold_us: 50_000,
               executor: executor
             )

    [query_run] = result.query_runs
    [iteration] = query_run.iterations

    assert iteration.status == :error
    assert iteration.error_class == expected_error_class
    assert iteration.penalty_reason == :failure
    assert query_run.failure_count == 1
    assert query_run.partial_failure_class == expected_partial_failure_class
    assert query_run.raw_query_text == query.raw_sparql
    assert query_run.normalized_query_text == query.sparql
    assert length(query_run.adjusted_timings_us) == 1
    assert hd(query_run.adjusted_timings_us) >= 100_000
  end

  defp fake_store do
    %{
      db: self(),
      dict_manager: self(),
      transaction: nil,
      path: "/tmp/wikidata_runner_test",
      schema: :triple
    }
  end

  defp dataset_manifest do
    {:ok, manifest} =
      DatasetManifest.new(
        dataset_id: "wikidata-runner-test",
        tier: :smoke,
        source_url: "https://example.org/wikidata-runner-test.nt",
        dump_version: "2024-10",
        checksum: "sha256:runner",
        triple_count: 10,
        format: :ntriples,
        normalization_flags: [:truthy_only]
      )

    manifest
  end

  defp build_query(benchmark_id, sparql) do
    {:ok, corpus} = PublicWorkloads.corpus(:wgpb, tier: :smoke)
    template = hd(corpus.queries)

    {:ok, query} =
      Query.new(%{
        manifest:
          template.manifest
          |> TripleStore.Benchmark.Wikidata.Manifest.to_map()
          |> Map.put(:benchmark_id, benchmark_id),
        name: template.name,
        description: template.description,
        raw_sparql: sparql,
        sparql: sparql,
        group: template.group,
        shape: template.shape,
        feature_tags: template.feature_tags,
        stress_points: template.stress_points
      })

    query
  end

  defp successful_executor(_store, sparql, _opts) do
    if String.contains?(sparql, "COUNT(") do
      {:ok, [%{"count" => {:literal, :typed, "1", "http://www.w3.org/2001/XMLSchema#integer"}}]}
    else
      {:ok, [%{"person" => {:named_node, "http://www.wikidata.org/entity/Q42"}}]}
    end
  end
end
