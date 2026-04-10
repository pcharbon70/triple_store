defmodule TripleStore.Benchmark.Wikidata.OperationsTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.Wikidata.Operations

  describe "run/2" do
    test "parser mode validates the full smoke corpus" do
      assert {:ok, result} = Operations.run(:parser, dataset_tier: :smoke)
      assert result.mode == :parser
      assert result.query_count > 0
      assert result.failures == []
    end

    test "corpus smoke executes representative queries" do
      fixture_root = unique_tmp_dir!("wikidata_ops_corpus_fixture")

      assert {:ok, result} =
               Operations.run(:corpus_smoke,
                 fixture_root: fixture_root,
                 output_root: unique_tmp_dir!("wikidata_ops_corpus_output")
               )

      assert result.mode == :corpus_smoke
      assert result.result_count == 4
      assert Enum.all?(result.results, &(&1.result_count >= 1))
    end

    test "smoke benchmark writes artifacts and enforces threshold failures with query ids" do
      fixture_root = unique_tmp_dir!("wikidata_ops_fixture")
      output_root = unique_tmp_dir!("wikidata_ops_output")

      assert {:ok, result} =
               Operations.run(:smoke,
                 fixture_root: fixture_root,
                 output_root: output_root,
                 report_id: "wikidata-ops-smoke"
               )

      assert result.mode == :smoke
      assert File.exists?(result.artifact_bundle.paths.json)
      assert File.exists?(result.artifact_bundle.paths.csv)
      assert File.exists?(result.artifact_bundle.paths.markdown)

      assert {:error, {:threshold_failed, failures}} =
               Operations.run(:smoke,
                 fixture_root: unique_tmp_dir!("wikidata_ops_fixture_fail"),
                 output_root: unique_tmp_dir!("wikidata_ops_output_fail"),
                 report_id: "wikidata-ops-smoke-fail",
                 max_adjusted_p95_us: 1
               )

      assert Enum.any?(failures, &String.contains?(&1, "wgpb"))

      assert Enum.any?(failures, &String.contains?(&1, "benchmark_id"))
             |> Kernel.not()
    end

    test "smoke benchmark can write portable accepted baseline artifacts" do
      fixture_root = unique_tmp_dir!("wikidata_ops_fixture_portable")
      output_root = unique_tmp_dir!("wikidata_ops_output_portable")
      baseline_path = Path.join(output_root, "reference_answers.json")
      accepted_report_dir = Path.join(output_root, "accepted_report")

      assert {:ok, _result} =
               Operations.run(:smoke,
                 fixture_root: fixture_root,
                 output_root: output_root,
                 report_id: "wikidata-ops-smoke-portable",
                 write_answer_baseline: baseline_path,
                 write_accepted_report: accepted_report_dir
               )

      baseline = Jason.decode!(File.read!(baseline_path))
      report = Jason.decode!(File.read!(Path.join(accepted_report_dir, "summary.json")))

      assert baseline["dataset_manifest"]["local_data_path"] == nil
      assert baseline["dataset_manifest"]["manifest_path"] == nil
      assert report["dataset_manifest"]["local_data_path"] == nil
      assert report["dataset_manifest"]["manifest_path"] == nil
      assert report["artifacts"]["output_dir"] == nil
      assert report["artifacts"]["paths"]["json"] == "summary.json"
    end
  end

  defp unique_tmp_dir!(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.rm_rf!(path)
    File.mkdir_p!(path)
    path
  end
end
