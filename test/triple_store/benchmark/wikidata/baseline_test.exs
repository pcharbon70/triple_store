defmodule TripleStore.Benchmark.Wikidata.BaselineTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.{AnswerNormalizer, Baseline}

  test "JSON baselines round-trip with portable dataset manifest fields" do
    {:ok, baseline} =
      Baseline.from_run_result(
        %{
          query_runs: [
            %{
              benchmark_id: "wgpb-1",
              query_name: "WGPB 1",
              suite: :wgpb,
              execution_variant: :raw,
              answer_record: answer_record()
            }
          ],
          dataset_manifest: %{
            dataset_id: "wikidata-smoke",
            tier: :smoke,
            local_data_path: "/tmp/wikidata/data.nt",
            manifest_path: "/tmp/wikidata/manifest.term"
          },
          runtime_config: %{dataset_tier: :smoke}
        },
        baseline_id: "wikidata-smoke-baseline",
        generated_at: ~U[2026-04-10 18:20:00Z]
      )

    path = unique_tmp_path!("wikidata_baseline_roundtrip.json")

    assert :ok = Baseline.write_json(path, baseline)
    assert {:ok, loaded} = Baseline.load_json(path)

    assert loaded.dataset_manifest.local_data_path == nil
    assert loaded.dataset_manifest.manifest_path == nil
    assert loaded.entries |> hd() |> Map.fetch!(:benchmark_id) == "wgpb-1"
  end

  defp answer_record do
    {:ok, record} =
      AnswerNormalizer.normalize(
        [%{"person" => {:named_node, "http://example.org/Q42"}}],
        execution_variant: :raw,
        ordering: :unordered,
        blank_node_policy: :anonymous
      )

    record
  end

  defp unique_tmp_path!(name) do
    dir = Path.join(System.tmp_dir!(), "wikidata_baseline_test")
    File.mkdir_p!(dir)
    Path.join(dir, "#{System.unique_integer([:positive])}_#{name}")
  end
end
