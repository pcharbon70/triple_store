defmodule Mix.Tasks.Benchmark.WikidataTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  test "smoke mode runs from a clean checkout path and prints artifact location" do
    Mix.Task.reenable("benchmark.wikidata")

    fixture_root = unique_tmp_dir!("wikidata_mix_fixture")
    output_root = unique_tmp_dir!("wikidata_mix_output")

    output =
      capture_io(fn ->
        Mix.Tasks.Benchmark.Wikidata.run([
          "smoke",
          "--fixture-root",
          fixture_root,
          "--output-root",
          output_root,
          "--report-id",
          "wikidata-mix-smoke"
        ])
      end)

    assert output =~ "Benchmark run passed"
    assert output =~ "wikidata-mix-smoke"
  end

  defp unique_tmp_dir!(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.rm_rf!(path)
    File.mkdir_p!(path)
    path
  end
end
