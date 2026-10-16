defmodule Mix.Tasks.Benchmark.WatdivTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.Benchmark.Watdiv, as: BenchmarkWatdivTask

  test "split command writes one file per official generator query" do
    root = unique_tmp_dir!("watdiv_mix_split")
    input = Path.join(root, "raw.txt")
    output_dir = Path.join(root, "workload")

    File.write!(input, """
    SELECT ?s WHERE { ?s <http://example.org/p1> ?o . }
    SELECT ?s WHERE { ?s <http://example.org/p2> ?o . }
    """)

    Mix.Task.reenable("benchmark.watdiv")

    output =
      capture_io(fn ->
        BenchmarkWatdivTask.run([
          "split",
          "--input",
          input,
          "--output-dir",
          output_dir,
          "--template",
          "L1"
        ])
      end)

    assert output =~ "WatDiv query instances written: 2"
    assert File.read!(Path.join(output_dir, "L1-0001.rq")) =~ "p1"
    assert File.read!(Path.join(output_dir, "L1-0002.rq")) =~ "p2"
  end

  test "run command requires provenance and correctness inputs" do
    Mix.Task.reenable("benchmark.watdiv")

    assert_raise Mix.Error, ~r/missing required option --dataset/, fn ->
      BenchmarkWatdivTask.run(["run"])
    end
  end

  defp unique_tmp_dir!(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.rm_rf!(path)
    File.mkdir_p!(path)
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
