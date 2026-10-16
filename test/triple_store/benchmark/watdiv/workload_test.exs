defmodule TripleStore.Benchmark.WatDiv.WorkloadTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.WatDiv.Workload

  @moduletag :benchmark

  test "splits the official generator's concatenated SELECT output" do
    generated = """
    SELECT ?s WHERE {
      ?s <http://example.org/p> <http://example.org/o1> .
    }
    SELECT ?s WHERE {
      ?s <http://example.org/p> <http://example.org/o2> .
    }
    """

    assert {:ok, [first, second]} = Workload.split_queries(generated)
    assert first =~ "o1"
    assert second =~ "o2"
  end

  test "materializes stable one-query files and reloads the workload" do
    root = tmp_dir("workload")
    raw_path = Path.join(root, "raw.txt")
    output_dir = Path.join(root, "basic")

    File.mkdir_p!(root)

    File.write!(raw_path, """
    SELECT ?s WHERE { ?s <http://example.org/p> <http://example.org/o1> . }
    SELECT ?s WHERE { ?s <http://example.org/p> <http://example.org/o2> . }
    """)

    assert {:ok, paths} = Workload.split_file(raw_path, output_dir, "L1")
    assert Enum.map(paths, &Path.basename/1) == ["L1-0001.rq", "L1-0002.rq"]

    assert {:ok, workload} = Workload.load_dir(root)
    assert workload.query_count == 2
    assert Enum.map(workload.queries, & &1.id) == ["basic/L1-0001", "basic/L1-0002"]
    assert String.starts_with?(workload.fingerprint, "sha256:")
  end

  test "rejects unresolved WatDiv placeholders" do
    root = tmp_dir("placeholder")
    File.mkdir_p!(root)
    File.write!(Path.join(root, "L1.rq"), "SELECT ?s WHERE { ?s ?p %v1% . }")

    assert {:error, {:unresolved_placeholder, "L1"}} = Workload.load_dir(root)
  end

  test "rejects files containing more than one query" do
    root = tmp_dir("multiple")
    File.mkdir_p!(root)

    File.write!(Path.join(root, "L1.rq"), """
    SELECT ?s WHERE { ?s ?p ?o . }
    SELECT ?o WHERE { ?s ?p ?o . }
    """)

    assert {:error, {:query_file_count, "L1", 2}} = Workload.load_dir(root)
  end

  defp tmp_dir(label) do
    path =
      Path.join(
        System.tmp_dir!(),
        "triple_store_watdiv_#{label}_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
