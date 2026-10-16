defmodule TripleStore.Benchmark.WatDiv.RunnerIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.WatDiv.{Runner, Workload}

  @moduletag :benchmark

  test "verifies independent answers before producing measured results" do
    fixture = build_fixture(["http://example.org/s1", "http://example.org/s2"])

    assert {:ok, result} = Runner.run(fixture.opts)
    assert result.status == :passed
    assert result.load.triple_count == 2
    assert result.workload.query_count == 1

    assert [query] = result.queries
    assert query.id == "basic/L1-0001"
    assert query.result_count == 2
    assert query.correctness == :match
    assert length(query.timings_us) == 1

    assert File.exists?(Path.join(fixture.output_dir, "results.json"))
    assert File.exists?(Path.join(fixture.output_dir, "results.csv"))
  end

  test "refuses to measure when TripleStore differs from the reference engine" do
    fixture = build_fixture(["http://example.org/s1"])

    assert {:error, {:correctness_mismatch, mismatch}} = Runner.run(fixture.opts)
    assert mismatch.query_id == "basic/L1-0001"
    assert mismatch.expected_row_count == 1
    assert mismatch.actual_row_count == 2
    refute File.exists?(Path.join(fixture.output_dir, "results.json"))
  end

  test "refuses to replace an existing result directory" do
    fixture = build_fixture(["http://example.org/s1", "http://example.org/s2"])
    File.mkdir_p!(fixture.output_dir)
    marker = Path.join(fixture.output_dir, "keep.txt")
    File.write!(marker, "previous run")

    assert {:error, {:output_path_exists, output_dir}} = Runner.run(fixture.opts)
    assert output_dir == Path.expand(fixture.output_dir)
    assert File.read!(marker) == "previous run"
  end

  defp build_fixture(reference_iris) do
    root = tmp_dir()
    dataset_path = Path.join(root, "watdiv.nt")
    dataset_manifest_path = Path.join(root, "dataset-manifest.json")
    workload_dir = Path.join(root, "workload")
    reference_dir = Path.join(root, "reference")
    output_dir = Path.join(root, "output")

    File.mkdir_p!(Path.join(workload_dir, "basic"))
    File.mkdir_p!(Path.join(reference_dir, "basic"))

    File.write!(dataset_path, """
    <http://example.org/s1> <http://example.org/p> <http://example.org/o> .
    <http://example.org/s2> <http://example.org/p> <http://example.org/o> .
    """)

    {:ok, dataset_sha256} = Artifact.checksum(dataset_path)

    File.write!(
      dataset_manifest_path,
      Jason.encode!(%{
        schema_version: 1,
        benchmark: "watdiv",
        watdiv_version: "0.6",
        scale_factor: 1,
        generator: "official-watdiv-v0.6",
        source_archive_url: "https://dsg.uwaterloo.ca/watdiv/watdiv_v06.tar",
        source_archive_md5: "9eac247dfdec044d7fa0141ea3ad361f",
        model_sha256: "sha256:8a888eded6552b981857f7f0cd0c92cf04de3cf4801ec91dba9e4dd18a32bf76",
        dataset_sha256: dataset_sha256,
        triple_count: 2
      })
    )

    File.write!(
      Path.join(workload_dir, "basic/L1-0001.rq"),
      "SELECT ?s WHERE { ?s <http://example.org/p> <http://example.org/o> . }\n"
    )

    {:ok, workload} = Workload.load_dir(workload_dir)

    File.write!(
      Path.join(reference_dir, "manifest.json"),
      Jason.encode!(%{
        schema_version: 1,
        engine: "Apache Jena ARQ",
        engine_version: "5.3.0",
        dataset_sha256: dataset_sha256,
        workload_sha256: workload.fingerprint
      })
    )

    bindings =
      Enum.map(reference_iris, fn iri ->
        %{s: %{type: "uri", value: iri}}
      end)

    File.write!(
      Path.join(reference_dir, "basic/L1-0001.srj"),
      Jason.encode!(%{head: %{vars: ["s"]}, results: %{bindings: bindings}})
    )

    %{
      output_dir: output_dir,
      opts: [
        dataset_path: dataset_path,
        dataset_manifest_path: dataset_manifest_path,
        workload_dir: workload_dir,
        reference_dir: reference_dir,
        output_dir: output_dir,
        warmup_iterations: 0,
        measurement_iterations: 1,
        timeout_ms: 5_000,
        batch_size: 100
      ]
    }
  end

  defp tmp_dir do
    path =
      Path.join(
        System.tmp_dir!(),
        "triple_store_watdiv_runner_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
