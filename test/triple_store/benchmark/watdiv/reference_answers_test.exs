defmodule TripleStore.Benchmark.WatDiv.ReferenceAnswersTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.WatDiv.{ReferenceAnswers, Workload}
  alias TripleStore.Benchmark.Wikidata.AnswerNormalizer

  @moduletag :benchmark

  test "loads Jena SPARQL JSON answers bound to the dataset and workload" do
    root = tmp_dir("answers")
    workload_dir = Path.join(root, "workload")
    reference_dir = Path.join(root, "reference")
    File.mkdir_p!(workload_dir)
    File.mkdir_p!(reference_dir)

    File.write!(Path.join(workload_dir, "L1-0001.rq"), query())
    {:ok, workload} = Workload.load_dir(workload_dir)

    dataset_manifest = %{dataset_sha256: "sha256:" <> String.duplicate("a", 64)}
    write_reference_manifest(reference_dir, dataset_manifest.dataset_sha256, workload.fingerprint)
    write_sparql_json(Path.join(reference_dir, "L1-0001.srj"), "http://example.org/s")

    assert {:ok, reference} =
             ReferenceAnswers.load(reference_dir, workload, dataset_manifest)

    assert reference.engine == "Apache Jena ARQ"
    assert reference.entries["L1-0001"].row_count == 1
    assert reference.entries["L1-0001"].fingerprint =~ ~r/^[0-9a-f]{64}$/
  end

  test "rejects answers generated for another workload" do
    root = tmp_dir("mismatch")
    workload_dir = Path.join(root, "workload")
    reference_dir = Path.join(root, "reference")
    File.mkdir_p!(workload_dir)
    File.mkdir_p!(reference_dir)

    File.write!(Path.join(workload_dir, "L1-0001.rq"), query())
    {:ok, workload} = Workload.load_dir(workload_dir)
    dataset_sha256 = "sha256:" <> String.duplicate("a", 64)

    write_reference_manifest(
      reference_dir,
      dataset_sha256,
      "sha256:" <> String.duplicate("b", 64)
    )

    assert {:error, {:workload_fingerprint_mismatch, _expected, _actual}} =
             ReferenceAnswers.load(reference_dir, workload, %{
               dataset_sha256: dataset_sha256
             })
  end

  test "normalizes Jena literals exactly like TripleStore query results" do
    datatype = "http://www.w3.org/2001/XMLSchema#integer"

    jena_json =
      Jason.encode!(%{
        head: %{vars: ["value"]},
        results: %{
          bindings: [
            %{value: %{type: "literal", value: "42", datatype: datatype}}
          ]
        }
      })

    assert {:ok, reference} = ReferenceAnswers.parse_sparql_json(jena_json)

    assert {:ok, triple_store} =
             AnswerNormalizer.normalize(
               [%{"value" => {:literal, :typed, "42", datatype}}],
               ordering: :unordered
             )

    assert reference.fingerprint == triple_store.fingerprint
  end

  defp query do
    "SELECT ?s WHERE { ?s <http://example.org/p> <http://example.org/o> . }"
  end

  defp write_reference_manifest(dir, dataset_sha256, workload_sha256) do
    File.write!(
      Path.join(dir, "manifest.json"),
      Jason.encode!(%{
        schema_version: 1,
        engine: "Apache Jena ARQ",
        engine_version: "5.3.0",
        dataset_sha256: dataset_sha256,
        workload_sha256: workload_sha256
      })
    )
  end

  defp write_sparql_json(path, iri) do
    File.write!(
      path,
      Jason.encode!(%{
        head: %{vars: ["s"]},
        results: %{bindings: [%{s: %{type: "uri", value: iri}}]}
      })
    )
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
