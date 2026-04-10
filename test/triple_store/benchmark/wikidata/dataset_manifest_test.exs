defmodule TripleStore.Benchmark.Wikidata.DatasetManifestTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Wikidata.DatasetManifest

  describe "from_source/2" do
    test "builds a manifest from a local RDF source" do
      source_path =
        write_fixture("""
        <http://example.org/s1> <http://example.org/p> "o1" .
        <http://example.org/s2> <http://example.org/p> "o2" .
        """)

      assert {:ok, manifest} =
               DatasetManifest.from_source(
                 source_path,
                 dataset_id: "wikidata-smoke",
                 tier: :smoke,
                 source_url: "https://example.org/wikidata-smoke.nt",
                 dump_version: "2024-10"
               )

      assert manifest.dataset_id == "wikidata-smoke"
      assert manifest.tier == :smoke
      assert manifest.triple_count == 2
      assert manifest.format == :ntriples
      assert String.starts_with?(manifest.checksum, "sha256:")
    end
  end

  describe "infer_format/1" do
    test "infers supported line-oriented RDF formats" do
      assert DatasetManifest.infer_format("fixture.nt") == {:ok, :ntriples}
      assert DatasetManifest.infer_format("fixture.nq") == {:ok, :nquads}
      assert DatasetManifest.infer_format("fixture.ttl") == {:ok, :turtle}
    end

    test "returns an error for unknown formats" do
      assert DatasetManifest.infer_format("fixture.data") == {:error, :unknown_format}
    end
  end

  describe "statement_count/1" do
    test "ignores blank lines and comments" do
      source_path =
        write_fixture("""
        # comment

        <http://example.org/s1> <http://example.org/p> "o1" .
        <http://example.org/s2> <http://example.org/p> "o2" .
        """)

      assert DatasetManifest.statement_count(source_path) == {:ok, 2}
    end
  end

  describe "new/1" do
    test "validates the dataset manifest fields" do
      assert {:error, errors} =
               DatasetManifest.new(
                 dataset_id: "",
                 tier: :tiny,
                 source_url: "",
                 dump_version: "",
                 checksum: "",
                 triple_count: -1,
                 format: "ntriples",
                 normalization_flags: [:truthy_only]
               )

      assert {:dataset_id, _} = Enum.find(errors, fn {field, _} -> field == :dataset_id end)
      assert {:tier, _} = Enum.find(errors, fn {field, _} -> field == :tier end)
      assert {:triple_count, _} = Enum.find(errors, fn {field, _} -> field == :triple_count end)
      assert {:format, _} = Enum.find(errors, fn {field, _} -> field == :format end)
    end
  end

  defp write_fixture(content) do
    path =
      Path.join(
        System.tmp_dir!(),
        "wikidata_dataset_manifest_#{System.unique_integer([:positive])}.nt"
      )

    File.write!(path, content)
    path
  end
end
