defmodule TripleStore.Benchmark.WatDiv.ProvenanceTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.WatDiv.Provenance

  @moduletag :benchmark
  @official_model_sha256 "sha256:8a888eded6552b981857f7f0cd0c92cf04de3cf4801ec91dba9e4dd18a32bf76"

  test "loads an official dataset manifest and verifies the local artifact" do
    root = tmp_dir("manifest")
    dataset_path = Path.join(root, "watdiv.nt")
    manifest_path = Path.join(root, "dataset-manifest.json")
    File.mkdir_p!(root)

    File.write!(dataset_path, "<http://ex/s> <http://ex/p> <http://ex/o> .\n")
    {:ok, checksum} = Artifact.checksum(dataset_path)

    write_manifest(manifest_path, checksum, 1)

    assert {:ok, manifest} = Provenance.load_dataset_manifest(manifest_path, dataset_path)
    assert manifest.watdiv_version == "0.6"
    assert manifest.scale_factor == 1
    assert manifest.triple_count == 1
    assert manifest.dataset_sha256 == checksum
  end

  test "rejects a dataset whose checksum differs from its manifest" do
    root = tmp_dir("checksum")
    dataset_path = Path.join(root, "watdiv.nt")
    manifest_path = Path.join(root, "dataset-manifest.json")
    File.mkdir_p!(root)

    File.write!(dataset_path, "<http://ex/s> <http://ex/p> <http://ex/o> .\n")
    write_manifest(manifest_path, "sha256:" <> String.duplicate("0", 64), 1)

    assert {:error, {:checksum_mismatch, _expected, _actual}} =
             Provenance.load_dataset_manifest(manifest_path, dataset_path)
  end

  test "rejects manifests without official WatDiv provenance" do
    root = tmp_dir("provenance")
    dataset_path = Path.join(root, "watdiv.nt")
    manifest_path = Path.join(root, "dataset-manifest.json")
    File.mkdir_p!(root)

    File.write!(dataset_path, "<http://ex/s> <http://ex/p> <http://ex/o> .\n")
    {:ok, checksum} = Artifact.checksum(dataset_path)

    write_manifest(manifest_path, checksum, 1, generator: "local-elixir")

    assert {:error, {:invalid_manifest, errors}} =
             Provenance.load_dataset_manifest(manifest_path, dataset_path)

    assert "generator must identify the official WatDiv tool" in errors
  end

  test "rejects a manifest naming a modified data model" do
    root = tmp_dir("model")
    dataset_path = Path.join(root, "watdiv.nt")
    manifest_path = Path.join(root, "dataset-manifest.json")
    File.mkdir_p!(root)

    File.write!(dataset_path, "<http://ex/s> <http://ex/p> <http://ex/o> .\n")
    {:ok, checksum} = Artifact.checksum(dataset_path)

    write_manifest(manifest_path, checksum, 1,
      model_sha256: "sha256:" <> String.duplicate("0", 64)
    )

    assert {:error, {:invalid_manifest, errors}} =
             Provenance.load_dataset_manifest(manifest_path, dataset_path)

    assert "model_sha256 must match the model in the official WatDiv v0.6 archive" in errors
  end

  defp write_manifest(path, checksum, triple_count, opts \\ []) do
    manifest = %{
      schema_version: 1,
      benchmark: "watdiv",
      watdiv_version: "0.6",
      scale_factor: 1,
      generator: Keyword.get(opts, :generator, "official-watdiv-v0.6"),
      source_archive_url: "https://dsg.uwaterloo.ca/watdiv/watdiv_v06.tar",
      source_archive_md5: "9eac247dfdec044d7fa0141ea3ad361f",
      model_sha256: Keyword.get(opts, :model_sha256, @official_model_sha256),
      dataset_sha256: checksum,
      triple_count: triple_count
    }

    File.write!(path, Jason.encode!(manifest))
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
