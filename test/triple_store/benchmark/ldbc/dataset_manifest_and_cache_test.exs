defmodule TripleStore.Benchmark.LDBC.DatasetManifestAndCacheTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.Artifact
  alias TripleStore.Benchmark.LDBC.{ArtifactCache, DatasetManifest, ExternalCommand}

  test "manifest round trips with separate source, transformation, and store identity" do
    assert {:ok, manifest} = DatasetManifest.new(valid_manifest_attrs())
    assert {:ok, encoded} = DatasetManifest.encode(manifest)
    assert {:ok, decoded} = DatasetManifest.decode(encoded)

    assert decoded == manifest
    assert DatasetManifest.identity(decoded).suite == :snb_bi
    assert decoded.source.generator_pin == String.duplicate("a", 40)
    assert decoded.transformation.statement_count == 3
    assert decoded.store.schema == :quad
  end

  test "newer schemas and incomplete provenance fail explicitly" do
    assert {:error, {:unsupported_schema_version, 2, 1}} =
             valid_manifest_attrs()
             |> Map.put(:schema_version, 2)
             |> DatasetManifest.new()

    attrs = put_in(valid_manifest_attrs(), [:source, :generator_pin], "main")
    assert {:error, errors} = DatasetManifest.new(attrs)
    assert {{:source, :generator_pin}, :invalid} in errors
  end

  test "cache registration, validation, and partial promotion are checksum gated", %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)
    source = Path.join(root, "source.nq")
    File.mkdir_p!(root)
    File.write!(source, "<urn:s> <urn:p> <urn:o> <urn:g> .\n")
    {:ok, checksum} = Artifact.checksum(source)

    assert {:ok, cached} =
             ArtifactCache.register(root, "spb-smoke", "dataset.nq", source, checksum)

    assert {:ok, ^cached} =
             ArtifactCache.validate(root, "spb-smoke", "dataset.nq", checksum)

    assert {:error, :stale_completion_marker} =
             ArtifactCache.validate(
               root,
               "spb-smoke",
               "dataset.nq",
               "sha256:#{String.duplicate("0", 64)}"
             )

    {:ok, partial} = ArtifactCache.partial_path(root, "snb-smoke", "dataset.nq")
    File.mkdir_p!(Path.dirname(partial))
    File.cp!(source, partial)
    assert {:ok, byte_size} = ArtifactCache.resume_offset(root, "snb-smoke", "dataset.nq")
    assert byte_size > 0

    assert {:ok, promoted} =
             ArtifactCache.promote_partial(root, "snb-smoke", "dataset.nq", checksum)

    assert File.regular?(promoted)
    refute File.exists?(partial)
  end

  test "network and generator work require explicit opt in" do
    assert {:error, :explicit_network_action_required} =
             ArtifactCache.acquire(
               "/tmp/not-used",
               "dataset",
               "file.tgz",
               "https://example.invalid/file.tgz",
               "sha256:#{String.duplicate("0", 64)}"
             )

    assert {:error, :explicit_external_action_required} =
             ExternalCommand.run(%{
               executable: "echo",
               args: ["not-run"],
               working_directory: "/tmp/not-used",
               commit: String.duplicate("a", 40)
             })
  end

  defp valid_manifest_attrs do
    checksum = "sha256:#{String.duplicate("1", 64)}"

    %{
      dataset_id: "snb-bi-smoke",
      suite: :snb_bi,
      profile: "snb-bi-smoke",
      scale_factor: "smoke",
      source: %{
        generator_source_id: "ldbc-snb-datagen-spark-v0.5.1",
        generator_pin: String.duplicate("a", 40),
        generator_settings: %{serializer: "csv"},
        seed: 42,
        format: :csv,
        checksum: checksum,
        license: "Apache-2.0"
      },
      transformation: %{
        version: "1",
        mapping_version: "snb-rdf-v1",
        output_checksum: checksum,
        statement_count: 3,
        entity_count: 1,
        relationship_count: 1,
        update_streams: ["updates/part-000.csv"]
      },
      store: %{
        schema: :quad,
        loader_settings: %{batch_size: 1_000},
        path_identity: "store-snb-bi-smoke",
        post_load_stats: %{}
      },
      components: [
        %{role: :initial, path: "dataset.nq", checksum: checksum, count: 3}
      ]
    }
  end

  defp tmp_dir(test) do
    Path.join(System.tmp_dir!(), "ldbc_manifest_#{test}_#{System.unique_integer([:positive])}")
  end
end
