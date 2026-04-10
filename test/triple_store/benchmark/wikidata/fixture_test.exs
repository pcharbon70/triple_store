defmodule TripleStore.Benchmark.Wikidata.FixtureTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.Wikidata.{DatasetManifest, Fixture}

  setup do
    root =
      Path.join(System.tmp_dir!(), "wikidata_fixture_test_#{System.unique_integer([:positive])}")

    source_path = Path.join(root, "source.nt")
    File.mkdir_p!(root)

    File.write!(
      source_path,
      Enum.map_join(1..20, "\n", fn i ->
        "<http://example.org/s#{i}> <http://example.org/p> \"o#{i}\" ."
      end)
    )

    {:ok, manifest} =
      DatasetManifest.from_source(
        source_path,
        dataset_id: "wikidata-source",
        tier: :large,
        source_url: "https://example.org/wikidata-source.nt",
        dump_version: "2024-10"
      )

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, root: root, source_path: source_path, manifest: manifest}
  end

  describe "register_dataset/3" do
    test "copies the data file and persists the manifest", %{
      root: root,
      source_path: source_path,
      manifest: manifest
    } do
      assert {:ok, registered_manifest} = Fixture.register_dataset(root, manifest, source_path)
      assert File.exists?(registered_manifest.local_data_path)
      assert File.exists?(registered_manifest.manifest_path)
      assert :ok = Fixture.validate_registered_dataset(root, registered_manifest.dataset_id)
    end
  end

  describe "create_subset/4" do
    test "creates deterministic subsets for a registered dataset", %{
      root: root,
      source_path: source_path,
      manifest: manifest
    } do
      {:ok, registered_manifest} = Fixture.register_dataset(root, manifest, source_path)

      assert {:ok, subset_a} =
               Fixture.create_subset(root, registered_manifest, :smoke,
                 seed: 7,
                 target_statements: 5
               )

      assert {:ok, subset_b} =
               Fixture.create_subset(root, registered_manifest, :smoke,
                 dataset_id: "wikidata-source-smoke-seed7-copy",
                 seed: 7,
                 target_statements: 5
               )

      assert File.read!(subset_a.local_data_path) == File.read!(subset_b.local_data_path)
      assert subset_a.generated_from.dataset_id == "wikidata-source"
      assert subset_a.subset_seed == 7
      assert subset_a.triple_count == 5
    end
  end

  describe "selection_plan/3" do
    test "returns a deterministic stride plan" do
      plan = Fixture.selection_plan(100, 10, 42)

      assert plan.total_statements == 100
      assert plan.target_statements == 10
      assert plan.stride >= 1
      assert plan.offset >= 0
    end
  end
end
