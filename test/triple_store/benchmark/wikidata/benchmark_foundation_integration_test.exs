defmodule TripleStore.Benchmark.Wikidata.BenchmarkFoundationIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.Wikidata.{DatasetManifest, Fixture, StoreFixture}

  @moduletag :integration

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "wikidata_benchmark_foundation_#{System.unique_integer([:positive])}"
      )

    source_path = Path.join(root, "source.nt")

    File.mkdir_p!(root)

    File.write!(
      source_path,
      Enum.map_join(1..30, "\n", fn i ->
        "<http://example.org/s#{i}> <http://example.org/p#{rem(i, 3)}> \"o#{i}\" ."
      end)
    )

    {:ok, manifest} =
      DatasetManifest.from_source(
        source_path,
        dataset_id: "wikidata-foundation-source",
        tier: :large,
        source_url: "https://example.org/wikidata-foundation-source.nt",
        dump_version: "2024-10",
        source_date: ~D[2024-10-01],
        normalization_flags: [:truthy_only]
      )

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, root: root, source_path: source_path, manifest: manifest}
  end

  describe "1.3.1 dataset pipeline integration" do
    test "fixture manifests round-trip and subset generation is deterministic", %{
      root: root,
      source_path: source_path,
      manifest: manifest
    } do
      assert {:ok, registered_manifest} = Fixture.register_dataset(root, manifest, source_path)
      assert :ok = Fixture.validate_registered_dataset(root, registered_manifest.dataset_id)

      assert {:ok, loaded_manifest} = Fixture.load_manifest(root, registered_manifest.dataset_id)
      assert loaded_manifest.dataset_id == registered_manifest.dataset_id
      assert loaded_manifest.checksum == registered_manifest.checksum
      assert loaded_manifest.triple_count == registered_manifest.triple_count

      assert {:ok, subset_a} =
               Fixture.create_subset(root, registered_manifest, :smoke,
                 seed: 11,
                 target_statements: 7
               )

      assert {:ok, subset_b} =
               Fixture.create_subset(root, registered_manifest, :smoke,
                 dataset_id: "wikidata-foundation-source-smoke-seed11-copy",
                 seed: 11,
                 target_statements: 7
               )

      assert File.read!(subset_a.local_data_path) == File.read!(subset_b.local_data_path)
      assert subset_a.generated_from.dataset_id == registered_manifest.dataset_id
      assert subset_a.generated_from.checksum == registered_manifest.checksum
      assert subset_a.subset_seed == 11
    end

    test "invalid or partial manifest metadata fails explicitly", %{root: root} do
      broken_dir = Path.join([root, "datasets", "broken-fixture"])
      File.mkdir_p!(broken_dir)

      File.write!(
        Path.join(broken_dir, "manifest.term"),
        :erlang.term_to_binary(%{dataset_id: "broken-fixture"})
      )

      assert {:error, errors} = Fixture.load_manifest(root, "broken-fixture")
      assert Enum.any?(errors, fn {field, _} -> field == :tier end)
      assert Enum.any?(errors, fn {field, _} -> field == :source_url end)
      assert Enum.any?(errors, fn {field, _} -> field == :checksum end)
    end
  end

  describe "1.3.2 load pipeline integration" do
    test "prepared fixtures load, reopen, and clean up repeatedly", %{
      root: root,
      source_path: source_path,
      manifest: manifest
    } do
      {:ok, registered_manifest} = Fixture.register_dataset(root, manifest, source_path)

      {:ok, smoke_manifest} =
        Fixture.create_subset(root, registered_manifest, :smoke,
          seed: 21,
          target_statements: 8,
          dataset_id: "wikidata-foundation-smoke"
        )

      {:ok, medium_manifest} =
        Fixture.create_subset(root, registered_manifest, :medium,
          seed: 22,
          target_statements: 12,
          dataset_id: "wikidata-foundation-medium"
        )

      {:ok, full_dump_manifest} =
        DatasetManifest.from_source(
          source_path,
          dataset_id: "wikidata-foundation-full-dump",
          tier: :full_dump,
          source_url: "https://example.org/wikidata-foundation-source.nt",
          dump_version: "2024-10",
          source_date: ~D[2024-10-01],
          normalization_flags: [:truthy_only]
        )

      {:ok, full_dump_manifest} = Fixture.register_dataset(root, full_dump_manifest, source_path)

      Enum.each(
        [registered_manifest, smoke_manifest, medium_manifest, full_dump_manifest],
        fn dataset_manifest ->
          assert {:ok, fixture_state} =
                   StoreFixture.setup(root, dataset_manifest,
                     store_id: "store-#{dataset_manifest.dataset_id}",
                     load_preset: :truthy_only,
                     warmup: fn store -> TripleStore.stats(store) end
                   )

          assert {:ok, stats} = TripleStore.stats(fixture_state.store)
          assert stats.triple_count == dataset_manifest.triple_count

          assert :ok = StoreFixture.teardown(fixture_state, delete_store: true)
          refute File.exists?(fixture_state.store_path)
        end
      )

      Enum.each(1..2, fn _ ->
        assert {:ok, fixture_state} =
                 StoreFixture.setup(root, smoke_manifest,
                   store_id: "wikidata-foundation-store",
                   load_preset: :truthy_only,
                   warmup: fn store -> TripleStore.stats(store) end
                 )

        assert {:ok, stats} = TripleStore.stats(fixture_state.store)
        assert stats.triple_count == smoke_manifest.triple_count

        assert :ok = TripleStore.close(fixture_state.store)
        closed_state = %{fixture_state | store: nil}

        assert {:ok, reopened_state} = StoreFixture.reopen(closed_state)
        assert {:ok, reopened_stats} = TripleStore.stats(reopened_state.store)
        assert reopened_stats.triple_count == smoke_manifest.triple_count

        assert :ok = StoreFixture.teardown(reopened_state, delete_store: true)
        refute File.exists?(reopened_state.store_path)
      end)
    end
  end
end
