defmodule TripleStore.Benchmark.Wikidata.StoreFixtureTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.Wikidata.{DatasetManifest, Fixture, StoreFixture}

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "wikidata_store_fixture_test_#{System.unique_integer([:positive])}"
      )

    source_path = Path.join(root, "source.nt")
    File.mkdir_p!(root)

    File.write!(
      source_path,
      Enum.map_join(1..6, "\n", fn i ->
        "<http://example.org/s#{i}> <http://example.org/p> \"o#{i}\" ."
      end)
    )

    {:ok, manifest} =
      DatasetManifest.from_source(
        source_path,
        dataset_id: "wikidata-loadable",
        tier: :smoke,
        source_url: "https://example.org/wikidata-loadable.nt",
        dump_version: "2024-10"
      )

    {:ok, registered_manifest} = Fixture.register_dataset(root, manifest, source_path)

    on_exit(fn -> File.rm_rf(root) end)

    {:ok, root: root, manifest: registered_manifest}
  end

  test "load presets are available for truthy and full RDF ingestion" do
    assert {:ok, truthy_only} = StoreFixture.load_preset(:truthy_only)
    assert truthy_only[:format] == :ntriples
    assert truthy_only[:batch_size] == 10_000

    assert {:ok, full_rdf} = StoreFixture.load_preset(:full_rdf)
    assert full_rdf[:batch_size] == 5_000

    assert StoreFixture.load_preset(:unknown) == {:error, :unknown_preset}
  end

  test "setup/3 loads a registered dataset and captures metrics", %{
    root: root,
    manifest: manifest
  } do
    assert {:ok, fixture_state} =
             StoreFixture.setup(root, manifest,
               load_preset: :truthy_only,
               warmup: fn store -> TripleStore.stats(store) end
             )

    assert fixture_state.load_metrics.count == manifest.triple_count
    assert fixture_state.load_metrics.elapsed_us > 0
    assert fixture_state.load_metrics.throughput_tps > 0
    assert fixture_state.load_metrics.warmed?

    assert {:ok, stats} = TripleStore.stats(fixture_state.store)
    assert stats.triple_count == manifest.triple_count

    assert :ok = StoreFixture.teardown(fixture_state, delete_store: true)
  end

  test "reopen/1 opens an existing benchmark store without reimporting data", %{
    root: root,
    manifest: manifest
  } do
    {:ok, fixture_state} = StoreFixture.setup(root, manifest)
    :ok = TripleStore.close(fixture_state.store)

    fixture_state = %{fixture_state | store: nil}

    assert {:ok, reopened_state} = StoreFixture.reopen(fixture_state)
    assert {:ok, stats} = TripleStore.stats(reopened_state.store)
    assert stats.triple_count == manifest.triple_count

    assert :ok = StoreFixture.teardown(reopened_state, delete_store: true)
  end
end
