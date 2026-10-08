defmodule TripleStore.Benchmark.LDBC.SNBBIUpdateBatchTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Benchmark.LDBC.{StoreFixture, SNB.Converter}
  alias TripleStore.Benchmark.LDBC.SNB.{BI.Checkpoint, BI.UpdateBatch, Mapping, UpdateStream}
  alias TripleStore.QuadOperations

  test "mixed quad mutations commit atomically in caller order", %{test: test} do
    path = tmp_path(test)
    on_exit(fn -> File.rm_rf!(path) end)
    assert {:ok, store} = TripleStore.open(path, schema: :quad)

    graph = Mapping.graph_iri(:snb_bi, :updates)

    assert {:ok, alice} =
             Mapping.entity_quads("Person", %{"id" => "1", "firstName" => "Alice"}, graph)

    assert {:ok, bob} =
             Mapping.entity_quads("Person", %{"id" => "2", "firstName" => "Bob"}, graph)

    update_path = path <> ".updates"

    assert {:ok, 3} =
             UpdateStream.write(update_path, [
               %{sequence: 1, operation: :insert, quads: alice},
               %{sequence: 2, operation: :insert, quads: bob},
               %{sequence: 3, operation: :delete, quads: alice}
             ])

    assert {:ok, receipt} = UpdateBatch.apply(store, update_path)
    assert receipt.records == 3
    assert receipt.last_sequence == 3

    assert {:ok, encoded_alice} = TripleStore.Adapter.from_rdf_quads(store.dict_manager, alice)
    assert {:ok, encoded_bob} = TripleStore.Adapter.from_rdf_quads(store.dict_manager, bob)
    refute Enum.any?(encoded_alice, &QuadOperations.quad_exists?(store.db, &1))
    assert Enum.all?(encoded_bob, &QuadOperations.quad_exists?(store.db, &1))

    assert :ok = TripleStore.close(store)
  end

  test "failed storage batch does not run post-commit work", %{test: test} do
    path = tmp_path(test)
    on_exit(fn -> File.rm_rf!(path) end)
    assert {:ok, store} = TripleStore.open(path, schema: :quad)
    graph = Mapping.graph_iri(:snb_bi, :updates)
    assert {:ok, quads} = Mapping.entity_quads("Person", %{"id" => "3"}, graph)
    update_path = path <> ".updates"

    assert {:ok, 1} =
             UpdateStream.write(update_path, [%{sequence: 1, operation: :insert, quads: quads}])

    assert {:error, {:atomic_update_failed, :injected}} =
             UpdateBatch.apply(store, update_path,
               writer: fn _db, _mutations, _opts -> {:error, :injected} end,
               after_commit: fn _store -> flunk("post-commit callback ran") end
             )

    assert :ok = TripleStore.close(store)
  end

  test "named checkpoints restore exact state and update position", %{test: test} do
    root = tmp_path(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, snb_fixture(), Path.join(root, "converted"))

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)
    initial_count = count(fixture)

    assert {:ok, fixture, initial} =
             Checkpoint.create(fixture, :initial_load, Path.join(root, "checkpoints"), %{
               batch_position: 0,
               batch_checksum: nil
             })

    update = Enum.find(manifest.components, &(&1.role == :bi_update_batch))
    assert {:ok, receipt} = UpdateBatch.apply(fixture.store, update.path)
    assert count(fixture) > initial_count

    assert {:ok, restored, state} = Checkpoint.restore(fixture, initial)
    assert count(restored) == initial_count
    assert state == %{batch_position: 0, batch_checksum: nil}
    assert receipt.last_sequence == 1
    assert :ok = StoreFixture.teardown(restored, delete: true)
  end

  defp count(fixture) do
    ErlangAdapter.fold_keys(fixture.store.db, :gspo, <<>>, 0, fn _key, total -> total + 1 end,
      fill_cache: false
    )
  end

  defp snb_fixture do
    Path.join([
      to_string(:code.priv_dir(:triple_store)),
      "benchmarks",
      "ldbc",
      "fixtures",
      "snb-bi"
    ])
  end

  defp tmp_path(test),
    do:
      Path.join(
        System.tmp_dir!(),
        "triple-store-bi-update-#{test}-#{System.unique_integer([:positive])}"
      )
end
