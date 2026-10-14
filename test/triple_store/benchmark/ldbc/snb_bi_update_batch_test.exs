defmodule TripleStore.Benchmark.LDBC.SNBBIUpdateBatchTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter

  alias TripleStore.Benchmark.LDBC.SNB.{
    BI.Checkpoint,
    BI.UpdateBatch,
    BI.UpdateSchema,
    UpdateStream
  }

  alias TripleStore.Benchmark.LDBC.SNB.Converter
  alias TripleStore.Benchmark.LDBC.StoreFixture
  alias TripleStore.QuadOperations

  test "mixed quad mutations commit atomically in caller order", %{test: test} do
    path = tmp_path(test)
    on_exit(fn -> File.rm_rf!(path) end)
    assert {:ok, store} = TripleStore.open(path, schema: :quad)

    alice = person_record(1, "1", "Alice")
    bob = person_record(2, "2", "Bob")
    delete_alice = %{fixture("delete-01") | sequence: 3, fields: %{"personId" => "1"}}

    assert {:ok, [alice_mutation]} = UpdateSchema.translate_records(nil, [alice])
    assert {:ok, [bob_mutation]} = UpdateSchema.translate_records(nil, [%{bob | sequence: 1}])

    update_path = path <> ".updates"

    assert {:ok, 3} =
             UpdateStream.write(update_path, [
               alice,
               bob,
               delete_alice
             ])

    assert {:ok, receipt} = UpdateBatch.apply(store, update_path)
    assert receipt.records == 3
    assert receipt.last_sequence == 3

    assert {:ok, encoded_alice} =
             TripleStore.Adapter.from_rdf_quads(store.dict_manager, alice_mutation.quads)

    assert {:ok, encoded_bob} =
             TripleStore.Adapter.from_rdf_quads(store.dict_manager, bob_mutation.quads)

    refute encoded_alice |> List.first() |> then(&QuadOperations.quad_exists?(store.db, &1))
    assert Enum.all?(encoded_bob, &QuadOperations.quad_exists?(store.db, &1))

    assert :ok = TripleStore.close(store)
  end

  test "failed storage batch does not run post-commit work", %{test: test} do
    path = tmp_path(test)
    on_exit(fn -> File.rm_rf!(path) end)
    assert {:ok, store} = TripleStore.open(path, schema: :quad)
    update_path = path <> ".updates"
    record = person_record(1, "3", "Carol")

    assert {:ok, 1} =
             UpdateStream.write(update_path, [record])

    assert {:error, {:atomic_update_failed, :injected}} =
             UpdateBatch.apply(store, update_path,
               writer: fn _db, _mutations, _opts -> {:error, :injected} end,
               after_commit: fn _store -> flunk("post-commit callback ran") end
             )

    assert :ok = TripleStore.close(store)
  end

  test "canonical records are translated into one ordered mixed batch", %{test: test} do
    path = tmp_path(test)
    on_exit(fn -> File.rm_rf!(path) end)
    assert {:ok, store} = TripleStore.open(path, schema: :quad)
    update_path = path <> ".updates"
    parent = self()

    [person, friendship, delete_friendship] =
      ["insert-01", "insert-08", "delete-08"]
      |> Enum.map(fn type -> Enum.find(UpdateSchema.fixture_records(), &(&1.type == type)) end)
      |> Enum.with_index(1)
      |> Enum.map(fn {record, sequence} -> %{record | sequence: sequence} end)

    assert {:ok, 3} = UpdateStream.write(update_path, [person, friendship, delete_friendship])

    writer = fn _db, mutations, [sync: true] ->
      send(parent, {:mutations, mutations})
      :ok
    end

    assert {:ok, %{records: 3}} =
             UpdateBatch.apply(store, update_path,
               writer: writer,
               cache_invalidator: fn _db ->
                 send(parent, :cache_invalidated)
                 :ok
               end,
               statistics_invalidator: fn _db ->
                 send(parent, :statistics_invalidated)
                 :ok
               end
             )

    assert_receive {:mutations, mutations}
    assert Enum.any?(mutations, &(elem(&1, 0) == :insert))
    assert Enum.any?(mutations, &(elem(&1, 0) == :delete))
    assert_receive :cache_invalidated
    assert_receive :statistics_invalidated
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

  defp fixture(type), do: Enum.find(UpdateSchema.fixture_records(), &(&1.type == type))

  defp person_record(sequence, id, first_name) do
    base = fixture("insert-01")

    %{
      base
      | sequence: sequence,
        fields: %{base.fields | "personId" => id, "personFirstName" => first_name}
    }
  end
end
