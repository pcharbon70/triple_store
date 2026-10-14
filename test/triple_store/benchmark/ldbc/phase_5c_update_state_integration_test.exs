defmodule TripleStore.Benchmark.LDBC.Phase5CUpdateStateIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.SNB.Converter
  alias TripleStore.Benchmark.LDBC.StoreFixture

  alias TripleStore.Benchmark.LDBC.SNB.BI.{
    Analytics,
    Checkpoint,
    Scoring,
    StateIdentity,
    StateProbe,
    UpdateBatch,
    UpdateSchema,
    Workload
  }

  alias TripleStore.Benchmark.LDBC.SNB.UpdateStream

  @moduletag :integration

  @insert_types ~w(insert-01 insert-04 insert-06 insert-07 insert-02 insert-03 insert-05 insert-08)
  @relationship_delete_types ~w(delete-02 delete-03 delete-05 delete-08)
  @entity_delete_types ~w(delete-07 delete-06 delete-04 delete-01)

  test "complete update sequence preserves index, answer, and checkpoint state identity", %{
    test: test
  } do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, definitions} = Workload.load()
    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)
    checkpoint_root = Path.join(root, "checkpoints")

    assert {:ok, initial_snapshot} = StateProbe.capture(fixture.store)
    assert_index_agreement(initial_snapshot)
    assert {:ok, initial_identity} = StateIdentity.capture(fixture, :initial_load)

    assert {:ok, fixture, initial_checkpoint} =
             Checkpoint.create(fixture, :initial_load, checkpoint_root, initial_identity)

    insert_records = records(@insert_types, 1)
    assert {:ok, insert_mutations} = UpdateSchema.translate_records(fixture.store, insert_records)

    selected_definitions =
      definitions
      |> StateProbe.affected_reads(insert_mutations)
      |> Enum.take(5)

    assert selected_definitions != []
    initial_answers = answer_hashes(fixture.store, selected_definitions)

    assert initial_answers[{"ldbc/snb-bi/read-01@v1.0.3", "default"}] ==
             "sha256:3255eb8b221ef966e538e2172935bd856d8e7a0f48f57b66592a969453d91424"

    assert {:ok, insert_receipt} =
             apply_records(fixture.store, Path.join(root, "insert.updates"), insert_records)

    assert insert_receipt.records == 8
    assert insert_receipt.first_sequence == 1
    assert insert_receipt.last_sequence == 8
    assert {:ok, post_validation_snapshot} = StateProbe.capture(fixture.store)
    assert_index_agreement(post_validation_snapshot)
    assert :ok = StateProbe.verify_transition(initial_snapshot, post_validation_snapshot)
    assert post_validation_snapshot.counts == expected_counts(7, 21, 52, 2, 118)

    assert post_validation_snapshot.state_checksum ==
             "sha256:16a13d0c4d0037f18ff199e3edf61369a1dce6514739d645a9e2c58c73cd0102"

    post_validation_answers = answer_hashes(fixture.store, selected_definitions)

    assert post_validation_answers[{"ldbc/snb-bi/read-01@v1.0.3", "default"}] ==
             "sha256:058df443305256117a13ed7d0f921b16f7869c6f1b7d09c003724010e672328c"

    assert {:ok, post_validation_identity} =
             StateIdentity.capture(fixture, :post_validation,
               last_applied_batch: 1,
               update_stream_checksum: insert_receipt.checksum
             )

    assert {:ok, fixture, post_validation_checkpoint} =
             Checkpoint.create(
               fixture,
               :post_validation,
               checkpoint_root,
               post_validation_identity
             )

    relationship_delete_records = records(@relationship_delete_types, 9)

    assert {:ok, relationship_delete_mutations} =
             UpdateSchema.translate_records(fixture.store, relationship_delete_records)

    assert {:ok, relationship_delete_receipt} =
             apply_records(
               fixture.store,
               Path.join(root, "relationship-delete.updates"),
               relationship_delete_records
             )

    assert relationship_delete_receipt.first_sequence == 9
    assert relationship_delete_receipt.last_sequence == 12
    assert {:ok, pre_power_snapshot} = StateProbe.capture(fixture.store)
    assert_index_agreement(pre_power_snapshot)
    assert pre_power_snapshot.counts == expected_counts(7, 17, 48, 2, 94)

    assert pre_power_snapshot.state_checksum ==
             "sha256:5be8fd1ab530aef984d0ef09fe8a8961c7abf198243ffba2602c8c7d9518250a"

    pre_power_answers = answer_hashes(fixture.store, selected_definitions)

    assert {:ok, pre_power_identity} =
             StateIdentity.capture(fixture, :pre_power,
               last_applied_batch: 2,
               update_stream_checksum: relationship_delete_receipt.checksum
             )

    assert {:ok, fixture, pre_power_checkpoint} =
             Checkpoint.create(fixture, :pre_power, checkpoint_root, pre_power_identity)

    entity_delete_records = records(@entity_delete_types, 13)

    assert {:ok, entity_delete_mutations} =
             UpdateSchema.translate_records(fixture.store, entity_delete_records)

    assert {:ok, entity_delete_receipt} =
             apply_records(
               fixture.store,
               Path.join(root, "entity-delete.updates"),
               entity_delete_records
             )

    assert entity_delete_receipt.first_sequence == 13
    assert entity_delete_receipt.last_sequence == 16
    assert {:ok, pre_throughput_snapshot} = StateProbe.capture(fixture.store)
    assert_index_agreement(pre_throughput_snapshot)
    assert pre_throughput_snapshot.counts == initial_snapshot.counts
    assert pre_throughput_snapshot.state_checksum == initial_snapshot.state_checksum
    pre_throughput_answers = answer_hashes(fixture.store, selected_definitions)
    assert pre_throughput_answers == initial_answers

    assert {:ok, pre_throughput_identity} =
             StateIdentity.capture(fixture, :pre_throughput,
               last_applied_batch: 3,
               update_stream_checksum: entity_delete_receipt.checksum
             )

    assert {:ok, fixture, pre_throughput_checkpoint} =
             Checkpoint.create(
               fixture,
               :pre_throughput,
               checkpoint_root,
               pre_throughput_identity
             )

    assert MapSet.new(Enum.map(insert_records, & &1.type)) ==
             MapSet.new(
               Enum.filter(
                 UpdateSchema.fixture_records(),
                 &String.starts_with?(&1.type, "insert-")
               ),
               & &1.type
             )

    all_delete_records = relationship_delete_records ++ entity_delete_records

    assert MapSet.new(Enum.map(all_delete_records, & &1.type)) ==
             MapSet.new(
               Enum.filter(
                 UpdateSchema.fixture_records(),
                 &String.starts_with?(&1.type, "delete-")
               ),
               & &1.type
             )

    all_mutations = insert_mutations ++ relationship_delete_mutations ++ entity_delete_mutations
    assert StateProbe.affected_reads(definitions, all_mutations) != []

    expectations = [
      {initial_checkpoint, initial_identity, initial_snapshot, initial_answers},
      {post_validation_checkpoint, post_validation_identity, post_validation_snapshot,
       post_validation_answers},
      {pre_power_checkpoint, pre_power_identity, pre_power_snapshot, pre_power_answers},
      {pre_throughput_checkpoint, pre_throughput_identity, pre_throughput_snapshot,
       pre_throughput_answers}
    ]

    fixture =
      Enum.reduce(expectations, fixture, fn {checkpoint, identity, snapshot, answers}, current ->
        assert {:ok, restored, restored_identity} =
                 Checkpoint.restore(current, checkpoint, identity)

        assert :ok = StateIdentity.compare(identity, restored_identity)
        assert {:ok, restored_snapshot} = StateProbe.capture(restored.store)
        assert restored_snapshot.counts == snapshot.counts
        assert restored_snapshot.index_counts == snapshot.index_counts
        assert restored_snapshot.state_checksum == snapshot.state_checksum
        assert_index_agreement(restored_snapshot)
        assert answer_hashes(restored.store, selected_definitions) == answers
        restored
      end)

    assert fixture.store != nil
    assert :ok = StoreFixture.teardown(fixture, delete: true)
  end

  test "translation, dependency, storage, interruption, and restore failures preserve state", %{
    test: test
  } do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, definitions} = Workload.load()
    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)
    selected = Enum.filter(definitions, &(&1.number == 1))
    answers = answer_hashes(fixture.store, selected)
    assert {:ok, before} = StateProbe.capture(fixture.store)
    assert {:ok, initial_identity} = StateIdentity.capture(fixture, :initial_load)

    assert {:ok, fixture, checkpoint} =
             Checkpoint.create(
               fixture,
               :initial_load,
               Path.join(root, "checkpoints"),
               initial_identity
             )

    unknown = [%{hd(UpdateSchema.fixture_records()) | sequence: 1, type: "insert-99"}]

    assert {:error, {:unknown_update_type, "insert-99"}} =
             apply_records(fixture.store, Path.join(root, "translation.updates"), unknown)

    assert_preserved(fixture.store, before, answers, selected)

    dependency_records = records(["insert-02", "insert-06"], 1)

    assert {:error, {:out_of_order_update_dependency, 1, {"Post", "6001"}, 2}} =
             apply_records(
               fixture.store,
               Path.join(root, "dependency.updates"),
               dependency_records
             )

    assert_preserved(fixture.store, before, answers, selected)

    valid = records(["insert-01"], 1)
    storage_path = Path.join(root, "storage.updates")
    assert {:ok, 1} = UpdateStream.write(storage_path, valid)

    assert {:error, {:atomic_update_failed, :injected_storage_failure}} =
             UpdateBatch.apply(fixture.store, storage_path,
               writer: fn _db, _mutations, _opts -> {:error, :injected_storage_failure} end
             )

    assert_preserved(fixture.store, before, answers, selected)

    interruption_path = Path.join(root, "interruption.updates")
    assert {:ok, 1} = UpdateStream.write(interruption_path, valid)

    assert {:error, {:atomic_update_failed, :interrupted}} =
             UpdateBatch.apply(fixture.store, interruption_path,
               writer: fn _db, _mutations, _opts -> {:error, :interrupted} end
             )

    assert_preserved(fixture.store, before, answers, selected)

    metadata_path = Path.join(checkpoint.path, ".checkpoint.etf")
    File.write!(metadata_path, "corrupt checkpoint metadata")

    assert {:error, {:checkpoint_metadata_mismatch, :initial_load}} =
             Checkpoint.restore(fixture, checkpoint, initial_identity)

    assert_preserved(fixture.store, before, answers, selected)
    assert :ok = StoreFixture.teardown(fixture, delete: true)
  end

  test "post-commit maintenance failure reports committed evidence and cannot be scored", %{
    test: test
  } do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)
    store_path = Path.join(root, "store")
    update_path = Path.join(root, "updates.bin")

    assert {:ok, store} = TripleStore.open(store_path, schema: :quad)
    [record | _rest] = UpdateSchema.fixture_records()
    assert {:ok, before} = StateProbe.capture(store)

    assert {:ok, initial_identity} =
             StateIdentity.new(%{
               dataset_checksum: sha256("phase-5c-post-commit-fixture"),
               mapping_version: "snb-rdf-v1",
               checkpoint_name: :initial_load,
               last_applied_batch: 0,
               update_stream_checksum: nil,
               state_checksum: before.state_checksum
             })

    assert {:ok, 1} = UpdateStream.write(update_path, [record])

    assert {:error,
            {:post_commit_maintenance_failed, {:post_commit_refresh_failed, :injected}, committed}} =
             UpdateBatch.apply(store, update_path,
               cache_invalidator: fn _db -> {:error, :injected} end,
               state_identity: initial_identity,
               batch_position: 1
             )

    assert committed.committed?
    refute committed.score_eligible?
    refute committed.complete?
    assert committed.first_sequence == 1
    assert committed.last_sequence == 1
    assert String.starts_with?(committed.state_checksum, "sha256:")
    assert committed.state_identity.last_applied_batch == 1
    assert committed.state_identity.update_stream_checksum == committed.checksum
    assert committed.state_identity.state_checksum == committed.state_checksum
    assert {:ok, snapshot} = StateProbe.capture(store)
    assert snapshot.state_checksum == committed.state_checksum
    refute snapshot.state_checksum == before.state_checksum
    assert_index_agreement(snapshot)

    scoring_record =
      Map.merge(committed, %{
        batch_type: :power,
        label: "writes",
        duration_s: 0.1,
        correct?: true
      })

    assert {:error, errors} =
             Scoring.calculate([scoring_record], scale_factor: 1, load_time_s: 0.1)

    assert :incorrect_or_incomplete_operation in errors
    assert :ok = TripleStore.close(store)
  end

  defp apply_records(store, path, records) do
    with {:ok, _count} <- UpdateStream.write(path, records) do
      UpdateBatch.apply(store, path)
    end
  end

  defp assert_preserved(store, before, answers, definitions) do
    assert {:ok, after_failure} = StateProbe.capture(store)
    after_answers = answer_hashes(store, definitions)
    assert :ok = StateProbe.verify_preserved(before, after_failure, answers, after_answers)
    assert_index_agreement(after_failure)
  end

  defp assert_index_agreement(snapshot) do
    assert snapshot.indices_consistent?
    assert snapshot.index_counts |> Map.values() |> Enum.uniq() |> length() == 1
  end

  defp expected_counts(entities, relationships, properties, graphs, statements) do
    %{
      entities: entities,
      relationships: relationships,
      properties: properties,
      graphs: graphs,
      statements: statements
    }
  end

  defp answer_hashes(store, definitions) do
    context = Map.put(store, :permit_all, true)

    Map.new(definitions, fn definition ->
      assert {:ok, result} =
               Analytics.execute(definition, context, parameters(definition), timeout: 5_000)

      digest =
        result
        |> Map.take([:columns, :types, :rows, :ordering])
        |> :erlang.term_to_binary([:deterministic])
        |> then(&:crypto.hash(:sha256, &1))
        |> Base.encode16(case: :lower)

      {{definition.catalog_id, definition.variant}, "sha256:" <> digest}
    end)
  end

  defp parameters(definition),
    do: Map.new(definition.parameters, &{&1.name, parameter_value(&1.type)})

  defp parameter_value("ID"), do: 1
  defp parameter_value("32-bit Integer"), do: 1
  defp parameter_value("64-bit Integer"), do: 1
  defp parameter_value("32-bit Float"), do: 1.0
  defp parameter_value("Boolean"), do: true
  defp parameter_value("Date"), do: "2012-11-29"
  defp parameter_value("DateTime"), do: "2012-11-29T00:00:00Z"
  defp parameter_value(type) when type in ["String", "Long String"], do: "value"
  defp parameter_value("\\{String\\}"), do: ["value"]

  defp records(types, first_sequence) do
    fixtures = Map.new(UpdateSchema.fixture_records(), &{&1.type, &1})

    types
    |> Enum.with_index(first_sequence)
    |> Enum.map(fn {type, sequence} -> %{Map.fetch!(fixtures, type) | sequence: sequence} end)
  end

  defp fixture_root do
    Path.join([
      to_string(:code.priv_dir(:triple_store)),
      "benchmarks",
      "ldbc",
      "fixtures",
      "snb-bi"
    ])
  end

  defp sha256(value),
    do: "sha256:" <> (:crypto.hash(:sha256, value) |> Base.encode16(case: :lower))

  defp tmp_dir(test) do
    Path.join(
      System.tmp_dir!(),
      "phase_5c_update_state_#{:erlang.phash2(test)}_#{System.unique_integer([:positive])}"
    )
  end
end
