defmodule TripleStore.Benchmark.LDBC.Phase5DQualificationIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.{Artifacts, StoreFixture}
  alias TripleStore.Benchmark.LDBC.SNB.Converter

  alias TripleStore.Benchmark.LDBC.SNB.BI.{
    Analytics,
    Checkpoint,
    Parameters,
    Protocol,
    ProtocolReconciler,
    Qualification,
    Scoring,
    StateIdentity,
    UpdateBatch,
    Workload
  }

  @moduletag :integration

  test "Phase 5D reconciles complete diagnostic execution and accepted-answer evidence",
       %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, definitions} = Workload.load()
    assert length(definitions) == 31

    assert {:ok, parameters} =
             Parameters.new(manifest, parameter_sequences(definitions), provenance: :smoke)

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)
    assert {:ok, initial_identity} = StateIdentity.capture(fixture, :initial_load)

    assert {:ok, fixture, checkpoint} =
             Checkpoint.create(
               fixture,
               :initial_load,
               Path.join(root, "checkpoints"),
               initial_identity
             )

    assert {:ok, before} =
             Qualification.run(definitions, fixture, parameters,
               reference_profile: :snb_bi_smoke,
               timeout: 5_000
             )

    assert before.execution == %{pass: 31}
    assert before.comparisons == %{pass: 31}
    assert before.qualified?
    update = Enum.find(manifest.components, &(&1.role == :bi_update_batch))
    assert {:ok, schedule} = Protocol.schedule(:power)

    assert {:ok, run} =
             Protocol.run(schedule, parameters,
               manifest: manifest,
               fixture: fixture,
               state_identity: initial_identity,
               reference_profile: :snb_bi_smoke,
               apply_update: fn block ->
                 UpdateBatch.apply(fixture.store, update.path,
                   state_identity: block.continuation_state_identity,
                   batch_position: block.position
                 )
               end,
               precompute: fn _block -> {:ok, %{operations: []}} end,
               execute_read: fn operation ->
                 definition = definition_for(definitions, operation.variant)

                 with {:ok, result} <-
                        Analytics.execute(
                          definition,
                          Map.put(fixture.store, :permit_all, true),
                          operation.parameters,
                          timeout: 5_000
                        ) do
                   {:ok,
                    %{
                      correct?: true,
                      complete?: true,
                      result_checksum: digest(result)
                    }}
                 end
               end
             )

    assert run.operation_count == 32
    assert {:ok, score_records} = ProtocolReconciler.score_records(run)
    assert length(score_records) == 32
    assert {:ok, reconciliation} = ProtocolReconciler.reconcile(schedule, run, score_records)

    assert {:ok, score} =
             Scoring.calculate(score_records,
               scale_factor: 1,
               load_time_s: 1,
               reconciled?: reconciliation.valid?
             )

    assert score.namespace == :diagnostic
    refute score.qualified?

    assert {:ok, after_update} =
             Qualification.run(definitions, fixture, parameters,
               reference_profile: :snb_bi_smoke,
               timeout: 5_000
             )

    assert {:ok, matrix} = Qualification.matrix(definitions)
    assert {:ok, combined} = Qualification.reconcile_entries(matrix.entries, after_update.entries)

    identities = qualification_identities(manifest, definitions, parameters, run, checkpoint)

    assert {:ok, qualification} =
             Qualification.evidence(combined, identities,
               reconciled?: reconciliation.valid?,
               updates_complete?: true
             )

    assert qualification.complete?
    assert qualification.operation_count == 31
    assert qualification.blockers == []

    assert {:ok, restored, restored_identity} =
             Checkpoint.restore(fixture, checkpoint, initial_identity)

    assert restored_identity == initial_identity

    assert {:ok, after_restore} =
             Qualification.run(definitions, restored, parameters,
               reference_profile: :snb_bi_smoke,
               timeout: 5_000
             )

    assert execution_digests(after_restore) == execution_digests(before)
    assert comparison_digests(after_restore) == comparison_digests(before)

    artifact_run = %{
      suite: :snb_bi,
      manifest: %{
        suite: :snb_bi,
        dataset_id: manifest.dataset_id,
        state_identity: run.state_identity
      },
      environment: %{identity: identities.environment, profile: :snb_bi_smoke},
      catalog: Enum.map(definitions, &%{id: &1.id}),
      raw_samples: [],
      errors: [],
      correctness: [
        %{position: 1, state_identity: run.state_identity, qualified?: qualification.complete?}
      ],
      protocol_blocks: run.blocks,
      disclosure: %{
        comparable: false,
        state_identity: run.state_identity,
        remaining_blockers: qualification.blockers
      },
      resources: %{protocol_operations: run.operation_count},
      summary: %{measured_count: 32, valid_sample_count: 32, invalid_sample_count: 0},
      qualification: qualification,
      gates: %{
        profile: true,
        correctness: qualification.complete?,
        scheduling: true,
        duration: true,
        completeness: true
      }
    }

    assert {:ok, artifacts} = Artifacts.write(Path.join(root, "artifacts"), artifact_run)
    summary = artifacts.paths.summary |> File.read!() |> Jason.decode!()
    assert summary["claim_profile"] == "diagnostic"
    assert summary["official_score_eligible"] == false
    assert File.regular?(artifacts.paths.checksums)
    assert :ok = StoreFixture.teardown(restored, delete: true)
  end

  test "Phase 5D failure gates reject incorrect reads and incomplete score inputs" do
    assert {:ok, schedule} = Protocol.schedule(:power)

    assert {:error, :empty_protocol_run} =
             ProtocolReconciler.reconcile([], %{blocks: []}, [])

    record = %{
      batch_type: :power,
      day: ~D[2012-11-29],
      label: "writes",
      duration_s: 1.0,
      correct?: false,
      complete?: true,
      score_eligible?: false
    }

    assert {:error, errors} =
             Scoring.calculate([record],
               scale_factor: 1,
               load_time_s: 1,
               reconciled?: false
             )

    assert :incorrect_or_incomplete_operation in errors
    assert :incomplete_power_block in errors
    assert :protocol_reconciliation_failed in errors
    assert length(schedule) == 1
  end

  defp definition_for(definitions, label) do
    Enum.find(definitions, fn definition -> variant_label(definition) == label end)
  end

  defp execution_digests(run) do
    Map.new(run.entries, &{{&1.operation_id, &1.variant}, &1.execution.digest})
  end

  defp comparison_digests(run) do
    Map.new(run.entries, &{{&1.operation_id, &1.variant}, &1.comparison.digest})
  end

  defp qualification_identities(manifest, definitions, parameters, run, checkpoint) do
    %{
      dataset: manifest.transformation.output_checksum,
      mapping: manifest.transformation.mapping_version,
      translation: digest(Enum.map(definitions, & &1[:translation][:checksum])),
      parameters: digest(parameters),
      reference_answers: file_digest(reference_manifest()),
      update_stream: run.state_identity.update_stream_checksum,
      checkpoint: checkpoint.state_identity.state_checksum,
      scorer: Scoring.identity(),
      environment: digest({System.version(), System.otp_release(), :quad})
    }
  end

  defp parameter_sequences(definitions) do
    Map.new(definitions, fn definition ->
      row = Map.new(definition.parameters, &{&1.name, parameter_value(&1.type)})
      {variant_label(definition), [row]}
    end)
  end

  defp parameter_value("ID"), do: 1
  defp parameter_value("32-bit Integer"), do: 1
  defp parameter_value("64-bit Integer"), do: 1
  defp parameter_value("32-bit Float"), do: 1.0
  defp parameter_value("Boolean"), do: true
  defp parameter_value("Date"), do: "2012-11-29"
  defp parameter_value("DateTime"), do: "2012-11-29T00:00:00Z"
  defp parameter_value(type) when type in ["String", "Long String"], do: "value"
  defp parameter_value("\\{String\\}"), do: ["value"]

  defp variant_label(definition) do
    suffix = if definition.variant == "default", do: "", else: definition.variant
    Integer.to_string(definition.number) <> suffix
  end

  defp reference_manifest do
    Path.join([
      to_string(:code.priv_dir(:triple_store)),
      "benchmarks",
      "ldbc",
      "snb-bi",
      "reference",
      "manifest.exs"
    ])
  end

  defp file_digest(path), do: path |> File.read!() |> digest()

  defp digest(term) do
    "sha256:" <>
      (term
       |> :erlang.term_to_binary([:deterministic])
       |> then(&:crypto.hash(:sha256, &1))
       |> Base.encode16(case: :lower))
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

  defp tmp_dir(test) do
    Path.join(
      System.tmp_dir!(),
      "phase_5d_qualification_#{:erlang.phash2(test)}_#{System.unique_integer([:positive])}"
    )
  end
end
