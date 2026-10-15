defmodule TripleStore.Benchmark.LDBC.SNBBIProtocolTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.SNB.BI.{
    Parameters,
    PlanEvidence,
    Protocol,
    Scoring,
    StateIdentity,
    Workload
  }

  test "official schedule preserves update-read blocks and canonical variant order" do
    assert {:ok, definitions} = Workload.load()
    assert {:ok, [validation]} = Protocol.schedule(:validation)
    assert validation.kind == :validation

    assert {:ok, [power, throughput]} = Protocol.schedule(:throughput, max_batches: 2)
    assert power.date == ~D[2012-11-29]
    assert power.kind == :power
    assert throughput.date == ~D[2012-11-30]
    assert throughput.kind == :throughput
    assert power.stages == [:updates, :precomputations, :reads]
    assert power.variants == Protocol.official_variants()
    assert power.variants == Workload.variant_labels(definitions)
    assert length(power.variants) == 31

    assert {:error, :comparable_schedule_cannot_be_reduced} =
             Protocol.schedule(:throughput, max_batches: 2, comparable: true)
  end

  test "protocol records complete operation identity and fails fast on ineligible reads" do
    identity = identity(:initial_load, 0, nil, "a")
    assert {:ok, schedule} = Protocol.schedule(:validation)

    callbacks = [
      manifest: parameter_manifest(),
      state_identity: identity,
      apply_update: fn _block ->
        {:ok, %{checksum: checksum("1"), state_checksum: checksum("b")}}
      end,
      precompute: fn _block -> {:ok, %{operations: []}} end,
      execute_read: fn operation ->
        {:ok,
         %{
           correct?: operation.variant != "8m",
           complete?: true,
           result_checksum: checksum(operation.variant)
         }}
      end
    ]

    assert {:error, failure} = Protocol.run(schedule, parameter_bundle(), callbacks)
    assert failure.stage == :reads
    assert failure.score_eligible? == false
    assert failure.reason == {:ineligible_read, "8m", :incorrect}

    assert Enum.map(failure.partial.reads, & &1.variant) ==
             Enum.take_while(Protocol.official_variants(), &(&1 != "8m"))

    assert Enum.all?(failure.partial.reads, fn receipt ->
             receipt.operation_id =~ "ldbc/snb-bi/read-" and
               receipt.parameter_row_identity.variant == receipt.variant and
               receipt.parameter_row_identity.row_index == 0 and
               String.starts_with?(receipt.parameter_row_identity.checksum, "sha256:") and
               String.starts_with?(receipt.result_checksum, "sha256:") and
               receipt.correctness_status == :correct and
               receipt.score_eligible? == true and receipt.duration_s > 0
           end)
  end

  test "local scorer matches pinned formulas and fails closed" do
    power =
      ["writes" | Protocol.official_variants()]
      |> Enum.map(&record(:power, ~D[2012-11-29], &1, 4.0))

    throughput = [
      record(:throughput, ~D[2012-11-30], "writes", 1_800.0),
      record(:throughput, ~D[2012-11-30], "reads", 1_800.0)
    ]

    assert {:ok, score} =
             Scoring.calculate(power ++ throughput,
               scale_factor: 10,
               load_time_s: 3_600,
               throughput_min_s: 3_600,
               comparable: true
             )

    assert_in_delta score.power, 900.0, 1.0e-9
    assert_in_delta score.power_at_scale, 9_000.0, 1.0e-8
    assert_in_delta score.throughput, 23.0, 1.0e-9
    assert score.namespace == :canonical

    bad = put_in(hd(power)[:correct?], false)

    assert {:error, errors} =
             Scoring.calculate([bad | tl(power)] ++ throughput,
               scale_factor: 10,
               load_time_s: 3_600,
               comparable: true
             )

    assert :incorrect_or_incomplete_operation in errors
  end

  test "optimization evidence rejects changed answers and incomplete measurements" do
    measurements = %{
      plan: :hash_join,
      estimated_cardinality: 3,
      iterator_count: 2,
      materialized_rows: 3,
      memory_bytes: 128,
      io_bytes: 256
    }

    assert {:ok, evidence} = PlanEvidence.record("bi-1", [[1], [2]], [[1], [2]], measurements)
    assert evidence.plan == :hash_join
    assert {:error, :answer_changed} = PlanEvidence.record("bi-1", [[1]], [[2]], measurements)
  end

  test "protocol advances and exposes one exact identity on every block" do
    identity = identity(:initial_load, 0, nil, "a")
    bundle = parameter_bundle()
    manifest = parameter_manifest()
    assert {:ok, schedule} = Protocol.schedule(:throughput, max_batches: 2)

    assert {:ok, run} =
             Protocol.run(schedule, bundle,
               manifest: manifest,
               state_identity: identity,
               apply_update: fn block ->
                 {:ok,
                  %{
                    checksum: checksum(Integer.to_string(block.position)),
                    state_checksum: checksum(<<96 + block.position>>)
                  }}
               end,
               precompute: fn _block -> {:ok, %{operations: []}} end,
               execute_read: fn _operation -> {:ok, %{correct?: true, complete?: true}} end
             )

    [power, throughput] = run.blocks
    assert %StateIdentity{last_applied_batch: 1} = power.state_identity
    assert %StateIdentity{last_applied_batch: 2} = throughput.state_identity
    assert power.state_identity.dataset_checksum == identity.dataset_checksum
    assert throughput.state_identity.update_stream_checksum == checksum("2")
  end

  test "protocol rejects continuation when any reported identity field disagrees" do
    identity = identity(:pre_power, 0, nil, "a")
    assert {:ok, schedule} = Protocol.schedule(:power)

    expected = %{
      identity
      | last_applied_batch: 1,
        update_stream_checksum: checksum("1"),
        state_checksum: checksum("b")
    }

    mismatched = %{expected | dataset_checksum: checksum("other")}

    assert {:error,
            %{stage: :updates, reason: {:state_identity_mismatch, :dataset_checksum, _, _}}} =
             Protocol.run(schedule, parameter_bundle(),
               manifest: parameter_manifest(),
               state_identity: identity,
               apply_update: fn _block ->
                 {:ok,
                  %{
                    checksum: checksum("1"),
                    state_checksum: checksum("b"),
                    state_identity: mismatched
                  }}
               end,
               precompute: fn _block -> {:ok, %{}} end,
               execute_read: fn _operation -> {:ok, %{}} end
             )
  end

  test "state identity enforces initial and updated batch boundaries" do
    initial = Map.from_struct(identity(:initial_load, 0, nil, "a"))

    assert {:error, {:invalid_state_identity, :checkpoint_name}} =
             StateIdentity.new(%{initial | checkpoint_name: nil})

    assert {:error, {:invalid_state_identity, :update_stream_checksum}} =
             StateIdentity.new(%{initial | update_stream_checksum: checksum("unexpected")})

    assert {:error, {:invalid_state_identity, :update_stream_checksum}} =
             StateIdentity.new(%{initial | last_applied_batch: 1})
  end

  defp record(batch_type, day, label, duration) do
    %{
      batch_type: batch_type,
      day: day,
      label: label,
      duration_s: duration,
      correct?: true,
      complete?: true
    }
  end

  defp identity(checkpoint, last_batch, update_checksum, state) do
    {:ok, identity} =
      StateIdentity.new(%{
        dataset_checksum: checksum("dataset"),
        mapping_version: "snb-rdf-v1",
        checkpoint_name: checkpoint,
        last_applied_batch: last_batch,
        update_stream_checksum: update_checksum,
        state_checksum: checksum(state)
      })

    identity
  end

  defp checksum(value), do: "sha256:" <> Base.encode16(:crypto.hash(:sha256, value), case: :lower)

  defp parameter_manifest do
    %{
      scale_factor: "1",
      profile: "smoke",
      transformation: %{
        output_checksum: checksum("dataset"),
        mapping_version: "snb-rdf-v1"
      },
      store: %{path_identity: "fixture"}
    }
  end

  defp parameter_bundle do
    {:ok, definitions} = Workload.load()

    sequences =
      Map.new(definitions, fn definition ->
        variant =
          Integer.to_string(definition.number) <>
            if(definition.variant == "default", do: "", else: definition.variant)

        values = Map.new(definition.parameters, &{&1.name, parameter_value(&1.type)})
        {variant, [values]}
      end)

    {:ok, bundle} =
      Parameters.new(parameter_manifest(), sequences, provenance: :smoke)

    bundle
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
end
