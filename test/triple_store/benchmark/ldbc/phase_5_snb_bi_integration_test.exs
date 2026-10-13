defmodule TripleStore.Benchmark.LDBC.Phase5SNBBIIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Benchmark.LDBC.{Artifacts, SNB.Converter, StoreFixture}

  alias TripleStore.Benchmark.LDBC.SNB.BI.{
    Checkpoint,
    GraphAlgorithms,
    Parameters,
    Protocol,
    Scoring,
    UpdateBatch,
    Workload
  }

  @moduletag :integration

  test "BI smoke protocol reconciles workload, updates, checkpoints, paths, scoring, and artifacts",
       %{
         test: test
       } do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, fixture_root(), Path.join(root, "converted"))

    assert {:ok, definitions} = Workload.load()

    assert {:ok, bundle} =
             Parameters.new(manifest, parameter_sequences(definitions), provenance: :smoke)

    assert :ok = Parameters.validate(bundle, manifest, definitions)
    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)

    assert {:ok, fixture, checkpoint} =
             Checkpoint.create(fixture, :initial_load, Path.join(root, "checkpoints"), %{
               batch_position: 0,
               batch_checksum: nil
             })

    assert_path_extension_uses_store_indices(fixture)
    update = Enum.find(manifest.components, &(&1.role == :bi_update_batch))
    assert {:ok, schedule} = Protocol.schedule(:throughput, max_batches: 2)

    assert {:ok, run} =
             Protocol.run(schedule, bundle,
               manifest: manifest,
               apply_update: fn block ->
                 with {:ok, receipt} <- UpdateBatch.apply(fixture.store, update.path) do
                   {:ok, Map.put(receipt, :position, block.position)}
                 end
               end,
               precompute: fn block -> {:ok, %{date: block.date, operations: []}} end,
               execute_read: fn operation ->
                 {:ok,
                  %{
                    operation_id: operation.variant,
                    answer_digest: digest({operation.variant, operation.parameters}),
                    correct?: true,
                    complete?: true
                  }}
               end
             )

    assert run.operation_count == 58
    assert length(run.parameter_sequence) == 56
    assert Enum.all?(run.blocks, & &1.complete?)

    assert {:ok, restored, state} = Checkpoint.restore(fixture, checkpoint)
    assert state.batch_position == 0

    records = scoring_fixture()

    assert {:ok, diagnostic_score} =
             Scoring.calculate(records,
               scale_factor: 1,
               load_time_s: 1,
               throughput_min_s: 1
             )

    assert diagnostic_score.namespace == :diagnostic
    refute diagnostic_score.qualified?

    assert {:ok, artifacts} =
             write_artifacts(
               Path.join(root, "artifacts"),
               manifest,
               definitions,
               run,
               diagnostic_score
             )

    assert_artifacts(artifacts)
    assert_fail_closed_protocol(bundle, manifest)
    assert_fail_closed_scoring(records)
    assert :ok = StoreFixture.teardown(restored, delete: true)
  end

  defp assert_path_extension_uses_store_indices(fixture) do
    knows = RDF.iri("https://ldbcouncil.org/snb/ontology/knows")
    person_1 = RDF.iri("https://ldbcouncil.org/snb/entity/Person/1")
    person_2 = RDF.iri("https://ldbcouncil.org/snb/entity/Person/2")
    assert {:ok, predicate_id} = Adapter.from_rdf_iri(fixture.store.dict_manager, knows)
    assert {:ok, person_1_id} = Adapter.from_rdf_iri(fixture.store.dict_manager, person_1)
    assert {:ok, person_2_id} = Adapter.from_rdf_iri(fixture.store.dict_manager, person_2)

    provider = GraphAlgorithms.index_provider(fixture.store.db, predicate_id)

    assert {:ok, %{rows: [%{vertex: ^person_2_id, distance: 1}]}} =
             GraphAlgorithms.shortest_path_range(person_1_id, 1, 1,
               neighbors: provider,
               timeout: 1_000
             )
  end

  defp assert_fail_closed_protocol(bundle, manifest) do
    assert {:ok, schedule} = Protocol.schedule(:validation)

    assert {:error, %{stage: :reads, score_eligible?: false}} =
             Protocol.run(schedule, bundle,
               manifest: manifest,
               apply_update: fn _block -> {:ok, %{}} end,
               precompute: fn _block -> {:ok, %{}} end,
               execute_read: fn %{variant: variant} ->
                 if variant == "10a", do: {:error, :timeout}, else: {:ok, %{correct?: true}}
               end
             )

    assert {:error, %{stage: :updates, score_eligible?: false}} =
             Protocol.run(schedule, bundle,
               manifest: manifest,
               apply_update: fn _block -> {:error, :injected} end,
               precompute: fn _block -> {:ok, %{}} end,
               execute_read: fn _operation -> {:ok, %{}} end
             )
  end

  defp assert_fail_closed_scoring(records) do
    [first | rest] = records

    assert {:error, errors} =
             Scoring.calculate([Map.put(first, :correct?, false) | rest],
               scale_factor: 1,
               load_time_s: 1,
               throughput_min_s: 1
             )

    assert :incorrect_or_incomplete_operation in errors

    incomplete = Enum.reject(records, &(&1.label == "20b"))

    assert {:error, errors} =
             Scoring.calculate(incomplete,
               scale_factor: 1,
               load_time_s: 1,
               throughput_min_s: 1
             )

    assert :incomplete_power_block in errors
  end

  defp write_artifacts(directory, manifest, definitions, run, score) do
    samples =
      for block <- run.blocks, read <- block.reads do
        %{
          operation_id: read.operation_id,
          mode: block.kind,
          status: :success,
          total_us: round(read.duration_s * 1_000_000),
          sample_us: round(read.duration_s * 1_000_000),
          score_eligible?: false
        }
      end

    Artifacts.write(directory, %{
      manifest: %{
        dataset_id: manifest.dataset_id,
        checksum: manifest.transformation.output_checksum
      },
      environment: %{profile: :snb_bi_smoke, schema: :quad},
      catalog: Enum.map(definitions, & &1.id),
      raw_samples: samples,
      errors: [],
      correctness: Enum.map(run.blocks, &%{position: &1.position, complete?: &1.complete?}),
      disclosure: %{
        protocol_reduction: "two dated blocks; minimum duration disabled",
        comparable: false,
        official_score_name_used: false
      },
      resources: %{blocks: length(run.blocks)},
      summary: %{
        measured_count: length(samples),
        valid_sample_count: length(samples),
        invalid_sample_count: 0,
        diagnostic_score: score
      },
      gates: %{
        profile: true,
        correctness: true,
        scheduling: true,
        duration: false,
        completeness: true
      }
    })
  end

  defp assert_artifacts(artifacts) do
    assert Enum.all?(artifacts.paths, fn {_name, path} -> File.regular?(path) end)
    summary = artifacts.paths.summary |> File.read!() |> Jason.decode!()
    assert summary["official_score_eligible"] == false
    refute Map.has_key?(summary, "official_score")
  end

  defp parameter_sequences(definitions) do
    Map.new(definitions, fn definition ->
      label =
        Integer.to_string(definition.number) <>
          if(definition.variant == "default", do: "", else: definition.variant)

      {label, [Map.new(definition.parameters, &{&1.name, parameter_value(&1.type)})]}
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

  defp scoring_fixture do
    power =
      ["writes" | Protocol.official_variants()]
      |> Enum.map(&score_record(:power, ~D[2012-11-29], &1, 1.0))

    power ++
      [
        score_record(:throughput, ~D[2012-11-30], "writes", 1.0),
        score_record(:throughput, ~D[2012-11-30], "reads", 1.0)
      ]
  end

  defp score_record(batch, day, label, duration) do
    %{
      batch_type: batch,
      day: day,
      label: label,
      duration_s: duration,
      correct?: true,
      complete?: true
    }
  end

  defp digest(term) do
    term
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
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
    Path.join(System.tmp_dir!(), "snb_bi_phase5_#{test}_#{System.unique_integer([:positive])}")
  end
end
