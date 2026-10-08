defmodule TripleStore.Benchmark.LDBC.Phase4SPBIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.{Artifacts, DatasetManifest, SPB.Pipeline, StoreFixture}

  alias TripleStore.Benchmark.LDBC.SPB.{
    Aggregation,
    Editorial,
    MixedWorkload,
    Resilience,
    Semantics,
    Workload
  }

  alias TripleStore.SPARQL.Query

  @moduletag :integration

  test "complete SPB smoke workflow is correct, recoverable, and fully disclosed", %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(Path.join(root, "generated"), seed: 404)
    assert :ok = DatasetManifest.validate(manifest)
    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)

    assert {:ok, package} = Workload.load()
    assert length(package.operations) == 55
    assert {:ok, binding} = parameter_binding(manifest)
    assert :ok = Workload.validate_aggregation_queries(package, binding)
    assert {:ok, reasoning} = Semantics.materialize(fixture.store)
    assert reasoning.total_derived > 0
    assert {:ok, graph_evidence} = Semantics.verify_contexts(fixture.store)
    assert length(graph_evidence.graphs) == 3

    context = Semantics.execution_context(fixture.store)
    assert {:ok, baseline} = Aggregation.execute_all(context, package, binding, timeout: 5_000)
    assert baseline.operation_count == 25
    assert baseline.correct_count == 25

    assert_failed_editorial_is_atomic(fixture.store)
    editorial = run_editorial_sequence(fixture.store, 404)
    mixed = run_mixed_workload(fixture.store)

    assert {:ok, reset} = StoreFixture.reset(fixture)
    assert {:ok, _reasoning} = Semantics.materialize(reset.store)

    assert {:ok, reset_answers} =
             Aggregation.execute_all(
               Semantics.execution_context(reset.store),
               package,
               binding,
               timeout: 5_000
             )

    assert answer_evidence(reset_answers) == answer_evidence(baseline)

    backup_path = Path.join(root, "backup")
    restore_path = Path.join(root, "restored")

    assert {:ok, resilience} =
             Resilience.run_backup_restore(reset.store, backup_path, restore_path,
               manifest: manifest
             )

    assert resilience.state.equivalent?

    for profile <- [:online_replication, :failover] do
      assert {:error, %{stage: :profile_gate}} =
               Resilience.run_backup_restore(reset.store, "unused", "unused", profile: profile)
    end

    assert {:ok, artifacts} =
             write_artifacts(
               Path.join(root, "artifacts"),
               manifest,
               package,
               baseline,
               editorial,
               mixed,
               resilience
             )

    assert_artifacts(artifacts)
    assert :ok = StoreFixture.teardown(reset, delete: true)
  end

  defp parameter_binding(manifest) do
    parameters = Enum.find(manifest.components, &(&1.role == :parameters))

    with {:ok, values} <- Pipeline.read_parameters(parameters.path),
         do: Workload.bind_parameters(manifest, values)
  end

  defp assert_failed_editorial_is_atomic(store) do
    context = Semantics.execution_context(store)
    probe = "SELECT ?work WHERE { ?work <http://schema.org/about> ?entity }"
    assert {:ok, before} = Query.query(context, probe)

    invalid = Editorial.parameters(999) |> Map.put("cwGraphUri", "not an iri")

    assert {:error, {:invalid_parameter, "invalid IRI"}} =
             Editorial.execute(store.transaction, store, :insert, invalid)

    assert {:ok, after_failure} = Query.query(context, probe)

    assert Aggregation.canonical_answer(after_failure, :unordered) ==
             Aggregation.canonical_answer(before, :unordered)
  end

  defp run_editorial_sequence(store, sequence) do
    parameters = Editorial.parameters(sequence)

    assert {:ok, inserted} = Editorial.execute(store.transaction, store, :insert, parameters)
    assert inserted.validation.valid?

    updated_parameters = Editorial.parameters(sequence, title: "Phase 4 updated title")

    assert {:ok, updated} =
             Editorial.execute(store.transaction, store, :update, updated_parameters)

    assert updated.validation.valid?

    assert {:ok, deleted} =
             Editorial.execute(store.transaction, store, :delete, %{
               "cwGraphUri" => parameters["cwGraphUri"]
             })

    assert deleted.validation.valid?
    %{insert: inserted, update: updated, delete: deleted}
  end

  defp run_mixed_workload(store) do
    context = Semantics.execution_context(store)

    handlers = %{
      aggregation: fn %{query: query} -> Query.query(context, query) end,
      editorial: fn %{action: action, parameters: parameters} ->
        Editorial.execute(store.transaction, store, action, parameters)
      end
    }

    assert {:ok, scheduler} = MixedWorkload.start_link(handlers: handlers, max_queue: 32)

    warmup = [agent("warmup", :aggregation, [query_payload("warmup")])]
    assert {:ok, warmup_report} = MixedWorkload.run_agents(scheduler, warmup, :warmup)
    refute warmup_report.score_eligible?

    parameters = Editorial.parameters(405)

    measured = [
      agent("read-1", :aggregation, [query_payload("read-1"), query_payload("read-2")]),
      agent("read-2", :aggregation, [query_payload("read-3")]),
      agent("editorial", :editorial, [
        %{operation_id: "insert", action: :insert, parameters: parameters},
        %{
          operation_id: "update",
          action: :update,
          parameters: Editorial.parameters(405, title: "Mixed update")
        },
        %{
          operation_id: "delete",
          action: :delete,
          parameters: %{"cwGraphUri" => parameters["cwGraphUri"]}
        }
      ])
    ]

    assert {:ok, report} =
             MixedWorkload.run_agents(scheduler, measured, :measured, concurrency: 3)

    assert report.warmup_count == 1
    assert report.measured_count == 6
    assert report.failures == []
    refute report.score_eligible?
    refute report.score_qualified?
    assert report.rates_per_second == %{}
    assert Enum.sum(Enum.map(report.agents, fn {_agent, metrics} -> metrics.operations end)) == 7
    report
  end

  defp agent(id, type, operations), do: %{id: id, type: type, operations: operations}

  defp query_payload(id) do
    %{
      operation_id: id,
      query: "SELECT ?work WHERE { ?work <http://schema.org/about> ?entity }"
    }
  end

  defp write_artifacts(directory, manifest, package, baseline, editorial, mixed, resilience) do
    raw_samples =
      mixed.records
      |> Enum.filter(&(&1.phase == :measured))
      |> Enum.map(fn record ->
        %{
          operation_id: record.operation_id,
          mode: :measured,
          status: record.status,
          total_us: record.latency_us,
          sample_us: record.latency_us,
          score_eligible?: mixed.score_eligible? and record.status == :success
        }
      end)

    correctness =
      Enum.map(baseline.records, fn record ->
        Map.take(record, [:operation_id, :status, :result_count, :result_digest])
      end)

    Artifacts.write(directory, %{
      manifest: DatasetManifest.identity(manifest),
      environment: %{profile: "spb-smoke", schema: :quad},
      catalog: Enum.map(package.operations, & &1.operation.id),
      raw_samples: raw_samples,
      errors: [],
      correctness: correctness,
      disclosure: Resilience.disclosure(),
      resources: %{backup: resilience.backup, restore: resilience.restore},
      summary: %{
        measured_count: mixed.measured_count,
        valid_sample_count: mixed.measured_count,
        invalid_sample_count: 0,
        aggregation_correct: baseline.correct_count,
        editorial_transitions: map_size(editorial)
      },
      gates: %{
        profile: true,
        correctness: baseline.score_eligible?,
        scheduling: mixed.failures == [] and mixed.measured_count == 6,
        duration: false,
        completeness: baseline.operation_count == 25
      }
    })
  end

  defp assert_artifacts(artifacts) do
    for {_name, path} <- artifacts.paths do
      assert File.regular?(path)
      assert File.stat!(path).size > 0
    end

    for name <- [
          :manifest,
          :environment,
          :catalog,
          :raw_samples,
          :errors,
          :correctness,
          :disclosure,
          :resources,
          :summary
        ] do
      assert {:ok, %{"schema_version" => 1}} =
               artifacts.paths[name] |> File.read!() |> Jason.decode()
    end

    assert File.read!(artifacts.paths.samples_csv) =~ "operation_id,mode,status"
    assert File.read!(artifacts.paths.report_markdown) =~ "Official score eligible: false"

    disclosure = artifacts.paths.disclosure |> File.read!() |> Jason.decode!()
    assert disclosure["backup_is_replication?"] == false
    assert disclosure["availability_score_eligible?"] == false
  end

  defp answer_evidence(report) do
    Enum.map(
      report.records,
      &Map.take(&1, [:operation_id, :status, :result_count, :result_digest])
    )
  end

  defp tmp_dir(test) do
    Path.join(System.tmp_dir!(), "spb_phase4_#{test}_#{System.unique_integer([:positive])}")
  end
end
