defmodule TripleStore.Benchmark.LDBC.SPB.MixedWorkloadTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.{SPB.Pipeline, StoreFixture}
  alias TripleStore.Benchmark.LDBC.SPB.{Editorial, MixedWorkload, Semantics}
  alias TripleStore.SPARQL.Query

  test "multiple agents preserve editorial order and separate warmup from measured rates",
       %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(Path.join(root, "generated"), seed: 117)
    assert {:ok, fixture} = StoreFixture.setup(root, manifest)
    on_exit(fn -> StoreFixture.teardown(fixture, delete: true) end)
    assert {:ok, coordinator} = Editorial.start_coordinator(fixture.store)
    on_exit(fn -> TripleStore.Transaction.stop(coordinator) end)
    assert {:ok, _stats} = Semantics.materialize(fixture.store)
    context = Semantics.execution_context(fixture.store)

    handlers = %{
      aggregation: fn %{query: query} -> Query.query(context, query) end,
      editorial: fn %{action: action, parameters: parameters} ->
        Editorial.execute(coordinator, fixture.store, action, parameters)
      end
    }

    assert {:ok, scheduler} = MixedWorkload.start_link(handlers: handlers, max_queue: 20)

    warmup = [
      %{
        id: "aggregation-warmup",
        type: :aggregation,
        operations: [aggregation_payload("warmup-1")]
      }
    ]

    assert {:ok, warmup_summary} = MixedWorkload.run_agents(scheduler, warmup, :warmup)
    refute warmup_summary.score_eligible?
    assert warmup_summary.warmup_count == 1

    parameters = Editorial.parameters(7)

    measured = [
      %{
        id: "aggregation-1",
        type: :aggregation,
        operations: [aggregation_payload("read-1"), aggregation_payload("read-2")]
      },
      %{
        id: "aggregation-2",
        type: :aggregation,
        operations: [aggregation_payload("read-3")]
      },
      %{
        id: "editorial-1",
        type: :editorial,
        operations: [
          %{operation_id: "insert", action: :insert, parameters: parameters},
          %{
            operation_id: "update",
            action: :update,
            parameters: Editorial.parameters(7, title: "Updated")
          },
          %{
            operation_id: "delete",
            action: :delete,
            parameters: %{"cwGraphUri" => parameters["cwGraphUri"]}
          }
        ]
      }
    ]

    assert {:ok, report} =
             MixedWorkload.run_agents(scheduler, measured, :measured, concurrency: 3)

    assert report.warmup_count == 1
    assert report.measured_count == 6
    assert report.score_eligible?
    assert report.rates_per_second.aggregation > 0
    assert report.rates_per_second.editorial > 0
    assert report.agents[{:editorial, "editorial-1"}].operations == 3
    assert report.agents[{:editorial, "editorial-1"}].failures == 0
  end

  test "rejects out-of-order dependent editorial submissions" do
    handlers = %{
      aggregation: fn payload -> {:ok, payload} end,
      editorial: fn payload -> {:ok, payload} end
    }

    assert {:ok, scheduler} = MixedWorkload.start_link(handlers: handlers)

    assert {:error, {:editorial_sequence, 0, 1}} =
             MixedWorkload.submit(
               scheduler,
               :editorial,
               "editorial-1",
               1,
               :measured,
               %{operation_id: "update"}
             )
  end

  defp aggregation_payload(id) do
    %{
      operation_id: id,
      query: "SELECT ?s WHERE { ?s <http://schema.org/about> ?o }"
    }
  end

  defp tmp_dir(test) do
    Path.join(System.tmp_dir!(), "spb_mixed_#{test}_#{System.unique_integer([:positive])}")
  end
end
