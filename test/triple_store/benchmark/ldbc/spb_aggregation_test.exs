defmodule TripleStore.Benchmark.LDBC.SPB.AggregationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.{SPB.Pipeline, StoreFixture}
  alias TripleStore.Benchmark.LDBC.SPB.{Aggregation, Semantics, Workload}

  test "preserves duplicate and unbound rows while canonicalizing unordered results" do
    rows = [%{"x" => :unbound}, %{"x" => 2}, %{"x" => 2}]
    canonical = Aggregation.canonical_answer(rows, :unordered)
    assert Enum.frequencies(canonical)[%{"x" => 2}] == 2
    assert %{"x" => :unbound} in canonical
    assert Aggregation.canonical_answer(rows, :ordered) == rows
  end

  test "executes and explains the complete aggregation corpus against reference plans",
       %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(Path.join(root, "generated"), seed: 93)
    assert {:ok, fixture} = StoreFixture.setup(root, manifest)
    on_exit(fn -> StoreFixture.teardown(fixture, delete: true) end)
    assert {:ok, _stats} = Semantics.materialize(fixture.store)
    assert {:ok, package} = Workload.load()

    parameter_component = Enum.find(manifest.components, &(&1.role == :parameters))
    assert {:ok, parameters} = Pipeline.read_parameters(parameter_component.path)
    assert {:ok, binding} = Workload.bind_parameters(manifest, parameters)
    context = Semantics.execution_context(fixture.store)

    assert {:ok, report} = Aggregation.execute_all(context, package, binding, timeout: 5_000)
    assert report.operation_count == 25
    assert report.correct_count == 25
    assert report.score_eligible?
    assert Enum.all?(report.records, & &1.reference_match?)

    assert {:ok, plans} = Aggregation.explain_all(context, package, binding)
    assert length(plans) == 25
    assert Enum.all?(plans, &(map_size(&1.optimized.operators) > 0))

    alternate = %{binding | values: ["urn:ldbc:spb:entity:absent"]}

    assert {:ok, alternate_report} =
             Aggregation.execute_all(context, package, alternate, timeout: 5_000)

    assert alternate_report.score_eligible?
  end

  defp tmp_dir(test) do
    Path.join(System.tmp_dir!(), "spb_aggregation_#{test}_#{System.unique_integer([:positive])}")
  end
end
