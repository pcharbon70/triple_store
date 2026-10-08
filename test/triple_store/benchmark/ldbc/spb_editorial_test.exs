defmodule TripleStore.Benchmark.LDBC.SPB.EditorialTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.SPB.{Editorial, Pipeline, Semantics}
  alias TripleStore.Benchmark.LDBC.StoreFixture
  alias TripleStore.Exporter

  test "create, alter, and delete are ordered graph-aware validated transitions", %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(Path.join(root, "generated"), seed: 104)
    assert {:ok, fixture} = StoreFixture.setup(root, manifest)
    on_exit(fn -> StoreFixture.teardown(fixture, delete: true) end)
    assert {:ok, coordinator} = Editorial.start_coordinator(fixture.store)
    on_exit(fn -> TripleStore.Transaction.stop(coordinator) end)
    assert {:ok, _stats} = Semantics.materialize(fixture.store)

    initial = Editorial.parameters(1)
    assert {:ok, insert} = Editorial.execute(coordinator, fixture.store, :insert, initial)
    assert insert.validation.valid?

    altered =
      Editorial.parameters(1, title: "Altered editorial work", modified: "2026-02-01T00:00:00Z")

    assert {:ok, update} = Editorial.execute(coordinator, fixture.store, :update, altered)
    assert update.validation.valid?

    assert {:ok, delete} =
             Editorial.execute(coordinator, fixture.store, :delete, %{
               "cwGraphUri" => initial["cwGraphUri"]
             })

    assert delete.validation == %{row_count: 0, valid?: true}
  end

  test "invalid editorial parameters fail before mutation and preserve explicit state", %{
    test: test
  } do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(Path.join(root, "generated"), seed: 105)
    assert {:ok, fixture} = StoreFixture.setup(root, manifest)
    on_exit(fn -> StoreFixture.teardown(fixture, delete: true) end)
    assert {:ok, coordinator} = Editorial.start_coordinator(fixture.store)
    on_exit(fn -> TripleStore.Transaction.stop(coordinator) end)
    assert {:ok, before} = Exporter.export_nquads_string(fixture.store.db)

    invalid = Map.put(Editorial.parameters(2), "unexpected", "DROP ALL")

    assert {:error, {:parameter_names, _expected, _actual}} =
             Editorial.execute(coordinator, fixture.store, :insert, invalid)

    assert {:ok, after_failure} = Exporter.export_nquads_string(fixture.store.db)
    assert before == after_failure
  end

  defp tmp_dir(test) do
    Path.join(System.tmp_dir!(), "spb_editorial_#{test}_#{System.unique_integer([:positive])}")
  end
end
