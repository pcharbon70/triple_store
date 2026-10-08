defmodule TripleStore.Benchmark.LDBC.SPB.SemanticsTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.LDBC.{SPB.Pipeline, StoreFixture}
  alias TripleStore.Benchmark.LDBC.SPB.{Semantics, Workload}
  alias TripleStore.Reasoner.DerivedStore
  alias TripleStore.SPARQL.Query

  test "profile covers SPB hierarchy, property, and equality semantics" do
    assert {:ok, profile} = Semantics.profile()
    assert profile.scope == :global
    assert profile.storage_strategy == :per_graph_cf
    assert profile.explicit_and_derived_distinct?

    assert Enum.all?(
             [:scm_sco, :scm_spo, :cax_sco, :prp_spo1, :prp_trp, :prp_symp, :eq_trans],
             &(&1 in profile.required_rules)
           )
  end

  test "query context makes union graphs and derived facts explicit" do
    store = %{db: :db, dict_manager: :dictionary}
    context = Semantics.execution_context(store)
    assert context.union_default_graph
    assert context.include_derived
    assert context.authorization == :disabled
  end

  test "quad union default graph includes explicit named graphs and graph-zero derived facts",
       %{test: test} do
    root = tmp_dir(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, manifest} = Pipeline.generate_smoke(Path.join(root, "generated"), seed: 82)
    assert {:ok, fixture} = StoreFixture.setup(root, manifest)
    on_exit(fn -> StoreFixture.teardown(fixture, delete: true) end)

    assert {:ok, context_evidence} = Semantics.verify_contexts(fixture.store)
    assert length(context_evidence.graphs) == 3

    context = Semantics.execution_context(fixture.store)

    assert {:ok, rows} =
             Query.query(
               context,
               "SELECT ?work WHERE { ?work <http://schema.org/about> ?entity }"
             )

    assert length(rows) == 1

    assert {:ok, stats} = Semantics.materialize(fixture.store)
    assert stats.total_derived >= 1

    assert {:ok, inferred_rows} =
             Query.query(
               context,
               "SELECT ?work WHERE { ?work <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/CreativeWork> }"
             )

    assert length(inferred_rows) == 1

    {:ok, [subject_id, predicate_id, object_id]} =
      TripleStore.Dictionary.Manager.get_or_create_ids(fixture.store.dict_manager, [
        RDF.iri("urn:derived:subject"),
        RDF.iri("urn:derived:predicate"),
        RDF.iri("urn:derived:object")
      ])

    assert :ok =
             DerivedStore.insert_derived_quads(fixture.store.db, [
               {0, subject_id, predicate_id, object_id}
             ])

    assert {:ok, [%{"s" => subject}]} =
             Query.query(
               context,
               "SELECT ?s WHERE { ?s <urn:derived:predicate> <urn:derived:object> }"
             )

    assert subject == {:named_node, "urn:derived:subject"}
  end

  test "conformance qualification requires every mandatory action" do
    assert {:ok, package} = Workload.load()

    ids =
      package.operations
      |> Enum.filter(&(&1.family == :conformance))
      |> Map.new(&{&1.operation.id, true})

    assert :ok = Semantics.conformance_gate(package, ids)
    [{failed_id, true} | rest] = Map.to_list(ids)

    assert {:error, {:failed_conformance_actions, [^failed_id]}} =
             Semantics.conformance_gate(package, Map.new([{failed_id, false} | rest]))
  end

  defp tmp_dir(test) do
    Path.join(System.tmp_dir!(), "spb_semantics_#{test}_#{System.unique_integer([:positive])}")
  end
end
