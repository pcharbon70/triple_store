defmodule TripleStore.Benchmark.LDBC.SNBBIUpdateAnswersTest do
  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Benchmark.LDBC.Result

  alias TripleStore.Benchmark.LDBC.SNB.{
    BI.Qualification,
    BI.StateProbe,
    BI.UpdateBatch,
    Converter
  }

  alias TripleStore.Benchmark.LDBC.SNB.Mapping
  alias TripleStore.Benchmark.LDBC.SNB.UpdateStream
  alias TripleStore.Benchmark.LDBC.StoreFixture
  alias TripleStore.QuadIndex
  alias TripleStore.QuadOperations

  @rdf_type RDF.iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  @ontology "https://ldbcouncil.org/snb/ontology/"

  test "captures deterministic semantic counts and proves identity state in every index", %{
    test: test
  } do
    with_store(test, fn store ->
      graph = Mapping.graph_iri(:snb_bi, :updates)
      person = Mapping.entity_iri("Person", "probe-person")
      friend = Mapping.entity_iri("Person", "probe-friend")

      quads = [
        {person, @rdf_type, ontology("Person"), graph},
        {friend, @rdf_type, ontology("Person"), graph},
        {person, ontology("firstName"), RDF.literal("Probe"), graph},
        {person, ontology("knows"), friend, graph}
      ]

      assert {:ok, encoded} = Adapter.from_rdf_quads(store.dict_manager, quads)
      assert :ok = QuadOperations.insert_quads(store.db, encoded, [])

      observations = [
        cache_observer: fn -> %{generation: 3} end,
        statistics_observer: fn -> %{generation: 8, stale: false} end
      ]

      assert {:ok, first} = StateProbe.capture(store, observations)
      assert {:ok, second} = StateProbe.capture(store, observations)
      assert first.state_checksum == second.state_checksum

      assert first.counts == %{
               entities: 2,
               graphs: 1,
               properties: 1,
               relationships: 1,
               statements: 4
             }

      assert first.index_counts == %{gpos: 4, gspo: 4, posg: 4, spog: 4}
      assert first.facets.entities == %{"Person" => 2}
      assert first.facets.relationships == %{"knows" => 1}
      assert first.facets.properties == %{"firstName" => 1}
      assert first.facets.graphs == %{"https://ldbcouncil.org/snb/graph/snb_bi/updates" => 4}
      assert first.indices_consistent?

      assert :ok =
               StateProbe.verify(store, %{
                 counts: first.counts,
                 present: quads,
                 absent: [
                   {person, ontology("lastName"), RDF.literal("Removed"), graph}
                 ],
                 state_checksum: first.state_checksum
               })

      assert :ok = QuadOperations.delete_quads(store.db, encoded, [])

      assert :ok = StateProbe.verify(store, %{absent: quads})
    end)
  end

  test "detects stale cache and statistics observations across a committed state change", %{
    test: test
  } do
    with_store(test, fn store ->
      graph = Mapping.graph_iri(:snb_bi, :updates)
      quad = {Mapping.entity_iri("Person", "4"), @rdf_type, ontology("Person"), graph}
      observer = fn -> %{generation: 1} end
      opts = [cache_observer: observer, statistics_observer: observer]

      assert {:ok, before} = StateProbe.capture(store, opts)
      assert {:ok, [encoded]} = Adapter.from_rdf_quads(store.dict_manager, [quad])
      assert :ok = QuadOperations.insert_quad(store.db, encoded)
      assert {:ok, stale} = StateProbe.capture(store, opts)

      assert {:error, {:stale_post_update_observations, [:cache, :statistics]}} =
               StateProbe.verify_transition(before, stale)

      assert {:ok, fresh} =
               StateProbe.capture(store,
                 cache_observer: fn -> %{generation: 2} end,
                 statistics_observer: fn -> %{generation: 2} end
               )

      assert :ok = StateProbe.verify_transition(before, fresh)
    end)
  end

  test "derives affected reads from mapped predicates and compares every complete row" do
    graph = Mapping.graph_iri(:snb_bi, :updates)

    mutations = [
      %{
        operation: :insert,
        quads: [
          {Mapping.entity_iri("Person", "9"), ontology("knows"),
           Mapping.entity_iri("Person", "10"), graph}
        ]
      }
    ]

    definitions = [
      definition("read-knows", 1, "SELECT ?friend WHERE { ?person <#{@ontology}knows> ?friend }"),
      definition("read-likes", 2, "SELECT ?post WHERE { ?person <#{@ontology}likes> ?post }")
    ]

    expected = result([["9"], ["10"]])
    actual = result([["9"], ["10"]])

    assert {:ok, evidence} =
             Qualification.post_update_evidence(
               definitions,
               mutations,
               %{first_sequence: 11, last_sequence: 12, state_checksum: "sha256:state"},
               fn %{catalog_id: "read-knows"} -> {:ok, actual} end,
               fn %{catalog_id: "read-knows"} -> {:ok, expected} end
             )

    assert evidence.batch_sequence == %{first: 11, last: 12}
    assert evidence.state_checksum == "sha256:state"
    assert evidence.affected_operations == ["read-knows"]
    assert [%{comparison: %{status: :correct, comparison: :full}}] = evidence.answers
  end

  test "rejects a deletion that leaves a stale explicit index entry", %{test: test} do
    with_store(test, fn store ->
      graph = Mapping.graph_iri(:snb_bi, :updates)
      quad = {Mapping.entity_iri("Person", "partial"), @rdf_type, ontology("Person"), graph}

      assert {:ok, [{subject, predicate, object, graph_id} = encoded]} =
               Adapter.from_rdf_quads(store.dict_manager, [quad])

      assert :ok = QuadOperations.insert_quad(store.db, encoded)

      assert :ok =
               ErlangAdapter.delete(
                 store.db,
                 :gspo,
                 QuadIndex.gspo_key(graph_id, subject, predicate, object)
               )

      assert {:error, {:explicit_index_divergence, details}} = StateProbe.capture(store)
      assert details.gspo.missing == 1
      assert Enum.all?([:gpos, :spog, :posg], &(details[&1].missing == 0))
    end)
  end

  test "derives affected graph-extension reads from registered adapter vocabulary" do
    graph = Mapping.graph_iri(:snb_bi, :updates)

    mutations = [
      %{
        operation: :insert,
        quads: [
          {Mapping.entity_iri("Person", "9"), ontology("studyAt"),
           Mapping.entity_iri("Organisation", "4"), graph}
        ]
      }
    ]

    definitions = [
      %{
        id: "read-20-a",
        catalog_id: "ldbc/snb-bi/read-20@v1.0.3",
        number: 20,
        variant: "a",
        strategy: {:extension, "ldbc.snb.bi-weighted-tag-path.v1", __MODULE__}
      },
      %{
        id: "read-19-a",
        catalog_id: "ldbc/snb-bi/read-19@v1.0.3",
        number: 19,
        variant: "a",
        strategy: {:extension, "ldbc.snb.bi-weighted-interaction-path.v1", __MODULE__}
      }
    ]

    assert [%{number: 20}, %{number: 19}] = StateProbe.affected_reads(definitions, mutations)
  end

  test "rejects incomplete post-update answers with full row evidence" do
    mutations = [
      %{
        operation: :delete,
        quads: [
          {Mapping.entity_iri("Person", "9"), ontology("knows"),
           Mapping.entity_iri("Person", "10"), Mapping.graph_iri(:snb_bi, :updates)}
        ]
      }
    ]

    definitions = [
      definition("read-knows", 1, "SELECT ?friend WHERE { ?person <#{@ontology}knows> ?friend }")
    ]

    expected = result([["9"], ["10"]])
    actual = result([["9"]])

    assert {:error,
            {:post_update_answer_mismatch,
             %{comparison: %{status: :incorrect, category: :row_mismatch, comparison: :full}}}} =
             Qualification.post_update_evidence(
               definitions,
               mutations,
               %{first_sequence: 8, last_sequence: 8, state_checksum: "sha256:state"},
               fn _definition -> {:ok, actual} end,
               fn _definition -> {:ok, expected} end
             )
  end

  test "failed batches preserve state, answers, cache and statistics observations", %{test: test} do
    with_store(test, fn store ->
      probe_opts = [
        cache_observer: fn -> %{generation: 7, size: 1} end,
        statistics_observer: fn -> %{generation: 4, stale: false} end
      ]

      answer = result([["stable"]])
      assert {:ok, before} = StateProbe.capture(store, probe_opts)
      update_path = store.path <> ".failed-updates"
      on_exit(fn -> File.rm(update_path) end)

      assert {:ok, 1} =
               UpdateStream.write(update_path, [
                 %{
                   sequence: 1,
                   operation: :insert,
                   quads: [
                     {Mapping.entity_iri("Person", "failed"), @rdf_type, ontology("Person"),
                      Mapping.graph_iri(:snb_bi, :updates)}
                   ]
                 }
               ])

      assert {:error, {:atomic_update_failed, :injected}} =
               UpdateBatch.apply(store, update_path,
                 writer: fn _db, _mutations, _opts -> {:error, :injected} end
               )

      assert {:ok, after_failure} = StateProbe.capture(store, probe_opts)

      assert :ok =
               StateProbe.verify_preserved(before, after_failure, %{read: answer}, %{read: answer})

      changed = put_in(after_failure.observations.cache, %{generation: 8, size: 0})

      assert {:error, {:failed_batch_state_changed, [:cache]}} =
               StateProbe.verify_preserved(before, changed, %{read: answer}, %{read: answer})
    end)
  end

  test "versioned smoke probes match initial and committed fixture states", %{test: test} do
    root = tmp_path(test)
    on_exit(fn -> File.rm_rf!(root) end)

    assert {:ok, expected} = StateProbe.expectations()

    assert {:ok, manifest} =
             Converter.convert(:snb_bi_smoke, snb_fixture(), Path.join(root, "converted"))

    assert {:ok, fixture} = StoreFixture.setup(Path.join(root, "fixture"), manifest)
    assert :ok = StateProbe.verify(fixture.store, expected.checkpoints.initial)

    update = Enum.find(manifest.components, &(&1.role == :bi_update_batch))
    assert {:ok, fixture, receipt} = StoreFixture.apply_updates(fixture, update.path)

    assert receipt.first_sequence == 1
    assert receipt.last_sequence == 16
    assert receipt.state_checksum == expected.checkpoints.after_update_batch.state_checksum
    assert :ok = StateProbe.verify(fixture.store, expected.checkpoints.after_update_batch)
    assert :ok = StoreFixture.teardown(fixture, delete: true)
  end

  defp definition(id, number, sparql) do
    %{
      id: id,
      catalog_id: id,
      number: number,
      variant: "default",
      translation: %{sparql: sparql}
    }
  end

  defp result(rows) do
    %Result{
      columns: ["value"],
      types: [:string],
      rows: rows,
      ordering: %{mode: :ordered, keys: []}
    }
  end

  defp ontology(name), do: RDF.iri(@ontology <> name)

  defp snb_fixture do
    Path.join([
      to_string(:code.priv_dir(:triple_store)),
      "benchmarks",
      "ldbc",
      "fixtures",
      "snb-bi"
    ])
  end

  defp tmp_path(test) do
    Path.join(
      System.tmp_dir!(),
      "triple-store-bi-probe-fixture-#{test}-#{System.unique_integer([:positive])}"
    )
  end

  defp with_store(test, fun) do
    path =
      Path.join(
        System.tmp_dir!(),
        "triple-store-bi-probe-#{test}-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(path)
    {:ok, store} = TripleStore.open(path, schema: :quad)

    try do
      fun.(store)
    after
      TripleStore.close(store)
      File.rm_rf!(path)
    end
  end
end
