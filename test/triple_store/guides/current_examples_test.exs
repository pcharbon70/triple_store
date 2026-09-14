defmodule TripleStore.Guides.CurrentExamplesTest do
  use ExUnit.Case, async: false

  alias TripleStore.Benchmark.Targets
  alias TripleStore.Config.RocksDB
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Query

  test "triple guide examples use the current facade and query result shapes" do
    with_store(:triple, fn store ->
      turtle = """
      @prefix ex: <http://example.org/> .
      ex:alice ex:name "Alice" .
      ex:alice ex:knows ex:bob .
      """

      assert {:ok, 2} = TripleStore.load_string(store, turtle, :turtle)

      assert {:ok, [%{"name" => {:literal, :simple, "Alice"}}]} =
               TripleStore.query(store, """
               SELECT ?name WHERE {
                 <http://example.org/alice> <http://example.org/name> ?name
               }
               """)

      triple =
        {RDF.iri("http://example.org/bob"), RDF.iri("http://example.org/name"),
         RDF.literal("Bob")}

      assert {:ok, 1} = TripleStore.insert(store, triple)
      assert {:ok, 1} = TripleStore.delete(store, triple)
      assert {:ok, %RDF.Graph{}} = TripleStore.export(store, :graph)
      assert {:ok, turtle_export} = TripleStore.export(store, {:string, :turtle})
      assert is_binary(turtle_export)
      assert {:ok, %{status: :healthy}} = TripleStore.health(store)
      assert {:ok, %{triple_count: 2}} = TripleStore.stats(store)

      context = %{db: store.db, dict_manager: store.dict_manager}
      assert {:ok, prepared} = Query.prepare("SELECT ?s WHERE { ?s ?p ?o }")
      assert {:ok, rows} = Query.execute(context, prepared, %{})
      assert length(rows) == 2
      assert {:ok, stream} = Query.stream_query(context, "SELECT ?s WHERE { ?s ?p ?o }")
      assert length(Enum.to_list(stream)) == 2
    end)
  end

  test "quad guide examples preserve graph identity through dedicated APIs" do
    with_store(:quad, fn store ->
      assert {:ok, 1} =
               TripleStore.update(store, """
               INSERT DATA {
                 GRAPH <http://example.org/people> {
                   <http://example.org/alice> <http://example.org/name> "Alice"
                 }
               }
               """)

      graph = RDF.iri("http://example.org/people")
      assert {:ok, [^graph]} = QuadOperations.list_graphs(store.db)
      assert QuadOperations.graph_exists?(store.db, store.dict_manager, graph)

      query = """
      SELECT ?name WHERE {
        GRAPH <http://example.org/people> {
          <http://example.org/alice> <http://example.org/name> ?name
        }
      }
      """

      assert {:error, :unauthorized} = TripleStore.query(store, query)

      context = %{db: store.db, dict_manager: store.dict_manager}

      assert :ok =
               TripleStore.SPARQL.Authorization.set_public(
                 context,
                 "http://example.org/people"
               )

      assert {:ok, [%{"name" => {:literal, :simple, "Alice"}}]} =
               TripleStore.query(store, query)

      assert {:ok, %RDF.Dataset{}} = TripleStore.Exporter.export_dataset(store.db)
      assert {:ok, nquads} = TripleStore.Exporter.export_nquads_string(store.db)
      assert nquads =~ "http://example.org/people"
    end)
  end

  test "configuration and benchmark examples use current function contracts" do
    config = RocksDB.preset(:production_low_memory)
    assert :ok = RocksDB.validate(config)
    assert is_binary(RocksDB.format_summary(config))
    assert is_integer(RocksDB.estimate_memory_usage(config))

    assert :pass = Targets.check_simple_query(p95_us: 5_000)
    assert :pass = Targets.check_complex_query(p95_us: 50_000)
    assert :pass = Targets.check_bulk_load(triples_per_sec: 125_000)
    assert :pass = Targets.check_query_mix(p95_us: 25_000)

    assert {:ok, %{passed: true} = report} = Targets.validate_bulk_load(125_000, 1_000)
    assert Targets.format_report(report) =~ "PASSED"
  end

  defp with_store(schema, fun) do
    path =
      Path.join(
        System.tmp_dir!(),
        "triple_store_guide_examples_#{schema}_#{System.unique_integer([:positive])}"
      )

    {:ok, store} = TripleStore.open(path, schema: schema)

    try do
      fun.(store)
    after
      TripleStore.close(store)
      File.rm_rf!(path)
    end
  end
end
