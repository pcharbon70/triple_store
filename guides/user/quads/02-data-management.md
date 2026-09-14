# Data Management: Quad Schema

~~~elixir
{:ok, store} = TripleStore.open("./quad_data", schema: :quad)
~~~

Quad keys use four atomic indices: `gspo`, `gpos`, `spog`, and `posg`. The
default graph ID is 0; dictionary-backed named graph IDs are nonzero.

## Insert data with graph identity

The facade's direct graph-aware mutation is SPARQL Update:

~~~elixir
{:ok, 1} =
  TripleStore.update(store, """
  INSERT DATA {
    GRAPH <http://example.org/source> {
      <http://example.org/alice> <http://example.org/name> "Alice"
    }
  }
  """)
~~~

`TripleStore.QuadOperations` is the expert CRUD surface. Its tuple order is
`{subject_id, predicate_id, object_id, graph_id}`.

~~~elixir
{:ok, graphs} = TripleStore.QuadOperations.list_graphs(store.db)

exists? =
  TripleStore.QuadOperations.graph_exists?(
    store.db,
    store.dict_manager,
    RDF.iri("http://example.org/source")
  )
~~~

Low-level `QuadOperations` mutations do not automatically invalidate query
result or plan caches.

## Dataset-preserving ingestion

Generic facade file and string loading are graph-oriented and extract the
default graph from N-Quads or TriG input. Selecting `schema: :quad` does not
change that behavior. Use dedicated loader entry points when all named graphs
must survive:

~~~elixir
{:ok, count} =
  TripleStore.Loader.load_nquads_file(
    store.db,
    store.dict_manager,
    "dataset.nq"
  )
~~~

`Loader.load_trig_file/4`, `load_nquads_string/4`, `load_trig_string/4`, and
`load_graph/4` with an `RDF.Dataset` provide corresponding expert paths.
Callers using them own result-cache invalidation.

## Dataset-preserving export

`TripleStore.export/3` is graph-oriented. Use `TripleStore.Exporter` for full
datasets or named-graph formats:

~~~elixir
{:ok, dataset} = TripleStore.Exporter.export_dataset(store.db)
{:ok, nquads} = TripleStore.Exporter.export_nquads_string(store.db)
~~~

For one graph and its recovery metadata, use `TripleStore.GraphBackup`.
