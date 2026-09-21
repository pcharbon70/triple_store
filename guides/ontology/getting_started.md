# Getting Started with Ontology Data

This is the shortest path to loading and querying an RDF graph in the default
triple schema. See the [full user guide](https://github.com/pcharbon70/triple_store/blob/main/guides/user/README.md) for quad datasets,
streaming, updates, backup, and operational APIs.

## Open, load, and query

Add the dependency version selected by your application, run `mix deps.get`,
and ensure the native build requirements in the project README are installed.

~~~elixir
path = Path.join(System.tmp_dir!(), "triple_store_example")
{:ok, store} = TripleStore.open(path)

data = """
@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice" .
ex:alice ex:knows ex:bob .
"""

{:ok, 2} = TripleStore.load_string(store, data, :turtle)

{:ok, result} =
  TripleStore.query(store, """
  SELECT ?name WHERE {
    <http://example.org/alice> <http://example.org/name> ?name
  }
  """)

:ok = TripleStore.close(store)
~~~

The SELECT result is a list of binding maps containing TripleStore's tagged RDF
term representation rather than `RDF.IRI` or `RDF.Literal` structs.

Use unique paths in tests and close the store before removing its directory.
Opening the same persisted path with `schema: :quad` is rejected.

## Mutate and export

~~~elixir
triple = {
  RDF.iri("http://example.org/bob"),
  RDF.iri("http://example.org/name"),
  RDF.literal("Bob")
}

{:ok, 1} = TripleStore.insert(store, triple)
{:ok, 1} = TripleStore.delete(store, triple)

{:ok, graph} = TripleStore.export(store, :graph)
{:ok, turtle} = TripleStore.export(store, {:string, :turtle})
~~~

`insert/2`, `delete/2`, and loads are direct storage writes. SPARQL UPDATE uses
`TripleStore.update/2` and the coordinator created with the store. These paths
do not share a single global lock.

## Reasoning

~~~elixir
{:ok, stats} = TripleStore.materialize(store, profile: :rdfs)
{:ok, status} = TripleStore.reasoning_status(store)
~~~

The default local facade materialization computes an in-memory closure and
returns statistics, but discards the returned facts. Use the reasoner and
derived-store APIs appropriate to your persistence and graph-scope requirements
when queries must include maintained inferred facts.
