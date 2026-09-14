# Data Management: Triple Schema

Open the v1 triple schema explicitly or rely on the default:

~~~elixir
{:ok, store} = TripleStore.open("./triple_data", schema: :triple)
~~~

## Load RDF

`load/3` detects a format from the extension unless `:format` is supplied.
`load_graph/3` accepts an `RDF.Graph`, and `load_string/4` takes an explicit
format. The loader's default batch size is 10,000.

~~~elixir
{:ok, file_count} = TripleStore.load(store, "data.ttl", batch_size: 1_000)

ttl = """
@prefix ex: <http://example.org/> .
ex:alice ex:name "Alice" .
"""

{:ok, string_count} = TripleStore.load_string(store, ttl, :turtle)
~~~

Optional RDF/XML and JSON-LD parsing depends on the corresponding RDF.ex decoder
modules being available. Errors are returned as tagged tuples.

## Insert and delete RDF terms

`insert/2` and `delete/2` accept one triple, a list of triples, an
`RDF.Description`, or an `RDF.Graph`.

~~~elixir
triple = {
  RDF.iri("http://example.org/alice"),
  RDF.iri("http://example.org/name"),
  RDF.literal("Alice")
}

{:ok, 1} = TripleStore.insert(store, triple)
{:ok, 1} = TripleStore.delete(store, triple)
~~~

These direct facade mutations write storage batches without using
`TripleStore.Transaction`. Successful changes invalidate materialized result
caches for the open store, but direct writers do not share a store-wide lock
with independently created transaction coordinators.

## Export

~~~elixir
{:ok, graph} = TripleStore.export(store, :graph)
{:ok, turtle} = TripleStore.export(store, {:string, :turtle})
{:ok, count} = TripleStore.export(store, {:file, "export.nt", :ntriples})
~~~

The optional `:pattern` uses internal bound IDs, so ordinary callers should
usually export the full graph.

## Backup and restore

~~~elixir
{:ok, metadata} = TripleStore.backup(store, "/backups/triple-store")
:ok = TripleStore.close(store)
{:ok, restored} = TripleStore.restore("/backups/triple-store", "./restored")
~~~

Follow the
[production checklist](../../../docs/production/pre-production-checklist.md)
and [migration runbook](../../../docs/production/migration-runbook.md).
