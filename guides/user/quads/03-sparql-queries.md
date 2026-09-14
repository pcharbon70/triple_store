# SPARQL Queries: Quad Schema

Use a `GRAPH` clause to select a named graph. A pattern without `GRAPH` reads
the default graph. The following public-facade examples require the graph ACLs
to be public:

~~~elixir
ctx = %{db: store.db, dict_manager: store.dict_manager}
:ok = TripleStore.SPARQL.Authorization.set_public(ctx, "http://example.org/people")

{:ok, rows} =
  TripleStore.query(store, """
  SELECT ?name WHERE {
    GRAPH <http://example.org/people> {
      ?person <http://example.org/name> ?name
    }
  }
  """)
~~~

A graph variable enumerates named graphs and binds the graph term:

~~~elixir
{:ok, rows} =
  TripleStore.query(store, """
  SELECT ?g ?s WHERE {
    GRAPH ?g { ?s <http://example.org/name> ?name }
  }
  LIMIT 100
  """)
~~~

The optimizer can select quad Leapfrog execution for some basic graph patterns.
The v0.1.0 full-suite baseline contains known binding and iterator failures in
parts of that path; use focused tests for production query shapes.

## Authorization boundary

The public `TripleStore.query/3` facade builds a context containing only the DB
and dictionary manager, so it queries named graphs as `:public`. An existing
non-public named graph returns `{:error, :unauthorized}`. Actor-aware graph
authorization requires the lower-level API:

~~~elixir
ctx = %{db: store.db, dict_manager: store.dict_manager, user: actor}
:ok =
  TripleStore.SPARQL.Authorization.grant(
    ctx,
    "http://example.org/people",
    actor.id,
    :read
  )

{:ok, rows} = TripleStore.SPARQL.Query.query(ctx, sparql)
~~~

ACL-governed quad contexts bypass materialized-result caching because ACL state
has no stable authorization revision. An explicitly privileged
`permit_all: true` context may use it. Store identity is always part of a
production result-cache key.

Eager query timeout, explain mode, prepared queries, and SELECT-only streaming
behave as described in the [triple query guide](../triples/03-sparql-queries.md).
Lazy stream consumption has no end-to-end timeout.
