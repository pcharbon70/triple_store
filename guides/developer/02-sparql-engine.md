# SPARQL Engine

`TripleStore.SPARQL.Query` is the supported query entry point. It coordinates
parsing, algebra translation, optimization, execution, result shaping, timeout
isolation for eager requests, and optional result caching.

## Parse and execute

~~~elixir
alias TripleStore.SPARQL.Query

{:ok, prepared} =
  Query.prepare("SELECT ?s WHERE { ?s <http://example.org/p> ?o }")

{:ok, result} = Query.execute(store, prepared)
~~~

`TripleStore.query/3` delegates to this pipeline. SELECT returns a list of
binding maps; ASK returns a boolean; CONSTRUCT and DESCRIBE produce RDF graphs.
Bound SELECT values use the engine's tagged RDF-term representation.

The Rust `sparql_parser_nif` uses `spargebra` and dirty CPU scheduling to parse
query and update text. `Parser` converts the native representation at the
Rust/Elixir boundary. `Algebra` represents BGPs, joins, filters, graph patterns,
paths, projection, grouping, ordering, and solution modifiers. The Elixir
executor owns RDF evaluation semantics.

## Options and streaming

Eager query options include `:timeout`, `:optimize`, `:stats`, `:explain`,
`:log`, `:use_cache`, and `:cache_name`. Unsupported or invalid queries return
tagged errors.

~~~elixir
{:ok, stream} =
  Query.stream_query(store, "SELECT ?s WHERE { ?s ?p ?o }",
    optimize: true,
    stats: true
  )

rows = Enum.take(stream, 10)
~~~

Streaming is SELECT-only and defers work until enumeration. The eager timeout
does not cover later stream consumption, so callers must enforce any
consumption deadline and always allow stream resources to close.

## Graph and authorization context

GRAPH clauses compile to quad patterns. The lower-level execution context can
carry an actor for named-graph checks. The facade does not expose actor-aware
query options and therefore queries as `:public`; an existing non-public named
graph returns `:unauthorized`. ACL-governed quad queries bypass the optional
result cache because authorization lacks a stable revision identity.

Variable-graph execution enumerates candidate graphs with a configured bound.
Review `Executor` and authorization tests when changing GRAPH semantics.

## Caches

`SPARQL.PlanCache` is application-supervised and stores normalized query plans.
`TripleStore.Query.Cache` is an optional result cache. `SPARQL.QueryCache` is a
separate implementation used by its own callers and tests. Do not treat these
modules as interchangeable.
