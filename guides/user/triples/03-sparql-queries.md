# SPARQL Queries: Triple Schema

The public query entry point is `TripleStore.query/3`.

~~~elixir
{:ok, rows} =
  TripleStore.query(store, """
  SELECT ?s ?o WHERE { ?s <http://example.org/name> ?o }
  ORDER BY ?s
  LIMIT 100
  """, timeout: 5_000)
~~~

The query pipeline parses through the Rust NIF, builds algebra, optionally
optimizes it, executes against dictionary IDs and indices, and serializes the
result. `SELECT` returns binding maps, `ASK` returns a boolean, and
`CONSTRUCT` and `DESCRIBE` return RDF graph results.

~~~elixir
{:ok, exists?} =
  TripleStore.query(store, """
  ASK { <http://example.org/alice> <http://example.org/name> ?name }
  """)

{:ok, graph} =
  TripleStore.query(store, """
  CONSTRUCT { ?s <http://example.org/label> ?name }
  WHERE { ?s <http://example.org/name> ?name }
  """)
~~~

## Options

- `timeout: milliseconds` defaults to 30,000 for eager queries.
- `optimize: false` bypasses optimization.
- `explain: true` returns `{:ok, {:explain, information}}`.
- `use_cache: true` opts into `TripleStore.Query.Cache`; the cache process must
  already be running. `cache_name:` selects a named cache.

~~~elixir
{:ok, {:explain, info}} = TripleStore.query(store, query, explain: true)
~~~

Prepared and streaming queries are expert APIs on `TripleStore.SPARQL.Query`.
Streaming supports `SELECT` only and has no timeout over later lazy consumption.

~~~elixir
ctx = %{db: store.db, dict_manager: store.dict_manager}
{:ok, prepared} = TripleStore.SPARQL.Query.prepare("SELECT ?s WHERE { ?s ?p ?o }")
{:ok, rows} = TripleStore.SPARQL.Query.execute(ctx, prepared, %{})
{:ok, stream} = TripleStore.SPARQL.Query.stream_query(ctx, "SELECT ?s WHERE { ?s ?p ?o }")
first_ten = Enum.take(stream, 10)
~~~
