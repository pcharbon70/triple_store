# Getting Started

TripleStore is an embedded Elixir/OTP library. `TripleStore.open/2` creates or
opens a RocksDB database and starts a dictionary manager owned by the caller.
The returned handle contains `:db`, `:dict_manager`, `:transaction`, `:path`,
and `:schema`.

## Choose a persisted schema

| Schema | Indices | Key width | Graph behavior |
| --- | --- | --- | --- |
| `:triple` (default, v1) | `spo`, `pos`, `osp` | 24 bytes | One implicit graph |
| `:quad` (v2) | `gspo`, `gpos`, `spog`, `posg` | 32 bytes | Default graph ID 0 plus named graphs |

The schema is persisted. Opening an existing database with the other schema is
rejected. Moving data between schemas requires export and import into a new
store. Choose `:quad` when graph identity is part of the data model. No
repository benchmark establishes a fixed percentage performance difference.

## Build requirements

The project requires Elixir, Erlang/OTP, Rust, C/C++ build tools, CMake, and
RocksDB development libraries. Check `.tool-versions` and
`.github/workflows/ci.yml` for repository versions and Ubuntu packages.

```bash
mix deps.get
ERLANG_ROCKSDB_OPTS=-DCMAKE_POLICY_VERSION_MINIMUM=3.5 mix compile
```

## Triple-store example

```elixir
path = Path.join(System.tmp_dir!(), "triple_store_example")
{:ok, store} = TripleStore.open(path)

triple = {
  RDF.iri("http://example.org/alice"),
  RDF.iri("http://example.org/name"),
  RDF.literal("Alice")
}

{:ok, 1} = TripleStore.insert(store, triple)

{:ok, results} =
  TripleStore.query(store, """
  SELECT ?name WHERE {
    <http://example.org/alice> <http://example.org/name> ?name
  }
  """)

:ok = TripleStore.close(store)
```

`SELECT` returns a list of binding maps. Bound values use the tagged term
representation returned by the query layer.

## Quad-store example

```elixir
{:ok, store} = TripleStore.open("./quad_data", schema: :quad)

{:ok, 1} =
  TripleStore.update(store, """
  INSERT DATA {
    GRAPH <http://example.org/people> {
      <http://example.org/alice> <http://example.org/name> "Alice"
    }
  }
  """)

ctx = %{db: store.db, dict_manager: store.dict_manager}
:ok = TripleStore.SPARQL.Authorization.set_public(ctx, "http://example.org/people")

{:ok, results} =
  TripleStore.query(store, """
  SELECT ?name WHERE {
    GRAPH <http://example.org/people> {
      <http://example.org/alice> <http://example.org/name> ?name
    }
  }
  """)

:ok = TripleStore.close(store)
```

Named-graph reads are authorization-aware. The example marks the graph public;
without that ACL entry, the facade query returns `{:error, :unauthorized}` for
an existing named graph.

## RDF loading and export

The facade provides `load/3`, `load_graph/3`, `load_string/4`, and `export/3`.
Generic file/string loading and facade export are graph-oriented. In a quad
store, use `TripleStore.Loader` or `TripleStore.Exporter` dataset functions when
named-graph identities must survive a round trip.

## Errors and cleanup

Facade operations return tagged results. Exported bang variants raise
`TripleStore.Error` on failure. Always call `TripleStore.close/1`. Separately
started caches, metrics, statistics servers, and scheduled backups remain
caller-owned.

The schema is available as `store.schema`; the facade has no separate schema
accessor.
