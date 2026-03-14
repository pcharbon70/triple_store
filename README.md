# TripleStore

A high-performance RDF store implementation in Elixir with RocksDB storage, full SPARQL 1.1 support, and OWL 2 RL reasoning. Supports both triple and quad (named graph) storage schemas.

## Features

- **Persistent Storage**: RocksDB backend via erlang-rocksdb with dictionary encoding and optimized indices
  - Triple store (v1): SPO, POS, OSP indices
  - Quad store (v2): GSPO, GPOS, SPOG, POSG indices with named graphs
- **Named Graphs**: Quad store support for data isolation, multi-tenancy, and provenance tracking
- **SPARQL 1.1**: Full query support including SELECT, CONSTRUCT, ASK, DESCRIBE, UPDATE, and GRAPH clauses
- **OWL 2 RL Reasoning**: Forward-chaining materialization with semi-naive evaluation and graph-scoped reasoning
- **Query Optimization**: Cost-based optimizer with Leapfrog Triejoin for complex BGP queries

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    TripleStore Public API                     │
├───────────────┬──────────────────────┬───────────────────────┤
│ SPARQL Engine │   OWL 2 RL Reasoner  │   Transaction Mgr     │
├───────────────┴──────────────────────┴───────────────────────┤
│                    Index & Dictionary Layer                   │
├──────────────────────────────────────────────────────────────┤
│                    Erlang-RocksDB Adapter                     │
├──────────────────────────────────────────────────────────────┤
│                      RocksDB Instance                         │
└──────────────────────────────────────────────────────────────┘
```

## Specifications

Canonical architecture and governance specs now live under [`specs/`](specs/README.md).

Key entry points:

- [`specs/architecture-overview.md`](specs/architecture-overview.md)
- [`specs/topology.md`](specs/topology.md)
- [`specs/boundaries.md`](specs/boundaries.md)
- [`specs/control-planes.md`](specs/control-planes.md)

## Installation

Add `triple_store` to your dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:triple_store, "~> 0.1.0"}
  ]
end
```

## Usage

### Triple Store (Default)

```elixir
# Open a triple store
{:ok, store} = TripleStore.open("./data")

# Load RDF data
TripleStore.load(store, "ontology.ttl")

# Query with SPARQL
results = TripleStore.query(store, """
  SELECT ?s ?name
  WHERE {
    ?s a foaf:Person .
    ?s foaf:name ?name .
  }
""")

# Enable reasoning
TripleStore.materialize(store, profile: :owl2rl)

# Clean up
TripleStore.close(store)
```

### Quad Store (Named Graphs)

```elixir
# Open a quad store for named graphs
{:ok, store} = TripleStore.open("./data", schema: :quad)

# Load data into named graphs
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:source1 {
      ex:alice a foaf:Person ;
               foaf:name "Alice" .
    }
  }
""")

# Query with GRAPH clause
results = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?g ?name
  WHERE {
    GRAPH ?g {
      ?s foaf:name ?name .
    }
  }
""")
```

See [Named Graphs](guides/user/07-named-graphs.md) for more details on quad store usage.

## Requirements

- Elixir 1.18+
- Erlang/OTP 27+
- Rust toolchain (for the SPARQL parser NIF)
- RocksDB C++ library:
  - Ubuntu/Debian: `sudo apt-get install librocksdb-dev`
  - macOS: `brew install rocksdb`
  - Fedora/RHEL: `sudo dnf install rocksdb-devel`

## Development

```bash
# Fetch dependencies
mix deps.get

# Compile
mix compile

# Run tests
mix test

# Run benchmarks
mix run bench/bsbm.exs
```

`mix compile` builds the SPARQL parser NIF from `native/sparql_parser_nif` into `priv/native/`.
Those native artifacts are generated locally and SHOULD NOT be committed.
If `sparql_parser_nif` fails to load after switching branches or toolchains, remove
`priv/native/sparql_parser_nif.so` and rerun `mix compile --force`.

## License

Apache License 2.0 - see [LICENSE.md](LICENSE.md) for details.
