# TripleStore

A high-performance RDF triple store implementation in Elixir with RocksDB storage, full SPARQL 1.1 support, and OWL 2 RL reasoning.

## Features

- **Persistent Storage**: RocksDB backend via Rustler NIFs with dictionary encoding and optimized triple indices (SPO, POS, OSP)
- **SPARQL 1.1**: Full query support including SELECT, CONSTRUCT, ASK, DESCRIBE, and UPDATE operations
- **OWL 2 RL Reasoning**: Forward-chaining materialization with semi-naive evaluation and incremental maintenance
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
│                    Rustler NIF Boundary                       │
├──────────────────────────────────────────────────────────────┤
│                      RocksDB Instance                         │
└──────────────────────────────────────────────────────────────┘
```

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

```elixir
# Open a store
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

## Requirements

- Elixir 1.14+
- Erlang/OTP 25+
- Rust toolchain (for NIF compilation)

## Development

```bash
# Fetch dependencies
mix deps.get

# Compile (includes NIF compilation)
mix compile

# Run tests
mix test

# Run Rust NIF tests
(cd native/rocksdb_nif && cargo test)

# Run both
mix test && (cd native/rocksdb_nif && cargo test)

# Run benchmarks
mix run bench/bsbm.exs
```

## License

Apache License 2.0 - see [LICENSE.md](LICENSE.md) for details.
