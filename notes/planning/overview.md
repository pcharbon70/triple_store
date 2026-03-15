# Triple Store Implementation Plan

## Executive Summary

This plan outlines the implementation of a high-performance RDF triple store in Elixir with RocksDB + Rustler as the storage layer, full SPARQL 1.1 support (including UPDATE), and OWL 2 RL reasoning with forward-chaining materialization. The implementation is divided into 5 phases, each building upon the previous to deliver a production-ready triple store.

## Phase Overview

| Phase | Focus | Key Deliverables |
|-------|-------|------------------|
| 1 | Storage Foundation | RocksDB NIFs, dictionary encoding, triple indices |
| 2 | SPARQL Query Engine | Parser NIF, algebra compiler, iterator execution |
| 3 | Advanced Query Processing | Leapfrog Triejoin, cost-based optimizer, SPARQL UPDATE |
| 4 | OWL 2 RL Reasoning | Rule compiler, semi-naive evaluation, incremental maintenance |
| 5 | Production Hardening | Benchmarks, tuning, telemetry, API finalization |

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Embedding Application                      │
├──────────────────────────────────────────────────────────────┤
│                    TripleStore Public API                     │
│         (query/1, insert/1, delete/1, materialize/1)         │
├───────────────┬──────────────────────┬───────────────────────┤
│ SPARQL Engine │   OWL 2 RL Reasoner  │   Transaction Mgr     │
│  (pure Elixir)│     (pure Elixir)    │    (GenServer)        │
├───────────────┴──────────────────────┴───────────────────────┤
│                    Index & Dictionary Layer                   │
│              (Elixir with NIF calls for I/O)                 │
├──────────────────────────────────────────────────────────────┤
│                    Rustler NIF Boundary                       │
├──────────────────────────────────────────────────────────────┤
│   ┌─────────┬─────────┬─────────┬──────────┬──────────┐      │
│   │  spo    │   pos   │   osp   │  id2str  │  str2id  │      │
│   │  (CF)   │  (CF)   │  (CF)   │   (CF)   │   (CF)   │      │
│   └─────────┴─────────┴─────────┴──────────┴──────────┘      │
│                      RocksDB Instance                         │
└──────────────────────────────────────────────────────────────┘
```

## File Structure

```
lib/
├── triple_store.ex                    # Public API facade
├── triple_store/
│   ├── application.ex                 # OTP application
│   ├── backend/
│   │   └── rocksdb.ex                # RocksDB NIF bindings
│   ├── dictionary.ex                  # Term encoding/decoding
│   ├── index.ex                       # Triple indices
│   ├── statistics.ex                  # Cardinality statistics
│   ├── rdf_adapter.ex                 # RDF.ex integration
│   ├── sparql/
│   │   ├── parser.ex                 # SPARQL parser NIF
│   │   ├── algebra.ex                # Algebra representation
│   │   ├── optimizer.ex              # Query optimization
│   │   ├── executor.ex               # Query execution
│   │   ├── update.ex                 # SPARQL UPDATE
│   │   └── property_path.ex          # Property path evaluation
│   ├── query/
│   │   ├── leapfrog_triejoin.ex      # Worst-case optimal join
│   │   ├── cost_optimizer.ex         # Cost-based optimization
│   │   ├── plan_cache.ex             # Plan caching
│   │   └── cache.ex                  # Result caching
│   ├── reasoner/
│   │   ├── rule_compiler.ex          # OWL 2 RL rules
│   │   ├── semi_naive.ex             # Fixpoint evaluation
│   │   ├── incremental.ex            # Incremental maintenance
│   │   └── tbox_cache.ex             # Hierarchy caching
│   ├── transaction.ex                 # Write coordination
│   ├── telemetry.ex                   # Telemetry events
│   ├── health_check.ex                # Health monitoring
│   ├── backup.ex                      # Backup/restore
│   ├── benchmark.ex                   # Benchmark suite
│   └── config/
│       └── rocksdb.ex                # RocksDB configuration

native/
├── rocksdb_nif/
│   ├── Cargo.toml
│   └── src/
│       └── lib.rs                    # RocksDB NIF implementation
└── sparql_parser_nif/
    ├── Cargo.toml
    └── src/
        └── lib.rs                    # SPARQL parser NIF

test/
├── triple_store/
│   ├── backend/
│   │   └── rocksdb_test.exs
│   ├── dictionary_test.exs
│   ├── index_test.exs
│   ├── sparql/
│   │   ├── parser_test.exs
│   │   ├── algebra_test.exs
│   │   ├── optimizer_test.exs
│   │   └── executor_test.exs
│   ├── query/
│   │   └── leapfrog_test.exs
│   ├── reasoner/
│   │   ├── rule_compiler_test.exs
│   │   └── semi_naive_test.exs
│   └── integration/
│       ├── phase_1_test.exs
│       ├── phase_2_test.exs
│       ├── phase_3_test.exs
│       ├── phase_4_test.exs
│       └── phase_5_test.exs
└── support/
    ├── fixtures/
    └── helpers.exs
```

## Dependencies

### Elixir (mix.exs)

```elixir
defp deps do
  [
    {:rdf, "~> 2.0"},
    {:rustler, "~> 0.30"},
    {:rustler_precompiled, "~> 0.7"},
    {:flow, "~> 1.2"},
    {:telemetry, "~> 1.2"},
    {:telemetry_metrics, "~> 0.6"},
    {:telemetry_poller, "~> 1.0"},
    {:stream_data, "~> 0.6", only: [:test, :dev]},
    {:benchee, "~> 1.1", only: :dev}
  ]
end
```

### Rust (Cargo.toml)

```toml
[dependencies]
rustler = "0.30"
rocksdb = "0.21"
spargebra = "0.2"
```

## Phase Documents

- [Phase 1: Storage Foundation](phase-01-storage-foundation.md)
- [Phase 2: SPARQL Query Engine](phase-02-sparql-query-engine.md)
- [Phase 3: Advanced Query Processing](phase-03-advanced-query-processing.md)
- [Phase 4: OWL 2 RL Reasoning](phase-04-owl2rl-reasoning.md)
- [Phase 5: Production Hardening](phase-05-production-hardening.md)
- [Phase 5 Companion: Dialyzer Remediation](phase-05-dialyzer-remediation.md)
