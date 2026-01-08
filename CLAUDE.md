# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance RDF triple store implementation in Elixir with the following goals:
- Persistent storage using RocksDB via erlang-rocksdb (C++ NIF)
- Full SPARQL 1.1 support (including UPDATE)
- OWL 2 RL reasoning with forward-chaining materialization

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    TripleStore Public API                     │
├───────────────┬──────────────────────┬───────────────────────┤
│ SPARQL Engine │   OWL 2 RL Reasoner  │   Transaction Mgr     │
│  (pure Elixir)│     (pure Elixir)    │    (GenServer)        │
├───────────────┴──────────────────────┴───────────────────────┤
│                    Index & Dictionary Layer                   │
│              (Elixir with adapter for I/O)                   │
├──────────────────────────────────────────────────────────────┤
│         ErlangAdapter GenServer (manages CF handles)         │
├──────────────────────────────────────────────────────────────┤
│              erlang-rocksdb NIF (C++ binding)                │
├──────────────────────────────────────────────────────────────┤
│   Column Families: spo, pos, osp, id2str, str2id, derived    │
│                      RocksDB Instance                         │
└──────────────────────────────────────────────────────────────┘
```

### Storage Backend: erlang-rocksdb

The storage layer uses the `erlang-rocksdb` C++ NIF library via a GenServer-based adapter:

- **ErlangAdapter** (`TripleStore.Backend.RocksDB.ErlangAdapter`): GenServer that manages
  the database reference and column family handles
- **NIF** (`TripleStore.Backend.RocksDB.NIF`): Deprecated convenience wrapper that delegates
  to ErlangAdapter. Use ErlangAdapter directly for new code.
- **Fold operations**: The adapter supports `fold` and `fold_keys` for efficient iteration
  without explicit iterator management

### Column Families

The database uses multiple column families for optimized access patterns:

| CF | Purpose | Access Pattern |
|----|---------|----------------|
| `spo` | Subject-Predicate-Object index | Prefix scans |
| `pos` | Predicate-Object-Subject index | Prefix scans |
| `osp` | Object-Subject-Predicate index | Prefix scans |
| `id2str` | ID to string dictionary | Point lookups |
| `str2id` | String to ID dictionary | Point lookups |
| `derived` | Materialized reasoning results | Full scans |
| `numeric_range` | Numeric range index | Range queries |

### Storage Design

- **Dictionary encoding**: All URIs, blank nodes, and literals map to 64-bit integer IDs with type tagging
- **Triple indices**: SPO, POS, OSP indices (big-endian keys) provide O(log n) access for all pattern types
- **Inline encoding**: Numeric types (xsd:integer, xsd:decimal, xsd:dateTime) encoded directly without dictionary lookup

### Key Design Decisions

- **NIFs for**: RDF/SPARQL parsing (via sparql_parser_nif), RocksDB storage (via erlang-rocksdb)
- **Pure Elixir for**: query execution (must be preemptible), SPARQL algebra optimization, reasoning rule evaluation, transaction coordination
- **ErlangAdapter pattern**: GenServer manages database state and CF handle mapping for erlang-rocksdb
- **Fold operations**: Use `fold`/`fold_keys` for efficient iteration instead of explicit iterator loops
- **Leapfrog Triejoin**: Algorithm for complex BGP queries with 4+ patterns
- **Semi-naive evaluation**: Reasoning processes only delta from previous iteration

## Development Phases

The implementation follows five phases documented in `notes/research/development-overview.md`:
1. Storage Foundation (RocksDB via erlang-rocksdb, dictionary encoding, indices)
2. SPARQL Query Engine (parser NIF via sparql_parser_nif, algebra compiler, iterator execution)
3. Advanced Query Processing (Leapfrog Triejoin, cost-based optimizer, SPARQL UPDATE)
4. OWL 2 RL Reasoning (rule compiler, semi-naive evaluation, incremental maintenance)
5. Production Hardening (benchmarking, RocksDB tuning, telemetry)

## Dependencies

### System Requirements

- Elixir 1.18+
- Erlang/OTP 27+
- RocksDB C++ library (librocksdb-dev):
  - Ubuntu/Debian: `sudo apt-get install librocksdb-dev`
  - macOS: `brew install rocksdb`
  - Fedora/RHEL: `sudo dnf install rocksdb-devel`

### Elixir Dependencies

Key dependencies in `mix.exs`:
- `rdf` - RDF parsing and data structures
- `rocksdb` - erlang-rocksdb (C++ NIF for RocksDB)
- `rustler` - NIF compilation (for sparql_parser_nif only)
- `flow` - Concurrent processing for bulk loading
- `telemetry` - Metrics

### Native Dependencies

- **erlang-rocksdb**: C++ NIF library for RocksDB storage (handles all RocksDB operations)
- **sparql_parser_nif**: Rust NIF for SPARQL parsing (via rustler)

### Note on NIFs

This project uses two NIFs:
1. **erlang-rocksdb** (C++): For all RocksDB storage operations
2. **sparql_parser_nif** (Rust): For SPARQL query parsing

The old Rust RocksDB NIF (`native/rocksdb_nif/`) has been removed in favor of erlang-rocksdb.

## Build Commands

Once the project scaffolding is created:
```bash
mix deps.get          # Fetch dependencies
mix compile           # Compile (includes NIF compilation)
mix test              # Run tests
mix test path/to/test.exs:LINE  # Run single test
```
