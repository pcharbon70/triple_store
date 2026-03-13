# TripleStore Boundaries

## Purpose

This document defines the architectural boundaries for `TripleStore` and prevents semantic drift between Elixir-owned behavior, native adapters, persisted schema, and expert helper modules.

Primary recommendation:

- keep semantic authority in Elixir
- keep schema differences explicit
- keep native code and RocksDB bytes behind named adapter and data-plane boundaries

## Boundary Principles

1. `TripleStore` is the primary public API authority, but expert modules are still first-class documented boundaries and MUST NOT be treated as invisible internals.
2. Query semantics, update semantics, graph semantics, authorization semantics, and reasoning semantics remain Elixir-owned responsibilities.
3. Schema selection is a hard boundary: triple store and quad store persistence are different on-disk shapes with different indices and column families.
4. Native code is an adapter boundary for RocksDB and SPARQL parsing, not a second semantic control plane.
5. Persisted data remains explicit: dictionary surfaces, triple or quad indices, derived facts, provenance, numeric ranges, and ACL bytes are all first-class data-plane boundaries.
6. Coordination processes own runtime lifecycle and serialization concerns, but they do not own query or reasoning meaning.
7. Optional helpers such as metrics, result caches, statistics servers, and scheduled backups are real boundaries that callers may wire in, but they are not implicit baseline runtime guarantees.
8. Guides, notes, and benchmark harnesses may explain the system, but they do not override the canonical specs in this directory.

## Boundary Matrix

| Boundary | Classification | Canonical Ownership | Notes |
|---|---|---|---|
| `TripleStore` facade | Core | Public API Plane | Primary caller-facing API |
| Expert modules (`Update`, `GraphBackup`, `QuadOperations`, `Health`, `Authorization`) | Core | Varies by module | Narrower entry points; still documented public boundaries |
| Application supervisor and helper processes | Core | Coordination Plane | Own process lifecycle and optional support services |
| Dictionary, index, quad-index, loader, exporter, adapter | Core | Storage Plane | Own term encoding, pattern selection, graph ID handling, and batch fanout policy |
| SPARQL algebra, optimizer, executor, update executor, authorization | Core | Query Plane | Own query/update meaning and graph access policy |
| Reasoner modules, status, provenance, incremental maintenance | Core | Reasoning Plane | Own inference and derived-fact semantics |
| RocksDB adapter | Native adapter | Native Adapter Plane | Owns bounded DB capability only |
| Parser NIF | Native adapter | Native Adapter Plane | Owns parse capability only |
| RocksDB column families, snapshots, backup directories, graph backup files | Data plane | Data Plane | Canonical persisted bytes and serialized recovery artifacts |
| Embedding application | External | Embedding Plane | Chooses startup, paths, config, and optional service wiring |
| Benchmarks, guides, notes | External to runtime authority | n/a | Important documentation and validation tools, not semantic runtime owners |

## Split Gates

A concern should move out of the Elixir core only if all gates pass:

1. The behavior can be expressed through an explicit contract.
2. Extraction does not create a second semantic control plane.
3. The store schema and graph semantics remain explicit and reviewable.
4. Telemetry, error typing, and recovery behavior remain coherent.
5. The extracted piece can fail without redefining query, update, or reasoning meaning.
6. The public API and expert-module contracts remain stable for callers.

Default decision: if a boundary is unclear, keep semantic ownership in Elixir and use native code only as a bounded adapter.

## Current Boundary Decisions

- `TripleStore.Backend.RocksDB.ErlangAdapter` owns RocksDB open, close, iterator, snapshot, and batch primitives, but not storage semantics.
- `TripleStore.SPARQL.Parser.NIF` owns parsing capability only; AST compilation, optimization, and execution remain Elixir-owned.
- `TripleStore.Transaction` owns serialized SPARQL UPDATE coordination when used, but direct load, insert, delete, and some quad expert operations remain separate batch-write paths.
- Graph ACL policy lives in `TripleStore.SPARQL.Authorization`; the `acl` column family stores bytes, not policy meaning.
- Derivation provenance lives in `TripleStore.Reasoner.DerivationProvenance`; the `derivation_provenance` column family stores records, not inference semantics.
- Triple-to-quad migration is an export and import boundary, not an in-place upgrade boundary.
