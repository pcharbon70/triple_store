# TripleStore Boundaries

## Purpose

This document defines the architectural boundaries for `TripleStore` and prevents semantic drift between Elixir-owned behavior, native adapters, and persisted data.

Primary recommendation:

- keep semantic authority in Elixir
- use native code for bounded parser and storage capabilities
- keep the on-disk RocksDB layout explicit and reviewable

## Boundary Principles

1. `TripleStore` is the only public API authority for store lifecycle and high-level behavior.
2. Query optimization, execution semantics, transaction coordination, and reasoning remain Elixir-owned responsibilities.
3. Native code is an adapter boundary for RocksDB and SPARQL parsing, not a second semantic control plane.
4. The persisted data model is explicit: dictionary surfaces, explicit triple indices, and derived-triple storage are all first-class boundaries.
5. Operations such as health, telemetry, and backup must describe the same runtime/data model as the core engine.
6. Existing planning and research notes may inform the architecture, but they do not override the canonical specs in this directory.

## Boundary Matrix

| Boundary | Classification | Canonical Ownership | Notes |
|---|---|---|---|
| Public library facade | Core | `TripleStore` | Owns user-facing contracts and tagged-result semantics |
| OTP application services | Core | `TripleStore.Application` and supervised Elixir processes | Owns runtime coordination support, not persisted facts |
| Dictionary and index logic | Core | Elixir modules over RocksDB NIF calls | Owns ID policy, index fanout policy, and lookup strategy |
| SPARQL algebra, optimization, execution | Core | Elixir query modules | Owns semantic correctness and plan selection |
| Transaction and snapshot coordination | Core | `TripleStore.Transaction` and supporting runtime services | Owns single-writer and read-consistency semantics |
| Reasoning engine | Core | `TripleStore.Reasoner.*` | Owns rule application, fixpoint semantics, and derived-fact policy |
| RocksDB access | Native adapter | `TripleStore.Backend.RocksDB.NIF` | Owns bounded storage capability, not higher-level semantics |
| SPARQL parsing | Native adapter | `TripleStore.SPARQL.Parser.NIF` | Owns parsing capability only |
| RocksDB column families and files | Data plane | On-disk database | Owns canonical bytes, not semantic interpretation |
| Backup directories and restore inputs | Data plane | Filesystem operations over canonical bytes | Operationally important extension of the persisted state |
| Embedding application | External | Caller application | Chooses deployment, process wiring, and invocation patterns |

## Split Gates

A concern should move out of the Elixir core only if all gates pass:

1. The behavior can be expressed through an explicit contract.
2. Extraction does not create a second semantic control plane.
3. Parser/storage performance benefits are material.
4. Telemetry, error typing, and recovery semantics remain coherent.
5. The extracted piece can fail without redefining query or reasoning semantics.
6. The public API contract remains stable for callers.

Default decision: if a boundary is unclear, keep semantic ownership in Elixir and use native code only as a bounded adapter.

## Current Boundary Decisions

- RocksDB stays behind `TripleStore.Backend.RocksDB.NIF`.
- SPARQL parsing stays behind `TripleStore.SPARQL.Parser.NIF`.
- Query execution does not move into the parser NIF.
- Reasoning does not write inferred facts into the explicit index surfaces.
- Backup and restore operate on canonical data-plane bytes but remain governed by Elixir-side safety checks.
