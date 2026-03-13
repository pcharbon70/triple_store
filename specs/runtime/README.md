# Runtime Specs Index

## Purpose

`Runtime Specs Index` is the entry point for the caller-facing and coordination
surfaces implemented by `TripleStore`.

It covers:

- the `TripleStore` facade and expert runtime modules
- OTP application services
- store-local coordination processes such as dictionary managers and transaction coordinators
- lifecycle expectations for open, close, read, write, and helper-process wiring

## Control Plane

Primary control-plane ownership: **Public API Plane** and **Coordination Plane**.

## Primary Runtime Components

- `TripleStore`
- `TripleStore.Update`
- `TripleStore.Application`
- `TripleStore.Transaction`
- `TripleStore.Dictionary.Manager`
- `TripleStore.Dictionary.ShardedManager`
- `TripleStore.Snapshot`
- `TripleStore.Statistics.Cache`
- `TripleStore.Statistics.Server`

## Component Specs

- [public_api_and_store_lifecycle.md](public_api_and_store_lifecycle.md)
- [application_and_support_services.md](application_and_support_services.md)

## Current Codebase Notes

- Store handles default `transaction` to `nil` and also carry `schema` at runtime.
- `TripleStore.open/2` chooses triple schema or quad schema and starts dictionary coordination for that store only.
- `TripleStore.update/2` creates a temporary transaction coordinator when the handle does not already include one.
- `TripleStore.insert/2`, `delete/2`, and load paths are direct storage-batch flows and do not route through `Transaction`.
- `TripleStore.query/3` builds a direct SPARQL execution context and does not currently surface actor context or transaction-query snapshots.
- `TripleStore.load_graph/3` delegates to `Loader.load_graph/4`, whose implementation accepts both `RDF.Graph` and `RDF.Dataset`; some public type docs still lag this runtime behavior.
- The default application runtime starts only `SPARQL.PlanCache` and `Snapshot`; statistics, metrics, result caches, and scheduling helpers are optional.

## Acceptance Criteria

| Acceptance ID | Criterion | Related Requirements | Related Scenarios |
|---|---|---|---|
| `AC-RT-01` | `TripleStore.open/2` validates the path, opens RocksDB with an explicit schema, and returns a store handle with the required runtime references. | `REQ-CP-*`, `REQ-STO-*` | `SCN-002` |
| `AC-RT-02` | `TripleStore.update/2` preserves SPARQL update coordination semantics by using a managed or temporary transaction coordinator. | `REQ-TXN-*` | `SCN-008` |
| `AC-RT-03` | Direct load, insert, and delete flows remain explicit batch-mutation paths and MUST NOT be misdocumented as transaction-query snapshot flows. | `REQ-STO-*`, `REQ-TXN-*` | `SCN-004`, `SCN-008`, `SCN-016` |
| `AC-RT-04` | Store-local manager processes are created and released through the public lifecycle without leaking semantic ownership into callers. | `REQ-CP-*`, `REQ-TXN-*` | `SCN-001`, `SCN-002` |
| `AC-RT-05` | Runtime surfaces return tagged results and preserve explicit optional-helper behavior rather than assuming caches, metrics, or stats helpers always exist. | `REQ-OBS-*`, `REQ-CP-*` | `SCN-007`, `SCN-013` |

## Canonical References

- [../architecture-overview.md](../architecture-overview.md)
- [../topology.md](../topology.md)
- [../control-planes.md](../control-planes.md)
