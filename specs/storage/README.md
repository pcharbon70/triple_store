# Storage Specs Index

## Purpose

`Storage Specs Index` is the entry point for persistent storage documentation in
`TripleStore`.

It covers:

- dictionary encoding and ID policy
- explicit and derived storage for both triple and quad schemas
- RocksDB column families and native I/O
- bulk loading, export, backup, graph backup, and restore boundaries

## Control Plane

Mixed ownership:

- **Storage Plane** for Elixir-owned encoding, graph handling, and index fanout policy
- **Native Adapter Plane** for RocksDB execution surfaces
- **Data Plane** for canonical persisted bytes

## Primary Storage Components

- `TripleStore.Dictionary`
- `TripleStore.Dictionary.Manager`
- `TripleStore.Dictionary.ShardedManager`
- `TripleStore.Index`
- `TripleStore.QuadIndex`
- `TripleStore.QuadOperations`
- `TripleStore.Adapter`
- `TripleStore.Loader`
- `TripleStore.Exporter`
- `TripleStore.Backend.RocksDB.ErlangAdapter`
- `TripleStore.Config` and `TripleStore.Config.*`
- RocksDB column families for triple schema and quad schema

## Component Specs

- [dictionary_and_index_layer.md](dictionary_and_index_layer.md)
- [native_backend_and_rdf_io.md](native_backend_and_rdf_io.md)

## Current Codebase Notes

- The store has two persisted schemas today: triple schema v1 and quad schema v2.
- Triple schema persists `id2str`, `str2id`, `spo`, `pos`, `osp`, `derived`, and `numeric_range`.
- Quad schema persists `id2str`, `str2id`, `gspo`, `gpos`, `spog`, `posg`, `derived`, `derivation_provenance`, `numeric_range`, and `acl`.
- Statistics persistence currently reuses reserved keys in `id2str` rather than a dedicated statistics column family.
- Generic `load/3`, `load_string/4`, and `export/3` remain graph-oriented facades, while `Loader`, `Exporter`, and `GraphBackup` expose richer quad-, dataset-, and named-graph workflows.
- In-place migration from triple schema to quad schema is not supported; export and import is the current migration path.

## Acceptance Criteria

| Acceptance ID | Criterion | Related Requirements | Related Scenarios |
|---|---|---|---|
| `AC-STO-01` | RDF terms encode into tagged 64-bit IDs with no type-space collision between dictionary and inline encodings. | `REQ-STO-*` | `SCN-003` |
| `AC-STO-02` | Explicit mutation fans out atomically across the schema-appropriate index set: `spo`/`pos`/`osp` for triple stores and `gspo`/`gpos`/`spog`/`posg` for quad stores. | `REQ-STO-*`, `REQ-TXN-*` | `SCN-004`, `SCN-008` |
| `AC-STO-03` | Lookup paths follow the canonical pattern-to-index selection rules for both triple and quad access paths. | `REQ-STO-*`, `REQ-QRY-*` | `SCN-005` |
| `AC-STO-04` | Derived facts, provenance, graph IDs, and ACL bytes remain explicit persistence surfaces rather than undocumented side channels. | `REQ-STO-*`, `REQ-RSN-*`, `REQ-QRY-*` | `SCN-010`, `SCN-017` |
| `AC-STO-05` | Loader, exporter, backup, and graph-backup flows preserve the current schema-aware RDF I/O and recovery boundaries. | `REQ-STO-*`, `REQ-OBS-*` | `SCN-012`, `SCN-016` |

## Canonical References

- [../architecture-overview.md](../architecture-overview.md)
- [../topology.md](../topology.md)
- [../boundaries.md](../boundaries.md)
