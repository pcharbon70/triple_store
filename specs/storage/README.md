# Storage Specs Index

## Purpose

`Storage Specs Index` is the entry point for persistent storage documentation in `TripleStore`.

It covers:

- dictionary encoding and ID policy
- explicit and derived triple storage
- RocksDB column families and native I/O
- bulk loading, export, backup, and restore boundaries

## Control Plane

Mixed ownership:

- **Coordination Plane** for Elixir-owned encoding and fanout policy
- **Native Adapter Plane** for RocksDB execution surfaces
- **Data Plane** for canonical persisted bytes

## Primary Storage Components

- `TripleStore.Dictionary`
- `TripleStore.Dictionary.Manager`
- `TripleStore.Dictionary.ShardedManager`
- `TripleStore.Index`
- `TripleStore.Adapter`
- `TripleStore.Loader`
- `TripleStore.Exporter`
- `TripleStore.Backend.RocksDB.NIF`
- RocksDB column families: `id2str`, `str2id`, `spo`, `pos`, `osp`, `derived`

## Component Specs

- [dictionary_and_index_layer.md](dictionary_and_index_layer.md)
- [native_backend_and_rdf_io.md](native_backend_and_rdf_io.md)

## Current Codebase Notes

- Statistics persistence currently reuses a reserved key in `id2str`.
- RDF I/O remains triple-oriented even when file formats can encode quads.
- Named graph support is still a documented limitation in the current code.

## Acceptance Criteria

| Acceptance ID | Criterion | Related Requirements | Related Scenarios |
|---|---|---|---|
| `AC-STO-01` | RDF terms encode into tagged 64-bit IDs with no type-space collision between dictionary and inline encodings. | `REQ-STO-*` | `SCN-003` |
| `AC-STO-02` | Explicit triple mutation fans out atomically across `spo`, `pos`, and `osp`. | `REQ-STO-*`, `REQ-TXN-*` | `SCN-004`, `SCN-008` |
| `AC-STO-03` | Lookup paths follow the canonical triple-pattern-to-index selection rules. | `REQ-STO-*`, `REQ-QRY-*` | `SCN-005` |
| `AC-STO-04` | Inferred triples remain stored separately from explicit triples. | `REQ-STO-*`, `REQ-RSN-*` | `SCN-010` |
| `AC-STO-05` | Backup and restore operate on canonical store bytes with path-safety and recoverability guarantees. | `REQ-STO-*`, `REQ-OBS-*` | `SCN-012` |

## Canonical References

- [../architecture-overview.md](../architecture-overview.md)
- [../topology.md](../topology.md)
- [../boundaries.md](../boundaries.md)
