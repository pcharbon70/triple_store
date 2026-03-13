# Runtime Specs Index

## Purpose

`Runtime Specs Index` is the entry point for public API and coordination-surface documentation in `TripleStore`.

It covers:

- the `TripleStore` public API facade
- OTP application services
- store-local coordination processes such as dictionary managers and transaction coordinators
- lifecycle expectations for open, close, read, and write flows

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

- Store handles default `transaction` to `nil`; update paths create a temporary coordinator when needed.
- The default application runtime starts plan cache and snapshot support, not every optional helper.
- Statistics support is currently split between integrated-but-deprecated `Statistics.Cache` and newer `Statistics.Server`.

## Acceptance Criteria

| Acceptance ID | Criterion | Related Requirements | Related Scenarios |
|---|---|---|---|
| `AC-RT-01` | `TripleStore.open/2` validates the path, opens RocksDB, and returns a store handle with the required runtime references. | `REQ-CP-*`, `REQ-TXN-*` | `SCN-002` |
| `AC-RT-02` | Runtime coordination preserves single-writer update semantics even when the caller did not pre-start a transaction coordinator. | `REQ-TXN-*` | `SCN-008` |
| `AC-RT-03` | Store-local manager processes are created and released through the public lifecycle without leaking semantic authority into callers. | `REQ-CP-*`, `REQ-TXN-*` | `SCN-001`, `SCN-002` |
| `AC-RT-04` | Runtime surfaces return tagged errors rather than partial success under lifecycle or coordination failure. | `REQ-TXN-*`, `REQ-OBS-*` | `SCN-002`, `SCN-008`, `SCN-014` |
| `AC-RT-05` | Runtime coordination changes keep plan-cache and statistics assumptions coherent after mutations. | `REQ-QRY-*`, `REQ-TXN-*` | `SCN-007` |

## Canonical References

- [../architecture-overview.md](../architecture-overview.md)
- [../topology.md](../topology.md)
- [../control-planes.md](../control-planes.md)
