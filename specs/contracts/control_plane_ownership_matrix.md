# TripleStore Control Plane Ownership Matrix

This matrix is the canonical authority for assigning control-plane ownership within the `specs/` system.

## Requirement Set

- `REQ-CP-001`: Every baseline or area spec MUST identify its primary control plane.
- `REQ-CP-002`: Public API behavior MUST map to the Public API Plane, not to the native or data planes.
- `REQ-CP-003`: Native adapters MUST be documented as bounded execution surfaces, not semantic authorities.
- `REQ-CP-004`: Data-plane artifacts MUST be described as canonical bytes owned by higher semantic planes.
- `REQ-CP-005`: Control-plane conflicts MUST be resolved by this matrix first, then by ADR-0001.

## Ownership Matrix

| Area | Canonical Plane | Notes |
|---|---|---|
| `specs/architecture-overview.md` | Public API Plane + Coordination Plane | Baseline system shape |
| `specs/topology.md` | Public API Plane + Coordination Plane | Runtime composition and flow |
| `specs/boundaries.md` | Mixed | Boundary policy across core/native/data/external surfaces |
| `specs/control-planes.md` | Mixed | Plane definitions only |
| `specs/runtime/README.md` | Public API Plane + Coordination Plane | Store lifecycle and runtime coordination |
| `specs/storage/README.md` | Coordination Plane + Native Adapter Plane + Data Plane | Encoding and persisted layout |
| `specs/query/README.md` | Query Plane + Native Adapter Plane | Query semantics and parser boundary |
| `specs/reasoning/README.md` | Reasoning Plane | Inference and derived-fact lifecycle |
| `TripleStore` public functions | Public API Plane | Canonical external surface |
| `TripleStore.Application` and runtime support processes | Coordination Plane | Supervises shared services and dynamic helpers |
| `TripleStore.Backend.RocksDB.NIF` | Native Adapter Plane | Bounded RocksDB execution surface |
| `TripleStore.SPARQL.Parser.NIF` | Native Adapter Plane | Bounded parser execution surface |
| RocksDB column families and backup directories | Data Plane | Canonical bytes on disk |
| Telemetry, health, backup, restore, Prometheus | Operations Plane | Operational visibility and recovery |

## ADR Reference

- [../adr/ADR-0001-control-plane-authority.md](../adr/ADR-0001-control-plane-authority.md)
