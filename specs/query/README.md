# Query Specs Index

## Purpose

`Query Specs Index` is the entry point for SPARQL and query-processing documentation in `TripleStore`.

It covers:

- native SPARQL parsing
- algebra construction and optimization
- iterator-based execution
- property paths, join strategies, and caches

## Control Plane

Primary ownership: **Query Plane**.

The parser surface also touches the **Native Adapter Plane**, but semantic authority remains in the Query Plane.

## Primary Query Components

- `TripleStore.SPARQL.Parser`
- `TripleStore.SPARQL.Parser.NIF`
- `TripleStore.SPARQL.Algebra`
- `TripleStore.SPARQL.Optimizer`
- `TripleStore.SPARQL.Executor`
- `TripleStore.SPARQL.Query`
- `TripleStore.SPARQL.UpdateExecutor`
- `TripleStore.SPARQL.PropertyPath`
- `TripleStore.SPARQL.PlanCache`
- `TripleStore.Query.Cache`
- `TripleStore.SPARQL.Leapfrog.*`

## Component Specs

- [sparql_execution_pipeline.md](sparql_execution_pipeline.md)
- [planning_and_cache.md](planning_and_cache.md)

## Current Codebase Notes

- `SPARQL.Query` supports explain, prepared-query, timeout, and optional result-cache paths today.
- `PlanCache` is part of the default runtime; `Query.Cache` is opt-in.
- Named graph operations remain limited by the store's single-graph architecture.

## Acceptance Criteria

| Acceptance ID | Criterion | Related Requirements | Related Scenarios |
|---|---|---|---|
| `AC-QRY-01` | SPARQL query and update text are parsed through a bounded native adapter and then compiled into Elixir-owned execution structures. | `REQ-QRY-*`, `REQ-CP-*` | `SCN-006`, `SCN-015` |
| `AC-QRY-02` | The optimizer can reorder or otherwise improve plans without changing query meaning. | `REQ-QRY-*` | `SCN-006` |
| `AC-QRY-03` | Query execution remains lazy and index-backed rather than requiring eager whole-result materialization. | `REQ-QRY-*`, `REQ-STO-*` | `SCN-005`, `SCN-006` |
| `AC-QRY-04` | Property paths and advanced join strategies remain bounded, typed execution paths. | `REQ-QRY-*`, `REQ-OBS-*` | `SCN-006`, `SCN-014` |
| `AC-QRY-05` | Query-plan caching does not survive invalidating writes incorrectly. | `REQ-QRY-*`, `REQ-TXN-*` | `SCN-007` |

## Canonical References

- [../architecture-overview.md](../architecture-overview.md)
- [../topology.md](../topology.md)
- [../contracts/query_execution_contract.md](../contracts/query_execution_contract.md)
