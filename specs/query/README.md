# Query Specs Index

## Purpose

`Query Specs Index` is the entry point for SPARQL and query-processing documentation in `TripleStore`.

It covers:

- native SPARQL parsing
- algebra construction and optimization
- triple and quad execution
- property paths, join strategies, authorization, and caches

## Control Plane

Primary ownership: **Query Plane**.

The parser surface also touches the **Native Adapter Plane**, but semantic authority remains in the Query Plane.

## Primary Query Components

- `TripleStore.SPARQL.Parser`
- `TripleStore.SPARQL.Parser.NIF`
- `TripleStore.SPARQL.Algebra`
- `TripleStore.SPARQL.Optimizer`
- `TripleStore.SPARQL.Cardinality`
- `TripleStore.SPARQL.QuadCardinality`
- `TripleStore.SPARQL.CostModel`
- `TripleStore.SPARQL.JoinEnumeration`
- `TripleStore.SPARQL.Executor`
- `TripleStore.SPARQL.Query`
- `TripleStore.SPARQL.UpdateExecutor`
- `TripleStore.SPARQL.PropertyPath`
- `TripleStore.SPARQL.Authorization`
- `TripleStore.SPARQL.Validation`
- `TripleStore.SPARQL.QueryLogger`
- `TripleStore.SPARQL.ErrorHandler`
- `TripleStore.SPARQL.ParallelExecutor`
- `TripleStore.SPARQL.PlanCache`
- `TripleStore.Query.Cache`
- `TripleStore.SPARQL.QueryCache`
- `TripleStore.SPARQL.Leapfrog.*`

## Component Specs

- [sparql_execution_pipeline.md](sparql_execution_pipeline.md)
- [planning_and_cache.md](planning_and_cache.md)

## Current Codebase Notes

- `SPARQL.Query` supports explain, prepared-query, parameter-binding, timeout, streaming, and optional result-cache paths today.
- `PlanCache` is part of the default runtime; `Query.Cache` is opt-in.
- `SPARQL.QueryCache` is still present and tested as a separate ETS-backed cache implementation.
- The execution stack supports triple and quad patterns, graph clauses, quad cardinality, and graph-aware optimization paths.
- Named-graph authorization hooks exist in lower-level query and update contexts when a `:user` is supplied, but `TripleStore.query/3` does not surface that actor context today.

## Acceptance Criteria

| Acceptance ID | Criterion | Related Requirements | Related Scenarios |
|---|---|---|---|
| `AC-QRY-01` | SPARQL query and update text are parsed through a bounded native adapter and then compiled into Elixir-owned execution structures. | `REQ-QRY-*`, `REQ-CP-*` | `SCN-006`, `SCN-015` |
| `AC-QRY-02` | The optimizer can reorder or otherwise improve plans across triple and quad queries without changing query meaning. | `REQ-QRY-*` | `SCN-006` |
| `AC-QRY-03` | Query execution remains schema-aware, index-backed, and streaming where supported rather than requiring eager whole-result materialization. | `REQ-QRY-*`, `REQ-STO-*` | `SCN-005`, `SCN-006` |
| `AC-QRY-04` | Property paths, graph clauses, validation, authorization, and advanced join strategies remain bounded, typed execution paths. | `REQ-QRY-*`, `REQ-OBS-*` | `SCN-006`, `SCN-014`, `SCN-017` |
| `AC-QRY-05` | Query-plan and query-result caching do not survive invalidating writes incorrectly. | `REQ-QRY-*`, `REQ-TXN-*` | `SCN-007` |

## Canonical References

- [../architecture-overview.md](../architecture-overview.md)
- [../topology.md](../topology.md)
- [../contracts/query_execution_contract.md](../contracts/query_execution_contract.md)
