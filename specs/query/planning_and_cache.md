# Query Planning And Cache Surfaces

## Purpose

This document backfills the planning, cost, and cache support currently implemented by:

- `TripleStore.SPARQL.Cardinality`
- `TripleStore.SPARQL.QuadCardinality`
- `TripleStore.SPARQL.CostModel`
- `TripleStore.SPARQL.JoinEnumeration`
- `TripleStore.SPARQL.PlanCache`
- `TripleStore.Query.Cache`
- `TripleStore.SPARQL.QueryCache`
- `TripleStore.SPARQL.Leapfrog.*`

## Control Plane

Primary ownership: **Query Plane** with supporting **Coordination Plane** processes for cache state.

## Current Codebase Notes

- `PlanCache` is part of the default application runtime and is the canonical cache for optimized plans.
- `Query.Cache` is a separate, opt-in cache for materialized query results and is not automatically supervised today.
- `SPARQL.QueryCache` is still present and tested as a distinct ETS-based cache implementation, but it is not the cache wired into `SPARQL.Query`.
- Cost-based planning support now includes triple cardinality, quad cardinality, join enumeration, and Leapfrog Triejoin support modules.
- Cache invalidation is currently wired on mutation for plans and updates can also invalidate the result cache.
- Query planning is therefore split into two layers:
  - structural plan reuse through `PlanCache`
  - optional materialized-result reuse through `Query.Cache`

## Acceptance Criteria

| Acceptance ID | Criterion | Related Tests |
|---|---|---|
| `AC-QRY-10` | Cost, triple cardinality, and quad cardinality support modules remain the basis for plan choice rather than ad hoc pattern ordering alone. | `test/triple_store/sparql/cardinality_test.exs`, `test/triple_store/sparql/quad_cardinality_test.exs`, `test/triple_store/sparql/histogram_cardinality_test.exs`, `test/triple_store/sparql/cost_model_test.exs`, `test/triple_store/sparql/join_enumeration_test.exs` |
| `AC-QRY-11` | `PlanCache` remains the default supervised cache for optimized query plans. | `test/triple_store/sparql/plan_cache_test.exs` |
| `AC-QRY-12` | `Query.Cache` remains an optional runtime feature with explicit persistence, warming, predicate invalidation, and size limits, while `SPARQL.QueryCache` remains a separate tested cache surface. | `test/triple_store/query/cache_test.exs`, `test/triple_store/sparql/query_test.exs`, `test/triple_store/sparql/query_cache_test.exs`, `test/triple_store/sparql/cache_metrics_test.exs` |
| `AC-QRY-13` | Leapfrog and join-enumeration support remain optimizer-selected execution families rather than separate public APIs. | `test/triple_store/sparql/leapfrog/leapfrog_test.exs`, `test/triple_store/sparql/leapfrog/leapfrog_integration_test.exs`, `test/triple_store/sparql/leapfrog/quad_leapfrog_test.exs`, `test/triple_store/sparql/cost_optimizer_integration_test.exs` |
