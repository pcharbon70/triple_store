# Query Planning And Cache Surfaces

## Purpose

This document backfills the planning, cost, and cache support currently implemented by:

- `TripleStore.SPARQL.Cardinality`
- `TripleStore.SPARQL.CostModel`
- `TripleStore.SPARQL.JoinEnumeration`
- `TripleStore.SPARQL.PlanCache`
- `TripleStore.Query.Cache`
- `TripleStore.SPARQL.Leapfrog.*`

## Control Plane

Primary ownership: **Query Plane** with supporting **Coordination Plane** processes for cache state.

## Current Codebase Notes

- `PlanCache` is part of the default application runtime and is the canonical cache for optimized plans.
- `Query.Cache` is a separate, opt-in cache for materialized query results and is not automatically supervised today.
- Cost-based planning support now includes cardinality estimation, join enumeration, and Leapfrog Triejoin support modules.
- Cache invalidation is currently wired on mutation for plans and updates can also invalidate the result cache.
- Query planning is therefore split into two layers:
  - structural plan reuse through `PlanCache`
  - optional materialized-result reuse through `Query.Cache`

## Acceptance Criteria

| Acceptance ID | Criterion | Related Tests |
|---|---|---|
| `AC-QRY-10` | Cost and cardinality support modules remain the basis for plan choice rather than ad hoc pattern ordering alone. | `test/triple_store/sparql/cardinality_test.exs`, `test/triple_store/sparql/cost_model_test.exs`, `test/triple_store/sparql/join_enumeration_test.exs` |
| `AC-QRY-11` | `PlanCache` remains the default supervised cache for optimized query plans. | `test/triple_store/sparql/plan_cache_test.exs` |
| `AC-QRY-12` | `Query.Cache` remains an optional runtime feature with explicit persistence, warming, predicate invalidation, and size limits. | `test/triple_store/query/cache_test.exs`, `test/triple_store/sparql/query_test.exs` |
| `AC-QRY-13` | Leapfrog support remains an optimizer-selected execution family rather than a separate public API. | `test/triple_store/sparql/leapfrog/leapfrog_test.exs`, `test/triple_store/sparql/leapfrog/leapfrog_integration_test.exs` |
