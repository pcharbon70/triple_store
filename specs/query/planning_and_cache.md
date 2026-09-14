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
- Production result-cache keys include the identity of the currently open store.
  Reopening a path produces a new identity, so entries cannot cross store
  lifetimes. Triple-store queries and quad-store contexts with explicit
  `permit_all: true` can use the result cache. ACL-governed quad contexts bypass
  it because the current ACL storage does not expose a stable revision covering
  user, role, owner, and public-access changes.
- Cache persistence format version 2 excludes legacy unscoped entries. Warming a
  version 1 file returns `{:error, {:unsupported_version, 1}}`.
- Every active named `Query.Cache` registers with the application registry.
  Successful supported mutations synchronously invalidate entries for the open
  store identity in all of those caches. Per-store generations prevent a query
  that began before a commit from publishing a stale entry after invalidation.

## Mutation Invalidation Matrix

| Mutation surface | Plan cache | Result cache | Statistics cache |
| --- | --- | --- | --- |
| `TripleStore.load/3`, `load_graph/3`, `load_string/4` | Caller/update coordination remains explicit | Full invalidation for the mutated open-store identity after a positive committed count | Loader-owned behavior |
| `TripleStore.insert/2`, `delete/2` | Caller/update coordination remains explicit | Full invalidation for the mutated open-store identity after a positive committed count | Existing direct-path behavior |
| SPARQL INSERT DATA / DELETE DATA / MODIFY | `Transaction` invalidates the supervised plan cache after update execution | Full open-store invalidation after each committed operation, including before a later operation fails | Quad insert/delete retain their graph-scoped statistics invalidation |
| CREATE / DROP / CLEAR / COPY / MOVE / ADD | `Transaction` invalidates the supervised plan cache after update execution | Full open-store invalidation after each successful graph mutation | Graph-operation-owned behavior |
| Lower-level `Index`, `QuadOperations`, adapter, and ACL calls | None | None; callers using these expert surfaces own invalidation | Operation-specific |

The result cache uses full store-scoped invalidation because variable predicates,
graph-wide changes, and unknown dependencies cannot always be narrowed safely.
Cache absence or a concurrent cache stop does not change a committed write result.

## Acceptance Criteria

| Acceptance ID | Criterion | Related Tests |
|---|---|---|
| `AC-QRY-10` | Cost, triple cardinality, and quad cardinality support modules remain the basis for plan choice rather than ad hoc pattern ordering alone. | `test/triple_store/sparql/cardinality_test.exs`, `test/triple_store/sparql/quad_cardinality_test.exs`, `test/triple_store/sparql/histogram_cardinality_test.exs`, `test/triple_store/sparql/cost_model_test.exs`, `test/triple_store/sparql/join_enumeration_test.exs` |
| `AC-QRY-11` | `PlanCache` remains the default supervised cache for optimized query plans. | `test/triple_store/sparql/plan_cache_test.exs` |
| `AC-QRY-12` | `Query.Cache` remains an optional runtime feature with explicit persistence, warming, predicate invalidation, and size limits, while `SPARQL.QueryCache` remains a separate tested cache surface. | `test/triple_store/query/cache_test.exs`, `test/triple_store/sparql/query_test.exs`, `test/triple_store/sparql/query_cache_test.exs`, `test/triple_store/sparql/cache_metrics_test.exs` |
| `AC-QRY-13` | Leapfrog and join-enumeration support remain optimizer-selected execution families rather than separate public APIs. | `test/triple_store/sparql/leapfrog/leapfrog_test.exs`, `test/triple_store/sparql/leapfrog/leapfrog_integration_test.exs`, `test/triple_store/sparql/leapfrog/quad_leapfrog_test.exs`, `test/triple_store/sparql/cost_optimizer_integration_test.exs` |
| `AC-QRY-14` | Materialized results are isolated by open store instance; legacy unscoped persisted entries are rejected, and ACL-governed quad queries bypass result caching until authorization has a stable revision identity. | `test/triple_store/query/cache_test.exs`, `test/triple_store/sparql/query_test.exs`, `test/triple_store/backend/rocksdb/lifecycle_test.exs` |
| `AC-QRY-15` | Every graph produced by variable substitution in a quad MODIFY template is write-authorized before any explicit index mutation. | `test/triple_store/sparql/update_authorization_test.exs`, `test/triple_store/remediation_integration_test.exs` |
| `AC-QRY-16` | Supported mutations invalidate every active named materialized-result cache for the affected open store, preserve unrelated-store entries, and reject stale in-flight fills by generation. | `test/triple_store/query/cache_store_invalidation_test.exs`, `test/triple_store/sparql/phase_2_correctness_test.exs`, `test/triple_store/remediation_integration_test.exs` |
