# Task 5.3.2: Cache Invalidation

**Date:** 2025-12-28
**Branch:** `feature/task-5.3.2-cache-invalidation`
**Status:** Complete

## Overview

Implemented intelligent cache invalidation for the query result cache. The system automatically invalidates cached queries when SPARQL UPDATE operations modify relevant predicates, while falling back to full invalidation for complex operations.

## Implementation Details

### Files Modified

1. **`lib/triple_store/query/cache.ex`**
   - Added telemetry event emission for cache hits, misses, and expirations
   - Events: `[:triple_store, :cache, :query, :hit | :miss | :expired]`

2. **`lib/triple_store/sparql/update_executor.ex`**
   - Added `invalidate_cache_for_operations/1` function
   - Extracts predicates from update operations (INSERT DATA, DELETE DATA, DELETE/INSERT WHERE)
   - Calls `QueryCache.invalidate_predicates/1` for targeted invalidation
   - Falls back to `QueryCache.invalidate/0` for complex operations (CLEAR, LOAD, DROP)
   - Gracefully skips invalidation if cache is not running
   - Emits telemetry events for invalidation operations

3. **`test/triple_store/query/cache_test.exs`**
   - Added 3 new telemetry tests (34 tests total, up from 31)

### Features Implemented

| Feature | Description |
|---------|-------------|
| Predicate Tracking | Already implemented in Task 5.3.1 - each cached entry stores accessed predicates |
| Automatic Invalidation | Update executor calls invalidation after successful updates |
| Predicate Extraction | Extracts predicates from INSERT DATA, DELETE DATA, and DELETE/INSERT WHERE |
| Full Invalidation Fallback | CLEAR, LOAD, DROP, and unknown operations trigger full cache clear |
| Telemetry Events | Hit, miss, expired, and invalidation events for monitoring |
| Graceful Degradation | Cache invalidation skipped if cache process not running |

### Cache Invalidation Strategy

```
UPDATE Operation
       |
       v
  Extract Predicates
       |
       +-- Can extract predicates? --> Targeted invalidation
       |                                (invalidate_predicates)
       |
       +-- Complex operation? --------> Full invalidation
           (CLEAR, LOAD, DROP)           (invalidate)
```

### Predicate Extraction by Operation Type

| Operation | Predicate Source |
|-----------|------------------|
| INSERT DATA | Predicate from each quad |
| DELETE DATA | Predicate from each quad |
| DELETE/INSERT WHERE | Predicates from delete template, insert template, and WHERE pattern |
| CLEAR | Full invalidation |
| LOAD | Full invalidation |
| DROP | Full invalidation |
| CREATE | No invalidation (no data change) |

### Telemetry Events

| Event | Measurements | Metadata |
|-------|--------------|----------|
| `[:triple_store, :cache, :query, :hit]` | `%{count: 1}` | `%{}` |
| `[:triple_store, :cache, :query, :miss]` | `%{count: 1}` | `%{}` |
| `[:triple_store, :cache, :query, :expired]` | `%{count: 1}` | `%{}` |
| `[:triple_store, :cache, :query, :invalidate]` | `%{count: 1, predicate_count: n}` | `%{type: :predicate | :full}` |

### Test Coverage

```
34 tests, 0 failures
```

New tests added:
- `emits hit event on cache hit`
- `emits miss event on cache miss`
- `emits expired event when entry has expired`

### Key Design Decisions

1. **Graceful Degradation**: If the query cache is not running (e.g., not started in test environment), invalidation is silently skipped.

2. **Conservative Full Invalidation**: For complex operations that we can't analyze (CLEAR, LOAD, unknown patterns), we invalidate the entire cache to ensure correctness.

3. **Post-Update Invalidation**: Invalidation happens after the update succeeds, not before. This ensures we only invalidate when data actually changed.

4. **Predicate Variables Ignored**: If a predicate position contains a variable (not a ground term), we can't determine which predicates are affected.

## Usage

The cache invalidation is automatic - no changes needed to calling code. When using the cache:

```elixir
# Cache queries with predicate tracking
{:ok, result} = Query.Cache.get_or_execute(query, fn ->
  TripleStore.SPARQL.Query.execute(db, query)
end, predicates: [RDF.iri("http://example.org/name")])

# Updates automatically invalidate relevant cached queries
{:ok, _} = UpdateExecutor.execute(ctx, ast)
# ^ Cached queries touching updated predicates are now invalidated
```

## Relationship to Other Tasks

- **Task 5.3.1 (Result Cache)**: Built on predicate tracking infrastructure from 5.3.1
- **Task 5.3.3 (Cache Warming)**: Can still be implemented independently
- **Task 5.4 (Telemetry)**: Telemetry events ready for integration with Prometheus

## Dependencies

No new dependencies added.
