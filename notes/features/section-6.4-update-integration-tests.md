# Section 6.4: Update Integration Tests

## Overview

This feature implements Section 6.4 of the quad store integration tests, focusing on SPARQL UPDATE operations with graphs.

## Implementation Plan

### 6.4.1 Graph Management Updates (6 tests) - COMPLETE ✓

- [x] 6.4.1.1 Test CREATE GRAPH then query returns empty
- [x] 6.4.1.2 Test DROP GRAPH removes all data
- [x] 6.4.1.3 Test CLEAR GRAPH empties graph
- [x] 6.4.1.4 Test CREATE SILENT on existing graph
- [x] 6.4.1.5 Test DROP SILENT on missing graph
- [x] 6.4.1.6 Test CLEAR ALL empties database

### 6.4.2 INSERT/DELETE with Graphs (6 tests) - COMPLETE ✓

- [x] 6.4.2.1 Test INSERT DATA to named graph
- [x] 6.4.2.2 Test INSERT DATA with multiple GRAPH blocks
- [x] 6.4.2.3 Test DELETE DATA from named graph
- [x] 6.4.2.4 Test DELETE DATA with multiple GRAPH blocks
- [x] 6.4.2.5 Test INSERT then DELETE same quad
- [x] 6.4.2.6 Test INSERT creates graph if needed

### 6.4.3 MODIFY Operations (5 tests) - COMPLETE ✓

- [x] 6.4.3.1 Test MODIFY in named graph
- [x] 6.4.3.2 Test MODIFY with WHERE across graphs
- [x] 6.4.3.3 Test MODIFY WITH graph context
- [x] 6.4.3.4 Test MODIFY atomicity (all or nothing)
- [x] 6.4.3.5 Test MODIFY returns correct counts

### 6.4.4 COPY/MOVE/ADD Operations (5 tests) - COMPLETE ✓

- [x] 6.4.4.1 Test COPY GRAPH duplicates graph
- [x] 6.4.4.2 Test MOVE GRAPH moves and deletes source
- [x] 6.4.4.3 Test ADD merges source into target
- [x] 6.4.4.4 Test operations with SILENT modifier
- [x] 6.4.4.5 Test operations on non-existent graphs

## Files Created

1. `test/triple_store/integration/update_operations_test.exs` - UPDATE operation tests (22 tests, all passing)

## Files Modified

1. `lib/triple_store/statistics.ex` - Fixed `invalidate_quad_cache/2` and `invalidate_all_quad_cache/1` to check if ETS table exists before attempting operations (handles case when Statistics server is not running)

## Test Results

**All 22 tests passing** ✓

### Test Breakdown by Category

| Category | Tests | Status |
|----------|-------|--------|
| 6.4.1 Graph Management Updates | 6 | All passing |
| 6.4.2 INSERT/DELETE with Graphs | 6 | All passing |
| 6.4.3 MODIFY Operations | 5 | All passing |
| 6.4.4 COPY/MOVE/ADD Operations | 5 | All passing |
| **Total** | **22** | **All passing** |

## Implementation Notes

### Statistics Cache Fix

The `invalidate_quad_cache/2` and `invalidate_all_quad_cache/1` functions in `statistics.ex` were updated to check if the ETS table exists before attempting operations. This fixes issues that occur when the Statistics GenServer is not running (as is the case in integration tests).

The fix uses `:ets.whereis/1` to check if the table exists:
```elixir
if :ets.whereis(@quad_cache_table) != :undefined do
  # ... perform cache invalidation
end
```

### MOVE Operation Behavior

The parser implements MOVE as a sequence of operations:
1. DROP the target graph
2. COPY source to target
3. DROP the source graph

This means the count returned for MOVE includes all operations. For example, moving 2 quads from source to target returns 4:
- DROP target: 0 quads
- COPY: 2 quads
- DROP source: 2 quads
- Total: 4

### Test Structure

Tests follow the existing integration test patterns:
- Use `ExUnit.Case, async: false` for database operations
- Create temporary databases with unique IDs
- Use `on_exit` for cleanup
- Use `TripleStore.SPARQL.Parser.parse_update/1` for parsing
- Use `TripleStore.SPARQL.UpdateExecutor.execute/2` for execution
- Verify results with `TripleStore.QuadOperations` assertions

### Authorization Setup

Tests include authorization setup to allow UPDATE operations on test graphs:
```elixir
defp setup_graph_authorization(ctx) do
  :ok = Authorization.set_public(ctx, "#{@ex}graph1")
  :ok = Authorization.set_public(ctx, "#{@ex}graph2")
  :ok = Authorization.set_public(ctx, "#{@ex}graph3")
  :ok = Authorization.set_public(ctx, "#{@ex}source")
  :ok = Authorization.set_public(ctx, "#{@ex}target")
end
```

## Dependencies

- `TripleStore.Backend.RocksDB.NIF` - Database operations
- `TripleStore.Dictionary.Manager` - Dictionary encoding
- `TripleStore.SPARQL.Parser` - SPARQL UPDATE parsing
- `TripleStore.SPARQL.UpdateExecutor` - UPDATE operation execution
- `TripleStore.SPARQL.Authorization` - Access control
- `TripleStore.QuadOperations` - Quad CRUD operations
- `TripleStore.SPARQL.Query` - Query execution (for verification)

## Status

**Complete** - All 22 tests passing. Ready for commit and merge.
