# Section 4.3: DELETE DATA with Graphs

**Status:** COMPLETE
**Branch:** `feature/section-4.3-delete-data-with-graphs`
**Created:** 2025-01-12
**Completed:** 2025-01-12

## Overview

This section implements DELETE DATA with named graph support. Previously, the `execute_delete_data` function used `Index.delete_triples` for triple stores. This section updates it to use `QuadOperations.delete_quads/2` for quad stores and properly handle the graph component.

## Tasks

### 4.3.1 Single Graph DELETE - COMPLETE
- [x] 4.3.1.1 Update `execute_delete_data/2` to detect quad schema
- [x] 4.3.1.2 Handle `DELETE DATA { GRAPH <g> { ... } }` using `delete_quads/2`
- [x] 4.3.1.3 Handle `DELETE DATA { ... }` (default graph) with graph=0
- [x] 4.3.1.4 Parse graph component from quad AST
- [x] 4.3.1.5 Return count of quads deleted

### 4.3.2 Multi-Graph DELETE - COMPLETE
- [x] 4.3.2.1 Parse multiple GRAPH blocks in single DELETE DATA
- [x] 4.3.2.2 Group quads by graph in the operation
- [x] 4.3.2.3 Delete from each graph independently
- [x] 4.3.2.4 Return total count of quads deleted
- [x] 4.3.2.5 Handle missing quads gracefully (no-op)

### 4.3.3 DELETE Error Handling - COMPLETE
- [x] 4.3.3.1 Handle non-existent quad (no-op, not error)
- [x] 4.3.3.2 Handle non-existent graph (returns count 0)
- [x] 4.3.3.3 Handle invalid graph terms
- [x] 4.3.3.4 Return detailed error messages
- [x] 4.3.3.5 Emit telemetry for delete operations

### 4.3.4 Tests - COMPLETE
- [x] 4.3.4.1 Test DELETE DATA from default graph (quad schema)
- [x] 4.3.4.2 Test DELETE DATA from named graph
- [x] 4.3.4.3 Test DELETE DATA with GRAPH clause
- [x] 4.3.4.4 Test DELETE DATA returns correct count
- [x] 4.3.4.5 Test DELETE DATA with multiple triples
- [x] 4.3.4.6 Test DELETE non-existent quads is no-op

## Implementation Summary

### Files Modified

1. **lib/triple_store/sparql/update_executor.ex**
   - Modified `execute_delete_data/2` to detect quad schema and route accordingly
   - Added `delete_quads/2` for quad store deletion
   - Added `do_delete_quads/2` for actual quad deletion with existence check
   - Added `get_graph_id_for_delete/2` for graph ID resolution (lookup only, don't create)
   - Added `lookup_term_id_no_create/2` to look up term IDs without creating new entries
   - Added `delete_triples_from_store/2` for triple store deletion (legacy support)

2. **test/triple_store/sparql/delete_data_quad_test.exs** (NEW)
   - Created comprehensive test suite with 14 tests
   - Tests use quad schema database
   - Tests cover default graph, named graphs, parser-based DELETE, error handling, idempotence

## Key Technical Decisions

1. **Schema Detection**: Used `ErlangAdapter.is_quad_store?/1` to detect quad schema at runtime
2. **Graph Component Preservation**: Used `quads_to_rdf_quads/1` (from section 4.2) to preserve graph component
3. **Lookup-Only for DELETE**: Unlike INSERT, DELETE uses lookup-only operations that don't create new dictionary entries
4. **Existence Check**: Used `QuadOperations.quad_exists?/2` to verify quads exist before counting them, ensuring accurate count reporting
5. **Inline-Encoded Literals**: Properly handled inline-encoded literals (integers, decimals, datetimes) in lookup operations

## Test Results

```
14 tests, 0 failures
```

All tests pass:
- DELETE from default graph (3 tests)
- DELETE from named graphs (3 tests)
- Parser-based DELETE (3 tests)
- Error handling (3 tests)
- Idempotence (1 test)

## Dependencies

- Section 4.1 (CREATE/DROP/CLEAR GRAPH) - complete
- Section 4.2 (INSERT DATA with Graphs) - complete (provides `quads_to_rdf_quads/1`)
- `QuadOperations.delete_quads/2` - already exists
- `QuadOperations.quad_exists?/2` - already exists
- Parser already produces quad AST with graph component
