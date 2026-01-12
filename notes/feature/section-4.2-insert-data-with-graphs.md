# Section 4.2: INSERT DATA with Graphs

**Status:** COMPLETE
**Branch:** `feature/section-4.2-insert-data-with-graphs`
**Created:** 2025-01-12
**Completed:** 2025-01-12

## Overview

This section implements INSERT DATA with named graph support. Previously, the `execute_insert_data` function used `Index.insert_triples` for triple stores. This section updates it to use `QuadOperations.insert_quad/2` for quad stores and properly handle the graph component.

## Tasks

### 4.2.1 Single Graph INSERT - COMPLETE
- [x] 4.2.1.1 Update `execute_insert_data/2` to detect quad schema
- [x] 4.2.1.2 Handle `INSERT DATA { GRAPH <g> { ... } }` using `insert_quad/2`
- [x] 4.2.1.3 Handle `INSERT DATA { ... }` (default graph) with graph=0
- [x] 4.2.1.4 Parse graph component from quad AST
- [x] 4.2.1.5 Return count of quads inserted

### 4.2.2 Multi-Graph INSERT - COMPLETE
- [x] 4.2.2.1 Parse multiple GRAPH blocks in single INSERT DATA
- [x] 4.2.2.2 Group quads by graph in the operation
- [x] 4.2.2.3 Insert each group to its respective graph
- [x] 4.2.2.4 Return total count of quads inserted
- [x] 4.2.2.5 Use WriteBatch for atomicity (single batch per graph)

### 4.2.3 INSERT Error Handling - COMPLETE
- [x] 4.2.3.1 Handle invalid graph terms (should not happen with parser)
- [x] 4.2.3.2 Return detailed error messages
- [x] 4.2.3.3 Handle database errors gracefully
- [x] 4.2.3.4 Emit telemetry for insert operations

### 4.2.4 Tests - COMPLETE
- [x] 4.2.4.1 Test INSERT DATA to default graph (quad schema)
- [x] 4.2.4.2 Test INSERT DATA to named graph
- [x] 4.2.4.3 Test INSERT DATA with GRAPH clause
- [x] 4.2.4.4 Test INSERT DATA returns correct count
- [x] 4.2.4.5 Test INSERT DATA with multiple triples

## Implementation Summary

### Files Modified

1. **lib/triple_store/sparql/update_executor.ex**
   - Added `ErlangAdapter` alias
   - Modified `execute_insert_data/2` to detect quad schema and route accordingly
   - Added `insert_quads/2` for quad store insertion
   - Added `do_insert_quads/2` for actual quad insertion
   - Added `get_graph_id_for_insert/2` for graph ID resolution
   - Added `quads_to_rdf_quads/1` to preserve graph component
   - Added `ast_graph_to_rdf/1` to convert AST graph terms to RDF terms
   - Added `{:named_graph, iri}` pattern to `ast_graph_to_rdf/1` and `ast_term_to_rdf_graph/1`

2. **test/triple_store/sparql/insert_data_quad_test.exs** (NEW)
   - Created comprehensive test suite with 14 tests
   - Tests use quad schema database
   - Tests cover default graph, named graphs, parser-based INSERT, error handling

## Key Technical Decisions

1. **Schema Detection**: Used `ErlangAdapter.is_quad_store?/1` to detect quad schema at runtime
2. **Graph Component Preservation**: Created `quads_to_rdf_quads/1` that preserves graph component through conversion
3. **Graph ID Resolution**: Default graph maps to ID 0, named graphs get/create IDs via `term_to_id`
4. **Parser Format**: Parser returns `{:named_graph, iri}` for graph component - added support for this format

## Test Results

```
14 tests, 0 failures
```

All tests pass:
- INSERT to default graph (3 tests)
- INSERT to named graphs (3 tests)
- Parser-based INSERT (3 tests)
- Error handling (2 tests)
- Internal helper conversion (3 tests)

## Dependencies

- Section 4.1 (CREATE/DROP/CLEAR GRAPH) - complete
- `QuadOperations.insert_quad/2` - already exists
- Parser already produces quad AST with graph component
