# Section 4.2: INSERT DATA with Graphs - Summary

**Date:** 2025-01-12
**Branch:** `feature/section-4.2-insert-data-with-graphs`
**Status:** COMPLETE

## Overview

Implemented SPARQL INSERT DATA with named graph support for quad stores. Previously, `execute_insert_data` used `Index.insert_triples` which only works for triple stores. This section adds quad store support using `QuadOperations.insert_quad/2` with proper graph component handling.

## Implementation Details

### Files Modified

1. **lib/triple_store/sparql/update_executor.ex**
   - Added `ErlangAdapter` alias for schema detection
   - Modified `execute_insert_data/2` to detect quad schema and route accordingly
   - Added `insert_quads/2` for quad store insertion
   - Added `do_insert_quads/2` for actual quad insertion with count aggregation
   - Added `get_graph_id_for_insert/2` for graph ID resolution (default = 0, named = looked up/created)
   - Added `quads_to_rdf_quads/1` to convert AST quads to RDF quads while preserving graph component
   - Added `ast_graph_to_rdf/1` to convert AST graph terms (`:default`, `:default_graph`, `{:named_node, iri}`, `{:named_graph, iri}`, `{:iri, iri}`) to RDF terms
   - Updated `ast_term_to_rdf_graph/1` to handle `{:named_graph, iri}` pattern

2. **test/triple_store/sparql/insert_data_quad_test.exs** (NEW)
   - Created comprehensive test suite with 14 tests
   - Tests use quad schema: `NIF.open(test_path, schema: :quad)`
   - Tests cover default graph, named graphs, parser-based INSERT, error handling

## Key Technical Decisions

1. **Schema Detection**: Use `ErlangAdapter.is_quad_store?/1` to detect quad schema at runtime. This allows `execute_insert_data` to work for both triple and quad stores.

2. **Graph Component Preservation**: Created `quads_to_rdf_quads/1` that preserves graph component through the entire conversion pipeline. The function handles:
   - `{:quad, s, p, o, g}` - Full quad with graph
   - `{:triple, s, p, o}` - Legacy triple format (defaults to :default)
   - `{s, p, o}` - Bare triple format (defaults to :default)

3. **Graph ID Resolution**:
   - Default graph (`:default`, `:default_graph`) maps to graph ID 0
   - Named graphs use `Adapter.term_to_id/2` to get/create IDs
   - The `get_graph_id_for_insert/2` helper handles this resolution

4. **Parser Format Support**: The SPARQL parser returns `{:named_graph, iri}` for the graph component in GRAPH clauses. Added support for this format in both `ast_graph_to_rdf/1` and `ast_term_to_rdf_graph/1`.

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

## Breaking Changes

None. The changes are fully backward compatible with triple stores.

## Next Steps

This section is complete. The next section in Phase 4 would be DELETE DATA with graphs (Section 4.3).

## Files for Review

- `lib/triple_store/sparql/update_executor.ex` - Added quad-aware INSERT DATA support
- `test/triple_store/sparql/insert_data_quad_test.exs` - New test file
- `notes/feature/section-4.2-insert-data-with-graphs.md` - Working plan
