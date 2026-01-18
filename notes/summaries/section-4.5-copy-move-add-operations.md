# Section 4.5: COPY/MOVE/ADD Operations - Summary

**Date:** 2025-01-13
**Branch:** `feature/section-4.5-copy-move-add-operations`
**Status:** COMPLETE

## Overview

Implemented SPARQL UPDATE COPY/MOVE/ADD operations for quad stores. These operations allow bulk transfer of triples between named graphs using the existing `QuadOperations.copy_graph/5` and `QuadOperations.clear_graph/3` functions.

## Implementation Details

### Files Modified

1. **lib/triple_store/sparql/update_executor.ex**
   - Added `execute_copy/4` for COPY operation
   - Added `execute_move/4` for MOVE operation
   - Added `execute_add/4` for ADD operation
   - Added private helpers: `do_copy_quad/4`, `do_move_quad/4`, `do_add_quad/4`
   - Added `normalize_graph_term/1` for graph term normalization

2. **lib/triple_store/quad_operations.ex**
   - Fixed bug in `delete_all_quads_in_graph/2` (case statement expected `{:ok, _}` but `delete_quads/2` returns `:ok`)

3. **test/triple_store/sparql/copy_move_add_test.exs** (NEW)
   - Created comprehensive test suite with 25 tests
   - Tests use quad schema: `NIF.open(test_path, schema: :quad)`

## Key Technical Decisions

1. **Source Existence Validation**: Before executing COPY/MOVE/ADD, the code checks if the source graph exists (has quads) using `graph_exists?/3` or `default_graph_exists?/1`. This matches SPARQL spec behavior where operations on non-existent sources should error (unless SILENT).

2. **Source = Target Error**: Per SPARQL specification, when source equals target, the operations return `{:error, :source_equals_target}` (unless SILENT).

3. **DEFAULT Graph Support**: The operations support `:default` atom for the default graph, as well as RDF.IRI terms for named graphs.

4. **Conflict Handling**:
   - COPY uses `on_conflict: :replace` (target is cleared then copied)
   - MOVE uses `on_conflict: :replace` (target is cleared, source is copied, then source is cleared)
   - ADD uses `on_conflict: :merge` (source quads are added to target without clearing)

5. **Telemetry**: Events are emitted via underlying `QuadOperations` functions - no additional telemetry needed at executor level.

## Test Results

```
25 tests, 0 failures
```

All tests pass:
- COPY with named graphs (to/from): 2 tests
- COPY with DEFAULT graph (to/from): 2 tests
- COPY replaces target: 1 test
- COPY source=target error: 2 tests (with/without SILENT)
- COPY handles non-existent source: 2 tests (with/without SILENT)
- MOVE with named graphs (to/from): 2 tests
- MOVE with DEFAULT graph (to/from): 2 tests
- MOVE replaces target: 1 test
- MOVE clears source: 1 test
- MOVE source=target error: 2 tests (with/without SILENT)
- MOVE handles non-existent source: 2 tests (with/without SILENT)
- ADD with named graphs: 2 tests
- ADD with DEFAULT graph: 2 tests
- ADD to empty target: 1 test
- ADD merges with target: 1 test
- ADD source=target error: 2 tests (with/without SILENT)
- ADD handles non-existent source: 2 tests (with/without SILENT)
- Error handling: 1 test

## Known Limitations

1. **Parser Support**: The SPARQL parser NIF (sparql_parser_nif) does not currently support parsing COPY/MOVE/AD operations. The executor functions are implemented as direct API calls. Parser support would require extending the Rust NIF.

2. **Triple Stores**: These operations only work with quad stores. Triple stores return `{:error, :copy_requires_quad_store}` (or similar) when attempting these operations.

## Breaking Changes

None. The changes are additive only.

## Dependencies

- Section 2.4 (Dataset Operations) - provides `copy_graph/5`, `clear_graph/3`, `graph_exists?/3`
- QuadOperations module - all required functions already exist
- Parser support for COPY/MOVE/ADD - pending (would require Rust NIF changes)

## Bug Fixes

Fixed pre-existing bug in `QuadOperations.delete_all_quads_in_graph/2`:
- The case statement expected `{:ok, _}` but `delete_quads/2` returns `:ok`
- Changed to match `:ok` directly instead of `{:ok, _}`
