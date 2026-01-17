# Section 4.4: MODIFY with Graphs

**Status:** COMPLETE
**Branch:** `feature/section-4.4-modify-with-graphs`
**Created:** 2025-01-12
**Completed:** 2025-01-12

## Overview

This section implements MODIFY (DELETE/INSERT WHERE) with named graph support for quad stores. The `execute_modify` function now handles both triple and quad patterns, routing to appropriate operations based on the store schema.

## Implementation Summary

### Changes to `lib/triple_store/sparql/update_executor.ex`

1. **Modified `execute_modify/4`** to detect quad schema and route appropriately:
   - Added schema detection using `ErlangAdapter.is_quad_store?/1`
   - Routes to `do_execute_modify_quad/4` for quad stores
   - Routes to `do_execute_modify_triples/4` for triple stores

2. **Added `do_execute_modify_quad/4`** for quad store MODIFY:
   - Always uses quad operations for quad stores
   - Handles both 3-tuple (triple) and 4-tuple (quad) patterns
   - Converts 3-tuples to default graph quads

3. **Updated `instantiate_pattern/2`** to handle quad patterns:
   - `{:triple, s, p, o}` returns 3-tuple `{s, p, o}`
   - `{:quad, s, p, o, g}` returns 4-tuple `{s, p, o, g}` with graph component
   - Added `substitute_graph/2` for graph variable substitution

4. **Added `quads_to_internal/3`** for quad conversion:
   - `:create` mode - creates IDs as needed (for INSERT)
   - `:lookup` mode - only looks up existing IDs (for DELETE)
   - Handles 3-tuples by treating them as default graph quads
   - Handles 4-tuples with `:default` or `RDF.IRI{}` graph

5. **Added `execute_atomic_modify_quads/4`** for atomic quad operations:
   - Deletes quads using `QuadOperations.delete_quads/2`
   - Inserts quads using `QuadOperations.insert_quad/2`
   - Returns combined count of deleted + inserted

### New Tests: `test/triple_store/sparql/modify_quad_test.exs`

14 tests covering:
- Ground quad patterns with named graphs
- Ground quad patterns with default graph
- Triple templates (legacy support, treated as default graph)
- Parser-based MODIFY (with BGP limitation notes)
- Error handling (empty templates, non-existent data, mixed templates)
- Atomicity of DELETE/INSERT operations

## Known Limitations

1. **WHERE clause execution**: Full WHERE clause support with graph-scoped quad patterns is now implemented. The implementation works with:
   - Ground template patterns (no variables)
   - Variable patterns with binding substitution
   - `nil` pattern (no WHERE clause)
   - Parser-based queries with WHERE clauses
   - GRAPH clauses in WHERE for graph-scoped pattern matching

2. **Cross-graph WHERE**: WHERE clauses with GRAPH patterns to match across named graphs are supported via GRAPH clause execution.

## Tasks Completed

### 4.4.1 DELETE/INSERT with Graph Context
- [x] 4.4.1.1 Extract graph context from "using" property
- [x] 4.4.1.2 Update `execute_modify/4` to handle quad store schema
- [x] 4.4.1.3 Pass graph context through delete/insert operations
- [x] 4.4.1.4 Use `delete_quads/2` and `insert_quads/2` for quad stores
- [x] 4.4.1.5 Return combined counts (deleted, inserted)

### 4.4.2 Template Instantiation with Quads
- [x] 4.4.2.1 Update `instantiate_template/2` to handle quad patterns
- [x] 4.4.2.2 Preserve graph component through instantiation
- [x] 4.4.2.3 Handle graph variable in template (?g)
- [x] 4.4.2.4 Use WITH clause graph as default for quads

### 4.4.3 WHERE Clause with Graph
- [x] 4.4.3.1 Pass graph context to WHERE clause execution
- [x] 4.4.3.2 Use quad pattern matching for WHERE
- [x] 4.4.3.3 Bindings include graph variable when GRAPH in WHERE
- [x] 4.4.3.4 Handle cross-graph WHERE

### 4.4.4 Quad Conversion
- [x] 4.4.4.1 Add `quads_to_internal/3` for quad conversion
- [x] 4.4.4.2 Handle :lookup mode (for DELETE) with quads
- [x] 4.4.4.3 Handle :create mode (for INSERT) with quads
- [x] 4.4.4.4 Support mixed triples/quads in templates

### 4.4.5 Tests
- [x] 4.4.5.1 Test MODIFY with default graph
- [x] 4.4.5.2 Test MODIFY with named graph (WITH clause)
- [x] 4.4.5.3 Test MODIFY with GRAPH in DELETE template
- [x] 4.4.5.4 Test MODIFY with GRAPH in INSERT template
- [x] 4.4.5.5 Test MODIFY returns correct counts

## Test Results

```
17 tests, 0 failures
```

All tests pass for:
- Ground quad patterns (DELETE/INSERT with explicit graph)
- Ground triple patterns (legacy support, treated as default graph)
- Variable patterns with WHERE clause binding substitution
- Parser-based MODIFY (with full WHERE clause support)
- GRAPH clause execution in WHERE
- Error handling
- Atomicity

## Bug Fixes

### Schema Detection Pattern Matching Bug
During implementation, discovered that `ErlangAdapter.is_quad_store?/1` returns `{:ok, boolean}` (tuple), not just `true` or `false`. Using the result directly in an `if` statement would treat `{:ok, false}` as truthy (non-nil values are truthy in Elixir).

**Fix**: Changed all `if ErlangAdapter.is_quad_store?(ctx.db) do` checks to `case ErlangAdapter.is_quad_store?(ctx.db) do` with explicit pattern matching:
```elixir
case ErlangAdapter.is_quad_store?(ctx.db) do
  {:ok, true} -> # quad store path
  {:ok, false} -> # triple store path
end
```

**Files affected**:
- `lib/triple_store/sparql/executor.ex` - `extend_bindings/3`
- `lib/triple_store/sparql/update_executor.ex` - `execute_insert_data/2`, `execute_delete_data/2`, `execute_modify/4`, `execute_clear/2`

### execute_clear Triple Store Support
Added `execute_clear_triple/3` and `execute_clear_quad/3` routing functions to support CLEAR operations on both triple and quad stores. Previously, CLEAR only worked for quad stores.

**Files affected**:
- `lib/triple_store/sparql/update_executor.ex` - `execute_clear/2`, `execute_clear_triple/3`, `execute_clear_quad/3`

## Dependencies

- Section 4.1 (CREATE/DROP/CLEAR GRAPH) - complete
- Section 4.2 (INSERT DATA with Graphs) - complete (provides `insert_quads/2`)
- Section 4.3 (DELETE DATA with Graphs) - complete (provides `delete_quads/2`)
- `QuadOperations.quad_exists?/2` - already exists
- Parser already produces quad AST with graph component

## Files Modified

1. `lib/triple_store/sparql/update_executor.ex` - Main implementation
2. `test/triple_store/sparql/modify_quad_test.exs` - New test file
3. `notes/feature/section-4.4-modify-with-graphs.md` - This file
4. `notes/summaries/section-4.4-modify-with-graphs.md` - Summary document
