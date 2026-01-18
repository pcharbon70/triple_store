# Section 4.5: COPY/MOVE/ADD Operations

**Status:** COMPLETE
**Branch:** `feature/section-4.5-copy-move-add-operations`
**Created:** 2025-01-13

## Overview

This section implements SPARQL UPDATE COPY/MOVE/ADD operations for quad stores. These operations allow bulk transfer of triples between named graphs.

**Note:** The underlying `copy_graph/5` function already exists in `QuadOperations` (Section 2.4). This section adds the SPARQL UPDATE executor layer to handle the parser syntax and wire up to the existing operations.

## SPARQL Specification

From SPARQL 1.1 Update:

1. **COPY** - Copy all triples from source graph to target graph, replacing target
   ```
   COPY (SILENT)? <graph_source> TO <graph_target>
   ```

2. **MOVE** - Move all triples from source to target, then clear source
   ```
   MOVE (SILENT)? <graph_source> TO <graph_target>
   ```

3. **ADD** - Add all triples from source to target (merge, no replace)
   ```
   ADD (SILENT)? <graph_source> TO <graph_target>
   ```

## Implementation Plan

### 4.5.1 COPY Operation
- [x] 4.5.1.1 Add `execute_copy/3` to UpdateExecutor
- [ ] 4.5.1.2 Handle `COPY <source> TO <target>` syntax from parser
- [x] 4.5.1.3 Handle `COPY SILENT` suppressing errors
- [x] 4.5.1.4 Call `QuadOperations.copy_graph/5` with `on_conflict: :replace`
- [x] 4.5.1.5 Validate source and target graph IRIs
- [x] 4.5.1.6 Return count of triples copied

### 4.5.2 MOVE Operation
- [x] 4.5.2.1 Add `execute_move/3` to UpdateExecutor
- [ ] 4.5.2.2 Handle `MOVE <source> TO <target>` syntax from parser
- [x] 4.5.2.3 Handle `MOVE SILENT` suppressing errors
- [x] 4.5.2.4 Copy source to target with `on_conflict: :replace`
- [x] 4.5.2.5 Clear source graph after successful copy
- [x] 4.5.2.6 Handle rollback if clear fails
- [x] 4.5.2.7 Return count of triples moved

### 4.5.3 ADD Operation
- [x] 4.5.3.1 Add `execute_add/3` to UpdateExecutor
- [ ] 4.5.3.2 Handle `ADD <source> TO <target>` syntax from parser
- [x] 4.5.3.3 Handle `ADD SILENT` suppressing errors
- [x] 4.5.3.4 Call `QuadOperations.copy_graph/5` with `on_conflict: :merge`
- [x] 4.5.3.5 Validate source and target graph IRIs
- [x] 4.5.3.6 Return count of triples added

### 4.5.4 Error Handling
- [x] 4.5.4.1 Handle source graph doesn't exist (error unless SILENT)
- [x] 4.5.4.2 Handle source and target are same graph (error per spec)
- [x] 4.5.4.3 Handle invalid graph IRIs
- [x] 4.5.4.4 Handle COPY/MOVE/ADD to/from default graph
- [x] 4.5.4.5 Return appropriate error codes

### 4.5.5 Telemetry
- [x] 4.5.5.1 Emit telemetry for COPY operations (via QuadOperations.copy_graph)
- [x] 4.5.5.2 Emit telemetry for MOVE operations (via copy_graph + clear_graph)
- [x] 4.5.5.3 Emit telemetry for ADD operations (via QuadOperations.copy_graph)
- [x] 4.5.5.4 Include operation type, source graph, target graph, count

### 4.5.6 Tests
- [x] 4.5.6.1 Test COPY moves all triples to target
- [x] 4.5.6.2 Test COPY replaces target graph contents
- [x] 4.5.6.3 Test COPY SILENT ignores missing source
- [x] 4.5.6.4 Test MOVE empties source graph
- [x] 4.5.6.5 Test MOVE fails if source equals target
- [x] 4.5.6.6 Test ADD merges with target graph
- [x] 4.5.6.7 Test ADD SILENT ignores missing source
- [x] 4.5.6.8 Test error when source doesn't exist (no SILENT)
- [ ] 4.5.6.9 Test parser-based COPY execution
- [ ] 4.5.6.10 Test parser-based MOVE execution
- [ ] 4.5.6.11 Test parser-based ADD execution

## Test Results

```
25 tests, 0 failures
```

All tests pass for:
- COPY with named graphs (to/from)
- COPY with DEFAULT graph (to/from)
- COPY replaces target contents
- COPY with source=target error
- COPY SILENT handles errors
- MOVE with named graphs (to/from)
- MOVE with DEFAULT graph (to/from)
- MOVE clears source after copy
- MOVE SILENT handles errors
- ADD with named graphs (to/from)
- ADD with DEFAULT graph (to/from)
- ADD merges with target contents
- ADD SILENT handles errors
- Error handling for non-existent source

## Notes

### Parser Support
The SPARQL parser NIF (sparql_parser_nif) does not currently support parsing COPY/MOVE/ADD operations. The executor functions are implemented as direct API calls and can be wired up to the parser once the Rust NIF is extended.

To use these operations via the API:
```elixir
# COPY
{:ok, count} = UpdateExecutor.execute_copy(ctx, source_graph, target_graph)

# MOVE
{:ok, count} = UpdateExecutor.execute_move(ctx, source_graph, target_graph)

# ADD
{:ok, count} = UpdateExecutor.execute_add(ctx, source_graph, target_graph)

# With SILENT option
{:ok, count} = UpdateExecutor.execute_copy(ctx, source_graph, target_graph, silent: true)
```

### Telemetry
Telemetry events are emitted via the underlying QuadOperations functions:
- `[:triple_store, :quad, :copy_graph, :start]` / `[:triple_store, :quad, :copy_graph, :stop]`
- `[:triple_store, :quad, :clear_graph, :start]` / `[:triple_store, :quad, :clear_graph, :stop]`

### Bug Fix
Fixed pre-existing bug in `QuadOperations.delete_all_quads_in_graph/2` where the case statement expected `{:ok, _}` but `delete_quads/2` returns `:ok` (not a tuple).

## Dependencies
- `QuadOperations.copy_graph/5` - Already implemented (Section 2.4)
- `QuadOperations.clear_graph/3` - Already implemented (Section 2.4)
- `QuadOperations.graph_exists?/3` - Already implemented (Section 2.4)
- Parser produces `{:copy, ...}`, `{:move, ...}`, `{:add, ...}` AST nodes

### Need to Add
- `execute_copy/3`, `execute_move/3`, `execute_add/3` in UpdateExecutor
- Wire up in `execute_operation/2`
- Test file: `test/triple_store/sparql/copy_move_add_test.exs`

## Files to Modify

1. `lib/triple_store/sparql/update_executor.ex` - Add COPY/MOVE/ADD execution
2. `test/triple_store/sparql/copy_move_add_test.exs` - NEW test file
3. `notes/feature/section-4.5-copy-move-add-operations.md` - This file
4. `notes/summaries/section-4.5-copy-move-add-operations.md` - NEW summary file

## Test Targets

**Minimum:** 11 tests, all passing
**Target:** 15+ tests covering edge cases

## Success Criteria

1. COPY replaces target graph with source graph contents
2. MOVE moves triples and empties source
3. ADD merges source into target
4. SILENT suppresses errors for missing source graph
5. Error when source equals target
6. All operations emit telemetry
7. All tests pass

## Questions for Developer

1. Should COPY/MOVE/ADD operations work with the default graph?
   - SPARQL spec says default graph is a valid target
   - Need to confirm if our implementation supports this

2. Should we validate that source graph exists before operation?
   - SPARQL spec: error if source doesn't exist (unless SILENT)
   - Our `copy_graph` may handle empty source gracefully

3. How should we handle the case where source equals target?
   - SPARQL spec: should be an error
   - Need to add explicit check

## Implementation Notes

### Parser AST Format

Based on existing patterns, the parser likely produces:
```elixir
{:copy, [
  {"silent", true | false},
  {"source", graph_iri},
  {"target", graph_iri}
]}
```

### Error Handling

SPARQL 1.1 Update spec:
- COPY/MOVE/ADD with non-existent source: error (unless SILENT)
- COPY/MOVE/ADD where source equals target: error
- SILENT modifier: suppress errors, return success

### Atomicity

- COPY: Should be atomic (all or nothing)
- MOVE: Should be atomic (copy + clear in single transaction)
- ADD: Should be atomic (all or nothing)

Since we're using WriteBatch operations via QuadOperations, the underlying operations are already atomic per-batch.
