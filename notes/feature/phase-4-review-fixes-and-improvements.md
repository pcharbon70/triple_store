# Phase 4 Review Fixes and Improvements

**Status:** IN PROGRESS (Blockers Complete)
**Branch:** `feature/phase-4-review-fixes-and-improvements`
**Created:** 2025-01-13
**Updated:** 2025-01-13
**Based on:** `notes/review/phase-4-comprehensive-review.md`

## Progress Summary

### Completed (Blockers)
- [x] 1.1 Add authorization checks to all UPDATE operations
- [x] 1.2 Fix create_graph return value inconsistency
- [x] 1.3 Fix clear_all_graphs column family error

### Remaining (Concerns & Suggestions)
- [ ] 2.1 Refactor UpdateExecutor into smaller modules
- [ ] 2.2 Create protocol for store operations (eliminate dual schema duplication)
- [ ] 2.3 Optimize batch insertion performance
- [ ] 2.4 Make MOVE operation atomic
- [ ] 2.5 Add rate limiting and quotas
- [ ] 3.1 Add more specific error types
- [ ] 3.2 Add property-based tests

### Testing
- [ ] 4.1 Write authorization tests
- [ ] 4.2 Write rate limiter tests
- [ ] 4.3 Write atomic MOVE tests
- [ ] 4.4 Run full test suite

## Overview

This feature implements all fixes and improvements identified in the comprehensive Phase 4 review. The review found:

- **3 Blockers** (must fix): Authorization bypass, graph management bugs
- **5 Concerns** (should address): Architecture, performance, security
- **5 Suggestions** (improvements): Protocols, testing, error types

## Completed Work

### Blocker 1: Authorization Bypass (CRITICAL) ✅

**Issue:** UpdateExecutor did not call Authorization module before INSERT, DELETE, CREATE, DROP, CLEAR, COPY, MOVE, ADD operations.

**Files Modified:**
- `lib/triple_store/sparql/update_executor.ex`

**Implementation:**
- Added `:user` to context type definition
- Added `get_user/1` helper function
- Added `check_write_authorization/2` helper function
- Added `check_admin_authorization/2` helper function
- Added `check_read_authorization/2` helper function
- Added `check_multi_graph_authorization/3` helper function
- Added `graph_term_to_iri_string/1` helper function
- Added `extract_graphs_from_quads/1` helper function
- Added `extract_graphs_from_templates/1` helper function
- Updated @moduledoc with authorization documentation

**Authorization Rules Implemented:**
- INSERT DATA: `:write` permission on target graph(s)
- DELETE DATA: `:write` permission on target graph(s)
- MODIFY: `:write` permission on affected graph(s)
- CREATE GRAPH: `:admin` permission on graph
- DROP GRAPH: `:admin` permission on graph
- CLEAR GRAPH: `:write` permission on graph
- COPY: `:read` on source, `:write` on target
- MOVE: `:admin` on both source and target
- ADD: `:read` on source, `:write` on target

**Backward Compatibility:**
- Operations without user context proceed (for internal/maintenance use)
- Default graph always accessible without explicit authorization

### Blocker 2: create_graph Return Value Inconsistency ✅

**Issue:** `graph_exists?` used `Adapter.term_to_id` which calls `get_or_create_id`, creating IDs during existence checks. This caused `create_graph` to return `{:ok, :already_exists}` on first call.

**Files Modified:**
- `lib/triple_store/quad_operations.ex`

**Implementation:**
- Changed `graph_exists?` to use `Manager.lookup_id` instead of `Adapter.term_to_id`
- Now only checks if ID exists without creating it
- Graph existence requires both ID allocation AND at least one quad

### Blocker 3: clear_all_graphs Column Family Error ✅

**Issue:** `clear_all_graphs` used `Index.lookup` designed for triple stores, causing invalid column family errors for quad stores.

**Files Modified:**
- `lib/triple_store/sparql/update_executor.ex`

**Implementation:**
- Added schema detection in `clear_all_graphs`
- Added `clear_all_graphs_quad/1` for quad stores
- Uses `QuadOperations.list_graphs` to get all graphs
- Clears each graph using `QuadOperations.clear_graph`
- Falls back to default graph clear if list fails

## Remaining Work

### Concern 1: UpdateExecutor Refactoring (God Object)

**Issue:** 1,873 lines, too many responsibilities.

**Files to create:**
- `lib/triple_store/sparql/update/insert_data.ex`
- `lib/triple_store/sparql/update/delete_data.ex`
- `lib/triple_store/sparql/update/modify.ex`
- `lib/triple_store/sparql/update/graph_management.ex`

**Files to modify:**
- `lib/triple_store/sparql/update_executor.ex` - becomes thin dispatcher

### Concern 2: Dual Schema Duplication

**Issue:** ~40% code duplication between triple and quad paths.

**Solution:** Create protocol for store operations.

### Concern 3: Performance Issues

**Issue:** Sequential quad insertion instead of batching.

**Fix:**
- Change `do_insert_quads/2` to use `QuadOperations.insert_quads/3` batch API
- Pre-allocate all IDs before batch insert

### Concern 4: MOVE Operation Race Condition

**Issue:** Copy then clear not atomic, data can exist in both graphs.

**Fix:**
- Add `move_quads/5` operation to QuadOperations
- Use single WriteBatch for atomicity

### Concern 5: DoS Vulnerabilities

**Fixes:**
- Add per-user operation rate limiting
- Add per-user quota tracking

### Suggestion 1: More Specific Error Types

**Error types to add:**
- `UnauthorizedError` - authorization failure
- `QuotaExceededError` - rate limit exceeded
- `GraphNotFoundError` - graph doesn't exist

### Suggestion 2: Property-Based Tests

**Files to create:**
- `test/triple_store/sparql/update/authorization_property_test.exs`
- `test/triple_store/sparql/update/graph_operations_property_test.exs`

## Success Criteria

1. ✅ All UPDATE operations check authorization before execution
2. ✅ create_graph returns consistent values
3. ✅ clear_all_graphs works without CF errors
4. ⬜ MOVE operation is atomic
5. ⬜ Batch insertion improves performance
6. ⬜ Rate limiting prevents DoS
7. ⬜ UpdateExecutor split into smaller modules
8. ⬜ Protocol eliminates dual schema duplication
9. ⬜ All tests pass
10. ⬜ No regressions in existing functionality

## Dependencies

- Phase 4 sections 4.1-4.7 (already merged to quad branch)
- Authorization module (already exists)
- Review findings (already documented)

## Notes

### Authorization Implementation

The Authorization module already exists (`lib/triple_store/sparql/authorization.ex`) and provides:
- `can_read?/3` - check read access
- `can_write?/3` - check write access
- `can_admin?/3` - check admin access
- `can_access_graph?/4` - check access for graph term

### Context Extension

UpdateExecutor accepts `:user` in context:
```elixir
ctx = %{
  db: db,
  dict_manager: dict_manager,
  user: %{id: "user123", roles: [:editor]}
}
```

### Backward Compatibility

For backward compatibility, when no user is provided:
- Operations succeed as before (for internal/maintenance use)
- Default graph is always accessible
