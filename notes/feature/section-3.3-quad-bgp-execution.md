# Working Plan: Section 3.3 - Quad BGP Execution

## Branch: `feature/section-3.3-quad-bgp-execution`

## Status: COMPLETED

## Overview

Section 3.3 implements true quad pattern BGP execution, extending the existing triple
pattern execution to handle quad patterns (with graph position) directly. This enables
efficient graph-scoped queries without converting back to triple patterns.

---

## Part 1: BGP Pattern Extension (3.3.1)

### Task 1.1: Update execute_bgp/3 to accept quad patterns
- [x] Update `execute_bgp/3` to handle patterns with `:quad` tag
- [x] Add pattern type detection for quad vs triple
- [x] Preserve backward compatibility for all-triple BGPs

**File:** `lib/triple_store/sparql/executor.ex`

### Task 1.2: Add is_quad_bgp?/1 helper
- [x] Detect if BGP contains any quad patterns
- [x] Return true if any pattern is a 4-tuple `:quad`
- [x] Return false if all patterns are 3-tuple `:triple`

**File:** `lib/triple_store/sparql/executor.ex`

### Task 1.3: Handle mixed patterns
- [x] For BGPs with both triple and quad patterns, convert all to quad
- [x] Use default graph for triple patterns when converting

**File:** `lib/triple_store/sparql/executor.ex`

---

## Part 2: Quad Pattern Execution (3.3.2)

### Task 2.1: Implement execute_quad_pattern/5
- [x] Create function `execute_single_quad_pattern/5` for single quad pattern
- [x] Signature: `execute_single_quad_pattern(ctx, binding, s, p, o, g)`
- [x] Extend existing triple pattern logic to handle 4th position

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.2: Use QuadIndex for index selection
- [x] Use `QuadOperations.lookup_quads/3` for quad lookup
- [x] Handle pattern match results with filtering needs

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.3: Perform prefix scan with quad patterns
- [x] Build prefix based on pattern bound positions
- [x] Use appropriate index (GSPO, GPOS, SPOG, POSG) via QuadOperations
- [x] Handle graph position in prefix building

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.4: Apply post-filtering if needed
- [x] Filter results if pattern has unbound positions not covered by index
- [x] Convert quad results to bindings

**File:** `lib/triple_store/sparql/executor.ex`

---

## Part 3: Graph Binding in Joins (3.3.3)

### Task 3.1: Ensure graph variable consistency
- [x] When graph is bound, all patterns in join must use same graph
- [x] Validate graph variable binding consistency

**File:** `lib/triple_store/sparql/executor.ex`

### Task 3.2: Handle cross-graph joins
- [x] When graph is variable, allow cross-graph joining
- [x] Group patterns by graph for optimization

**File:** `lib/triple_store/sparql/executor.ex`

---

## Part 4: Default Graph BGP (3.3.4)

### Task 4.1: Detect default graph BGP
- [x] BGPs without GRAPH clause use default graph
- [x] Maintain backward compatibility with existing triple queries

**File:** `lib/triple_store/sparql/executor.ex`

### Task 4.2: Convert triple patterns to default graph quads
- [x] Use `@default_graph_id` (0) for graph position
- [x] Convert only when executing against quad store

**File:** `lib/triple_store/sparql/executor.ex`

---

## Part 5: Unit Tests

### Task 5.1: Quad BGP detection tests
- [x] Test `is_quad_bgp?` returns true for quad patterns
- [x] Test `is_quad_bgp?` returns false for triple patterns
- [x] Test `is_quad_bgp?` handles mixed patterns

**File:** `test/triple_store/sparql/quad_bgp_test.exs`

### Task 5.2: Quad pattern execution tests
- [x] Test quad pattern with bound graph
- [x] Test quad pattern with variable graph
- [x] Test quad pattern with default graph

**File:** `test/triple_store/sparql/quad_bgp_test.exs`

### Task 5.3: Integration tests
- [x] Test BGP with multiple quad patterns
- [x] Test BGP with mixed quad and triple patterns
- [x] Test cross-graph joins with graph variable

**File:** `test/triple_store/sparql/quad_bgp_test.exs`

---

## Summary

**Status: COMPLETED**

This section implements true quad BGP execution:
1. [x] Extend BGP to handle quad patterns directly
2. [x] Implement quad-specific index selection and scanning
3. [x] Handle graph variable binding in joins
4. [x] Maintain backward compatibility with triple queries

## Test Results

```
test/triple_store/sparql/quad_bgp_test.exs:13 tests, 0 failures
```

## Implementation Details

### Added Functions

1. **`is_quad_bgp?/1`** - Public helper to detect quad patterns in BGPs
2. **`execute_single_quad_pattern/6`** - Execute single quad pattern
3. **`term_to_index_pattern_for_graph/3`** - Convert graph term to index pattern
4. **`extend_binding_from_quad_match/10`** - Extend binding from quad match
5. **`maybe_bind_graph/4`** - Handle graph variable binding

### Pattern Support

The executor now directly handles quad patterns in BGP execution:
- `{:quad, s, p, o, :default_graph}` - Default graph queries
- `{:quad, s, p, o, {:named_node, iri}}` - Named graph queries
- `{:quad, s, p, o, {:variable, "g"}}` - Graph variable queries

### Backward Compatibility

- All-triple BGPs continue to work as before
- Triple patterns in quad BGPs are implicitly in default graph
- Existing tests continue to pass

## Next Steps

After this section, Phase 3.4 (Graph-Specific Optimizations) will add optimization
for graph-scoped queries.

---

## Key Files Modified

1. `lib/triple_store/sparql/executor.ex` - Quad BGP execution functions
2. `test/triple_store/sparql/quad_bgp_test.exs` - New test file
