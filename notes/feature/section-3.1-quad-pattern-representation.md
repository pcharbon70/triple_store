# Working Plan: Section 3.1 - Quad Pattern Representation

## Branch: `feature/section-3.1-quad-pattern-representation`

## Status: COMPLETED

## Overview

Section 3.1 implements the core pattern representation for quad store query execution.
This extends the existing triple pattern representation to include a graph position, enabling
GRAPH clause execution in SPARQL queries.

---

## Part 1: Pattern Type Extension (3.1.1)

### Task 1.1.1: Define quad pattern type
- [x] Define `quad_pattern() :: {pattern_s, pattern_p, pattern_o, pattern_g}`
- [x] Each position is `:bound | {:var, atom()} | :var`
- [x] Update type specs in executor module

**File:** `lib/triple_store/sparql/executor.ex`

### Task 1.1.2: Update pattern type
- [x] Update `@type pattern :: triple_pattern() | quad_pattern()`
- [x] Define `triple_pattern() :: {pattern_s, pattern_p, pattern_o}`
- [x] Ensure backward compatibility with triple patterns

**File:** `lib/triple_store/sparql/executor.ex`

### Task 1.1.3: Add guard functions
- [x] Implement `is_quad_pattern/1` - returns true for 4-tuple patterns
- [x] Implement `is_triple_pattern/1` - returns true for 3-tuple patterns
- [x] Add `@spec` for both guard functions

**File:** `lib/triple_store/sparql/executor.ex`

---

## Part 2: Pattern Conversion (3.1.2)

### Task 2.1: Implement triple_pattern_to_quad/2
- [x] Create function to convert triple pattern to quad pattern
- [x] Signature: `triple_pattern_to_quad(triple_pattern, graph_context) :: quad_pattern`
- [x] Add graph context as 4th element

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.2: Handle default graph context
- [x] When `graph_context == :default`, bind to `@default_graph_id` (0)
- [x] Use `:bound` wrapper for default graph ID
- [x] Ensure default graph queries work correctly

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.3: Handle named graph context
- [x] When `graph_context` is an IRI, bind to specific graph ID
- [x] Use `:bound` wrapper for named graph ID
- [x] Validate graph exists in store

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.4: Handle graph variable context
- [x] When `graph_context` is a variable, add as pattern variable
- [x] Use `{:var, atom()}` format for graph variable
- [x] Preserve variable bindings through conversion

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.5: Handle nil graph context
- [x] When `graph_context == nil`, treat as unbound
- [x] Use `:var` for graph position (matches any graph)
- [x] For cross-graph queries

**File:** `lib/triple_store/sparql/executor.ex`

---

## Part 3: Variable Binding Extension (3.1.3)

### Task 3.1: Update binding type documentation
- [x] Document that graph variable can appear in bindings
- [x] Graph variable binds to graph term IRIs (not integers)
- [x] Default graph (ID 0) never appears in bindings

**File:** `lib/triple_store/sparql/executor.ex` (moduledoc)

### Task 3.2: Add binding_has_graph?/2 helper
- [x] Check if binding contains a specific graph variable
- [x] Signature: `binding_has_graph?(binding, graph_var) :: boolean()`
- [x] Returns true if graph_var is bound in the binding

**File:** `lib/triple_store/sparql/executor.ex`

### Task 3.3: Add extract_graph_from_binding/2
- [x] Extract graph ID or variable from binding
- [x] Signature: `extract_graph_from_binding(binding, graph_var) :: {:bound, id} | {:var, atom()} | :not_bound`
- [x] Used by GRAPH clause execution

**File:** `lib/triple_store/sparql/executor.ex`

---

## Part 4: Unit Tests

### Task 4.1: Pattern type tests
- [x] Test `is_quad_pattern?` detects 4-tuple patterns
- [x] Test `is_triple_pattern?` detects 3-tuple patterns
- [x] Test `is_quad_pattern?` returns false for triple patterns
- [x] Test `is_triple_pattern?` returns false for quad patterns

**File:** `test/triple_store/sparql/quad_pattern_test.exs`

### Task 4.2: Pattern conversion tests
- [x] Test triple pattern converts to quad with default graph
- [x] Test triple pattern converts to quad with named graph ID
- [x] Test triple pattern converts to quad with graph variable
- [x] Test triple pattern converts to quad with nil (unbound) graph

**File:** `test/triple_store/sparql/quad_pattern_test.exs`

### Task 4.3: Binding helper tests
- [x] Test `binding_has_graph?` returns true when graph var bound
- [x] Test `binding_has_graph?` returns false when graph var absent
- [x] Test `extract_graph_from_binding` with bound graph
- [x] Test `extract_graph_from_binding` with variable graph
- [x] Test `extract_graph_from_binding` with missing graph

**File:** `test/triple_store/sparql/quad_pattern_test.exs`

---

## Summary

**Status: COMPLETED**

This section implements the foundational pattern types for quad query execution.
All tasks completed:

1. [x] Quad patterns are properly typed with 4 positions (s, p, o, g)
2. [x] Triple patterns can be converted to quad patterns with graph context
3. [x] Helper functions for graph variable binding
4. [x] Unit tests verifying pattern handling (27 tests, all passing)

## Test Results

```
test/triple_store/sparql/quad_pattern_test.exs:27 tests, 0 failures
```

## Next Steps

After this section, Phase 3.2 (GRAPH Clause Execution) will use these patterns
to implement actual query execution with GRAPH clauses.

---

## Key Files to Modify

1. `lib/triple_store/sparql/executor.ex` - Pattern types and conversion functions
2. `test/triple_store/sparql/quad_pattern_test.exs` - New test file
