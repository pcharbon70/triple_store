# Task 4.3.2: Backward Phase - Summary

**Date:** 2025-12-26
**Branch:** feature/4.3.2-backward-phase

## Overview

Implemented the backward phase of the Backward/Forward deletion algorithm for incremental reasoning. When facts are deleted from a reasoned triple store, this module identifies all derived facts that may have depended on the deleted facts.

## Key Implementation Details

### New Module: `TripleStore.Reasoner.BackwardTrace`

Location: `lib/triple_store/reasoner/backward_trace.ex`

#### Main Functions

1. **`trace_in_memory/4`** - Traces backward from deleted facts to find all potentially invalid derived facts
   - Takes deleted facts, all derived facts, and rules
   - Returns set of potentially invalid facts with statistics
   - Handles recursive dependencies through the derivation graph

2. **`find_direct_dependents/3`** - Finds derived facts that directly depend on a given fact
   - Single-level dependency detection
   - Useful for checking immediate impact

3. **`could_derive?/4`** - Checks if a derived fact could have been produced by a rule using a given input fact
   - Pattern matching against rule head and body
   - Binding consistency verification

### Algorithm

The backward tracing algorithm works by:

1. **Initial Step**: Start with the set of deleted facts
2. **Pattern Matching**: For each deleted fact, find rules where it could match a body pattern
3. **Dependency Detection**: Find derived facts that match the corresponding rule heads
4. **Recursive Tracing**: Repeat for newly found potentially invalid facts
5. **Cycle Detection**: Track visited facts to prevent infinite loops
6. **Termination**: Stop when no new dependencies are found or max depth reached

### Key Design Decisions

1. **In-Memory First**: Focused on in-memory API for testing and correctness; database API can be added later
2. **Conservative Tracing**: Marks facts as potentially invalid even if alternative derivations might exist (forward phase handles re-derivation)
3. **Cycle Safe**: Uses visited set to prevent infinite loops in circular dependencies
4. **Configurable Depth**: `max_depth` option prevents runaway tracing in pathological cases

### Statistics Returned

```elixir
%{
  potentially_invalid: MapSet.t(),  # Facts that may need re-evaluation
  trace_depth: non_neg_integer(),    # Maximum depth reached
  facts_examined: non_neg_integer()  # Total facts examined
}
```

### Options

- `:max_depth` - Maximum recursion depth (default: 100)
- `:include_deleted` - Include deleted facts in result (default: false)

## Test Coverage

**New test file:** `test/triple_store/reasoner/backward_trace_test.exs`

25 new tests covering:

- Basic functionality (empty inputs, statistics)
- Class hierarchy dependencies (instance types, subclass relationships)
- Subclass transitivity dependencies
- sameAs reasoning (symmetry, transitivity)
- Recursive dependency tracing
- Cycle handling
- Options (max_depth, include_deleted)
- Direct dependent finding
- could_derive? predicate
- Edge cases (multiple rules, multiple deletions, explicit facts)

## Algorithm Correctness

The backward phase is intentionally **conservative** - it may mark facts as potentially invalid that could be re-derived through alternative paths. This is correct because:

1. The forward phase (Task 4.3.3) will attempt to re-derive each potentially invalid fact
2. Facts that can be re-derived will be kept
3. Only facts without alternative derivations will be deleted

This two-phase approach prevents **over-deletion** while ensuring correctness.

## Files Changed

| File | Change |
|------|--------|
| `lib/triple_store/reasoner/backward_trace.ex` | New module |
| `test/triple_store/reasoner/backward_trace_test.exs` | New test file |

## Test Results

```
25 tests, 0 failures
Total project: 2758 tests, 0 failures
```

## Next Steps

Task 4.3.2 is complete. The next tasks in Section 4.3 are:

- **4.3.3**: Forward Phase - Re-derive facts with alternative justifications
  - Implement `can_rederive?(db, fact)` checking for alternative derivations
  - Attempt re-derivation for each potentially invalid fact
  - Keep facts that can be re-derived, delete those that cannot

- **4.3.4**: Delete with Reasoning - Coordinate backward and forward phases

- **4.3.5**: Unit Tests - Additional tests for deletion with reasoning
