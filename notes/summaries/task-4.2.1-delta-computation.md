# Task 4.2.1: Delta Computation Summary

**Date:** 2025-12-25
**Branch:** feature/4.2.1-delta-computation

## Overview

This task implements delta computation for semi-naive evaluation of reasoning rules. Semi-naive evaluation is an optimization over naive fixpoint iteration that processes only newly derived facts (delta) in each iteration, dramatically reducing redundant computation.

## Implementation

### Core Module: `TripleStore.Reasoner.DeltaComputation`

Location: `lib/triple_store/reasoner/delta_computation.ex`

The module provides the following key functions:

#### `apply_rule_delta/5`

Main entry point for semi-naive rule application.

```elixir
@spec apply_rule_delta(
  lookup_fn,    # Function to look up facts matching a pattern
  rule,         # The reasoning rule to apply
  delta,        # Set of new facts from previous iteration
  existing,     # Set of all existing facts
  opts          # Options (max_derivations, trace)
) :: {:ok, fact_set()} | {:error, term()}
```

Features:
- Uses delta positions from rule metadata
- Indexes delta facts by predicate for efficient lookup
- Applies rule for each delta position
- Filters out existing facts from derivations
- Respects `max_derivations` limit (default: 100,000)

#### `generate_bindings/6`

Generates all variable bindings from matching a rule body against a database.

```elixir
@spec generate_bindings(
  lookup_fn,
  patterns,
  delta,
  delta_index,
  delta_pos,
  conditions
) :: [binding()]
```

Features:
- Iteratively builds bindings through pattern matching
- Uses delta facts at specified position, full database elsewhere
- Applies filter conditions to resulting bindings
- Handles variable unification correctly

#### `instantiate_head/2`

Instantiates a rule head pattern with a binding to produce a ground triple.

```elixir
@spec instantiate_head(pattern, binding) :: triple() | nil
```

Returns `nil` if any variables remain unbound.

#### Helper Functions

- `index_by_predicate/1` - Creates predicate-indexed view of facts for efficient lookup
- `ground_term?/1` - Checks if a term contains no variables
- `filter_existing/2` - Removes existing facts from derived set
- `merge_delta/2` - Combines existing and delta facts for next iteration

### Algorithm Details

For a rule with n body patterns, we generate derivations by trying each pattern position as the "delta position":

1. For each delta position i in [0, n-1]:
   - Use delta facts for pattern i
   - Use full database for all other patterns
   - Generate all valid bindings through pattern matching
   - Instantiate head for each binding
   - Collect derived facts

2. Union all derivations from all delta positions
3. Filter out facts that already exist
4. Return new facts as the next delta

This ensures we find all new derivations without redundantly reprocessing combinations of old facts that were already explored in previous iterations.

### Pattern Matching Optimization

When looking up facts from delta:
- If predicate is ground, use the predicate index for O(1) lookup
- If predicate is variable, scan all delta facts

This optimization is important because most patterns have ground predicates (e.g., `rdf:type`, `rdfs:subClassOf`).

## Test Coverage

Location: `test/triple_store/reasoner/delta_computation_test.exs`

25 new tests covering:

| Category | Tests |
|----------|-------|
| Basic delta application | 5 |
| Transitive rules | 2 |
| sameAs rules | 2 |
| Binding generation | 3 |
| Head instantiation | 3 |
| Predicate indexing | 2 |
| Ground term check | 4 |
| Utility functions | 2 |
| Delta position handling | 1 |
| Complex multi-pattern rules | 1 |

## Integration Points

The DeltaComputation module integrates with:

- `TripleStore.Reasoner.Rule` - Uses `delta_positions/1`, `body_patterns/1`, `body_conditions/1`, `substitute/2`, `evaluate_condition/2`
- `TripleStore.Reasoner.Rules` - Uses standard rule definitions for testing

## Files Created/Modified

| File | Description |
|------|-------------|
| `lib/triple_store/reasoner/delta_computation.ex` | New module (312 lines) |
| `test/triple_store/reasoner/delta_computation_test.exs` | New test file (25 tests) |

## Test Results

```
4 properties, 2611 tests, 0 failures
```

- Previous test count: 2586
- New tests added: 25
- All tests pass

## Design Decisions

1. **MapSet for fact storage**: Uses `MapSet` for O(1) membership testing and set operations
2. **Predicate indexing**: Groups delta facts by predicate for efficient lookup
3. **Configurable limits**: `max_derivations` option prevents runaway computation
4. **Separation of concerns**: Lookup function passed as parameter, decoupling from storage layer
5. **Rule.delta_positions/1 integration**: Uses metadata from Rule module for delta position hints

## Next Steps

Task 4.2.2 (Fixpoint Loop) will use this module to implement:
- `materialize(db, rules)` main entry point
- Initialize delta with all explicit facts
- Loop applying rules until delta is empty
- Track iteration count and derivation statistics
