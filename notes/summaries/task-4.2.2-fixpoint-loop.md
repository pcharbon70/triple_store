# Task 4.2.2: Fixpoint Loop Summary

**Date:** 2025-12-25
**Branch:** feature/4.2.2-fixpoint-loop

## Overview

This task implements the fixpoint iteration loop for semi-naive evaluation. The SemiNaive module provides the main entry point for forward-chaining materialization, iterating rules until no new facts can be derived.

## Implementation

### Core Module: `TripleStore.Reasoner.SemiNaive`

Location: `lib/triple_store/reasoner/semi_naive.ex`

The module provides the following key functions:

#### `materialize/5`

Main entry point for forward-chaining materialization with external storage.

```elixir
@spec materialize(lookup_fn, store_fn, rules, initial_facts, opts) ::
  {:ok, stats} | {:error, term()}
```

Parameters:
- `lookup_fn` - Function to look up facts: `(pattern) -> {:ok, [triple]} | {:error, reason}`
- `store_fn` - Function to store derived facts: `(fact_set) -> :ok | {:error, reason}`
- `rules` - List of reasoning rules to apply
- `initial_facts` - Initial set of explicit facts (used as first delta)
- `opts` - Options (max_iterations, max_facts, trace, emit_telemetry)

Returns statistics:
- `iterations` - Number of fixpoint iterations
- `total_derived` - Total facts derived
- `derivations_per_iteration` - List of derivation counts per iteration
- `duration_ms` - Total time in milliseconds
- `rules_applied` - Total rule applications

#### `materialize_in_memory/3`

Convenience function that maintains facts in memory using an Agent.

```elixir
@spec materialize_in_memory(rules, initial_facts, opts) ::
  {:ok, all_facts, stats} | {:error, term()}
```

Useful for testing and small datasets.

#### `stratify_rules/1`

Stratifies rules based on negation dependencies.

```elixir
@spec stratify_rules(rules) :: [stratum]
```

For OWL 2 RL (which has no negation), all rules are placed in stratum 0.
Future extensibility for stratified negation is built in.

#### `compute_stats/2`

Computes statistics about materialization state.

```elixir
@spec compute_stats(all_facts, initial_count) :: map()
```

Returns total facts, derived facts, and expansion ratio.

### Algorithm

1. **Initialization**: Set delta = all explicit facts
2. **Stratification**: Group rules by stratum (all stratum 0 for OWL 2 RL)
3. **Iteration Loop**: While delta is non-empty:
   - For each stratum, apply all rules using DeltaComputation
   - Collect new derivations not already in database
   - Store new derivations
   - Set delta = new derivations
   - Track iteration count and derivation statistics
4. **Termination**: When delta is empty, fixpoint is reached

### Safety Limits

- `max_iterations` - Default 1000, prevents infinite loops
- `max_facts` - Default 10,000,000, prevents memory exhaustion

### Telemetry Integration

Emits telemetry events:
- `[:triple_store, :reasoner, :materialize, :start]`
- `[:triple_store, :reasoner, :materialize, :stop]`
- `[:triple_store, :reasoner, :materialize, :iteration]`

## Test Coverage

Location: `test/triple_store/reasoner/semi_naive_test.exs`

19 new tests covering:

| Category | Tests |
|----------|-------|
| Basic materialization | 5 |
| Transitive closure | 2 |
| Multiple rule interaction | 2 |
| Limits and error handling | 2 |
| Statistics | 2 |
| Rule stratification | 2 |
| External store callbacks | 2 |
| Complex scenarios | 2 |

### Test Scenarios

- Simple class hierarchy materialization
- Transitive closure of subClassOf and transitive properties
- Diamond inheritance patterns
- Multiple instances with shared hierarchy
- sameAs equality propagation
- Max iterations and max facts limits
- Lookup and store function callbacks

## Files Created/Modified

| File | Description |
|------|-------------|
| `lib/triple_store/reasoner/semi_naive.ex` | New module (410 lines) |
| `test/triple_store/reasoner/semi_naive_test.exs` | New test file (19 tests) |

## Test Results

```
4 properties, 2630 tests, 0 failures
```

- Previous test count: 2611
- New tests added: 19
- All tests pass

## Design Decisions

1. **Callback-based storage**: Uses lookup_fn and store_fn for flexibility with different storage backends
2. **In-memory convenience**: materialize_in_memory/3 uses Agent for simple cases
3. **Stratification framework**: Built-in support for future stratified negation
4. **Defensive limits**: max_iterations and max_facts prevent runaway computation
5. **Rich statistics**: Tracks iterations, derivations, timing for monitoring
6. **Telemetry integration**: Uses existing Telemetry module for observability

## Performance Characteristics

- **Time complexity**: O(|derived| × |rules| × avg_rule_cost)
- **Space complexity**: O(|facts|) for fact storage
- **Convergence**: Guaranteed for monotonic rules (no negation)
- **Typical iterations**: 3-10 for most ontologies

## Integration Points

- `TripleStore.Reasoner.DeltaComputation` - Core delta application
- `TripleStore.Reasoner.Rule` - Rule structure and delta positions
- `TripleStore.Reasoner.Rules` - Standard OWL 2 RL rule definitions
- `TripleStore.Reasoner.Telemetry` - Event emission

## Next Steps

Task 4.2.3 (Parallel Rule Evaluation) will add:
- Parallel rule application via Task.async_stream
- Merging results from parallel applications
- Configurable parallelism level
- Deterministic results despite parallelism
