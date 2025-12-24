# Task 3.2.3: Join Enumeration - Summary

## Overview

Implemented join enumeration for SPARQL query optimization. The module enumerates different join orderings and selects the optimal execution plan based on cost estimates, using exhaustive enumeration for small queries and the DPccp algorithm for larger queries.

## Files Created

### Implementation
- `lib/triple_store/sparql/join_enumeration.ex` (~540 lines)
  - Exhaustive enumeration for small queries (n <= 5)
  - DPccp algorithm for larger queries
  - Join graph construction
  - Cartesian product pruning
  - Strategy selection (nested loop, hash join, Leapfrog)

### Tests
- `test/triple_store/sparql/join_enumeration_test.exs` (~540 lines)
  - 44 comprehensive tests covering all functionality

## Key Functions

### Main Entry Point

```elixir
@spec enumerate([pattern()], stats()) :: {:ok, plan()} | {:error, term()}
```

Takes a list of triple patterns and returns an optimized execution plan.

### Join Graph Construction

```elixir
@spec build_join_graph([pattern()]) :: %{non_neg_integer() => pattern_set()}
@spec shared_variables(pattern(), pattern()) :: [String.t()]
@spec sets_connected?(pattern_set(), pattern_set(), join_graph()) :: boolean()
```

Builds a graph where patterns are nodes and edges represent shared variables.

### Pattern Analysis

```elixir
@spec pattern_variables(pattern()) :: [String.t()]
@spec shared_variables_between_sets([pattern()], pattern_set(), pattern_set()) :: [String.t()]
```

## Algorithm Details

### Exhaustive Enumeration (n <= 5)

For small queries:
1. Generate all permutations of patterns
2. Build left-deep plans for each ordering
3. Prune orderings that create unnecessary Cartesian products
4. Select the plan with lowest total cost

Complexity: O(n!)

### DPccp Algorithm (n > 5)

Dynamic Programming with Connected Complement Pairs:
1. Initialize memoization table with single-pattern plans
2. For each subset size 2 to n:
   - Generate all connected complement pairs (ccp)
   - Find best way to join each pair from memoized subplans
   - Store best plan for each subset
3. Return optimal plan for full pattern set

Complexity: O(3^n) - much better than O(n!) for large n

### Cartesian Product Handling

- First attempts to find plans without Cartesian products
- Falls back to allowing Cartesian products only if necessary
- Empty join_vars in a join node indicates Cartesian product

### Leapfrog Selection

Considers Leapfrog Triejoin when:
- 4+ patterns in the BGP
- At least one variable appears in 3+ patterns
- Cost model indicates Leapfrog is cheaper than pairwise cascade

## Plan Structure

```elixir
@type plan_node ::
  {:scan, pattern()}
  | {:join, strategy(), plan_node(), plan_node(), [String.t()]}
  | {:leapfrog, [pattern()], [String.t()]}

@type plan :: %{
  tree: plan_node(),
  cost: CostModel.cost(),
  cardinality: float()
}
```

## Integration Points

- **Cardinality module**: Uses cardinality estimates for pattern and join selectivity
- **CostModel module**: Uses cost estimates for strategy selection
- **Optimizer**: Will integrate with existing BGP optimization

## Test Coverage

- Pattern variable extraction
- Shared variable detection
- Join graph construction
- Sets connectivity checking
- Single pattern enumeration
- Two pattern enumeration (connected and disconnected)
- Three pattern enumeration (chain and star)
- Exhaustive enumeration (up to 5 patterns)
- DPccp enumeration (6+ patterns)
- Leapfrog selection
- Plan structure validation
- Cartesian product handling
- Cost comparison
- Edge cases (blank nodes, empty stats, all bound patterns)

## Design Decisions

1. **Exhaustive threshold at 5**: Balances optimality (exhaustive finds best) vs speed (DPccp is faster for large n)
2. **Left-deep plans**: Exhaustive enumeration builds left-deep trees for simplicity
3. **Bushy plans**: DPccp can produce bushy join trees for better parallelization
4. **Cartesian fallback**: Only allows Cartesian products when no connected ordering exists
5. **Leapfrog as alternative**: Can replace entire pairwise cascade for qualifying queries

## Next Steps

Task 3.2.4 (Plan Cache) will cache optimized plans for repeated queries, avoiding re-enumeration for identical or similar queries.
