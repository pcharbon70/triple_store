# Task 3.2.2: Cost Model - Summary

## Overview

Implemented a cost model for SPARQL query optimization. The module estimates execution costs for different join strategies and index operations, enabling the cost-based optimizer to select efficient execution plans.

## Files Created

### Implementation
- `lib/triple_store/sparql/cost_model.ex` (~620 lines)
  - Nested loop join cost model
  - Hash join cost model
  - Leapfrog Triejoin cost model (AGM bound)
  - Index scan cost model
  - Strategy selection functions

### Tests
- `test/triple_store/sparql/cost_model_test.exs` (~570 lines)
  - 53 comprehensive tests covering all functionality

## Key Functions

### Join Cost Models

```elixir
@spec nested_loop_cost(number(), number()) :: cost()
@spec hash_join_cost(number(), number()) :: cost()
@spec leapfrog_cost([number()], [String.t()], stats()) :: cost()
```

### Index Scan Costs

```elixir
@spec index_scan_cost(scan_type(), number(), stats()) :: cost()
@spec pattern_scan_type(triple_pattern()) :: scan_type()
@spec pattern_cost(triple_pattern(), stats()) :: cost()
```

### Strategy Selection

```elixir
@spec select_join_strategy(number(), number(), [String.t()], stats()) :: {join_strategy(), cost()}
@spec should_use_leapfrog?([number()], [String.t()], stats()) :: boolean()
```

## Cost Model Details

### Nested Loop Join
- CPU: O(left * right) comparisons
- Memory: O(right) for materializing inner relation
- I/O: 0 (inputs assumed materialized)
- Best for: Very small inputs, early termination scenarios

### Hash Join
- CPU: O(left) for building + O(right) for probing
- Memory: O(left) for hash table
- I/O: 0 (inputs assumed materialized)
- Best for: Medium to large inputs with good hash distribution

### Leapfrog Triejoin
- Based on AGM bound approximation
- CPU: O(k * OUT * log(N)) where k = patterns, OUT = output size
- Memory: O(k) for iterator state
- I/O: O(seeks) for index positioning
- Best for: 3+ pattern multi-way joins with shared variables

### Index Scan Types

| Scan Type | Use Case | Cost Characteristics |
|-----------|----------|---------------------|
| `:point_lookup` | Fully bound pattern (S,P,O) | O(1) seek, O(1) read |
| `:prefix_scan` | Partially bound pattern | O(1) seek, O(results) read |
| `:full_scan` | Unbound pattern (?,?,?) | O(N) read |

## Cost Components

Each cost estimate includes:
- `cpu`: Estimated CPU operations
- `io`: Estimated I/O operations
- `memory`: Estimated memory usage
- `total`: Weighted combination for comparison

## Cost Weights (Tunable Constants)

```elixir
@comparison_cost 1.0
@hash_cost 2.0
@hash_probe_cost 1.5
@index_seek_cost 10.0
@sequential_read_cost 0.1
@memory_weight 1.0
@leapfrog_seek_cost 5.0
@leapfrog_comparison_cost 1.5
@hash_join_threshold 100
```

## Integration Points

- **Cardinality module**: Uses cardinality estimates for output size predictions
- **Optimizer**: Will use cost model for plan selection
- **Executor**: Strategy selection determines join algorithm

## Test Coverage

- Nested loop cost (complexity, scaling, components)
- Hash join cost (complexity, scaling, memory)
- Leapfrog cost (AGM bound, pattern count, join variables)
- Index scan cost (all scan types)
- Pattern scan type detection
- Strategy selection (threshold behavior)
- Cost utilities (total, compare)
- Edge cases (zero cardinality, empty stats, large values)
- Integration scenarios (star queries, chain queries)

## Design Decisions

1. **Abstract cost units**: Costs are relative, not absolute time predictions
2. **Component breakdown**: Separate CPU, I/O, memory for fine-grained analysis
3. **Tunable weights**: Constants can be adjusted based on benchmarking
4. **AGM bound approximation**: Simplified geometric mean approach for Leapfrog
5. **Hash join threshold**: 100 tuples before preferring hash over nested loop

## Next Steps

Task 3.2.3 (Join Enumeration) will use this cost model to enumerate and compare different join orderings, selecting the optimal execution plan.
