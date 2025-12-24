# Task 3.2.1: Cardinality Estimation - Summary

## Overview

Implemented cardinality estimation for SPARQL query optimization. The module estimates the number of results (cardinality) for triple patterns and joins, enabling the cost-based optimizer to select efficient execution plans.

## Files Created

### Implementation
- `lib/triple_store/sparql/cardinality.ex` (~380 lines)
  - Pattern cardinality estimation
  - Predicate histogram integration
  - Join cardinality estimation
  - Multi-pattern estimation

### Tests
- `test/triple_store/sparql/cardinality_test.exs` (~380 lines)
  - 44 comprehensive tests covering all functionality

## Key Functions

### Pattern Estimation
```elixir
@spec estimate_pattern(triple_pattern(), stats()) :: cardinality()
@spec estimate_pattern_with_bindings(triple_pattern(), stats(), bindings()) :: cardinality()
```

Estimates cardinality based on:
- Pattern structure (bound vs unbound positions)
- Predicate histogram for specific predicate selectivity
- Position selectivity (1/distinct_count for bound positions)

### Join Estimation
```elixir
@spec estimate_join(cardinality(), cardinality(), [String.t()], stats()) :: cardinality()
@spec estimate_multi_pattern([triple_pattern()], stats()) :: cardinality()
```

Uses independence assumption:
- `card(A ⋈ B) = card(A) * card(B) * join_selectivity`
- Join selectivity based on join variable domain sizes

### Selectivity
```elixir
@spec estimate_selectivity(triple_pattern(), stats()) :: float()
```

Returns selectivity as fraction (0.0-1.0) of database matched.

## Selectivity Model

The estimation uses a standard selectivity model:

| Pattern Type | Selectivity Formula |
|-------------|---------------------|
| `(?s, ?p, ?o)` | 1.0 (all triples) |
| `(S, ?p, ?o)` | 1/distinct_subjects |
| `(?s, P, ?o)` | predicate_histogram[P]/triple_count or 1/distinct_predicates |
| `(?s, ?p, O)` | 1/distinct_objects |
| `(S, P, ?o)` | (1/distinct_subjects) * predicate_selectivity |
| Multiple bounds | Product of individual selectivities |

## Integration Points

- **Statistics.Cache**: Uses `predicate_histogram/1` for per-predicate counts
- **PatternUtils**: Leverages existing pattern classification utilities
- **Statistics module**: Falls back to basic counts when histogram unavailable

## Test Coverage

- Basic pattern cardinality (all/none bound)
- Single bound position (S, P, O separately)
- Multiple bound positions
- Pattern with pre-bound variables
- Join cardinality (varying join variables)
- Multi-pattern estimation
- Selectivity calculation
- Edge cases (empty stats, zero counts, large values)
- Integration with real database

## Design Decisions

1. **Predicate histogram priority**: When predicate is bound and histogram available, use exact count rather than estimate
2. **Independence assumption**: Join selectivity assumes statistical independence between patterns
3. **Minimum cardinality**: Always return at least 1.0 to avoid division-by-zero issues
4. **Default statistics**: Sensible defaults when statistics unavailable

## Performance Characteristics

- All estimates are O(1) operations
- No database access during estimation (uses cached statistics)
- Designed for frequent calls during query optimization

## Next Steps

Task 3.2.2 (Cost Model) will build on this cardinality estimation to compute actual execution costs for different query plans.
