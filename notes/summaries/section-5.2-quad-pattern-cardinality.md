# Section 5.2: Quad Pattern Cardinality Estimation - Summary

## Overview

Implemented cardinality estimation for quad patterns in SPARQL queries. The query optimizer needs accurate estimates of how many quads match a given pattern to choose optimal execution plans. This section extends the existing triple pattern estimation to handle the fourth component (graph).

## Implementation Details

### New Module: TripleStore.SPARQL.QuadCardinality

Created a dedicated module for quad pattern cardinality estimation, separate from the existing `TripleStore.SPARQL.Cardinality` module for triple patterns.

#### Core Functions

**`estimate_pattern/2`**
- Main entry point for quad pattern cardinality estimation
- Detects whether graph is bound (graph-scoped) or unbound (cross-graph)
- Routes to appropriate estimation function

**`estimate_graph_scoped_pattern/4`**
- Handles patterns with bound graph (specific named graph or default)
- Uses per-graph statistics from `Statistics.graph_summary/3`
- More accurate for graph-scoped queries

**`estimate_cross_graph_pattern/3`**
- Handles patterns with unbound graph (variable)
- Iterates through all graphs and sums estimates
- Falls back to aggregate statistics when per-graph stats unavailable

**`position_selectivity/4`**
- Calculates selectivity for each position (S, P, O, G)
- Formula: `1 / distinct_count` for bound constants
- Returns `1.0` for unbound variables

**`estimate_quad_join/5`**
- Estimates cardinality of joining two quad patterns
- Accounts for graph variable sharing (same graph vs different graphs)
- Applies join selectivity based on variable domains

**`estimate_multi_quad_pattern/2`**
- Estimates cardinality for multiple quad patterns joined together
- Accumulates bindings and joins patterns left-to-right
- Handles both same-graph and cross-graph joins

**`estimate_pattern_with_bindings/3`**
- Estimates cardinality when some variables are already bound
- Uses graph-specific distinct counts for binding adjustment
- Important for multi-join optimization

### Statistics Map Format

The stats map supports both triple and quad counts for backward compatibility:

```elixir
%{
  # Aggregate statistics
  quad_count: 15_000,           # Total quads (primary)
  triple_count: 10_000,         # Total triples (backward compat)
  distinct_subjects: 1_000,
  distinct_predicates: 50,
  distinct_objects: 2_000,
  total_graphs: 3,             # Number of graphs with data

  # Per-graph breakdown
  per_graph_stats: %{
    0 => %{                     # Default graph
      quad_count: 5_000,
      distinct_subjects: 500,
      distinct_predicates: 20,
      distinct_objects: 800,
      predicate_counts: %{42 => 1_000, 43 => 500}
    },
    123 => %{                   # Named graph
      quad_count: 10_000,
      distinct_subjects: 800,
      distinct_predicates: 40,
      distinct_objects: 1_500,
      predicate_counts: %{42 => 2_000}
    }
  }
}
```

### Pattern Representation

Uses existing SPARQL algebra format:

```elixir
# Quad pattern: {:quad, subject, predicate, object, graph}
{:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}        # Graph-scoped
{:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}  # Cross-graph
{:quad, 123, 456, 789, 0}         # Fully bound
{:quad, {:variable, "s"}, 42, {:variable, "o"}, :default_graph}  # Default graph
```

## Test Coverage

Created `test/triple_store/sparql/quad_cardinality_test.exs` with 41 tests:

| Test Category | Tests | Description |
|--------------|-------|-------------|
| `estimate_pattern/2` | 9 | Graph-scoped, cross-graph, bound positions |
| `position_selectivity/4` | 6 | S, P, O, G position selectivity |
| `estimate_cross_graph_pattern/3` | 5 | Sum across graphs, fallback, edge cases |
| `estimate_quad_join/5` | 5 | Same/different graph joins, join variables |
| `estimate_multi_quad_pattern/2` | 5 | Multi-pattern joins, graph grouping |
| `estimate_pattern_with_bindings/3` | 4 | Binding adjustments, graph-specific counts |
| `estimate_selectivity/2` | 3 | Selectivity calculations |
| Edge cases | 4 | Empty stats, nil handling, defaults |

**Test Results:** 41 tests, 0 failures

## Design Decisions

1. **Separate Module**: Created `TripleStore.SPARQL.QuadCardinality` instead of extending `TripleStore.SPARQL.Cardinality` to keep concerns separate and avoid mixing triple/quad logic.

2. **Iteration for Cross-Graph**: Chose to iterate all graphs for cross-graph patterns instead of using pre-aggregated statistics. This is slower but more accurate, especially when graphs have very different sizes.

3. **Dual Count Support**: Stats map supports both `:triple_count` and `:quad_count` for backward compatibility with existing code.

4. **Graph-Specific Binding Adjustment**: When variables are already bound from previous joins, the binding adjustment uses graph-specific distinct counts when available, falling back to global counts for cross-graph patterns.

5. **Default Minimum Cardinality**: All estimates return at least 1.0 to avoid division by zero and to represent that a pattern could match at least one result.

## Files Changed

### Created
- `lib/triple_store/sparql/quad_cardinality.ex` (~700 lines)
- `test/triple_store/sparql/quad_cardinality_test.exs` (~550 lines)
- `notes/feature/section-5.2-quad-pattern-cardinality.md` (working plan)
- `notes/summaries/section-5.2-quad-pattern-cardinality.md` (this file)

## Integration Points

This module is designed to integrate with:
- **TripleStore.Statistics** - For per-graph statistics data
- **TripleStore.SPARQL.Optimizer** - For using cardinality estimates in query planning
- **TripleStore.SPARQL.Executor** - For quad pattern execution

## Next Steps

This completes Section 5.2 of Phase 5 (Statistics and Optimization). The quad pattern cardinality estimation functions are now available for:
- Query optimizer integration (Section 5.3)
- Cost-based optimization for named graph queries
- Statistics cache integration (Section 5.4)
- Leapfrog triejoin for quads (Section 5.5)

## Example Usage

```elixir
# Graph-scoped pattern
pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
stats = %{per_graph_stats: %{0 => %{quad_count: 5000, predicate_counts: %{42 => 1000}}}}
{:ok, graph_stats} = Statistics.graph_summary(db, 0)
card = QuadCardinality.estimate_pattern(pattern, %{per_graph_stats: %{0 => graph_stats}})
# => 1000.0

# Cross-graph pattern
pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}}
card = QuadCardinality.estimate_pattern(pattern, stats)
# => Sum of predicate 42 counts across all graphs

# Join estimation
card1 = QuadCardinality.estimate_pattern(pattern1, stats)
card2 = QuadCardinality.estimate_pattern(pattern2, stats)
join_card = QuadCardinality.estimate_quad_join(card1, card2, ["s"], true, stats)
```
