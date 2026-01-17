# Section 5.3: Query Optimizer Adaptation - Summary

## Overview

Extended the query optimizer to handle quad patterns efficiently. The optimizer now accounts for the graph component when choosing execution plans, ordering patterns, and selecting indices. This completes the integration of Section 5.2's QuadCardinality module into the optimizer pipeline.

## Implementation Details

### Graph-Aware Cost Model (CostModel)

Added new cost functions to `TripleStore.SPARQL.CostModel`:

**`graph_switch_cost/0`**
- Returns cost estimate for switching between graphs
- CPU: 5.0 (context switching overhead)
- I/O: 20.0 (column family prefix change)
- Memory: 0 (no additional memory)

**`cross_graph_join_cost/3`**
- Estimates cost of joins across different graphs
- Single graph (num_graphs=1): Same as hash join
- Multi-graph: Multiplies by `log(num_graphs + 1)` for logarithmic scaling
- Avoids over-penalizing queries with many small graphs

**`quad_index_scan_cost/3`**
- Index scan cost for quad patterns
- Supports `:point_lookup`, `:prefix_scan`, `:full_scan`
- Uses `quad_count` from stats when available, falls back to `triple_count`

**`quad_pattern_scan_type/1`**
- Determines scan type based on bound positions
- 4 bound positions → `:point_lookup`
- 0 bound positions → `:full_scan`
- Otherwise → `:prefix_scan`

**`quad_pattern_cost/2`**
- Pattern cost estimation with QuadCardinality integration
- Uses `TripleStore.SPARQL.QuadCardinality.estimate_pattern/2` when available
- Falls back to position-based estimation otherwise

### Graph Grouping (Optimizer)

Added graph grouping functions to `TripleStore.SPARQL.Optimizer`:

**`is_quad_pattern?/1`**
- Public API to check if a pattern is a quad pattern
- Returns `true` for `{:quad, _, _, _, _}`, `false` otherwise

**`extract_graph_key/1`**
- Extracts graph key from pattern for grouping
- Returns:
  - `:default_graph` for default graph quads or triple patterns
  - `{:named_graph, iri}` for named graphs
  - `{:variable, name}` for graph variables
  - `:cross_graph` for unknown graph terms

**`group_patterns_by_graph/1`**
- Groups patterns by their graph binding
- Returns map of graph key to list of patterns
- Enables graph-local optimization

**`reorder_patterns_with_graph_grouping/2`**
- Reorders patterns with graph awareness
- Groups by graph, then applies greedy selectivity within each group
- Order: bound graphs → shared graph variables → cross-graph

**Modified `reorder_patterns/2`**
- Now detects quad patterns and uses graph grouping
- Falls back to greedy algorithm for triple-only patterns

## Type Specifications

Added new types to CostModel:

```elixir
@type quad_pattern :: {:quad, pattern_term(), pattern_term(), pattern_term(), pattern_term()}

@type pattern_term ::
        {:variable, String.t()}
        | {:named_node, String.t()}
        | {:literal, :plain, String.t(), String.t() | nil}
        | {:literal, :typed, String.t(), String.t()}
        | {:literal, :language, String.t(), String.t()}
        | {:blank_node, String.t()}
        | :default_graph
        | integer()
```

## Test Coverage

Extended `test/triple_store/sparql/graph_optimization_test.exs` with 28 new tests:

| Test Category | Tests | Description |
|--------------|-------|-------------|
| `is_quad_pattern?/1` | 3 | Quad pattern detection |
| `extract_graph_key/1` | 5 | Graph key extraction |
| `group_patterns_by_graph/1` | 3 | Pattern grouping |
| `reorder_patterns_with_graph_grouping` | 3 | Graph-aware reordering |
| `CostModel.graph_switch_cost/0` | 1 | Graph switch cost |
| `CostModel.cross_graph_join_cost/3` | 3 | Cross-graph join costs |
| `CostModel.quad_pattern_scan_type/1` | 3 | Scan type detection |
| `CostModel.quad_index_scan_cost/3` | 4 | Index scan costs |
| `CostModel.quad_pattern_cost/2` | 3 | Pattern cost estimation |

**Test Results**: 42 tests, 0 failures

## Design Decisions

1. **Logarithmic Scaling for Cross-Graph Joins**: Used `log(n + 1)` multiplier instead of linear scaling to avoid over-penalizing queries with many small graphs.

2. **Graph Grouping Before Selectivity**: Patterns are grouped by graph first, then selectivity-based reordering is applied within each group. This minimizes graph switches.

3. **QuadCardinality Integration**: CostModel integrates with QuadCardinality when available, but has fallback logic for backward compatibility.

4. **Module Extension Over New Module**: Extended existing Optimizer and CostModel modules rather than creating separate QuadOptimizer module for consistency.

5. **Public API Functions**: Made `is_quad_pattern?/1`, `extract_graph_key/1`, and `group_patterns_by_graph/1` public for external use and testing.

## Files Changed

### Modified
- `lib/triple_store/sparql/cost_model.ex` - Added graph-aware cost functions
- `lib/triple_store/sparql/optimizer.ex` - Added graph grouping functions
- `test/triple_store/sparql/graph_optimization_test.exs` - Extended with 28 new tests

### Created
- `notes/feature/section-5.3-query-optimizer-adaptation.md` (working plan)
- `notes/summaries/section-5.3-query-optimizer-adaptation.md` (this file)

## Integration Points

This section integrates with:
- **TripleStore.SPARQL.QuadCardinality** (Section 5.2) - For accurate cardinality estimates
- **TripleStore.QuadIndex** (Section 3.4) - For index selection
- **TripleStore.SPARQL.Executor** - For pattern execution

## Next Steps

This completes Section 5.3 of Phase 5 (Statistics and Optimization). The query optimizer now fully supports quad patterns with:
- Graph-aware pattern reordering
- Graph grouping for efficient execution
- Accurate cardinality estimation using QuadCardinality
- Graph-aware cost estimation

Remaining Phase 5 sections:
- Section 5.4: Statistics Cache
- Section 5.5: Leapfrog Triejoin for Quads

## Example Usage

```elixir
# Graph grouping
patterns = [
  {:quad, {:variable, "s1"}, {:variable, "p"}, {:variable, "o1"}, {:variable, "g"}},
  {:quad, {:variable, "s2"}, {:variable, "p"}, {:variable, "o2"}, :default_graph}
]
groups = Optimizer.group_patterns_by_graph(patterns)
# => %{{:variable, "g"} => [...], :default_graph => [...]}

# Graph-aware cost estimation
cost = CostModel.cross_graph_join_cost(1000, 500, 5)
# => %{cpu: ..., io: ..., memory: ..., total: ...}

# Quad pattern cost with statistics
pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
stats = %{quad_count: 10_000, per_graph_stats: %{0 => %{quad_count: 5000}}}
cost = CostModel.quad_pattern_cost(pattern, stats)
# => Uses QuadCardinality for accurate estimation
```
