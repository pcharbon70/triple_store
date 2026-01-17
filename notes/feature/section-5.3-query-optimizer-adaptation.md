# Section 5.3: Query Optimizer Adaptation

## Overview

Extend the query optimizer to handle quad patterns efficiently. The optimizer needs to account for the graph component when choosing execution plans, ordering patterns, and selecting indices.

---

## 5.3.1 Quad Pattern Ordering

### Tasks

- [ ] 5.3.1.1 Update `reorder_bgp_patterns/2` for quad patterns
  - Extend existing BGP reordering to handle `{:quad, s, p, o, g}` patterns
  - Use `QuadCardinality.estimate_pattern/2` for selectivity
  - Maintain backward compatibility with triple patterns

- [ ] 5.3.1.2 Use `estimate_quad_pattern/2` for selectivity
  - Call `QuadCardinality.estimate_pattern/2` for quad patterns
  - Fall back to `Cardinality.estimate_pattern/2` for triple patterns
  - Get per-graph statistics when available

- [ ] 5.3.1.3 Prefer patterns with bound graph first
  - Graph-scoped patterns are more selective
  - Process bound-graph patterns before cross-graph patterns
  - Reduces intermediate result sizes

- [ ] 5.3.1.4 Within same graph, use existing heuristics
  - Apply existing triple ordering logic within each graph
  - Predicate selectivity, bound positions, etc.

- [ ] 5.3.1.5 Test ordering produces efficient plans
  - Verify quad patterns are ordered correctly
  - Verify mixed triple/quad patterns work

---

## 5.3.2 Index Selection for Quads

### Tasks

- [ ] 5.3.2.1 Use `QuadIndex.select_index_for_quad/1` in optimizer
  - Integrate with existing index selection logic
  - Handle quad patterns in cost calculation

- [ ] 5.3.2.2 Consider index access pattern in cost calculation
  - GSPO/GPOS for graph-scoped (prefix scan on graph)
  - SPOG/POSG for cross-graph (no graph prefix)
  - Account for index order efficiency

- [ ] 5.3.2.3 Prefer GSPO/GPOS for graph-scoped queries
  - GSPO: Graph-Subject-Predicate-Object (excellent for graph prefix)
  - GPOS: Graph-Predicate-Object-Subject (good for predicate-first in graph)
  - Both allow efficient prefix scan on graph_id

- [ ] 5.3.2.4 Prefer SPOG/POSG for cross-graph queries
  - SPOG: Subject-Predicate-Object-Graph (matches triple pattern with graph suffix)
  - POSG: Predicate-Object-Subject-Graph (good for predicate-first)
  - These don't have graph prefix, suitable for cross-graph

- [ ] 5.3.2.5 Document index selection strategy
  - Add documentation explaining quad index selection
  - Include examples and rationale

---

## 5.3.3 Graph-Aware Cost Model

### Tasks

- [ ] 5.3.3.1 Add cost for graph switching in execution
  - When patterns span different graphs, add switching cost
  - Model the cost of changing graph context

- [ ] 5.3.3.2 Add cost for cross-graph joins
  - Cross-graph joins may have higher cardinality
  - Account for independent graph domains

- [ ] 5.3.3.3 Model cost of iterating over graphs
  - Cost multiplier for cross-graph patterns
  - Based on number of graphs with data

- [ ] 5.3.3.4 Use per-graph statistics for accurate costing
  - Leverage per-graph stats from Statistics module
  - More accurate cost estimates for graph-scoped queries

- [ ] 5.3.3.5 Update total plan cost calculation
  - Incorporate graph-aware costs into total cost
  - Maintain backward compatibility with triple queries

---

## 5.3.4 Join Reordering with Graphs

### Tasks

- [ ] 5.3.4.1 Detect when graph variable is shared across patterns
  - Identify patterns that join on graph variable
  - This creates a graph constraint that reduces cardinality

- [ ] 5.3.4.2 Prefer joining on graph early when beneficial
  - Graph joins are highly selective
  - Order patterns to exploit graph sharing

- [ ] 5.3.4.3 Avoid unnecessary cross-graph joins
  - Detect when patterns are in disjoint graphs
  - Avoid planning inefficient cross-graph joins

- [ ] 5.3.4.4 Group patterns by graph when possible
  - Partition patterns by graph binding
  - Execute each group independently before combining

- [ ] 5.3.4.5 Document optimization strategy
  - Explain graph-aware join reordering
  - Include examples

---

## Implementation Notes

### Existing Code to Review

- **TripleStore.SPARQL.Optimizer** - Query optimizer
  - `reorder_bgp_patterns/2` - BGP pattern reordering
  - `calculate_plan_cost/2` - Cost calculation
  - Index selection logic

- **TripleStore.SPARQL.CostModel** - Cost model
  - `estimate_cost/2` - Cost estimation
  - `index_access_cost/2` - Index access costs

- **TripleStore.SPARQL.QuadCardinality** - From Section 5.2
  - `estimate_pattern/2` - Quad pattern cardinality
  - `estimate_quad_join/5` - Quad join cardinality

- **TripleStore.QuadIndex** - Quad index selection
  - `select_index_for_quad/1` - Index selection for quads

### Module Structure

Most changes will be in existing modules:

```elixir
defmodule TripleStore.SPARQL.Optimizer do
  # Extend existing functions for quads
  def reorder_bgp_patterns(patterns, context)  # Handle quad patterns
  def select_index_for_pattern(pattern, stats)  # Use QuadIndex for quads
end

defmodule TripleStore.SPARQL.CostModel do
  # Add graph-aware cost calculations
  def estimate_graph_switch_cost()
  def estimate_cross_graph_join_cost()
end
```

---

## Test Plan

### Tests to Implement

1. **Quad pattern ordering tests**
   - Graph-scoped patterns ordered before cross-graph
   - Within graph, ordered by selectivity
   - Mixed triple/quad patterns work correctly

2. **Index selection tests**
   - GSPO selected for graph-scoped with subject bound
   - GPOS selected for graph-scoped with predicate bound
   - SPOG selected for cross-graph with subject bound
   - POSG selected for cross-graph with predicate bound

3. **Cost model tests**
   - Graph switching cost added to plan
   - Cross-graph join cost calculated correctly
   - Per-graph stats used for accurate costing

4. **Join reordering tests**
   - Patterns with shared graph variable joined early
   - Patterns grouped by graph when possible
   - Cross-graph joins handled correctly

5. **Integration tests**
   - End-to-end query planning with quads
   - Complex queries with multiple graphs

---

## Progress

- [x] 5.3.1 Quad Pattern Ordering - Implemented graph-aware pattern reordering with `reorder_patterns_with_graph_grouping/2`
- [x] 5.3.2 Index Selection for Quads - QuadIndex already has `select_index_for_quad/1` from Section 3.4
- [x] 5.3.3 Graph-Aware Cost Model - Added `graph_switch_cost/0`, `cross_graph_join_cost/3`, `quad_pattern_cost/2` to CostModel
- [x] 5.3.4 Join Reordering with Graphs - Added `group_patterns_by_graph/1` and `extract_graph_key/1`
- [x] Unit Tests - 28 new tests added, 42 total tests passing
- [x] Documentation

---

## Questions for Developer

1. Should we modify the existing Optimizer module or create a separate QuadOptimizer?
   **Answer**: Modified existing Optimizer module for consistency

2. What cost multiplier should we use for graph switching (e.g., 1.5x, 2x)?
   **Answer**: Used logarithmic scaling based on graph count (`log(n + 1)`)

3. Should cross-graph patterns be executed as parallel scans or sequential?
   **Answer**: Sequential with graph grouping - patterns are grouped by graph before execution

4. How should we handle GRAPH clause in the optimizer - as a filter or as pattern modifier?
   **Answer**: Already handled in Section 3.4 - as pattern modifier in `estimate_selectivity`

5. Should we add telemetry events for optimizer decisions with quads?
   **Answer**: Not added - telemetry is already in place at the optimizer level

---

## Implementation Summary

### Files Modified

**`lib/triple_store/sparql/cost_model.ex`** - Added graph-aware cost functions:
- `graph_switch_cost/0` - Cost for switching between graphs (CPU: 5.0, I/O: 20.0)
- `cross_graph_join_cost/3` - Cost of cross-graph joins with logarithmic scaling
- `quad_index_scan_cost/3` - Index scan cost for quad patterns
- `quad_pattern_scan_type/1` - Determines scan type for quad patterns
- `quad_pattern_cost/2` - Pattern cost estimation using QuadCardinality
- Added `quad_pattern` and `pattern_term` types

**`lib/triple_store/sparql/optimizer.ex`** - Added graph grouping functions:
- `is_quad_pattern?/1` - Check if pattern is a quad pattern
- `extract_graph_key/1` - Extract graph key from pattern for grouping
- `group_patterns_by_graph/1` - Group patterns by their graph binding
- `reorder_patterns_with_graph_grouping/2` - Reorder with graph awareness
- Modified `reorder_patterns/2` to use graph grouping for quad patterns

**`test/triple_store/sparql/graph_optimization_test.exs`** - Added 28 new tests:
- Graph grouping tests (is_quad_pattern?, extract_graph_key, group_patterns_by_graph)
- Pattern reordering tests with graph grouping
- Graph-aware cost model tests (graph_switch_cost, cross_graph_join_cost, etc.)

### Test Results

```
Running ExUnit with seed: 105896, max_cases: 40
..........................................
Finished in 0.1 seconds (0.1s async, 0.00s sync)
42 tests, 0 failures
```

### Integration with Section 5.2

The CostModel now integrates with `TripleStore.SPARQL.QuadCardinality.estimate_pattern/2` for more accurate cardinality estimates when available. This provides:
- Per-graph statistics for graph-scoped patterns
- Cross-graph pattern estimation that sums across graphs
- Position-based selectivity calculation

### Graph Grouping Strategy

Patterns are grouped by priority:
1. **Bound graphs** (default or named) - Most selective, executed first
2. **Shared graph variables** - Can bind early, executed second
3. **Cross-graph patterns** - Least selective, executed last

This minimizes graph switches and allows the optimizer to exploit graph-local optimizations.
