# Section 5.2: Quad Pattern Cardinality Estimation

## Overview

Implement cardinality estimation for quad patterns. The query optimizer needs accurate estimates of how many quads match a given pattern to choose optimal execution plans. This section extends the existing triple pattern estimation to handle the fourth component (graph).

---

## 5.2.1 Quad Pattern Estimation

### Tasks

- [ ] 5.2.1.1 Implement `estimate_quad_pattern/2` for quad patterns
  - Add to `TripleStore.Statistics` or `TripleStore.SPARQL.Optimizer`
  - Accept quad pattern: `{subject, predicate, object, graph}`
  - Return estimated cardinality as float

- [ ] 5.2.1.2 Use per-graph statistics when graph bound
  - When graph is bound (term or ID), use that graph's statistics
  - Use `graph_summary/3` for accurate per-graph stats
  - More accurate than aggregate statistics

- [ ] 5.2.1.3 Use aggregate statistics when graph unbound
  - When graph is variable/unbound, sum across all graphs
  - Use `all_graphs_summary/2` for global statistics
  - Return estimate for cross-graph query

- [ ] 5.2.1.4 Apply selectivity based on bound positions
  - Calculate selectivity for each bound position (S, P, O, G)
  - Multiply selectivities for overall estimate
  - Handle edge cases (all bound, all unbound)

- [ ] 5.2.1.5 Return estimated cardinality as float
  - Float allows for fractional estimates
  - Can represent uncertainty in estimates
  - Use 0.0 for impossible patterns

---

## 5.2.2 Bound Position Selectivity

### Tasks

- [ ] 5.2.2.1 Calculate subject selectivity: `count / distinct_subjects`
  - When subject bound, estimate based on distinct subjects in graph
  - Use per-graph distinct_subjects count
  - Formula: `quad_count / distinct_subjects`

- [ ] 5.2.2.2 Calculate predicate selectivity: `count / predicate_count`
  - When predicate bound, use predicate histogram
  - More selective if predicate is rare
  - Formula: `predicate_count / quad_count`

- [ ] 5.2.2.3 Calculate object selectivity: `count / distinct_objects`
  - When object bound, use distinct objects count
  - Formula: `quad_count / distinct_objects`

- [ ] 5.2.2.4 Calculate graph selectivity: `count / total_graphs` or 1.0 if bound
  - When graph bound, selectivity = 1.0 (fully selective)
  - When graph unbound, may need to scan all graphs
  - Consider default vs named graphs

- [ ] 5.2.2.5 Combine selectivities: `product of all bound positions`
  - Multiply selectivities of all bound positions
  - Final estimate: `base_count * product_of_selectivities`
  - Handle edge cases where count is 0

---

## 5.2.3 Cross-Graph Pattern Estimation

### Tasks

- [ ] 5.2.3.1 Handle pattern with unbound graph (cross-graph)
  - Detect when graph position is variable
  - Sum estimates across all graphs
  - Return aggregated cardinality

- [ ] 5.2.3.2 Sum cardinalities across all graphs
  - Iterate through all graphs with statistics
  - Add per-pattern estimates for each graph
  - Handle graphs with no matching patterns

- [ ] 5.2.3.3 Use graph summary data for efficient estimation
  - Use `all_graphs_summary/2` for pre-computed aggregates
  - Avoid scanning each graph individually
  - Cache results when possible

- [ ] 5.2.3.4 Account for graphs with no matching patterns
  - Some graphs may not have matching predicates
  - Some graphs may be empty
  - Only count graphs that contribute to results

- [ ] 5.2.3.5 Return aggregated estimate
  - Sum of per-graph estimates
  - Or use aggregate statistics directly
  - Return as float

---

## 5.2.4 Join Cardinality Estimation

### Tasks

- [ ] 5.2.4.1 Extend `estimate_join_cardinality/3` for quads
  - Add quad pattern support to existing join estimation
  - Handle patterns with 4 components (S, P, O, G)
  - Account for graph variable in joins

- [ ] 5.2.4.2 Account for graph variable joining
  - When graph variable is shared between patterns
  - Reduces cardinality significantly (intersection)
  - When graph variables differ, may be cross-graph join

- [ ] 5.2.4.3 Account for cross-graph joins (when compatible)
  - Some joins can span graphs (graph variables independent)
  - Some joins are graph-scoped (same graph variable)
  - Model difference in cost and cardinality

- [ ] 5.2.4.4 Use independent join when graphs disjoint
  - When patterns operate on different graphs
  - Cardinality is product (cartesian product)
  - Higher cost, higher cardinality

- [ ] 5.2.4.5 Return estimate with confidence interval
  - Point estimate (expected cardinality)
  - Optional confidence based on data availability
  - Return as map or tuple

---

## Implementation Notes

### Existing Code to Review

- **TripleStore.Statistics** - Existing statistics functions
  - `graph_summary/3` - Per-graph statistics
  - `all_graphs_summary/2` - Aggregate statistics
  - `graph_predicate_histogram/2` - Predicate frequencies

- **TripleStore.SPARQL.Optimizer** - Query optimizer
  - `estimate_pattern_cardinality/2` - For triple patterns
  - `estimate_join_cardinality/3` - For joins

### Module Structure

Consider adding to `TripleStore.Statistics`:

```elixir
defmodule TripleStore.Statistics do
  @doc "Estimate cardinality for a quad pattern"
  def estimate_quad_pattern(db, pattern, opts \\ [])

  @doc "Calculate selectivity for bound position"
  def position_selectivity(db, position, value, graph_id, opts \\ [])

  @doc "Estimate cross-graph pattern"
  def estimate_cross_graph_pattern(db, pattern, opts \\ [])

  @doc "Estimate join cardinality for quad patterns"
  def estimate_quad_join_cardinality(db, pattern1, pattern2, opts \\ [])
end
```

### Pattern Representation

Quad patterns can be represented as:
```elixir
# Fully bound
{subject_id, predicate_id, object_id, graph_id}

# Partially bound (variables as :_ or nil)
{subject_id, :_, object_id, graph_id}

# Cross-graph (unbound graph)
{:_, predicate_id, object_id, :_}
```

### Selectivity Calculation

```elixir
# Subject selectivity: how many quads have this subject?
selectivity_s = quad_count / distinct_subjects

# Predicate selectivity: how many quads have this predicate?
selectivity_p = predicate_count / quad_count

# Object selectivity: how many quads have this object?
selectivity_o = quad_count / distinct_objects

# Graph selectivity: 1.0 if bound, spread across graphs if unbound
selectivity_g = if graph_bound?, do: 1.0, else: 1.0 / graph_count

# Combined selectivity
combined = selectivity_s * selectivity_p * selectivity_o * selectivity_g

# Final estimate
estimate = base_count * combined
```

---

## Test Plan

### Tests to Implement

1. **estimate_quad_pattern tests**
   - Fully bound pattern returns 1.0 (exact match)
   - Fully unbound returns total quad count
   - Partially bound returns selective estimate
   - Graph-bound pattern uses per-graph stats
   - Cross-graph pattern sums across graphs

2. **position_selectivity tests**
   - Subject selectivity calculated correctly
   - Predicate selectivity uses histogram
   - Object selectivity for distinct objects
   - Graph selectivity is 1.0 when bound
   - Combined selectivity is product

3. **cross_graph_pattern_estimation tests**
   - Unbound graph sums across all graphs
   - Handles empty graphs correctly
   - Respects bound positions in sum
   - Uses aggregate stats efficiently

4. **join_cardinality tests**
   - Same graph join reduces cardinality
   - Different graph join increases cardinality
   - Shared variables reduce estimate
   - Independent variables multiply estimates

5. **Edge cases**
   - Empty database returns 0.0
   - Non-existent predicate returns 0.0
   - Non-existent graph returns 0.0
   - All bound returns 1.0

---

## Progress

- [x] 5.2.1 Quad Pattern Estimation - `estimate_pattern/2` implemented in `TripleStore.SPARQL.QuadCardinality`
- [x] 5.2.2 Bound Position Selectivity - `position_selectivity/4` implemented
- [x] 5.2.3 Cross-Graph Pattern Estimation - `estimate_cross_graph_pattern/3` implemented with iteration
- [x] 5.2.4 Join Cardinality Estimation - `estimate_quad_join/5` implemented
- [x] Unit Tests - 41 tests created in `test/triple_store/sparql/quad_cardinality_test.exs`
- [x] Documentation - Full module documentation with examples

---

## Questions for Developer

1. Should quad pattern estimation be added to `TripleStore.Statistics` or `TripleStore.SPARQL.Optimizer`?
   **Answer**: Create new `TripleStore.SPARQL.QuadCardinality` module

2. How should we represent variables in quad patterns for estimation? (e.g., `nil`, `:_`, or a special struct)
   **Answer**: Use existing `{:variable, name}` format from SPARQL algebra

3. For cross-graph patterns, should we iterate all graphs or use pre-aggregated statistics?
   **Answer**: Iterate graphs for accuracy (slower but more accurate)

4. Should join cardinality return just the estimate or also include confidence/metadata?
   **Answer**: Return just the estimate as float (simple API)

5. How should we handle the case where a pattern references a non-existent graph?
   **Answer**: Return minimum cardinality (1.0)

---

## Implementation Summary

### Files Created

**`lib/triple_store/sparql/quad_cardinality.ex`** (~700 lines)
- New `TripleStore.SPARQL.QuadCardinality` module
- `estimate_pattern/2` - Main cardinality estimation for quad patterns
- `estimate_graph_scoped_pattern/4` - For patterns with bound graph
- `estimate_cross_graph_pattern/3` - For patterns with unbound graph
- `position_selectivity/4` - Selectivity for each position (S, P, O, G)
- `estimate_quad_join/5` - Join cardinality for quad patterns
- `estimate_multi_quad_pattern/2` - Multi-pattern join estimation
- `estimate_pattern_with_bindings/3` - Cardinality with bound variables
- `estimate_selectivity/2` - Pattern selectivity (0.0 to 1.0)

**`test/triple_store/sparql/quad_cardinality_test.exs`** (~550 lines)
- 41 comprehensive unit tests covering all functions
- Tests for graph-scoped and cross-graph patterns
- Tests for position selectivity
- Tests for join cardinality
- Tests for binding adjustments
- Tests for edge cases

### Test Results

```
Finished in 0.1 seconds (0.1s async, 0.00s sync)
41 tests, 0 failures
```

### Key Design Decisions

1. **Separate Module**: Created `TripleStore.SPARQL.QuadCardinality` instead of extending `TripleStore.SPARQL.Cardinality` to keep concerns separate and avoid mixing triple/quad logic.

2. **Graph-Scoped vs Cross-Graph**:
   - Graph-scoped (bound graph): Uses per-graph statistics for accuracy
   - Cross-graph (unbound graph): Iterates all graphs and sums estimates

3. **Dual Count Support**: Stats map supports both `:triple_count` and `:quad_count` for backward compatibility.

4. **Position Selectivity**: Each position (S, P, O, G) has its own selectivity calculation based on distinct counts in the relevant graph.

5. **Binding Adjustment**: When variables are already bound from previous joins, the binding adjustment uses graph-specific distinct counts when available.
