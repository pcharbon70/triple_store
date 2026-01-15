# Section 5.1: Per-Graph Statistics

## Overview

Implement per-graph statistics for the quad store. This allows tracking counts and statistics for each named graph independently, enabling the query optimizer to make better decisions for graph-scoped queries.

---

## 5.1.1 Graph Quad Counts

### Tasks

- [ ] 5.1.1.1 Implement `graph_quad_count/2` returning count for specific graph
  - Add to `TripleStore.QuadOperations` or new `TripleStore.Statistics.Quad`
  - Use GSPO prefix scan with graph_id to count quads
  - Return `{:ok, count}` or `{:error, :not_found}`

- [ ] 5.1.1.2 Use GSPO prefix scan to count quads in graph
  - Prefix: `{graph_id, 0, 0, 0}` to `{graph_id, max, max, max}`
  - Efficient fold over GSPO index

- [ ] 5.1.1.3 Cache per-graph counts in statistics cache
  - Cache key: `{graph_id, :quad_count}`
  - Use existing statistics cache infrastructure

- [ ] 5.1.1.4 Handle default graph count separately
  - Default graph is graph_id 0
  - Optimize for default graph access

- [ ] 5.1.1.5 Return `:not_found` for non-existent graph
  - Check if graph exists before counting
  - Use `graph_exists?` for existence check

---

## 5.1.2 Graph Predicate Statistics

### Tasks

- [ ] 5.1.2.1 Implement `graph_predicate_counts/2` for specific graph
  - Return `{:ok, %{predicate_id => count}}`
  - Build histogram of predicate frequencies

- [ ] 5.1.2.2 Scan graph to build predicate frequency map
  - Use GSPO prefix scan for graph
  - Extract predicate from each quad
  - Count occurrences

- [ ] 5.1.2.3 Cache per-graph predicate histograms
  - Cache key: `{graph_id, :predicate_counts}`
  - Invalidate on graph modifications

- [ ] 5.1.2.4 Update on graph modifications
  - Hook into INSERT/DELETE operations
  - Update cached predicate counts incrementally if possible

- [ ] 5.1.2.5 Return map: `%{predicate_id => count}`
  - Map integer predicate IDs to counts
  - Optionally map to IRIs using dictionary

---

## 5.1.3 Graph Subject/Object Statistics

### Tasks

- [ ] 5.1.3.1 Implement `graph_subject_count/2` for distinct subjects
  - Count distinct subject values in graph
  - Use GSPO prefix scan and collect subject IDs

- [ ] 5.1.3.2 Implement `graph_object_count/2` for distinct objects
  - Count distinct object values in graph
  - Use appropriate index for efficiency

- [ ] 5.1.3.3 Use sampling for large graphs (configurable)
  - Sample threshold: e.g., 10000 quads
  - HyperLogLog or reservoir sampling for approximation

- [ ] 5.1.3.4 Cache results in statistics cache
  - Cache key: `{graph_id, :subject_count}`
  - Cache key: `{graph_id, :object_count}`

- [ ] 5.1.3.5 Return approximate counts
  - Mark as `:approximate` when sampled
  - Return exact count for small graphs

---

## 5.1.4 Graph Summary

### Tasks

- [ ] 5.1.4.1 Implement `graph_summary/2` returning complete stats
  - Aggregate all statistics for a single graph
  - Return structured map

- [ ] 5.1.4.2 Include: quad count, distinct predicates, subjects, objects
  - `%{quad_count: n, distinct_predicates: n, distinct_subjects: n, distinct_objects: n}`

- [ ] 5.1.4.3 Include: size estimate, last modified time
  - Size estimate in bytes (key + value sizes)
  - Last modified timestamp from metadata

- [ ] 5.1.4.4 Return structured map for consumption
  - Use atom keys for consistency
  - Include metadata about accuracy (exact vs approximate)

- [ ] 5.1.4.5 Use cached data when available
  - Combine results from individual cached stats
  - Minimize recomputation

---

## 5.1.5 All Graphs Summary

### Tasks

- [ ] 5.1.5.1 Implement `all_graphs_summary/1` returning summary for all
  - Return summary across all graphs in store

- [ ] 5.1.5.2 Include: total quads, graph count, largest graph
  - `%{total_quads: n, graph_count: n, largest_graph: graph_id, largest_graph_count: n}`

- [ ] 5.1.5.3 Include: per-graph breakdown
  - Map or list of per-graph statistics
  - Include graph IRIs where available

- [ ] 5.1.5.4 Use cached per-graph data
  - Aggregate from cached statistics
  - Only recompute if cache stale

- [ ] 5.1.5.5 Return aggregated statistics
  - Structured map with global stats
  - Include per-graph map

---

## Implementation Notes

### Module Structure

Consider adding to `TripleStore.Statistics` or creating `TripleStore.Statistics.Quad`:

```elixir
defmodule TripleStore.Statistics.Quad do
  @doc "Get quad count for specific graph"
  def graph_quad_count(db, dict_manager, graph_term)

  @doc "Get predicate histogram for graph"
  def graph_predicate_counts(db, dict_manager, graph_term)

  @doc "Get distinct subject count for graph"
  def graph_subject_count(db, dict_manager, graph_term)

  @doc "Get distinct object count for graph"
  def graph_object_count(db, dict_manager, graph_term)

  @doc "Get complete summary for graph"
  def graph_summary(db, dict_manager, graph_term)

  @doc "Get summary across all graphs"
  def all_graphs_summary(db, dict_manager)
end
```

### Index Usage

- **GSPO** (Graph-Subject-Predicate-Object): Primary for graph-scoped queries
  - Prefix scan on `{graph_id}` gets all quads in graph
  - Excellent for quad counting and predicate statistics

- **GPOS** (Graph-Predicate-Object-Subject): Alternative for predicate-first
  - Useful for predicate-specific queries within graph

### Cache Integration

Use existing `TripleStore.Statistics.Cache`:

```elixir
# Cache keys
{{:graph, graph_id}, :quad_count}
{{:graph, graph_id}, :predicate_counts}
{{:graph, graph_id}, :subject_count}
{{:graph, graph_id}, :object_count}
{{:graph, graph_id}, :summary}
{:all_graphs, :summary}
```

### Existing Code to Review

- `TripleStore.QuadOperations` - Already has `graph_quad_count/3`
- `TripleStore.QuadIndex` - Index key encoding/decoding
- `TripleStore.Statistics` - Existing statistics infrastructure

---

## Test Plan

### Tests to Implement

1. **graph_quad_count tests**
   - Returns correct count for populated graph
   - Returns 0 for empty graph
   - Returns error for non-existent graph
   - Works with default graph (graph_id 0)
   - Works with named graphs

2. **graph_predicate_counts tests**
   - Returns correct histogram for single predicate
   - Returns correct histogram for multiple predicates
   - Returns empty map for empty graph
   - Caches results correctly

3. **graph_subject_count tests**
   - Returns exact count for small graphs
   - Uses sampling for large graphs
   - Marks approximate results

4. **graph_object_count tests**
   - Returns exact count for small graphs
   - Uses sampling for large graphs
   - Marks approximate results

5. **graph_summary tests**
   - Returns complete statistics map
   - Includes all required fields
   - Uses cached data when available

6. **all_graphs_summary tests**
   - Aggregates across all graphs
   - Includes per-graph breakdown
   - Correctly handles default graph

---

## Progress

- [x] 5.1.1 Graph Quad Counts - `graph_quad_count/2` already existed in `TripleStore.QuadOperations`
- [x] 5.1.2 Graph Predicate Statistics - `graph_predicate_histogram/2` already existed
- [x] 5.1.3 Graph Subject/Object Statistics - Added `graph_object_count/2` to `TripleStore.Statistics`
- [x] 5.1.4 Graph Summary - Added `graph_summary/3` to `TripleStore.Statistics`
- [x] 5.1.5 All Graphs Summary - Added `all_graphs_summary/2` to `TripleStore.Statistics`
- [x] Unit Tests - Created `test/triple_store/statistics_quad_test.exs` with 26 tests, all passing
- [x] Documentation - Added module documentation and examples

---

## Questions for Developer

1. Should per-graph statistics be in a new `TripleStore.Statistics.Quad` module or added to existing `TripleStore.QuadOperations`?
   **Answer**: Add to existing `TripleStore.Statistics` module

2. What sampling threshold should we use for "large graphs"? (e.g., 10000 quads)
   **Answer**: 10,000 quads

3. Should we implement incremental updates to predicate counts on INSERT/DELETE, or recompute on demand?
   **Answer**: Recompute on demand

4. Should the `all_graphs_summary` return a map keyed by graph term or graph ID?
   **Answer**: Return map keyed by integer graph ID

---

## Implementation Summary

### Files Modified

**`lib/triple_store/statistics.ex`**
- Added `graph_object_count/2` - Counts distinct object values in a graph
- Added `graph_summary/3` - Returns complete statistics for a single graph with sampling support
- Added `all_graphs_summary/2` - Returns aggregate statistics across all graphs
- Added `maybe_check_graph_exists/2` - Private helper for graph existence checking

### Files Created

**`test/triple_store/statistics_quad_test.exs`** (359 lines)
- 26 comprehensive unit tests for per-graph statistics functions
- Tests for quad counts, predicate histograms, distinct subjects/objects
- Tests for graph summaries with sampling thresholds
- Tests for all-graphs aggregation

### Test Results

All 26 tests pass:
- 3 tests for `graph_quad_count/2`
- 3 tests for `graph_predicate_histogram/2`
- 2 tests for `graph_distinct_subjects/2`
- 2 tests for `graph_object_count/2`
- 6 tests for `graph_summary/3`
- 5 tests for `all_graphs_summary/2`
- 3 tests for `build_per_graph_histograms/2`
- 2 tests for `graph_statistics/2`
