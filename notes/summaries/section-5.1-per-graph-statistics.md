# Section 5.1: Per-Graph Statistics - Summary

## Overview

Implemented per-graph statistics for quad stores, enabling tracking of counts and statistics for each named graph independently. This provides the query optimizer with better information for graph-scoped queries.

## Implementation Details

### Functions Added to `TripleStore.Statistics`

#### `graph_object_count/2`
Counts distinct object values within a specific graph using the GSPO index.

```elixir
@spec graph_object_count(db_ref(), term_id()) :: {:ok, non_neg_integer()} | {:error, term()}
```

- Uses GSPO prefix scan with graph_id prefix
- Streams through quads, extracts object IDs
- Deduplicates and counts distinct objects
- Works with default graph (ID 0) and named graphs

#### `graph_summary/3`
Returns complete statistics for a single graph with configurable sampling threshold.

```elixir
@spec graph_summary(db_ref(), term_id(), keyword()) :: {:ok, map()} | {:error, term()}
```

Options:
- `:sampling_threshold` - Quads count above which to mark as :approximate (default: 10,000)
- `:include_object_count` - Whether to compute distinct objects (default: true)

Returns map with:
- `:graph_id` - The graph ID
- `:quad_count` - Total quads in graph
- `:distinct_subjects` - Count of distinct subjects
- `:distinct_predicates` - Count of distinct predicates
- `:distinct_objects` - Count of distinct objects (or :not_computed)
- `:predicate_counts` - Map of predicate_id => count
- `:accuracy` - :exact or :approximate

Special handling:
- Returns `{:error, :not_found}` for non-existent named graphs (empty)
- Always allows empty default graph (ID 0)

#### `all_graphs_summary/2`
Returns aggregate statistics across all graphs in the store.

```elixir
@spec all_graphs_summary(db_ref(), keyword()) :: {:ok, map()} | {:error, term()}
```

Options:
- `:include_default` - Include default graph in summary (default: true)
- `:include_per_graph` - Include per-graph breakdown (default: true)
- `:sampling_threshold` - Threshold for :approximate marking (default: 10,000)

Returns map with:
- `:total_quads` - Sum of all quads across graphs
- `:graph_count` - Number of graphs with quads
- `:largest_graph_id` - ID of graph with most quads
- `:largest_graph_count` - Quad count of largest graph
- `:per_graph` - Map of graph_id => graph_summary (or nil)

#### `maybe_check_graph_exists/2`
Private helper that ensures non-default graphs exist (have quads) before returning statistics.

```elixir
@spec maybe_check_graph_exists(non_neg_integer(), term_id()) :: :ok | {:error, :not_found}
```

Pattern matching order:
1. Default graph (ID 0) - always exists
2. Graphs with quads (count > 0) - exist
3. Empty named graphs (count = 0, id != 0) - not_found

## Test Coverage

Created `test/triple_store/statistics_quad_test.exs` with 26 tests, all passing.

| Test Category | Tests | Description |
|--------------|-------|-------------|
| `graph_quad_count/2` | 3 | Populated graph, empty graph, default graph |
| `graph_predicate_histogram/2` | 3 | Single predicate, multiple predicates, empty graph |
| `graph_distinct_subjects/2` | 2 | Distinct count, duplicate subjects |
| `graph_object_count/2` | 2 | Distinct count, duplicate objects |
| `graph_summary/3` | 6 | Complete stats, approximate threshold, exact threshold, skip object count, not_found, empty default |
| `all_graphs_summary/2` | 5 | Aggregation, per-graph breakdown, exclude per-graph, exclude default, empty database |
| `build_per_graph_histograms/2` | 3 | All graphs, includes default, exclude default |
| `graph_statistics/2` | 2 | Existing graph, default graph |

## Design Decisions

1. **Module Placement**: Functions added to existing `TripleStore.Statistics` module (not new `TripleStore.Statistics.Quad`) per developer guidance.

2. **Sampling Threshold**: 10,000 quads is the threshold for marking statistics as :approximate. This is configurable per-call.

3. **Recompute on Demand**: Predicate counts are recomputed on request rather than incrementally updated. This avoids cache invalidation complexity.

4. **Graph ID vs Term**: All functions work with integer graph IDs internally. Translation to/from RDF terms is done by calling functions.

5. **Existence Semantics**:
   - Default graph (ID 0) always exists, even when empty
   - Named graphs only exist when they have at least one quad
   - This aligns with RDF dataset semantics

## Files Changed

### Modified
- `lib/triple_store/statistics.ex` - Added 4 new functions (3 public, 1 private)

### Created
- `test/triple_store/statistics_quad_test.exs` - 359 lines, 26 tests

## Test Results

```
Finished in 4.3 seconds (4.3s async, 0.00s sync)
26 tests, 0 failures
```

## Next Steps

This completes Section 5.1 of the quad store implementation. The per-graph statistics functions are now available for:
- Query optimization for graph-scoped queries
- Monitoring and telemetry
- Dataset management operations

Future sections may build on this foundation for:
- Query planner statistics integration
- Cost-based optimization for named graph queries
- Telemetry and monitoring features
