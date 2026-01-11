# Section 3.4: Graph-Specific Optimizations - Implementation Summary

## Branch: `feature/section-3.4-graph-specific-optimizations`

## Status: COMPLETED

## Date: 2026-01-11

## Overview

This section implements graph-specific query optimizations for quad store queries,
enabling efficient query execution by optimizing pattern ordering for quad patterns,
cross-graph queries, and per-graph statistics.

## Changes Made

### Files Modified

1. **lib/triple_store/sparql/optimizer.ex** (~60 lines added)
   - Added `estimate_selectivity/3` clause for quad patterns `{:quad, s, p, o, g}`
   - Added `graph_position_score/3` for graph position scoring
   - Added `apply_range_filter_boost_quad/5` for quad range filter optimization
   - Updated `pattern_variables/1` to handle quad patterns
   - Updated `binds_range_filtered_variable?/2` to handle quad patterns

2. **lib/triple_store/statistics.ex** (~120 lines added)
   - Added `graph_statistics/2` - Get statistics for a specific graph
   - Added `graph_quad_count/2` - Count quads in a specific graph
   - Added `graph_distinct_subjects/2` - Count distinct subjects in a graph
   - Added `graph_predicate_histogram/2` - Predicate counts per graph
   - Added `build_per_graph_histograms/2` - Build all graph histograms

3. **test/triple_store/sparql/graph_optimization_test.exs** (new file, ~194 lines)
   - Quad pattern selectivity tests (5 tests)
   - Pattern ordering tests (2 tests)
   - Range filter tests with quad patterns (2 tests)
   - Cross-graph query detection tests (2 tests)
   - Graph position score tests (3 tests)

## Implementation Details

### Graph-First Pattern Ordering

The optimizer now considers the graph position in quad patterns when estimating
selectivity. The graph position scoring is:

- **Bound graph** (`:default_graph` or `{:named_node, iri}`): 0.1 (very selective)
- **Bound graph variable** (already in binding): 0.1 (very selective)
- **Unbound graph variable**: 10.0 (less selective)

This ensures that patterns with bound graphs are executed first, reducing the
search space early in the query.

### Quad Pattern Selectivity Formula

For quad patterns `{:quad, s, p, o, g}`:

```
score = s_score * p_score * o_score * g_score
```

Where:
- `s_score`: Subject position score (1 for bound, 100 for unbound variable)
- `p_score`: Predicate position score (predicate-based or 50 for unbound)
- `o_score`: Object position score (2-5 for bound, 100 for unbound variable)
- `g_score`: Graph position score (0.1 for bound, 10 for unbound variable)

Example scores:
- `{?s ?p ?o :default_graph}`: 100 * 50 * 100 * 0.1 = 50,000
- `{?s ?p ?o ?g}`: 100 * 50 * 100 * 10 = 5,000,000
- `{<Alice> ?p ?o :default_graph}`: 1 * 50 * 100 * 0.1 = 500

### Per-Graph Statistics

The statistics module now supports per-graph statistics collection:

```elixir
# Get statistics for a specific graph (ID 0 = default graph)
{:ok, stats} = Statistics.graph_statistics(db, 0)
# => %{quad_count: 1000, distinct_subjects: 50, ...}

# Get predicate histogram for a specific graph
{:ok, histogram} = Statistics.graph_predicate_histogram(db, 0)
# => %{42 => 500, 43 => 1500}

# Get histograms for all graphs
{:ok, histograms} = Statistics.build_per_graph_histograms(db)
# => %{0 => %{42 => 500}, 123 => %{42 => 200}}
```

### Cross-Graph Query Optimization

The optimizer now detects cross-graph patterns:
- **Single graph query**: All patterns have the same bound graph
  - Uses GSPO or GPOS index for efficient graph-scoped access
- **Cross-graph query**: Patterns have different graphs or graph variable
  - Uses SPOG or POSG index for cross-graph pattern matching
  - Graph-first ordering reduces graph context switches

## Test Results

All tests pass:

```
test/triple_store/sparql/graph_optimization_test.exs:14 tests, 0 failures
```

### Test Coverage

- **Quad pattern selectivity** (5 tests): Bound graph, named graph, default graph, bound graph variable, bound subject + graph
- **Pattern ordering** (2 tests): Quad variable extraction, mixed triple/quad patterns
- **Range filters** (2 tests): Quad pattern with range-filtered variable
- **Cross-graph detection** (2 tests): Single graph vs multi-graph queries
- **Graph position scoring** (3 tests): Default graph, bound variable, unbound variable

## Backward Compatibility

- All existing triple pattern optimization continues to work
- Triple patterns are treated as implicit default graph patterns
- Existing optimizer tests continue to pass

## Design Decisions

1. **Graph position multiplier of 0.1**: Bound graphs are highly selective
   because they partition the dataset. A multiplier of 0.1 ensures that
   graph-scoped patterns are prioritized in the ordering.

2. **Separate `graph_position_score/3` function**: Isolates graph position
   logic for easier maintenance and potential future enhancements (e.g.,
   graph cardinality statistics).

3. **Per-graph predicate histograms**: Enables graph-specific cardinality
   estimation. A predicate may be very common in one graph but rare in
   another, affecting join ordering.

4. **Quad patterns in `estimate_selectivity/3`**: Direct handling rather
   than conversion to triple patterns. This preserves graph information
   throughout the optimization process.

## Provides Foundation For

- Full per-graph statistics in query optimization
- Section 3.5: Solution Modifier Adaptation (graph variables in SELECT/GROUP BY/ORDER BY)
- Advanced cross-graph query planning

---

**Implementation Date:** 2026-01-11
**Branch:** `feature/section-3.4-graph-specific-optimizations`
