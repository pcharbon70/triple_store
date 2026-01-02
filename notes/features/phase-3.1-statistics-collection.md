# Phase 3.1 Statistics Collection - Feature Plan

**Date:** 2026-01-02
**Branch:** `feature/phase-3-statistics-collection`
**Source:** `notes/planning/performance/phase-03-query-engine-improvements.md`

## Overview

Implement statistics collection to enable accurate cost-based query optimization. Current cardinality estimation uses hardcoded factors; collecting actual statistics enables the optimizer to make smarter join ordering and strategy decisions.

## Implementation Plan

### Task 3.1.1: Cardinality Statistics Module

- [x] 3.1.1.1 Design statistics data structure (already done in cardinality.ex)
- [x] 3.1.1.2 Identify collection points (collect on-demand, refresh on triggers)
- [x] 3.1.1.3 Create `TripleStore.Statistics` module enhancements
- [x] 3.1.1.4 Implement `collect/1` function to gather all statistics
- [x] 3.1.1.5 Implement `total_triples/1` count (existing)
- [x] 3.1.1.6 Implement `distinct_subjects/1` count (existing)
- [x] 3.1.1.7 Implement `distinct_predicates/1` count (existing)
- [x] 3.1.1.8 Implement `distinct_objects/1` count (existing)
- [x] 3.1.1.9 Store statistics in RocksDB (id2str with reserved key prefix)
- [x] 3.1.1.10 Implement `load/1` to read persisted statistics

### Task 3.1.2: Predicate Cardinalities

- [x] 3.1.2.1 Design predicate statistics structure (in stats map)
- [x] 3.1.2.2 Implement `predicate_count/2` for specific predicate (existing)
- [x] 3.1.2.3 Implement `build_predicate_histogram/1` scanning all predicates
- [x] 3.1.2.4 Store predicate cardinality map in statistics
- [ ] 3.1.2.5 Implement incremental update on insert/delete (deferred - use refresh)
- [x] 3.1.2.6 Add telemetry for statistics collection time

### Task 3.1.3: Numeric Histograms

- [x] 3.1.3.1 Design histogram structure (equi-width buckets)
- [x] 3.1.3.2 Identify numeric predicates via inline encoding detection
- [x] 3.1.3.3 Implement `build_numeric_histogram/3` for predicate
- [x] 3.1.3.4 Implement `estimate_range_selectivity/4` using histogram
- [x] 3.1.3.5 Store histograms in statistics structure
- [x] 3.1.3.6 Configure histogram bucket count (default: 100)

### Task 3.1.4: Statistics Refresh

- [x] 3.1.4.1 Design refresh strategy (manual + threshold-based)
- [x] 3.1.4.2 Implement `refresh/1` for full statistics rebuild
- [ ] 3.1.4.3 Implement `refresh_incremental/2` for delta updates (deferred)
- [x] 3.1.4.4 Add `:auto_refresh` option to server configuration
- [x] 3.1.4.5 Implement background refresh via GenServer
- [x] 3.1.4.6 Add Statistics.Server with get_stats/refresh/notify_modification APIs

### Task 3.1.5: Unit Tests

- [x] 3.1.5.1 Test statistics collection on empty store
- [x] 3.1.5.2 Test statistics collection with data
- [x] 3.1.5.3 Test predicate cardinality accuracy
- [x] 3.1.5.4 Test histogram range estimates
- [x] 3.1.5.5 Test statistics persistence across restart
- [x] 3.1.5.6 Test auto-refresh triggers
- [x] 3.1.5.7 Test server caching and refresh

## Architecture

### Statistics Data Structure

```elixir
%{
  triple_count: 10000,
  distinct_subjects: 1000,
  distinct_predicates: 50,
  distinct_objects: 2000,
  predicate_histogram: %{
    42 => 500,    # predicate_id => count
    43 => 1500
  },
  numeric_histograms: %{
    price_pred_id => %{
      min: 0.0,
      max: 1000.0,
      bucket_count: 100,
      buckets: [45, 67, 89, ...],  # counts per bucket
      total_count: 5000
    }
  },
  collected_at: ~U[2026-01-02 12:00:00Z],
  version: 1
}
```

### Collection Strategy

1. **Full Collection**: Scan all indices once to gather all statistics
2. **Persistence**: Store in RocksDB id2str CF with reserved key prefix
3. **Cache**: Statistics.Server GenServer holds current statistics in memory
4. **Auto-refresh**: Trigger refresh after N modifications or periodic interval

### Integration Points

- `TripleStore.SPARQL.Cardinality` - Uses statistics for estimation
- `TripleStore.SPARQL.Optimizer` - Uses statistics for join reordering
- `TripleStore.Statistics.Server` - In-memory caching with auto-refresh

## Files Changed

| File | Changes |
|------|---------|
| `lib/triple_store/statistics.ex` | Enhanced with collect, save, load, histograms |
| `lib/triple_store/statistics/server.ex` | New GenServer for caching and auto-refresh |
| `test/triple_store/statistics_test.exs` | Added 21 new tests |
| `test/triple_store/statistics/server_test.exs` | New server tests (12 tests) |

## Progress Tracking

| Task | Status | Notes |
|------|--------|-------|
| 3.1.1 Cardinality Statistics | Complete | Enhanced Statistics module |
| 3.1.2 Predicate Cardinalities | Complete | build_predicate_histogram implemented |
| 3.1.3 Numeric Histograms | Complete | Equi-width histograms with range selectivity |
| 3.1.4 Statistics Refresh | Complete | GenServer with auto-refresh |
| 3.1.5 Unit Tests | Complete | 49 tests passing |

## Current Status

**Started:** 2026-01-02
**Completed:** 2026-01-02
**Status:** All tasks complete, all 4424 tests passing
