# Section 5.4: Statistics Cache Extension

## Overview

Extend the statistics cache system to efficiently cache per-graph statistics for quad patterns. This reduces redundant computation for frequently accessed graph statistics.

---

## 5.4.1 Cache Key Design

### Tasks

- [ ] 5.4.1.1 Use `{graph_id, :quad_count}` cache key
  - Extend cache key format to include graph_id
  - Distinguish between triple and quad counts
  - Support graph-specific cache keys

- [ ] 5.4.1.2 Use `{graph_id, :predicate_counts}` cache key
  - Cache predicate histograms per graph
  - Use tuple format for consistency

- [ ] 5.4.1.3 Use `{:all_graphs, :summary}` cache key
  - Aggregate key for all graphs summary
  - Separate from graph-specific keys

- [ ] 5.4.1.4 Ensure cache keys don't collide with triple stats
  - Use different key structure for quads
  - Add type marker to distinguish

- [ ] 5.4.1.5 Document cache key structure
  - Add module documentation
  - Include examples

---

## 5.4.2 Cache Invalidation

### Tasks

- [ ] 5.4.2.1 Invalidate graph-specific stats on graph modification
  - Track which graphs are modified
  - Invalidate only affected graph stats
  - Preserve stats for unmodified graphs

- [ ] 5.4.2.2 Invalidate all-graphs summary on any modification
  - Global summary depends on all graphs
  - Invalidate on any quad modification

- [ ] 5.4.2.3 Implement `invalidate_graph/2` for specific graph
  - Remove graph-specific cache entries
  - Handle default graph and named graphs

- [ ] 5.4.2.4 Implement `invalidate_all/1` for complete invalidation
  - Clear all quad statistics from cache
  - Preserve triple statistics if separate

- [ ] 5.4.2.5 Add telemetry for cache invalidation
  - Emit events on invalidation
  - Track invalidation frequency

---

## 5.4.3 Cache Warming

### Tasks

- [ ] 5.4.3.1 Implement `warm_graph_cache/2` for specific graph
  - Compute and cache all stats for a graph
  - Include quad_count, predicate_counts, subject_count, object_count

- [ ] 5.4.3.2 Implement `warm_all_graphs_cache/1` for all graphs
  - Iterate through all graphs
  - Warm cache for each graph

- [ ] 5.4.3.3 Use parallel warming for multiple graphs
  - Use Task.async_stream for parallel warming
  - Limit concurrency with max_concurrency option

- [ ] 5.4.3.4 Add configurable warming on startup
  - Add config option for auto-warming
  - Selective warming (frequently accessed graphs)

- [ ] 5.4.3.5 Add telemetry for warming operations
  - Emit events on warming start/complete
  - Track warming duration

---

## Implementation Notes

### Existing Code to Review

- **TripleStore.Statistics** - Current statistics module
  - Cache implementation
  - Statistics collection functions
  - Cache invalidation logic

### Module Structure

Most changes will be in existing `TripleStore.Statistics`:

```elixir
defmodule TripleStore.Statistics do
  # Cache keys for quad statistics
  defp quad_count_key(graph_id)
  defp predicate_counts_key(graph_id)
  defp all_graphs_summary_key()

  # Cache invalidation
  def invalidate_quad_cache(db, graph_id)
  def invalidate_all_quad_cache(db)

  # Cache warming
  def warm_graph_cache(db, graph_id, opts \\ [])
  def warm_all_graphs_cache(db, opts \\ [])
end
```

---

## Test Plan

### Tests to Implement

1. **Cache key tests**
   - Verify quad_count_key format
   - Verify predicate_counts_key format
   - Verify all_graphs_summary_key format
   - Verify no collision with triple stats

2. **Cache invalidation tests**
   - Invalidate specific graph removes only that graph's stats
   - Invalidate all removes all quad stats
   - All-graphs summary invalidated on any modification
   - Triple stats preserved when quad stats invalidated

3. **Cache warming tests**
   - Warm graph cache populates all stats
   - Warm all graphs iterates through all graphs
   - Parallel warming uses concurrency
   - Telemetry events emitted

4. **Integration tests**
   - Cache hit after first access
   - Cache miss after invalidation
   - Warm cache improves query performance

---

## Progress

- [x] 5.4.1 Cache Key Design - Implemented `quad_cache_key/1` and `all_graphs_cache_key/0` with distinct prefix
- [x] 5.4.2 Cache Invalidation - Implemented `invalidate_quad_cache/2` and `invalidate_all_quad_cache/1`
- [x] 5.4.3 Cache Warming - Implemented `warm_graph_cache/3` and `warm_all_graphs_cache/2` with parallel warming
- [x] Unit Tests - 23 tests, all passing
- [x] Documentation

---

## Questions for Developer

1. Should quad statistics use a separate cache table or share with triple statistics?
   **Answer**: Using ETS table with distinct cache key prefix to share infrastructure

2. What should be the default max concurrency for parallel cache warming?
   **Answer**: 4 concurrent tasks

3. Should we implement TTL (time-to-live) for cached statistics?
   **Answer**: Not implemented - manual invalidation only

4. Should cache warming be automatic on startup or opt-in?
   **Answer**: Opt-in via `warm_all_graphs_cache/2`

---

## Implementation Summary

### Files Modified

**`lib/triple_store/statistics.ex`** - Added quad statistics caching:
- Converted to GenServer for cache management
- Added `graph_stats` type specification
- Added constants: `@quad_stats_prefix`, `@quad_cache_table`, `@default_max_concurrency`
- Added GenServer callbacks: `init/1`, `handle_info/2`
- Added `get_cached_graph_stats/2` - Get cached stats with compute-on-miss
- Added `get_cached_all_graphs_summary/2` - Get cached summary with compute-on-miss
- Added `invalidate_quad_cache/2` - Invalidate specific graph cache
- Added `invalidate_all_quad_cache/1` - Invalidate all quad caches
- Added `warm_graph_cache/3` - Warm cache for specific graph
- Added `warm_all_graphs_cache/2` - Warm cache for all graphs with parallel processing
- Added `quad_cache_key/1` - Generate cache key for graph stats
- Added `all_graphs_cache_key/0` - Generate cache key for all-graphs summary
- Added `compute_and_cache_graph_stats/2` - Compute and cache graph statistics

**`test/triple_store/statistics/quad_cache_test.exs`** - Created:
- 23 tests covering all cache functionality

### Test Results

```
23 tests, 0 failures
```

### Cache Key Design

```
# Graph-specific key
{@quad_stats_prefix, graph_id}
# Example: {<<0, 0, 0, 0, 0, 0, 0, 2>>, 0}

# All-graphs summary key
{@quad_stats_prefix, :all_graphs}
# Example: {<<0, 0, 0, 0, 0, 0, 0, 2>>, :all_graphs}
```

The prefix `<<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02>>` is distinct from the
triple stats prefix `<<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01>>` to prevent collisions.

### Cache Invalidation Strategy

1. **Graph-specific invalidation**: When a graph is modified, only that graph's cache is invalidated
2. **All-graphs summary invalidation**: Invalidated whenever any graph is modified
3. **Full invalidation**: Available via `invalidate_all_quad_cache/1`

### Cache Warming Strategy

1. **Single graph warming**: `warm_graph_cache/3` computes and stores all stats for one graph
2. **Multi-graph warming**: `warm_all_graphs_cache/2` uses `Task.async_stream` with configurable concurrency
3. **Parallel processing**: Default max concurrency of 4 for efficient warming

### Telemetry Events

- `[:triple_store, :statistics, :quad_cache, :miss]` - Cache miss with graph_id
- `[:triple_store, :statistics, :quad_cache, :invalidate]` - Graph invalidation with graph_id
- `[:triple_store, :statistics, :quad_cache, :invalidate_all]` - Full cache invalidation
- `[:triple_store, :statistics, :quad_cache, :warm]` - Graph warming with graph_id and duration
- `[:triple_store, :statistics, :quad_cache, :warm_all]` - Bulk warming with graph_count and duration
