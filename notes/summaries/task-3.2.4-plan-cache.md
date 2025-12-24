# Task 3.2.4: Plan Cache - Summary

## Overview

Implemented a GenServer-based cache for optimized SPARQL query execution plans. The cache uses ETS for fast lookups and implements an LRU (Least Recently Used) eviction policy to manage memory usage.

## Files Created

### Implementation
- `lib/triple_store/sparql/plan_cache.ex` (~430 lines)
  - GenServer with ETS backend
  - Query normalization for key computation
  - LRU eviction policy
  - Cache invalidation
  - Statistics tracking

### Tests
- `test/triple_store/sparql/plan_cache_test.exs` (~540 lines)
  - 34 comprehensive tests covering all functionality

## Key Functions

### Client API

```elixir
@spec start_link(keyword()) :: GenServer.on_start()
@spec get_or_compute(term(), (-> term()), keyword()) :: term()
@spec get(term(), keyword()) :: {:ok, term()} | :miss
@spec put(term(), term(), keyword()) :: :ok
@spec invalidate(keyword()) :: :ok
@spec invalidate(term(), keyword()) :: :ok
@spec stats(keyword()) :: cache_stats()
@spec size(keyword()) :: non_neg_integer()
```

### Key Computation

```elixir
@spec compute_key(term()) :: cache_key()
```

Normalizes query algebra and computes SHA256 hash for cache key.

## Features

### Query Normalization

Variable names are normalized to positional indices, so structurally identical queries share cache entries:

```elixir
# These queries produce the same cache key:
{:triple, {:variable, "x"}, 1, {:variable, "y"}}
{:triple, {:variable, "subject"}, 1, {:variable, "object"}}
```

### LRU Eviction

- Two ETS tables: plans (key -> entry) and LRU (timestamp+key -> true)
- Access updates timestamp in both tables
- Eviction removes oldest entry when max_size exceeded
- Configurable max_size (default: 1000)

### Cache Invalidation

- `invalidate/1` - Clears entire cache (after bulk loads)
- `invalidate/2` - Clears specific entry (after targeted updates)

### Statistics

```elixir
%{
  size: 42,        # Current cache entries
  hits: 1000,      # Total cache hits
  misses: 50,      # Total cache misses
  hit_rate: 0.95,  # hits / (hits + misses)
  evictions: 10    # Total evictions performed
}
```

## ETS Table Design

| Table | Type | Purpose |
|-------|------|---------|
| `{name}_plans` | `:set` | Maps cache_key to entry |
| `{name}_lru` | `:ordered_set` | Orders entries by access time |

Entry structure:
```elixir
%{
  plan: term(),          # The cached plan
  access_time: integer(),# Last access (monotonic ms)
  created_at: integer()  # Creation time (monotonic ms)
}
```

## Configuration

```elixir
PlanCache.start_link(
  name: MyCache,     # Process name (default: PlanCache)
  max_size: 500      # Max cached plans (default: 1000)
)
```

## Integration Points

- **JoinEnumeration**: Caches optimized execution plans
- **Optimizer**: Should invalidate on schema changes
- **Statistics**: Should invalidate after bulk loads

## Test Coverage

- Start/stop lifecycle
- Get/put/get_or_compute operations
- Cache key computation and normalization
- Variable name independence
- Full and partial invalidation
- LRU eviction (oldest evicted, access updates order)
- Statistics tracking (hits, misses, hit rate, evictions)
- Edge cases (complex structures, maps, concurrent access)
- Realistic usage patterns

## Design Decisions

1. **ETS for speed**: Concurrent reads without GenServer bottleneck
2. **Ordered set for LRU**: Efficient oldest-first eviction
3. **SHA256 for keys**: Consistent, collision-resistant hashing
4. **Variable normalization**: Structural sharing for parameterized queries
5. **Async put**: Cast for non-blocking cache population

## Usage Example

```elixir
# Start cache (in supervision tree)
{:ok, _} = PlanCache.start_link(max_size: 1000)

# Get or compute plan
plan = PlanCache.get_or_compute(query_algebra, fn ->
  JoinEnumeration.enumerate(patterns, stats)
end)

# Invalidate after data change
PlanCache.invalidate()

# Check statistics
stats = PlanCache.stats()
# => %{size: 42, hits: 1000, misses: 50, hit_rate: 0.95, evictions: 10}
```

## Next Steps

Task 3.2.5 (Unit Tests) will add additional integration tests for the cost-based optimizer components working together.
