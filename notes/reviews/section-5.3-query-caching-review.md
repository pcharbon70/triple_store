# Section 5.3: Query Caching - Comprehensive Review

**Date:** 2025-12-28
**Reviewed Files:**
- `lib/triple_store/query/cache.ex`
- `test/triple_store/query/cache_test.exs`

**Status:** Complete with recommendations

---

## Executive Summary

Section 5.3 implements a full-featured query result cache with LRU eviction, predicate-based invalidation, TTL expiration, disk persistence, and cache warming. The implementation is well-structured with comprehensive test coverage (46 tests). Several security and performance concerns should be addressed before production use.

---

## 1. Requirements Verification

| Requirement | Status | Implementation |
|-------------|--------|----------------|
| 5.3.1.1 GenServer with ETS backend | PASS | GenServer with dual ETS tables (results + LRU ordering) |
| 5.3.1.2 Cache by query hash | PASS | SHA256 hash of query string/algebra |
| 5.3.1.3 Configurable max entries | PASS | `max_entries` option with LRU eviction |
| 5.3.1.4 Skip large result sets | PASS | `max_result_size` with `:skipped` return |
| 5.3.2.1 Track predicates per query | PASS | MapSet stored per entry |
| 5.3.2.2 Invalidate by predicates | PASS | `invalidate_predicates/2` function |
| 5.3.2.3 Full invalidation fallback | PASS | `invalidate/1` clears all |
| 5.3.2.4 Telemetry for hit/miss | PASS | Events for hit, miss, expired, persist, warm |
| 5.3.3.1 Persist to disk | PASS | `persist_to_file/2` with compressed binary |
| 5.3.3.2 Restore on startup | PASS | `warm_on_start` option |
| 5.3.3.3 Pre-execute common queries | PASS | `warm_queries/2` function |

### Test Coverage

- **Total tests:** 46
- **All passing:** Yes
- **Categories covered:** start_link, get_or_execute, get/put, compute_key, LRU eviction, TTL expiration, invalidation, stats, size, result size calculation, concurrent access, telemetry events, persistence, warm on start, query pre-execution

---

## 2. Security Review

### CRITICAL: Unsafe Deserialization

**Location:** `cache.ex:900`

```elixir
data = :erlang.binary_to_term(binary, [:safe])
```

**Issue:** While `:safe` prevents atom creation, it does NOT prevent arbitrary code execution through crafted terms (e.g., anonymous functions, module calls).

**Risk:** An attacker with write access to the cache file could execute arbitrary code on cache load.

**Recommendation:**
```elixir
# Validate structure before using
defp load_entries_from_data(state, %{version: 1, entries: entries} = data)
    when is_list(entries) do
  # Validate each entry has expected keys and types
  validated_entries = Enum.filter(entries, &valid_entry?/1)
  # ... rest of loading logic
end

defp valid_entry?(%{key: k, result: _, result_size: s, predicates: p})
    when is_binary(k) and is_integer(s) and is_list(p), do: true
defp valid_entry?(_), do: false
```

Alternatively, consider using a safer serialization format like JSON for the metadata structure, keeping only the result as binary.

### HIGH: Path Traversal Vulnerability

**Location:** `cache.ex:410, 437`

```elixir
def persist_to_file(path, opts \\ [])
def warm_from_file(path, opts \\ [])
```

**Issue:** No validation of path parameter. User-controlled paths could lead to:
- Writing to arbitrary locations (`../../../etc/crontab`)
- Reading sensitive files

**Recommendation:**
```elixir
defp validate_path!(path) do
  expanded = Path.expand(path)
  unless String.starts_with?(expanded, allowed_cache_dir()) do
    raise ArgumentError, "Cache path must be within #{allowed_cache_dir()}"
  end
  expanded
end
```

### HIGH: DoS via Cache Exhaustion

**Issue:** No rate limiting on cache puts. An attacker could:
1. Send many unique queries to exhaust memory before LRU kicks in
2. Flood with queries just under `max_result_size` to maximize memory usage

**Recommendation:**
- Add memory-based limits (not just entry count)
- Consider rate limiting per-client if exposed externally

### MEDIUM: Timing Side Channel

**Issue:** Cache hit/miss can leak information about query frequency patterns.

**Mitigation:** Document this limitation; not fixable without fundamental redesign.

---

## 3. Architecture Review

### GenServer Bottleneck

**Location:** All public functions go through `GenServer.call/2`

**Issue:** Single GenServer serializes all operations, limiting throughput.

**Analysis:** For read-heavy workloads, this becomes a bottleneck. The ETS tables use `read_concurrency: true` (line 540), but reads still go through GenServer.

**Recommendation:** For high-throughput scenarios, consider:
1. Direct ETS reads with GenServer only for writes
2. Partitioned caches (shard by query hash)
3. `:persistent_term` for extremely hot entries

### O(n) Predicate Invalidation

**Location:** `cache.ex:628-649`

```elixir
to_invalidate =
  :ets.foldl(
    fn {key, entry}, acc ->
      if MapSet.size(MapSet.intersection(entry.predicates, predicate_set)) > 0 do
        [{key, entry} | acc]
      ...
```

**Issue:** Full table scan for every predicate invalidation.

**Impact:** With 1000s of cached queries, invalidation becomes slow.

**Recommendation:** Add a reverse index:
```elixir
# During init
:ets.new(predicate_index_table, [:bag, :named_table])

# During put
Enum.each(predicates, fn pred ->
  :ets.insert(predicate_index_table, {pred, key})
end)

# During invalidate_predicates
keys_to_invalidate =
  Enum.flat_map(predicates, fn pred ->
    :ets.lookup(predicate_index_table, pred) |> Enum.map(&elem(&1, 1))
  end)
```

### Missing terminate/2 Callback

**Issue:** No `terminate/2` callback to persist cache on shutdown.

**Recommendation:**
```elixir
@impl true
def terminate(_reason, state) do
  if state.persistence_path do
    do_persist_to_file(state, state.persistence_path)
  end
  :ok
end
```

---

## 4. Consistency Review

### Telemetry Events Not Registered

**Issue:** Telemetry events are emitted but not documented in a centralized location (e.g., `TripleStore.Telemetry`).

**Recommendation:** Update `TripleStore.Telemetry.cache_events/0` to include:
```elixir
@prefix ++ [:cache, :query, :hit],
@prefix ++ [:cache, :query, :miss],
@prefix ++ [:cache, :query, :expired],
@prefix ++ [:cache, :query, :persist],
@prefix ++ [:cache, :query, :warm]
```

### GenServer Pattern Consistency

Query.Cache follows established patterns:
- Uses `start_link/1` with keyword opts
- Name registration via `:name` option
- Standard `init/1` callback
- Uses `@impl true` annotations

**Minor Issue:** Uses separate `@impl true` annotations per `handle_call` clause (verbose but valid).

### Error Return Consistency

| Pattern | Usage |
|---------|-------|
| `{:ok, result}` | Success with value |
| `{:error, reason}` | Failure with reason |
| `:ok` | Success without value |
| `:miss` | Cache miss (specialized) |
| `:expired` | Cache expired (specialized) |
| `:skipped` | Result too large to cache |

These are intentional variations for cache-specific functionality.

---

## 5. Redundancy Review

### Potential LRU Code Duplication

**Observation:** If `TripleStore.SPARQL.PlanCache` uses similar LRU logic, consider extracting to a shared module.

**Recommendation:** Evaluate extracting common LRU logic:
```elixir
defmodule TripleStore.Cache.LRU do
  @moduledoc "Reusable LRU cache with ETS backend"
  # Common implementation
end
```

### Repeated MapSet Conversion

**Location:** Multiple places convert predicates to/from lists

```elixir
predicates: MapSet.to_list(entry.predicates)  # line 739, 867
predicates: MapSet.new(entry.predicates)       # line 767, 934
```

**Minor issue:** Slightly inefficient but not critical.

---

## 6. Elixir Best Practices

### Use MapSet.disjoint?/2

**Location:** `cache.ex:633`

```elixir
if MapSet.size(MapSet.intersection(entry.predicates, predicate_set)) > 0 do
```

**Recommendation:**
```elixir
if not MapSet.disjoint?(entry.predicates, predicate_set) do
```

This is more readable and potentially more efficient.

### Type Definitions

Custom types are well-documented:
```elixir
@typedoc "Cache key (query hash)"
@type cache_key :: binary()

@typedoc "Cached result entry"
@type cache_entry :: %{...}
```

### Documentation Quality

- Module documentation is comprehensive
- All public functions have `@doc` and `@spec`
- Usage examples included

---

## 7. Test Quality Review

### Strengths

- Comprehensive coverage of all features
- Good use of `on_exit` for cleanup
- Concurrent access tests
- Telemetry verification
- Edge cases covered (empty cache, missing files, large results)

### Improvements

1. **Property-based testing:** Consider adding PropEr tests for LRU ordering invariants

2. **Stress testing:** Add tests with larger entry counts (1000+) to verify performance characteristics

3. **Error injection:** Test behavior when ETS operations fail (though rare)

---

## 8. Summary of Recommendations

### Priority 1 (Security - Before Production)

| Issue | Severity | Effort |
|-------|----------|--------|
| Safe deserialization validation | CRITICAL | Medium |
| Path traversal prevention | HIGH | Low |
| Memory-based cache limits | HIGH | Medium |

### Priority 2 (Performance - Near-term)

| Issue | Severity | Effort |
|-------|----------|--------|
| Predicate reverse index | MEDIUM | Medium |
| Direct ETS reads for hot path | MEDIUM | High |
| Add terminate/2 for graceful shutdown | LOW | Low |

### Priority 3 (Code Quality - When Convenient)

| Issue | Severity | Effort |
|-------|----------|--------|
| Register telemetry events | LOW | Low |
| Use MapSet.disjoint?/2 | LOW | Low |
| Extract common LRU module | LOW | Medium |
| Use centralized telemetry helpers | LOW | Low |

---

## Conclusion

Section 5.3 provides a solid foundation for query result caching. The implementation is well-tested (46 tests, 0 failures) and follows Elixir conventions. The security concerns around deserialization and path handling should be addressed before production deployment. The performance recommendations become important at scale but are not blockers for initial use.

**Overall Assessment:** Good implementation with addressable security concerns.
