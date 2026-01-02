# Phase 3.1 Statistics Collection - Comprehensive Review

**Date:** 2026-01-02
**Reviewers:** 7 parallel review agents (Factual, QA, Senior Engineer, Security, Consistency, Redundancy, Elixir-specific)
**Files Reviewed:**
- `lib/triple_store/statistics.ex`
- `lib/triple_store/statistics/server.ex`
- `test/triple_store/statistics_test.exs`
- `test/triple_store/statistics/server_test.exs`

---

## Summary

| Category | Count | Priority |
|----------|-------|----------|
| Blockers | 2 | Must fix |
| Concerns | 12 | Should address |
| Suggestions | 15 | Nice to have |
| Good Practices | 20+ | Positive |

---

## 🚨 Blockers (Must Fix Before Production)

### B1: Unsafe `binary_to_term/1` Usage

**Location:** `statistics.ex:220`

```elixir
stats = :erlang.binary_to_term(encoded)  # UNSAFE
```

**Risk:** `binary_to_term/1` without the `:safe` option can deserialize arbitrary Erlang terms, including atoms. An attacker who can write to the RocksDB storage could cause atom table exhaustion (denial of service).

**Fix:**
```elixir
stats = :erlang.binary_to_term(encoded, [:safe])
```

### B2: Unbounded Memory Allocation in Histogram Building

**Location:** `statistics.ex:485-491`

```elixir
numeric_values =
  stream
  |> Stream.map(fn {key, _value} -> extract_second_id(key) end)
  |> Stream.filter(&is_numeric_id?/1)
  |> Stream.map(&decode_numeric_value/1)
  |> Enum.to_list()  # Materializes entire list into memory
```

**Risk:** For predicates with millions of numeric values (e.g., timestamps, prices), this loads the entire list into memory, potentially causing OOM.

**Fix:** Implement two-pass streaming:
1. First pass: Stream to find min/max only
2. Second pass: Stream to populate buckets without materializing

---

## ⚠️ Concerns (Should Address)

### C1: Duplicate GenServer Implementations

**Location:** Both `statistics/server.ex` and `statistics/cache.ex` exist

Two GenServers serve similar purposes:
- `Statistics.Server` - New, with write-threshold and periodic refresh
- `Statistics.Cache` - Existing, with periodic refresh

The application supervision tree references `Statistics.Cache`. This creates confusion about which to use.

**Recommendation:** Consolidate into one implementation or clearly document when to use each.

### C2: GenServer Call with `:infinity` Timeout

**Location:** `server.ex:109, 129`

```elixir
GenServer.call(server, :get_stats, :infinity)
```

**Risk:** For large datasets, statistics collection can take significant time. This blocks the calling process indefinitely with no way to cancel or recover from a hung NIF.

**Recommendation:** Use a configurable timeout with reasonable default (e.g., 60 seconds).

### C3: `refresh_in_progress` Flag Race Condition

**Location:** `server.ex:330`

The `refresh_in_progress` flag is never set to `true` before starting refresh - only set to `false` after completion. Concurrent background refresh triggers could start multiple refreshes.

**Fix:** Set `refresh_in_progress: true` at start of `do_refresh/1`.

### C4: Multiple Full Index Scans in `collect/2`

**Location:** `statistics.ex:144-171`

The function performs 5 separate index scans:
1. `triple_count(db)` - SPO full scan
2. `distinct_subjects(db)` - SPO full scan
3. `distinct_predicates(db)` - POS full scan
4. `distinct_objects(db)` - OSP full scan
5. `build_predicate_histogram(db)` - POS full scan

Plus per-predicate scans for numeric histograms.

**Recommendation:** Combine into single-pass collection where possible.

### C5: Duplicated Type Tag Constants

**Location:** `statistics.ex:100-103`

```elixir
@type_integer 0b0100
@type_decimal 0b0101
@type_datetime 0b0110
```

These duplicate constants from `Dictionary` module which already exposes:
- `Dictionary.type_integer/0`
- `Dictionary.type_decimal/0`
- `Dictionary.type_datetime/0`

**Fix:** Use Dictionary's public accessors.

### C6: Duplicated Inline Numeric Decoding Logic

**Location:** `statistics.ex:639-699`

Functions `decode_inline_integer/1`, `decode_inline_decimal/1` duplicate Dictionary module functionality.

**Fix:** Refactor to use Dictionary's public API (`Dictionary.decode_inline/1`).

### C7: No Validation of Loaded Statistics Structure

**Location:** `statistics.ex:220`

Even with `:safe` option, there's no validation that the deserialized term has the expected structure.

**Recommendation:** Add validation that deserialized term matches `stats()` type.

### C8: Missing Error Handling Tests

**Location:** Test files

No tests verify behavior when:
- `NIF.prefix_stream/3` fails
- `NIF.put/4` fails in `save/2`
- `NIF.get/4` fails in `load/1`

### C9: Missing Numeric Type Tests in Histograms

**Location:** `statistics_test.exs:401-449`

Tests only cover integers. Implementation handles decimals (`@type_decimal`) and datetimes (`@type_datetime`) but these are not tested. Negative integers also not tested.

### C10: Periodic Refresh Not Tested

**Location:** `server.ex:304-319`

The `:periodic_refresh` message handler exists but is not tested.

### C11: Telemetry Event Naming Inconsistency

**Location:** `statistics.ex:164-168`, `server.ex:337-341`

Events use `[:triple_store, :statistics, ...]` but other caches use `[:triple_store, :cache, :stats, ...]`.

### C12: Test Uses `Process.sleep` for Synchronization

**Location:** `server_test.exs:74, 100, 151`

Using `Process.sleep(100)` for synchronization is fragile and can cause flaky tests.

---

## 💡 Suggestions (Nice to Have)

### S1: Single-Pass Statistics Collection
Combine distinct counting and histogram building into single passes over indices.

### S2: Add Sampling for Large Datasets
Consider reservoir sampling for approximate histograms on very large predicates.

### S3: Add Telemetry for Cache Hits
Server doesn't emit telemetry when returning cached stats vs. fresh collection.

### S4: Store Bucket Width in Histogram
Avoid recalculation during `estimate_range_from_histogram/3`.

### S5: Use `Keyword.validate!/2` for Options
Elixir 1.13+ provides cleaner option validation.

### S6: Add `@compile {:inline, ...}` for Hot Paths
Functions like `extract_first_id/1` called millions of times during scans.

### S7: Consider Adding `terminate/2` Callback
For cleanup if needed on shutdown.

### S8: Add Memory Tracking Like SubjectCache
If statistics caching could grow large.

### S9: Extract Shared Telemetry Timing Pattern
Multiple modules repeat same timing pattern.

### S10: Use `Stream.dedup_by/2` Instead of Map + Dedup
More efficient streaming.

### S11: Test Custom `bucket_count` Option
Only default is tested.

### S12: Test Telemetry Events
Verify events are emitted with correct measurements.

### S13: Add Schema Versioning Migration Logic
Check version on load and trigger re-collection if outdated.

### S14: Consider Child Spec for Supervision
Cleaner supervision tree integration.

### S15: Document Test Tag Usage
`@tag :skip_db_close` appears unused.

---

## ✅ Good Practices Noticed

### Architecture & Design
- Clean module organization with section separators
- Comprehensive type specifications with `@typedoc`
- Version field for forward compatibility (`@stats_version 1`)
- Reserved key prefix for statistics storage (can't conflict with term IDs)
- Proper `@impl true` annotations on all callbacks

### Error Handling
- Excellent use of `with` chains for error propagation
- Consistent `{:ok, value} | {:error, reason}` returns
- Graceful error handling in server with logging

### OTP Patterns
- Proper GenServer cast vs call usage
- Self-scheduling with `send/2` and `Process.send_after/3`
- Refresh-in-progress guard prevents concurrent refreshes

### Performance
- Stream usage for large dataset processing
- Compressed serialization for persistence
- Telemetry integration for observability

### Testing
- Comprehensive empty store tests
- Single value edge cases tested
- Large ID edge case tested (`0xFFFFFFFFFFFFFFFF`)
- Insert/delete consistency tested
- Cache verification tests
- Proper test isolation with unique paths
- Proper cleanup in `on_exit` callbacks

### Documentation
- Comprehensive `@moduledoc` with examples
- Clear `@doc` blocks with Arguments/Returns/Examples sections
- Follows project documentation conventions

---

## Implementation vs Plan Verification

All planned tasks (3.1.1-3.1.5) are implemented as specified:

| Task | Status | Notes |
|------|--------|-------|
| 3.1.1 Cardinality Statistics | ✅ Complete | All 10 subtasks implemented |
| 3.1.2 Predicate Cardinalities | ✅ Complete | 5/6 subtasks (incremental deferred) |
| 3.1.3 Numeric Histograms | ✅ Complete | All 6 subtasks implemented |
| 3.1.4 Statistics Refresh | ✅ Complete | 5/6 subtasks (incremental deferred) |
| 3.1.5 Unit Tests | ✅ Complete | All test requirements covered |

**Documented Deferrals:**
- 3.1.2.5: Incremental update on insert/delete (use refresh instead)
- 3.1.4.3: Incremental refresh (full refresh is simpler)

---

## Priority Action Items

### Must Fix (Blockers)
1. Add `:safe` option to `binary_to_term` call
2. Implement streaming histogram building to avoid OOM

### Should Fix (High Priority Concerns)
3. Resolve duplicate GenServer implementations (Server vs Cache)
4. Fix `refresh_in_progress` race condition
5. Remove duplicated type tag constants (use Dictionary)
6. Add configurable timeout instead of `:infinity`

### Should Add (Testing Gaps)
7. Add error handling tests
8. Add decimal/datetime histogram tests
9. Add periodic refresh test
10. Replace `Process.sleep` with proper synchronization

---

## Conclusion

The Phase 3.1 Statistics Collection implementation is well-structured with good documentation and comprehensive functionality. The primary issues are:

1. **Security:** Unsafe `binary_to_term` must be fixed before production
2. **Memory:** Histogram building needs streaming to handle large predicates
3. **Architecture:** Duplicate GenServer implementations need resolution
4. **Testing:** Some edge cases and error paths need coverage

Overall quality is high with proper OTP patterns, type safety, and telemetry integration. The concerns are addressable and don't require major refactoring.
