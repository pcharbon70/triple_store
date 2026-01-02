# Phase 3.2 Query Result Caching - Feature Plan

**Date:** 2026-01-02
**Branch:** `feature/phase-3.2-query-result-caching`
**Source:** `notes/planning/performance/phase-03-query-engine-improvements.md`

## Overview

Enhance the existing Query.Cache implementation to complete Section 3.2 requirements:
- Add query normalization for cache keys (variable renaming)
- Add executor integration with `:use_cache` option
- Add non-cacheable query detection (RAND/NOW/UPDATE)

## Existing Implementation Status

The `TripleStore.Query.Cache` module already implements most of Section 3.2:

| Task | Status | Notes |
|------|--------|-------|
| 3.2.1.1 Cache key structure | ✅ Complete | SHA256 hash of query |
| 3.2.1.2 Cache value structure | ✅ Complete | Result + metadata + predicates |
| 3.2.1.3 QueryCache module | ✅ Complete | `TripleStore.Query.Cache` |
| 3.2.1.4 ETS with access tracking | ✅ Complete | LRU tables |
| 3.2.1.5 get/2 function | ✅ Complete | Returns :ok/:miss/:expired |
| 3.2.1.6 put/3 function | ✅ Complete | With TTL support |
| 3.2.1.7 LRU eviction | ✅ Complete | Entry count + memory limits |
| 3.2.1.8 Configuration | ✅ Complete | max_entries, max_memory, ttl_ms |
| 3.2.2.1 Normalization rules | ✅ Complete | Variable renaming implemented |
| 3.2.2.2 normalize_query/1 | ✅ Complete | Ported from PlanCache |
| 3.2.2.3 Variable renaming | ✅ Complete | Positional indices |
| 3.2.2.4 Predicate ordering | ⏳ Optional | Not critical for correctness |
| 3.2.2.5 hash_query/1 | ✅ Complete | Uses :crypto.hash/2 |
| 3.2.2.6 Parameter substitution | ✅ Complete | Handled by key structure |
| 3.2.3.1 TTL storage | ✅ Complete | created_at in entry |
| 3.2.3.2 check_ttl/1 | ✅ Complete | expired?/2 function |
| 3.2.3.3 Background expiration | ✅ Complete | cleanup_expired/1 |
| 3.2.3.4 Per-query TTL | ⏳ Partial | Server-wide TTL only |
| 3.2.3.5 Default TTL | ✅ Complete | 5 minutes default |
| 3.2.3.6 clear/1 | ✅ Complete | invalidate/1 |
| 3.2.4.1 Invalidation strategy | ✅ Complete | Predicate-based |
| 3.2.4.2 Predicate invalidation | ✅ Complete | invalidate_predicates/2 |
| 3.2.4.3 Predicate tracking | ✅ Complete | predicate_index ETS |
| 3.2.4.4 Insert/delete hooks | ✅ Complete | In update_executor |
| 3.2.4.5 cache_invalidation option | ⏳ Partial | Conservative only |
| 3.2.5.1 Integration point | ✅ Complete | Query.query/3 integration |
| 3.2.5.2 get_or_execute/3 | ✅ Complete | In Query.Cache |
| 3.2.5.3 :use_cache option | ✅ Complete | Added to Query.query/3 |
| 3.2.5.4 Skip UPDATE queries | ✅ Complete | In update_executor |
| 3.2.5.5 Skip RAND/NOW | ✅ Complete | has_non_deterministic_functions?/1 |
| 3.2.5.6 Hit rate telemetry | ✅ Complete | hit/miss/expired events |

## Implementation Plan

### Task 1: Add Query Normalization (3.2.2)

- [x] 1.1 Add normalize_query/1 to Query.Cache (port from PlanCache)
- [x] 1.2 Add compute_normalized_key/1 that normalizes before hashing
- [x] 1.3 Update get_or_execute to use normalized keys
- [x] 1.4 Add tests for variable renaming

### Task 2: Add Executor Integration (3.2.5)

- [x] 2.1 Add :use_cache option to Query.query/3
- [x] 2.2 Add :cache_name option for custom cache
- [x] 2.3 Add :predicates extraction from query for invalidation
- [x] 2.4 Add tests for cache integration

### Task 3: Add Non-Cacheable Detection (3.2.5)

- [x] 3.1 Add has_non_deterministic_functions?/1 to detect RAND/NOW
- [x] 3.2 Skip caching when non-deterministic functions present
- [x] 3.3 Add tests for non-cacheable detection

## Files Changed

| File | Changes |
|------|---------|
| `lib/triple_store/query/cache.ex` | Add normalization functions |
| `lib/triple_store/sparql/query.ex` | Add :use_cache option integration |
| `test/triple_store/query/cache_test.exs` | Add normalization tests |
| `test/triple_store/sparql/query_test.exs` | Add cache integration tests |

## Current Status

**Started:** 2026-01-02
**Completed:** 2026-01-02
**Status:** Complete - All 189 tests passing
