# Phase 3.1 Statistics Collection - Review Fixes

**Date:** 2026-01-02
**Branch:** `feature/phase-3.1-review-fixes`
**Source:** `notes/reviews/phase-3.1-statistics-collection-review.md`

## Overview

Address all blockers, concerns, and suggestions from the Phase 3.1 Statistics Collection review.

## Implementation Plan

### Blockers (Must Fix)

- [x] B1: Add `:safe` option to `binary_to_term` call in `statistics.ex:220`
- [x] B2: Implement streaming histogram building to avoid OOM

### Concerns (Should Address)

- [x] C1: Resolve duplicate GenServer implementations (Server vs Cache) - Consolidated to Server
- [x] C2: Add configurable timeout instead of `:infinity`
- [x] C3: Fix `refresh_in_progress` race condition
- [ ] C4: Optimize multiple index scans (deferred - optimization not critical)
- [x] C5: Remove duplicated type tag constants (use Dictionary)
- [x] C6: Remove duplicated inline numeric decoding logic
- [x] C7: Add validation of loaded statistics structure
- [x] C8: Add error handling tests
- [x] C9: Add decimal/datetime histogram tests
- [x] C10: Add periodic refresh test
- [x] C11: Fix telemetry event naming consistency
- [x] C12: Replace `Process.sleep` with proper synchronization

### Suggestions (Nice to Have)

- [ ] S1: Single-pass statistics collection (deferred - complexity vs benefit)
- [ ] S2: Add sampling for large datasets (deferred - no OOM after B2 fix)
- [x] S3: Add telemetry for cache hits
- [x] S4: Store bucket width in histogram
- [x] S5: Use `Keyword.validate!/2` for options
- [x] S6: Add `@compile {:inline, ...}` for hot paths
- [x] S7: Add `terminate/2` callback for cleanup
- [ ] S8: Add memory tracking like SubjectCache (deferred - low priority)
- [ ] S9: Extract shared telemetry timing pattern (deferred - out of scope)
- [ ] S10: Use `Stream.dedup_by/2` instead of Map + Dedup (minimal impact)
- [x] S11: Test custom `bucket_count` option
- [x] S12: Test telemetry events
- [x] S13: Add schema versioning/migration logic
- [x] S14: Add child spec for supervision
- [ ] S15: Document test tag usage (tag removed)

## Changes Made

### `lib/triple_store/statistics.ex`

1. Added `:safe` option to `binary_to_term/2` (B1)
2. Implemented two-pass streaming for histogram building (B2)
3. Added validation for loaded statistics structure (C7)
4. Replaced duplicated type tag constants with Dictionary accessors (C5)
5. Replaced duplicated numeric decoding with Dictionary.decode_inline/1 (C6)
6. Stored bucket_width in histogram structure (S4)
7. Added `@compile {:inline, ...}` for hot paths (S6)
8. Added schema version migration support (S13)

### `lib/triple_store/statistics/server.ex`

1. Added configurable timeout with default 60 seconds (C2)
2. Fixed refresh_in_progress race condition (C3)
3. Added telemetry for cache hits (S3)
4. Used `Keyword.validate!/2` for options validation (S5)
5. Added `terminate/2` callback (S7)
6. Added `child_spec/1` function (S14)
7. Fixed telemetry event naming to use `:cache` namespace (C11)

### `lib/triple_store/statistics/cache.ex`

1. Deprecated in favor of Statistics.Server (C1)
2. Added deprecation warning in moduledoc

### `lib/triple_store/application.ex`

1. Updated to use Statistics.Server instead of Cache

### Test Changes

1. Added error handling tests (C8)
2. Added decimal/datetime histogram tests (C9)
3. Added periodic refresh test (C10)
4. Replaced `Process.sleep` with synchronization helpers (C12)
5. Added custom bucket_count option test (S11)
6. Added telemetry events tests (S12)

## Deferred Items

- C4: Multiple index scan optimization (complex, requires major refactor)
- S1: Single-pass statistics collection (same reason as C4)
- S2: Sampling for large datasets (B2 fix removes OOM risk)
- S8: Memory tracking (low priority for statistics)
- S9: Shared telemetry pattern (out of scope)
- S10: Stream.dedup_by (minimal impact)
- S15: Test tag documentation (tag removed)

## Progress Tracking

| Task | Status | Notes |
|------|--------|-------|
| B1 | Complete | Added :safe option |
| B2 | Complete | Two-pass streaming |
| C1 | Complete | Deprecated Cache, use Server |
| C2 | Complete | 60s default timeout |
| C3 | Complete | Set flag at start |
| C5-C6 | Complete | Use Dictionary module |
| C7 | Complete | Validate structure on load |
| C8-C10 | Complete | Tests added |
| C11 | Complete | Fixed naming |
| C12 | Complete | Proper sync |
| S3-S7 | Complete | Improvements added |
| S11-S14 | Complete | Tests and features |

## Current Status

**Started:** 2026-01-02
**Completed:** 2026-01-02
**Status:** Complete - All 4445 tests passing
