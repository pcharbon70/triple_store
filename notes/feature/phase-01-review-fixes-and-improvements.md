# Working Plan: Phase 1 Review Fixes and Improvements

## Branch: `feature/phase-01-review-fixes-and-improvements`

## Status: IN PROGRESS

## Overview

This feature addresses all concerns and implements suggested improvements from the comprehensive Phase 1 review (notes/reviews/phase-01-quad-storage-foundation-review.md). The review identified no blockers, but several concerns and suggestions for production readiness.

## Reference

- **Review Document:** `notes/reviews/phase-01-quad-storage-foundation-review.md`
- **Overall Grade:** A- (Strong, Production-Ready)
- **Test Coverage:** 155 tests, all passing

---

## Part 1: Fix Concerns

### 1. Architecture Concerns

#### 1.1 Error Handling in perform_prefix_scan/2
**Location:** `lib/triple_store/quad_operations.ex`
**Issue:** Catches all errors, not just `:halt`, which could mask serious errors
**Fix:** Replace catch/throw with pattern matching on expected return values

#### 1.2 NIF Dependency in QuadOperations
**Location:** `lib/triple_store/quad_operations.ex`
**Issue:** Direct NIF usage despite deprecation notice
**Fix:** Use ErlangAdapter instead of NIF module

#### 1.3 Add Schema Validation on Write
**Location:** `lib/triple_store/quad_operations.ex`
**Issue:** No validation that database is quad store before writing
**Fix:** Add quad store check in insert/delete operations

### 2. Security Concerns

#### 2.1 Path Validation
**Location:** `lib/triple_store/backend/rocksdb/erlang_adapter.ex:1659-1692`
**Issue:** Literal `".."` check can be bypassed with URL encoding
**Fix:** Use `Path.safe_relative/1` or equivalent validation

#### 2.2 Security Documentation
**Location:** New file `notes/security/threat-model.md`
**Issue:** Missing security documentation and threat model
**Fix:** Document security assumptions and threat model

### 3. Consistency Concerns

#### 3.1 CRUD Return Value Inconsistency
**Location:** `lib/triple_store/quad_operations.ex`
**Issue:**
- `insert_quad/2` returns `{:ok, :inserted}` vs triple's `:ok`
- `delete_quad/2` returns `{:ok, :deleted}`/`{:ok, :not_found}` vs triple's `:ok`
**Fix:** Align with triple store API for consistency

---

## Part 2: Implement Suggested Improvements

### 2.1 Telemetry Hooks
**Location:** New file `lib/triple_store/telemetry.ex`
**Goal:** Add observability for operations
- `[:triple_store, :quad, :insert, :start]`
- `[:triple_store, :quad, :insert, :stop]`
- `[:triple_store, :quad, :delete, :start]`
- `[:triple_store, :quad, :delete, :stop]`
- `[:triple_store, :quad, :lookup, :start]`
- `[:triple_store, :quad, :lookup, :stop]`

### 2.2 Streaming API
**Location:** `lib/triple_store/quad_operations.ex`
**Goal:** Add lazy evaluation for large result sets
- Add `lookup_quads_stream/2` returning Stream
- Backward compatible with existing list-based API

### 2.3 Audit Logging (Optional)
**Location:** `lib/triple_store/audit_log.ex` (if needed)
**Goal:** Log sensitive operations for compliance

---

## Tasks

- [ ] Part 1.1: Fix error handling in perform_prefix_scan
- [ ] Part 1.2: Replace NIF dependency with ErlangAdapter
- [ ] Part 1.3: Add schema validation on write
- [ ] Part 2.1: Improve path validation
- [ ] Part 2.2: Add security documentation and threat model
- [ ] Part 3.1: Align CRUD return values with triple store API
- [ ] Part 2.3: Add telemetry hooks
- [ ] Part 2.4: Add streaming APIs
- [ ] Update tests for any API changes
- [ ] Run full test suite (155 tests)
- [ ] Write implementation summary

---

## Success Criteria

1. All concerns from review addressed
2. No existing tests broken
3. New functionality tested
4. Documentation updated
5. Code compiles without warnings

## Implementation Notes

- Changes to return values are breaking changes - document migration path
- Telemetry is opt-in via configuration
- Streaming API is additive, not replacing existing list API
