# Phase 4.1 Prefix Extractor Optimization - Feature Plan

**Date:** 2026-01-02
**Branch:** `feature/phase-4.1-prefix-extractor`
**Source:** `notes/planning/performance/phase-04-storage-layer-tuning.md`

## Overview

This implements Section 4.1 from the Phase 4 Storage Layer Tuning plan. The goal is to configure RocksDB's native prefix extractor to eliminate manual bounds checking on every `iterator_next` call.

## Current State

The current implementation in `native/rocksdb_nif/src/lib.rs`:

1. Creates iterators without a prefix extractor (lines 181-187)
2. Manually checks prefix bounds on every `iterator_next` call (lines 873-876):
   ```rust
   if !key.starts_with(&iter_ref.prefix) {
       return Ok(atoms::iterator_end().encode(env));
   }
   ```

This adds per-row overhead that can be eliminated by letting RocksDB handle bounds checking natively.

## Solution

Use RocksDB's `SliceTransform` prefix extractor with `ReadOptions::set_prefix_same_as_start(true)` to enable native prefix bounds checking.

### Key Design Decisions

1. **8-byte fixed prefix for index CFs (spo, pos, osp)**: The first 8 bytes represent the first component of the triple (subject, predicate, or object ID).

2. **8-byte fixed prefix for numeric_range**: The first 8 bytes represent the predicate ID.

3. **No prefix extractor for dictionary CFs (id2str, str2id)**: These use point lookups, not prefix scans.

4. **No prefix extractor for derived CF**: Uses sequential full-table scans.

5. **Use `prefix_same_as_start(true)`**: Tells RocksDB to stop iteration when prefix changes.

6. **Keep manual prefix check as fallback**: For edge cases and safety.

## Implementation Plan

### Task 1: Configure SliceTransform for Index CFs
- [x] Add `SliceTransform::create_fixed_prefix(8)` for spo, pos, osp, numeric_range
- [x] Update `open()` function to use per-CF options

### Task 2: Update Iterator Creation
- [x] Add `set_prefix_same_as_start(true)` to ReadOptions
- [x] Add `set_total_order_seek(false)` to ReadOptions
- [x] Update `prefix_iterator()` function
- [x] Update `snapshot_prefix_iterator()` function

### Task 3: Optimize iterator_next
- [x] Remove or keep optional manual prefix check
- [x] Update both `iterator_next()` and `snapshot_iterator_next()`

### Task 4: Add Tests
- [x] Test prefix iterator returns correct results
- [x] Test iterator stops at prefix boundary
- [x] Test seek positions correctly

### Task 5: Update Documentation
- [x] Mark tasks complete in plan
- [x] Create summary document

## Current Status

**Started:** 2026-01-02
**Completed:** 2026-01-03
**Status:** Complete

## Implementation Notes

During implementation, we discovered that relying solely on native prefix bounds (`prefix_same_as_start(true)`) was unreliable for user prefixes shorter than the fixed 8-byte prefix extractor length. The solution was to:

1. Configure prefix extractors for bloom filter benefits (memtable_prefix_bloom_ratio: 0.1)
2. Use `prefix_same_as_start(true)` + `total_order_seek(false)` for prefixes >= 8 bytes
3. Use `total_order_seek(true)` for prefixes < 8 bytes to avoid bloom filter issues
4. Keep the manual `starts_with` prefix check as a safety net for all cases

This approach provides bloom filter performance benefits while maintaining correctness for all prefix lengths.
