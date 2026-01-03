# Phase 4.2 Column Family Tuning - Feature Plan

**Date:** 2026-01-03
**Branch:** `feature/phase-4.2-column-family-tuning`
**Source:** `notes/planning/performance/phase-04-storage-layer-tuning.md`

## Overview

This implements Section 4.2 from the Phase 4 Storage Layer Tuning plan. The goal is to optimize RocksDB column family settings for each access pattern.

## Current State

The Elixir configuration modules (`lib/triple_store/config/column_family.ex`, `compression.ex`, `rocksdb.ex`) define tuning parameters, but the Rust NIF's `open()` function uses default RocksDB settings for most options.

## Solution

Update the Rust NIF to configure column families with optimized settings per access pattern:

### Column Family Access Patterns

| CF | Type | Access Pattern | Bloom | Block Size |
|----|------|----------------|-------|------------|
| id2str, str2id | Dictionary | Point lookups | 14 bits/key | 2KB |
| spo, pos, osp | Index | Prefix scans | 12 bits/key (with prefix extractor) | 8KB |
| derived | Derived | Bulk sequential | Disabled | 32KB |

### Configuration Details

1. **Bloom Filters (4.2.1)**
   - Dictionary CFs: 14 bits/key (~0.01% FPR), full-key bloom
   - Index CFs: 12 bits/key (~0.09% FPR), prefix bloom via SliceTransform
   - Derived CF: Disabled (sequential access doesn't benefit)

2. **Block Sizes (4.2.2)**
   - Dictionary CFs: 2KB (small for point lookups)
   - Index CFs: 8KB (balanced for prefix scans)
   - Derived CF: 32KB (large for sequential reads)

3. **Compression (4.2.3)**
   - All CFs: LZ4 (fast, reasonable ratio)
   - L0: No compression (short-lived memtables)
   - Consider Zstd for cold data in future

4. **Cache Configuration (4.2.4)**
   - cache_index_and_filter_blocks: true (all CFs)
   - pin_l0_filter_and_index_blocks_in_cache: true (dictionary + index CFs)
   - optimize_filters_for_hits: true (dictionary CFs)

## Implementation Plan

### Task 1: Update Rust NIF open() with CF tuning
- [x] Add BlockBasedOptions configuration per CF
- [x] Configure bloom filters per CF type
- [x] Set block sizes per CF type
- [x] Configure cache hints
- [x] Configure per-level compression (L0: None, L1+: LZ4)

### Task 2: Update Elixir config modules
- [x] Update column_family.ex with new values
- [x] Ensure values match Rust implementation

### Task 3: Add tests
- [x] Verify tuning doesn't break existing tests (448 tests pass)
- [x] Update config validation tests to match new values

### Task 4: Documentation
- [x] Update planning document
- [x] Create summary

## Current Status

**Started:** 2026-01-03
**Completed:** 2026-01-03
**Status:** Complete
