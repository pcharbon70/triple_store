# Phase 4: Storage Layer Tuning

## Overview

Phase 4 optimizes the underlying storage layer for improved performance. These are infrastructure-level changes that provide incremental improvements across all operations.

Key improvements:
- **Prefix Extractor Optimization**: Let RocksDB handle iterator bounds checking natively
- **Column Family Tuning**: Optimize bloom filters and block sizes per column family
- **Snapshot Management**: Implement TTL and auto-release for long-lived snapshots

These optimizations are lower priority than Phases 1-3 but provide foundational improvements that benefit all workloads.

---

## 4.1 Prefix Extractor Optimization

- [x] **Section 4.1 Analysis Complete** (2025-12-31)

Current iterator implementation performs bounds checking in Rust on every `iterator_next()` call (`lib.rs:844-845`). This adds overhead for every row returned. RocksDB's SliceTransform prefix extractor can handle bounds checking natively, eliminating this per-row overhead.

### 4.1.1 SliceTransform Configuration

- [x] **Task 4.1.1 Analysis Complete** (2025-12-31)

Configure RocksDB prefix extractor for index column families.

- [x] 4.1.1.1 Analyze current iterator bounds checking in `lib.rs:844-845`
- [x] 4.1.1.2 Research RocksDB SliceTransform API
- [x] 4.1.1.3 Design prefix length for triple indices (8 bytes = first component)
- [x] 4.1.1.4 Implement `set_prefix_extractor` in column family options
- [x] 4.1.1.5 Configure for SPO, POS, OSP column families
- [x] 4.1.1.6 Test prefix extractor with existing data (migration considerations)

### 4.1.2 Iterator Bounds Optimization

- [x] **Task 4.1.2 Analysis Complete** (2025-12-31)

Modify iterator to leverage native prefix bounds.

- [x] 4.1.2.1 Analyze current IteratorRef structure in `lib.rs:37-52`
- [x] 4.1.2.2 Enable `total_order_seek(false)` for prefix iterators
- [x] 4.1.2.3 Enable `prefix_same_as_start(true)` for automatic bounds
- [x] 4.1.2.4 Keep manual prefix check as safety net (native bounds alone proved unreliable)
- [x] 4.1.2.5 Verified iterator correctness with all existing tests

### 4.1.3 Seek Optimization

- [x] **Task 4.1.3 Analysis Complete** (2025-12-31)

Optimize seek operations for Leapfrog Triejoin.

- [x] 4.1.3.1 Analyze current `iterator_seek` in `lib.rs`
- [x] 4.1.3.2 Configure prefix-based vs total-order seek based on prefix length
- [x] 4.1.3.3 Test seek performance with prefix extractor
- [x] 4.1.3.4 Validate Leapfrog correctness with new configuration

### 4.1.4 Unit Tests

- [x] **Task 4.1.4 Complete** (2026-01-03)

- [x] 4.1.4.1 Test prefix iterator returns same results with extractor (5 Rust tests, 38 Elixir iterator tests)
- [x] 4.1.4.2 Test iterator stops at prefix boundary correctly
- [x] 4.1.4.3 Test seek positions correctly with extractor
- [x] 4.1.4.4 Test Leapfrog operations work correctly (163 backend tests pass)
- [x] 4.1.4.5 Verified correctness with full test suite (4493 tests)

---

## 4.2 Column Family Tuning

- [x] **Section 4.2 Analysis Complete** (2025-12-31)

Each column family has different access patterns that warrant specialized configuration:
- **Dictionary CFs** (id2str, str2id): Point lookups, high read frequency
- **Index CFs** (spo, pos, osp): Prefix scans, range queries
- **Derived CF**: Bulk writes, sequential reads

### 4.2.1 Bloom Filter Configuration

- [x] **Task 4.2.1 Analysis Complete** (2025-12-31)

Optimize bloom filter settings per column family.

- [x] 4.2.1.1 Analyze current bloom filter config in `column_family.ex`
- [x] 4.2.1.2 Calculate optimal bits/key for target FPR
- [ ] 4.2.1.3 Increase dictionary CFs to 14 bits/key (~0.01% FPR)
- [ ] 4.2.1.4 Increase index CFs to 12 bits/key (~0.09% FPR)
- [ ] 4.2.1.5 Disable bloom filter for derived CF (sequential access)
- [ ] 4.2.1.6 Enable partition filters for large datasets
- [ ] 4.2.1.7 Benchmark bloom filter memory vs lookup improvement

### 4.2.2 Block Size Optimization

- [x] **Task 4.2.2 Analysis Complete** (2025-12-31)

Tune block sizes for access patterns.

- [x] 4.2.2.1 Analyze current block size config in `column_family.ex`
- [ ] 4.2.2.2 Reduce dictionary CF block size to 2KB (point lookups)
- [ ] 4.2.2.3 Increase index CF block size to 8KB (prefix scans)
- [ ] 4.2.2.4 Increase derived CF block size to 32KB (sequential reads)
- [ ] 4.2.2.5 Test block size impact on cache efficiency

### 4.2.3 Compression Tuning

- [x] **Task 4.2.3 Analysis Complete** (2025-12-31)

Fine-tune compression per column family and level.

- [x] 4.2.3.1 Analyze current compression config in `compression.ex`
- [ ] 4.2.3.2 Test Zstd dictionary training for better compression
- [ ] 4.2.3.3 Tune per-level compression thresholds
- [ ] 4.2.3.4 Consider disabling L0 compression for write speed
- [ ] 4.2.3.5 Benchmark compression ratio vs CPU trade-off

### 4.2.4 Cache Configuration

- [x] **Task 4.2.4 Analysis Complete** (2025-12-31)

Optimize block cache allocation.

- [x] 4.2.4.1 Analyze current cache config in `rocksdb.ex`
- [ ] 4.2.4.2 Implement per-CF cache priority hints
- [ ] 4.2.4.3 Pin index and filter blocks for hot CFs
- [ ] 4.2.4.4 Configure cache index and filter blocks
- [ ] 4.2.4.5 Add cache statistics telemetry

### 4.2.5 Unit Tests

- [ ] **Task 4.2.5 Complete**

- [ ] 4.2.5.1 Test bloom filter FPR matches configuration
- [ ] 4.2.5.2 Test block size affects cache behavior
- [ ] 4.2.5.3 Test compression ratios per CF
- [ ] 4.2.5.4 Test cache hit rates with new configuration
- [ ] 4.2.5.5 Benchmark overall read/write improvement

---

## 4.3 Snapshot Management

- [x] **Section 4.3 Analysis Complete** (2025-12-31)

Long-lived snapshots prevent compaction and retain old data, causing storage bloat and performance degradation. Implementing TTL and auto-release ensures snapshots don't leak resources.

### 4.3.1 TTL Implementation

- [x] **Task 4.3.1 Analysis Complete** (2025-12-31)

Add time-to-live for snapshots.

- [x] 4.3.1.1 Analyze current snapshot handling in `lib.rs:1006-1032`
- [ ] 4.3.1.2 Add timestamp to SnapshotRef struct
- [ ] 4.3.1.3 Implement `snapshot_with_ttl/2` NIF
- [ ] 4.3.1.4 Add TTL check to snapshot operations
- [ ] 4.3.1.5 Default TTL: 5 minutes
- [ ] 4.3.1.6 Add configuration for custom TTL

### 4.3.2 Auto-Release Mechanism

- [x] **Task 4.3.2 Analysis Complete** (2025-12-31)

Implement automatic snapshot cleanup.

- [x] 4.3.2.1 Design auto-release strategy (process monitoring vs timer)
- [ ] 4.3.2.2 Implement snapshot registry GenServer
- [ ] 4.3.2.3 Track snapshot owner processes
- [ ] 4.3.2.4 Release snapshots when owner terminates
- [ ] 4.3.2.5 Implement periodic TTL check (every minute)
- [ ] 4.3.2.6 Add telemetry for snapshot lifecycle

### 4.3.3 Safe Snapshot Wrapper

- [x] **Task 4.3.3 Analysis Complete** (2025-12-31)

Provide safe API for snapshot usage.

- [x] 4.3.3.1 Design `with_snapshot` API pattern
- [ ] 4.3.3.2 Create `TripleStore.Snapshot` module
- [ ] 4.3.3.3 Implement `with_snapshot/2` ensuring cleanup
- [ ] 4.3.3.4 Handle exceptions within snapshot scope
- [ ] 4.3.3.5 Add warning when snapshot exceeds soft TTL

### 4.3.4 Unit Tests

- [ ] **Task 4.3.4 Complete**

- [ ] 4.3.4.1 Test snapshot TTL expires correctly
- [ ] 4.3.4.2 Test auto-release on process termination
- [ ] 4.3.4.3 Test with_snapshot releases on success
- [ ] 4.3.4.4 Test with_snapshot releases on exception
- [ ] 4.3.4.5 Test snapshot registry cleanup

---

## 4.4 Integration Tests

- [ ] **Section 4.4 Complete**

End-to-end integration tests for storage layer improvements.

### 4.4.1 Storage Operations Tests

- [ ] **Task 4.4.1 Complete**

Test storage operations with tuned configuration.

- [ ] 4.4.1.1 Test prefix iterator with extractor
- [ ] 4.4.1.2 Test seek operations with extractor
- [ ] 4.4.1.3 Test bloom filter effectiveness
- [ ] 4.4.1.4 Test compression ratios

### 4.4.2 Resource Cleanup Tests

- [ ] **Task 4.4.2 Complete**

Test resource management and cleanup.

- [ ] 4.4.2.1 Test no snapshot leaks after workload
- [ ] 4.4.2.2 Test no iterator leaks after workload
- [ ] 4.4.2.3 Test database closes cleanly
- [ ] 4.4.2.4 Test storage reclaimed after delete

### 4.4.3 Performance Validation Tests

- [ ] **Task 4.4.3 Complete**

Validate storage performance improvements.

- [ ] 4.4.3.1 Test iterator throughput improvement
- [ ] 4.4.3.2 Test point lookup latency
- [ ] 4.4.3.3 Test bulk load with tuned configuration
- [ ] 4.4.3.4 Compare with baseline measurements

---

## Success Criteria

1. **Iterator Performance**: Prefix iterator throughput improved by removing manual bounds check
2. **Bloom Filter**: False positive rate <0.1% for dictionary lookups
3. **Block Cache**: Cache hit rate >80% for typical workloads
4. **Snapshot Safety**: No snapshot leaks, all released within TTL
5. **Compression**: Storage reduction >2x with acceptable CPU overhead

## Provides Foundation

This phase enables:
- **Future**: Advanced storage features (partitioning, tiered storage)
- **Operations**: Better monitoring and tuning capabilities

## References

- [Performance Improvement Analysis](../../reviews/benchmark-performance-improvements.md#part-5-storage-layer-improvements)
- [RocksDB NIF](../../../native/rocksdb_nif/src/lib.rs)
- [RocksDB Config](../../../lib/triple_store/config/rocksdb.ex)
- [Column Family Config](../../../lib/triple_store/config/column_family.ex)
- [Compression Config](../../../lib/triple_store/config/compression.ex)
