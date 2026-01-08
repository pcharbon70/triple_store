# Phase 2: Iterator and Snapshot Migration

## Overview

Phase 2 migrates advanced RocksDB features: iterators and snapshots. These are critical for SPARQL query execution (iterators for pattern matching) and isolation (snapshots for consistent reads).

**Key Goals:**
- Replace Rust iterator implementation with erlang-rocksdb iterators
- Implement prefix-based iteration using erlang-rocksdb's optimized seek
- Migrate snapshot functionality for consistent reads
- Optimize iteration using `rocksdb:fold` for reduced context switching

**Critical Changes:**
- Current: Manual `iterator_next()` calls with per-row BEAM-NIF boundary crossing
- Target: `rocksdb:fold` operations that iterate in C++ land

**Critical Files:**
- `lib/triple_store/sparql/leapfrog/trie_iterator.ex` - Leapfrog iterator (uses NIF directly)
- `lib/triple_store/index.ex` - Index scanning operations
- `lib/triple_store/snapshot.ex` - Current snapshot management

---

## 2.1 Iterator Operations Migration

- [x] **Section 2.1 Status** (Completed 2026-01-07)

Migrate iterator functionality from Rust NIF to erlang-rocksdb.

### 2.1.1 Basic Iterator Creation

- [x] **Task 2.1.1 Status** (Completed 2026-01-07)

Implement iterator creation matching current NIF API.

- [x] 2.1.1.1 Implement `iterator/3` for column-family-specific iterator creation
- [x] 2.1.1.2 Support iterator options: `{fill_cache, boolean()}`, `{snapshot, snapshot_ref}`
- [x] 2.1.1.3 Handle iterator resource lifecycle (creation, movement, closure)
- [x] 2.1.1.4 Map `iterator_move/2` operations: `next`, `prev`, `first`, `last`, `{seek, key}`
- [x] 2.1.1.5 Implement `iterator_close/1` for resource cleanup

### 2.1.2 Prefix Iterator Migration

- [x] **Task 2.1.2 Status** (Completed 2026-01-07)

Migrate prefix iterator functionality for index scanning.

- [x] 2.1.2.1 Implement `prefix_iterator/4` with prefix bounds checking
- [x] 2.1.2.2 Configure read options: `total_order_seek`, `prefix_same_as_start`
- [x] 2.1.2.3 Handle short prefix (< 8 bytes) vs long prefix (> 8 bytes) logic
- [x] 2.1.2.4 Map prefix iterator seek functionality
- [x] 2.1.2.5 Maintain prefix boundary checking (safety net for RocksDB behavior)

### 2.1.3 Seek Operations Migration

- [x] **Task 2.1.3 Status** (Completed 2026-01-07)

Migrate seek operations critical for Leapfrog Triejoin.

- [x] 2.1.3.1 Implement `iterator_seek/3` for positioning at specific keys
- [x] 2.1.3.2 Handle read options matching prefix extractor configuration
- [x] 2.1.3.3 Test seek behavior with non-existent keys (should position at next)
- [x] 2.1.3.4 Test seek past end of data (should return exhausted)
- [x] 2.1.3.5 Validate Leapfrog operations work correctly

### 2.1.4 Iterator Collect Operation

- [x] **Task 2.1.4 Status** (Completed 2026-01-07)

Implement convenience function for collecting all iterator results.

- [x] 2.1.4.1 Implement `iterator_collect/2` for collecting remaining entries
- [x] 2.1.4.2 Handle prefix boundary in collect operation
- [x] 2.1.4.3 Return results as list of `{key, value}` tuples
- [x] 2.1.4.4 Handle exhausted iterator case

### 2.1.5 Unit Tests

- [x] **Task 2.1.5 Status** (Completed 2026-01-07)

- [x] 2.1.5.1 Test basic iterator iteration matches current NIF
- [x] 2.1.5.2 Test prefix iterator respects prefix boundaries
- [x] 2.1.5.3 Test seek positions correctly for existing and non-existing keys
- [x] 2.1.5.4 Test iterator move operations (next, prev, first, last)
- [x] 2.1.5.5 Test iterator collect returns all entries within prefix
- [x] 2.1.5.6 Test Leapfrog Triejoin operations (163 tests)

---

## 2.2 Snapshot Operations Migration

- [x] **Section 2.2 Status** (Completed 2026-01-07)

Migrate snapshot functionality for point-in-time consistent reads.

### 2.2.1 Snapshot Creation and Release

- [x] **Task 2.2.1 Status** (Completed 2026-01-07)

Implement snapshot lifecycle management.

- [x] 2.2.1.1 Implement `snapshot/1` for creating point-in-time snapshots
- [x] 2.2.1.2 Implement `release_snapshot/1` for cleanup
- [x] 2.2.1.3 Track snapshot references for proper resource management
- [x] 2.2.1.4 Handle snapshot from database reference
- [x] 2.2.1.5 Return snapshot reference that can be used with read options

### 2.2.2 Snapshot Read Operations

- [x] **Task 2.2.2 Status** (Completed 2026-01-07)

Implement read operations from snapshots.

- [x] 2.2.2.1 Implement `snapshot_get/3` for reading from snapshot
- [x] 2.2.2.2 Configure read options with snapshot reference
- [x] 2.2.2.3 Handle not_found case consistently with normal get
- [x] 2.2.2.4 Verify snapshot provides point-in-time consistency

### 2.2.3 Snapshot Iterator Operations

- [x] **Task 2.2.3 Status** (Completed 2026-01-07)

Implement iterator creation from snapshots.

- [x] 2.2.3.1 Implement `snapshot_prefix_iterator/4` for iterating over snapshot
- [x] 2.2.3.2 Pass snapshot reference in iterator read options
- [x] 2.2.3.3 Ensure iterator sees data as of snapshot time
- [x] 2.2.3.4 Handle iterator lifecycle with snapshot reference

### 2.2.4 Unit Tests

- [x] **Task 2.2.4 Status** (Completed 2026-01-07)

- [x] 2.2.4.1 Test snapshot creation captures current state
- [x] 2.2.4.2 Test snapshot doesn't see subsequent writes
- [x] 2.2.4.3 Test snapshot_get returns data from snapshot time
- [x] 2.2.4.4 Test snapshot iterator sees historical data
- [x] 2.2.4.5 Test multiple snapshots can coexist
- [x] 2.2.4.6 Test snapshot release allows compaction of old data

---

## 2.3 Fold-Based Iteration Optimization

- [x] **Section 2.3 Status** (Completed 2026-01-07)

Optimize iteration using erlang-rocksdb's `fold` operations to reduce BEAM-NIF boundary crossings.

### 2.3.1 Fold Operations Implementation

- [x] **Task 2.3.1 Status** (Completed 2026-01-07)

Implement fold-based iteration for bulk operations.

- [x] 2.3.1.1 Implement `fold/5` for folding over prefix range
- [x] 2.3.1.2 Use `rocksdb:fold/4` with accumulator function
- [x] 2.3.1.3 Handle fold function signature: `fun(Key, Value, Acc) -> {ok, NewAcc}`
- [x] 2.3.1.4 Support `stop` return for early termination
- [x] 2.3.1.5 Configure read options for prefix-based folding

### 2.3.2 Fold Keys Operation

- [x] **Task 2.3.2 Status** (Completed 2026-01-07)

Implement keys-only folding for index scanning.

- [x] 2.3.2.1 Implement `fold_keys/5` for iterating keys without values
- [x] 2.3.2.2 Use `rocksdb:fold_keys/4` for efficiency
- [x] 2.3.2.3 Handle prefix boundary in fold_keys
- [x] 2.3.2.4 Support accumulator and stop mechanisms

### 2.3.3 Stream Operations

- [x] **Task 2.3.3 Status** (Completed 2026-01-07)

Implement lazy stream operations for large result sets.

- [x] 2.3.3.1 Create `prefix_stream/4` for Elixir Stream compatibility
- [x] 2.3.3.2 Use Stream.resource for proper resource cleanup
- [x] 2.3.3.3 Ensure iterator closes on stream termination
- [x] 2.3.3.4 Handle stream consumer errors

### 2.3.4 Unit Tests

- [x] **Task 2.3.4 Status** (Completed 2026-01-07)

- [x] 2.3.4.1 Test fold accumulates all entries in prefix
- [x] 2.3.4.2 Test fold_keys iterates keys only
- [x] 2.3.4.3 Test fold stop terminates early
- [x] 2.3.4.4 Test stream resources are properly cleaned up
- [x] 2.3.4.5 Benchmark fold vs manual iteration performance

---

## 2.4 Leapfrog TrieIterator Integration

- [x] **Section 2.4 Status** (Completed 2026-01-08)

Update Leapfrog TrieIterator to use erlang-rocksdb adapter.

### 2.4.1 TrieIterator Module Updates

- [x] **Task 2.4.1 Status** (Completed 2026-01-08)

Modify TrieIterator to use new adapter API.

- [x] 2.4.1.1 Update `TrieIterator.new/4` to use erlang adapter (type specs only)
- [x] 2.4.1.2 Update `TrieIterator.next/1` to use new iterator API
- [x] 2.4.1.3 Update `TrieIterator.seek/2` to use new seek API
- [x] 2.4.1.4 Update `TrieIterator.close/1` to use new close API
- [x] 2.4.1.5 Verify all 141 Leapfrog tests still pass

### 2.4.2 Index Module Updates

- [x] **Task 2.4.2 Status** (Completed 2026-01-08)

Update Index module for iterator changes.

- [x] 2.4.2.1 Update index scan operations to use new prefix iterator
- [x] 2.4.2.2 Update range queries to use fold operations where beneficial
- [x] 2.4.2.3 Verify pattern matching still works correctly
- [x] 2.4.2.4 Test index lookup performance

### 2.4.3 Unit Tests

- [x] **Task 2.4.3 Status** (Completed 2026-01-08)

- [x] 2.4.3.1 Test TrieIterator basic operations
- [x] 2.4.3.2 Test TrieIterator seek functionality
- [x] 2.4.3.3 Test TrieIterator multi-level iteration
- [x] 2.4.3.4 Test all Leapfrog integration tests pass
- [x] 2.4.3.5 Verify no regression in query performance

---

## 2.5 Integration Tests

- [ ] **Section 2.5 Status** (Pending)

End-to-end integration tests for iterator and snapshot migration.

### 2.5.1 Iterator Integration Tests

- [ ] **Task 2.5.1 Status** (Pending)

Test iterator functionality matches Rust NIF exactly.

- [ ] 2.5.1.1 Test prefix iterator returns same results as Rust NIF
- [ ] 2.5.1.2 Test iterator seek behaves identically to Rust NIF
- [ ] 2.5.1.3 Test iterator handles empty results correctly
- [ ] 2.5.1.4 Test iterator respects prefix boundaries
- [ ] 2.5.1.5 Test iterator closes cleanly under all conditions

### 2.5.2 Snapshot Integration Tests

- [ ] **Task 2.5.2 Status** (Pending)

Test snapshot functionality for consistency.

- [ ] 2.5.2.1 Test snapshot provides consistent read across writes
- [ ] 2.5.2.2 Test snapshot iterator sees historical data
- [ ] 2.5.2.3 Test multiple snapshots see different time points
- [ ] 2.5.2.4 Test snapshot release allows space reclamation
- [ ] 2.5.2.5 Test snapshot with concurrent modifications

### 2.5.3 Query Execution Tests

- [ ] **Task 2.5.3 Status** (Pending)

Test SPARQL queries work with new iterators.

- [ ] 2.5.3.1 Test simple pattern matching queries
- [ ] 2.5.3.2 Test complex join queries use iterators correctly
- [ ] 2.5.3.3 Test range queries with prefix scans
- [ ] 2.5.3.4 Test Leapfrog Triejoin with new iterators
- [ ] 2.5.3.5 Verify all SPARQL query tests pass

### 2.5.4 Performance Validation Tests

- [ ] **Task 2.5.4 Status** (Pending)

Validate iteration performance meets targets.

- [ ] 2.5.4.1 Test iterator throughput > 100K keys/sec
- [ ] 2.5.4.2 Test fold operations reduce context switching overhead
- [ ] 2.5.4.3 Test seek latency < 100us for in-cache data
- [ ] 2.5.4.4 Compare query performance to Rust NIF baseline

---

## Success Criteria

1. **Iterator Compatibility**: All iterator operations match Rust NIF behavior exactly
2. **Snapshot Consistency**: Snapshots provide point-in-time consistent reads
3. **Performance**: Fold operations reduce context switching overhead by >50%
4. **Test Coverage**: All Phase 2 tests pass (iterators, snapshots, queries)

## Provides Foundation

This phase enables:
- **Phase 3**: Advanced optimization using erlang-rocksdb-specific features
- **Operations**: Cleaner resource management with BEAM-native patterns

## References

- [TrieIterator Module](../../../lib/triple_store/sparql/leapfrog/trie_iterator.ex)
- [Index Module](../../../lib/triple_store/index.ex)
- [Snapshot Module](../../../lib/triple_store/snapshot.ex)
- [Erlang-RocksDB Fold Documentation](https://hexdocs.pm/rocksdb/api.html#fold-5)
