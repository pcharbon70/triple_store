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

- [ ] **Section 2.1 Status** (Pending)

Migrate iterator functionality from Rust NIF to erlang-rocksdb.

### 2.1.1 Basic Iterator Creation

- [ ] **Task 2.1.1 Status** (Pending)

Implement iterator creation matching current NIF API.

- [ ] 2.1.1.1 Implement `iterator/3` for column-family-specific iterator creation
- [ ] 2.1.1.2 Support iterator options: `{fill_cache, boolean()}`, `{snapshot, snapshot_ref}`
- [ ] 2.1.1.3 Handle iterator resource lifecycle (creation, movement, closure)
- [ ] 2.1.1.4 Map `iterator_move/2` operations: `next`, `prev`, `first`, `last`, `{seek, key}`
- [ ] 2.1.1.5 Implement `iterator_close/1` for resource cleanup

### 2.1.2 Prefix Iterator Migration

- [ ] **Task 2.1.2 Status** (Pending)

Migrate prefix iterator functionality for index scanning.

- [ ] 2.1.2.1 Implement `prefix_iterator/4` with prefix bounds checking
- [ ] 2.1.2.2 Configure read options: `total_order_seek`, `prefix_same_as_start`
- [ ] 2.1.2.3 Handle short prefix (< 8 bytes) vs long prefix (> 8 bytes) logic
- [ ] 2.1.2.4 Map prefix iterator seek functionality
- [ ] 2.1.2.5 Maintain prefix boundary checking (safety net for RocksDB behavior)

### 2.1.3 Seek Operations Migration

- [ ] **Task 2.1.3 Status** (Pending)

Migrate seek operations critical for Leapfrog Triejoin.

- [ ] 2.1.3.1 Implement `iterator_seek/3` for positioning at specific keys
- [ ] 2.1.3.2 Handle read options matching prefix extractor configuration
- [ ] 2.1.3.3 Test seek behavior with non-existent keys (should position at next)
- [ ] 2.1.3.4 Test seek past end of data (should return exhausted)
- [ ] 2.1.3.5 Validate Leapfrog operations work correctly

### 2.1.4 Iterator Collect Operation

- [ ] **Task 2.1.4 Status** (Pending)

Implement convenience function for collecting all iterator results.

- [ ] 2.1.4.1 Implement `iterator_collect/2` for collecting remaining entries
- [ ] 2.1.4.2 Handle prefix boundary in collect operation
- [ ] 2.1.4.3 Return results as list of `{key, value}` tuples
- [ ] 2.1.4.4 Handle exhausted iterator case

### 2.1.5 Unit Tests

- [ ] **Task 2.1.5 Status** (Pending)

- [ ] 2.1.5.1 Test basic iterator iteration matches current NIF
- [ ] 2.1.5.2 Test prefix iterator respects prefix boundaries
- [ ] 2.1.5.3 Test seek positions correctly for existing and non-existing keys
- [ ] 2.1.5.4 Test iterator move operations (next, prev, first, last)
- [ ] 2.1.5.5 Test iterator collect returns all entries within prefix
- [ ] 2.1.5.6 Test Leapfrog Triejoin operations (163 tests)

---

## 2.2 Snapshot Operations Migration

- [ ] **Section 2.2 Status** (Pending)

Migrate snapshot functionality for point-in-time consistent reads.

### 2.2.1 Snapshot Creation and Release

- [ ] **Task 2.2.1 Status** (Pending)

Implement snapshot lifecycle management.

- [ ] 2.2.1.1 Implement `snapshot/1` for creating point-in-time snapshots
- [ ] 2.2.1.2 Implement `release_snapshot/1` for cleanup
- [ ] 2.2.1.3 Track snapshot references for proper resource management
- [ ] 2.2.1.4 Handle snapshot from database reference
- [ ] 2.2.1.5 Return snapshot reference that can be used with read options

### 2.2.2 Snapshot Read Operations

- [ ] **Task 2.2.2 Status** (Pending)

Implement read operations from snapshots.

- [ ] 2.2.2.1 Implement `snapshot_get/3` for reading from snapshot
- [ ] 2.2.2.2 Configure read options with snapshot reference
- [ ] 2.2.2.3 Handle not_found case consistently with normal get
- [ ] 2.2.2.4 Verify snapshot provides point-in-time consistency

### 2.2.3 Snapshot Iterator Operations

- [ ] **Task 2.2.3 Status** (Pending)

Implement iterator creation from snapshots.

- [ ] 2.2.3.1 Implement `snapshot_prefix_iterator/4` for iterating over snapshot
- [ ] 2.2.3.2 Pass snapshot reference in iterator read options
- [ ] 2.2.3.3 Ensure iterator sees data as of snapshot time
- [ ] 2.2.3.4 Handle iterator lifecycle with snapshot reference

### 2.2.4 Unit Tests

- [ ] **Task 2.2.4 Status** (Pending)

- [ ] 2.2.4.1 Test snapshot creation captures current state
- [ ] 2.2.4.2 Test snapshot doesn't see subsequent writes
- [ ] 2.2.4.3 Test snapshot_get returns data from snapshot time
- [ ] 2.2.4.4 Test snapshot iterator sees historical data
- [ ] 2.2.4.5 Test multiple snapshots can coexist
- [ ] 2.2.4.6 Test snapshot release allows compaction of old data

---

## 2.3 Fold-Based Iteration Optimization

- [ ] **Section 2.3 Status** (Pending)

Optimize iteration using erlang-rocksdb's `fold` operations to reduce BEAM-NIF boundary crossings.

### 2.3.1 Fold Operations Implementation

- [ ] **Task 2.3.1 Status** (Pending)

Implement fold-based iteration for bulk operations.

- [ ] 2.3.1.1 Implement `fold/5` for folding over prefix range
- [ ] 2.3.1.2 Use `rocksdb:fold/4` with accumulator function
- [ ] 2.3.1.3 Handle fold function signature: `fun(Key, Value, Acc) -> {ok, NewAcc}`
- [ ] 2.3.1.4 Support `stop` return for early termination
- [ ] 2.3.1.5 Configure read options for prefix-based folding

### 2.3.2 Fold Keys Operation

- [ ] **Task 2.3.2 Status** (Pending)

Implement keys-only folding for index scanning.

- [ ] 2.3.2.1 Implement `fold_keys/5` for iterating keys without values
- [ ] 2.3.2.2 Use `rocksdb:fold_keys/4` for efficiency
- [ ] 2.3.2.3 Handle prefix boundary in fold_keys
- [ ] 2.3.2.4 Support accumulator and stop mechanisms

### 2.3.3 Stream Operations

- [ ] **Task 2.3.3 Status** (Pending)

Implement lazy stream operations for large result sets.

- [ ] 2.3.3.1 Create `prefix_stream/4` for Elixir Stream compatibility
- [ ] 2.3.3.2 Use Stream.resource for proper resource cleanup
- [ ] 2.3.3.3 Ensure iterator closes on stream termination
- [ ] 2.3.3.4 Handle stream consumer errors

### 2.3.4 Unit Tests

- [ ] **Task 2.3.4 Status** (Pending)

- [ ] 2.3.4.1 Test fold accumulates all entries in prefix
- [ ] 2.3.4.2 Test fold_keys iterates keys only
- [ ] 2.3.4.3 Test fold stop terminates early
- [ ] 2.3.4.4 Test stream resources are properly cleaned up
- [ ] 2.3.4.5 Benchmark fold vs manual iteration performance

---

## 2.4 Leapfrog TrieIterator Integration

- [ ] **Section 2.4 Status** (Pending)

Update Leapfrog TrieIterator to use erlang-rocksdb adapter.

### 2.4.1 TrieIterator Module Updates

- [ ] **Task 2.4.1 Status** (Pending)

Modify TrieIterator to use new adapter API.

- [ ] 2.4.1.1 Update `TrieIterator.new/4` to use erlang adapter
- [ ] 2.4.1.2 Update `TrieIterator.next/1` to use new iterator API
- [ ] 2.4.1.3 Update `TrieIterator.seek/2` to use new seek API
- [ ] 2.4.1.4 Update `TrieIterator.close/1` to use new close API
- [ ] 2.4.1.5 Verify all 163 Leapfrog tests still pass

### 2.4.2 Index Module Updates

- [ ] **Task 2.4.2 Status** (Pending)

Update Index module for iterator changes.

- [ ] 2.4.2.1 Update index scan operations to use new prefix iterator
- [ ] 2.4.2.2 Update range queries to use fold operations where beneficial
- [ ] 2.4.2.3 Verify pattern matching still works correctly
- [ ] 2.4.2.4 Test index lookup performance

### 2.4.3 Unit Tests

- [ ] **Task 2.4.3 Status** (Pending)

- [ ] 2.4.3.1 Test TrieIterator basic operations
- [ ] 2.4.3.2 Test TrieIterator seek functionality
- [ ] 2.4.3.3 Test TrieIterator multi-level iteration
- [ ] 2.4.3.4 Test all Leapfrog integration tests pass
- [ ] 2.4.3.5 Verify no regression in query performance

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
