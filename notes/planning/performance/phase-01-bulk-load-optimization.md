# Phase 1: Bulk Load Optimization

## Overview

Phase 1 addresses the critical bulk load performance bottleneck. Current throughput is 42,000 triples/second, which is 58% below the target of 100,000 triples/second. This phase implements five key optimizations that together should exceed the target.

The primary bottleneck is the Dictionary Manager GenServer which serializes all term encoding operations. Secondary bottlenecks include small batch sizes, lack of pipeline parallelism, and suboptimal RocksDB write configuration.

By the end of this phase, bulk loading should achieve >100K triples/second through:
- Sharded dictionary manager for parallel term encoding
- Lock-free ETS cache for read-heavy workloads
- Flow-based parallel pipeline for overlapped I/O
- Optimized RocksDB write options
- Properly sized write buffers

---

## 1.1 Dictionary Manager Parallelization

- [x] **Section 1.1 Analysis Complete** (2025-12-31)

The Dictionary Manager (`lib/triple_store/dictionary/manager.ex`) is the primary bottleneck in bulk loading. All `get_or_create_id` operations serialize through a single GenServer, limiting throughput to approximately 10K operations/second regardless of available CPU cores.

Analysis shows that 100K triples/second requires 300K term encoding operations/second (3 terms per triple). The current single-GenServer architecture cannot meet this demand.

Two complementary solutions are proposed:
1. **Sharded Manager**: Partition dictionary operations across N GenServers by term hash
2. **Lock-Free Read Cache**: ETS table for read-only lookups, bypassing GenServer entirely

### 1.1.1 Sharded Manager Design

- [x] **Task 1.1.1 Complete** (2025-12-31)

Design a sharded dictionary manager that distributes load across multiple GenServers. Each shard handles a partition of the term space based on consistent hashing.

- [x] 1.1.1.1 Analyze current GenServer serialization in `manager.ex:187-198`
- [x] 1.1.1.2 Identify hot path: `handle_call({:get_or_create_ids, terms}, ...)`
- [x] 1.1.1.3 Design sharding strategy using `:erlang.phash2(term, shard_count)`
- [x] 1.1.1.4 Plan shard count configuration (default: `System.schedulers_online()`)
- [x] 1.1.1.5 Create `TripleStore.Dictionary.ShardedManager` module
- [x] 1.1.1.6 Implement `start_link/1` spawning N child GenServers
- [x] 1.1.1.7 Implement `get_or_create_id/2` with shard routing
- [x] 1.1.1.8 Implement `get_or_create_ids/2` with batch partitioning by shard
- [x] 1.1.1.9 Add configuration option `:dictionary_shards` to store options
- [x] 1.1.1.10 Update `TripleStore.open/2` to use ShardedManager when configured

### 1.1.2 Lock-Free Read Cache

- [x] **Task 1.1.2 Complete** (2026-01-01)

Implement an ETS-based read cache that allows concurrent term lookups without GenServer involvement. This dramatically improves performance for repeated terms during bulk loading.

- [x] 1.1.2.1 Design ETS table structure: `{term_binary, id}` with `:set` type
- [x] 1.1.2.2 Plan read concurrency via `{:read_concurrency, true}` option
- [x] 1.1.2.3 Design cache population strategy (write-through on create)
- [x] 1.1.2.4 Create `:dictionary_cache` ETS table in Manager init
- [x] 1.1.2.5 Modify `get_or_create_id/2` to check ETS before GenServer call
- [x] 1.1.2.6 Modify `get_or_create_id/2` to populate cache after creation
- [x] 1.1.2.7 Add cache size limit with LRU eviction (deferred - memory bounded by unique terms)
- [x] 1.1.2.8 Add telemetry for cache hit rate monitoring
- [x] 1.1.2.9 Update ShardedManager to share cache across shards

### 1.1.3 Batch Sequence Allocation

- [x] **Task 1.1.3 Complete** (2026-01-01)

Pre-allocate ID ranges per batch to reduce contention on the sequence counter. Instead of allocating one ID at a time, each shard requests a block of IDs.

- [x] 1.1.3.1 Analyze current sequence counter in `manager.ex:134-137`
- [x] 1.1.3.2 Design block allocation strategy (block size: 1000 IDs)
- [x] 1.1.3.3 Add `allocate_range/3` to SequenceCounter for atomic range allocation
- [x] 1.1.3.4 Update Manager batch processing to use range allocation per type
- [x] 1.1.3.5 Handle sequence exhaustion with rollback on overflow
- [x] 1.1.3.6 Crash recovery via existing safety margin mechanism

### 1.1.4 Unit Tests

- [x] **Task 1.1.4 Complete** (2026-01-01)

Comprehensive test coverage for dictionary parallelization features.

- [x] 1.1.4.1 Test sharded manager distributes terms across shards evenly
- [x] 1.1.4.2 Test concurrent `get_or_create_id` from multiple processes
- [x] 1.1.4.3 Test ETS cache hit returns correct ID
- [x] 1.1.4.4 Test ETS cache miss falls through to GenServer
- [x] 1.1.4.5 Test cache population on term creation
- [x] 1.1.4.6 Test batch operations partition correctly by shard
- [x] 1.1.4.7 Test sequence block allocation under contention
- [x] 1.1.4.8 Test dictionary consistency after parallel operations

---

## 1.2 Batch Size Optimization

- [x] **Section 1.2 Analysis Complete** (2025-12-31)

The current default batch size of 1,000 triples (`loader.ex:89`) creates excessive NIF round-trips. Each batch requires a separate RocksDB WriteBatch commit, introducing overhead for synchronization and syscalls.

Increasing batch size to 10,000-50,000 triples amortizes this overhead while remaining within reasonable memory bounds (~2-10 MB per batch).

### 1.2.1 Default Batch Size Increase

- [x] **Task 1.2.1 Analysis Complete** (2025-12-31)

Increase the default batch size and add configuration options for different use cases.

- [x] 1.2.1.1 Analyze current `@default_batch_size 1000` in `loader.ex:89`
- [x] 1.2.1.2 Calculate memory usage: 10K triples × 3 indices × 24 bytes = 720 KB
- [x] 1.2.1.3 Benchmark different batch sizes (1K, 5K, 10K, 50K)
- [ ] 1.2.1.4 Change `@default_batch_size` to `10_000`
- [ ] 1.2.1.5 Add `:batch_size` option to `TripleStore.load/3`
- [ ] 1.2.1.6 Document batch size recommendations for different scenarios
- [ ] 1.2.1.7 Add validation for minimum/maximum batch size bounds

### 1.2.2 Dynamic Batch Sizing

- [x] **Task 1.2.2 Analysis Complete** (2025-12-31)

Implement dynamic batch size selection based on available memory and dataset characteristics.

- [x] 1.2.2.1 Design memory budget calculation
- [ ] 1.2.2.2 Implement `optimal_batch_size/1` function
- [ ] 1.2.2.3 Add `:memory_budget` option (:low, :medium, :high, :auto)
- [ ] 1.2.2.4 Implement system memory detection via `:memsup` or `/proc/meminfo`
- [ ] 1.2.2.5 Calculate batch size from memory budget and triple size estimate

### 1.2.3 Unit Tests

- [ ] **Task 1.2.3 Complete**

- [ ] 1.2.3.1 Test default batch size is 10K
- [ ] 1.2.3.2 Test batch size option is respected
- [ ] 1.2.3.3 Test dynamic sizing selects appropriate values
- [ ] 1.2.3.4 Test boundary conditions (empty input, single triple)
- [ ] 1.2.3.5 Test memory budget options

---

## 1.3 Parallel Pipeline Loading

- [x] **Section 1.3 Analysis Complete** (2025-12-31)

Current loading is strictly sequential: each batch must complete dictionary encoding and index writing before the next batch begins. This wastes CPU cycles during I/O waits and vice versa.

A Flow-based pipeline architecture overlaps computation and I/O:
- Stage 1: Chunking (sequential, trivial)
- Stage 2: Dictionary encoding (parallel, CPU-bound)
- Stage 3: Index writing (sequential for atomicity, I/O-bound)

### 1.3.1 Flow Pipeline Design

- [x] **Task 1.3.1 Analysis Complete** (2025-12-31)

Design the multi-stage pipeline using Elixir Flow for parallel processing.

- [x] 1.3.1.1 Analyze current sequential processing in `loader.ex:469-493`
- [x] 1.3.1.2 Identify parallelizable stages (dictionary encoding)
- [x] 1.3.1.3 Design Flow pipeline topology
- [ ] 1.3.1.4 Add `flow` dependency if not present
- [ ] 1.3.1.5 Implement `load_triples_parallel/4` function
- [ ] 1.3.1.6 Configure stage count based on CPU cores
- [ ] 1.3.1.7 Implement backpressure via `max_demand` tuning

### 1.3.2 Encoding Stage Implementation

- [x] **Task 1.3.2 Analysis Complete** (2025-12-31)

Implement the parallel dictionary encoding stage.

- [x] 1.3.2.1 Design encoding worker function
- [ ] 1.3.2.2 Implement batch encoding in Flow.map
- [ ] 1.3.2.3 Handle encoding errors with proper propagation
- [ ] 1.3.2.4 Add telemetry for encoding throughput
- [ ] 1.3.2.5 Implement graceful shutdown on encoding failure

### 1.3.3 Writing Stage Implementation

- [x] **Task 1.3.3 Analysis Complete** (2025-12-31)

Implement the sequential index writing stage.

- [x] 1.3.3.1 Design atomic batch write function
- [ ] 1.3.3.2 Implement Flow.partition for single-writer semantics
- [ ] 1.3.3.3 Implement Flow.reduce for accumulating write count
- [ ] 1.3.3.4 Handle write errors with proper rollback
- [ ] 1.3.3.5 Add telemetry for write throughput

### 1.3.4 Progress Reporting

- [ ] **Task 1.3.4 Complete**

Implement progress callbacks for long-running bulk loads.

- [ ] 1.3.4.1 Add `:progress_callback` option to load function
- [ ] 1.3.4.2 Implement periodic progress updates (every N batches)
- [ ] 1.3.4.3 Include triples loaded, elapsed time, estimated remaining
- [ ] 1.3.4.4 Support cancellation via progress callback return value

### 1.3.5 Unit Tests

- [ ] **Task 1.3.5 Complete**

- [ ] 1.3.5.1 Test parallel loading produces correct results
- [ ] 1.3.5.2 Test stage count configuration
- [ ] 1.3.5.3 Test error handling in encoding stage
- [ ] 1.3.5.4 Test error handling in writing stage
- [ ] 1.3.5.5 Test progress callbacks are invoked
- [ ] 1.3.5.6 Test cancellation via callback

---

## 1.4 RocksDB Write Options

- [x] **Section 1.4 Analysis Complete** (2025-12-31)

The Rust NIF uses default RocksDB WriteOptions which fsync after every write. For bulk loading, this is unnecessarily conservative. Setting `sync=false` defers fsync to the OS, significantly improving throughput while maintaining durability via WAL.

### 1.4.1 Write Options Parameter

- [x] **Task 1.4.1 Analysis Complete** (2025-12-31)

Add sync control parameter to NIF write functions.

- [x] 1.4.1.1 Analyze current `write_batch` in `lib.rs:481-569`
- [x] 1.4.1.2 Design sync parameter interface
- [ ] 1.4.1.3 Modify `write_batch` NIF to accept sync option
- [ ] 1.4.1.4 Create `WriteOptions::default()` with `set_sync(sync)`
- [ ] 1.4.1.5 Update Elixir NIF wrapper in `nif.ex`
- [ ] 1.4.1.6 Add `:sync` option to `Index.insert_triples/3`

### 1.4.2 Bulk Load Mode

- [x] **Task 1.4.2 Analysis Complete** (2025-12-31)

Implement bulk load mode with optimized write settings.

- [x] 1.4.2.1 Design bulk load configuration preset
- [ ] 1.4.2.2 Add `:bulk_mode` option to loader
- [ ] 1.4.2.3 When bulk_mode: `sync=false`, larger batches
- [ ] 1.4.2.4 Implement final sync after bulk load complete
- [ ] 1.4.2.5 Document bulk mode trade-offs (durability vs speed)

### 1.4.3 Unit Tests

- [ ] **Task 1.4.3 Complete**

- [ ] 1.4.3.1 Test write_batch with sync=true (default behavior)
- [ ] 1.4.3.2 Test write_batch with sync=false
- [ ] 1.4.3.3 Test bulk mode uses correct options
- [ ] 1.4.3.4 Test final sync is called after bulk load

---

## 1.5 Write Buffer Configuration

- [x] **Section 1.5 Analysis Complete** (2025-12-31)

Current write buffer sizing is optimized for general use, not bulk loading. Larger write buffers reduce compaction frequency during ingestion, improving throughput.

### 1.5.1 Bulk Load Preset

- [x] **Task 1.5.1 Analysis Complete** (2025-12-31)

Add a RocksDB configuration preset optimized for bulk loading.

- [x] 1.5.1.1 Analyze current presets in `config/rocksdb.ex:91-123`
- [x] 1.5.1.2 Design bulk_load preset parameters
- [ ] 1.5.1.3 Add `bulk_load` preset to `@presets` map
- [ ] 1.5.1.4 Configure larger write_buffer_size (512 MB)
- [ ] 1.5.1.5 Configure max_write_buffer_number (6)
- [ ] 1.5.1.6 Raise level0 compaction triggers
- [ ] 1.5.1.7 Document memory requirements for bulk_load preset

### 1.5.2 Dynamic Configuration

- [x] **Task 1.5.2 Analysis Complete** (2025-12-31)

Allow configuration changes during bulk load operations.

- [x] 1.5.2.1 Research RocksDB SetOptions for runtime config
- [ ] 1.5.2.2 Implement `set_options/2` NIF if supported
- [ ] 1.5.2.3 Add `prepare_for_bulk_load/1` function
- [ ] 1.5.2.4 Add `restore_normal_config/1` function
- [ ] 1.5.2.5 Handle configuration restoration on error

### 1.5.3 Unit Tests

- [ ] **Task 1.5.3 Complete**

- [ ] 1.5.3.1 Test bulk_load preset applies correct settings
- [ ] 1.5.3.2 Test memory usage with bulk_load preset
- [ ] 1.5.3.3 Test configuration switch during operation
- [ ] 1.5.3.4 Test restoration on error

---

## 1.6 Integration Tests

- [ ] **Section 1.6 Complete**

End-to-end integration tests verifying bulk load pipeline functionality.

### 1.6.1 Load Pipeline Tests

- [ ] **Task 1.6.1 Complete**

Test complete bulk load operations with various configurations.

- [ ] 1.6.1.1 Test parallel loading with 100K synthetic triples
- [ ] 1.6.1.2 Test parallel loading with LUBM dataset
- [ ] 1.6.1.3 Test parallel loading with BSBM dataset
- [ ] 1.6.1.4 Test error handling and recovery
- [ ] 1.6.1.5 Test memory usage stays bounded
- [ ] 1.6.1.6 Test CPU utilization across cores

### 1.6.2 Consistency Tests

- [ ] **Task 1.6.2 Complete**

Verify data integrity after parallel bulk loading.

- [ ] 1.6.2.1 Test dictionary consistency (no duplicate IDs)
- [ ] 1.6.2.2 Test dictionary bidirectionality (encode/decode roundtrip)
- [ ] 1.6.2.3 Test all three indices contain same triples
- [ ] 1.6.2.4 Test queries return correct results after bulk load
- [ ] 1.6.2.5 Test persistence survives restart

### 1.6.3 Performance Validation

- [ ] **Task 1.6.3 Complete**

Validate performance improvements meet targets.

- [ ] 1.6.3.1 Test throughput exceeds 80K tps (conservative)
- [ ] 1.6.3.2 Test throughput scales with CPU cores
- [ ] 1.6.3.3 Test latency distribution is reasonable
- [ ] 1.6.3.4 Compare with baseline measurements

---

## Success Criteria

1. **Throughput**: Bulk load exceeds 100,000 triples/second on reference hardware
2. **Parallelism**: Dictionary encoding utilizes available CPU cores
3. **Memory**: Memory usage remains bounded during large imports
4. **Consistency**: All data integrity tests pass after parallel loading
5. **Reliability**: Error handling properly rolls back partial operations

## Provides Foundation

This phase enables:
- **Phase 2**: Efficient benchmark dataset loading for query testing
- **Phase 3**: Statistics collection on larger datasets
- **Phase 4**: Stress testing storage layer configurations

## References

- [Performance Improvement Analysis](../../reviews/benchmark-performance-improvements.md#part-1-bulk-load-optimization-critical)
- [Dictionary Manager](../../../lib/triple_store/dictionary/manager.ex)
- [Loader Module](../../../lib/triple_store/loader.ex)
- [RocksDB NIF](../../../native/rocksdb_nif/src/lib.rs)
