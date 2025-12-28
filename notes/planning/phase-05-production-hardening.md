# Phase 5: Production Hardening

## Overview

Phase 5 prepares the triple store for production deployment through benchmarking, performance optimization, operational tooling, and API finalization. By the end of this phase, the system will be ready for embedding in production applications with comprehensive telemetry, backup/restore capabilities, and documented performance characteristics.

The focus is on measuring and optimizing real-world performance, providing operational visibility through Telemetry integration, and delivering a clean, well-documented public API.

---

## 5.1 Benchmarking Suite

- [x] **Section 5.1 Complete**

This section implements benchmarking infrastructure using standard RDF benchmarks (LUBM, BSBM) to measure and track performance.

### 5.1.1 Data Generators

- [x] **Task 5.1.1 Complete**

Implement data generators for standard benchmarks.

- [x] 5.1.1.1 Implement LUBM data generator with configurable scale
- [x] 5.1.1.2 Implement BSBM data generator with configurable scale
- [x] 5.1.1.3 Support deterministic generation for reproducible benchmarks
- [x] 5.1.1.4 Output as RDF.Graph for direct loading

### 5.1.2 Query Templates

- [x] **Task 5.1.2 Complete**

Implement standard benchmark query templates.

- [x] 5.1.2.1 Implement LUBM query set (14 queries with varied complexity)
- [x] 5.1.2.2 Implement BSBM query mix (12 queries simulating e-commerce)
- [x] 5.1.2.3 Support parameterized queries for varied inputs
- [x] 5.1.2.4 Include expected result counts for validation

### 5.1.3 Benchmark Runner

- [x] **Task 5.1.3 Complete**

Implement benchmark execution infrastructure.

- [x] 5.1.3.1 Implement `Benchmark.run(db, benchmark, opts)` entry point
- [x] 5.1.3.2 Support warmup iterations before measurement
- [x] 5.1.3.3 Collect latency percentiles (p50, p95, p99)
- [x] 5.1.3.4 Collect throughput metrics (queries/sec, triples/sec)
- [x] 5.1.3.5 Output results in structured format (JSON, CSV)

### 5.1.4 Performance Targets

- [x] **Task 5.1.4 Complete**

Define and validate performance targets.

- [x] 5.1.4.1 Simple BGP query: <10ms p95 on 1M triples
- [x] 5.1.4.2 Complex join query: <100ms p95 on 1M triples
- [x] 5.1.4.3 Bulk load: >100K triples/second
- [x] 5.1.4.4 BSBM overall: <50ms p95 for query mix

### 5.1.5 Unit Tests

- [x] **Task 5.1.5 Complete**

- [x] Test LUBM generator produces valid RDF
- [x] Test BSBM generator produces valid RDF
- [x] Test benchmark runner collects accurate metrics
- [x] Test percentile calculation is correct

---

## 5.2 RocksDB Tuning

- [x] **Section 5.2 Complete**

This section implements RocksDB configuration tuning for production workloads, with documentation of tuning rationale.

### 5.2.1 Memory Configuration

- [x] **Task 5.2.1 Complete**

Configure memory allocation for optimal performance.

- [x] 5.2.1.1 Size block cache based on available RAM (40% guideline)
- [x] 5.2.1.2 Configure write buffer size for write-heavy loads
- [x] 5.2.1.3 Set max_open_files based on system limits
- [x] 5.2.1.4 Document memory usage patterns and tuning

### 5.2.2 Compression Configuration

- [x] **Task 5.2.2 Complete**

Configure compression per column family.

- [x] 5.2.2.1 Use LZ4 for frequently accessed data (indices)
- [x] 5.2.2.2 Use Zstd for archival data (derived facts)
- [x] 5.2.2.3 Benchmark compression ratio vs speed tradeoffs
- [x] 5.2.2.4 Document compression settings

### 5.2.3 Compaction Configuration

- [x] **Task 5.2.3 Complete**

Configure compaction for balanced performance.

- [x] 5.2.3.1 Set level compaction with appropriate level sizes
- [x] 5.2.3.2 Configure rate limiting to bound I/O impact
- [x] 5.2.3.3 Schedule background compaction appropriately
- [x] 5.2.3.4 Monitor compaction lag metrics

### 5.2.4 Column Family Tuning

- [x] **Task 5.2.4 Complete**

Tune individual column families for their access patterns.

- [x] 5.2.4.1 Add bloom filters to dictionary column families
- [x] 5.2.4.2 Set prefix extractors for index column families
- [x] 5.2.4.3 Configure block sizes per access pattern
- [x] 5.2.4.4 Document per-CF tuning rationale

### 5.2.5 Unit Tests

- [x] **Task 5.2.5 Complete**

- [x] Test configuration loads without errors
- [x] Test bloom filters reduce negative lookups
- [x] Test compression achieves expected ratio
- [x] Test compaction completes without errors

---

## 5.3 Query Caching

- [x] **Section 5.3 Complete**

This section implements result caching for frequently executed queries, with intelligent invalidation on updates.

### 5.3.1 Result Cache

- [x] **Task 5.3.1 Complete**

Implement query result caching.

- [x] 5.3.1.1 Create `TripleStore.Query.Cache` GenServer with ETS backend
- [x] 5.3.1.2 Cache results keyed by query hash
- [x] 5.3.1.3 Limit cache size with configurable max entries
- [x] 5.3.1.4 Skip caching for large result sets

### 5.3.2 Cache Invalidation

- [x] **Task 5.3.2 Complete**

Implement intelligent cache invalidation.

- [x] 5.3.2.1 Track predicates accessed by each cached query
- [x] 5.3.2.2 Invalidate queries touching updated predicates
- [x] 5.3.2.3 Full invalidation fallback for complex updates
- [x] 5.3.2.4 Report cache hit/miss rates via telemetry

### 5.3.3 Cache Warming

- [x] **Task 5.3.3 Complete**

Support cache warming on startup.

- [x] 5.3.3.1 Persist cache to disk on shutdown
- [x] 5.3.3.2 Restore cache on startup (optional)
- [x] 5.3.3.3 Pre-execute common queries on startup

### 5.3.4 Unit Tests

- [x] **Task 5.3.4 Complete**

- [x] Test cache stores and retrieves results
- [x] Test cache evicts LRU entries at capacity
- [x] Test cache invalidates on update
- [x] Test cache hit rate reporting

### 5.3.5 Review Fixes

- [x] **Task 5.3.5 Complete** (Post-review hardening)

Security and performance fixes from comprehensive review:

- [x] 5.3.5.1 Safe deserialization with structure validation
- [x] 5.3.5.2 Path traversal prevention with `allowed_persistence_dir` option
- [x] 5.3.5.3 Memory-based cache limits with `max_memory_bytes` option
- [x] 5.3.5.4 Predicate reverse index for O(1) invalidation
- [x] 5.3.5.5 Graceful shutdown with automatic persist via `terminate/2`
- [x] 5.3.5.6 Query cache events registered in `TripleStore.Telemetry`
- [x] 5.3.5.7 Tests for security features (55 tests total)

---

## 5.4 Telemetry Integration

- [x] **Section 5.4 Complete**

This section implements comprehensive telemetry for operational visibility using Elixir's Telemetry library.

### 5.4.1 Event Definitions

- [x] **Task 5.4.1 Complete**

Define telemetry events for all major operations.

- [x] 5.4.1.1 Define `[:triple_store, :query, :start | :stop | :exception]`
- [x] 5.4.1.2 Define `[:triple_store, :insert, :start | :stop]`
- [x] 5.4.1.3 Define `[:triple_store, :reasoning, :start | :stop]`
- [x] 5.4.1.4 Define `[:triple_store, :cache, :hit | :miss]`

Note: All events were already implemented. Task 5.4.1 verified the implementation
and added 27 comprehensive tests for the telemetry API.

### 5.4.2 Metrics Collection

- [x] **Task 5.4.2 Complete**

Implement metrics collection and aggregation.

- [x] 5.4.2.1 Collect query duration histogram
- [x] 5.4.2.2 Collect insert/delete throughput
- [x] 5.4.2.3 Collect cache hit rate
- [x] 5.4.2.4 Collect reasoning iteration count

Note: Implemented `TripleStore.Metrics` GenServer that attaches to telemetry events
and provides aggregated statistics with 23 comprehensive tests.

### 5.4.3 Health Checks

- [x] **Task 5.4.3 Complete**

Implement health check endpoint.

- [x] 5.4.3.1 Implement `TripleStore.health(db)` returning health status
- [x] 5.4.3.2 Report triple count and index sizes
- [x] 5.4.3.3 Report compaction status and lag
- [x] 5.4.3.4 Report memory usage estimates

Note: Enhanced `TripleStore.Health` module with `get_index_sizes/1`, `estimate_memory/1`,
`get_compaction_status/0`, and integration with Metrics. Added 32 comprehensive tests.

### 5.4.4 Prometheus Integration

- [x] **Task 5.4.4 Complete**

Support Prometheus metrics export.

- [x] 5.4.4.1 Define Prometheus metric specifications
- [x] 5.4.4.2 Implement telemetry handlers updating Prometheus metrics
- [x] 5.4.4.3 Document Grafana dashboard setup
- [x] 5.4.4.4 Provide example alerting rules

Note: Implemented `TripleStore.Prometheus` GenServer with 19 metric definitions (counters,
histograms, gauges), telemetry event handlers, Prometheus text exposition format output,
and comprehensive documentation with Grafana dashboard JSON and 8 alerting rules.
Added 25 tests.

### 5.4.5 Unit Tests

- [x] **Task 5.4.5 Complete**

- [x] Test telemetry events emitted for queries
- [x] Test telemetry events emitted for updates
- [x] Test health check returns accurate status
- [x] Test Prometheus metrics update correctly

Note: All telemetry-related tests are covered across Tasks 5.4.1-5.4.4:
- Task 5.4.1: 27 telemetry API tests
- Task 5.4.2: 23 metrics collection tests
- Task 5.4.3: 32 health check tests
- Task 5.4.4: 25 Prometheus tests
Total: 107 tests for Section 5.4

### Section 5.4 Review Fixes

- [x] **Review Fixes Complete** (2025-12-28)

Applied fixes from comprehensive review (`notes/reviews/section-5.4-telemetry-integration-review.md`):

**Blockers Fixed:**
1. SPARQL query sanitization (security) - Added `sanitize_query/2` to prevent PII exposure
2. Exception sanitization (security) - Added `sanitize_exception/2` to prevent stacktrace exposure
3. Shared handler attachment - Extracted common code to `attach_metrics_handlers/2`
4. Shared duration extraction - Added `duration_ms/1` and `duration_seconds/1`
5. SPARQL telemetry test fixes - Corrected event names and assertions

**Concerns Addressed:**
- Handler ID collision prevention via state tracking
- Prometheus label value escaping for security
- Security documentation for metrics endpoint
- Compaction status limitation documentation

See `notes/summaries/section-5.4-review-fixes.md` for full details.

---

## 5.5 Backup and Restore

- [x] **Section 5.5 Complete**

This section implements backup and restore functionality using RocksDB checkpoints.

### 5.5.1 Checkpoint Backup

- [x] **Task 5.5.1 Complete**

Implement backup via RocksDB checkpoints.

- [x] 5.5.1.1 Implement `TripleStore.backup(db, path)` creating checkpoint
- [x] 5.5.1.2 Support incremental backups if possible
- [x] 5.5.1.3 Validate checkpoint integrity after creation
- [x] 5.5.1.4 Return backup metadata (size, timestamp)

### 5.5.2 Restore

- [x] **Task 5.5.2 Complete**

Implement restore from backup.

- [x] 5.5.2.1 Implement `TripleStore.restore(backup_path, target_path)`
- [x] 5.5.2.2 Validate backup before restore
- [x] 5.5.2.3 Support restore to different location
- [x] 5.5.2.4 Handle atomics counter restoration

Note: Task 5.5.2.4 implemented sequence counter export/import for backup/restore.
- Added `SequenceCounter.export/import_values` and file-based persistence
- Backup creates `.counter_state` file with counter values
- Restore applies counters with safety margin to prevent ID collisions
- Backward compatible with legacy backups (no counter file)
See `notes/summaries/task-5.5.2.4-atomics-counter-restoration.md` for details.

### 5.5.3 Scheduled Backups

- [ ] **Task 5.5.3 Complete** (Partial - rotation implemented, scheduling deferred)

Support scheduled periodic backups.

- [ ] 5.5.3.1 Implement `TripleStore.schedule_backup(db, interval, path)` (deferred)
- [x] 5.5.3.2 Implement backup rotation (keep N most recent)
- [x] 5.5.3.3 Report backup status via telemetry
- [x] 5.5.3.4 Handle backup failures gracefully

### 5.5.4 Unit Tests

- [x] **Task 5.5.4 Complete**

- [x] Test backup creates valid checkpoint
- [x] Test restore produces identical database
- [ ] Test scheduled backup runs at interval (deferred with 5.5.3.1)
- [x] Test backup rotation deletes old backups

### Section 5.5 Review Fixes

- [x] **Review Fixes Complete** (2025-12-28)

Applied fixes from comprehensive review (`notes/reviews/section-5.5-backup-restore-review.md`):

**Blockers Fixed:**
1. Safe deserialization - Added `safe_binary_to_term/1` with `:safe` option
2. Path traversal protection - Added `validate_path_safety/1` rejecting `..` paths
3. Backup telemetry events - Added `backup_events/0` to `TripleStore.Telemetry`

**Concerns Addressed:**
- C4: Documented race condition in restore counter restoration
- C5: Added mtime comparison to incremental backup (size + mtime)
- C6: Added symlink protection with `check_for_symlinks/1`
- C7: Fixed doc example (import vs import_values)
- C8: Replaced bare rescue with specific exception handling

**Suggested Improvements Implemented:**
- S7: Added backup Prometheus metrics (counters, histograms, gauges)
- S8: Documented incremental backup limitations and security features

**New Security Tests:**
- Path traversal rejection for backup and restore paths
- Symlink detection in backup source
- Empty database backup and restore

See `notes/summaries/section-5.5-review-fixes.md` for full details.

---

## 5.6 Public API Finalization

- [x] **Section 5.6 Complete**

This section finalizes the public API with clean documentation and usage examples.

### 5.6.1 API Design

- [x] **Task 5.6.1 Complete** (2025-12-28)

Finalize the public module interface.

- [x] 5.6.1.1 Define `TripleStore.open/2` and `TripleStore.close/1` (already existed)
- [x] 5.6.1.2 Define `TripleStore.load/2` and `TripleStore.export/2` (export/2 added)
- [x] 5.6.1.3 Define `TripleStore.insert/2` and `TripleStore.delete/2` (added)
- [x] 5.6.1.4 Define `TripleStore.query/2` and `TripleStore.update/2` (already existed)
- [x] 5.6.1.5 Define `TripleStore.materialize/2` and `TripleStore.reasoning_status/1` (reasoning_status/1 added)
- [x] 5.6.1.6 Define `TripleStore.backup/2` and `TripleStore.restore/2` (already existed)
- [x] 5.6.1.7 Define `TripleStore.stats/1` and `TripleStore.health/1` (already existed)

Note: Added `insert/2`, `delete/2`, `export/2`, and `reasoning_status/1` to complete the public API.
Also added `Loader.insert/3` and `Loader.delete/3` for underlying implementation.
See `notes/summaries/task-5.6.1-api-design.md` for details.

### 5.6.2 Documentation

- [x] **Task 5.6.2 Complete** (2025-12-28)

Write comprehensive documentation.

- [x] 5.6.2.1 Write module documentation with overview (enhanced @moduledoc)
- [x] 5.6.2.2 Document all public functions with @doc (all 16 functions documented)
- [x] 5.6.2.3 Add @spec for all public functions (verified complete)
- [x] 5.6.2.4 Include usage examples in documentation (examples in all functions)
- [x] 5.6.2.5 Write Getting Started guide (`guides/getting_started.md`)
- [x] 5.6.2.6 Write Performance Tuning guide (`guides/performance_tuning.md`)

Note: Enhanced @moduledoc with complete API reference, architecture details, and feature summaries.
Added two comprehensive guides for users. See `notes/summaries/task-5.6.2-documentation.md` for details.

### 5.6.3 Error Handling

- [x] **Task 5.6.3 Complete** (2025-12-28)

Implement consistent error handling.

- [x] 5.6.3.1 Define error types with `TripleStore.Error` (already existed with codes and categories)
- [x] 5.6.3.2 Return `{:ok, result}` or `{:error, error}` consistently (verified - all functions consistent)
- [x] 5.6.3.3 Provide `!` variants that raise on error (added 13 bang variants)
- [x] 5.6.3.4 Include helpful error messages (safe_message/detailed message system exists)

Note: Added bang variants for all public functions (`open!/2`, `query!/3`, `insert!/2`, etc.).
Added `error_for/3` helper to convert raw errors to structured TripleStore.Error.
See `notes/summaries/task-5.6.3-error-handling.md` for details.

### 5.6.4 Unit Tests

- [x] **Task 5.6.4 Complete**

- [x] Test all public functions have documentation
- [x] Test all public functions have specs
- [x] Test error handling returns correct types
- [x] Test bang functions raise appropriate errors

See `notes/summaries/task-5.6.4-unit-tests.md` for details.

### Section 5.6 Review Fixes

- [x] **Review Fixes Complete** (2025-12-28)

Applied fixes from comprehensive review (`notes/reviews/section-5.6-public-api-review.md`):

**Blockers Fixed:**
1. Path traversal protection - Added `validate_path/1` to `open/2`
2. Path traversal protection - Added `validate_file_path/1` to `export_file/4`
3. Fixed `materialize/2` - Now loads facts from database via `load_facts_from_db/1`
4. Atom table exhaustion - Changed `path_to_status_key/1` to use binary keys

**Concerns Addressed:**
- C1: Fully implemented `create_if_missing` option
- C2: Fixed bang variant error categorization with `has_natural_category?/1`
- C3: Added 11 new tests (path traversal, close behavior, load_graph!, load_string!)
- C4: Simplified `reasoning_status!/1` and `health!/1` (removed unreachable code)
- C5: Added `load_graph!/3`, `load_string!/4`, and `close!/1` bang variants
- C6: Sanitized paths in backup telemetry with `Path.basename/1`
- C7: Consolidated error conversion in `Error.from_reason/2`

**Suggested Improvements Implemented:**
- S1: Created `unwrap_or_raise!/3` helper for DRY bang variants
- S6: Added `close!/1` for API completeness

See `notes/summaries/section-5.6-review-fixes.md` for full details.

---

## 5.7 Phase 5 Integration Tests

- [ ] **Section 5.7 Complete**

Integration tests validate the complete production-ready system.

### 5.7.1 Full System Testing

- [x] **Task 5.7.1 Complete** (2025-12-28)

Test complete system under realistic workloads.

- [x] 5.7.1.1 Test load -> query -> update -> query cycle
- [x] 5.7.1.2 Test concurrent read/write workload
- [x] 5.7.1.3 Test system under memory pressure
- [x] 5.7.1.4 Test recovery after crash simulation

Implemented 17 integration tests in `test/triple_store/full_system_integration_test.exs`:
- CRUD cycle tests with Turtle data and RDF.Graph
- Concurrent read/write workload with 20+ concurrent tasks
- Memory pressure tests with 1000+ triples
- Recovery tests for persistence, backup/restore, and close handling

See `notes/summaries/task-5.7.1-full-system-integration.md` for details.

### 5.7.2 Benchmark Validation

- [ ] **Task 5.7.2 Complete**

Validate performance targets are met.

- [ ] 5.7.2.1 Run LUBM benchmark, verify targets met
- [ ] 5.7.2.2 Run BSBM benchmark, verify targets met
- [ ] 5.7.2.3 Profile and identify remaining bottlenecks
- [ ] 5.7.2.4 Document achieved performance characteristics

### 5.7.3 Operational Testing

- [ ] **Task 5.7.3 Complete**

Test operational features.

- [ ] 5.7.3.1 Test backup/restore cycle preserves all data
- [ ] 5.7.3.2 Test telemetry integration with Prometheus
- [ ] 5.7.3.3 Test health check under various conditions
- [ ] 5.7.3.4 Test graceful shutdown and restart

### 5.7.4 API Testing

- [ ] **Task 5.7.4 Complete**

Test public API completeness and usability.

- [ ] 5.7.4.1 Test all documented examples work correctly
- [ ] 5.7.4.2 Test error messages are helpful
- [ ] 5.7.4.3 Test API documentation is accurate
- [ ] 5.7.4.4 Review API for consistency and usability

---

## Success Criteria

1. **Performance**: All benchmark targets met and documented
2. **Telemetry**: Comprehensive metrics available via Telemetry/Prometheus
3. **Operations**: Backup/restore works correctly
4. **Stability**: System stable under sustained load
5. **Documentation**: Complete API documentation with examples
6. **API**: Clean, consistent public interface

## Key Outputs

- `TripleStore.Benchmark` - Benchmark suite (LUBM, BSBM)
- `TripleStore.Config.RocksDB` - Documented tuning options
- `TripleStore.Query.Cache` - Query result caching
- `TripleStore.Telemetry` - Telemetry integration
- `TripleStore.HealthCheck` - Health monitoring
- `TripleStore.Backup` - Backup/restore operations
- `TripleStore` - Final public API module
- Documentation and guides
