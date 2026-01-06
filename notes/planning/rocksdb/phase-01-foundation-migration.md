# Phase 1: Foundation Migration - Erlang-RocksDB Backend

## Overview

This phase migrates the TripleStore from a custom Rust NIF implementation to the Erlang-RocksDB C++ NIF library. The migration maintains 100% data compatibility while replacing the native storage layer with a more mature, community-maintained implementation.

**Key Migration Goals:**
- Replace Rust NIF (`native/rocksdb_nif/`) with Erlang-RocksDB (`:rocksdb` Hex package)
- Maintain all existing functionality: column families, snapshots, iterators, batches
- Preserve binary encoding formats for zero-data-migration compatibility
- Improve iteration performance using `rocksdb:fold` instead of manual next/move
- Eliminate Rust toolchain dependency from build process

**Migration Strategy:**
- Implement a compatibility adapter layer that translates current NIF calls to erlang-rocksdb
- Phase 1 focuses on basic operations (open, get, put, delete, write_batch)
- Phase 2 will handle advanced features (iterators, snapshots, prefix operations)
- Phase 3 will optimize for erlang-rocksdb-specific features (fold operations)

**Critical Files:**
- `native/rocksdb_nif/src/lib.rs` - Current Rust NIF (to be removed)
- `lib/triple_store/backend/rocksdb/nif.ex` - Current NIF wrapper (to be replaced)
- `mix.exs` - Dependencies (remove rustler, add :rocksdb)
- `lib/triple_store/backend/rocksdb/` - New adapter module location

---

## 1.1 Dependency Management

- [x] **Section 1.1 Status** (Completed 2026-01-06)

Remove Rust toolchain dependencies and add erlang-rocksdb package.

### 1.1.1 Remove Rust Dependencies

- [x] **Task 1.1.1 Status** (Completed)

Remove rustler and Rust compilation configuration from the project.

- [x] 1.1.1.1 Remove `{:rustler, "~> 1.0"}` from dependencies in `mix.exs`
- [x] 1.1.1.2 Remove `compilers: [:rustler] ++ Mix.compilers()` from `mix.exs`
- [x] 1.1.1.3 Remove `rustler` configuration block specifying `native/rocksdb_nif` crate
- [x] 1.1.1.4 Remove `:rustler` from project dependencies in `mix.exs`

### 1.1.2 Add Erlang-RocksDB Dependency

- [x] **Task 1.1.2 Status** (Completed)

Add erlang-rocksdb package and system dependencies.

- [x] 1.1.2.1 Add `{:rocksdb, "~> 1.9"}` to dependencies in `mix.exs`
- [x] 1.1.2.2 Verify `rocksdb` package supports required features (column families, snapshots, iterators)
- [x] 1.1.2.3 Document system dependency: `librocksdb-dev` (Ubuntu) or `rocksdb` (brew)
- [ ] 1.1.2.4 Add pre-compilation hook to verify system RocksDB library availability (deferred - not essential)
- [x] 1.1.2.5 Update README.md with build requirements for erlang-rocksdb

### 1.1.3 Delete Rust Implementation

- [x] **Task 1.1.3 Status** (Completed)

Delete the Rust implementation completely (complete refactor, no archive needed).

- [x] 1.1.3.1 Delete `native/rocksdb_nif/` directory entirely
- [x] 1.1.3.2 Delete `Cargo.toml` from project root (N/A - did not exist)
- [x] 1.1.3.3 Delete `.cargo/` directory from project root (N/A - did not exist)
- [x] 1.1.3.4 Delete `native/` directory if empty (not empty - sparql_parser_nif remains)

### 1.1.4 Unit Tests

- [x] **Task 1.1.4 Status** (Completed)

- [x] 1.1.4.1 Test project compiles without rustler dependency
- [x] 1.1.4.2 Test erlang-rocksdb package loads successfully
- [x] 1.1.4.3 Test system RocksDB library is accessible
- [x] 1.1.4.4 Test basic `:rocksdb.open_with_cf/3` creates a new database

**Implementation Notes:**
- NIF module temporarily stubbed with error-raising functions until Task 1.2 implementation
- erlang-rocksdb C++ NIF compiled successfully via CMake
- Project compiles without Rust toolchain
- Basic operations (open, put, get, close) verified working

---

## 1.2 Database Operations Adapter

- [ ] **Section 1.2 Status** (Pending)

Implement adapter layer for basic database operations using erlang-rocksdb.

### 1.2.1 Database Open/Close Operations

- [ ] **Task 1.2.1 Status** (Pending)

Create database open/close functions that match current NIF API using erlang-rocksdb.

- [ ] 1.2.1.1 Create `lib/triple_store/backend/rocksdb/erlang_adapter.ex`
- [ ] 1.2.1.2 Implement `open/3` with column families matching current config
- [ ] 1.2.1.3 Map current column family names: `["id2str", "str2id", "spo", "pos", "osp", "derived", "numeric_range"]`
- [ ] 1.2.1.4 Implement `close/1` for graceful database shutdown
- [ ] 1.2.1.5 Implement `is_open?/1` status check
- [ ] 1.2.1.6 Implement `get_path/1` to retrieve database path
- [ ] 1.2.1.7 Handle `create_if_missing` and `error_if_exists` options

### 1.2.2 Basic Key-Value Operations

- [ ] **Task 1.2.2 Status** (Pending)

Implement get, put, delete, and exists operations.

- [ ] 1.2.2.1 Implement `get/3` for column-family-specific key lookup
- [ ] 1.2.2.2 Implement `put/4` for column-family-specific key insert
- [ ] 1.2.2.3 Implement `delete/3` for column-family-specific key deletion
- [ ] 1.2.2.4 Implement `exists/3` for key existence check
- [ ] 1.2.2.5 Map return values: `{:ok, binary}` vs `:not_found` matching current NIF
- [ ] 1.2.2.6 Handle binary key/value encoding (current format uses binaries)

### 1.2.3 Write Batch Operations

- [ ] **Task 1.2.3 Status** (Pending)

Implement atomic batch operations for bulk loading.

- [ ] 1.2.3.1 Implement `write_batch/3` for atomic multi-operation writes
- [ ] 1.2.3.2 Support batch operations: put, delete across multiple column families
- [ ] 1.2.3.3 Handle batch format: `[{cf, key, op}, ...]` where `op` is `{:put, value}` or `:delete`
- [ ] 1.2.3.4 Implement `delete_batch/3` for atomic multi-key deletions
- [ ] 1.2.3.5 Implement `mixed_batch/3` for combined put/delete operations
- [ ] 1.2.3.6 Handle `sync: false` option for bulk loading optimization

### 1.2.4 Database Utility Operations

- [ ] **Task 1.2.4 Status** (Pending)

Implement utility functions for database management.

- [ ] 1.2.4.1 Implement `flush_wal/2` for WAL flushing
- [ ] 1.2.4.2 Implement `list_column_families/2` for CF enumeration
- [ ] 1.2.4.3 Implement `set_options/3` for runtime reconfiguration
- [ ] 1.2.4.4 Map mutable options: `write_buffer_size`, `max_write_buffer_number`, etc.

### 1.2.5 Unit Tests

- [ ] **Task 1.2.5 Status** (Pending)

- [ ] 1.2.5.1 Test database open creates all 7 column families
- [ ] 1.2.5.2 Test existing database opens without data loss
- [ ] 1.2.5.3 Test get/put/delete operations match current NIF behavior
- [ ] 1.2.5.4 Test write_batch performs atomic multi-CF operations
- [ ] 1.2.5.5 Test flush_wal persists data correctly
- [ ] 1.2.5.6 Test binary key/value encoding compatibility

---

## 1.3 Binary Encoding Compatibility

- [ ] **Section 1.3 Status** (Pending)

Ensure binary encoding formats match between Rust NIF and erlang-rocksdb for data continuity.

### 1.3.1 Triple Key Encoding Verification

- [ ] **Task 1.3.1 Status** (Pending)

Verify triple key encoding (24 bytes, big-endian) works with erlang-rocksdb.

- [ ] 1.3.1.1 Document current key format: `<<subject::64-big, predicate::64-big, object::64-big>>`
- [ ] 1.3.1.2 Test Elixir binary encoding matches Rust byte-for-byte
- [ ] 1.3.1.3 Verify SPO index: `<<s::64-big, p::64-big, o::64-big>>`
- [ ] 1.3.1.4 Verify POS index: `<<p::64-big, o::64-big, s::64-big>>`
- [ ] 1.3.1.5 Verify OSP index: `<<o::64-big, s::64-big, p::64-big>>`

### 1.3.2 Dictionary Encoding Verification

- [ ] **Task 1.3.2 Status** (Pending)

Verify dictionary term encoding matches current implementation.

- [ ] 1.3.2.1 Document current ID format: 64-bit with type tag in high 4 bits
- [ ] 1.3.2.2 Verify URI encoding: `<<1::4, uri_string::binary>>`
- [ ] 1.3.2.3 Verify Blank Node encoding: `<<2::4, bnode_id::binary>>`
- [ ] 1.3.2.4 Verify Literal encoding: `<<3::4, format, value::binary>>`
- [ ] 1.3.2.5 Verify inline numeric types: xsd:integer, xsd:decimal, xsd:dateTime

### 1.3.3 Unit Tests

- [ ] **Task 1.3.3 Status** (Pending)

- [ ] 1.3.3.1 Test round-trip encoding: write with Rust NIF, read with erlang-rocksdb
- [ ] 1.3.3.2 Test triple key ordering is preserved across implementations
- [ ] 1.3.3.3 Test dictionary ID encoding preserves type tags
- [ ] 1.3.3.4 Test existing databases can be read by new adapter
- [ ] 1.3.3.5 Benchmark encoding performance vs Rust NIF

---

## 1.4 Column Family Configuration

- [ ] **Section 1.4 Status** (Pending)

Configure erlang-rocksdb column families to match current Rust NIF tuning.

### 1.4.1 Column Family Options Mapping

- [ ] **Task 1.4.1 Status** (Pending)

Map current Rust ColumnFamily options to erlang-rocksdb format.

- [ ] 1.4.1.1 Map bloom filter settings: 14 bits/key (dict), 12 bits/key (index), none (derived)
- [ ] 1.4.1.2 Map block size settings: 2KB (dict), 8KB (index), 32KB (derived)
- [ ] 1.4.1.3 Map compression settings: LZ4 for all levels, L0: none
- [ ] 1.4.1.4 Configure prefix extractor: `fixed_prefix(8)` for index CFs
- [ ] 1.4.1.5 Set memtable prefix bloom ratio: 0.1 for index CFs

### 1.4.2 Cache Configuration

- [ ] **Task 1.4.2 Status** (Pending)

Configure block cache and index/filter block caching.

- [ ] 1.4.2.1 Configure shared block cache: `lru_cache` with appropriate size
- [ ] 1.4.2.2 Set `cache_index_and_filter_blocks: true` for dict and index CFs
- [ ] 1.4.2.3 Set `pin_l0_filter_and_index_blocks_in_cache: true` for dict CFs
- [ ] 1.4.2.4 Disable cache pinning for derived CF (sequential access)

### 1.4.3 Compression Tuning

- [ ] **Task 1.4.3 Status** (Pending)

Configure per-level compression matching current settings.

- [ ] 1.4.3.1 Set L0 compression: `none`
- [ ] 1.4.3.2 Set L1-L6 compression: `lz4`
- [ ] 1.4.3.3 Configure compression options per column family
- [ ] 1.4.3.4 Verify compression ratios match Rust implementation

### 1.4.4 Unit Tests

- [ ] **Task 1.4.4 Status** (Pending)

- [ ] 1.4.4.1 Test column families open with correct options
- [ ] 1.4.4.2 Test bloom filter effectiveness matches configuration
- [ ] 1.4.4.3 Test cache hit rates with tuned configuration
- [ ] 1.4.4.4 Test compression ratios per column family
- [ ] 1.4.4.5 Test prefix extractor works correctly for index CFs

---

## 1.5 Integration Tests

- [ ] **Section 1.5 Status** (Pending)

End-to-end integration tests for basic migration functionality.

### 1.5.1 Database Lifecycle Tests

- [ ] **Task 1.5.1 Status** (Pending)

Test database creation, opening, and closing.

- [ ] 1.5.1.1 Test creating new database with all column families
- [ ] 1.5.1.2 Test opening existing database preserves data
- [ ] 1.5.1.3 Test database close releases resources
- [ ] 1.5.1.4 Test concurrent database open handles
- [ ] 1.5.1.5 Test database reopen after unclean shutdown

### 1.5.2 Data Migration Compatibility Tests

- [ ] **Task 1.5.2 Status** (Pending)

Test existing data can be read with new adapter.

- [ ] 1.5.2.1 Test reading dictionary data written by Rust NIF
- [ ] 1.5.2.2 Test reading triple indices written by Rust NIF
- [ ] 1.5.2.3 Test reading numeric range data written by Rust NIF
- [ ] 1.5.2.4 Test reading derived data written by Rust NIF
- [ ] 1.5.2.5 Verify no data loss across all column families

### 1.5.3 Basic Operations Tests

- [ ] **Task 1.5.3 Status** (Pending)

Test basic CRUD operations match Rust NIF behavior.

- [ ] 1.5.3.1 Test put/get round-trip for all column families
- [ ] 1.5.3.2 Test delete operation removes data correctly
- [ ] 1.5.3.3 Test exists returns correct results
- [ ] 1.5.3.4 Test write_batch performs atomic operations
- [ ] 1.5.3.5 Test mixed batch with puts and deletes

### 1.5.4 Performance Validation Tests

- [ ] **Task 1.5.4 Status** (Pending)

Validate performance is comparable to Rust NIF.

- [ ] 1.5.4.1 Benchmark point lookup latency vs Rust NIF
- [ ] 1.5.4.2 Benchmark write throughput vs Rust NIF
- [ ] 1.5.4.3 Benchmark batch write performance vs Rust NIF
- [ ] 1.5.4.4 Verify no significant regression in basic operations

---

## Success Criteria

1. **Data Compatibility**: 100% of existing databases can be opened and read without migration
2. **API Compatibility**: All existing NIF function calls work through adapter layer
3. **Performance**: Basic operations within 10% of Rust NIF performance
4. **Build Simplicity**: Project compiles without Rust toolchain
5. **Test Coverage**: All Phase 1 tests pass

## Provides Foundation

This phase enables:
- **Phase 2**: Iterator and snapshot migration
- **Phase 3**: Advanced optimization with fold operations

## References

- [Erlang-RocksDB Documentation](https://hexdocs.pm/rocksdb/)
- [Erlang-RocksDB GitHub](https://github.com/EnkiMultimedia/erlang-rocksdb)
- [Original Migration Analysis](../../research/1.03-key-value-storage/1.03.1-erlang-rocksdb.md)
- [Current Rust NIF](../../../native/_archive/rocksdb_nif/src/lib.rs)
- [Current NIF Wrapper](../../../lib/triple_store/backend/rocksdb/nif.ex)
