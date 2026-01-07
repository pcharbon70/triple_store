# Erlang-RocksDB Migration Plan

## Overview

This directory contains the phased migration plan for transitioning the TripleStore from a custom Rust NIF implementation to the Erlang-RocksDB C++ NIF library.

## Migration Goals

1. **Eliminate Rust Build Dependency**: Remove rustler and Rust toolchain from the build process
2. **Maintain Data Compatibility**: 100% compatibility with existing databases
3. **Improve Performance**: Leverage erlang-rocksdb's `fold` operations to reduce context switching
4. **Simplify Maintenance**: Use a battle-tested, community-maintained library

## Phases

| Phase | Document | Status | Description |
|-------|----------|--------|-------------|
| 1 | [phase-01-foundation-migration.md](./phase-01-foundation-migration.md) | In Progress | Basic operations (open, get, put, delete, write_batch) |
| 2 | [phase-02-iterator-snapshot-migration.md](./phase-02-iterator-snapshot-migration.md) | Pending | Iterators, snapshots, and prefix operations |
| 3 | [phase-03-optimization-cleanup.md](./phase-03-optimization-cleanup.md) | Pending | Fold optimization and Rust removal |

## Progress

### Phase 1 Status (Foundation Migration)

- [x] 1.1 Dependency Management - COMPLETED
  - Removed rustler from mix.exs
  - Added erlang-rocksdb dependency
  - Deleted Rust RocksDB NIF code
  - Updated README with new build requirements

- [x] 1.2 Database Operations Adapter - COMPLETED
  - Created ErlangAdapter module
  - Implemented open/close with 7 column families
  - Implemented get/put/delete/exists operations
  - Implemented write_batch for atomic operations
  - Implemented utility functions (flush_wal, list_column_families, set_options)
  - All 4,523 unit tests pass

- [x] 1.3 Binary Encoding Compatibility - COMPLETED (2026-01-07)
  - Verified triple key encoding (24-byte big-endian)
  - Verified dictionary ID encoding (64-bit with type tags)
  - Verified inline numeric encoding (integer, decimal, datetime)
  - Created 23 encoding compatibility tests (all passing)
  - See `notes/summaries/phase-1.3-binary-encoding-compatibility.md` for details

- [x] 1.4 Column Family Configuration - COMPLETED (2026-01-07)
  - Configured bloom filters: 14 bits/key (dict), 12 bits/key (index), none (derived)
  - Configured block sizes: 2KB (dict), 8KB (index), 32KB (derived)
  - Configured compression: LZ4 for L1-L6, none for L0
  - Configured prefix extractor: 8-byte fixed prefix for index CFs
  - Created ColumnFamilyConfig module with all CF options
  - Created 25 configuration tests (all passing)
  - See `notes/summaries/phase-1.4-column-family-configuration.md` for details

- [x] 1.5 Integration Tests - COMPLETED (2026-01-07)
  - Created 19 integration tests covering database lifecycle, CRUD operations, encoding compatibility
  - All tests passing
  - See `notes/summaries/phase-1.5-integration-tests.md` for details

### Phase 2 Status (Iterator & Snapshot Migration)

All tasks pending.

### Phase 3 Status (Optimization & Cleanup)

All tasks pending.

## References

- [Research Analysis](../../research/1.03-key-value-storage/1.03.1-erlang-rocksdb.md)
- [Erlang-RocksDB Documentation](https://hexdocs.pm/rocksdb/)
- [Erlang-RocksDB GitHub](https://github.com/EnkiMultimedia/erlang-rocksdb)
