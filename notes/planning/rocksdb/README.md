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

- [ ] 1.3 Binary Encoding Compatibility - PENDING
- [ ] 1.4 Column Family Configuration - PENDING
- [ ] 1.5 Integration Tests - PENDING

### Phase 2 Status (Iterator & Snapshot Migration)

All tasks pending.

### Phase 3 Status (Optimization & Cleanup)

All tasks pending.

## References

- [Research Analysis](../../research/1.03-key-value-storage/1.03.1-erlang-rocksdb.md)
- [Erlang-RocksDB Documentation](https://hexdocs.pm/rocksdb/)
- [Erlang-RocksDB GitHub](https://github.com/EnkiMultimedia/erlang-rocksdb)
