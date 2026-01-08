# Feature Plan: Phase 3.3 - Rust NIF Removal

**Feature**: Section 3.3 - Rust NIF Removal
**Branch**: `feature/section-3.3-rust-nif-removal`
**Date**: 2026-01-08
**Status**: Planning

---

## Problem Statement

The TripleStore project has completed migration from a custom Rust NIF implementation to the erlang-rocksdb C++ NIF library. However, some artifacts remain:

1. **`native/rocksdb_nif/`** directory - Old Rust RocksDB NIF code (no longer used)
2. **`lib/triple_store/backend/rocksdb/nif.ex`** - NIF wrapper module (delegates to ErlangAdapter)
3. **26 files** still reference `TripleStore.Backend.RocksDB.NIF`

These artifacts create confusion about the architecture and should be removed to complete the migration.

---

## Solution Overview

Remove all Rust NIF artifacts and update references to use ErlangAdapter directly.

### Key Decisions Needed from Developer

**Question 1**: Should we delete `lib/triple_store/backend/rocksdb/nif.ex` entirely?

**Current State**:
- `nif.ex` is a thin wrapper that delegates all calls to `ErlangAdapter`
- It provides a clean API with documentation
- 26 files in the codebase reference `NIF.function(...)`

**Options**:
- **A**: Delete `nif.ex` and update all 26 files to use `ErlangAdapter` directly
  - Pros: Simpler architecture, one less module to maintain
  - Cons: Large refactoring (26 files), potential for bugs

- **B**: Keep `nif.ex` as a convenience alias/wrapper
  - Pros: Minimal changes, maintains stable API
  - Cons: Extra indirection layer

**Recommendation**: Option B - Keep `nif.ex` as a documented convenience wrapper, since it's already delegating to ErlangAdapter and provides a clean public API.

---

## Current State Analysis

### Native Directory Contents

```
native/
├── rocksdb_nif/     # Old Rust RocksDB NIF (CAN BE DELETED)
└── sparql_parser_nif/  # SPARQL parser NIF (MUST KEEP - still in use)
```

**Note**: Only `rocksdb_nif/` should be deleted. `sparql_parser_nif/` is still actively used.

### NIF Module Analysis

File: `lib/triple_store/backend/rocksdb/nif.ex`
- Lines: ~200+
- Purpose: Thin wrapper around ErlangAdapter
- All functions delegate to ErlangAdapter GenServer
- Already updated to use erlang-rocksdb (not Rust)

Example:
```elixir
def open(path, opts \\ []) do
  ErlangAdapter.open(path, opts)
end
```

### Files Referencing NIF (26 total)

Core modules:
- `lib/triple_store.ex`
- `lib/triple_store/backend/rocksdb/nif.ex`
- `lib/triple_store/backend/rocksdb/read_options.ex`
- `lib/triple_store/backend/rocksdb/write_options.ex`

Index modules:
- `lib/triple_store/index.ex`
- `lib/triple_store/index/subject_cache.ex`
- `lib/triple_store/index/numeric_range.ex`

Dictionary modules:
- `lib/triple_store/dictionary/id_to_string.ex`
- `lib/triple_store/dictionary/string_to_id.ex`
- `lib/triple_store/dictionary/manager.ex`
- `lib/triple_store/dictionary/sequence_counter.ex`

SPARQL modules:
- `lib/triple_store/sparql/parser.ex`
- `lib/triple_store/sparql/parser/nif.ex`
- `lib/triple_store/sparql/update_executor.ex`
- `lib/triple_store/sparql/leapfrog/trie_iterator.ex`

Reasoner modules:
- `lib/triple_store/reasoner/derived_store.ex`
- `lib/triple_store/reasoner/delete_with_reasoning.ex`
- `lib/triple_store/reasoner/incremental.ex`

Other modules:
- `lib/triple_store/transaction.ex`
- `lib/triple_store/statistics.ex`
- `lib/triple_store/statistics/cache.ex`
- `lib/triple_store/health.ex`
- `lib/triple_store/snapshot.ex`
- `lib/triple_store/loader.ex`
- `lib/triple_store/backup.ex`
- `lib/triple_store/config/runtime.ex`

---

## Implementation Plan

### Task 3.3.1: Remove Native Directory Artifacts

- [ ] 3.3.1.1 Delete `native/_archive/rocksdb_nif/` directory (if exists)
- [ ] 3.3.1.2 Delete `native/rocksdb_nif/` directory
- [ ] 3.3.1.3 Verify `.cargo/` and `Cargo.toml` don't exist (already removed)
- [ ] 3.3.1.4 Keep `native/sparql_parser_nif/` (still in use)
- [ ] 3.3.1.5 Remove `native/` directory if empty (after sparql_parser_nif review)

### Task 3.3.2: Update NIF Wrapper

**PENDING DECISION**: Based on developer response to Question 1 above.

Option A (Delete NIF module):
- [ ] 3.3.2.1 Delete `lib/triple_store/backend/rocksdb/nif.ex`
- [ ] 3.3.2.2 Update all 26 files to use `ErlangAdapter` directly
- [ ] 3.3.2.3 Update import statements
- [ ] 3.3.2.4 Verify no orphaned NIF calls remain
- [ ] 3.3.2.5 Test all backend operations work correctly

Option B (Keep NIF module as wrapper):
- [ ] 3.3.2.1 Update `nif.ex` documentation to clarify it's a convenience wrapper
- [ ] 3.3.2.2 Verify all functions properly delegate to ErlangAdapter
- [ ] 3.3.2.3 Mark as deprecated with migration path (optional)
- [ ] 3.3.2.4 No code changes needed in 26 files
- [ ] 3.3.2.5 Test all backend operations work correctly

### Task 3.3.3: Update Configuration Modules

- [ ] 3.3.3.1 Check if `lib/triple_store/config/column_family.ex` exists
- [ ] 3.3.3.2 Check if `lib/triple_store/config/rocksdb.ex` exists
- [ ] 3.3.3.3 Check if `lib/triple_store/config/compression.ex` exists
- [ ] 3.3.3.4 Remove any Rust-specific configuration options
- [ ] 3.3.3.5 Add erlang-rocksdb specific configuration if needed

### Task 3.3.4: Unit Tests

- [ ] 3.3.4.1 Test no references to Rust NIF remain (grep for "rustler", "Cargo")
- [ ] 3.3.4.2 Test project compiles without Rust dependencies
- [ ] 3.3.4.3 Test all configuration modules work correctly
- [ ] 3.3.4.4 Verify build time reduced significantly
- [ ] 3.3.4.5 Test deployment no longer requires Rust toolchain

---

## Success Criteria

1. No `native/rocksdb_nif/` directory exists
2. No Rust source files (`.rs`) in project (except sparql_parser_nif)
3. No `rustler` references in `mix.exs`
4. All tests pass without Rust toolchain
5. Build completes without Rust compilation
6. Documentation updated to reflect final architecture

---

## Risks and Considerations

1. **Breaking Change**: Deleting `nif.ex` would require updating 26 files
2. **sparql_parser_nif**: Must NOT be deleted (still actively used)
3. **Configuration Modules**: May not exist (need to verify)
4. **Test Coverage**: Must verify all 26 files still work after changes

---

## Questions for Developer

1. **Should we delete `nif.ex` entirely, or keep it as a convenience wrapper?**
   - Option A: Delete and update 26 files
   - Option B: Keep as documented wrapper (recommended)

2. **Should we create a deprecation warning if keeping `nif.ex`?**
   - Add `@deprecated "Use TripleStore.Backend.RocksDB.ErlangAdapter instead"`

3. **What should we do about the `native/` directory after deleting `rocksdb_nif/`?**
   - It will still contain `sparql_parser_nif/`
   - Keep the directory or move `sparql_parser_nif/` elsewhere?

---

## Next Steps

1. **Get developer decision** on Question 1 (delete or keep `nif.ex`)
2. **Verify configuration modules** exist and need updates
3. **Create backup** before大规模 refactoring (if Option A chosen)
4. **Execute implementation** based on decisions
5. **Run full test suite** to verify no regressions

---

## Status

**Current**: **COMPLETED** (2026-01-08)

**Implementation Summary**:
- Kept `nif.ex` as convenience wrapper with deprecation warning
- Deleted `native/rocksdb_nif/` directory
- Preserved `native/sparql_parser_nif/` (still in use)
- Updated documentation with migration guide
- All configuration modules verified (no changes needed)

**Last Updated**: 2026-01-08
