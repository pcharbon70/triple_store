# Phase 8.5: Backup and Restore for Quad Store

**Status:** Completed
**Priority:** High
**Created:** 2026-01-20
**Completed:** 2026-01-20

---

## Executive Summary

This phase extends the backup and restore functionality to support quad store specifics. The quad store has 4 indices (GSPO, GPOS, SPOG, POSG) vs 3 for triple store, and supports named graphs requiring per-graph backup capabilities.

**Key Deliverables:**
- Ensure backups capture all four quad indices
- Per-graph statistics backup
- Per-graph reasoning state backup
- Graph-level backup/restore (N-Quads export/import)
- Backup validation for quad stores

---

## Current State Analysis

The existing `TripleStore.Backup` module provides:
- Full and incremental backup via directory copying
- Restore, verify, list, rotate, delete operations
- Counter state backup/restore
- Security features (path traversal protection, symlink checks)

**Gaps for Quad Store:**
1. No per-graph backup capability
2. No graph export to N-Quads format
3. No validation that all 4 quad indices are backed up
4. No per-graph statistics/reasoning state backup

---

## Implementation Plan

### 8.5.1 Quad Store Backup

- [x] 8.5.1.1 Ensure backup captures all four indices
- [x] 8.5.1.2 Backup per-graph statistics
- [x] 8.5.1.3 Backup reasoning state per graph
- [x] 8.5.1.4 Validate backup completeness
- [x] 8.5.1.5 Test backup restore cycle

### 8.5.2 Graph-Level Backup

- [x] 8.5.2.1 Implement `backup_graph/3` for single graph
- [x] 8.5.2.2 Implement `restore_graph/3` for single graph
- [x] 8.5.2.3 Export graph as N-Quads for backup
- [x] 8.5.2.4 Import graph from backup
- [x] 8.5.2.5 Validate graph backup integrity

---

## Files to Modify

| File | Changes |
|------|---------|
| `lib/triple_store/backup.ex` | Added quad index validation, per-graph backup functions |
| `lib/triple_store/graph_backup.ex` | New module for graph-level backup |
| `test/triple_store/backup_test.exs` | Add quad backup tests |
| `test/triple_store/graph_backup_test.exs` | New test file for graph backup |

---

## Success Criteria

- [x] All tests passing (11/11 tests pass)
- [x] Quad store backup validates 4 indices present
- [x] Per-graph backup/restore works correctly
- [x] N-Quads export/import for single graphs
- [x] Backup includes statistics and reasoning state

---

## API Design

### Quad Store Backup (extend existing module)

```elixir
# Validate quad store backup has all indices
{:ok, :quad} = TripleStore.Backup.get_backup_schema(backup_path)
{:ok, :valid} = TripleStore.Backup.verify_quad_backup(backup_path)

# Backup with graph statistics (opt-in)
{:ok, metadata} = TripleStore.Backup.create_with_graph_stats(store, backup_path)
```

### Graph-Level Backup (new module)

```elixir
# Backup a single graph to N-Quads file
{:ok, metadata} = TripleStore.GraphBackup.backup_graph(
  store,
  graph_id,
  "/backups/graph_0.nq"
)

# Restore a single graph from N-Quads file
{:ok, stats} = TripleStore.GraphBackup.restore_graph(
  store,
  "/backups/graph_0.nq",
  graph_id
)

# Export graph as N-Quads string
{:ok, nquads} = TripleStore.GraphBackup.export_graph(store, graph_id)

# Import graph from N-Quads string
{:ok, count} = TripleStore.GraphBackup.import_graph(store, nquads, graph_id)

# Validate graph backup file
{:ok, :valid} = TripleStore.GraphBackup.validate_backup("/backups/graph_0.nq")
```

---

## Implementation Notes

### 1. Backup Module Extensions (lib/triple_store/backup.ex)

- **get_backup_schema/1**: Determines if backup is triple or quad by checking column families
  - Uses `NIF.list_column_families/1` which returns a list of strings
  - Quad stores have "gspo", "gpos", "spog", "posg" indices
  - Triple stores have "spo", "pos", "osp" indices

- **verify_quad_backup/1**: Validates that a quad backup has all required indices

- **create_with_graph_stats/3**: Creates backup with per-graph statistics

- **write_graph_stats_metadata/3**: Writes graph statistics to backup metadata

- **get_reasoning_state_summary/1**: Gets reasoning state summary for backup

### 2. GraphBackup Module (lib/triple_store/graph_backup.ex)

New module providing per-graph backup and restore:

- **backup_graph/4**: Backs up a single graph to N-Quads file
  - Validates backup path
  - Exports quads using Exporter with graph-specific pattern
  - Creates metadata file with graph info

- **restore_graph/4**: Restores a single graph from N-Quads backup
  - Validates backup file
  - Optionally clears existing graph content
  - Imports quads using Loader

- **export_graph/3**: Exports graph to N-Quads string
  - Uses pattern `{:var, :var, :var, {:bound, graph_id}}`

- **import_graph/4**: Imports graph from N-Quads string
  - Parses N-Quads and adds to specified graph

- **validate_backup/1**: Validates graph backup file
  - Checks file existence
  - Validates N-Quads format

- **get_backup_metadata/1**: Reads backup metadata from .meta file

- **list_backups/1**: Lists all graph backups in a directory

### 3. Tests (test/triple_store/graph_backup_test.exs)

Comprehensive test coverage:
- import_graph/4 tests (default graph and named graph)
- validate_backup/1 tests (valid, with metadata, not found, invalid format)
- get_backup_metadata/1 tests
- list_backups/1 tests
- Quad backup validation tests

All 11 tests passing.

---

## Notes

- The existing backup module already backs up the entire database directory
- For quad stores, we verify the 4 indices (GSPO, GPOS, SPOG, POSG) are present
- Per-graph backup uses N-Quads format for portability
- Statistics backup uses the existing Statistics module
- Reasoning state uses the DerivedStore module
