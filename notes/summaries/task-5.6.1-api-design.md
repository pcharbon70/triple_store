# Task 5.6.1 API Design Summary

**Date:** 2025-12-28
**Branch:** `feature/task-5.6.1-api-design`

## Overview

This task finalizes the public module interface for `TripleStore`, adding missing API functions and ensuring all core operations are exposed through a consistent, well-documented interface.

## API Audit

Before this task, the following were already implemented:
- `TripleStore.open/2` and `TripleStore.close/1`
- `TripleStore.load/2`, `load_graph/3`, `load_string/4`
- `TripleStore.query/2` and `TripleStore.update/2`
- `TripleStore.materialize/2`
- `TripleStore.backup/2` and `TripleStore.restore/2`
- `TripleStore.stats/1` and `TripleStore.health/1`

## New Functions Implemented

### 5.6.1.3: insert/2 and delete/2

**File:** `lib/triple_store.ex`

Added `TripleStore.insert/2` and `TripleStore.delete/2` for direct triple manipulation:

```elixir
# Single triple
{:ok, 1} = TripleStore.insert(store, {~I<http://ex.org/s>, ~I<http://ex.org/p>, ~L"value"})

# Multiple triples
{:ok, 2} = TripleStore.insert(store, [triple1, triple2])

# From RDF.Graph
{:ok, count} = TripleStore.insert(store, graph)

# Delete
{:ok, 1} = TripleStore.delete(store, {~I<http://ex.org/s>, ~I<http://ex.org/p>, ~L"value"})
```

**File:** `lib/triple_store/loader.ex`

Added underlying `Loader.insert/3` and `Loader.delete/3` functions:
- Handle single triples, lists, RDF.Description, and RDF.Graph
- Convert RDF terms to internal IDs via Adapter
- Use Index.insert_triples/delete_triples for storage

### 5.6.1.2: export/2

**File:** `lib/triple_store.ex`

Added `TripleStore.export/2` as a unified export interface:

```elixir
# Export as RDF.Graph
{:ok, graph} = TripleStore.export(store, :graph)

# Export to file
{:ok, count} = TripleStore.export(store, {:file, "data.ttl", :turtle})

# Export as string
{:ok, ttl} = TripleStore.export(store, {:string, :turtle})

# With pattern filter
{:ok, graph} = TripleStore.export(store, :graph, pattern: {:var, {:bound, pred_id}, :var})
```

Delegates to `TripleStore.Exporter` which already existed with full functionality.

### 5.6.1.5: reasoning_status/1

**File:** `lib/triple_store.ex`

Added `TripleStore.reasoning_status/1` to query reasoning subsystem state:

```elixir
{:ok, status} = TripleStore.reasoning_status(store)
# => {:ok, %{
#      state: :materialized,
#      profile: :owl2rl,
#      derived_count: 1500,
#      explicit_count: 5000,
#      total_count: 6500,
#      last_materialization: ~U[2025-12-28 10:00:00Z],
#      needs_rematerialization: false
#    }}
```

Uses `TripleStore.Reasoner.ReasoningStatus` module for status tracking.

## Complete Public API Summary

| Function | Purpose | Status |
|----------|---------|--------|
| `open/2` | Open a store | ✓ Existing |
| `close/1` | Close a store | ✓ Existing |
| `load/2` | Load RDF file | ✓ Existing |
| `load_graph/3` | Load RDF.Graph | ✓ Existing |
| `load_string/4` | Load RDF string | ✓ Existing |
| `insert/2` | Insert triples | ✓ **New** |
| `delete/2` | Delete triples | ✓ **New** |
| `export/2` | Export triples | ✓ **New** |
| `query/2` | SPARQL SELECT/ASK/CONSTRUCT | ✓ Existing |
| `update/2` | SPARQL UPDATE | ✓ Existing |
| `materialize/2` | OWL 2 RL reasoning | ✓ Existing |
| `reasoning_status/1` | Get reasoning state | ✓ **New** |
| `backup/2` | Create backup | ✓ Existing |
| `restore/2` | Restore backup | ✓ Existing |
| `stats/1` | Get statistics | ✓ Existing |
| `health/1` | Health check | ✓ Existing |

## Files Modified

| File | Changes |
|------|---------|
| `lib/triple_store.ex` | Added insert/2, delete/2, export/2, reasoning_status/1 |
| `lib/triple_store/loader.ex` | Added insert/3 and delete/3 functions |

## Test Results

- All 3838 tests pass
- Loader tests: 27 tests pass
- Exporter tests: 26 tests pass
- ReasoningStatus tests: 49 tests pass

## Design Decisions

1. **insert/delete in Loader**: Added to Loader module since it handles all data ingestion and modification, keeping Index module focused on low-level storage.

2. **export/2 unified interface**: Single entry point that delegates to Exporter based on target type (`:graph`, `{:file, ...}`, `{:string, ...}`).

3. **reasoning_status path-based keys**: Uses `phash2` of store path to generate unique atom keys for status lookup, avoiding atom table bloat.

4. **Return types**: All functions return `{:ok, result}` or `{:error, reason}` for consistency.
