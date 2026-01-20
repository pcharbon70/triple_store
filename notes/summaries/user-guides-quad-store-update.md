# User Guides Quad Store Update - Summary

**Date:** 2026-01-20
**Feature Branch:** `feature/update-user-guides-quad`
**Target Branch:** `quad`
**Status:** Complete

## Overview

Updated all 7 user guides to focus on quad store as the primary format, emphasizing N-Quads/TriG serialization formats and named graph management throughout.

## Motivation

The quad store is the more capable format, supporting everything the triple store does plus named graphs for data isolation, provenance tracking, and multi-tenancy. The documentation needed to reflect this by making quad store the recommended default.

## Changes Made

### Files Modified

| File | Lines Changed | Key Updates |
|------|---------------|-------------|
| `guides/user/01-getting-started.md` | ~50 | Quad store as default, N-Quads/TriG added to formats table |
| `guides/user/02-data-management.md` | ~80 | Added GraphBackup module, N-Quads/TriG loading, per-graph operations |
| `guides/user/03-sparql-queries.md` | ~5 | Added quad store assumption note |
| `guides/user/04-sparql-updates.md` | ~10 | Added quad store assumption note |
| `guides/user/05-reasoning.md` | ~5 | Added graph-scoped reasoning note |
| `guides/user/06-configuration.md` | ~5 | Added quad configuration note |
| `guides/user/07-named-graphs.md` | ~10 | Added recommendation note, updated comparison table |

### Detailed Changes by File

#### 01-getting-started.md
- Changed opening examples from `schema: :triple` (default) to `schema: :quad` (recommended)
- Added N-Quads and TriG to the supported formats table
- Updated complete example to use quad store with named graphs
- Added recommendation callout at top of guide

#### 02-data-management.md
- Added N-Quads format loading examples with `.nq` files
- Added TriG format loading examples with `.trig` files
- Documented `TripleStore.GraphBackup` module for per-graph backup
- Added per-graph export/import code examples
- Added graph management section (list graphs, copy graphs, move graphs)
- Updated backup section for quad store considerations

#### 03-sparql-queries.md
- Added note at top explaining quad store assumption
- Already had comprehensive GRAPH clause section (no additional changes needed)

#### 04-sparql-updates.md
- Added note at top explaining quad store assumption
- Updated basic example to use GRAPH clause
- Already had comprehensive Named Graph Updates section (no additional changes needed)

#### 05-reasoning.md
- Added note at top explaining graph-scoped reasoning requires quad store
- Already had comprehensive graph-scoped reasoning section (no additional changes needed)

#### 06-configuration.md
- Added note at top explaining quad store has different memory requirements
- Already had comprehensive Quad Store Configuration section (no additional changes needed)

#### 07-named-graphs.md
- Added recommendation callout at top
- Updated comparison table with version numbers (v1 vs v2) and Default Graph ID

## Key Pattern Changes

### Before
```elixir
# Open or create a triple store (default schema)
{:ok, store} = TripleStore.open("./my_database")
```

### After
```elixir
# Open or create a quad store (recommended)
{:ok, store} = TripleStore.open("./my_database", schema: :quad)
```

## Format Emphasis

Added prominence to:
- **N-Quads (`.nq`)**: Line-based quad format with graph context
- **TriG (`.trig`)**: Human-readable Turtle with named graphs

## Backward Compatibility

Triple store is still mentioned as an alternative for simple use cases where named graphs are not needed. All changes are additive - no breaking changes to the API or existing documentation structure.

## Testing

No code changes were made, only documentation updates. The existing test suite validates all functionality described in the guides.

## Git Status

```
On branch feature/update-user-guides-quad
Changes not staged for commit:
  modified:   guides/user/01-getting-started.md
  modified:   guides/user/02-data-management.md
  modified:   guides/user/03-sparql-queries.md
  modified:   guides/user/04-sparql-updates.md
  modified:   guides/user/05-reasoning.md
  modified:   guides/user/06-configuration.md
  modified:   guides/user/07-named-graphs.md

Untracked files:
  notes/features/update-user-guides-quad.md
```

## Next Steps

Pending user approval to:
1. Commit changes with descriptive message
2. Merge `feature/update-user-guides-quad` branch into `quad` branch

## Planning Document

See `notes/features/update-user-guides-quad.md` for detailed implementation plan and checklist.
