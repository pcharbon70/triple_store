# Phase 8.3: Documentation Updates for Quad Store

**Date**: 2026-01-20
**Branch**: `feature/phase-8.3-documentation`
**Status**: Complete

## Overview

This phase involved updating all user-facing documentation to cover the quad store (named graphs) feature. The quad store adds support for RDF named graphs, enabling use cases like multi-tenancy, provenance tracking, and data isolation.

## Work Completed

### 1. New Guide: `07-named-graphs.md` (Created)

A comprehensive new guide covering quad store usage:

- Triple vs Quad store comparison table
- When to use named graphs
- Opening a quad store with `schema: :quad`
- Loading data into named graphs (N-Quads, TriG formats)
- Querying named graphs with GRAPH clause
- Updating data in named graphs
- Listing graphs and statistics
- Graph-scoped reasoning configuration
- Performance considerations
- Best practices
- Complete working example

### 2. Updated: `01-getting-started.md`

- Added triple vs quad store comparison in the overview
- Updated "Opening a Store" section to show both schemas
- Added note about schema immutability
- Updated RDF formats table to show quad store support for N-Quads and TriG
- Added Named Graphs link to Next Steps

### 3. Updated: `03-sparql-queries.md`

Added comprehensive "GRAPH Clause (Named Graphs)" section:
- Query a specific named graph
- Query all graphs with graph variable
- Combine graph patterns with UNION
- Graph variable in patterns for correlation
- Filter by graph with IN clause
- Graph with subpatterns
- Count per graph
- Default graph vs named graphs comparison

### 4. Updated: `04-sparql-updates.md`

Added "Named Graph Updates" section:
- INSERT DATA into named graphs
- DELETE DATA from named graphs
- INSERT ... WHERE with GRAPH
- DELETE ... WHERE with GRAPH
- DELETE/INSERT ... WHERE for moving data between graphs
- DELETE entire graph (two methods)
- COPY graphs
- MOVE graphs
- INSERT into multiple graphs dynamically
- Graph-scoped update patterns (archive by date, merge sources)

### 5. Updated: `05-reasoning.md`

Added "Graph-Scoped Reasoning" section:
- Local vs Global reasoning configuration
- ReasoningConfig with `:local` and `:global` scope options
- When to use each scope
- Local reasoning example (isolated graph reasoning)
- Global reasoning example (merged graph reasoning)
- Materializing specific graphs with GraphScopedReasoner
- Graph-scoped best practices (schema placement, multi-tenancy, performance)

### 6. Updated: `06-configuration.md`

Added "Quad Store Configuration" subsection:
- Triple vs Quad performance comparison table
- Memory budget considerations (1.5-2x more memory for quad)
- Quad-specific configuration presets
- When to use quad vs triple configuration
- Quad store performance tuning tips (block cache, memtable, bloom filters)

### 7. Updated: `README.md`

- Updated description to mention both triple and quad schemas
- Enhanced features list with named graphs and quad indices
- Split Usage section into "Triple Store (Default)" and "Quad Store (Named Graphs)"
- Added quad store usage example with GRAPH clause
- Added reference to Named Graphs guide

## Files Modified

```
Modified:
  README.md
  guides/user/01-getting-started.md
  guides/user/03-sparql-queries.md
  guides/user/04-sparql-updates.md
  guides/user/05-reasoning.md
  guides/user/06-configuration.md

Created:
  guides/user/07-named-graphs.md
```

## Key Documentation Themes

1. **Clear distinction between triple and quad schemas** - Users understand when to choose each
2. **Consistent examples** - All guides follow the same pattern and style
3. **Cross-references** - Guides link to each other appropriately
4. **Performance awareness** - Quad store overhead is clearly documented

## Testing Notes

Documentation was verified against the existing implementation:
- Quad store column families (GSPO, GPOS, SPOG, POSG)
- SPARQL GRAPH clause query support
- SPARQL UPDATE with GRAPH keyword
- Graph-scoped reasoning with ReasoningConfig

## Next Steps

User has requested permission to commit and merge to the `quad` branch.
