# Update User Guides for Quad Store Focus

**Status:** ✅ Complete
**Priority:** High
**Created:** 2026-01-20
**Completed:** 2026-01-20

---

## Executive Summary

Update all user guides to focus on quad store as the primary format, with N-Quads/TriG as the default serialization formats, and named graph management throughout.

**Rationale:**
- Quad store is the more capable format (supports everything triple store does plus named graphs)
- N-Quads/TriG are more modern formats with explicit graph context
- Named graphs enable multi-tenancy, provenance tracking, and data isolation
- Aligning documentation with codebase reality (quad branch is the future)

---

## Current State Analysis

### User Guides Status

| File | Status | Changes Made |
|------|--------|--------------|
| `01-getting-started.md` | ✅ Complete | Quad store now recommended default, N-Quads/TriG added to formats table, complete example uses quad store with named graphs |
| `02-data-management.md` | ✅ Complete | Added N-Quads/TriG loading examples, documented GraphBackup module, added per-graph export/import and graph management sections |
| `03-sparql-queries.md` | ✅ Complete | Added note about quad store assumption, already had comprehensive GRAPH clause section |
| `04-sparql-updates.md` | ✅ Complete | Added note about quad store assumption, updated examples to use GRAPH clause, already had comprehensive Named Graph Updates section |
| `05-reasoning.md` | ✅ Complete | Added note about graph-scoped reasoning requiring quad store, already had comprehensive graph-scoped reasoning section |
| `06-configuration.md` | ✅ Complete | Added note about quad store configuration requirements, already had comprehensive Quad Store Configuration section |
| `07-named-graphs.md` | ✅ Complete | Added recommendation note about quad store, updated comparison table with version numbers and Default Graph ID |

---

## Implementation Plan

### 1. Getting Started Guide (01-getting-started.md)

- [x] Make quad store the recommended default in examples
- [x] Promote N-Quads and TriG alongside Turtle/N-Triples
- [x] Add graph context to quick start examples
- [x] Update complete example to use quad store
- [x] Emphasize named graphs from the beginning

### 2. Data Management Guide (02-data-management.md)

- [x] Add N-Quads format to loading section
- [x] Add TriG format to loading section
- [x] Document GraphBackup module for per-graph backup
- [x] Add per-graph export/import examples
- [x] Update backup section for quad store considerations
- [x] Add batch loading examples for named graphs

### 3. SPARQL Queries Guide (03-sparql-queries.md)

- [x] Add GRAPH clause examples
- [x] Show querying across named graphs
- [x] Add graph variable examples
- [x] Update triple patterns to show quad patterns
- [x] Include default graph vs named graph query patterns

### 4. SPARQL Updates Guide (04-sparql-updates.md)

- [x] Add GRAPH clause to INSERT examples
- [x] Add GRAPH clause to DELETE examples
- [x] Show COPY/MOVE between graphs
- [x] Document graph creation and management
- [x] Add INSERT DATA with GRAPH examples

### 5. Reasoning Guide (05-reasoning.md)

- [x] Review for graph-scoped reasoning coverage
- [x] Ensure local vs global reasoning is clear
- [x] Add quad-specific reasoning examples
- [x] Document materialization per graph

### 6. Configuration Guide (06-configuration.md)

- [x] Add quad-specific configuration options
- [x] Document 4-index vs 3-index settings
- [x] Add graph-related configuration
- [x] Update performance tuning for quad store

### 7. Named Graphs Guide (07-named-graphs.md)

- [x] Review and ensure consistency with other guides
- [x] Add any missing quad-specific features
- [x] Ensure cross-references are correct

---

## Key Changes Summary

### Default Schema Change

**Before:**
```elixir
# Open or create a triple store (default schema)
{:ok, store} = TripleStore.open("./my_database")
```

**After:**
```elixir
# Open or create a quad store (recommended)
{:ok, store} = TripleStore.open("./my_database", schema: :quad)
```

### Format Emphasis

**Add to all guides:**
- N-Quads (`.nq`) as primary quad format
- TriG (`.trig`) as human-readable quad format
- Show graph context in all examples

### GRAPH Clause Examples

**Add to query guide:**
```sparql
# Query specific named graph
SELECT ?s ?p ?o
WHERE {
  GRAPH ex:mygraph {
    ?s ?p ?o
  }
}

# Query all graphs
SELECT ?g ?s ?p ?o
WHERE {
  GRAPH ?g {
    ?s ?p ?o
  }
}
```

### Per-Graph Backup

**Add to data management:**
```elixir
# Backup single graph
{:ok, metadata} = TripleStore.GraphBackup.backup_graph(
  store,
  graph_id,
  "/backups/graph.nq"
)

# Export graph as N-Quads
{:ok, nquads} = TripleStore.GraphBackup.export_graph(store, graph_id)
```

---

## Files to Modify

| File | Priority | Changes |
|------|----------|---------|
| `guides/user/01-getting-started.md` | High | Quad as default, N-Quads examples |
| `guides/user/02-data-management.md` | High | Add GraphBackup, N-Quads, TriG |
| `guides/user/03-sparql-queries.md` | High | Add GRAPH clause examples |
| `guides/user/04-sparql-updates.md` | High | Add GRAPH to INSERT/DELETE |
| `guides/user/05-reasoning.md` | Medium | Review graph-scoped reasoning |
| `guides/user/06-configuration.md` | Medium | Add quad-specific settings |
| `guides/user/07-named-graphs.md` | Low | Review for consistency |

---

## Success Criteria

- [x] All examples use quad store as default
- [x] N-Quads and TriG formats prominently featured
- [x] GRAPH clause examples in query/update guides
- [x] GraphBackup module documented
- [x] All guides cross-reference consistently
- [x] Triple store still mentioned as alternative for simple use cases

---

## Notes

- Keep triple store as an alternative for simple use cases
- Don't break existing URLs (file names stay the same)
- Maintain backward compatibility in examples (show both schemas)
- Focus on additive changes (quad = triple + graphs)
