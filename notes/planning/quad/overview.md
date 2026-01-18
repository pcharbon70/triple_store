# Quad Store Implementation Plan: Overview

## Executive Summary

This document outlines the implementation plan for adding named graph (quad) support to the triple store. The quad store extends the existing triple-only storage to support SPARQL 1.1 named graphs, enabling graph-scoped queries and updates.

**Key Changes:**
- Keys: 24 bytes → 32 bytes (add graph component)
- Indices: 3 (SPO, POS, OSP) → 4 (GSPO, GPOS, SPOG, POSG)
- Schema: Version 1 (triple) → Version 2 (quad)
- **No backward compatibility** - triple stores cannot open in quad mode

---

## Architecture Overview

### Triple Store (Current)

```
┌──────────────────────────────────────────────────────────────┐
│                    TripleStore Public API                     │
├──────────────────────────────────────────────────────────────┤
│                    SPARQL Query Engine                        │
├──────────────────────────────────────────────────────────────┤
│                    Index & Dictionary Layer                   │
│              SPO | POS | OSP (3 indices, 24-byte keys)        │
├──────────────────────────────────────────────────────────────┤
│                    RocksDB Storage                            │
│         id2str | str2id | spo | pos | osp | derived          │
└──────────────────────────────────────────────────────────────┘
```

### Quad Store (Target)

```
┌──────────────────────────────────────────────────────────────┐
│                    TripleStore Public API                     │
│                   (now graph-aware)                           │
├──────────────────────────────────────────────────────────────┤
│                    SPARQL Query Engine                        │
│                   (GRAPH clause support)                      │
├──────────────────────────────────────────────────────────────┤
│                    QuadIndex Layer                            │
│        GSPO | GPOS | SPOG | POSG (4 indices, 32-byte keys)   │
├──────────────────────────────────────────────────────────────┤
│                    RocksDB Storage                            │
│      id2str | str2id | gspo | gpos | spog | posg | derived    │
└──────────────────────────────────────────────────────────────┘
```

---

## Phase Breakdown

| Phase | Focus | Duration (Est.) | Status |
|-------|-------|-----------------|--------|
| 1 | Quad Storage Foundation | 2-3 weeks | Not Started |
| 2 | RDF Integration & Loading | 1-2 weeks | Not Started |
| 3 | SPARQL Query Execution | 2-3 weeks | Not Started |
| 4 | SPARQL UPDATE with Graphs | 1-2 weeks | Not Started |
| 5 | Statistics & Optimization | 1-2 weeks | Not Started |
| 6 | Integration Tests | 1 week | Not Started |
| 7 | Reasoning with Named Graphs | 2 weeks | Not Started |
| 8 | Production Hardening | 1-2 weeks | Not Started |

**Total Estimated Duration:** 13-19 weeks

---

## Phase Details

### Phase 1: Quad Storage Foundation

**Goal:** Establish quad storage with four indices.

**Key Deliverables:**
- 32-byte quad key encoding (s, p, o, g)
- Four indices: GSPO, GPOS, SPOG, POSG
- Default graph ID = 0
- Schema versioning (v2 = quad)
- All 16 quad patterns supported

**Critical Files:**
- `lib/triple_store/quad_index.ex` (new)
- `lib/triple_store/backend/rocksdb/column_family_config.ex` (update)

---

### Phase 2: RDF Integration & Loading

**Goal:** Support N-Quads and TriG formats.

**Key Deliverables:**
- N-Quads loading/export
- TriG loading/export
- Graph enumeration (list_graphs)
- Graph deletion (delete_graph)
- Graph-scoped loading

**Critical Files:**
- `lib/triple_store/rdf_adapter.ex` (update)
- `lib/triple_store/loader.ex` (update)
- `lib/triple_store/exporter.ex` (update)

---

### Phase 3: SPARQL Query Execution

**Goal:** Execute GRAPH clauses.

**Key Deliverables:**
- GRAPH <iri> execution (named graph)
- GRAPH ?var execution (graph variable)
- Quad BGP execution
- Cross-graph queries
- Graph variable in results

**Critical Files:**
- `lib/triple_store/sparql/executor.ex` (update)

---

### Phase 4: SPARQL UPDATE with Graphs

**Goal:** Full SPARQL UPDATE with named graphs.

**Key Deliverables:**
- CREATE/DROP/CLEAR GRAPH
- INSERT/DELETE DATA with graphs
- MODIFY with graph context
- COPY/MOVE/ADD operations

**Critical Files:**
- `lib/triple_store/sparql/update_executor.ex` (update)

---

### Phase 5: Statistics & Optimization

**Goal:** Quad-aware optimization.

**Key Deliverables:**
- Per-graph statistics
- Quad pattern cardinality estimation
- Graph-aware query optimization
- Quad leapfrog triejoin

**Critical Files:**
- `lib/triple_store/statistics.ex` (update)
- `lib/triple_store/sparql/optimizer.ex` (update)

---

### Phase 6: Integration Tests

**Goal:** Validate complete quad functionality.

**Key Deliverables:**
- End-to-end loading/query/update tests
- Real-world scenario tests
- Performance benchmarks
- Migration tests

**Critical Files:**
- `test/triple_store/integration/quad_integration_test.exs` (new)

---

### Phase 7: Reasoning with Named Graphs

**Goal:** OWL 2 RL reasoning with graphs.

**Key Deliverables:**
- Graph-local materialization
- Global materialization option
- Incremental maintenance with graphs
- Graph reasoning configuration

**Critical Files:**
- `lib/triple_store/reasoner/` (update)

---

### Phase 8: Production Hardening

**Goal:** Production-ready quad store.

**Key Deliverables:**
- Performance tuning for quads
- Migration tooling
- Complete documentation
- Monitoring and alerting

**Critical Files:**
- `lib/triple_store/migration.ex` (new)
- Documentation updates

---

## Key Design Decisions

### 1. Number of Indices

**Decision:** 4 indices (GSPO, GPOS, SPOG, POSG)

**Rationale:**
- Covers all common SPARQL patterns
- GSPO/GPOS for `GRAPH <g> { ... }` queries
- SPOG/POSG for cross-graph queries
- Skip OSPG/GOSP (less common, use filtering)

### 2. Default Graph Representation

**Decision:** Use ID = 0 for default graph

**Rationale:**
- Simple special value check
- Never allocated by dictionary
- Efficient comparison
- Default graph implicit in results (not bound)

### 3. Schema Versioning

**Decision:** Separate schema versions, no compatibility

**Rationale:**
- Cleaner break between triple/quad stores
- No performance penalty for compatibility layer
- Migration tool handles conversion
- User can choose when to migrate

### 4. Storage Strategy

**Decision:** Same indices for explicit and derived quads

**Rationale:**
- Simpler query execution (single scan)
- Option: use separate `derived` CF for tracking
- Configuration option for storage strategy

### 5. Reasoning Scope

**Decision:** Support both graph-local and global reasoning

**Rationale:**
- Different use cases require different approaches
- Multi-tenancy: graph-local reasoning
- Unified KB: global reasoning
- Configurable per graph

---

## Migration Path

### From Triple Store to Quad Store

```bash
# 1. Export triple store
mix triple_store.export triples.nt

# 2. Convert to N-Quads (add default graph)
# (manual step or tool)

# 3. Create new quad store
TripleStore.open("./quad_db")

# 4. Load N-Quads
TripleStore.load(quad_db, "quads.nq")
```

**Or use migration tool:**
```elixir
TripleStore.Migration.migrate(triple_db_path, quad_db_path)
```

---

## Performance Considerations

### Storage Overhead

| Metric | Triple Store | Quad Store | Increase |
|--------|--------------|------------|----------|
| Key Size | 24 bytes | 32 bytes | +33% |
| Indices | 3 | 4 | +33% |
| Write Amplification | 3x | 4x | +33% |

### Expected Performance

| Operation | Triple Store | Quad Store (Target) |
|-----------|--------------|---------------------|
| Bulk Load | >100k triples/sec | >50k quads/sec |
| Simple Query | <10ms | <10ms (graph-scoped) |
| Cross-Graph Query | N/A | <100ms |
| Graph Enumeration | N/A | <100ms (100 graphs) |

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| Performance degradation | High | Benchmark tuning, larger block sizes |
| Migration complexity | Medium | Automated migration tool |
| Backward compatibility | Low | No compatibility (clear break) |
| Reasoning complexity | Medium | Flexible configuration |
| Test coverage | Medium | Comprehensive integration tests |

---

## Success Criteria

1. **All SPARQL 1.1 GRAPH features supported**
2. **Performance targets met**
3. **Migration tool works reliably**
4. **Complete test coverage**
5. **Production-ready monitoring**
6. **Complete documentation**

---

## References

- SPARQL 1.1 Query Language: https://www.w3.org/TR/sparql11-query/
- SPARQL 1.1 Update: https://www.w3.org/TR/sparql11-update/
- RDF 1.1 Concepts: https://www.w3.org/TR/rdf11-concepts/
- N-Quads: https://www.w3.org/TR/n-quads/
- TriG: https://www.w3.org/TR/trig/

---

## Document Index

- [Phase 1: Quad Storage Foundation](./phase-01-quad-storage-foundation.md)
- [Phase 2: RDF Integration & Loading](./phase-02-rdf-integration-and-loading.md)
- [Phase 3: SPARQL Query Execution](./phase-03-sparql-query-execution.md)
- [Phase 4: SPARQL UPDATE with Graphs](./phase-04-sparql-update-with-graphs.md)
- [Phase 5: Statistics & Optimization](./phase-05-statistics-and-optimization.md)
- [Phase 6: Integration Tests](./phase-06-integration-tests.md)
- [Phase 7: Reasoning with Named Graphs](./phase-07-reasoning-with-named-graphs.md)
- [Phase 8: Production Hardening](./phase-08-production-hardening.md)
