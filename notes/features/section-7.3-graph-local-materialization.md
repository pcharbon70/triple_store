# Section 7.3: Graph-Local Materialization

**Status:** PLANNING
**Phase:** 7 - Reasoning with Named Graphs
**Created:** 2025-01-18
**Feature Branch:** `feature/section-7.3-graph-local-materialization`

## 1. Problem Statement

The current reasoning system materializes all derived triples to a single derived store without graph awareness. Section 7.1 introduced the infrastructure for graph-scoped reasoning (GraphScopedReasoner, GraphReasoningConfig) and Section 7.2 added quad pattern support to rules. However, the core materialization logic still needs to be implemented to support:

1. **Graph-local reasoning** - Materialize derived facts within a specific graph
2. **TBox sharing** - Use schema (TBox) from one graph for reasoning in another graph
3. **Per-graph rematerialization** - Re-materialize a single graph without affecting others
4. **Multi-graph coordination** - Efficiently materialize multiple graphs with shared TBox

### 1.1 Current Limitations

- `GraphScopedReasoner.materialize_graph/2` exists but is not fully implemented
- No TBox extraction and caching mechanism
- No coordination for shared TBox across multiple graphs
- DerivedStore has graph-aware operations but materialization doesn't use them

### 1.2 Why This Matters

- Enables per-graph reasoning where each graph has its own inferred closure
- Allows schema (TBox) to be defined once and shared across many data graphs
- Supports incremental rematerialization of individual graphs
- Foundation for Section 7.4 (Global Materialization)

## 2. Solution Overview

### 2.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Graph-Local Materialization                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. TBox Extraction                                              │
│     ┌─────────────┐     ┌──────────────┐                        │
│     │ TBox Source │────▶│ TBox Extract │─▶ TBox Facts            │
│     │   (Graph N) │     │  (Schema)    │   (Cached)              │
│     └─────────────┘     └──────────────┘                        │
│                                                                   │
│  2. Per-Graph Materialization                                     │
│     ┌─────────────┐     ┌──────────────┐     ┌─────────────┐   │
│     │ Target Graph │────▶│ Load Facts   │────▶│ SemiNaive   │   │
│     │             │     │ + Shared TBox│     │ Evaluation  │   │
│     └─────────────┘     └──────────────┘     └─────────────┘   │
│                                                                   │
│  3. Derived Storage                                               │
│     ┌─────────────┐                                               │
│     │ DerivedStore│─▶ Derived Quads (with graph_id)              │
│     └─────────────┘                                               │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Design Decisions

1. **TBox Extraction**: Create a dedicated `TBoxExtractor` module to identify and cache schema triples
2. **Shared TBox Cache**: Extend `TBoxCache` to support per-graph TBox with fingerprint-based invalidation
3. **Graph-Scoped Lookup**: Create lookup functions that union TBox facts with graph-specific facts
4. **Parallel Graph Processing**: Use `Task.async_stream` for concurrent graph materialization

## 3. Technical Details

### 3.1 File Locations

| File | Current State | Changes Needed |
|------|---------------|----------------|
| `lib/triple_store/reasoner/tbox_extractor.ex` | Doesn't exist | Create - TBox extraction and caching |
| `lib/triple_store/reasoner/graph_scoped_reasoner.ex` | Skeleton functions | Implement materialization logic |
| `lib/triple_store/reasoner/tbox_cache.ex` | Basic cache | Extend for graph-aware TBox |
| `lib/triple_store/reasoner/derived_store.ex` | Has graph ops | May need minor updates |

### 3.2 New Module: TBoxExtractor

```elixir
defmodule TripleStore.Reasoner.TBoxExtractor do
  @moduledoc """
  Extracts TBox (schema) facts from a graph for use in reasoning.

  TBox facts include:
  - rdfs:subClassOf declarations
  - rdfs:subPropertyOf declarations
  - rdfs:domain and rdfs:range declarations
  - owl:TransitiveProperty, owl:SymmetricProperty declarations
  - Property characteristics
  """

  @type tbox_facts :: MapSet.set(DerivedStore.id_quad())

  @doc "Extracts TBox facts from a graph"
  @spec extract_tbox(db_context(), non_neg_integer()) :: {:ok, tbox_facts()} | {:error, term()}

  @doc "Extracts TBox with caching"
  @spec extract_with_cache(db_context(), non_neg_integer(), atom()) :: {:ok, tbox_facts()}

  @doc "Computes fingerprint for TBox change detection"
  @spec tbox_fingerprint(db_context(), non_neg_integer()) :: {:ok, String.t()} | {:error, term()}
end
```

### 3.3 Dependencies

- Elixir 1.18+ for Task.async_stream
- Existing modules:
  - `TripleStore.Reasoner.GraphScopedReasoner` - Core coordination
  - `TripleStore.Reasoner.DerivedStore` - Derived quad storage
  - `TripleStore.Reasoner.SemiNaive` - Fixpoint evaluation
  - `TripleStore.Reasoner.RuleCompiler` - Rule compilation with graph context
  - `TripleStore.QuadIndex` - Quad index operations

## 4. Success Criteria

### 4.1 Functional Requirements

- [ ] Graph-local materialization produces derived quads in source graph
- [ ] TBox can be extracted from a designated graph
- [ ] Shared TBox is cached and reused across multiple graphs
- [ ] Per-graph rematerialization doesn't affect other graphs
- [ ] Multiple graphs can be materialized in parallel

### 4.2 Non-Functional Requirements

- No performance regression for single-graph reasoning
- TBox extraction completes in < 100ms for typical schemas
- Parallel graph materialization shows near-linear speedup

### 4.3 Measurable Outcomes

- All existing tests pass without modification
- New tests for graph-local materialization pass
- Benchmark shows < 10% overhead for TBox sharing

## 5. Implementation Plan

### 5.1 Task 7.3.1: TBox Extractor Module

**Goal:** Create module to extract and cache TBox facts.

#### Subtasks

1. **7.3.1.1** Create `tbox_extractor.ex` module skeleton
2. **7.3.1.2** Implement `extract_tbox/3` to identify schema triples
3. **7.3.1.3** Implement `tbox_fingerprint/2` for change detection
4. **7.3.1.4** Implement `extract_with_cache/4` with TBoxCache integration
5. **7.3.1.5** Add unit tests for TBox extraction

#### Files to Create/Modify
- **Create:** `lib/triple_store/reasoner/tbox_extractor.ex`
- **Modify:** `lib/triple_store/reasoner/tbox_cache.ex` (extend for graph TBox)

#### Testing
- Tests for identifying TBox predicates (rdfs:subClassOf, etc.)
- Tests for fingerprint computation
- Tests for cache hit/miss scenarios

### 5.2 Task 7.3.2: Graph-Local Materialization Core

**Goal:** Implement core graph-local materialization with TBox support.

#### Subtasks

1. **7.3.2.1** Implement `do_materialize_graph/5` with TBox loading
2. **7.3.2.2** Create TBox-aware lookup function (union of TBox + graph facts)
3. **7.3.2.3** Implement derived quad storage in source graph
4. **7.3.2.4** Add graph-local fact loading helper
5. **7.3.2.5** Track per-graph materialization status

#### Files to Modify
- `lib/triple_store/reasoner/graph_scoped_reasoner.ex`

#### Testing
- Test single graph materialization
- Test graph with shared TBox
- Test derived quads stored in correct graph
- Test status tracking

### 5.3 Task 7.3.3: Multi-Graph Materialization

**Goal:** Coordinate materialization across multiple graphs.

#### Subtasks

1. **7.3.3.1** Implement `materialize_graphs/3` with parallel execution
2. **7.3.3.2** Add TBox sharing optimization
3. **7.3.3.3** Implement per-graph rematerialization
4. **7.3.3.4** Add progress tracking for multi-graph scenarios

#### Files to Modify
- `lib/triple_store/reasoner/graph_scoped_reasoner.ex`

#### Testing
- Test parallel graph materialization
- Test TBox sharing across graphs
- Test selective rematerialization

### 5.4 Task 7.3.4: Public API Integration

**Goal:** Expose graph-local materialization through TripleStore API.

#### Subtasks

1. **7.3.4.1** Add `TripleStore.materialize_graph/3` for single graph
2. **7.3.4.2** Add `TripleStore.materialize_graphs/3` for multiple graphs
3. **7.3.4.3** Integrate with existing `materialize/2` options
4. **7.3.4.4** Update documentation

#### Files to Modify
- `lib/triple_store/reasoning.ex` (public API)

#### Testing
- Integration tests through public API
- Test options for graph_id, tbox_graph, parallel

### 5.5 Task 7.3.5: Integration and Testing

**Goal:** Comprehensive test coverage and integration.

#### Subtasks

1. **7.3.5.1** Unit tests for TBoxExtractor
2. **7.3.5.2** Unit tests for graph-local materialization
3. **7.3.5.3** Integration tests with real ontologies
4. **7.3.5.4** Performance benchmarks

#### Files to Create
- `test/triple_store/reasoner/section_7_3_graph_local_materialization_test.exs`

## 6. Testing Strategy

### 6.1 Unit Tests

**TBoxExtractor Tests**
- Identifying TBox predicates correctly
- Fingerprint computation consistency
- Cache key generation

**GraphScopedReasoner Tests**
- Single graph materialization
- TBox sharing scenarios
- Derived quad storage locations

### 6.2 Integration Tests

**End-to-end scenarios**
- Materialize graph 1 with TBox from graph 0
- Materialize multiple graphs with shared TBox
- Rematerialize single graph
- Parallel graph materialization

### 6.3 Performance Tests

- TBox extraction time
- Multi-graph parallel speedup
- Memory usage with shared TBox

## 7. Notes and Considerations

### 7.1 Edge Cases

1. **Empty TBox** - What if source graph has no schema triples?
   - Resolution: Materialize with only ABox facts (no TBox inference)

2. **Circular TBox sharing** - Graph A's TBox depends on Graph B's TBox
   - Resolution: Detect and error, or limit to single-level sharing

3. **Concurrent rematerialization** - Multiple threads rematerializing same graph
   - Resolution: Use GraphReasoningStatus locks

### 7.2 Performance Considerations

- TBox extraction should be done once per TBox graph
- Shared TBox enables significant speedup for multi-graph scenarios
- Parallel materialization limited by database connections

### 7.3 Future Work

- **Section 7.4**: Global Materialization (derive across all graphs)
- Incremental TBox updates
- TBox dependency resolution

## 8. Implementation Status

**Status:** IMPLEMENTATION COMPLETE (Testing Pending)
**Created:** 2025-01-18
**Completed:** 2025-01-18

### Task 7.3.1: TBox Extractor Module
- [x] 7.3.1.1 Create tbox_extractor.ex module skeleton
- [x] 7.3.1.2 Implement extract_tbox/3
- [x] 7.3.1.3 Implement tbox_fingerprint/2
- [x] 7.3.1.4 Implement extract_with_cache/4 (simplified - no caching initially)
- [ ] 7.3.1.5 Add unit tests (pending)

### Task 7.3.2: Graph-Local Materialization Core
- [x] 7.3.2.1 Implement do_materialize_graph/5
- [x] 7.3.2.2 Create TBox-aware lookup function
- [x] 7.3.2.3 Implement derived quad storage
- [x] 7.3.2.4 Add graph-local fact loading
- [x] 7.3.2.5 Track per-graph status

### Task 7.3.3: Multi-Graph Materialization
- [x] 7.3.3.1 Implement materialize_graphs/3 (already existed)
- [x] 7.3.3.2 Add TBox sharing optimization
- [x] 7.3.3.3 Implement per-graph rematerialization
- [x] 7.3.3.4 Add progress tracking

### Task 7.3.4: Public API Integration
- [x] 7.3.4.1 Add TripleStore.materialize_graph/3 (already existed)
- [x] 7.3.4.2 Add TripleStore.materialize_graphs/3
- [x] 7.3.4.3 Integrate with existing materialize/2
- [x] 7.3.4.4 Update documentation

### Task 7.3.5: Integration and Testing
- [ ] 7.3.5.1 Unit tests for TBoxExtractor
- [ ] 7.3.5.2 Unit tests for graph-local materialization
- [ ] 7.3.5.3 Integration tests
- [ ] 7.3.5.4 Performance benchmarks

## 9. Implementation Summary

### Files Created
- `lib/triple_store/reasoner/tbox_extractor.ex` - TBox extraction module

### Files Modified
- `lib/triple_store/reasoner/namespaces.ex` - Added missing OWL predicate functions
- `lib/triple_store/reasoner/graph_scoped_reasoner.ex` - Added TBox-aware materialization
- `lib/triple_store.ex` - Added `materialize_graphs/3` public API function

### Key Changes
1. **TBoxExtractor Module**: Extracts TBox facts from a graph using predicate filtering
2. **TBox-Aware Lookup**: `make_tbox_aware_lookup_fn/3` creates lookup functions that union TBox facts with graph facts
3. **Graph-Local Materialization**: `do_materialize_graph/5` now loads TBox from designated source graph
4. **Multi-Graph API**: `TripleStore.materialize_graphs/3` for parallel graph materialization

---

**Last Updated:** 2025-01-18
**Status:** Implementation complete, testing pending
