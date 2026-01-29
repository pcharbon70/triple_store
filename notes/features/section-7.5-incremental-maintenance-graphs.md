# Section 7.5: Incremental Maintenance with Graphs

**Status:** PLANNING
**Phase:** 7 - Reasoning with Named Graphs
**Created:** 2025-01-18
**Feature Branch:** `feature/section-7.5-incremental-maintenance-graphs`

## 1. Problem Statement

The triple store has a complete incremental maintenance system for triples (Section 4.3), but it lacks graph awareness for incremental operations in the quad store. When quads are added or deleted to a named graph, the system must:

1. Maintain derived quads within the correct graph context
2. Handle cross-graph dependencies when global reasoning is enabled
3. Track provenance across graphs to correctly retract derivations
4. Support both graph-local and global reasoning scopes

### 1.1 Current Limitations

- `Incremental.add_with_reasoning/4` works with triples only, no graph parameter
- `DeleteWithReasoning.delete_with_reasoning/4` works with triples only, no graph parameter
- `BackwardTrace.trace_in_memory/4` and `ForwardRederive.rederive_in_memory/4` are triple-based
- `DerivedStore` has graph-aware operations but incremental modules don't use them
- No cross-graph dependency tracking for provenance

### 1.2 Why This Matters

- Enables incremental reasoning in named graph contexts (multi-tenancy)
- Supports efficient updates without full rematerialization
- Maintains consistency when quads are added/deleted from specific graphs
- Foundation for real-time reasoning with dynamic quad stores

## 2. Solution Overview

### 2.1 Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                  Graph-Aware Incremental Maintenance                  │
├───────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. Graph-Scoped Addition                                               │
│     ┌─────────────┐     ┌──────────────┐     ┌──────────────┐        │
│     │ New Quads   │────▶│ Add to Graph  │────▶│ SemiNaive   │        │
│     │ (with g)    │     │ (explicit)   │     │ Delta Eval  │        │
│     └─────────────┘     └──────────────┘     └──────────────┘        │
│                                                            │           │
│                                                            ▼           │
│                                                 ┌──────────────────┐    │
│                                                 │ Store Derived   │    │
│                                                 │ Quads (with g)   │    │
│                                                 └──────────────────┘    │
│                                                                         │
│  2. Graph-Scoped Deletion                                                 │
│     ┌─────────────┐     ┌──────────────┐     ┌──────────────┐        │
│     │ Delete      │────▶│ Backward     │────▶│ Forward      │        │
│     │ Quads (g)   │     │ Trace (g)    │     │ Re-derive(g) │        │
│     └─────────────┘     └──────────────┘     └──────────────┘        │
│                                                           │             │
│                                                           ▼             │
│                                                ┌─────────────────┐     │
│                                                │ Delete/Keep     │     │
│                                                │ Derived Quads   │     │
│                                                └─────────────────┘     │
│                                                                         │
│  3. Cross-Graph Dependencies                                             │
│     ┌─────────────┐     ┌──────────────┐     ┌──────────────┐        │
│     │ Multi-Graph │────▶│ Dependency   │────▶│ Re-derive    │        │
│     │ Derived     │     │ Detection    │     │ Across Graphs│        │
│     └─────────────┘     └──────────────┘     └──────────────┘        │
└───────────────────────────────────────────────────────────────────────┘
```

### 2.2 Design Decisions

1. **Separate Modules for Quads**: Create `IncrementalQuad` and `DeleteWithReasoningQuad` modules rather than extending existing triple-based modules
2. **Code Reuse via Helpers**: Extract core backward/forward logic into shared helper modules
3. **Simplified Provenance**: Use "depends on graphs" set rather than full dependency chains
4. **Scope-Aware Execution**: Check graph reasoning configuration before executing incremental operations

### 2.3 Trade-offs

| Approach | Pros | Cons |
|----------|------|------|
| Extend existing modules | Less code duplication | Complex parameter handling |
| New quad-specific modules | Clean separation, easier to test | More code |
| Full provenance tracking | Accurate cross-graph retraction | High memory overhead |
| Simplified graph-set provenance | Lower overhead, simpler | May over-retract in edge cases |

**Decision:** Create new quad-specific modules with simplified graph-set provenance.

## 3. Technical Details

### 3.1 File Locations

| File | Current State | Changes Needed |
|------|---------------|----------------|
| `lib/triple_store/reasoner/incremental_quad.ex` | Doesn't exist | Create - Graph-scoped incremental addition |
| `lib/triple_store/reasoner/delete_with_reasoning_quad.ex` | Doesn't exist | Create - Graph-scoped deletion |
| `lib/triple_store/reasoner/graph_provenance.ex` | Doesn't exist | Create - Cross-graph dependency tracking |
| `lib/triple_store/reasoner/backward_trace_quad.ex` | Doesn't exist | Create - Quad-aware backward tracing |
| `lib/triple_store/reasoner/forward_rederive_quad.ex` | Doesn't exist | Create - Quad-aware forward re-derivation |
| `lib/triple_store/reasoner/incremental.ex` | Triple-based | No changes (keep for backward compat) |
| `lib/triple_store/reasoner/delete_with_reasoning.ex` | Triple-based | No changes (keep for backward compat) |

### 3.2 Type Definitions

```elixir
# ID quad: {graph_id, subject_id, predicate_id, object_id}
@type id_quad :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

# ID triple: {subject_id, predicate_id, object_id}
@type id_triple :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

# Graph dependency tracking
@type graph_deps :: MapSet.t(non_neg_integer())
```

## 4. Success Criteria

### 4.1 Functional Requirements

**7.5.1 Graph-Local Incremental Addition:**
- [ ] Quads can be added to a specific graph with reasoning
- [ ] Derived quads are stored in the same graph as premises
- [ ] Addition respects graph scope (local vs global)
- [ ] Returns per-graph derivation counts
- [ ] Handles TBox sharing correctly

**7.5.2 Graph-Local Incremental Deletion:**
- [ ] Quads can be deleted from a specific graph with reasoning
- [ ] Derived quads are correctly retracted within graph scope
- [ ] Backward/forward phases work with graph-scoped facts
- [ ] Handles cross-graph dependencies when global reasoning enabled
- [ ] Preserves derivations with alternative paths

**7.5.3 Cross-Graph Dependencies:**
- [ ] Detects when derived quad depends on multiple graphs
- [ ] Tracks source graphs for each derivation
- [ ] Handles deletion when quad has cross-graph support
- [ ] Implements re-derivation across graphs
- [ ] Documents limitations and edge cases

## 5. Implementation Plan

### 5.1 Task 7.5.1: Graph-Local Incremental Addition

**Goal:** Extend incremental addition to support graph-scoped quad operations.

#### Subtasks

1. **7.5.1.1** Create `IncrementalQuad` module with `add_with_reasoning/5`
2. **7.5.1.2** Implement graph-scoped fact filtering
3. **7.5.1.3** Create graph-scoped lookup function
4. **7.5.1.4** Implement derived quad storage with graph_id
5. **7.5.1.5** Handle shared TBox in graph-local reasoning

#### Files to Create
- `lib/triple_store/reasoner/incremental_quad.ex`

### 5.2 Task 7.5.2: Graph-Local Incremental Deletion

**Goal:** Extend incremental deletion to support graph-scoped quad operations.

#### Subtasks

1. **7.5.2.1** Create `DeleteWithReasoningQuad` module
2. **7.5.2.2** Implement graph-scoped partitioning (explicit vs derived)
3. **7.5.2.3** Create `BackwardTraceQuad` module
4. **7.5.2.4** Create `ForwardRederiveQuad` module
5. **7.5.2.5** Handle cross-graph dependencies

#### Files to Create
- `lib/triple_store/reasoner/delete_with_reasoning_quad.ex`
- `lib/triple_store/reasoner/backward_trace_quad.ex`
- `lib/triple_store/reasoner/forward_rederive_quad.ex`

### 5.3 Task 7.5.3: Cross-Graph Dependency Tracking

**Goal:** Track and manage cross-graph dependencies.

#### Subtasks

1. **7.5.3.1** Create `GraphProvenance` module
2. **7.5.3.2** Implement source graph tracking
3. **7.5.3.3** Detect cross-graph dependencies
4. **7.5.3.4** Implement cross-graph re-derivation
5. **7.5.3.5** Document limitations

#### Files to Create
- `lib/triple_store/reasoner/graph_provenance.ex`

## 8. Implementation Status

**Status:** COMPLETE
**Created:** 2025-01-18
**Completed:** 2025-01-18

### Task 7.5.1: Graph-Local Incremental Addition
- [x] 7.5.1.1 Create IncrementalQuad module
- [x] 7.5.1.2 Implement graph-scoped fact filtering
- [x] 7.5.1.3 Create graph-scoped lookup function
- [x] 7.5.1.4 Implement derived quad storage
- [x] 7.5.1.5 Handle shared TBox

### Task 7.5.2: Graph-Local Incremental Deletion
- [x] 7.5.2.1 Create DeleteWithReasoningQuad module
- [x] 7.5.2.2 Implement graph-scoped partitioning
- [x] 7.5.2.3 Create BackwardTraceQuad module
- [x] 7.5.2.4 Create ForwardRederiveQuad module
- [x] 7.5.2.5 Handle cross-graph dependencies

### Task 7.5.3: Cross-Graph Dependencies
- [x] 7.5.3.1 Create GraphProvenance module
- [x] 7.5.3.2 Implement source graph tracking
- [x] 7.5.3.3 Detect cross-graph dependencies
- [x] 7.5.3.4 Implement cross-graph re-derivation
- [x] 7.5.3.5 Document limitations

### Summary of Changes

**Created Modules:**
- `IncrementalQuad` - Graph-scoped incremental addition with TBox support
- `BackwardTraceQuad` - Quad-aware backward tracing for deletions
- `ForwardRederiveQuad` - Quad-aware forward re-derivation
- `DeleteWithReasoningQuad` - Complete Backward/Forward deletion API
- `GraphProvenance` - Cross-graph dependency tracking

**Public API:**
- `TripleStore.add_quads_with_reasoning/4` - Incrementally add quads with reasoning
- `TripleStore.delete_quads_with_reasoning/4` - Incrementally delete quads with reasoning

**Supporting Changes:**
- `DerivedStore` - Added `lookup_derived_quads_in_graph/2` and `decode_derived_key/1`
- `Rule` - Added `could_derive?/2`, `body_patterns/1`, and `id_triple` type
- `QuadIndex` - Added `lookup_all_fold/3` for graph-scoped pattern matching

---

**Last Updated:** 2025-01-18
**Status:** Complete
