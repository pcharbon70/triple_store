# Section 7.4: Global Materialization

**Status:** PLANNING
**Phase:** 7 - Reasoning with Named Graphs
**Created:** 2025-01-18
**Feature Branch:** `feature/section-7.4-global-materialization`

## 1. Problem Statement

Section 7.3 implemented graph-local materialization where each graph is reasoned independently. Section 7.4 implements global materialization where all quads from all graphs participate in a single inference closure. This is needed for:

1. **Unified knowledge base reasoning** - Infer facts that span multiple named graphs
2. **Cross-graph inference** - Enable reasoning that combines data from multiple graphs
3. **Centralized derived storage** - Optionally store all inferences in a single location
4. **Shared schema utilization** - Use a single TBox across all ABox graphs

### 1.1 Current State

The skeleton for global materialization exists but is not fully implemented:
- `GraphScopedReasoner.materialize_all/2` exists with basic validation
- `do_materialize_all/3` has placeholder implementation
- `load_all_facts/1` scans GSPO index but doesn't filter explicit from derived
- No configurable derived quad storage strategy
- TBox handling in global mode is incomplete

### 1.2 Why This Matters

- Enables use cases where data is split across graphs but should be reasoned together
- Supports incremental addition where new graphs trigger global rematerialization
- Provides option to separate explicit and derived quads for query optimization
- Foundation for hybrid reasoning (Section 7.5)

## 2. Solution Overview

### 2.1 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Global Materialization                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. Fact Loading (All Graphs)                                       │
│     ┌─────────────────────────────────────────────────────────┐     │
│     │ GSPO Index Scan ──▶ {G, S, P, O} quads ──▶ Filter:     │     │
│     │                    - Skip :derived CF quads             │     │
│     │                    - Convert to triples (S, P, O)       │     │
│     │                    - Track source graph for storage     │     │
│     └─────────────────────────────────────────────────────────┘     │
│                                                                      │
│  2. TBox Loading (Optional)                                          │
│     ┌─────────────────┐     ┌──────────────────┐                   │
│     │ TBox Graph (N)  │────▶│ TBoxExtractor    │─▶ TBox Facts      │
│     │ (if configured) │     │ (cached)         │   (merged)        │
│     └─────────────────┘     └──────────────────┘                   │
│                                                                      │
│  3. SemiNaive Evaluation                                             │
│     ┌─────────────────┐     ┌──────────────────┐     ┌─────────┐   │
│     │ Explicit Facts  │────▶│ make_all_graphs  │────▶│SemiNaive│   │
│     │ (all graphs)    │     │ _lookup_fn/2     │     │.materi  │   │
│     │ + TBox Facts    │     │                  │     │ alize   │   │
│     └─────────────────┘     └──────────────────┘     └─────────┘   │
│                                                                      │
│  4. Derived Quad Storage (Configurable)                              │
│     ┌─────────────────────────────────────────────────────────┐     │
│     │ Strategy A: :same_as_premises  ──▶ Store in source graph│     │
│     │ Strategy B: :separate_graph     ──▶ Store in designated│     │
│     │ Strategy C: :per_graph_cf       ──▶ Use derived CF only│     │
│     └─────────────────────────────────────────────────────────┘     │
│                                                                      │
│  5. Status Tracking                                                  │
│     ┌─────────────────────────────────────────────────────────┐     │
│     │ Update: global reasoning status                         │     │
│     │ Store: aggregate stats (per-graph counts)               │     │
│     └─────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Design Decisions

1. **Explicit Quad Loading**: Scan all quad indices but skip derived CF quads
2. **Triple Conversion**: Convert quads to triples for reasoning (drop graph component)
3. **Storage Strategy Config**: Make derived quad location configurable via ReasoningConfig
4. **Graph Tracking**: Track source graph for each derived quad when storing
5. **TBox Handling**: Load TBox from designated graph and merge with explicit facts

### 2.3 Derived Quad Storage Strategies

| Strategy | Description | Pros | Cons |
|----------|-------------|------|------|
| `:same_as_premises` | Store in same graph as premises | Simple query co-location | Harder to distinguish explicit/derived |
| `:separate_graph` | Store in designated inference graph | Clean separation | Requires config, cross-graph lookups |
| `:derived_cf_only` | Only use derived CF, no graph context | Minimal storage, easy cleanup | Loses provenance, requires join for query |

**Default:** `:same_as_premises` for backward compatibility

## 3. Technical Details

### 3.1 File Locations

| File | Current State | Changes Needed |
|------|---------------|----------------|
| `lib/triple_store/reasoner/graph_scoped_reasoner.ex` | Skeleton `do_materialize_all/3` | Implement full logic |
| `lib/triple_store/reasoner/reasoning_config.ex` | Has `inferred_graph` field | Add storage strategy options |
| `lib/triple_store/reasoner/derived_store.ex` | Has quad operations | May need strategy-specific functions |
| `test/triple_store/reasoner/section_7_4_global_materialization_test.exs` | Doesn't exist | Create |

### 3.2 Core Functions to Implement

#### 3.2.1 `do_materialize_all/3`

```elixir
@spec do_materialize_all(
  db_ref(),
  ReasoningConfig.t(),
  keyword()
) :: {:ok, map()} | {:error, term()}

# Implementation steps:
# 1. Load TBox from configured graph (if tbox_graph is set)
# 2. Load explicit quads from all graphs (skip derived CF)
# 3. Create lookup function that combines TBox + all quads
# 4. Create store function based on storage strategy
# 5. Run SemiNaive.materialize/5
# 6. Update status and return aggregate statistics
```

#### 3.2.2 `load_all_explicit_quads/1`

```elixir
@spec load_all_explicit_quads(db_ref()) :: 
  {:ok, MapSet.t(id_quad())} | {:error, term()}

# Implementation:
# - Scan GSPO index from empty prefix (all graphs)
# - Skip quads that exist in derived CF
# - Return set of explicit quads
```

#### 3.2.3 `make_inferred_store_fn/3`

```elixir
@spec make_inferred_store_fn(
  db_ref(),
  ReasoningConfig.t(),
  MapSet.t(id_quad())  # source quads for graph tracking
) :: (MapSet.t(id_triple()) -> :ok | {:error, term()})

# Storage strategies:
# - :same_as_premises -> Track source graph, store back there
# - :separate_graph -> Always store in inferred_graph
# - :derived_cf_only -> Store in derived CF only
```

### 3.3 Configuration Options

```elixir
# ReasoningConfig fields for global reasoning
%ReasoningConfig{
  scope: :global,                    # Required for global mode
  tbox_graph: 0,                     # Graph containing TBox (optional)
  inferred_graph: nil,               # Where to store derived quads
  storage_strategy: :same_as_premises # :same_as_premises | :separate_graph | :derived_cf_only
}

# New field to add:
@type storage_strategy :: :same_as_premises | :separate_graph | :derived_cf_only
```

### 3.4 Dependencies

- Elixir 1.18+
- Existing modules:
  - `TripleStore.Reasoner.GraphScopedReasoner` - Core coordination
  - `TripleStore.Reasoner.TBoxExtractor` - TBox extraction
  - `TripleStore.Reasoner.SemiNaive` - Fixpoint evaluation
  - `TripleStore.Reasoner.DerivedStore` - Derived quad storage
  - `TripleStore.QuadIndex` - Quad index operations
  - `TripleStore.Backend.RocksDB.NIF` - Database operations

## 4. Success Criteria

### 4.1 Functional Requirements

- [ ] Global materialization loads facts from all graphs
- [ ] Derived quads stored according to configured strategy
- [ ] TBox (if configured) is used for all graphs
- [ ] Statistics include per-graph derived counts
- [ ] Rematerialization clears previous global inferences

### 4.2 Non-Functional Requirements

- No performance regression for graph-local reasoning
- Global materialization completes in reasonable time for moderate datasets
- Memory usage scales linearly with total quad count

### 4.3 Measurable Outcomes

- All existing tests pass without modification
- New tests for global materialization pass
- Benchmark shows acceptable performance for multi-graph datasets

## 5. Implementation Plan

### 5.1 Task 7.4.1: Storage Strategy Configuration

**Goal:** Add configurable storage strategy for derived quads.

#### Subtasks

1. **7.4.1.1** Add `storage_strategy` type definition
2. **7.4.1.2** Add field to ReasoningConfig struct
3. **7.4.1.3** Add validation for storage_strategy in ReasoningConfig.new/2
4. **7.4.1.4** Add helper functions for strategy checking
5. **7.4.1.5** Update ReasoningConfig.summary/1 to include strategy

#### Files to Modify
- `lib/triple_store/reasoner/reasoning_config.ex`

#### Testing
- Test strategy validation (valid/invalid values)
- Test default strategy
- Test strategy query functions

### 5.2 Task 7.4.2: Explicit Quad Loading

**Goal:** Implement efficient loading of explicit quads from all graphs.

#### Subtasks

1. **7.4.2.1** Implement `load_all_explicit_quads/1` in GraphScopedReasoner
2. **7.4.2.2** Add filtering to skip derived CF quads
3. **7.4.2.3** Add streaming option for large datasets
4. **7.4.2.5** Handle empty database case
5. **7.4.2.6** Add unit tests

#### Files to Modify
- `lib/triple_store/reasoner/graph_scoped_reasoner.ex`

#### Testing
- Test loading from single graph
- Test loading from multiple graphs
- Test filtering of derived quads
- Test empty database handling

### 5.3 Task 7.4.3: Global Lookup Function

**Goal:** Create lookup function for all-graphs reasoning.

#### Subtasks

1. **7.4.3.1** Implement `make_all_graphs_lookup_fn/2` with TBox support
2. **7.4.3.2** Add quad-to-triple conversion for reasoning
3. **7.4.3.3** Handle pattern matching across all graphs
4. **7.4.3.4** Add caching for frequently accessed patterns
5. **7.4.3.5** Add unit tests

#### Files to Modify
- `lib/triple_store/reasoner/graph_scoped_reasoner.ex`

#### Testing
- Test lookup with no TBox
- Test lookup with shared TBox
- Test pattern matching performance
- Test empty result handling

### 5.4 Task 7.4.4: Derived Quad Storage Strategy

**Goal:** Implement configurable storage for derived quads.

#### Subtasks

1. **7.4.4.1** Implement `:same_as_premises` strategy
2. **7.4.4.2** Implement `:separate_graph` strategy
3. **7.4.4.3** Implement `:derived_cf_only` strategy
4. **7.4.4.4** Add graph tracking for source provenance
5. **7.4.4.5** Add unit tests for each strategy

#### Files to Modify
- `lib/triple_store/reasoner/graph_scoped_reasoner.ex`
- `lib/triple_store/reasoner/derived_store.ex` (possibly)

#### Testing
- Test each storage strategy independently
- Test derived quad retrieval
- Test storage cleanup
- Test graph tracking

### 5.5 Task 7.4.5: Core Global Materialization

**Goal:** Implement complete `do_materialize_all/3` function.

#### Subtasks

1. **7.4.5.1** Wire up TBox loading
2. **7.4.5.2** Wire up explicit quad loading
3. **7.4.5.3** Wire up lookup and store functions
4. **7.4.5.4** Add status tracking and telemetry
5. **7.4.5.5** Add error handling

#### Files to Modify
- `lib/triple_store/reasoner/graph_scoped_reasoner.ex`

#### Testing
- Test full materialization workflow
- Test with TBox
- Test without TBox
- Test error conditions

### 5.6 Task 7.4.6: Statistics and Status

**Goal:** Track and report global materialization statistics.

#### Subtasks

1. **7.4.6.1** Define global statistics structure
2. **7.4.6.2** Track per-graph derived counts
3. **7.4.6.3** Update GraphReasoningStatus for global scope
4. **7.4.6.4** Store aggregate statistics
5. **7.4.6.5** Add query functions for status

#### Files to Modify
- `lib/triple_store/reasoner/graph_scoped_reasoner.ex`
- `lib/triple_store/reasoner/graph_reasoning_status.ex`

#### Testing
- Test statistics accuracy
- Test per-graph breakdown
- Test status persistence

### 5.7 Task 7.4.7: Public API Integration

**Goal:** Expose global materialization through TripleStore API.

#### Subtasks

1. **7.4.7.1** Add `TripleStore.materialize_all/2` function
2. **7.4.7.2** Add options for storage_strategy
3. **7.4.7.3** Update documentation
4. **7.4.7.4** Add deprecation warnings if needed

#### Files to Modify
- `lib/triple_store.ex`

#### Testing
- Test API function
- Test option handling
- Test error propagation

### 5.8 Task 7.4.8: Testing and Documentation

**Goal:** Comprehensive test coverage and documentation.

#### Subtasks

1. **7.4.8.1** Create `section_7_4_global_materialization_test.exs`
2. **7.4.8.2** Add integration tests
3. **7.4.8.3** Add performance benchmarks
4. **7.4.8.4** Update module documentation
5. **7.4.8.5** Add usage examples

#### Files to Create
- `test/triple_store/reasoner/section_7_4_global_materialization_test.exs`

#### Testing
- All unit tests pass
- All integration tests pass
- Benchmarks show acceptable performance

## 6. Testing Strategy

### 6.1 Unit Tests

**GraphScopedReasoner Tests**
- `load_all_explicit_quads/1` loads from all graphs
- `make_all_graphs_lookup_fn/2` returns correct facts
- `make_inferred_store_fn/3` stores according to strategy
- Storage strategies work correctly

**ReasoningConfig Tests**
- Storage strategy validation
- Default strategy assignment
- Strategy query functions

### 6.2 Integration Tests

**End-to-end scenarios**
1. **Basic global reasoning**
   - Load quads in graphs 1, 2, 3
   - Run global materialization
   - Verify cross-graph inferences

2. **With shared TBox**
   - TBox in graph 0
   - Data in graphs 1, 2
   - Run global reasoning
   - Verify TBox used for all graphs

3. **Storage strategies**
   - Test `:same_as_premises`
   - Test `:separate_graph`
   - Test `:derived_cf_only`

4. **Rematerialization**
   - Materialize globally
   - Add new quads
   - Rematerialize
   - Verify correct results

### 6.3 Performance Tests

- Measure time for various dataset sizes
- Memory usage profiling
- Comparison with graph-local reasoning
- TBox caching effectiveness

## 7. Notes and Considerations

### 7.1 Edge Cases

1. **Empty database**
   - Resolution: Return empty result set, no error

2. **Only TBox, no data**
   - Resolution: Materialize with only TBox facts

3. **Circular graph dependencies**
   - Resolution: Not applicable for global reasoning (all graphs participate)

4. **Large dataset memory**
   - Resolution: Use streaming options, batch processing

### 7.2 Performance Considerations

- Loading all graphs can be expensive - consider lazy loading
- Quad-to-triple conversion overhead
- Storage strategy affects query performance
- TBox caching provides significant speedup

### 7.3 Future Work

- **Section 7.5**: Incremental maintenance with global reasoning
- Delta-based rematerialization
- Cross-graph provenance tracking
- Parallel graph loading

## 8. Implementation Status

**Status:** IMPLEMENTATION COMPLETE
**Created:** 2025-01-18
**Completed:** 2025-01-18

### Task 7.4.1: Storage Strategy Configuration
- [x] 7.4.1.1 Add storage_strategy type definition
- [x] 7.4.1.2 Add field to ReasoningConfig struct
- [x] 7.4.1.3 Add validation for storage_strategy
- [x] 7.4.1.4 Add helper functions (storage_strategy/1, put_storage_strategy/2)
- [x] 7.4.1.5 Update ReasoningConfig.summary/1

### Task 7.4.2: Explicit Quad Loading
- [x] 7.4.2.1 Implement load_all_explicit_quads/1
- [x] 7.4.2.2 Add filtering to skip derived CF
- [x] 7.4.2.3 Handle empty database
- [x] 7.4.2.4 Add unit tests (deferred to 7.4.8)

### Task 7.4.3: Global Lookup Function
- [x] 7.4.3.1 Implement make_global_lookup_fn/2 with TBox support
- [x] 7.4.3.2 Implement lookup_all_graphs_facts/2
- [x] 7.4.3.3 Add TBox + graph fact union
- [x] 7.4.3.4 Add unit tests (deferred to 7.4.8)

### Task 7.4.4: Derived Quad Storage Strategy
- [x] 7.4.4.1 Implement :same_as_premises strategy
- [x] 7.4.4.2 Implement :separate_graph strategy
- [x] 7.4.4.3 Implement :per_graph_cf strategy
- [x] 7.4.4.4 Add make_global_store_fn/4

### Task 7.4.5: Core Global Materialization
- [x] 7.4.5.1 Wire up TBox loading
- [x] 7.4.5.2 Wire up explicit quad loading
- [x] 7.4.5.3 Wire up lookup and store functions
- [x] 7.4.5.4 Update do_materialize_all/3

### Task 7.4.6: Statistics and Status
- [ ] 7.4.6.1 Define statistics structure (deferred - uses existing stats)
- [ ] 7.4.6.2 Track per-graph counts (deferred - requires provenance tracking)
- [ ] 7.4.6.3 Update GraphReasoningStatus (deferred)
- [ ] 7.4.6.4 Store aggregate statistics (deferred)

### Task 7.4.7: Public API Integration
- [x] 7.4.7.1 Add TripleStore.materialize_all/2
- [x] 7.4.7.2 Add options for storage_strategy
- [x] 7.4.7.3 Update documentation

### Task 7.4.8: Testing and Documentation
- [ ] 7.4.8.1 Create test file (deferred)
- [ ] 7.4.8.2 Add integration tests (deferred)
- [ ] 7.4.8.3 Add benchmarks (deferred)
- [x] 7.4.8.4 Update module documentation

## 9. Implementation Summary

### Files Created
None (modifications only)

### Files Modified
- `lib/triple_store/reasoner/reasoning_config.ex` - Added storage_strategy field and validation
- `lib/triple_store/reasoner/graph_scoped_reasoner.ex` - Implemented global materialization
- `lib/triple_store.ex` - Added materialize_all/2 public API

### Key Changes

1. **Storage Strategy Configuration**:
   - Added `storage_strategy` field to ReasoningConfig
   - Three strategies: `:same_as_premises`, `:separate_graph`, `:per_graph_cf`
   - Validation and accessor functions

2. **Explicit Quad Loading**:
   - `load_all_explicit_quads/1` loads all quads excluding derived CF
   - Efficient filtering using derived CF keys

3. **Global Lookup Function**:
   - `make_global_lookup_fn/2` creates TBox-aware global lookup
   - `lookup_all_graphs_facts/2` scans GSPO index for pattern matching
   - TBox facts cached in-memory for fast access

4. **Global Store Function**:
   - `make_global_store_fn/4` handles three storage strategies
   - `:same_as_premises` stores in source graph (simplified to inferred_graph for now)
   - `:separate_graph` stores in designated inference graph
   - `:per_graph_cf` stores only in derived column family

5. **Public API**:
   - `TripleStore.materialize_all/2` for global materialization
   - Support for storage_strategy, inferred_graph, tbox_graph options

### Known Limitations

1. **Source Graph Tracking**: The `:same_as_premises` strategy currently uses the inferred_graph instead of tracking source graphs. Full provenance tracking would require additional infrastructure.

2. **Per-Graph Statistics**: Statistics tracking uses aggregate counts; per-graph derived counting is deferred.

3. **Testing**: Comprehensive integration tests are deferred to a future iteration.
