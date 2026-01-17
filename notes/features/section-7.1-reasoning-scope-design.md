# Section 7.1: Reasoning Scope Design

**Status:** PLANNING
**Phase:** 7 - Reasoning with Named Graphs
**Created:** 2025-01-17
**Feature Branch:** `feature/section-7.1-reasoning-scope-design`

## 1. Problem Statement

The existing OWL 2 RL reasoner operates on a triple store without awareness of named graphs. As we implement quad store support (named graphs), we need to determine how reasoning should interact with the graph dimension. The core question is: **Should reasoning be graph-local, global, or configurable as hybrid?**

### 1.1 Context

- The existing reasoner uses `TripleStore.Index` which operates on `{subject_id, predicate_id, object_id}` triples
- The quad store uses 32-byte keys `{graph_id, subject_id, predicate_id, object_id}` across four indices (GSPO, GPOS, SPOG, POSG)
- Derived facts are stored in a separate `derived` column family using the same SPO key encoding
- Reasoning configuration is stored in `:persistent_term` via `ReasoningConfig`
- Per-graph reasoning state tracking does not currently exist

### 1.2 Requirements

From the Phase 7 planning document, Section 7.1 must address:

**7.1.1 Graph-Local Reasoning**
- Define graph-local reasoning: inferences stay in source graph
- Each graph materialized independently
- No cross-graph inference (default mode)
- Derived quads stored in same graph as explicit
- Use case: multi-tenancy, isolated datasets

**7.1.2 Global Reasoning**
- Define global reasoning: all quads participate
- Single inference closure across all graphs
- Derived quads stored in designated inference graph OR same graph as premises
- Use case: unified knowledge base

**7.1.3 Hybrid Reasoning**
- Define selected graphs for global reasoning
- Define graphs excluded from reasoning
- Allow per-graph reasoning profile configuration
- Support graph-specific rule sets
- Document use cases for each approach

## 2. Solution Overview

### 2.1 Design Decision: Flexible Hybrid Approach

After analysis, we propose a **flexible hybrid approach** where:

1. **Graph-local is the default** - Each graph reasons independently, providing isolation and predictability
2. **Global reasoning is opt-in** - Users can explicitly enable cross-graph reasoning
3. **Per-graph configuration** - Each graph can have its own reasoning profile and scope
4. **TBox sharing** - Schema (TBox) can be shared across graphs for efficiency

This approach satisfies all use cases while maintaining backward compatibility with existing triple-only reasoning.

### 2.2 Key Architectural Changes

1. **Extend `ReasoningConfig`** with graph scope settings
2. **Add `GraphReasoningConfig`** module for per-graph configuration
3. **Extend `PatternMatcher`** to handle quad patterns (4-element instead of 3)
4. **Extend `Rule`** to support graph variable in patterns
5. **Create `GraphScopedReasoner`** module for graph-aware reasoning operations
6. **Adapt `DerivedStore`** to handle quad storage with graph component

### 2.3 Backward Compatibility

- Default graph (ID = 0) reasoning maintains compatibility with triple store
- Existing `ReasoningConfig` API remains unchanged for triple-only operations
- New quad-aware APIs are additive, not breaking changes

## 3. Technical Details

### 3.1 File Locations

**New Modules to Create:**

| File | Purpose |
|------|---------|
| `lib/triple_store/reasoner/graph_scoped_reasoner.ex` | Graph-aware reasoning operations |
| `lib/triple_store/reasoner/graph_reasoning_config.ex` | Per-graph reasoning configuration |
| `lib/triple_store/reasoner/graph_reasoning_status.ex` | Per-graph reasoning status tracking |

**Modules to Modify:**

| File | Changes Required |
|------|------------------|
| `lib/triple_store/reasoner/reasoning_config.ex` | Add `:scope`, `:graph_configs`, `:tbox_graph`, `:inferred_graph` fields |
| `lib/triple_store/reasoner/reasoning_status.ex` | Support per-graph status tracking |
| `lib/triple_store/reasoner/pattern_matcher.ex` | Add quad pattern matching functions |
| `lib/triple_store/reasoner/rule.ex` | Add quad pattern constructors |
| `lib/triple_store/reasoner/derived_store.ex` | Add quad storage support |
| `lib/triple_store/reasoner/semi_naive.ex` | Support graph-scoped lookup/store functions |

**Test Files to Create:**

| File | Purpose |
|------|---------|
| `test/triple_store/reasoner/reasoning_scope_config_test.exs` | Reasoning scope configuration tests |
| `test/triple_store/reasoner/graph_local_reasoning_test.exs` | Graph-local reasoning tests |
| `test/triple_store/reasoner/global_reasoning_test.exs` | Global reasoning tests |
| `test/triple_store/reasoner/hybrid_reasoning_test.exs` | Hybrid reasoning tests |

### 3.2 Data Structures

#### 3.2.1 Extended ReasoningConfig

```elixir
# Extended ReasoningConfig with scope support
defmodule TripleStore.Reasoner.ReasoningConfig do
  @type reasoning_scope :: :local | :global | :hybrid

  defstruct [
    :profile,           # :rdfs | :owl2rl | :custom | :none
    :mode,              # :materialized | :query_time | :hybrid | :none
    :mode_config,       # Mode-specific configuration
    :profile_opts,      # Profile-specific options (rules, exclude)
    :scope,             # :local | :global | :hybrid (NEW)
    :graph_configs,     # %{graph_id => GraphReasoningConfig.t()} (NEW)
    :tbox_graph,        # graph_id for TBox (nil = each graph has own TBox) (NEW)
    :inferred_graph,    # graph_id for global inferences (nil = same as premises) (NEW)
    :created_at
  ]
end
```

#### 3.2.2 GraphReasoningConfig (New Module)

```elixir
defmodule TripleStore.Reasoner.GraphReasoningConfig do
  @moduledoc """
  Per-graph reasoning configuration.

  Each graph can have its own reasoning profile and scope,
  enabling fine-grained control over multi-tenant datasets.
  """

  @type scope :: :local | :global | :none

  defstruct [
    :graph_id,          # Graph identifier (term_id)
    :scope,             # :local (default) | :global | :none
    :profile,           # Override profile for this graph (nil = use default)
    :rules,             # Custom rules for this graph (nil = use profile rules)
    :exclude,           # Rules to exclude for this graph
    :enabled,           # Whether reasoning is enabled for this graph (default: true)
    :tbox_source,       # :self | :shared | :graph_id (where to get TBox from)
    :store_inferred,    # :self | :separate (where to store inferred quads)
    :metadata           # Additional graph-specific metadata
  ]

  @type t :: %__MODULE__{
    graph_id: non_neg_integer() | nil,
    scope: scope(),
    profile: atom() | nil,
    rules: [atom()] | nil,
    exclude: [atom()] | nil,
    enabled: boolean(),
    tbox_source: atom() | non_neg_integer(),
    store_inferred: :self | :separate,
    metadata: map()
  }
end
```

#### 3.2.3 Quad Pattern Extension

```elixir
# Extend Rule module for quad patterns
defmodule TripleStore.Reasoner.Rule do
  # Existing triple pattern:
  # {:pattern, [subject, predicate, object]}

  # New quad pattern:
  # {:quad_pattern, [graph, subject, predicate, object]}

  # Graph position can be:
  # - {:var, "g"} - Variable (matches any graph)
  # - {:bound, graph_id} - Specific graph ID
  # - :default - Default graph (ID = 0)
  # - :all - All graphs (for global reasoning)

  @type quad_pattern :: {:quad_pattern, [rule_term(), rule_term(), rule_term(), rule_term()]}
  @type rule_term :: variable() | iri_term() | blank_node() | literal_term()
end
```

#### 3.2.4 Per-Graph Reasoning Status

```elixir
defmodule TripleStore.Reasoner.GraphReasoningStatus do
  @moduledoc """
  Per-graph reasoning status tracking.

  Tracks reasoning state for each graph independently,
  enabling granular status reporting and rematerialization.
  """

  defstruct [
    :graph_id,
    :config,            # GraphReasoningConfig.t()
    :state,             # :initialized | :materialized | :stale | :error
    :derived_count,     # Number of derived quads in this graph
    :explicit_count,    # Number of explicit quads in this graph
    :last_materialization,
    :materialization_count,
    :error
  ]

  @type t :: %__MODULE__{
    graph_id: non_neg_integer(),
    config: GraphReasoningConfig.t() | nil,
    state: :initialized | :materialized | :stale | :error,
    derived_count: non_neg_integer(),
    explicit_count: non_neg_integer(),
    last_materialization: DateTime.t() | nil,
    materialization_count: non_neg_integer(),
    error: term() | nil
  }
end
```

### 3.3 API Design

#### 3.3.1 ReasoningConfig Extension

```elixir
# Create configuration with scope
{:ok, config} = ReasoningConfig.new(
  profile: :owl2rl,
  mode: :materialized,
  scope: :local  # NEW: default is :local
)

# Create hybrid configuration with per-graph settings
{:ok, config} = ReasoningConfig.new(
  profile: :owl2rl,
  mode: :materialized,
  scope: :hybrid,
  tbox_graph: 0,  # Use default graph for TBox
  inferred_graph: :separate  # Store all inferences in separate graph
)

# Add per-graph configuration
config = ReasoningConfig.add_graph_config(config,
  graph_id: 1,
  scope: :local,
  profile: :rdfs  # Use RDFS only for this graph
)
```

#### 3.3.2 Graph-Scoped Materialization API

```elixir
# Materialize specific graph (graph-local)
{:ok, stats} = GraphScopedReasoner.materialize_graph(db,
  graph_id: 1,
  config: config
)

# Materialize multiple graphs independently
{:ok, stats_list} = GraphScopedReasoner.materialize_graphs(db,
  graph_ids: [1, 2, 3],
  config: config,
  parallel: true
)

# Materialize all graphs globally (cross-graph inference)
{:ok, stats} = GraphScopedReasoner.materialize_all(db,
  config: global_config
)

# Materialize with hybrid configuration
{:ok, per_graph_stats} = GraphScopedReasoner.materialize_hybrid(db,
  config: hybrid_config
)
```

#### 3.3.3 Status Query API

```elixir
# Get reasoning status for specific graph
{:ok, status} = GraphReasoningStatus.get(db, graph_id: 1)

# Get reasoning status for all graphs
{:ok, statuses} = GraphReasoningStatus.list_all(db)

# Get aggregate reasoning status
{:ok, aggregate} = GraphReasoningStatus.aggregate(db)
# => %{total_graphs: 5, materialized: 3, stale: 1, error: 1}

# Check if specific graph needs rematerialization
needs_rematerialization? = GraphReasoningStatus.needs_rematerialization?(db, graph_id: 1)
```

### 3.4 Storage Strategy for Derived Quads

The design supports three storage strategies for derived quads:

#### Option A: Store in Same Graph (Default)
- Derived quads stored in same graph as premises
- Simple query model: all facts in one graph
- Graph-local reasoning is straightforward
- **Tradeoff:** Cannot distinguish explicit from derived without separate tracking

#### Option B: Separate Inferred Graph
- All derived quads stored in designated `:inferred` graph
- Clean separation of explicit and inferred knowledge
- Easy to clear all inferences
- **Tradeoff:** Queries must span graphs, more complex query patterns

#### Option C: Separate Column Family with Graph Tag
- Derived quads in `derived` CF but with full quad encoding including graph
- Combines benefits of A and B
- **Tradeoff:** More complex storage layer, need to track provenance

**Recommended Approach:** Start with Option A (same graph) as default, with Option B available via configuration. The `derived` CF already exists for tracking which quads are inferred.

## 4. Success Criteria

1. **Graph-Local Reasoning**: Each graph can be materialized independently with correct inferences
2. **Global Reasoning**: Cross-graph inferences are computed and stored correctly
3. **Hybrid Configuration**: Flexible per-graph configuration works as expected
4. **Backward Compatibility**: Default graph reasoning works identically to current triple store
5. **Performance**: Graph-local reasoning does not significantly degrade performance
6. **Test Coverage**: Comprehensive tests for all reasoning scope modes

## 5. Implementation Plan

### 5.1 Task Breakdown

**Task 7.1.1: Extend Data Structures**
- Extend `ReasoningConfig` with `:scope`, `:graph_configs`, `:tbox_graph`, `:inferred_graph` fields
- Create `GraphReasoningConfig` module with validation
- Create `GraphReasoningStatus` module for per-graph tracking
- Add quad pattern types and constructors to `Rule` module

**Task 7.1.2: Extend Pattern Matching**
- Add `matches_quad?/2` function to `PatternMatcher`
- Add `quad_to_index_pattern/1` and `index_to_quad_pattern/1`
- Add quad unification functions for rule evaluation
- Support graph variable binding in patterns

**Task 7.1.3: Implement Graph-Local Reasoning**
- Create `GraphScopedReasoner.materialize_graph/4`
- Implement graph-specific fact loading
- Implement graph-specific derived quad storage
- Add per-graph statistics tracking

**Task 7.1.4: Implement Global Reasoning**
- Create `GraphScopedReasoner.materialize_all/3`
- Implement cross-graph pattern matching
- Add TBox sharing logic (single TBox across graphs)
- Implement designated inference graph storage

**Task 7.1.5: Implement Hybrid Reasoning**
- Create `GraphScopedReasoner.materialize_hybrid/3`
- Implement per-graph configuration resolution
- Add graph exclusion/inclusion logic
- Support mixed graph-local and global graphs

**Task 7.1.6: Extend DerivedStore for Quads**
- Add quad key encoding functions
- Add quad pattern lookup functions
- Support graph-scoped derived fact storage
- Maintain compatibility with existing triple operations

**Task 7.1.7: Extend SemiNaive for Quads**
- Add graph-aware lookup function factory
- Add graph-aware store function factory
- Support graph variable in rule evaluation
- Ensure delta computation works with quads

**Task 7.1.8: Public API Integration**
- Extend `TripleStore.materialize/2` with graph options
- Add `TripleStore.materialize_graph/3` API
- Add `TripleStore.reasoning_status/2` with graph filtering
- Update documentation

**Task 7.1.9: Unit Tests**
- Test `ReasoningConfig` with scope options
- Test `GraphReasoningConfig` validation
- Test quad pattern matching
- Test graph-local materialization
- Test global materialization
- Test hybrid configuration
- Test per-graph status tracking

**Task 7.1.10: Integration Tests**
- Test end-to-end graph-local reasoning workflow
- Test cross-graph inference scenarios
- Test TBox sharing across graphs
- Test incremental maintenance with graphs
- Test backward compatibility with triple store

### 5.2 Dependencies

**Internal Dependencies:**
- Quad storage must be implemented (Phase 1) - COMPLETE
- SPARQL query with GRAPH clause must work (Phase 3) - COMPLETE
- Existing reasoner modules must be stable

**External Dependencies:**
- None new; uses existing erlang-rocksdb

### 5.3 Implementation Order

1. Start with data structure extensions (least risk)
2. Add pattern matching support (isolated changes)
3. Implement graph-local reasoning (default mode)
4. Extend DerivedStore and SemiNaive for quads
5. Implement global reasoning (opt-in feature)
6. Implement hybrid reasoning (most complex)
7. Add comprehensive tests
8. Update documentation

## 6. Considerations

### 6.1 Edge Cases

1. **Default Graph Handling**: Default graph (ID = 0) should behave identically to triple-only mode
2. **Empty Graphs**: Graphs with no explicit facts should have no derived facts
3. **Circular Dependencies**: Cross-graph reasoning must avoid infinite loops
4. **Graph Deletion**: Deleting a graph must also delete its derived facts and status
5. **TBox Updates**: TBox changes must trigger appropriate invalidation across dependent graphs

### 6.2 Performance Considerations

1. **Parallel Graph Materialization**: Multiple graphs can be materialized in parallel
2. **TBox Caching**: Shared TBox should be cached to avoid recomputation
3. **Index Selection**: Quad indices (GSPO, GPOS, SPOG, POSG) must be used optimally
4. **Memory Pressure**: Per-graph status tracking should use efficient storage
5. **Delta Computation**: Semi-naive evaluation must work efficiently with quads

### 6.3 Tradeoffs

| Decision | Option A | Option B | Rationale |
|----------|----------|----------|-----------|
| Default Scope | Local | Global | Local is safer default, global is opt-in |
| Derived Storage | Same graph | Separate graph | Same graph is simpler, separate available |
| TBox Handling | Per-graph | Shared | Shared is more efficient, per-graph available |
| Status Tracking | Per-graph | Global only | Per-graph enables granular control |

### 6.4 Future Extensibility

The design should allow for:
- Graph reasoning profiles (custom rule sets per graph)
- Graph-specific rule exclusions
- Priority-based graph reasoning (process important graphs first)
- Incremental reasoning with cross-graph dependencies
- Distributed reasoning across nodes

## 7. Implementation Status

### Task 7.1.1: Extend Data Structures - COMPLETE ✅
- [x] 7.1.1.1 Created `GraphReasoningConfig` module with per-graph configuration
- [x] 7.1.1.2 Created `GraphReasoningStatus` module with per-graph status tracking
- [x] 7.1.1.3 Extended `ReasoningConfig` with scope, graph_configs, tbox_graph, inferred_graph fields
- [x] 7.1.1.4 Added quad pattern types and constructors to `Rule` module

### Task 7.1.2: Extend Pattern Matching - COMPLETE ✅
- [x] 7.1.2.1 Added quad pattern matching functions to `PatternMatcher`
- [x] 7.1.2.2 Added `matches_quad?/2` function
- [x] 7.1.2.3 Added `quad_to_index_pattern/1` and `index_to_quad_pattern/1` functions
- [x] 7.1.2.4 Added quad unification functions for rule evaluation
- [x] 7.1.2.5 Added graph variable binding support

### Task 7.1.3: Implement Graph-Local Reasoning - COMPLETE ✅
- [x] 7.1.3.1 Create `GraphScopedReasoner.materialize_graph/4`
- [x] 7.1.3.2 Implement graph-specific fact loading
- [x] 7.1.3.3 Implement graph-specific derived quad storage
- [x] 7.1.3.4 Add per-graph statistics tracking

### Task 7.1.4: Implement Global Reasoning - COMPLETE ✅
- [x] 7.1.4.1 Create `GraphScopedReasoner.materialize_all/3`
- [x] 7.1.4.2 Implement cross-graph pattern matching
- [x] 7.1.4.3 Add TBox sharing logic
- [x] 7.1.4.4 Implement designated inference graph storage

### Task 7.1.5: Implement Hybrid Reasoning - COMPLETE ✅
- [x] 7.1.5.1 Create `GraphScopedReasoner.materialize_hybrid/3`
- [x] 7.1.5.2 Implement per-graph configuration resolution
- [x] 7.1.5.3 Add graph exclusion/inclusion logic
- [x] 7.1.5.4 Support mixed graph-local and global graphs

### Task 7.1.6: Extend DerivedStore for Quads - COMPLETE ✅
- [x] 7.1.6.1 Add quad key encoding functions
- [x] 7.1.6.2 Add quad pattern lookup functions
- [x] 7.1.6.3 Support graph-scoped derived fact storage

### Task 7.1.7: Extend SemiNaive for Quads - COMPLETE ✅
- [x] 7.1.7.1 Add graph-aware lookup function factory (DerivedStore)
- [x] 7.1.7.2 Add graph-aware store function factory (DerivedStore)
- [x] 7.1.7.3 Support graph variable in rule evaluation (DeltaComputation)
- [x] 7.1.7.4 Ensure delta computation works with quads

### Task 7.1.8: Public API Integration - COMPLETE ✅
- [x] 7.1.8.1 Extend `TripleStore.materialize/2` with graph options
- [x] 7.1.8.2 Add `TripleStore.materialize_graph/3` API
- [x] 7.1.8.3 Add `TripleStore.reasoning_status/2` with graph filtering
- [x] 7.1.8.4 Update documentation

### Task 7.1.9: Unit Tests - PENDING
- [ ] 7.1.9.1 Test `ReasoningConfig` with scope options
- [ ] 7.1.9.2 Test `GraphReasoningConfig` validation
- [ ] 7.1.9.3 Test quad pattern matching
- [ ] 7.1.9.4 Test graph-local materialization
- [ ] 7.1.9.5 Test global materialization
- [ ] 7.1.9.6 Test hybrid configuration
- [ ] 7.1.9.7 Test per-graph status tracking

### Task 7.1.10: Integration Tests - PENDING
- [ ] 7.1.10.1 Test end-to-end graph-local reasoning workflow
- [ ] 7.1.10.2 Test cross-graph inference scenarios
- [ ] 7.1.10.3 Test TBox sharing across graphs
- [ ] 7.1.10.4 Test incremental maintenance with graphs
- [ ] 7.1.10.5 Test backward compatibility with triple store

---

**Document Status:** Implementation in progress (2/10 tasks complete).

This document provides a comprehensive design for Section 7.1: Reasoning Scope Design. The approach maintains backward compatibility while enabling flexible graph-aware reasoning. The design follows existing patterns in the codebase and provides clear extension points for future enhancements.
