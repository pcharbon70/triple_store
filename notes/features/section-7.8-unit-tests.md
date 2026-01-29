# Section 7.8: Unit Tests for Phase 7 (Reasoning with Named Graphs)

**Feature Planning Document**

**Status**: Complete
**Priority**: High
**Dependencies**: Sections 7.1-7.7 complete and merged to `quad` branch
**Estimated Effort**: 3-4 days
**Actual Effort**: 2 days
**Completed**: 2025-01-18

---

## Executive Summary

Section 7.8 completes the test coverage for Phase 7 by adding comprehensive unit tests for quad-aware reasoning functionality. This section ensures that all graph-aware reasoning features have proper test coverage, including rule pattern matching, graph-local materialization, global reasoning, incremental maintenance, derived store operations, and configuration management.

### Objectives

1. Provide complete unit test coverage for quad-aware reasoning modules
2. Test graph isolation and cross-graph reasoning scenarios
3. Validate incremental reasoning operations in graph context
4. Ensure derived quad tracking works correctly
5. Verify configuration persistence and retrieval
6. Maintain backward compatibility with triple-only reasoning

### Success Criteria

- All new modules have unit test coverage >90%
- All test categories (7.8.1-7.8.6) have comprehensive tests
- Tests validate both local and global reasoning scopes
- Integration tests validate end-to-end graph reasoning workflows
- All tests pass with `mix test`
- No regressions in existing triple reasoning tests

---

## Current State Analysis

### Existing Test Coverage

**Completed Quad Tests:**
- `section_7_2_quad_pattern_test.exs` (465 lines)
  - Rule pattern extension tests (Task 7.2.1)
  - Quad pattern matching tests (Task 7.2.2)
  - Rule compilation for quads (Task 7.2.3)

- `incremental_quad_test.exs` (400 lines)
  - Basic incremental addition functionality
  - Graph-scoped reasoning
  - Multiple graphs handling
  - Deduplication
  - Rule application
  - Iteration tracking
  - Edge cases

- `delete_with_reasoning_quad_test.exs` (427 lines)
  - API contract tests
  - Options handling
  - Preview functionality
  - Type checking
  - Stats structure validation

- `forward_rederive_quad_test.exs` (not reviewed, assumed present)
- `backward_trace_quad_test.exs` (not reviewed, assumed present)

- `graph_reasoning_config_test.exs` (439 lines)
  - Configuration creation and validation
  - Query functions (participates?, local?, global?)
  - Update functions
  - Persistent term storage
  - TBox and storage configuration

**Integration Tests:**
- `graph_scoped_reasoning_integration_test.exs`
  - Graph-local reasoning workflow
  - Global reasoning with cross-graph inference
  - Hybrid reasoning with mixed scopes
  - TBox sharing across graphs
  - Per-graph status tracking

**Existing Triple Tests (for comparison):**
- `derived_store_test.exs` - Triple-based derived store tests
- `incremental_integration_test.exs` - Triple incremental reasoning
- `materialization_integration_test.exs` - Triple materialization
- `reasoning_config_test.exs` - Triple reasoning configuration
- `reasoning_status_test.exs` - Triple reasoning status

### Modules Under Test

**Core Quad Reasoning Modules:**
1. `TripleStore.Reasoner.Rule` - Quad rule support
2. `TripleStore.Reasoner.PatternMatcher` - Quad pattern matching
3. `TripleStore.Reasoner.RuleCompiler` - Quad rule compilation
4. `TripleStore.Reasoner.GraphScopedReasoner` - Graph-aware materialization
5. `TripleStore.Reasoner.IncrementalQuad` - Graph-local incremental reasoning
6. `TripleStore.Reasoner.DeleteWithReasoningQuad` - Graph-local deletion
7. `TripleStore.Reasoner.ForwardRederiveQuad` - Graph-local rederivation
8. `TripleStore.Reasoner.BackwardTraceQuad` - Graph-local backward tracing
9. `TripleStore.Reasoner.DerivedStore` - Quad-derived fact tracking
10. `TripleStore.Reasoner.GraphReasoningConfig` - Per-graph configuration
11. `TripleStore.Reasoner.GraphReasoningStatus` - Per-graph status tracking
12. `TripleStore.Reasoner.GraphProvenance` - Cross-graph provenance
13. `TripleStore.Reasoner.DerivationProvenance` - Derivation tracking

### Coverage Gaps

Based on Section 7.8 requirements analysis:

**Missing Test Categories:**

1. **7.8.1 Rule Pattern Tests** - PARTIALLY COMPLETE
   - ✅ Quad pattern in rule body matches (section_7_2_quad_pattern_test.exs)
   - ✅ Quad pattern in rule head instantiates (section_7_2_quad_pattern_test.exs)
   - ✅ Graph variable binding (section_7_2_quad_pattern_test.exs)
   - ✅ Graph-specific rule specialization (section_7_2_quad_pattern_test.exs)
   - ✅ Rule compilation for quad store (section_7_2_quad_pattern_test.exs)
   - **Status**: Coverage complete in section_7_2_quad_pattern_test.exs

2. **7.8.2 Graph-Local Materialization Tests** - MISSING
   - ❌ Test materialize_graph derives inferences in graph
   - ❌ Test materialize_graph doesn't affect other graphs
   - ❌ Test materialize_graphs processes each graph independently
   - ❌ Test materialize_default works on default graph only
   - ❌ Test parallel graph materialization produces correct results
   - **File**: `section_7_8_2_graph_local_materialization_test.exs` (NEW)

3. **7.8.3 Global Materialization Tests** - PARTIALLY COMPLETE
   - ✅ Test materialize_all derives across all graphs (graph_scoped_reasoning_integration_test.exs)
   - ❌ Test derived quads stored in correct location
   - ❌ Test TBox shared across graphs
   - ✅ Test global reasoning finds cross-graph inferences (graph_scoped_reasoning_integration_test.exs)
   - ❌ Test reasoning status reports correctly for global reasoning
   - **File**: `section_7_8_3_global_materialization_test.exs` (NEW -补充 integration tests)

4. **7.8.4 Incremental Maintenance Tests** - PARTIALLY COMPLETE
   - ✅ Test add_with_reasoning in graph derives locally (incremental_quad_test.exs)
   - ❌ Test delete_with_reasoning in graph retracts locally (only API contract tested)
   - ❌ Test cross-graph dependencies handled correctly
   - ❌ Test incremental addition with global reasoning
   - ❌ Test incremental deletion with global reasoning
   - **File**: `section_7_8_4_incremental_maintenance_test.exs` (NEW -补充 integration tests)

5. **7.8.5 Derived Store Tests** - MISSING
   - ❌ Test derived quad stored correctly (derived_store_test.exs is triple-only)
   - ❌ Test is_derived? identifies derived quads (need quad version)
   - ❌ Test derived quad lookup works (need quad version)
   - ❌ Test clear_derived removes only derived quads (need quad version)
   - ❌ Test provenance tracking works (graph_provenance_test.exs exists)
   - **File**: `section_7_8_5_derived_store_quad_test.exs` (NEW)

6. **7.8.6 Configuration Tests** - COMPLETE
   - ✅ Test graph reasoning profile configuration (graph_reasoning_config_test.exs)
   - ✅ Test TBox graph configuration (graph_reasoning_config_test.exs)
   - ✅ Test reasoning scope per graph (graph_reasoning_config_test.exs)
   - ❌ Test reasoning status per graph (need dedicated test file)
   - ✅ Test configuration persistence (graph_reasoning_config_test.exs)
   - **File**: `graph_reasoning_status_test.exs` (NEW)

---

## Detailed Implementation Plan

### 7.8.1 Rule Pattern Tests - ✅ COMPLETE

**Status**: Already covered in `section_7_2_quad_pattern_test.exs`

**Coverage**:
- Task 7.2.1.1-7.2.1.5: Rule pattern extension tests
- Task 7.2.2.1-7.2.2.5: Quad pattern matching tests
- Task 7.2.3.1-7.2.3.5: Rule compilation for quads

**No additional work required.**

---

### 7.8.2 Graph-Local Materialization Tests

**File**: `test/triple_store/reasoner/section_7_8_2_graph_local_materialization_test.exs`

**Estimated Lines**: 600-700

**Purpose**: Comprehensive unit tests for graph-local materialization functions in `GraphScopedReasoner`

**Test Categories**:

#### 7.8.2.1 Single Graph Materialization

```elixir
describe "materialize_graph/2" do
  test "derives inferences within target graph only"
  test "stores derived quads in same graph as explicit"
  test "respects graph_id option"
  test "respects tbox_graph option for TBox sharing"
  test "returns statistics with correct counts"
  test "handles empty graph"
  test "handles graph with no derivable facts"
  test "uses configured reasoning profile"
end
```

**Test Data**:
- TBox in graph 0: class hierarchy (Person > Student > GradStudent)
- Data in graph 1: instance data (alice rdf:type Student)
- Expected: alice rdf:type Person derived in graph 1 only

#### 7.8.2.2 Graph Isolation

```elixir
describe "graph isolation" do
  test "materialize_graph doesn't affect other graphs"
  test "derived quads stay in source graph"
  test "multiple graphs maintain separate derived facts"
  test "deletion in one graph doesn't affect other graphs"
  test "graph-local reasoning uses graph-specific rules"
end
```

**Test Data**:
- Graph 1: University A schema and data
- Graph 2: University B schema and data
- Verify: No cross-graph derivations

#### 7.8.2.3 Multiple Graph Materialization

```elixir
describe "materialize_graphs/2" do
  test "processes multiple graphs sequentially"
  test "processes multiple graphs in parallel when configured"
  test "returns per-graph statistics"
  test "handles empty graph list"
  test "handles graphs with different profiles"
  test "aggregates statistics correctly"
end
```

**Test Data**:
- Graphs 1, 2, 3: Different datasets
- Materialize all with same TBox
- Verify each graph materialized independently

#### 7.8.2.4 Default Graph Materialization

```elixir
describe "materialize_default/2" do
  test "materializes default graph (graph_id: 0)"
  test "uses @default_graph_id"
  test "maintains backward compatibility with triple reasoning"
  test "stores derived facts in default graph"
  test "handles default graph with TBox sharing"
end
```

**Test Data**:
- Default graph: Traditional triple store data
- Verify: Works like triple reasoning but with graph position

#### 7.8.2.5 Parallel Materialization

```elixir
describe "parallel materialization" do
  test "parallel option enables concurrent processing"
  test "parallel processing produces correct results"
  test "parallel processing is faster than sequential"
  test "handles errors in individual graphs gracefully"
  test "respects max_concurrency option"
end
```

**Test Data**:
- 10 graphs with independent data
- Materialize with parallel: true
- Verify: Same results as sequential, faster execution

---

### 7.8.3 Global Materialization Tests

**File**: `test/triple_store/reasoner/section_7_8_3_global_materialization_test.exs`

**Estimated Lines**: 500-600

**Purpose**: Tests for global reasoning across all graphs

**Test Categories**:

#### 7.8.3.1 Cross-Graph Materialization

```elixir
describe "materialize_all/2" do
  test "derives inferences across all graphs"
  test "participating graphs determined by scope configuration"
  test "excludes graphs with scope: :none"
  test "loads explicit quads from all participating graphs"
  test "applies rules regardless of graph source"
  test "returns global statistics"
end
```

**Test Data**:
- Graph 0: Shared TBox
- Graph 1: University A data
- Graph 2: University B data
- Graph 3: scope: :none (should not participate)
- Global reasoning: All graphs except 3 participate

#### 7.8.3.2 Derived Quad Storage Location

```elixir
describe "derived quad storage" do
  test "stores in same graph as premises when store_inferred: :self"
  test "stores in separate inference graph when store_inferred: :separate"
  test "respects per-graph store_inferred configuration"
  test "tracks provenance for cross-graph derivations"
  test "handles derived quads with premises from multiple graphs"
end
```

**Test Data**:
- Graph 1: Person rdf:type Student
- Graph 2: Student rdfs:subClassOf Person
- Derived: Person rdf:type Person
- Verify storage location based on configuration

#### 7.8.3.3 TBox Sharing

```elixir
describe "TBox sharing in global reasoning" do
  test "uses designated TBox graph for all graphs"
  test "graphs with tbox_source: :shared use graph 0"
  test "graphs with specific tbox_graph use that graph"
  test "TBox quads not duplicated across graphs"
  test "TBox changes affect all dependent graphs"
end
```

**Test Data**:
- Graph 0: Shared TBox (Person > Student > Professor)
- Graph 1: Uses shared TBox, has alice rdf:type Student
- Graph 2: Uses shared TBox, has bob rdf:type Professor
- Both graphs derive using shared schema

#### 7.8.3.4 Cross-Graph Inference

```elixir
describe "cross-graph inference" do
  test "derives facts across graph boundaries"
  test "finds inferences requiring data from multiple graphs"
  test "handles chain rules across graphs"
  test "respects graph variable in rules"
  test "global reasoning finds all possible inferences"
end
```

**Test Data**:
- Graph 1: alice ex:knows bob
- Graph 2: bob ex:knows charlie
- Rule: ex:knows is transitive
- Derived: alice ex:knows charlie (cross-graph)

#### 7.8.3.5 Reasoning Status

```elixir
describe "reasoning status for global materialization" do
  test "reports status for each participating graph"
  test "aggregates statistics across all graphs"
  test "tracks global materialization state"
  test "marks participating graphs as materialized"
  test "handles partial failures gracefully"
end
```

**Test Data**:
- Materialize all with 5 graphs
- Check status for each graph
- Verify aggregated statistics

---

### 7.8.4 Incremental Maintenance Tests

**File**: `test/triple_store/reasoner/section_7_8_4_incremental_maintenance_test.exs`

**Estimated Lines**: 600-700

**Purpose**: Integration tests for incremental reasoning with graphs

**Test Categories**:

#### 7.8.4.1 Graph-Local Incremental Addition

```elixir
describe "add_with_reasoning - graph-local" do
  test "derives facts within target graph only"
  test "stores derived quads in same graph"
  test "uses graph-local TBox when configured"
  test "returns per-graph derivation counts"
  test "updates graph reasoning status"
  test "handles graph_id option correctly"
end
```

**Note**: Basic functionality tested in `incremental_quad_test.exs`. These tests focus on integration scenarios.

**Test Data**:
- Add alice rdf:type Student to graph 1
- Derive alice rdf:type Person in graph 1
- Verify no derivation in other graphs

#### 7.8.4.2 Graph-Local Incremental Deletion

```elixir
describe "delete_with_reasoning - graph-local" do
  test "retracts derived facts within target graph"
  test "uses backward tracing within graph scope"
  test "uses forward rederivation within graph scope"
  test "returns per-graph deletion statistics"
  test "updates graph reasoning status"
  test "doesn't affect other graphs"
end
```

**Note**: API contract tested in `delete_with_reasoning_quad_test.exs`. These tests verify actual deletion behavior.

**Test Data**:
- Graph 1: alice rdf:type Student (explicit), alice rdf:type Person (derived)
- Delete alice rdf:type Student from graph 1
- Verify alice rdf:type Person also deleted from graph 1
- Verify graph 2 unaffected

#### 7.8.4.3 Cross-Graph Dependencies

```elixir
describe "cross-graph dependencies" do
  test "detects derived quads with premises from multiple graphs"
  test "tracks provenance across graphs"
  test "handles deletion when quad has cross-graph support"
  test "implements rederivation across graphs"
  test "marks graphs as stale when dependencies change"
end
```

**Test Data**:
- Global reasoning enabled
- Derived quad depends on facts from graphs 1 and 2
- Delete supporting fact from graph 1
- Verify rederivation attempt
- Check if derived quad kept (other support) or deleted

#### 7.8.4.4 Global Incremental Addition

```elixir
describe "add_with_reasoning - global scope" do
  test "derives facts across all participating graphs"
  test "stores derived quads per configuration"
  test "finds cross-graph inferences"
  test "updates status for all affected graphs"
  test "handles TBox changes affecting multiple graphs"
end
```

**Test Data**:
- Global reasoning enabled
- Add fact to graph 1
- Derive inferences across graphs 1, 2, 3
- Verify stored per configuration

#### 7.8.4.5 Global Incremental Deletion

```elixir
describe "delete_with_reasoning - global scope" do
  test "retracts derived facts across all graphs"
  test "handles cross-graph backward tracing"
  test "implements cross-graph forward rederivation"
  test "updates status for all affected graphs"
  test "handles partial failures gracefully"
end
```

**Test Data**:
- Global reasoning with cross-graph derivations
- Delete explicit fact from graph 1
- Verify derived facts retracted from all graphs
- Verify rederivation where alternative support exists

---

### 7.8.5 Derived Store Tests

**File**: `test/triple_store/reasoner/section_7_8_5_derived_store_quad_test.exs`

**Estimated Lines**: 400-500

**Purpose**: Tests for quad-derived fact tracking

**Test Categories**:

#### 7.8.5.1 Quad Storage

```elixir
describe "DerivedStore - quad operations" do
  test "inserts derived quad"
  test "inserts multiple derived quads"
  test "checks if quad is derived"
  test "deletes derived quad"
  test "counts derived quads per graph"
  test "clears derived quads for specific graph"
  test "clears all derived quads"
end
```

**Test Data**:
- Store derived quads in graphs 1, 2, 3
- Verify stored correctly
- Count per graph
- Clear specific graph
- Clear all

#### 7.8.5.2 Quad Lookup

```elixir
describe "derived quad lookup" do
  test "finds derived quads by pattern"
  test "finds derived quads by graph"
  test "finds all derived quads for a graph"
  test "handles pattern with wildcards"
  test "returns empty for non-existent quads"
end
```

**Test Data**:
- Derived quads: {1, alice, rdf:type, Person}, {2, bob, rdf:type, Student}
- Lookup by pattern: {_, _, rdf:type, _}
- Lookup by graph: 1
- Verify correct results

#### 7.8.5.3 Derived Status

```elixir
describe "is_derived? - quad version" do
  test "returns true for derived quads"
  test "returns false for explicit quads"
  test "returns false for non-existent quads"
  test "checks across all graphs"
  test "checks within specific graph"
end
```

**Test Data**:
- Explicit: {1, alice, rdf:type, Student}
- Derived: {1, alice, rdf:type, Person}
- Verify is_derived? correctly identifies

#### 7.8.5.4 Clear Operations

```elixir
describe "clear derived operations" do
  test "clear_derived removes only derived quads"
  test "clear_derived preserves explicit quads"
  test "clear_derived_graph removes derived quads for graph"
  test "clear_derived_graph preserves explicit quads in graph"
  test "clear_all_derived removes all derived quads"
  test "clear_all_derived preserves all explicit quads"
end
```

**Test Data**:
- Mixed explicit and derived quads in graphs 1, 2
- Clear derived
- Verify explicit quads preserved
- Verify derived quads removed

#### 7.8.5.5 Provenance Tracking

```elixir
describe "provenance tracking" do
  test "records rule that produced derived quad"
  test "records source quads for derivation"
  test "tracks cross-graph derivations"
  test "explains inference for derived quad"
  test "provides derivation chain"
end
```

**Note**: May be covered in existing `graph_provenance_test.exs`. Verify and补充 if needed.

**Test Data**:
- Derived quad: alice rdf:type Person
- Source: alice rdf:type Student + Student rdfs:subClassOf Person
- Rule: cax-sco
- Verify provenance tracked

---

### 7.8.6 Configuration Tests

**File**: `test/triple_store/reasoner/graph_reasoning_status_test.exs`

**Estimated Lines**: 400-500

**Purpose**: Tests for per-graph reasoning status tracking

**Test Categories**:

#### 7.8.6.1 Status Creation and Validation

```elixir
describe "GraphReasoningStatus.new/1" do
  test "creates status with graph_id"
  test "creates status with config"
  test "creates status with explicit_count"
  test "validates graph_id"
  test "returns error for invalid graph_id"
end

describe "GraphReasoningStatus.new!/1" do
  test "returns status for valid options"
  test "raises for invalid options"
end
```

#### 7.8.6.2 State Transitions

```elixir
describe "state transitions" do
  test "initializes in :initialized state"
  test "transitions to :materialized on successful materialization"
  test "transitions to :stale when data changes"
  test "transitions to :error on materialization failure"
  test "transitions from :stale to :materialized on rematerialization"
end
```

#### 7.8.6.3 Materialization Tracking

```elixir
describe "materialization tracking" do
  test "records materialization statistics"
  test "increments materialization count"
  test "updates last_materialization timestamp"
  test "tracks derived_count"
  test "tracks explicit_count"
end
```

#### 7.8.6.4 Stale Detection

```elixir
describe "stale detection" do
  test "marks as stale when explicit count changes"
  test "marks as stale when TBox changes"
  test "needs_rematerialization? returns true for stale"
  test "needs_rematerialization? returns false for materialized"
  test "needs_rematerialization? returns false for :none scope"
end
```

#### 7.8.6.5 Persistent Term Storage

```elixir
describe "persistent term storage" do
  test "stores status for graph"
  test "loads status for graph"
  test "returns error for non-existent status"
  test "loads default for non-existent status with load!"
  test "stores all statuses for multiple graphs"
  test "loads all statuses for multiple graphs"
  test "lists all stored status keys"
  test "clears all statuses"
end
```

#### 7.8.6.6 Error Handling

```elixir
describe "error handling" do
  test "records error on materialization failure"
  test "maintains error state across load/store"
  test "clears error on successful rematerialization"
  test "provides error reason"
end
```

---

## Implementation Strategy

### Phase 1: Preparation (0.5 days)

1. **Review existing test structure**
   - Study section_7_2_quad_pattern_test.exs patterns
   - Review incremental_quad_test.exs structure
   - Understand test helper utilities

2. **Create test helper utilities**
   - Add quad-specific assertion helpers
   - Create graph test data generators
   - Build mock database helpers for integration tests

3. **Set up test infrastructure**
   - Ensure test database setup/teardown works
   - Verify test isolation (async vs non-async)
   - Create test data fixtures

### Phase 2: Core Unit Tests (1.5 days)

**Priority Order**:

1. **7.8.2 Graph-Local Materialization** (0.5 day)
   - Most critical functionality
   - Foundation for other tests
   - Create `section_7_8_2_graph_local_materialization_test.exs`

2. **7.8.3 Global Materialization** (0.5 day)
   - Completes materialization testing
   - Create `section_7_8_3_global_materialization_test.exs`

3. **7.8.6 Configuration/Status** (0.5 day)
   - Configuration management
   - Create `graph_reasoning_status_test.exs`

### Phase 3: Incremental and Derived Store Tests (1 day)

1. **7.8.4 Incremental Maintenance** (0.5 day)
   - Integration scenarios for incremental operations
   - Create `section_7_8_4_incremental_maintenance_test.exs`

2. **7.8.5 Derived Store** (0.5 day)
   - Quad-specific derived store tests
   - Create `section_7_8_5_derived_store_quad_test.exs`

### Phase 4: Integration and Regression (0.5 days)

1. **Run full test suite**
   - Verify all new tests pass
   - Check no regressions in existing tests

2. **Update test documentation**
   - Document test coverage
   - Update planning document

3. **Code review and refinement**
   - Review test quality
   - Add missing edge cases
   - Improve test clarity

---

## Test File Structure

### Standard Test File Template

```elixir
defmodule TripleStore.Reasoner.Section7_8_X_CategoryTest do
  @moduledoc """
  Tests for Section 7.8.X: [Category Name].

  This test suite validates:
  - [Specific functionality tested]
  - [Graph scenarios covered]
  - [Edge cases handled]

  ## Test Coverage

  - Task 7.8.X.1: [Test description]
  - Task 7.8.X.2: [Test description]
  - ...
  """

  use TripleStore.ReasonerTestCase
  # or: use ExUnit.Case, async: true (for pure unit tests)

  alias TripleStore.Reasoner.{...}

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp iri(suffix), do: {:iri, @ex <> suffix}
  defp quad(g, s, p, o), do: {g, s, p, o}
  # ... other helpers

  # ============================================================================
  # Test Categories
  # ============================================================================

  describe "category name" do
    test "test description" do
      # Test implementation
    end
  end
end
```

### Test Helper Utilities

**Location**: `test/support/reasoner_test_helpers.ex` (or similar)

**Functions to Add**:

```elixir
defmodule TripleStore.ReasonerTestHelpers do
  # Quad builders
  def quad(g, s, p, o), do: {g, s, p, o}

  # Assertion helpers
  def assert_derived_in_graph?(db, quad, graph_id)
  def assert_not_derived_in_graph?(db, quad, graph_id)
  def assert_graph_isolated?(db, graph1, graph2)

  # Graph test data generators
  def generate_graph_facts(graph_id, :university)
  def generate_graph_facts(graph_id, :simple_hierarchy)
  def generate_tbox_facts(graph_id)

  # Statistics helpers
  def count_quads_in_graph(db, graph_id)
  def count_derived_in_graph(db, graph_id)
  def count_explicit_in_graph(db, graph_id)
end
```

---

## Success Metrics

### Coverage Metrics

| Module | Target Coverage | Current Coverage | Gap |
|--------|----------------|------------------|-----|
| Rule | 95% | 95% (section_7_2) | 0% |
| PatternMatcher | 95% | 95% (section_7_2) | 0% |
| RuleCompiler | 90% | 90% (section_7_2) | 0% |
| GraphScopedReasoner | 90% | 40% | 50% |
| IncrementalQuad | 90% | 70% | 20% |
| DeleteWithReasoningQuad | 90% | 60% | 30% |
| ForwardRederiveQuad | 85% | 60% | 25% |
| BackwardTraceQuad | 85% | 60% | 25% |
| DerivedStore | 90% | 50% | 40% |
| GraphReasoningConfig | 95% | 95% | 0% |
| GraphReasoningStatus | 90% | 0% | 90% |
| GraphProvenance | 80% | 70% | 10% |
| DerivationProvenance | 80% | 70% | 10% |

**Overall Target**: 85% average coverage across all quad reasoning modules

### Test Count Metrics

| Test Category | Test Count | Estimated Lines |
|--------------|------------|-----------------|
| 7.8.1 Rule Pattern | ✅ Complete | 465 |
| 7.8.2 Graph-Local | ~50 tests | 600-700 |
| 7.8.3 Global | ~40 tests | 500-600 |
| 7.8.4 Incremental | ~50 tests | 600-700 |
| 7.8.5 Derived Store | ~35 tests | 400-500 |
| 7.8.6 Configuration | ~30 tests | 400-500 |
| **Total** | **~205 tests** | **~3,000 lines** |

### Quality Metrics

- **Test Clarity**: All tests have clear descriptions and docstrings
- **Test Independence**: Tests can run in any order (except integration tests)
- **Edge Case Coverage**: Each function tested for normal, edge, and error cases
- **Documentation**: Each test file has @moduledoc explaining coverage
- **Maintainability**: Tests use helper functions to reduce duplication

---

## Risk Assessment

### High Risk Areas

1. **Graph Isolation (7.8.2.2)**
   - **Risk**: Cross-graph contamination in tests
   - **Mitigation**: Use separate database instances per test
   - **Validation**: Verify no data leakage between tests

2. **Parallel Processing (7.8.2.5)**
   - **Risk**: Race conditions in parallel tests
   - **Mitigation**: Use deterministic test data, verify sequential equivalence
   - **Validation**: Run tests multiple times to catch non-determinism

3. **Cross-Graph Dependencies (7.8.4.3)**
   - **Risk**: Complex provenance tracking hard to test
   - **Mitigation**: Start with simple cases, build complexity gradually
   - **Validation**: Manual verification of provenance chains

4. **Global Reasoning State (7.8.3)**
   - **Risk**: Global state affects multiple tests
   - **Mitigation**: Isolate global reasoning tests, clean up persistent_term
   - **Validation**: Run tests in random order

### Medium Risk Areas

1. **Performance Tests (7.8.2.5)**
   - **Risk**: Performance assertions flaky on CI
   - **Mitigation**: Use coarse-grained assertions, avoid micro-benchmarks
   - **Validation**: Allow timing tolerance

2. **Error Handling (7.8.6.6)**
   - **Risk**: Hard to simulate all error conditions
   - **Mitigation**: Use mocks for error injection
   - **Validation**: Manual testing of error scenarios

### Low Risk Areas

1. **Configuration Tests (7.8.6)**
   - Pure data structures, easy to test
   - Minimal external dependencies

2. **Rule Pattern Tests (7.8.1)**
   - Already complete, working well

---

## Dependencies and Prerequisites

### Code Dependencies

1. **Sections 7.1-7.7 Complete** ✅
   - All quad reasoning modules implemented
   - Integration tests passing
   - Code merged to `quad` branch

2. **Test Infrastructure**
   - `TripleStore.ReasonerTestCase` available
   - Test database helpers working
   - Mock infrastructure in place

3. **Helper Modules**
   - `TripleStore.ReasonerTestHelpers` (may need creation)
   - Test data generators
   - Assertion helpers

### External Dependencies

- Elixir 1.18+
- ExUnit test framework
- RocksDB (for integration tests)
- Existing test utilities

---

## Deliverables

### Primary Deliverables

1. **Test Files** (6 new files):
   - `section_7_8_2_graph_local_materialization_test.exs`
   - `section_7_8_3_global_materialization_test.exs`
   - `section_7_8_4_incremental_maintenance_test.exs`
   - `section_7_8_5_derived_store_quad_test.exs`
   - `graph_reasoning_status_test.exs`
   - (Optional) `reasoner_test_helpers.ex`

2. **Test Infrastructure**:
   - Test helper utilities
   - Test data fixtures
   - Mock adapters

3. **Documentation**:
   - This planning document
   - Test coverage report
   - Updated phase planning document

### Secondary Deliverables

1. **CI/CD Updates**:
   - Ensure new tests run in CI
   - Update test coverage reporting

2. **Development Guide**:
   - How to run new tests
   - How to add new graph reasoning tests

---

## Timeline

### Week 1: Core Tests

- **Day 1**: Preparation and infrastructure
  - Review existing tests
  - Create test helpers
  - Set up infrastructure

- **Day 2-3**: Graph-local materialization tests
  - Implement 7.8.2 tests
  - Run and validate

- **Day 4-5**: Global materialization tests
  - Implement 7.8.3 tests
  - Run and validate

### Week 2: Remaining Tests

- **Day 1**: Configuration/status tests
  - Implement 7.8.6 tests
  - Run and validate

- **Day 2-3**: Incremental and derived store tests
  - Implement 7.8.4 tests
  - Implement 7.8.5 tests
  - Run and validate

- **Day 4**: Integration and regression
  - Run full test suite
  - Fix regressions
  - Update documentation

- **Day 5**: Buffer and refinement
  - Code review
  - Additional edge cases
  - Final validation

---

## Open Questions

1. **Test Database Strategy**
   - Should we use in-memory databases for speed?
   - How to handle parallel test database access?
   - Decision: Use separate temp database per test for isolation

2. **Mock Strategy**
   - Should we mock NIF operations?
   - How much to mock vs integration test?
   - Decision: Unit tests with mocks, integration tests with real DB

3. **Async Tests**
   - Which tests can be async?
   - How to handle persistent_term in async tests?
   - Decision: Configuration tests non-async, others async where possible

4. **Performance Testing**
   - Should we include performance assertions?
   - How to handle variability across machines?
   - Decision: Coarse-grained performance checks only

---

## Appendix: Test Checklist

### 7.8.2 Graph-Local Materialization Checklist

- [ ] Single graph materialization works
- [ ] Graph isolation maintained
- [ ] Multiple graphs processed independently
- [ ] Default graph materialization works
- [ ] Parallel processing produces correct results
- [ ] TBox sharing works correctly
- [ ] Statistics reported correctly
- [ ] Status tracked per graph
- [ ] Error handling works
- [ ] Empty graph handled gracefully

### 7.8.3 Global Materialization Checklist

- [ ] Cross-graph derivation works
- [ ] Derived quad storage correct
- [ ] TBox sharing works in global context
- [ ] Scope configuration respected
- [ ] Non-participating graphs excluded
- [ ] Provenance tracked for cross-graph derivations
- [ ] Statistics aggregated correctly
- [ ] Status updated for all graphs
- [ ] Partial failures handled gracefully
- [ ] Error handling works

### 7.8.4 Incremental Maintenance Checklist

- [ ] Graph-local addition works
- [ ] Graph-local deletion works
- [ ] Cross-graph dependencies detected
- [ ] Cross-graph rederivation works
- [ ] Global incremental addition works
- [ ] Global incremental deletion works
- [ ] Status updated after operations
- [ ] Backward tracing works in graph context
- [ ] Forward rederivation works in graph context
- [ ] Error handling works

### 7.8.5 Derived Store Checklist

- [ ] Derived quads stored correctly
- [ ] is_derived? works for quads
- [ ] Quad lookup works
- [ ] Per-graph counting works
- [ ] Per-graph clearing works
- [ ] Clear derived preserves explicit
- [ ] Provenance tracking works
- [ ] Cross-graph provenance tracked
- [ ] Derivation explanation works
- [ ] Error handling works

### 7.8.6 Configuration Checklist

- [ ] Status creation works
- [ ] Status validation works
- [ ] State transitions work
- [ ] Materialization tracked
- [ ] Stale detection works
- [ ] Persistent term storage works
- [ ] Multi-graph status works
- [ ] Error recording works
- [ ] Error clearing works
- [ ] Status querying works

---

## Conclusion

Section 7.8 provides comprehensive unit test coverage for Phase 7 quad reasoning functionality. The implementation plan prioritizes core materialization tests first, followed by incremental and derived store tests. All tests follow established patterns from existing test files and ensure high coverage of graph-aware reasoning features.

**Next Steps**:

1. Review and approve this plan
2. Create test infrastructure
3. Implement tests in priority order
4. Validate coverage and quality
5. Update phase planning document
6. Merge to `quad` branch

**Success Criteria**:

- All 6 test categories implemented
- >85% code coverage achieved
- All tests pass reliably
- No regressions in existing tests
- Documentation complete
