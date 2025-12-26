# Phase 4: OWL 2 RL Reasoning

## Overview

Phase 4 implements the OWL 2 RL reasoning profile using forward-chaining with semi-naive evaluation. By the end of this phase, the triple store will support automatic inference of implicit triples based on ontology axioms, with incremental maintenance for efficient updates.

OWL 2 RL provides polynomial-time reasoning covering class hierarchies, property characteristics (transitive, symmetric, inverse), domain/range constraints, and limited forms of class restrictions. The forward-chaining approach materializes all inferred triples at load time, enabling subsequent queries to execute without reasoning overhead.

The semi-naive evaluation strategy processes only newly derived facts (delta) in each iteration, dramatically reducing redundant computation compared to naive fixpoint iteration.

---

## 4.1 Rule Compiler

- [x] **Section 4.1 Complete** (2025-12-25)
- [x] **Section 4.1 Review Fixes Complete** (2025-12-25)

This section implements compilation of OWL 2 RL axioms into Datalog-style rules suitable for forward-chaining evaluation.

### Review Fixes (2025-12-25)

The following issues from the comprehensive review were addressed:

**Blockers Fixed:**
- SPARQL injection vulnerability (IRI validation via Namespaces module)
- Atom table exhaustion (sanitized names, specialization limits)
- Improved error handling specificity

**New Modules Added:**
- `TripleStore.Reasoner.Namespaces` - Shared namespace constants, IRI validation
- `TripleStore.Reasoner.SchemaInfo` - Type-safe struct with validation
- `TripleStore.Reasoner.Telemetry` - Instrumentation events

**Enhancements:**
- :persistent_term lifecycle management (list_stored, clear_all, stale?)
- Rule specialization limits (max_specializations, max_properties)
- Delta pattern marking for semi-naive evaluation
- Rule.validate/1 for well-formedness checks
- Rule.explain/1 and explain_applicability/2 for debugging
- Blank node type support
- Module attributes for selectivity constants

**Test Coverage:** 258 tests (219 original + 39 review fixes)

### 4.1.1 Rule Representation

- [x] **Task 4.1.1 Complete** (2025-12-25)

Define the rule representation structure.

- [x] 4.1.1.1 Define `%Rule{name, body, head}` struct
- [x] 4.1.1.2 Define pattern structure `{:pattern, [var_or_term, var_or_term, var_or_term]}`
- [x] 4.1.1.3 Support variable binding between patterns (shared variables)
- [x] 4.1.1.4 Support literal conditions in rule bodies

### 4.1.2 OWL 2 RL Rules

- [x] **Task 4.1.2 Complete** (2025-12-25)

Implement the standard OWL 2 RL rule set.

- [x] 4.1.2.1 Implement rdfs:subClassOf transitivity (scm-sco)
- [x] 4.1.2.2 Implement class membership through subclass (cax-sco)
- [x] 4.1.2.3 Implement property domain (prp-dom)
- [x] 4.1.2.4 Implement property range (prp-rng)
- [x] 4.1.2.5 Implement transitive property (prp-trp)
- [x] 4.1.2.6 Implement symmetric property (prp-symp)
- [x] 4.1.2.7 Implement inverse properties (prp-inv1, prp-inv2)
- [x] 4.1.2.8 Implement functional property (prp-fp)
- [x] 4.1.2.9 Implement inverse functional property (prp-ifp)
- [x] 4.1.2.10 Implement owl:sameAs transitivity and symmetry (eq-trans, eq-sym)
- [x] 4.1.2.11 Implement hasValue restriction (cls-hv1, cls-hv2)
- [x] 4.1.2.12 Implement someValuesFrom (cls-svf1, cls-svf2)
- [x] 4.1.2.13 Implement allValuesFrom (cls-avf)

### 4.1.3 Rule Compilation

- [x] **Task 4.1.3 Complete** (2025-12-25)

Compile ontology axioms to applicable rules.

- [x] 4.1.3.1 Implement `RuleCompiler.compile(ontology)` returning rule list
- [x] 4.1.3.2 Filter rules to those applicable given ontology axioms
- [x] 4.1.3.3 Specialize rules with ontology constants where possible
- [x] 4.1.3.4 Store compiled rules in `:persistent_term` for fast access

### 4.1.4 Rule Optimization

- [x] **Task 4.1.4 Complete** (2025-12-25)

Optimize rules for efficient evaluation.

- [x] 4.1.4.1 Reorder body patterns by selectivity
- [x] 4.1.4.2 Identify rules that can be batched together
- [x] 4.1.4.3 Detect rules that cannot fire given current schema

### 4.1.5 Unit Tests

- [x] **Task 4.1.5 Complete** (2025-12-25)

- [x] Test rule representation captures patterns correctly
- [x] Test subClassOf rule produces correct inferences
- [x] Test domain/range rules produce type inferences
- [x] Test transitive property rule chains correctly
- [x] Test symmetric property rule generates inverse
- [x] Test sameAs rules propagate equality
- [x] Test rule compilation filters inapplicable rules
- [x] Test rule optimization reorders patterns

---

## 4.2 Semi-Naive Evaluation

- [x] **Section 4.2 Complete** (2025-12-26)
- [x] **Section 4.2 Review Fixes Complete** (2025-12-26)

This section implements semi-naive evaluation for forward-chaining materialization. The algorithm iterates until fixpoint, processing only newly derived facts (delta) in each iteration.

### Review Fixes (2025-12-26)

The following issues from the comprehensive review were addressed:

**Blockers Fixed:**
- Task timeout in parallel execution (60s default, configurable)
- Batched deletion in clear_all/1 to prevent OOM
- Fixed max_facts test logic
- Created PatternMatcher module eliminating code duplication

**Enhancements:**
- Error logging for lookup failures
- Refactored nested if to cond for clarity
- Stream-based lazy evaluation in delta computation
- Optional rule validation before materialization
- Documented materialize telemetry events
- Added emit_iteration/2 helper

**New Module:** `TripleStore.Reasoner.PatternMatcher` - Shared pattern matching utilities

**Test Coverage:** 381 tests (370 original + 11 telemetry tests)

### 4.2.1 Delta Computation

- [x] **Task 4.2.1 Complete** (2025-12-25)

Implement delta-based rule application.

- [x] 4.2.1.1 Implement `apply_rule_delta(db, rule, delta)` using delta for at least one body pattern
- [x] 4.2.1.2 Generate all rule instantiations with delta facts
- [x] 4.2.1.3 Filter instantiations already in database
- [x] 4.2.1.4 Return new facts derived in this iteration

### 4.2.2 Fixpoint Loop

- [x] **Task 4.2.2 Complete** (2025-12-25)

Implement the fixpoint iteration loop.

- [x] 4.2.2.1 Implement `materialize(db, rules)` main entry point
- [x] 4.2.2.2 Initialize delta with all explicit facts
- [x] 4.2.2.3 Loop applying rules to delta until delta is empty
- [x] 4.2.2.4 Track iteration count and derivation statistics
- [x] 4.2.2.5 Handle stratification for negation (if needed)

### 4.2.3 Parallel Rule Evaluation

- [x] **Task 4.2.3 Complete** (2025-12-25)

Parallelize rule evaluation across CPU cores.

- [x] 4.2.3.1 Apply independent rules in parallel via `Task.async_stream`
- [x] 4.2.3.2 Merge results from parallel rule applications
- [x] 4.2.3.3 Configure parallelism level based on available cores
- [x] 4.2.3.4 Ensure deterministic results despite parallelism

### 4.2.4 Derived Fact Storage

- [x] **Task 4.2.4 Complete** (2025-12-26)

Store derived facts distinctly from explicit facts.

- [x] 4.2.4.1 Write derived facts to `derived` column family
- [x] 4.2.4.2 Query both explicit and derived during evaluation
- [x] 4.2.4.3 Support querying derived facts only
- [x] 4.2.4.4 Support clearing all derived facts (rematerialization)

### 4.2.5 Unit Tests

- [x] **Task 4.2.5 Complete** (2025-12-26)

- [x] Test delta computation finds new facts only
- [x] Test fixpoint terminates correctly
- [x] Test fixpoint produces complete inference closure
- [x] Test parallel evaluation produces same results as sequential
- [x] Test derived facts stored separately
- [x] Test clear derived removes only inferred triples

---

## 4.3 Incremental Maintenance

- [x] **Section 4.3 Complete** (2025-12-26)
- [x] **Section 4.3 Review Fixes Complete** (2025-12-26)

This section implements incremental maintenance of materialized inferences when explicit facts are added or removed. The Backward/Forward algorithm handles deletions without over-deletion.

**Section Summary:**
- 5 tasks completed (4.3.1-4.3.5)
- 4 new modules: Incremental, BackwardTrace, ForwardRederive, DeleteWithReasoning
- 112 tests total covering all incremental maintenance functionality

### Review Fixes (2025-12-26)

The following issues from the comprehensive review were addressed:

**Blockers Fixed:**
- Unbounded binding set growth (added `@max_binding_sets 10_000` limit)

**Code Quality Improvements:**
- Consolidated duplicate pattern matching code (~60 lines) into PatternMatcher
- Fixed redundant MapSet union operation
- Added error logging for silent failures in database operations
- Fixed misleading documentation about non-existent `trace/4` API
- Standardized test helper section naming across test files

**Enhancements:**
- Added 4 new telemetry events for deletion operations:
  - `[:triple_store, :reasoner, :delete, :start]`
  - `[:triple_store, :reasoner, :delete, :stop]`
  - `[:triple_store, :reasoner, :backward_trace, :complete]`
  - `[:triple_store, :reasoner, :forward_rederive, :complete]`

**Test Coverage:** 493 reasoner tests total

### 4.3.1 Incremental Addition

- [x] **Task 4.3.1 Complete** (2025-12-26)

Handle incremental addition of new facts.

- [x] 4.3.1.1 Implement `add_with_reasoning(db, triples)` adding facts and deriving consequences
- [x] 4.3.1.2 Use semi-naive with new facts as initial delta
- [x] 4.3.1.3 Efficiently check for novel derivations vs existing facts

**Implementation Notes:**
- New module: `TripleStore.Reasoner.Incremental`
- Dual API: `add_in_memory/4` for testing, `add_with_reasoning/4` for database
- Preview functions: `preview_in_memory/3`, `preview_additions/3`
- 24 new tests added

### 4.3.2 Backward Phase

- [x] **Task 4.3.2 Complete** (2025-12-26)

Implement backward phase of deletion algorithm.

- [x] 4.3.2.1 Implement `backward_trace(db, deleted_triple)` finding dependent derivations
- [x] 4.3.2.2 Track all derived facts that used deleted triple in derivation
- [x] 4.3.2.3 Recursively trace facts derived from potentially invalid facts

**Implementation Notes:**
- New module: `TripleStore.Reasoner.BackwardTrace`
- `trace_in_memory/4` for backward tracing with recursive dependency detection
- `find_direct_dependents/3` for single-level dependency detection
- `could_derive?/4` for checking derivation possibility
- Cycle-safe with visited set tracking
- 25 new tests added

### 4.3.3 Forward Phase

- [x] **Task 4.3.3 Complete** (2025-12-26)

Implement forward phase to re-derive facts with alternative justifications.

- [x] 4.3.3.1 Implement `can_rederive?(db, fact)` checking for alternative derivations
- [x] 4.3.3.2 Attempt re-derivation for each potentially invalid fact
- [x] 4.3.3.3 Keep facts that can be re-derived, delete those that cannot

**Implementation Notes:**
- New module: `TripleStore.Reasoner.ForwardRederive`
- `rederive_in_memory/4` for attempting re-derivation with statistics
- `can_rederive?/3` for checking if a single fact can be re-derived
- `partition_invalid/4` for convenience partition of facts
- Complete pattern matching and unification for body/head matching
- 20 new tests added

### 4.3.4 Delete with Reasoning

- [x] **Task 4.3.4 Complete** (2025-12-26)

Implement complete deletion with reasoning.

- [x] 4.3.4.1 Implement `delete_with_reasoning(db, triples)` removing facts and retracting consequences
- [x] 4.3.4.2 Coordinate backward and forward phases
- [x] 4.3.4.3 Handle cascading deletions correctly
- [x] 4.3.4.4 Optimize for bulk deletions

**Implementation Notes:**
- New module: `TripleStore.Reasoner.DeleteWithReasoning`
- `delete_in_memory/5` for full deletion with comprehensive results
- `delete_with_reasoning/4` for database-backed deletion
- `bulk_delete_with_reasoning/4` for batched large deletions
- `preview_delete_in_memory/4` for dry-run capability
- Coordinates BackwardTrace and ForwardRederive phases
- 20 new tests added

### 4.3.5 Unit Tests

- [x] **Task 4.3.5 Complete** (2025-12-26)

- [x] Test incremental addition derives new consequences
- [x] Test backward trace finds dependent derivations
- [x] Test forward phase re-derives facts with alternatives
- [x] Test delete removes derived facts without alternatives
- [x] Test delete preserves derived facts with alternatives
- [x] Test cascading delete handles chains correctly
- [x] Test bulk delete is efficient

**Implementation Notes:**
- New integration test file: `incremental_maintenance_integration_test.exs`
- 23 new integration tests covering end-to-end workflows
- Tests cover: add-delete roundtrips, diamond inheritance, bulk operations
- Total Section 4.3 test coverage: 112 tests

---

## 4.4 TBox Caching

- [x] **Section 4.4 Complete** (2025-12-26)

This section implements caching of TBox (schema) inferences for efficient ABox (instance) reasoning. The class and property hierarchies are computed once at ontology load.

**Section Summary:**
- 4 tasks completed (4.4.1-4.4.4)
- New module: `TripleStore.Reasoner.TBoxCache`
- Features: class hierarchy, property hierarchy, property characteristics, inverse pairs, TBox update detection, cache invalidation
- 95 tests covering all TBox caching functionality

### 4.4.1 Class Hierarchy

- [x] **Task 4.4.1 Complete** (2025-12-26)

Compute and cache class hierarchy.

- [x] 4.4.1.1 Implement `compute_class_hierarchy(db)` building superclass map
- [x] 4.4.1.2 Store in `:persistent_term` for zero-copy access
- [x] 4.4.1.3 Implement `superclasses(class)` returning all superclasses
- [x] 4.4.1.4 Implement `subclasses(class)` returning all subclasses

**Implementation Notes:**
- New module: `TripleStore.Reasoner.TBoxCache`
- Computes transitive closure of `rdfs:subClassOf` relationships
- Dual API: in-memory for testing, persistent_term for production
- 30 tests covering hierarchy computation, queries, and cache management

### 4.4.2 Property Hierarchy

- [x] **Task 4.4.2 Complete** (2025-12-26)

Compute and cache property hierarchy.

- [x] 4.4.2.1 Implement `compute_property_hierarchy(db)` building superproperty map
- [x] 4.4.2.2 Implement `superproperties(prop)` returning all superproperties
- [x] 4.4.2.3 Cache property characteristics (transitive, symmetric, etc.)

**Implementation Notes:**
- Extended `TripleStore.Reasoner.TBoxCache` module
- Computes transitive closure of `rdfs:subPropertyOf` relationships
- Extracts property characteristics: transitive, symmetric, functional, inverse functional
- Extracts and caches inverse property pairs bidirectionally
- 32 new tests covering property hierarchy computation, characteristics, and cache management
- Total TBoxCache tests: 62 (30 class + 32 property)

### 4.4.3 TBox Updates

- [x] **Task 4.4.3 Complete** (2025-12-26)

Handle updates to TBox requiring hierarchy recomputation.

- [x] 4.4.3.1 Detect TBox-modifying updates (subClassOf, subPropertyOf, etc.)
- [x] 4.4.3.2 Trigger hierarchy recomputation on TBox changes
- [x] 4.4.3.3 Optionally trigger full rematerialization

**Implementation Notes:**
- Extended `TripleStore.Reasoner.TBoxCache` module
- Detects TBox-modifying predicates: rdfs:subClassOf, rdfs:subPropertyOf, owl:inverseOf, rdfs:domain, rdfs:range
- Detects property characteristics: owl:TransitiveProperty, owl:SymmetricProperty, owl:FunctionalProperty, owl:InverseFunctionalProperty
- `handle_tbox_update/4` main entry point for coordinating detection, invalidation, and recomputation
- Optional recomputation via `recompute: false` option
- 33 new tests covering TBox update detection and cache invalidation
- Total TBoxCache tests: 95 (30 class + 32 property + 33 updates)

### 4.4.4 Unit Tests

- [x] **Task 4.4.4 Complete** (2025-12-26)

- [x] Test class hierarchy computed correctly
- [x] Test superclasses returns transitive closure
- [x] Test property hierarchy computed correctly
- [x] Test TBox update triggers recomputation

**Implementation Notes:**
- All tests already implemented as part of Tasks 4.4.1, 4.4.2, and 4.4.3
- Total TBoxCache test coverage: 95 tests
- Covers: class hierarchy, property hierarchy, property characteristics, cache management, TBox update detection, cache invalidation, and recomputation

---

## 4.5 Reasoning Configuration

- [ ] **Section 4.5 Complete**

This section implements configuration options for reasoning behavior, including profile selection and materialization mode.

### 4.5.1 Profile Selection

- [ ] **Task 4.5.1 Complete**

Support different reasoning profiles.

- [ ] 4.5.1.1 Implement RDFS profile (subclass, domain, range only)
- [ ] 4.5.1.2 Implement OWL 2 RL profile (full rule set)
- [ ] 4.5.1.3 Implement custom profile (user-selected rules)
- [ ] 4.5.1.4 Configure profile via `TripleStore.materialize(db, profile: :owl2rl)`

### 4.5.2 Reasoning Mode

- [ ] **Task 4.5.2 Complete**

Support different reasoning modes.

- [ ] 4.5.2.1 Implement `:materialized` mode (all inferences pre-computed)
- [ ] 4.5.2.2 Implement `:hybrid` mode (common inferences materialized, rare backward-chained)
- [ ] 4.5.2.3 Implement `:query_time` mode (no materialization, backward chaining)
- [ ] 4.5.2.4 Configure mode via `TripleStore.configure_reasoning(db, mode: :materialized)`

### 4.5.3 Reasoning Status

- [ ] **Task 4.5.3 Complete**

Provide reasoning status information.

- [ ] 4.5.3.1 Implement `TripleStore.reasoning_status(db)` returning current state
- [ ] 4.5.3.2 Report derived triple count
- [ ] 4.5.3.3 Report last materialization time
- [ ] 4.5.3.4 Report active profile and mode

### 4.5.4 Unit Tests

- [ ] **Task 4.5.4 Complete**

- [ ] Test RDFS profile applies subset of rules
- [ ] Test OWL 2 RL profile applies full rule set
- [ ] Test custom profile applies selected rules only
- [ ] Test reasoning status reports accurate information

---

## 4.6 Phase 4 Integration Tests

- [ ] **Section 4.6 Complete**

Integration tests validate the complete reasoning subsystem with realistic ontologies.

### 4.6.1 Materialization Testing

- [ ] **Task 4.6.1 Complete**

Test full materialization on benchmark ontologies.

- [ ] 4.6.1.1 Test materialization on LUBM(1) dataset
- [ ] 4.6.1.2 Verify query results match expected inference closure
- [ ] 4.6.1.3 Benchmark: LUBM(1) materialization <60 seconds
- [ ] 4.6.1.4 Test parallel materialization shows linear speedup

### 4.6.2 Incremental Testing

- [ ] **Task 4.6.2 Complete**

Test incremental maintenance scenarios.

- [ ] 4.6.2.1 Test add instance -> new type inferences derived
- [ ] 4.6.2.2 Test delete instance -> dependent inferences retracted
- [ ] 4.6.2.3 Test delete with alternative derivation preserves fact
- [ ] 4.6.2.4 Test TBox update triggers rematerialization

### 4.6.3 Query with Reasoning Testing

- [ ] **Task 4.6.3 Complete**

Test SPARQL queries returning inferred results.

- [ ] 4.6.3.1 Test class hierarchy query returns inferred types
- [ ] 4.6.3.2 Test transitive property query returns inferred relationships
- [ ] 4.6.3.3 Test sameAs query returns canonicalized results
- [ ] 4.6.3.4 Compare materialized vs query-time reasoning results

### 4.6.4 Reasoning Correctness Testing

- [ ] **Task 4.6.4 Complete**

Validate reasoning correctness against reference implementation.

- [ ] 4.6.4.1 Compare inference results with reference reasoner (HermiT, Pellet)
- [ ] 4.6.4.2 Test known-hard cases (owl:sameAs chains, etc.)
- [ ] 4.6.4.3 Test consistency checking (owl:Nothing membership)
- [ ] 4.6.4.4 Test no spurious inferences generated

---

## Success Criteria

1. **Rule Coverage**: All OWL 2 RL rules implemented correctly
2. **Materialization**: LUBM(1) materializes in <60 seconds
3. **Parallelism**: Linear speedup with available cores
4. **Incremental**: Add/delete handle inference updates correctly
5. **Correctness**: Results match reference reasoner
6. **TBox Caching**: Hierarchy lookups are O(1)

## Provides Foundation

This phase establishes the infrastructure for:
- **Phase 5**: Reasoning telemetry, benchmark integration, production tuning

## Key Outputs

- `TripleStore.Reasoner.RuleCompiler` - OWL 2 RL rule compilation
- `TripleStore.Reasoner.SemiNaive` - Semi-naive fixpoint evaluation
- `TripleStore.Reasoner.Incremental` - Backward/Forward deletion algorithm
- `TripleStore.Reasoner.TBoxCache` - Cached class/property hierarchies
- `TripleStore.Reasoner` - Configuration and status API
- `TripleStore.materialize/2` - Public materialization API
