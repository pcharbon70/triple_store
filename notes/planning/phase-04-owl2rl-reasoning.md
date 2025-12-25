# Phase 4: OWL 2 RL Reasoning

## Overview

Phase 4 implements the OWL 2 RL reasoning profile using forward-chaining with semi-naive evaluation. By the end of this phase, the triple store will support automatic inference of implicit triples based on ontology axioms, with incremental maintenance for efficient updates.

OWL 2 RL provides polynomial-time reasoning covering class hierarchies, property characteristics (transitive, symmetric, inverse), domain/range constraints, and limited forms of class restrictions. The forward-chaining approach materializes all inferred triples at load time, enabling subsequent queries to execute without reasoning overhead.

The semi-naive evaluation strategy processes only newly derived facts (delta) in each iteration, dramatically reducing redundant computation compared to naive fixpoint iteration.

---

## 4.1 Rule Compiler

- [ ] **Section 4.1 Complete**

This section implements compilation of OWL 2 RL axioms into Datalog-style rules suitable for forward-chaining evaluation.

### 4.1.1 Rule Representation

- [x] **Task 4.1.1 Complete** (2025-12-25)

Define the rule representation structure.

- [x] 4.1.1.1 Define `%Rule{name, body, head}` struct
- [x] 4.1.1.2 Define pattern structure `{:pattern, [var_or_term, var_or_term, var_or_term]}`
- [x] 4.1.1.3 Support variable binding between patterns (shared variables)
- [x] 4.1.1.4 Support literal conditions in rule bodies

### 4.1.2 OWL 2 RL Rules

- [ ] **Task 4.1.2 Complete**

Implement the standard OWL 2 RL rule set.

- [ ] 4.1.2.1 Implement rdfs:subClassOf transitivity (scm-sco)
- [ ] 4.1.2.2 Implement class membership through subclass (cax-sco)
- [ ] 4.1.2.3 Implement property domain (prp-dom)
- [ ] 4.1.2.4 Implement property range (prp-rng)
- [ ] 4.1.2.5 Implement transitive property (prp-trp)
- [ ] 4.1.2.6 Implement symmetric property (prp-symp)
- [ ] 4.1.2.7 Implement inverse properties (prp-inv1, prp-inv2)
- [ ] 4.1.2.8 Implement functional property (prp-fp)
- [ ] 4.1.2.9 Implement inverse functional property (prp-ifp)
- [ ] 4.1.2.10 Implement owl:sameAs transitivity and symmetry (eq-trans, eq-sym)
- [ ] 4.1.2.11 Implement hasValue restriction (cls-hv1, cls-hv2)
- [ ] 4.1.2.12 Implement someValuesFrom (cls-svf1, cls-svf2)
- [ ] 4.1.2.13 Implement allValuesFrom (cls-avf)

### 4.1.3 Rule Compilation

- [ ] **Task 4.1.3 Complete**

Compile ontology axioms to applicable rules.

- [ ] 4.1.3.1 Implement `RuleCompiler.compile(ontology)` returning rule list
- [ ] 4.1.3.2 Filter rules to those applicable given ontology axioms
- [ ] 4.1.3.3 Specialize rules with ontology constants where possible
- [ ] 4.1.3.4 Store compiled rules in `:persistent_term` for fast access

### 4.1.4 Rule Optimization

- [ ] **Task 4.1.4 Complete**

Optimize rules for efficient evaluation.

- [ ] 4.1.4.1 Reorder body patterns by selectivity
- [ ] 4.1.4.2 Identify rules that can be batched together
- [ ] 4.1.4.3 Detect rules that cannot fire given current schema

### 4.1.5 Unit Tests

- [ ] **Task 4.1.5 Complete**

- [ ] Test rule representation captures patterns correctly
- [ ] Test subClassOf rule produces correct inferences
- [ ] Test domain/range rules produce type inferences
- [ ] Test transitive property rule chains correctly
- [ ] Test symmetric property rule generates inverse
- [ ] Test sameAs rules propagate equality
- [ ] Test rule compilation filters inapplicable rules
- [ ] Test rule optimization reorders patterns

---

## 4.2 Semi-Naive Evaluation

- [ ] **Section 4.2 Complete**

This section implements semi-naive evaluation for forward-chaining materialization. The algorithm iterates until fixpoint, processing only newly derived facts (delta) in each iteration.

### 4.2.1 Delta Computation

- [ ] **Task 4.2.1 Complete**

Implement delta-based rule application.

- [ ] 4.2.1.1 Implement `apply_rule_delta(db, rule, delta)` using delta for at least one body pattern
- [ ] 4.2.1.2 Generate all rule instantiations with delta facts
- [ ] 4.2.1.3 Filter instantiations already in database
- [ ] 4.2.1.4 Return new facts derived in this iteration

### 4.2.2 Fixpoint Loop

- [ ] **Task 4.2.2 Complete**

Implement the fixpoint iteration loop.

- [ ] 4.2.2.1 Implement `materialize(db, rules)` main entry point
- [ ] 4.2.2.2 Initialize delta with all explicit facts
- [ ] 4.2.2.3 Loop applying rules to delta until delta is empty
- [ ] 4.2.2.4 Track iteration count and derivation statistics
- [ ] 4.2.2.5 Handle stratification for negation (if needed)

### 4.2.3 Parallel Rule Evaluation

- [ ] **Task 4.2.3 Complete**

Parallelize rule evaluation across CPU cores.

- [ ] 4.2.3.1 Apply independent rules in parallel via `Task.async_stream`
- [ ] 4.2.3.2 Merge results from parallel rule applications
- [ ] 4.2.3.3 Configure parallelism level based on available cores
- [ ] 4.2.3.4 Ensure deterministic results despite parallelism

### 4.2.4 Derived Fact Storage

- [ ] **Task 4.2.4 Complete**

Store derived facts distinctly from explicit facts.

- [ ] 4.2.4.1 Write derived facts to `derived` column family
- [ ] 4.2.4.2 Query both explicit and derived during evaluation
- [ ] 4.2.4.3 Support querying derived facts only
- [ ] 4.2.4.4 Support clearing all derived facts (rematerialization)

### 4.2.5 Unit Tests

- [ ] **Task 4.2.5 Complete**

- [ ] Test delta computation finds new facts only
- [ ] Test fixpoint terminates correctly
- [ ] Test fixpoint produces complete inference closure
- [ ] Test parallel evaluation produces same results as sequential
- [ ] Test derived facts stored separately
- [ ] Test clear derived removes only inferred triples

---

## 4.3 Incremental Maintenance

- [ ] **Section 4.3 Complete**

This section implements incremental maintenance of materialized inferences when explicit facts are added or removed. The Backward/Forward algorithm handles deletions without over-deletion.

### 4.3.1 Incremental Addition

- [ ] **Task 4.3.1 Complete**

Handle incremental addition of new facts.

- [ ] 4.3.1.1 Implement `add_with_reasoning(db, triples)` adding facts and deriving consequences
- [ ] 4.3.1.2 Use semi-naive with new facts as initial delta
- [ ] 4.3.1.3 Efficiently check for novel derivations vs existing facts

### 4.3.2 Backward Phase

- [ ] **Task 4.3.2 Complete**

Implement backward phase of deletion algorithm.

- [ ] 4.3.2.1 Implement `backward_trace(db, deleted_triple)` finding dependent derivations
- [ ] 4.3.2.2 Track all derived facts that used deleted triple in derivation
- [ ] 4.3.2.3 Recursively trace facts derived from potentially invalid facts

### 4.3.3 Forward Phase

- [ ] **Task 4.3.3 Complete**

Implement forward phase to re-derive facts with alternative justifications.

- [ ] 4.3.3.1 Implement `can_rederive?(db, fact)` checking for alternative derivations
- [ ] 4.3.3.2 Attempt re-derivation for each potentially invalid fact
- [ ] 4.3.3.3 Keep facts that can be re-derived, delete those that cannot

### 4.3.4 Delete with Reasoning

- [ ] **Task 4.3.4 Complete**

Implement complete deletion with reasoning.

- [ ] 4.3.4.1 Implement `delete_with_reasoning(db, triples)` removing facts and retracting consequences
- [ ] 4.3.4.2 Coordinate backward and forward phases
- [ ] 4.3.4.3 Handle cascading deletions correctly
- [ ] 4.3.4.4 Optimize for bulk deletions

### 4.3.5 Unit Tests

- [ ] **Task 4.3.5 Complete**

- [ ] Test incremental addition derives new consequences
- [ ] Test backward trace finds dependent derivations
- [ ] Test forward phase re-derives facts with alternatives
- [ ] Test delete removes derived facts without alternatives
- [ ] Test delete preserves derived facts with alternatives
- [ ] Test cascading delete handles chains correctly
- [ ] Test bulk delete is efficient

---

## 4.4 TBox Caching

- [ ] **Section 4.4 Complete**

This section implements caching of TBox (schema) inferences for efficient ABox (instance) reasoning. The class and property hierarchies are computed once at ontology load.

### 4.4.1 Class Hierarchy

- [ ] **Task 4.4.1 Complete**

Compute and cache class hierarchy.

- [ ] 4.4.1.1 Implement `compute_class_hierarchy(db)` building superclass map
- [ ] 4.4.1.2 Store in `:persistent_term` for zero-copy access
- [ ] 4.4.1.3 Implement `superclasses(class)` returning all superclasses
- [ ] 4.4.1.4 Implement `subclasses(class)` returning all subclasses

### 4.4.2 Property Hierarchy

- [ ] **Task 4.4.2 Complete**

Compute and cache property hierarchy.

- [ ] 4.4.2.1 Implement `compute_property_hierarchy(db)` building superproperty map
- [ ] 4.4.2.2 Implement `superproperties(prop)` returning all superproperties
- [ ] 4.4.2.3 Cache property characteristics (transitive, symmetric, etc.)

### 4.4.3 TBox Updates

- [ ] **Task 4.4.3 Complete**

Handle updates to TBox requiring hierarchy recomputation.

- [ ] 4.4.3.1 Detect TBox-modifying updates (subClassOf, subPropertyOf, etc.)
- [ ] 4.4.3.2 Trigger hierarchy recomputation on TBox changes
- [ ] 4.4.3.3 Optionally trigger full rematerialization

### 4.4.4 Unit Tests

- [ ] **Task 4.4.4 Complete**

- [ ] Test class hierarchy computed correctly
- [ ] Test superclasses returns transitive closure
- [ ] Test property hierarchy computed correctly
- [ ] Test TBox update triggers recomputation

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
