# Phase 4: Semantic Publishing Benchmark

Description: Phase 4 implements the LDBC Semantic Publishing Benchmark against
TripleStore's RDF, SPARQL update, named-graph, and reasoning surfaces. It covers
aggregation and editorial operations, mixed execution, result validation, and the
selected SPB conformance profiles. Resilience tests that require replication remain
score-gated until an explicitly approved replication architecture exists; backup or
restore behavior must not be presented as online replication.

---

## Section 4.1: SPB Workload and Parameter Packaging

Description: This section imports the pinned SPB operation definitions and binds
them to the Phase 2 datasets and the Phase 3 canonical operation model.

### Task 4.1.1: Import aggregation operations

Description: Package every aggregation query in the selected SPB profile with its
canonical text, parameter schema, result rules, and source provenance.

- [ ] 4.1.1.1 Import all aggregation query templates without silently rewriting
  unsupported constructs.
- [ ] 4.1.1.2 Bind generated substitution parameters to their dataset manifest and scale.
- [ ] 4.1.1.3 Record query mix frequency, timeout class, ordering, limit, result
  schema, and inference expectations.
- [ ] 4.1.1.4 Validate every instantiated query through the native parser and algebra pipeline.

### Task 4.1.2: Import editorial and validation operations

Description: Package all create, update, delete, validation, and conformance actions
with their required preconditions and expected state transitions.

- [ ] 4.1.2.1 Import editorial operation templates and typed parameter definitions.
- [ ] 4.1.2.2 Preserve operation dependencies and any generated-resource identifiers.
- [ ] 4.1.2.3 Import query-result validation and OWL/RDFS conformance actions.
- [ ] 4.1.2.4 Record optional text, geospatial, context, replication, backup, and
  failover actions separately from the core operation mix.
- [ ] 4.1.2.5 Generate stable local IDs that retain SPB's canonical operation names.

## Section 4.2: Context and Reasoning Semantics

Description: This section makes SPB's graph-context and inference expectations
query-visible and update-safe rather than assuming that available reasoner modules
already satisfy the benchmark.

### Task 4.2.1: Implement context-preserving SPB storage

Description: Preserve the SPB repository contexts required by query and update
semantics using the selected TripleStore schema and documented mapping.

- [ ] 4.2.1.1 Identify which SPB source graphs and generated statements require
  distinct graph contexts.
- [ ] 4.2.1.2 Load those contexts through quad-aware APIs and preserve graph identity
  across update, export, backup, and restore.
- [ ] 4.2.1.3 Ensure aggregation and editorial operations address the correct default
  and named graphs.
- [ ] 4.2.1.4 Verify graph ACL behavior is disabled or explicitly configured so it
  cannot change benchmark answers between runs.

### Task 4.2.2: Implement the SPB inference profile

Description: Make the benchmark's required RDFS and OWL entailments persistent,
query-visible, deterministic, and maintainable after editorial updates.

- [ ] 4.2.2.1 Map the pinned SPB rule configuration to TripleStore reasoning rules,
  including required subclass, subproperty, transitive, symmetric, and same-as behavior.
- [ ] 4.2.2.2 Add missing rules or benchmark-profile behavior through the canonical
  reasoner rather than query-specific result patches.
- [ ] 4.2.2.3 Persist derived facts in the selected graph scope and include them in
  benchmark query execution.
- [ ] 4.2.2.4 Maintain or rederive affected inference after inserts, updates, and deletes.
- [ ] 4.2.2.5 Keep explicit and derived facts distinguishable for validation and reset.
- [ ] 4.2.2.6 Pass the SPB inference conformance cases before enabling performance scoring.

## Section 4.3: Aggregation Query Execution

Description: This section closes parser, algebra, execution, datatype, and optimizer
gaps exposed by the complete SPB aggregation corpus.

### Task 4.3.1: Achieve complete aggregation-query correctness

Description: Execute every SPB aggregation query with canonical parameters and
match accepted answers before measuring latency.

- [ ] 4.3.1.1 Add one focused behavior test per unsupported or incorrect query feature.
- [ ] 4.3.1.2 Implement required aggregate, expression, date, string, grouping,
  ordering, subquery, and graph-clause semantics in the query engine.
- [ ] 4.3.1.3 Preserve SPARQL duplicate and unbound-variable semantics through projection.
- [ ] 4.3.1.4 Verify inferred and explicit statements contribute exactly as specified.
- [ ] 4.3.1.5 Cross-validate complete results for multiple parameter sets and scale tiers.

### Task 4.3.2: Optimize only validated query plans

Description: Add statistics and plan improvements after correctness baselines exist,
with a reference path retained for comparison.

- [ ] 4.3.2.1 Capture explain plans and cardinality estimates for every aggregation query.
- [ ] 4.3.2.2 Identify repeated scans, materialization pressure, poor join order,
  graph-context misses, and aggregate memory growth.
- [ ] 4.3.2.3 Add focused optimizer or index improvements without embedding SPB query IDs
  in production planning logic.
- [ ] 4.3.2.4 Compare optimized and reference answers before accepting timing changes.

## Section 4.4: Editorial and Mixed-Workload Execution

Description: This section implements state-changing editorial operations and the
specified simultaneous query/update workload with observable coordination semantics.

### Task 4.4.1: Implement editorial state transitions

Description: Map SPB editorial operations to atomic, graph-aware update requests
that preserve inference and cache consistency.

- [ ] 4.4.1.1 Implement create, alter, and delete operations through the store-owned
  transaction coordinator.
- [ ] 4.4.1.2 Preserve request-level atomicity across explicit indices and graph metadata.
- [ ] 4.4.1.3 Invalidate or refresh result caches, plans, statistics, and derived facts
  only after successful commit.
- [ ] 4.4.1.4 Verify failed operations leave explicit and derived benchmark answers unchanged.
- [ ] 4.4.1.5 Validate state transitions against the SPB driver and expected result probes.

### Task 4.4.2: Implement SPB agent scheduling

Description: Drive simultaneous aggregation and editorial agents using the pinned
SPB driver configuration and preserve its warmup and measurement boundaries.

- [ ] 4.4.2.1 Connect aggregation and editorial agents through the benchmark bridge.
- [ ] 4.4.2.2 Route benchmark reads through a coordination or snapshot strategy that
  satisfies the accepted Phase 1 isolation decision.
- [ ] 4.4.2.3 Capture per-agent operation counts, latency, failures, retries, and rate.
- [ ] 4.4.2.4 Verify driver backpressure and queueing do not reorder dependent editorial operations.
- [ ] 4.4.2.5 Compute SPB query and editorial operation rates only for complete valid runs.

## Section 4.5: Resilience Profiles and Disclosure

Description: This section handles SPB backup, recovery, and replication-related
actions without conflating the embedded library's current backup support with an
online replicated RDF service.

### Task 4.5.1: Implement backup and recovery actions

Description: Exercise supported online or coordinated backup behavior using the
benchmark dataset and verify complete restoration of explicit, derived, and graph state.

- [ ] 4.5.1.1 Map SPB backup actions to the supported TripleStore backup surface.
- [ ] 4.5.1.2 Capture backup duration, workload impact, artifact size, and errors.
- [ ] 4.5.1.3 Restore into a fresh path and verify manifests plus accepted query answers.
- [ ] 4.5.1.4 Verify scheduled helpers and store processes are cleanly stopped after the action.

### Task 4.5.2: Gate replication and failover claims

Description: Decide and implement the product capability required for any SPB
replication or failover profile before that profile can be reported as supported.

- [ ] 4.5.2.1 Document the exact SPB replication and failover requirements from the
  pinned specification and driver.
- [ ] 4.5.2.2 Write a separate architecture decision if fulfilling them requires a
  replicated deployment or changes to TripleStore's embedded-library contract.
- [ ] 4.5.2.3 Implement the approved replication/failover adapter and state-consistency
  validation, or mark the profile unsupported with no score output.
- [ ] 4.5.2.4 Prevent backup/restore simulations from being labeled replication or failover.
- [ ] 4.5.2.5 Include supported SPB profiles and limitations in every report and disclosure.

## Section 4.6: Integration Tests

Description: This final section runs the complete SPB smoke workflow through data,
reasoning, queries, editorial updates, mixed scheduling, artifacts, and supported
resilience actions.

### Task 4.6.1: Validate SPB correctness end to end

Description: Prove that all core operations produce accepted answers and state
transitions on a deterministic smoke dataset.

- [ ] 4.6.1.1 Load reference data, ontologies, generated works, and contexts into a
  fresh store and materialize the selected inference profile.
- [ ] 4.6.1.2 Execute every aggregation operation with smoke parameters and compare answers.
- [ ] 4.6.1.3 Execute every editorial operation in canonical order and validate
  explicit, derived, and graph state after each checkpoint.
- [ ] 4.6.1.4 Verify failure injection and rollback preserve accepted answers.
- [ ] 4.6.1.5 Reset the store and reproduce the initial answer baseline exactly.

### Task 4.6.2: Validate SPB mixed and resilience execution

Description: Exercise concurrency and supported operational actions under the real driver.

- [ ] 4.6.2.1 Run the smoke query/editorial mix with multiple agents and verify no
  partial-index or partial-inference observations.
- [ ] 4.6.2.2 Verify warmup and measured periods are separated and complete operation
  counts reconcile with driver logs.
- [ ] 4.6.2.3 Run backup and restore during the supported profile and compare post-restore answers.
- [ ] 4.6.2.4 Verify unsupported replication/failover configurations fail before measurement.

### Task 4.6.3: Pass the Phase 4 SPB gate

Description: Establish SPB as a correctness-qualified benchmark suite.

- [ ] 4.6.3.1 Run formatting, strict compilation, affected query/update/reasoning tests,
  and the complete SPB smoke integration suite.
- [ ] 4.6.3.2 Verify all mandatory core operations are implemented and no incorrect
  operation contributes to SPB rates.
- [ ] 4.6.3.3 Generate and schema-validate JSON, CSV, Markdown, correctness, and
  disclosure artifacts.
- [ ] 4.6.3.4 Record supported profiles, unsupported resilience capabilities, exact
  commands, counts, and excluded large runs in the phase pull request.
