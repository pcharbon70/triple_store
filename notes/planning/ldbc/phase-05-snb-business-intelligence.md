# Phase 5: SNB Business Intelligence

Description: Phase 5 implements the complete pinned SNB Business Intelligence
profile over the Phase 2 RDF mapping. It translates the canonical BI operations,
implements missing analytical and path semantics in explicit engine boundaries,
applies microbatches, cross-validates results, and reproduces the selected power
and throughput protocols. Official score names remain gated on exact protocol and
correctness compliance.

---

## Section 5.1: BI Operation Translation and Parameters

Description: This section translates every canonical BI operation into an auditable
TripleStore execution definition while preserving input and result contracts.

### Task 5.1.1: Translate canonical BI reads

Description: Implement all reads and variants from the pinned BI catalog using
SPARQL where semantically exact and explicit benchmark extensions where necessary.

- [ ] 5.1.1.1 Create one versioned operation definition for every canonical BI read
  and variant.
- [ ] 5.1.1.2 Preserve canonical parameters, result columns, numeric widths, ordering,
  tie breakers, limits, and duplicate behavior.
- [ ] 5.1.1.3 Link every translation to its upstream query card, choke points, and
  reference implementation behavior.
- [ ] 5.1.1.4 Mark non-standard path or algorithm operations with an explicit extension
  identifier rather than disguising application-side work as SPARQL.
- [ ] 5.1.1.5 Parse and validate every SPARQL translation during corpus construction.

### Task 5.1.2: Integrate BI parameter generation

Description: Use the official parameter generator and preserve the distributions
that prevent trivial caching or empty-result workloads.

- [ ] 5.1.2.1 Bind parameter files to scale factor, dataset checksum, generator pin,
  and parameter-generator pin.
- [ ] 5.1.2.2 Validate parameter types and referenced values against the mapped store.
- [ ] 5.1.2.3 Preserve the canonical number and ordering of parameter sets required
  by validation, power, and throughput runs.
- [ ] 5.1.2.4 Reject hand-selected parameters from comparable profiles.

## Section 5.2: Analytical and Path Capability Completion

Description: This section implements engine semantics exposed by the BI corpus,
including aggregation-heavy plans and path computations that standard SPARQL may
not express completely.

### Task 5.2.1: Close SPARQL analytical gaps

Description: Add behavior-driven fixes for BI expressions, aggregation, ordering,
subqueries, and datatype operations through the canonical query stack.

- [ ] 5.2.1.1 Add a failing end-to-end regression for every unsupported or incorrect
  translated BI query.
- [ ] 5.2.1.2 Implement required date and duration arithmetic, string functions,
  conditional expressions, numeric coercion, and aggregate behavior.
- [ ] 5.2.1.3 Implement required nested aggregation, subquery, grouping, HAVING,
  ordering, limit, and tie-breaking semantics.
- [ ] 5.2.1.4 Keep all result typing aligned with BI result schemas rather than RDF
  lexical coincidence.
- [ ] 5.2.1.5 Update query contracts and conformance mappings for any semantic engine change.

### Task 5.2.2: Implement graph algorithm extensions

Description: Provide engine-owned execution for BI operations requiring weighted
shortest paths or other graph algorithms beyond standard SPARQL 1.1 connectivity.

- [ ] 5.2.2.1 Define a typed internal algebra or expert operation for each required
  path algorithm and document its semantics.
- [ ] 5.2.2.2 Execute traversal against TripleStore indices with bounded memory,
  cancellation, timeout, and deterministic tie behavior.
- [ ] 5.2.2.3 Keep graph algorithms inside a measurable engine boundary; prohibit
  driver-side graph extraction followed by hidden computation.
- [ ] 5.2.2.4 Validate weighted distance, reachability, path bounds, and disconnected
  cases against a reference implementation.
- [ ] 5.2.2.5 Add explain and telemetry output sufficient to diagnose traversal cost.

## Section 5.3: BI Update Batches and State Management

Description: This section applies canonical BI insert/delete microbatches with
correct request ordering, atomic mutation boundaries, and reset behavior.

### Task 5.3.1: Translate and apply BI update batches

Description: Convert generated BI update-batch records into canonical mapped RDF
mutations without losing dependency or deletion semantics.

- [ ] 5.3.1.1 Translate every insert and delete record type through the approved RDF mapping.
- [ ] 5.3.1.2 Preserve batch and within-batch ordering required by the generator output.
- [ ] 5.3.1.3 Apply one canonical batch through a defined transaction boundary and
  reject partial batch success.
- [ ] 5.3.1.4 Refresh statistics, plans, and result caches only after successful commit.
- [ ] 5.3.1.5 Validate entity counts, relationship counts, and selected query answers
  after each smoke batch.

### Task 5.3.2: Implement BI checkpoint and restore behavior

Description: Make validation, power, and throughput runs start from their exact
required states and remain independently reproducible.

- [ ] 5.3.2.1 Define initial-load, post-validation, pre-power, and pre-throughput checkpoints.
- [ ] 5.3.2.2 Restore checkpoints with all store processes closed and verify state checksums.
- [ ] 5.3.2.3 Prevent a failed or interrupted run from becoming the input to a later run.
- [ ] 5.3.2.4 Record update-batch position and state checksum in every run artifact.

## Section 5.4: BI Driver Protocol, Scoring, and Optimization

Description: This section connects the translated workload to the canonical
execution schedule and score calculations, then improves performance without
changing answers or protocol.

### Task 5.4.1: Reproduce validation, power, and throughput runs

Description: Implement the pinned BI protocol with the official parameter sequence,
update placement, concurrency, duration, and scoring inputs.

- [ ] 5.4.1.1 Run canonical validation parameter sets and compare all result rows.
- [ ] 5.4.1.2 Implement the power run with its required query variants and update timing.
- [ ] 5.4.1.3 Implement the throughput run with canonical batches, streams, and load-time accounting.
- [ ] 5.4.1.4 Feed raw durations into the pinned official scoring tool where practical;
  otherwise prove a local scorer equivalent with golden fixtures.
- [ ] 5.4.1.5 Invalidate score output when completeness, correctness, update, duration,
  or scheduling requirements fail.

### Task 5.4.2: Tune BI execution from measured evidence

Description: Address the largest validated cost centers while retaining reference
plans and answers as regression oracles.

- [ ] 5.4.2.1 Capture per-query plans, cardinality estimates, iterator counts,
  materialization size, memory, and I/O.
- [ ] 5.4.2.2 Improve statistics and join planning for high-cardinality BI patterns.
- [ ] 5.4.2.3 Optimize grouping, top-k, repeated subexpressions, and path operations
  through general engine capabilities.
- [ ] 5.4.2.4 Verify every optimization against complete answer baselines before
  accepting latency changes.

## Section 5.5: Integration Tests

Description: This final section validates BI data state, all operation families,
update batches, reference answers, protocol execution, scoring, and cleanup.

### Task 5.5.1: Validate the complete BI smoke workload

Description: Execute every canonical BI operation against a deterministic smoke
dataset and verify exact typed results.

- [ ] 5.5.1.1 Run all BI reads and variants with smoke validation parameters.
- [ ] 5.5.1.2 Compare full ordered results with the designated reference implementation.
- [ ] 5.5.1.3 Apply representative insert/delete batches and repeat affected answers.
- [ ] 5.5.1.4 Exercise empty, disconnected, tie, overflow, large-aggregation, and
  weighted-path boundary cases.
- [ ] 5.5.1.5 Reset the store and reproduce the initial result hashes exactly.

### Task 5.5.2: Validate BI protocol and scoring

Description: Prove that driver logs, TripleStore artifacts, and score inputs agree.

- [ ] 5.5.2.1 Run shortened non-comparable power and throughput smoke protocols.
- [ ] 5.5.2.2 Verify operation counts, parameter sequence, update positions, timing
  boundaries, and state checksums reconcile across driver and runner logs.
- [ ] 5.5.2.3 Feed golden raw durations to both official and local scoring paths and
  compare score outputs.
- [ ] 5.5.2.4 Inject an incorrect answer, failed update, timeout, and incomplete batch
  and verify score generation is blocked.

### Task 5.5.3: Pass the Phase 5 BI gate

Description: Establish SNB BI as correctness-qualified for scheduled scale runs.

- [ ] 5.5.3.1 Run formatting, strict compilation, affected query/update/path tests,
  and the complete BI smoke integration suite.
- [ ] 5.5.3.2 Verify the operation catalog has no unimplemented mandatory BI item.
- [ ] 5.5.3.3 Generate and validate all BI result, correctness, score-input, environment,
  and disclosure artifacts.
- [ ] 5.5.3.4 Record exact commands, counts, protocol reductions, and excluded large
  scale factors in the phase pull request.
