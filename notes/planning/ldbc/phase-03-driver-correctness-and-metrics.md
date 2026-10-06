# Phase 3: Common Driver, Correctness, and Metrics Foundation

Description: Phase 3 creates the suite-neutral execution model, benchmark-only
driver bridge, correctness pipeline, measurement controls, and artifacts used by
all three LDBC suites. The runner must reject failed or incorrect operations before
computing performance summaries. By the end of the phase, representative reads and
writes from each suite can be driven through one lifecycle with typed parameters,
typed results, deterministic reset behavior, and durable provenance.

---

## Section 3.1: Canonical Operation and Result Model

Description: This section defines a common internal representation without erasing
suite-specific semantics such as result ordering, update dependencies, or scoring.

### Task 3.1.1: Implement versioned operation definitions

Description: Represent canonical reads, writes, batches, validation actions, and
resilience actions as immutable definitions connected to the Phase 1 catalogs.

- [x] 3.1.1.1 Define operation identity, suite profile, upstream ID, kind, parameter
  schema, result schema, ordering, limit, timeout class, and tags.
- [x] 3.1.1.2 Support SPARQL text, native benchmark extensions, update batches, and
  driver callbacks as explicit execution strategies.
- [x] 3.1.1.3 Record the exact source artifact and transformation version for every
  translated operation.
- [x] 3.1.1.4 Reject duplicate IDs, unknown parameters, missing result columns, and
  unclassified execution strategies during catalog loading.

### Task 3.1.2: Implement typed parameter and result codecs

Description: Preserve benchmark datatypes and ordering across Java driver frames,
Elixir execution, RDF conversion, and correctness artifacts.

- [x] 3.1.2.1 Define codecs for IDs, signed integer widths, floating values, dates,
  timestamps, booleans, strings, lists, optional values, and RDF terms.
- [x] 3.1.2.2 Keep parameter substitution separate from raw query text and prevent
  injection through string interpolation.
- [x] 3.1.2.3 Define canonical result rows with explicit column order and type metadata.
- [x] 3.1.2.4 Apply canonical sorting only when the benchmark result contract permits it.
- [x] 3.1.2.5 Detect overflow, precision loss, timezone drift, invalid UTF-8, and
  unbound-versus-null mismatches as correctness failures.

## Section 3.2: Benchmark-Only Driver Bridge and Lifecycle

Description: This section lets canonical upstream drivers invoke TripleStore while
keeping network or process integration out of the product's public embedded API.

### Task 3.2.1: Implement the local bridge protocol

Description: Build the Phase 1-selected local transport with bounded frames,
request correlation, cancellation, health checks, and deterministic shutdown.

- [x] 3.2.1.1 Implement handshake negotiation for protocol version, suite profile,
  dataset manifest, and operation catalog checksum.
- [x] 3.2.1.2 Implement typed execute, batch, reset, checkpoint, health, cancel, and
  shutdown frames.
- [x] 3.2.1.3 Enforce frame-size, operation-count, parameter-size, concurrency, and
  timeout limits.
- [x] 3.2.1.4 Return structured parse, validation, execution, timeout, cancellation,
  storage, reasoning, and bridge errors.
- [x] 3.2.1.5 Prevent bridge startup under normal application supervision or release use.

### Task 3.2.2: Implement store and service ownership

Description: Make one component responsible for every resource used by a benchmark
run so reset and teardown do not rely on garbage collection.

- [x] 3.2.2.1 Open the selected store schema and own its dictionary manager and
  store transaction coordinator.
- [x] 3.2.2.2 Start optional statistics, result-cache, metrics, and reasoning helpers
  only when the selected profile declares them.
- [x] 3.2.2.3 Route stateful benchmark reads and writes through the coordination path
  approved in Phase 1.
- [x] 3.2.2.4 Release lazy result streams, iterators, snapshots, tasks, ports, and
  external driver processes on every exit.
- [x] 3.2.2.5 Emit a final resource-accounting record before store teardown.

## Section 3.3: Execution Protocol and Measurement

Description: This section defines timing boundaries, workload scheduling controls,
cache state, and metric collection without substituting generic runner behavior for
suite-specific rules.

### Task 3.3.1: Implement fail-fast operation execution

Description: Execute reads and writes with complete error accounting and prevent
invalid samples from entering performance calculations.

- [x] 3.3.1.1 Separate setup, parse, plan, execute, materialize, validation, and
  teardown timings where the API exposes those boundaries.
- [x] 3.3.1.2 Apply per-operation and per-phase timeouts, including lazy stream consumption.
- [x] 3.3.1.3 Mark an operation successful only after its complete result has been
  materialized or its update effect has been validated as required.
- [x] 3.3.1.4 Exclude errors, timeouts, cancellations, and incorrect answers from
  latency samples and invalidate any enclosing official-style score.
- [x] 3.3.1.5 Preserve warmup samples separately and prevent them from entering measured output.

### Task 3.3.2: Capture reproducible environment and engine state

Description: Record enough runtime state to explain and reproduce benchmark differences.

- [x] 3.3.2.1 Capture git SHA, dirty state, Elixir, OTP, Rust, RocksDB, dependency,
  OS, kernel, CPU, memory, filesystem, and storage-device metadata.
- [x] 3.3.2.2 Capture TripleStore schema, configuration, cache settings, statistics
  state, reasoning profile, timeout limits, and benchmark adapter version.
- [x] 3.3.2.3 Capture driver process configuration, concurrency, scheduling, warmup,
  duration, random seeds, and scale factor.
- [x] 3.3.2.4 Capture CPU, memory high-water mark, disk use, I/O counters, completion
  rate, and per-operation latency distributions where available.

## Section 3.4: Correctness Baselines and Artifact Schemas

Description: This section normalizes answers, compares them with trusted outputs,
and emits stable artifacts shared by all suites.

### Task 3.4.1: Implement answer normalization and comparison

Description: Compare results according to each operation's ordered or unordered
semantics without hiding datatype or duplicate errors.

- [x] 3.4.1.1 Normalize typed result rows, RDF terms, timestamps, numeric precision,
  and optional values using suite-specific rules.
- [x] 3.4.1.2 Preserve duplicate multiplicity and ordering when required by the operation.
- [x] 3.4.1.3 Compare small answers in full and large answers using row counts plus
  collision-resistant ordered or multiset hashes.
- [x] 3.4.1.4 Record missing, unexpected, misordered, mistyped, and numerically
  divergent values separately.
- [x] 3.4.1.5 Support accepted-divergence files only when the specification permits
  implementation-defined behavior and require a documented reason and source version.

### Task 3.4.2: Implement run artifacts and score gating

Description: Produce machine-readable and human-readable evidence whose schema
distinguishes diagnostic metrics from protocol-defined scores.

- [x] 3.4.2.1 Emit manifest, environment, operation catalog, raw samples, errors,
  correctness, resource, and run-summary JSON files.
- [x] 3.4.2.2 Emit normalized CSV tables and a Markdown report linked to raw evidence.
- [x] 3.4.2.3 Version artifact schemas and include checksums for every referenced input.
- [x] 3.4.2.4 Emit official score fields only when profile, correctness, scheduling,
  duration, and completeness gates all pass.
- [x] 3.4.2.5 Make baseline acceptance a separate explicit command that never runs as
  an automatic side effect of measurement.

## Section 3.5: Integration Tests

Description: This final section validates the complete common lifecycle before any
suite depends on it for performance claims.

### Task 3.5.1: Exercise bridge and resource lifecycle

Description: Drive representative read, write, reset, cancellation, and failure
operations through the real external-process boundary and embedded store.

- [ ] 3.5.1.1 Execute one representative operation from each suite through the bridge.
- [ ] 3.5.1.2 Verify concurrent request correlation and deterministic ordered responses
  where the upstream driver requires them.
- [ ] 3.5.1.3 Verify cancellation and timeout close streams, iterators, snapshots,
  tasks, ports, and store processes.
- [ ] 3.5.1.4 Verify driver crash, BEAM crash simulation, malformed frames, and early
  disconnect leave the fixture recoverable.

### Task 3.5.2: Exercise correctness and artifact gating

Description: Prove that only complete and correct runs can produce comparable scores.

- [ ] 3.5.2.1 Compare representative ordered, unordered, duplicate-bearing, empty,
  numeric, and timestamp results with known answers.
- [ ] 3.5.2.2 Inject parse, execution, timeout, and wrong-answer failures and verify
  no invalid sample contributes to a score.
- [ ] 3.5.2.3 Verify JSON, CSV, and Markdown artifacts agree on operation counts,
  timing samples, errors, correctness, and environment metadata.
- [ ] 3.5.2.4 Verify baseline acceptance requires an explicit command and leaves an
  auditable change.

### Task 3.5.3: Pass the Phase 3 common-runner gate

Description: Establish a clean shared foundation for the three suite implementations.

- [ ] 3.5.3.1 Run strict compilation, formatting, focused resource-lifetime tests,
  bridge protocol tests, and artifact schema tests.
- [ ] 3.5.3.2 Run repeated smoke lifecycles under concurrency and verify stable store
  checksums after reset.
- [ ] 3.5.3.3 Confirm the legacy generic runner is not used for LDBC score generation.
- [ ] 3.5.3.4 Record test commands, counts, excluded large profiles, and remaining
  engine-capability blockers in the phase pull request.
