# Phase 6: SNB Interactive

Description: Phase 6 implements the pinned audited-stable SNB Interactive workload
and prepares a separately versioned path for newer deep-delete semantics. Interactive
combines parameterized neighbourhood reads with sustained state-changing operations,
so transaction coordination, scheduling, result latency, and state reset are part of
correctness. By the end of the phase, the canonical driver can execute the complete
selected operation mix against TripleStore and cross-validate every operation.

---

## Section 6.1: Interactive Profile and Operation Packaging

Description: This section turns the selected Interactive driver catalog into
versioned TripleStore operations without mixing audited-stable and newer profiles.

### Task 6.1.1: Package complex and short reads

Description: Translate every complex and short read in the selected profile with
its canonical parameters, result schema, ordering, limit, and frequency.

- [ ] 6.1.1.1 Create one operation definition per canonical complex read.
- [ ] 6.1.1.2 Create one operation definition per canonical short read.
- [ ] 6.1.1.3 Preserve exact result typing, row order, tie breakers, duplicate behavior,
  and limit semantics.
- [ ] 6.1.1.4 Link translations to upstream operation definitions and reference queries.
- [ ] 6.1.1.5 Verify all instantiated reads parse and map only to terms defined by the
  Phase 2 RDF mapping.

### Task 6.1.2: Package state-changing operations

Description: Translate every insert and other update operation in the audited-stable
profile and preserve its dependency on generated update-stream state.

- [ ] 6.1.2.1 Define typed parameters and mapped RDF mutations for every operation.
- [ ] 6.1.2.2 Preserve creation timestamps, relationship properties, entity identity,
  and operation ordering.
- [ ] 6.1.2.3 Define the atomic boundary and expected postcondition for each operation.
- [ ] 6.1.2.4 Record which short reads are generated as consequences of updates and
  preserve that driver behavior.

## Section 6.2: Interactive Read Correctness and Execution

Description: This section closes query-engine gaps exposed by local neighbourhood,
multi-hop, top-k, and parameter-sensitive Interactive reads.

### Task 6.2.1: Implement complete read semantics

Description: Make every translated read return the same typed and ordered answer as
the selected reference implementation.

- [ ] 6.2.1.1 Add a focused failing regression for each unsupported or incorrect read.
- [ ] 6.2.1.2 Implement required expressions, OPTIONAL behavior, multi-hop traversal,
  date filtering, ordering, and top-k semantics through canonical engine modules.
- [ ] 6.2.1.3 Verify relationship-property mappings do not introduce duplicate or
  missing paths.
- [ ] 6.2.1.4 Cross-validate full answers over multiple validation parameter sets.
- [ ] 6.2.1.5 Capture explain plans and result cardinalities for later performance tuning.

### Task 6.2.2: Bound local traversal and materialization

Description: Ensure highly connected people or messages cannot make benchmark reads
leak resources or silently truncate results.

- [ ] 6.2.2.1 Apply explicit timeout, path-depth, intermediate-result, and memory limits.
- [ ] 6.2.2.2 Return typed limit failures and invalidate the enclosing run.
- [ ] 6.2.2.3 Release all iterators and streams on success, early limit, timeout,
  cancellation, and driver disconnect.
- [ ] 6.2.2.4 Verify limits are high enough for canonical parameters at supported scales
  or report the scale as unsupported.

## Section 6.3: Update Atomicity and Stateful Correctness

Description: This section makes the Interactive mutation stream safe under concurrent
driver activity and aligns query visibility with the benchmark's transactional model.

### Task 6.3.1: Implement atomic update operations

Description: Apply each canonical mutation through one store-owned coordination path
with complete index fanout and cache invalidation.

- [ ] 6.3.1.1 Translate each update into one canonical request-level mutation plan.
- [ ] 6.3.1.2 Preserve all relevant indices, graph metadata, and relationship-property
  statements atomically.
- [ ] 6.3.1.3 Validate preconditions and reject duplicate, missing, or out-of-order entities.
- [ ] 6.3.1.4 Publish cache, statistics, telemetry, and derived-state effects only after commit.
- [ ] 6.3.1.5 Verify storage failures and operation errors leave all accepted answers unchanged.

### Task 6.3.2: Implement benchmark read/write visibility

Description: Ensure concurrent reads observe a state allowed by the selected
Interactive specification rather than partial fanout or an undocumented direct-read path.

- [ ] 6.3.2.1 Route driver reads and writes through the Phase 1-approved coordinator
  or snapshot boundary.
- [ ] 6.3.2.2 Define and test visibility before, during, and after every update type.
- [ ] 6.3.2.3 Prevent independent temporary coordinators or direct writes from entering
  a comparable workload run.
- [ ] 6.3.2.4 Add explicit read and write timeout handling compatible with driver retries.
- [ ] 6.3.2.5 Update transaction contracts and public documentation for any new
  isolation behavior implemented here.

## Section 6.4: Canonical Driver Scheduling and Throughput

Description: This section connects TripleStore to the selected Interactive driver
and reproduces its warmup, dependency, frequency, and throughput-control behavior.

### Task 6.4.1: Implement driver bindings

Description: Map canonical Java driver operation classes to bridge operations and
return results in the exact schema expected by validation and measurement modes.

- [ ] 6.4.1.1 Implement driver callbacks for all complex reads, short reads, and updates.
- [ ] 6.4.1.2 Decode typed parameters and encode typed results without lexical shortcuts.
- [ ] 6.4.1.3 Propagate operation errors and timeouts to the driver as failed operations.
- [ ] 6.4.1.4 Implement driver initialization, cleanup, health, and store reset hooks.
- [ ] 6.4.1.5 Keep the adapter version aligned with the pinned driver catalog checksum.

### Task 6.4.2: Reproduce Interactive workload scheduling

Description: Run the canonical operation mix at controlled rates and preserve
driver-defined dependencies between updates and short reads.

- [ ] 6.4.2.1 Implement validation mode using official validation parameters.
- [ ] 6.4.2.2 Implement warmup with the required duration and exclude warmup samples.
- [ ] 6.4.2.3 Implement measured execution with driver-controlled target rate and concurrency.
- [ ] 6.4.2.4 Capture scheduled, started, completed, failed, late, and retried operation counts.
- [ ] 6.4.2.5 Compute throughput only from protocol-complete, correctness-qualified runs.

## Section 6.5: Newer Deep-Delete Profile

Description: This section adds newer Interactive delete semantics only after the
audited-stable profile is complete, and keeps its datasets, driver, results, and
claims separately versioned.

### Task 6.5.1: Assess and implement delete operations

Description: Map the newer profile's explicit and cascading delete semantics to
TripleStore without weakening the completed stable profile.

- [ ] 6.5.1.1 Pin the newer driver, Datagen, update streams, and reference implementations.
- [ ] 6.5.1.2 Catalog all delete operations and their cascade, relationship, and
  ordering semantics.
- [ ] 6.5.1.3 Implement atomic mapped-RDF deletions across every affected index and
  relationship-property representation.
- [ ] 6.5.1.4 Invalidate or rederive caches, statistics, and inferred facts after commit.
- [ ] 6.5.1.5 Cross-validate post-delete reads and complete graph state against a
  reference implementation.

### Task 6.5.2: Keep profile data and reporting isolated

Description: Prevent results from the stable and deep-delete profiles from sharing
baselines or being compared under one undifferentiated benchmark name.

- [ ] 6.5.2.1 Use distinct profile IDs, dataset manifests, operation catalogs,
  parameter sets, artifact directories, and baseline keys.
- [ ] 6.5.2.2 Reject driver/profile mismatches during handshake.
- [ ] 6.5.2.3 Label the profile according to its actual upstream readiness and audit status.
- [ ] 6.5.2.4 Document which performance results can and cannot be compared across profiles.

## Section 6.6: Integration Tests

Description: This final section validates complete stable-profile execution and the
separately versioned deep-delete extension through real driver, store, update, reset,
and correctness boundaries.

### Task 6.6.1: Validate every stable-profile operation

Description: Execute the full operation catalog against a deterministic fixture and
compare exact typed results and state transitions.

- [ ] 6.6.1.1 Run every complex and short read with official validation parameters.
- [ ] 6.6.1.2 Run every update operation in canonical stream order.
- [ ] 6.6.1.3 Compare read results and post-update probes with the selected reference implementation.
- [ ] 6.6.1.4 Exercise duplicate IDs, missing dependencies, storage failures, timeout,
  and cancellation without partial state.
- [ ] 6.6.1.5 Restore the pristine store and reproduce initial result hashes exactly.

### Task 6.6.2: Validate concurrent driver execution

Description: Exercise scheduling, isolation, resource ownership, and throughput
accounting under a shortened non-comparable workload.

- [ ] 6.6.2.1 Run validation mode followed by warmup and measured smoke periods.
- [ ] 6.6.2.2 Verify no read observes partial update fanout under concurrent load.
- [ ] 6.6.2.3 Reconcile driver and TripleStore operation counts, timestamps, failures,
  retries, and latency samples.
- [ ] 6.6.2.4 Inject bridge and driver termination and verify deterministic recovery and reset.

### Task 6.6.3: Validate the deep-delete profile separately

Description: Prove that delete semantics and profile isolation hold without changing
stable-profile baselines.

- [ ] 6.6.3.1 Execute every supported delete and validate cascading mapped-RDF effects.
- [ ] 6.6.3.2 Verify affected reads match reference results after each deletion checkpoint.
- [ ] 6.6.3.3 Verify stable-profile manifests and baselines reject deep-delete artifacts.
- [ ] 6.6.3.4 Verify failed deletes leave explicit, derived, cache, and statistics state unchanged.

### Task 6.6.4: Pass the Phase 6 Interactive gate

Description: Establish the selected Interactive profile as correctness-qualified
for scheduled scale runs.

- [ ] 6.6.4.1 Run formatting, strict compilation, transaction, update, query,
  resource-lifetime, and complete Interactive smoke integration tests.
- [ ] 6.6.4.2 Verify no mandatory stable-profile operation remains unimplemented.
- [ ] 6.6.4.3 Generate and validate result, correctness, throughput-input, environment,
  profile, and disclosure artifacts.
- [ ] 6.6.4.4 Record exact commands, counts, schedule reductions, deep-delete status,
  and excluded large scale factors in the phase pull request.
