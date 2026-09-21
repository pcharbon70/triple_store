# Phase 6 Companion: Codebase Correctness and Reliability Remediation

## Overview

Description: Remediate the actionable findings from the 2026-09-21 whole-codebase
review of `main` at `f3f4683bb5d532fe8634fe790a49a825c42447a5`.
The plan follows the repository's phase, section, task, and sub-task pattern.
Phase, section, and task counts reflect implementation dependencies rather than
a fixed template. Creating this plan does not implement or close a finding.

**Status:** Planned.

Source review:
[2026-09-21 entire-codebase review](../../.spec/reviews/2026-09-21T05-30-49-0400-parallel-code-review-entire-codebase.md).

The review executed the default suite after temporarily excluding the unfetched
local `spec_led_ex` declaration and restoring `mix.exs` byte-for-byte. The
baseline was 25 doctests, 10 properties, and 6,728 tests with 36 failures and
53 skipped tests; 345 tagged tests were excluded. The failures are concentrated
in quad Leapfrog planning and result conversion. Strict Credo also identified
`QuadLeapfrog.from_pattern/2` as the primary complexity hotspot.

## Findings and Traceability

The `R-*` identifiers are local to this remediation plan. They are not contract,
acceptance, or conformance identifiers.

| Finding | Priority | Problem | Implementation Phase | Governing requirements and scenarios |
| --- | --- | --- | --- | --- |
| `R-01` | P1 | Quad Leapfrog returns binding maps that the executor cannot consume | Phase 1 | `REQ-QRY-004`, `REQ-QRY-006`, `REQ-QRY-009`, `REQ-QRY-010`; `SCN-005`, `SCN-017` |
| `R-02` | P1 | Graph-bound scans zero-fill an unbound key component and exclude valid rows | Phase 1 | `REQ-QRY-004`, `REQ-QRY-006`; `SCN-005`, `SCN-017` |
| `R-03` | P1 | Query and RDF-derived identifiers are converted into permanent BEAM atoms | Phases 1 and 3 | `REQ-QRY-009`, `REQ-RSN-004`, `REQ-RSN-010`; `SCN-005`, `SCN-009` |
| `R-04` | P1 | A failed multi-operation update leaves earlier operations committed | Phase 2 | `REQ-TXN-002`, `REQ-TXN-005`; `SCN-004`, `SCN-008` |
| `R-05` | P2 | Public SPARQL updates use independent temporary coordinators | Phase 2 | `REQ-TXN-001`, `REQ-TXN-004`, `REQ-TXN-007`; `SCN-008` |
| `R-06` | P2 | Transaction snapshots are allocated but never used by transaction queries | Phase 2 | `REQ-TXN-003`, `REQ-TXN-008`, `REQ-TXN-010`; `SCN-008` |
| `R-07` | P2 | ACL and provenance records use unsafe deserialization without shape validation | Phase 3 | `REQ-STO-005`, `REQ-QRY-006`, `REQ-QRY-009`, `REQ-RSN-011`; `SCN-010`, `SCN-017` |
| `R-08` | P2 | Scheduled backup handles store death without monitoring a lifecycle process | Phase 4 | `REQ-OBS-002`, `REQ-OBS-005`; `SCN-012` |
| `R-09` | P2 | Public boundary errors and Quad Leapfrog types/docs/tests have diverged | Phases 1 and 4 | `REQ-QRY-009`, `REQ-RSN-010`; `SCN-005`, `SCN-009` |

Canonical references:
[query contract](../../specs/contracts/query_execution_contract.md),
[transaction contract](../../specs/contracts/transaction_and_isolation_contract.md),
[storage contract](../../specs/contracts/storage_runtime_contract.md),
[reasoning contract](../../specs/contracts/reasoning_contract.md),
[observability contract](../../specs/contracts/observability_contract.md),
[scenario catalog](../../specs/conformance/scenario_catalog.md), and
[conformance matrix](../../specs/conformance/spec_conformance_matrix.md).

## Planning Rules

- Every defect starts with a failing regression through the closest real public
  or expert entry point. Internal unit tests supplement rather than replace that
  evidence.
- Each task is independently reviewable. A phase may use multiple pull requests
  when its tasks have separate risk or ownership boundaries.
- Each phase ends with an integration-test section. That section is the phase
  gate and must pass before dependent work begins.
- Preserve the pre-existing `mix.exs` edits. Resolve or explicitly isolate the
  `spec_led_ex` dependency before recording validation evidence.
- Keep triple and quad schema behavior distinct. Any shared helper must retain
  the documented tuple orders, key layouts, graph-zero semantics, and explicit
  versus derived storage boundaries.
- Use `ErlangAdapter` for storage operations. Preserve iterator and snapshot
  cleanup on success, exhaustion, early halt, error, and caller exit.
- Update the relevant contract, area spec, acceptance criteria, and scenario
  mapping in the same change as a semantic behavior change.
- Do not use broad module splitting as a substitute for a behavior fix. Extract
  code only where the resulting boundary owns a stable contract, resource, or
  testable decision.

## Phase Overview and Dependencies

| Phase | Focus | Depends on | Exit result |
| --- | --- | --- | --- |
| 1 | Quad query execution and planner contract | None | Graph-variable queries and graph updates return correct bindings without invalid prefixes or atom creation |
| 2 | Transaction coordination and request atomicity | Phase 1, because transaction integration exercises graph updates | One coordinator owns public SPARQL updates and a failed request leaves no explicit-index changes |
| 3 | External identifier and persisted-state safety | Phase 1 binding contract; Phase 2 tagged update errors | External names remain binaries and corrupted ACL/provenance data fails safely |
| 4 | Operational lifecycle, boundary errors, and release qualification | Phases 1–3 | Backup lifecycle and public error behavior are deterministic; all quality gates pass or have recorded blockers |

## Scope and Success Criteria

- Quad queries preserve the exact SPARQL variable name and value representation
  from Leapfrog execution through projection and serialization.
- Graph-only and partially bound patterns encode only valid contiguous index
  prefixes and produce the same answers as the single-iterator reference path.
- Malformed planner input returns a tagged error and every opened iterator is
  closed on all exits.
- No unbounded query, RDF, rule, or persistence identifier reaches
  `String.to_atom/1` or an equivalent atom-creating path.
- Public SPARQL updates for one open store share one long-lived coordinator.
- All operations in one SPARQL Update request preserve sequential visibility
  while committing explicit-index changes atomically at the request boundary.
- Transaction documentation and APIs describe the implemented serialized-read
  model; unused snapshot state and misleading claims are removed or deprecated.
- ACL and provenance decoders use safe decoding, validate schemas, and fail
  closed with tagged errors.
- Scheduled backup stops deterministically when its monitored store lifecycle
  ends and releases timers and monitor references.
- The default test suite returns to zero failures, affected excluded suites pass,
  and strict compilation, formatting, Credo, Dialyzer, and governance checks are
  recorded.

---

## Phase 1: Quad Query Execution and Planner Contract

Description: Restore graph-query correctness first because authorization,
COPY/MOVE/ADD, concurrent-query, and later transaction tests all depend on this
execution path. Establish one binding and iterator-plan contract, repair prefix
construction, and reduce the planner's complexity around those stable rules.

### Section 1.1: Reproduction and Contract Baseline

Description: Capture the current failures through real query and update entry
points, then define the data shapes and ownership rules that the repair must
preserve.

#### Task 1.1.1: Establish deterministic failing regressions

Description: Convert the observed full-suite failures into focused regressions
that distinguish binding conversion, prefix selection, malformed input, and
resource cleanup failures.

- [x] 1.1.1.1 Record the starting commit, worktree changes, toolchain override,
  dependency state, and exact focused test commands.
- [x] 1.1.1.2 Add an end-to-end quad query with subject, predicate, object, and
  graph variables that reproduces `Executor.convert_leapfrog_bindings/1` failure.
- [x] 1.1.1.3 Add default-graph and named-graph queries with only the graph bound;
  compare their results with the single-iterator execution path.
- [x] 1.1.1.4 Identify the focused COPY, MOVE, and ADD regressions that exercise the same
  graph-variable result stream through update execution.
- [x] 1.1.1.5 Add malformed-pattern and iterator-initialization failure
  cases that assert tagged errors and complete iterator cleanup.

#### Task 1.1.2: Define the canonical planner and binding contracts

Description: Make variable keys, value encoding, iterator-plan entries, prefix
depth, and ownership explicit so producer, consumer, typespec, documentation,
and tests can share one definition.

- [x] 1.1.2.1 Choose binary SPARQL variable names as the canonical binding keys;
  preserve anonymous-variable behavior without manufacturing keys.
- [x] 1.1.2.2 Define one iterator-plan entry type containing scan level,
  selected index, actual prefix depth, and encoded prefix.
- [x] 1.1.2.3 Document whether bound components appear in result bindings; remove
  test-only binding behavior from production output if callers do not require it.
- [x] 1.1.2.4 Define stream ownership: the stream owns all opened iterators until
  exhaustion, early halt, explicit close, or construction failure.
- [x] 1.1.2.5 Update `QuadLeapfrog` moduledoc, typespecs, and nearby developer
  documentation before changing implementation behavior.

### Section 1.2: Binding and Prefix Correctness

Description: Repair the two independent runtime defects while keeping the
single-iterator path as an executable correctness oracle during the transition.

#### Task 1.2.1: Align Leapfrog bindings with executor bindings

Description: Produce the executor's canonical binding map directly from the
Leapfrog stream and remove the incompatible tagged-tuple conversion.

- [x] 1.2.1.1 Change extraction helpers to retain binary variable names and
  dictionary IDs without calling `String.to_atom/1`.
- [x] 1.2.1.2 Remove or replace `convert_leapfrog_bindings/1` so it accepts exactly
  the documented stream result and never interprets map enumeration as tags.
- [x] 1.2.1.3 Preserve existing outer bindings when merging a graph-pattern result;
  reject conflicting values using the same semantics as other join paths.
- [x] 1.2.1.4 Verify projection, FILTER, JOIN, GRAPH variable, and result decoding
  receive the expected variable key type.
- [x] 1.2.1.5 Add a stress regression with many unique variable names and assert
  the atom count does not grow with query input.

#### Task 1.2.2: Encode contiguous prefixes and accurate iterator depths

Description: Build prefixes from the selected index's leading bound components
only, without zero-filling gaps or claiming a deeper level than the prefix
actually represents.

- [x] 1.2.2.1 Centralize index-order projection for GSPO, GPOS, SPOG, and POSG.
- [x] 1.2.2.2 Stop prefix encoding at the first unbound component and return both
  the binary prefix and its component depth.
- [x] 1.2.2.3 Select an index whose leading components maximize real bindings;
  do not treat a non-leading bound value as an encodable prefix.
- [x] 1.2.2.4 Pass the actual prefix depth into iterator setup and seek logic.
- [x] 1.2.2.5 Cover graph ID zero, named graphs, all-bound, all-variable, sparse
  bindings, and each index order with table-driven tests.

#### Task 1.2.3: Return typed construction errors and preserve fallback safety

Description: Ensure invalid patterns and partial construction failures return
useful tagged errors, while optimizer fallback cannot leak resources or hide a
failure that occurs only during lazy consumption.

- [x] 1.2.3.1 Add a public validation clause for non-quad and malformed patterns.
- [x] 1.2.3.2 Validate bound IDs, variable forms, index choices, and plan entries
  before opening the first iterator.
- [x] 1.2.3.3 Use one owned physical iterator, eliminating partial multi-iterator acquisition.
- [x] 1.2.3.4 Restrict executor fallback to construction-time unsupported/error
  results; propagate lazy execution failures as tagged stream errors where the
  stream API supports them.
- [x] 1.2.3.5 Retain the existing `QuadOperations` reference path in integration tests rather
  than duplicating production planning logic in fixtures.

### Section 1.3: Planner Decomposition

Description: Reduce the complexity that allowed representation and prefix rules
to drift, without changing the behavior established in Sections 1.1 and 1.2.

#### Task 1.3.1: Extract stable planning responsibilities

Description: Split `from_pattern/2` around validation, component analysis, index
selection, prefix encoding, iterator opening, and stream binding extraction.

- [x] 1.3.1.1 Extract pure component-analysis and index-selection functions with
  exhaustive pattern tests.
- [x] 1.3.1.2 Extract pure prefix encoding from iterator construction.
- [x] 1.3.1.3 Isolate iterator acquisition in one function that owns rollback of
  partially acquired resources.
- [x] 1.3.1.4 Isolate key decoding and binding construction by index order.
- [x] 1.3.1.5 Remove stale three-tuple/four-tuple branches and test-only production
  behavior after all callers use the canonical types.
- [x] 1.3.1.6 Run strict Credo on the affected modules and record any remaining
  complexity that is justified by the execution algorithm.

Section evidence: strict Credo reported no issues across `QuadScanPlan` and
`QuadLeapfrog`; the focused planner and executor run completed 82 tests with
zero failures. Existing warnings in older test modules are outside this
section's production-code complexity scope.

### Section 1.4: Integration Tests

Description: Prove the repaired planner behaves correctly through query,
authorization, graph update, streaming, and concurrency entry points before any
transaction architecture changes begin.

#### Task 1.4.1: Exercise graph-query behavior through real APIs

Description: Validate that planner internals compose with the parser, optimizer,
executor, dictionary, indices, authorization hooks, and result materialization.

- [ ] 1.4.1.1 Run the complete `quad_leapfrog_test.exs` and
  `executor_quad_integration_test.exs` suites.
- [ ] 1.4.1.2 Run graph query, GRAPH clause, query authorization, and result-stream
  integration tests for default and named graphs.
- [ ] 1.4.1.3 Run COPY/MOVE/ADD and update authorization tests that previously
  failed in binding conversion.
- [ ] 1.4.1.4 Run iterator lifetime tests with exhaustion, early halt, caller exit,
  and construction failure.
- [ ] 1.4.1.5 Compare Leapfrog and reference execution results for a matrix of
  bound positions, graph IDs, empty datasets, and duplicate matches.

#### Task 1.4.2: Pass the Phase 1 quality gate

Description: Establish a clean query baseline and synchronized contract evidence
for downstream transaction work.

- [ ] 1.4.2.1 Run strict compilation and formatting checks.
- [ ] 1.4.2.2 Run all query, update authorization, and graph-management tests.
- [ ] 1.4.2.3 Run the default suite and confirm the 36-review-failure baseline is
  eliminated or record unrelated failures with reproducible evidence.
- [ ] 1.4.2.4 Update query specs, acceptance criteria, and conformance mappings for
  the canonical binding and iterator-plan contracts.
- [ ] 1.4.2.5 Record test commands, counts, skipped/excluded tags, and remaining
  risks in the phase pull request.

---

## Phase 2: Transaction Coordination and Request Atomicity

Description: Give public SPARQL updates one coordinator per open store, preserve
sequential SPARQL Update semantics in a staged request, and commit all explicit
index mutations once. Align the transaction API with its actual serialized-read
model instead of allocating snapshots that no reader consumes.

### Section 2.1: Store-Owned Transaction Coordinator

Description: Replace independent temporary coordinators with a lifecycle-owned
coordinator shared by every copy of one open store handle.

#### Task 2.1.1: Define coordinator ownership and failure behavior

Description: Specify how `open/2`, `close/1`, process links, caller exits, and
multiple handle copies manage one transaction coordinator.

- [ ] 2.1.1.1 Document coordinator ownership in the runtime lifecycle spec and
  distinguish SPARQL update serialization from direct load/insert/delete paths.
- [ ] 2.1.1.2 Decide whether the coordinator is linked directly to the opener or
  owned by a small store supervisor; preserve the dictionary manager's lifecycle.
- [ ] 2.1.1.3 Define open rollback when the coordinator fails after RocksDB or the
  dictionary manager has started.
- [ ] 2.1.1.4 Define close ordering, repeated-close behavior, and coordinator death
  behavior before or during an update.
- [ ] 2.1.1.5 Preserve explicit expert callers that supply their own transaction
  manager, with one documented precedence rule.

#### Task 2.1.2: Start and stop one coordinator with the store

Description: Make the normal store handle carry a live coordinator and remove
the per-call temporary-manager branch from public SPARQL updates.

- [ ] 2.1.2.1 Start the coordinator during `open/2` after storage and dictionary
  initialization; include it in the returned store handle.
- [ ] 2.1.2.2 Roll back already-started resources in reverse order when any open
  step fails.
- [ ] 2.1.2.3 Route `TripleStore.update/2` through the store-owned coordinator.
- [ ] 2.1.2.4 Stop the coordinator before closing dictionary/storage resources and
  ensure in-flight calls receive a deterministic tagged failure.
- [ ] 2.1.2.5 If temporary coordinators remain for compatibility, guard their
  cleanup with `try/after` and mark the path as expert-only.

### Section 2.2: Request-Level Mutation Session

Description: Preserve operation order and later-operation visibility without
publishing writes until every operation has validated and the full request can
commit atomically.

#### Task 2.2.1: Introduce a staged update session

Description: Represent pending explicit mutations and a read overlay for one
parsed SPARQL Update request, including the metadata needed for authorization,
counts, cache invalidation, and telemetry.

- [ ] 2.2.1.1 Define an update-session type containing the base read view, pending
  triple/quad puts and deletes, affected graphs, and operation results.
- [ ] 2.2.1.2 Normalize mutations into canonical per-column-family keys using
  existing Index, QuadIndex, and adapter helpers.
- [ ] 2.2.1.3 Implement overlay reads so a later operation observes earlier staged
  inserts/deletes as required by SPARQL Update sequencing.
- [ ] 2.2.1.4 Detect contradictory or duplicate staged mutations and preserve
  DELETE-before-INSERT semantics and documented affected counts.
- [ ] 2.2.1.5 Keep dictionary allocation outside the explicit-index atomicity claim;
  document that a failed request may leave unused dictionary IDs.

#### Task 2.2.2: Plan supported update operations without early commits

Description: Change data, MODIFY, and graph-management executors to append
validated intents to the session rather than submitting independent batches.

- [ ] 2.2.2.1 Convert INSERT DATA, DELETE DATA, and triple/quad MODIFY paths to
  staged intents.
- [ ] 2.2.2.2 Convert COPY, MOVE, ADD, CLEAR, CREATE, DROP, LOAD, and supported
  graph operations while preserving sequential visibility and SILENT behavior.
- [ ] 2.2.2.3 Resolve and authorize every graph target before adding its first
  mutation to the session.
- [ ] 2.2.2.4 Propagate parse, lookup, conversion, authorization, and storage-plan
  failures as tagged errors that discard the entire session.
- [ ] 2.2.2.5 Define explicit handling for any operation that cannot participate in
  the staged model; reject unsupported combinations before mutation rather than
  silently weakening atomicity.

#### Task 2.2.3: Commit once and publish side effects after success

Description: Submit the accumulated mutations through one adapter batch and
perform cache, statistics, and telemetry side effects only after that batch
succeeds.

- [ ] 2.2.3.1 Extend the adapter's supported mixed batch format only as needed to
  cover every touched explicit index and graph metadata column family.
- [ ] 2.2.3.2 Submit one batch after all operations have planned successfully.
- [ ] 2.2.3.3 On batch failure, return a tagged storage error and retain unchanged
  explicit indices, caches, statistics, and success telemetry.
- [ ] 2.2.3.4 On success, invalidate plan/result caches and refresh statistics once
  using the accumulated affected-store/graph metadata.
- [ ] 2.2.3.5 Replace the inaccurate “rollback is automatic” comment with the exact
  request-level commit boundary.

### Section 2.3: Serialized Read Semantics and Snapshot Cleanup

Description: Make the transaction API accurately expose serialized query/update
behavior. Remove native snapshot work that currently changes neither execution
context nor observable isolation.

#### Task 2.3.1: Remove unused update snapshots and misleading state

Description: Retain the consistent serialized queue while eliminating the dead
`current_snapshot` mechanism or deprecating it without claiming concurrent
snapshot reads.

- [ ] 2.3.1.1 Add a deterministic test proving a query through the same
  coordinator waits for an in-progress staged update and sees only the committed
  pre- or post-request state.
- [ ] 2.3.1.2 Remove snapshot creation/release from synchronous update execution.
- [ ] 2.3.1.3 Deprecate `current_snapshot/1` with a documented compatibility period,
  or redefine it only if a real snapshot-aware read API is implemented.
- [ ] 2.3.1.4 Remove unused `update_in_progress` state if it remains permanently
  false, or update state transitions if monitoring callers require it.
- [ ] 2.3.1.5 Update module docs, public lifecycle specs, and the transaction
  contract to describe serialized reads and the remaining direct-write boundary.

### Section 2.4: Integration Tests

Description: Verify coordinator ownership, sequential request semantics,
failure atomicity, and serialized visibility through public and expert APIs.

#### Task 2.4.1: Exercise request atomicity and concurrency

Description: Test successful and failed multi-operation requests against every
explicit index and under controlled concurrent access without timing sleeps.

- [ ] 2.4.1.1 Execute a successful first operation followed by a deterministic
  planning failure; assert all explicit indices remain byte-for-byte unchanged.
- [ ] 2.4.1.2 Inject final batch failure and assert the same unchanged state,
  unchanged cache generation, and failure telemetry.
- [ ] 2.4.1.3 Verify a later operation reads an earlier staged insert/delete and the
  final result matches sequential SPARQL Update semantics.
- [ ] 2.4.1.4 Run concurrent public updates against one store and assert they pass
  through one coordinator in deterministic order.
- [ ] 2.4.1.5 Run transaction queries during an update using barriers; assert no
  partial multi-index or intermediate multi-operation state is observable.
- [ ] 2.4.1.6 Reopen the store and verify triple and quad indices, graph metadata,
  and result-cache invalidation remain coherent.

#### Task 2.4.2: Pass the Phase 2 quality gate

Description: Establish traceable evidence for the transaction requirements
before security and persisted-data changes build on the new error behavior.

- [ ] 2.4.2.1 Run transaction, update executor, MODIFY, graph management,
  COPY/MOVE/ADD, cache invalidation, and concurrency suites.
- [ ] 2.4.2.2 Run `SCN-008` conformance coverage for triple and quad schemas.
- [ ] 2.4.2.3 Run strict compilation, formatting, and affected Credo checks.
- [ ] 2.4.2.4 Update `REQ-TXN-*`, `AC-RT-07`, scenario evidence, user/developer
  guides, and facade examples to match the implemented coordinator model.
- [ ] 2.4.2.5 Record remaining isolation limits for direct loader/insert/delete
  paths without representing them as transaction-backed operations.

---

## Phase 3: External Identifier and Persisted-State Safety

Description: Remove unbounded atom creation from query and reasoning inputs,
then make ACL and provenance decoding safe, shape-checked, and fail-closed.
Preserve readable persisted data where it conforms to the supported schema.

### Section 3.1: Atom-Safe Identifier Handling

Description: Inventory production atom-creation paths and replace every path
reachable from SPARQL, RDF, restored artifacts, configuration strings, or
caller-provided process names with bounded atoms or binary identifiers.

#### Task 3.1.1: Complete the production atom-creation inventory

Description: Classify dynamic atom creation by trust boundary and document the
finite internal vocabularies that may legitimately use existing atoms.

- [ ] 3.1.1.1 Inventory `String.to_atom/1`, `binary_to_atom/1`, interpolated
  registered names, and atom-producing decode paths under `lib/`.
- [ ] 3.1.1.2 Mark each source as compile-time finite, validated existing atom, or
  externally unbounded input.
- [ ] 3.1.1.3 Include query/cache process names, update helper property keys,
  benchmark artifact fields, rule names, and rule-optimizer batch names.
- [ ] 3.1.1.4 Add an allowlist comment or type for each intentionally finite atom
  conversion; reject undocumented dynamic creation.

#### Task 3.1.2: Support binary names in reasoning rules and batches

Description: Extend internal identifier types so generated rule and batch names
can remain binaries without changing semantic equality or diagnostic output.

- [ ] 3.1.2.1 Extend `Rule` name types and constructors to accept stable binary
  identifiers while preserving existing built-in atom names.
- [ ] 3.1.2.2 Generate specialized property and inverse-property rule names as
  binaries derived from validated IRIs.
- [ ] 3.1.2.3 Generate optimizer batch identifiers as binaries or opaque bounded
  references; keep them out of registered process names.
- [ ] 3.1.2.4 Update maps, equality checks, logging, telemetry metadata, provenance,
  and serialization that currently assume atom names.
- [ ] 3.1.2.5 Test many unique property IRIs and rule variables while asserting
  stable atom counts and deterministic rule identity.

### Section 3.2: Safe ACL Decoding and Fail-Closed Authorization

Description: Decode ACL records with safe mode, validate their exact schema,
and prevent read/corruption failures from becoming empty or permissive ACL state.

#### Task 3.2.1: Define and enforce the persisted ACL schema

Description: Centralize ACL encoding/decoding rules around supported key and
permission types with explicit corruption errors.

- [ ] 3.2.1.1 Define the accepted ACL map shape, principal key forms, permission
  values, owner representation, and format/version behavior.
- [ ] 3.2.1.2 Decode with `:erlang.binary_to_term(binary, [:safe])` and validate
  every key and value before use.
- [ ] 3.2.1.3 Return `{:error, {:corrupt_acl, reason}}` or the repository's chosen
  tagged equivalent for unsafe, malformed, or incompatible data.
- [ ] 3.2.1.4 Propagate storage read errors during ACL mutation; do not replace them
  with `%{}` and overwrite an unknown existing policy.
- [ ] 3.2.1.5 Ensure authorization checks fail closed when ACL state cannot be read.
- [ ] 3.2.1.6 Verify current valid ACL records remain readable without a rewrite;
  define an explicit migration only if a format change becomes necessary.

### Section 3.3: Safe Provenance Decoding

Description: Apply safe decoding and structural validation to derived-fact
provenance while preserving explicit-versus-derived and graph-scope semantics.

#### Task 3.3.1: Validate derivation records at the persistence boundary

Description: Treat persisted provenance as versioned structured data and return
tagged corruption errors to every caller.

- [ ] 3.3.1.1 Define the supported derivation record shape, fact key types, rule
  identifier types, premises, graph scope, and optional metadata.
- [ ] 3.3.1.2 Decode with safe mode and validate the complete record before
  constructing runtime provenance values.
- [ ] 3.3.1.3 Update lookup, explanation, deletion, and rederivation callers to
  propagate or explicitly handle tagged corruption errors.
- [ ] 3.3.1.4 Preserve valid existing records and add a versioned migration path if
  binary rule identifiers require persisted-format evolution.
- [ ] 3.3.1.5 Add malformed, truncated, unsafe-term, wrong-shape, and unsupported
  version fixtures.

### Section 3.4: Integration Tests

Description: Exercise atom safety and persisted-record validation through real
query, reasoning, authorization, backup/restore, and reopen workflows.

#### Task 3.4.1: Verify safety across runtime boundaries

Description: Prove that external identifiers do not allocate atoms and invalid
persisted state produces tagged failures without weakening authorization or
corrupting derived data.

- [ ] 3.4.1.1 Run a high-cardinality query-variable corpus and compare atom counts
  before and after garbage collection and query completion.
- [ ] 3.4.1.2 Compile and optimize rules for many unique property IRIs; verify
  deterministic results and bounded atom growth.
- [ ] 3.4.1.3 Inject corrupted ACL data and assert reads and writes fail closed,
  authorized data is not exposed, and existing bytes are not overwritten.
- [ ] 3.4.1.4 Inject corrupted provenance and assert explanation, deletion, and
  rederivation return tagged errors without changing explicit or derived facts.
- [ ] 3.4.1.5 Backup and restore valid ACL/provenance stores, reopen them, and
  verify policies and lineage survive unchanged.

#### Task 3.4.2: Pass the Phase 3 quality gate

Description: Confirm the safety changes remain compatible with authorization,
reasoning, persistence, and release build requirements.

- [ ] 3.4.2.1 Run authorization, update authorization, rule compiler, rule
  optimizer, provenance, rederivation, incremental reasoning, and backup suites.
- [ ] 3.4.2.2 Run `SCN-010`, `SCN-011`, `SCN-012`, and `SCN-017` affected coverage.
- [ ] 3.4.2.3 Run strict compilation, formatting, Credo, and Dialyzer for changed
  types and call sites.
- [ ] 3.4.2.4 Update storage, reasoning, authorization, and operational docs with
  format compatibility and corruption behavior.
- [ ] 3.4.2.5 Record valid-format compatibility evidence and any required migration
  command or operator action.

---

## Phase 4: Operational Lifecycle and Release Qualification

Description: Finish the review remediation by making scheduled-backup ownership
real, replacing remaining reviewed boundary crashes with tagged errors, and
running repository-wide qualification over the composed changes.

### Section 4.1: Scheduled Backup Ownership

Description: Tie the scheduler to an actual store lifecycle process so timers
stop when storage becomes unusable and shutdown races have deterministic results.

#### Task 4.1.1: Monitor the store lifecycle explicitly

Description: Monitor the dictionary manager or a new store supervisor rather
than the store map, and retain enough state to distinguish expected shutdown
from unrelated process messages.

- [ ] 4.1.1.1 Choose the lifecycle process established by Phase 2 and document
  why its death means scheduled backups must stop.
- [ ] 4.1.1.2 Call `Process.monitor/1` during scheduler initialization and store the
  monitored PID and reference.
- [ ] 4.1.1.3 Match `:DOWN` by the stored reference and PID; ignore unrelated
  monitor messages.
- [ ] 4.1.1.4 Cancel timers, demonitor when appropriate, and release in-progress
  backup resources during terminate/normal stop.
- [ ] 4.1.1.5 Define behavior when the store dies during a backup and when an
  operator explicitly stops the scheduler first.
- [ ] 4.1.1.6 Emit observable stop/failure metadata without repeatedly scheduling
  backups against a closed store.

### Section 4.2: Reviewed Public Error Boundaries

Description: Convert the remaining concrete review examples of public crashes
or silent defaults into typed errors without launching an unbounded rewrite of
every pattern match in the repository.

#### Task 4.2.1: Propagate materialization input failures

Description: Replace the hard match in local fact loading with tagged storage
errors that flow through the existing materialization result contract.

- [ ] 4.2.1.1 Make `load_facts_from_db/2` return `{:ok, facts}` or a tagged error
  for iterator, decode, and storage failures.
- [ ] 4.2.1.2 Propagate the result through `materialize/2` without claiming that the
  local in-memory path persists or returns derived facts.
- [ ] 4.2.1.3 Verify iterator cleanup on scan failure and early decode termination.
- [ ] 4.2.1.4 Add injected adapter-failure tests for the public facade and the
  lower-level reasoner entry point.

#### Task 4.2.2: Audit touched fallback and error paths

Description: Review only modules changed by this plan for rescues, hard matches,
and error-to-default conversions that could conceal the repaired failures.

- [ ] 4.2.2.1 Check query fallback paths for lazy exceptions that escape the
  construction-time rescue boundary.
- [ ] 4.2.2.2 Check transaction and authorization paths for errors converted into
  zero counts, empty maps, or successful telemetry.
- [ ] 4.2.2.3 Check scheduled backup and persistence paths for repeated retries
  after terminal lifecycle or corruption errors.
- [ ] 4.2.2.4 Add focused regressions for each verified issue; document inspected
  non-issues instead of making speculative changes.

### Section 4.3: Documentation and Maintainability Closure

Description: Synchronize the project's normative and explanatory documents and
retain only refactors that make the repaired contracts easier to maintain.

#### Task 4.3.1: Synchronize contracts, specs, and guides

Description: Update current-status sections and examples so they describe the
implemented behavior and no longer preserve remediated caveats as current facts.

- [ ] 4.3.1.1 Update transaction current-status text for coordinator ownership,
  request atomicity, and serialized reads.
- [ ] 4.3.1.2 Update query planning docs for canonical bindings, plan entries,
  graph prefixes, fallback, and stream ownership.
- [ ] 4.3.1.3 Update reasoning and storage docs for binary rule identifiers and
  safe persisted-record decoding.
- [ ] 4.3.1.4 Update operations guides for scheduled-backup ownership and shutdown.
- [ ] 4.3.1.5 Synchronize acceptance criteria, scenario catalog/matrix evidence,
  guides, moduledocs, and examples; do not mark standards conformance from
  structural validation alone.

#### Task 4.3.2: Close bounded maintainability findings

Description: Remove obsolete helpers and duplication created by the old paths,
then use static analysis as evidence rather than as a target for cosmetic churn.

- [ ] 4.3.2.1 Remove dead temporary-transaction, unused snapshot, old binding
  conversion, zero-filled prefix, and unsafe decode helpers after callers migrate.
- [ ] 4.3.2.2 Consolidate safe term-decoding mechanics only where ACL and provenance
  error schemas remain explicit.
- [ ] 4.3.2.3 Review cache lifecycle/name helpers for atom creation while preserving
  the distinct semantics of PlanCache, Query.Cache, and SPARQL.QueryCache.
- [ ] 4.3.2.4 Re-run complexity checks on `QuadLeapfrog` and executor paths; accept
  remaining complexity only with narrow tests and documented ownership.
- [ ] 4.3.2.5 Keep unrelated large-module decomposition as separate follow-up work
  unless a phase change establishes a stable extraction boundary.

### Section 4.4: Integration Tests

Description: Qualify the composed remediation across schemas, persistence,
concurrency, operations, excluded risk-focused suites, and repository governance.
This is the final release gate for the plan.

#### Task 4.4.1: Run composed end-to-end scenarios

Description: Exercise interactions that individual phase tests cannot establish,
especially graph authorization during atomic updates and lifecycle behavior
across close/reopen boundaries.

- [ ] 4.4.1.1 Run a named-graph workflow covering load, authorized query, atomic
  multi-operation update, result-cache invalidation, backup, close, restore,
  reopen, and query verification.
- [ ] 4.4.1.2 Repeat relevant storage/query/update workflows for triple schema to
  detect quad-specific refactoring regressions.
- [ ] 4.4.1.3 Run concurrent update/query scenarios with barriers and verify no
  partial index fanout or intermediate request state.
- [ ] 4.4.1.4 Run corrupted ACL/provenance scenarios through restore/reopen and
  assert fail-closed tagged outcomes.
- [ ] 4.4.1.5 Close a store during idle and in-progress scheduled backups; verify
  scheduler termination, timer cleanup, and observable status.
- [ ] 4.4.1.6 Verify atom counts remain bounded across combined query and reasoning
  workloads with unique external identifiers.

#### Task 4.4.2: Run repository quality and conformance gates

Description: Produce exact, reproducible validation evidence and distinguish
executed behavior from structural documentation checks.

- [ ] 4.4.2.1 Run `./scripts/compile_strict.sh` and
  `mix format --check-formatted`.
- [ ] 4.4.2.2 Run affected focused suites, then the complete default `mix test`.
- [ ] 4.4.2.3 Run relevant excluded `:lifetime_safety`, `:slow`, and
  `:large_dataset` tests deliberately; record tags that remain out of scope.
- [ ] 4.4.2.4 Run `mix credo --strict` and `mix dialyzer --format short`.
- [ ] 4.4.2.5 Run specs, guides, RFC, code-doc, and conformance validation scripts.
- [ ] 4.4.2.6 Run Rust formatter, Clippy, parser tests, and Elixir parser tests only
  if native parser code or its boundary changed.
- [ ] 4.4.2.7 Record toolchain overrides, dependency/NIF state, commands, counts,
  failures, skips, exclusions, and artifact locations.

#### Task 4.4.3: Close findings with evidence

Description: Mark only behavior demonstrated by source inspection and passing
tests as complete, and preserve any residual risks as explicit follow-up work.

- [ ] 4.4.3.1 Map each `R-*` finding to implementation commits, focused regressions,
  integration tests, requirements, acceptance criteria, and scenarios.
- [ ] 4.4.3.2 Confirm the default suite has zero failures or identify each remaining
  failure as a reproducible, independently tracked blocker.
- [ ] 4.4.3.3 Verify no generated NIFs, databases, benchmark artifacts, logs, or
  temporary dependency edits are tracked.
- [ ] 4.4.3.4 Review public API compatibility, persisted-format compatibility, and
  operator migration notes before release tagging.
- [ ] 4.4.3.5 Update this plan's status and checkboxes only after the corresponding
  evidence is committed and reviewable.
