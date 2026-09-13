# Phase 5 Companion: Correctness and Authorization Remediation

## Overview

Description: Remediate the six findings from the 2026-09-09 code review of
`main` at `3583d23569a6ee178a8382382be42f6f670fedbe`. This companion extends
Production Hardening and follows the existing phase, section, task, and
sub-task hierarchy. Creating this plan does not implement or close a finding.

**Status:** In progress. Phase 1 is complete; Phases 2–4 are pending.

The review was primarily source-based. A standalone reproduction using the real
result-cache module and a telemetry stub demonstrated reuse across execution
callbacks. Full application tests were not run: dependencies were absent and
the checked-in toolchain pins did not resolve locally. Every finding needs a
behavior regression against the real execution path before remediation is
marked complete. Preserve the existing unrelated `mix.exs` edit.

## Findings and Traceability

The `F-*` identifiers below are local to this plan, not new contract IDs.

| Finding | Priority | Problem | Implementation Section | Contracts | Scenarios |
| --- | --- | --- | --- | --- | --- |
| F-01 | P1 | Result-cache identity omits store and authorization context | 1.2 | `REQ-QRY-006`, `REQ-QRY-008`, `REQ-QRY-011` | `SCN-007`, `SCN-017` |
| F-02 | P1 | Variable graph MODIFY targets bypass write authorization | 1.3 | `REQ-QRY-006`, `REQ-QRY-009`, `REQ-QRY-011` | `SCN-017` |
| F-03 | P1 | Quad MODIFY separates writes and suppresses storage failures | 2.1 | `REQ-STO-004`, `REQ-TXN-002`, `REQ-TXN-005`, `REQ-QRY-009` | `SCN-004`, `SCN-008` |
| F-04 | P1 | Data updates fail to invalidate materialized query results | 2.2 | `REQ-QRY-008`, `REQ-TXN-006` | `SCN-007` |
| F-05 | P1 | Fully bound reasoning premises are accepted without existence checks | 3.1 | `REQ-RSN-001`, `REQ-RSN-002`, `REQ-RSN-009` | `SCN-009`, `SCN-011` |
| F-06 | P1 | `:per_graph_cf` writes SPOG keys where derived readers expect GSPO | 3.2 | `REQ-STO-005`, `REQ-STO-006`, `REQ-RSN-006`, `REQ-RSN-007` | `SCN-010`, `SCN-011` |

Canonical references: [query contract](../../specs/contracts/query_execution_contract.md),
[storage contract](../../specs/contracts/storage_runtime_contract.md),
[transaction contract](../../specs/contracts/transaction_and_isolation_contract.md),
[reasoning contract](../../specs/contracts/reasoning_contract.md),
[scenario catalog](../../specs/conformance/scenario_catalog.md), and
[conformance matrix](../../specs/conformance/spec_conformance_matrix.md).

## Planning Granularity

Phases represent delivery gates and dependencies. Sections group related work;
tasks represent independently reviewable outcomes; sub-tasks describe the steps
needed for that outcome. Their counts are not targets and need not match across
phases or sections. Choose the sections independently for each phase according
to its implementation needs. Do not add sections to fill a template or split
tasks merely to make counts different. Matching counts are fine when the work
justifies them. Split or combine work as implementation evidence warrants.

Regression-first implementation is the default for every finding: capture the
failure through the real entry point, implement the fix, and record the passing
assertion. Extend nearby fixtures and tests; do not create a separate test task
unless its setup or integration scope makes it independently meaningful. Keep
fixture cleanup reliable on failures. Update affected specs with each fix.

## Phase Overview and Dependencies

| Phase | Focus | Depends On | Exit Result | Status |
| --- | --- | --- | --- | --- |
| 1 | Reproduction baseline, cache isolation, graph authorization | None | Context-safe cached reads and authorized resolved write targets | Complete |
| 2 | Atomic quad MODIFY and mutation-driven cache invalidation | Phase 1 cache identity and authorization | Failed MODIFY leaves explicit indices unchanged; committed writes invalidate affected results | Planned |
| 3 | Ground-premise verification and canonical derived storage | Section 1.1 test environment | Sound negative-premise handling and GSPO-derived round trips | Planned |
| 4 | Integration, compatibility, and conformance evidence | Phases 1–3 | All six findings verified through real runtime tests and documented evidence | Planned |

The phase boundaries reflect different delivery gates: prevent unauthorized
access, establish committed-write/cache consistency, restore reasoning
correctness and persisted-data compatibility, then qualify their interactions.
Final qualification is a separate gate because the combined guarantees cannot
be established by any individual fix's tests.

Deliver bounded changes per task or closely related section. Phase 3 is logically
independent of Phase 2, but the default delivery sequence is 1 → 2 → 3 → 4.
This dependency structure does not require parallel agent work.

## Scope and Success Criteria

- Identical query text cannot reuse another store's or unauthorized user's result.
- Variable graph targets receive the same write checks as constant targets.
- One quad MODIFY commits its explicit-index deletes and inserts in one batch,
  or returns an error without partial explicit-index changes.
- Committed data or ACL changes cannot leave reusable stale results, including
  an in-flight query attempting to refill the cache after invalidation.
- Every required ground premise is checked against the appropriate fact source.
- Supported derived-quad writers/readers agree on GSPO encoding and report errors.
- Each finding has failing-before/passing-after regression evidence, focused
  tests, relevant integration coverage, and synchronized specifications.

This plan does not close the separate gaps in store-wide coordination across
temporary transaction managers, transaction snapshot propagation, whole-request
rollback across multiple SPARQL operations, default local facade materialization
persistence, or generic RDF loader graph preservation. Preserve those documented
limits. Discoveries needed to fix these six findings belong here; broader changes
need their own scoped follow-up rather than an implicit transaction redesign.

---

## Phase 1: Authorization and Cache Isolation

Description: Establish real regression evidence, then prevent cached read leakage
and unauthorized graph writes before changing mutation internals.

### Section 1.1: Reproduction Baseline

- [x] **Section 1.1 Status**

#### Task 1.1.1: Establish a usable runtime baseline

- [x] **Task 1.1.1 Status**
- [x] 1.1.1.1 Record the starting commit, working-tree changes, installed toolchains,
  and dependency/NIF availability; preserve unrelated edits.
- [x] 1.1.1.2 Resolve the supported local toolchain and fetch/build dependencies;
  record temporary overrides without silently changing project pins or locks.
- [x] 1.1.1.3 Run strict compilation and the affected existing test suites; record
  baseline failures separately from failures introduced by remediation.
- [x] 1.1.1.4 Establish isolated DB fixtures and deterministic failure injection
  at the storage boundary; avoid sleeps and global test-state collisions.

### Section 1.2: Result-Cache Context Isolation (F-01)

- [x] **Section 1.2 Status**

Primary files: `lib/triple_store/sparql/query.ex`,
`lib/triple_store/query/cache.ex`, `lib/triple_store/sparql/authorization.ex`.
Tests: `test/triple_store/query/cache_test.exs`,
`test/triple_store/sparql/query_test.exs`,
`test/triple_store/sparql/authorization_test.exs`.

#### Task 1.2.1: Separate cache entries by store instance

- [x] **Task 1.2.1 Status**
- [x] 1.2.1.1 Run identical query text against two stores sharing one cache and
  assert each returns only its own data, including differing schema contexts.
- [x] 1.2.1.2 Include store-instance and query identity plus result-affecting execution
  options in production cache keys; ensure closing and reopening a store does
  not make an earlier instance's entries reusable.
- [x] 1.2.1.3 Verify same-store repeated queries still hit the cache and match uncached
  answers; test both shared and explicitly named caches.

#### Task 1.2.2: Bind cache reuse to effective authorization

- [x] **Task 1.2.2 Status**
- [x] 1.2.2.1 Warm a protected graph query as an authorized actor; repeat as an
  unauthorized actor and assert no protected result is returned.
- [x] 1.2.2.2 Extend context identity to cover the effective user, roles, and privileged
  or bypass flags; a user ID alone is insufficient.
- [x] 1.2.2.3 Define behavior for contexts lacking a reliable authorization identity:
  bypass caching rather than share an entry with a privileged context.
- [x] 1.2.2.4 Prevent stale authorization after ACL grant/revoke or role changes,
  using a validated revision/invalidation policy or bypassing affected caching.
- [x] 1.2.2.5 Test public and privileged contexts, role changes, and ACL revocation
  after a cache fill; assert denied contexts never receive protected results.

#### Task 1.2.3: Handle persisted cache compatibility

- [x] **Task 1.2.3 Status**
- [x] 1.2.3.1 Update cache persistence/restore and reopen behavior so legacy unscoped
  keys and entries from an earlier store instance cannot be reused accidentally.
- [x] 1.2.3.2 Preserve explicit named-cache support and optional-cache behavior;
  keep the separate `SPARQL.QueryCache` implementation outside this change unless
  a verified caller requires it.
- [x] 1.2.3.3 Restore a cache containing legacy unscoped entries and verify they are
  discarded or inaccessible; verify entries cannot cross store reopen boundaries.

### Section 1.3: Resolved Graph Write Authorization (F-02)

- [x] **Section 1.3 Status**

Primary files: `lib/triple_store/sparql/update/modify.ex`,
`lib/triple_store/sparql/update/helpers.ex`.
Tests: `test/triple_store/sparql/update_authorization_test.exs`,
`test/triple_store/sparql/modify_quad_test.exs`.

#### Task 1.3.1: Authorize resolved targets before any explicit write

- [x] **Task 1.3.1 Status**
- [x] 1.3.1.1 Demonstrate a read-only actor cannot write through `GRAPH ?g`.
- [x] 1.3.1.2 Retain useful static checks, then collect all resolved delete and
  insert graph targets after WHERE evaluation and template substitution.
- [x] 1.3.1.3 Require write authorization on every resolved target before the
  first explicit mutation; WHERE read permission must not imply write permission.
- [x] 1.3.1.4 Preserve default-graph rules, supported graph-term representations,
  existing privileged behavior, and SPARQL treatment of unbound template terms.
- [x] 1.3.1.5 Return the existing tagged authorization error if any target is
  denied, without deleting or inserting data in permitted graphs first.
- [x] 1.3.1.6 Test constant plus variable templates, multiple bindings spanning
  allowed and denied graphs, and different delete/insert target graphs.
- [x] 1.3.1.7 Assert every explicit index is unchanged after rejection; verify
  fully authorized writes and existing constant-graph tests still pass.

**Phase 1 exit gate:** Reproductions for F-01/F-02 pass through actual query and
update APIs; no cache hit crosses store/authorization boundaries and no denied
resolved target receives a write. Other findings remain open until their phases.

Phase 1 evidence: strict compilation passed with temporary
`ASDF_ELIXIR_VERSION=1.19.5-otp-28` and `ASDF_ERLANG_VERSION=28.3.1`
overrides. The final focused lifecycle, cache, query, and quad MODIFY suites
passed 226 tests, including three targeted constant/resolved-target authorization
cases. The full update-authorization file retains a baseline failure in its
ADD test: an existing fully variable quad scan reaches
`Executor.convert_leapfrog_bindings/1` with a map instead of the tuple list that
function expects. That separate executor defect does not occur in the bounded
F-02 regression and is not caused by this phase.

---

## Phase 2: Atomic Writes and Cache Coherence

Description: Make a single quad MODIFY failure-atomic, then connect successful
mutations to the context-scoped result caches introduced in Phase 1.

### Section 2.1: Atomic Quad MODIFY (F-03)

- [ ] **Section 2.1 Status**

Primary files: `lib/triple_store/sparql/update/modify.ex`,
`lib/triple_store/quad_index.ex`, `lib/triple_store/quad_operations.ex`,
`lib/triple_store/backend/rocksdb/erlang_adapter.ex`.
Tests: `test/triple_store/sparql/modify_quad_test.exs`,
`test/triple_store/backend/rocksdb/write_batch_test.exs`.

#### Task 2.1.1: Build one explicit-index mutation batch

- [ ] **Task 2.1.1 Status**
- [ ] 2.1.1.1 Validate and authorize all instantiated data, then encode deletes
  and inserts for `gspo`, `gpos`, `spog`, and `posg` using canonical helpers.
- [ ] 2.1.1.2 Commit the complete operation through one supported mixed batch;
  preserve DELETE-before-INSERT semantics when the same quad appears in both.
- [ ] 2.1.1.3 Remove error-to-zero conversions and propagate conversion/storage
  failures. Distinguish a missing delete target from an actual lookup failure.
- [ ] 2.1.1.4 Specify affected-count behavior for duplicate templates, missing
  deletes, and no-op changes, then retain compatible behavior where possible.
- [ ] 2.1.1.5 Document the atomicity boundary: dictionary allocations may precede
  the batch; this fix does not provide whole-request rollback or store-wide
  isolation for WHERE evaluation across independent writers.

#### Task 2.1.2: Verify failure and success across all indices

- [ ] **Task 2.1.2 Status**
- [ ] 2.1.2.1 Inject failure at batch submission and assert a tagged error with
  byte-for-byte unchanged explicit index contents.
- [ ] 2.1.2.2 Test multi-graph deletes/inserts, overlapping quads, empty operations,
  duplicate bindings, and conversion errors before commit.
- [ ] 2.1.2.3 Verify successful changes across all four indices and after reopen;
  rerun triple MODIFY tests to preserve its existing mixed-batch behavior.
- [ ] 2.1.2.4 Assert invalidation and success telemetry do not report a failed
  mutation as a successful commit.

### Section 2.2: Mutation-Driven Result Invalidation (F-04)

- [ ] **Section 2.2 Status**

Primary files: `lib/triple_store/sparql/update_executor.ex`,
`lib/triple_store/sparql/update/`, `lib/triple_store/transaction.ex`,
`lib/triple_store/query/cache.ex`, and supported direct mutation callers.
Tests: `test/triple_store/sparql/update_cache_invalidation_test.exs`,
`test/triple_store/query/cache_test.exs`, `test/triple_store/sparql/query_test.exs`.

#### Task 2.2.1: Establish mutation and cache ownership coverage

- [ ] **Task 2.2.1 Status**
- [ ] 2.2.1.1 Inventory public and expert insert/delete/MODIFY, graph operations,
  loader, and ACL mutation paths; record where plan, result, and statistics
  caches are actually invalidated. Do not conflate those caches.
- [ ] 2.2.1.2 Establish a shared invalidation mechanism that reaches every affected
  active named result cache using the Phase 1 store identity; retain explicit
  plan/statistics invalidation requirements.
- [ ] 2.2.1.3 Record the supported mutation-to-cache invalidation matrix in the query
  planning spec; use it to identify concrete call sites and regression cases.

#### Task 2.2.2: Invalidate on committed data changes

- [ ] **Task 2.2.2 Status**
- [ ] 2.2.2.1 Warm a query, mutate matching data, and repeat the identical query;
  cover insert, delete, MODIFY, graph operations, both schemas, and named caches.
- [ ] 2.2.2.2 Invalidate after each committed operation, including earlier commits
  in a multi-operation request whose later operation fails. Avoid invalidation
  that depends solely on an overall `{:ok, count}` result.
- [ ] 2.2.2.3 Handle variable predicates, graph-wide operations, and unknown
  dependencies conservatively; prefer full store-scoped invalidation over stale
  answers when precise predicate tracking is insufficient.
- [ ] 2.2.2.4 Test variable-predicate queries, zero-result caches, unrelated stores,
  no-cache execution, and the supported direct mutation paths from the inventory.
- [ ] 2.2.2.5 Verify a failed batch preserves data and a later failed operation
  does not leave stale results from earlier committed operations.

#### Task 2.2.3: Reject stale cache fills across mutations

- [ ] **Task 2.2.3 Status**
- [ ] 2.2.3.1 Use explicit process barriers to reproduce invalidation racing a
  pending cache fill; assert the next post-commit query sees current data.
- [ ] 2.2.3.2 Prevent an in-flight pre-mutation computation from repopulating a
  valid cache entry after invalidation, for example by checking a generation at
  insertion. Define post-commit visibility without claiming snapshot isolation.

#### Task 2.2.4: Keep cache availability separate from write outcomes

- [ ] **Task 2.2.4 Status**
- [ ] 2.2.4.1 Preserve operation success when the optional cache is absent; avoid
  turning a committed write into an ambiguous retryable failure due to cache work.
- [ ] 2.2.4.2 Test absent and stopped cache processes around a successful write; assert
  the result still accurately describes the committed mutation and future cache
  use cannot revive entries from before it.

**Phase 2 exit gate:** F-03/F-04 regressions pass. Failed quad MODIFY leaves all
explicit indices unchanged; committed mutations and ACL changes cannot leave
reusable stale results in the supported cache scopes.

---

## Phase 3: Reasoning Soundness and Derived Storage

Description: Correct premise evaluation and make derived-quad persistence
compatible with its readers without reinterpreting ambiguous existing data.

### Section 3.1: Ground Premise Existence (F-05)

- [ ] **Section 3.1 Status**

Primary files: `lib/triple_store/reasoner/delta_computation.ex`,
`lib/triple_store/reasoner/semi_naive.ex`, and their lookup providers.
Tests: `test/triple_store/reasoner/delta_computation_test.exs`,
`test/triple_store/reasoner/semi_naive_test.exs`,
`test/triple_store/reasoner/reasoning_correctness_test.exs`.

#### Task 3.1.1: Verify ground facts through the lookup contract

- [ ] **Task 3.1.1 Status**
- [ ] 3.1.1.1 Replace unconditional acceptance of fully bound non-delta premises
  with an exact existence check against the supplied fact lookup.
- [ ] 3.1.1.2 Cover triple and quad pattern shapes, preserving graph bindings,
  repeated variables, and term representation used by the rule compiler.
- [ ] 3.1.1.3 Confirm lookup providers include the appropriate explicit and derived
  facts for later iterations; do not invent facts or lose derivations by changing
  the ground lookup contract alone.
- [ ] 3.1.1.4 Preserve typed failure behavior for backend lookup errors; distinguish
  unavailable storage from a valid empty match rather than claiming convergence.

#### Task 3.1.2: Exercise negative premises and multi-iteration closure

- [ ] **Task 3.1.2 Status**
- [ ] 3.1.2.1 Test `p(x,y) AND q(x,y) -> r(x,y)` with only `p(a,b)` present:
  no `r(a,b)` may be derived. Add `q(a,b)` and assert the positive case.
- [ ] 3.1.2.2 Repeat with already-ground rule terms, reversed body order, repeated
  variables, and quad premises whose matching fact exists only in another graph.
- [ ] 3.1.2.3 Verify a premise derived in an earlier iteration can satisfy a later
  rule, and sequential/parallel evaluation reaches the same final fact set.
- [ ] 3.1.2.4 Inject lookup failure and verify it cannot be reported as a successful
  complete fixpoint. Exercise existing rule profiles as well as synthetic rules.

### Section 3.2: Canonical Derived-Quad Encoding (F-06)

- [ ] **Section 3.2 Status**

Primary files: `lib/triple_store/reasoner/graph_scoped_reasoner.ex`,
`lib/triple_store/reasoner/derived_store.ex`,
`lib/triple_store/reasoner/reasoning_config.ex`.
Tests: `test/triple_store/reasoner/derived_store_test.exs`,
`test/triple_store/reasoner/section_7_8_5_derived_store_quad_test.exs`,
`test/triple_store/reasoner/graph_scoped_reasoning_integration_test.exs`.

#### Task 3.2.1: Align writer encoding and error handling

- [ ] **Task 3.2.1 Status**
- [ ] 3.2.1.1 Use distinct nonzero subject/predicate/object IDs and graph IDs so
  swapped fields cannot accidentally pass. Check raw keys and decoded results.
- [ ] 3.2.1.2 Route `:per_graph_cf` writes through the canonical GSPO derived
  writer; explicitly convert `{s,p,o,g}` to `{g,s,p,o}` where required.
- [ ] 3.2.1.3 Define and document the existing strategy's target graph behavior;
  fix byte ordering without silently changing graph-selection semantics.
- [ ] 3.2.1.4 Batch related derived writes and propagate storage failures instead
  of discarding `put` errors and unconditionally returning `:ok`.
- [ ] 3.2.1.5 Verify other supported strategies and derived readers agree on the
  same key layout; preserve separation from explicit quad indices.

#### Task 3.2.2: Verify persisted lookup and maintenance

- [ ] **Task 3.2.2 Status**
- [ ] 3.2.2.1 Materialize with `:per_graph_cf`, close/reopen, and verify derived
  lookup and deletion through the public DerivedStore APIs.
- [ ] 3.2.2.2 Test storage failure, multiple batches, and explicit facts remaining
  unchanged; distinguish persistence from normal SPARQL inferred-result visibility.

#### Task 3.2.3: Define recovery for previously malformed derived data

- [ ] **Task 3.2.3 Status**
- [ ] 3.2.3.1 Assess previously written SPOG-derived keys: both layouts are 32 bytes,
  so do not infer their format from key length or blindly rewrite mixed contents.
- [ ] 3.2.3.2 Document a backup-first, explicitly scoped rebuild from authoritative
  explicit facts for affected stores, including provenance/status refresh and
  verification. Do not automatically delete derived data on open or deploy.
- [ ] 3.2.3.3 Exercise the documented recovery procedure on a disposable affected-store
  fixture, preserving explicit data and confirming the rebuilt derived contents.

**Phase 3 exit gate:** F-05/F-06 regressions pass, absent premises never generate
facts, later derived premises remain usable, and persisted derived quads survive
reopen and maintenance with the correct field ordering.

---

## Phase 4: Integration and Conformance Evidence

Description: Verify that the fixes compose and make completion claims traceable
to executed behavior rather than documentation validation alone.

### Section 4.1: Release Readiness

- [ ] **Section 4.1 Status**

Documentation and regression evidence are updated with each fix. This section
checks their completeness and the interactions between fixes; it does not defer
per-task tests or specification updates until the end.

#### Task 4.1.1: Verify interactions between the fixes

- [ ] **Task 4.1.1 Status**
- [ ] 4.1.1.1 Warm caches for two stores and multiple actors; perform authorized
  variable-graph MODIFY and verify atomic results, cache freshness, and isolation.
- [ ] 4.1.1.2 Repeat with authorization denial and injected write failure; verify
  no partial explicit data changes or protected result disclosure.
- [ ] 4.1.1.3 Combine multi-iteration reasoning, canonical derived persistence,
  reopen, and incremental deletion; compare expected explicit/derived sets.
- [ ] 4.1.1.4 Verify fixture cleanup, iterator lifetime, and named-service isolation;
  use the existing quad iterator cleanup regression alongside changed query paths.

#### Task 4.1.2: Complete repository quality gates

- [ ] **Task 4.1.2 Status**
- [ ] 4.1.2.1 Run `./scripts/compile_strict.sh`, `mix format --check-formatted`,
  focused tests, `mix test`, `mix credo --strict`, and `mix dialyzer --format short`.
- [ ] 4.1.2.2 Run relevant excluded tests deliberately; record that default ExUnit
  excludes benchmark, large_dataset, slow, and lifetime_safety tags.
- [ ] 4.1.2.3 Run specs, guides, RFC, and code-doc validators plus
  `./scripts/run_conformance.sh`; record intentional skips and blockers precisely.
- [ ] 4.1.2.4 Run bounded Wikidata parser/corpus/smoke checks when query behavior
  changes; compare answer correctness before interpreting performance differences.

#### Task 4.1.3: Review evidence and close verified findings

- [ ] **Task 4.1.3 Status**
- [ ] 4.1.3.1 Confirm the query/authorization, transaction batch-boundary, and reasoning
  specs reflect the fixes, with actual regression paths in AC/SCN evidence mappings.
- [ ] 4.1.3.2 Check that guides cover cache compatibility, ACL invalidation, derived-data
  recovery, and any caller-visible error/count changes.
- [ ] 4.1.3.3 Update `AGENTS.md` caveats only where fixes are verified. Retain the
  separate documented gaps listed in Scope; do not claim full transaction or
  reasoning conformance from these six fixes.
- [ ] 4.1.3.4 For each F-ID, record the fix commit/PR, exact regression test path,
  failing-before/passing-after evidence, validation environment, and remaining limits.
- [ ] 4.1.3.5 Check completed sub-tasks first, then their tasks and sections; mark
  a phase complete only when its exit gate and required checks pass.
- [ ] 4.1.3.6 Keep unresolved findings open if runtime validation is blocked;
  document the blocker without treating source inspection as a passing test.

**Phase 4 exit gate:** All six findings have executed regression evidence and
passing applicable quality gates. Compatibility/recovery instructions are
reviewable, and remaining unrelated implementation gaps remain explicit.

## Completion Evidence

| Finding | Fix Commit / PR | Regression Evidence | Validation Result | Status |
| --- | --- | --- | --- | --- |
| F-01 | Pending | Pending | Not run | Open |
| F-02 | Pending | Pending | Not run | Open |
| F-03 | Pending | Pending | Not run | Open |
| F-04 | Pending | Pending | Not run | Open |
| F-05 | Pending | Pending | Not run | Open |
| F-06 | Pending | Pending | Not run | Open |
