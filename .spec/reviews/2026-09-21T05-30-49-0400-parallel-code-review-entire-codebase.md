---
kind: parallel_code_review
status: complete
title: Parallel Code Review - Entire Codebase
created_at: 2026-09-21T05:30:49-0400
review_target: Entire codebase and current worktree
repository: /home/ducky/code/semantic/triple_store
base_ref: origin/main
head_ref: main plus current worktree
review_lanes:
  - factual
  - qa
  - senior_engineering
  - security
  - consistency
  - redundancy
  - elixir
result: findings
---

# Parallel Code Review - Entire Codebase

## Review Context

- Repository: `/home/ducky/code/semantic/triple_store`
- Target: entire codebase and current worktree
- Base: `origin/main` at `f3f4683bb5d532fe8634fe790a49a825c42447a5`
- Head: `main` at the same commit, plus a pre-existing uncommitted `mix.exs` edit
- Reviewed at: `2026-09-21T05:30:49-0400`
- Review lanes: factual, QA, senior engineering, security, consistency, redundancy, Elixir
- Scale sampled: about 1,050 tracked files, 430 Credo-analyzed source files, and 97,883 lines under `lib/`

The review combined architecture/spec inspection, focused source tracing through public API, query, update, authorization, reasoning, backup, and storage paths, the full default ExUnit suite, and strict Credo. The existing `mix.exs` worktree edit was preserved byte-for-byte.

## Executive Summary

Release-blocking correctness defects remain in the quad Leapfrog execution path. The default suite reports 36 failures: 14 integration, authorization, update, and concurrency failures share a binding-format crash, while the remainder are concentrated in the `QuadLeapfrog` contract, planning, and graph-prefix behavior. The transaction contract also records three unmet guarantees in the current implementation: store-wide writer coordination, snapshot-aware transaction reads, and whole-request rollback.

The highest-value remediation order is: repair and simplify `QuadLeapfrog`; remove runtime atom creation from query/RDF input; make multi-operation updates request-atomic; then settle the transaction coordinator's actual isolation model. Unsafe persisted-term decoding and scheduled-backup lifecycle handling should follow.

## Blocking Findings

### [P1] Quad Leapfrog returns a binding shape the executor cannot consume

- `lib/triple_store/sparql/executor.ex:1059-1066,1109-1115`
- `lib/triple_store/sparql/leapfrog/quad_leapfrog.ex:1264-1285`

`QuadLeapfrog.stream/1` emits ordinary maps such as `%{p: id}`, but `convert_leapfrog_bindings/1` enumerates those maps and only accepts tagged tuples shaped as `{:variable, name}` or `{:bound, id}`. A valid graph-variable query therefore raises `FunctionClauseError`. The full suite reproduces this in graph queries, COPY/MOVE/ADD updates, authorization, and concurrent read tests.

Define one binding representation at the `QuadLeapfrog`/executor boundary. Prefer the executor's existing binary variable keys, remove the conversion if the stream already emits the final map, and add end-to-end tests for an all-variable graph query and COPY/MOVE/ADD.

### [P1] Graph-bound scans manufacture an impossible two-component prefix

- `lib/triple_store/sparql/leapfrog/quad_leapfrog.ex:287-341`

For `{:quad, ?s, ?p, ?o, graph_id}`, the GSPO values are `[graph_id, nil, nil, nil]`. `build_prefix_with_bound/2` substitutes the missing subject with zero and builds `<<graph_id, 0>>`. Since allocated subject IDs are nonzero, the iterator excludes valid records. The planner then declares prefix depth 3 even though only the graph is bound. This explains the empty graph-scoped results among the `QuadLeapfrog` failures.

Build only the longest contiguous bound prefix in index order and derive the iterator level from its real depth. Cover the default graph, named graphs, a graph-only prefix, and non-leading bound components.

### [P1] Query and RDF input is converted to permanent BEAM atoms

- `lib/triple_store/sparql/leapfrog/quad_leapfrog.ex:1264-1285`
- `lib/triple_store/reasoner/rule_compiler.ex:590-636`
- `lib/triple_store/reasoner/rule_optimizer.ex:531-540`

`String.to_atom/1` is applied to SPARQL variable names and IRI-derived rule/batch names. Atoms are not garbage collected, so repeated unique external names can exhaust the VM atom table and terminate the node. The comments in `RuleCompiler` explicitly claim to avoid atom exhaustion while still calling `String.to_atom/1`.

Keep external identifiers as binaries. If a finite internal vocabulary truly requires atoms, map it explicitly or use `String.to_existing_atom/1` after validation. Update rule name types and telemetry/diagnostic formatting rather than atomizing arbitrary content.

### [P1] A failed multi-operation update leaves earlier operations committed

- `lib/triple_store/sparql/update_executor.ex:180-197`
- `lib/triple_store/transaction.ex:424-457`
- Contract: `specs/contracts/transaction_and_isolation_contract.md:7-16,23-34`

`UpdateExecutor.execute/2` executes and commits each parsed operation before moving to the next. When a later operation fails, `reduce_while/3` returns the error but earlier writes remain. `Transaction`'s “rollback is automatic” comment is only valid for a single unapplied write batch and conflicts with `REQ-TXN-005`, which requires a failed update to leave no partial explicit-index mutation.

Plan the entire request into one atomic storage batch, or introduce an explicit rollback/journal protocol. Add a regression that performs a successful first operation followed by a deterministic failure and verifies every explicit index remains unchanged.

## Concerns

### [P2] Public updates do not share store-wide writer coordination

- `lib/triple_store.ex:295-319,1611-1625`
- Contract: `specs/contracts/transaction_and_isolation_contract.md:7-16,23-26`

`open/2` always returns `transaction: nil`; each `update/2` call therefore starts an independent transaction GenServer. Concurrent facade calls and direct loader mutations do not share a writer queue, so per-process serialization does not satisfy `REQ-TXN-001` or `REQ-TXN-004`. Also, `GenServer.stop/2` is skipped if `Transaction.update/2` exits or times out, allowing a temporary coordinator to outlive the caller.

Own one coordinator for each open store and stop it in `close/1`, or register a coordinator by stable store identity. At minimum, protect the temporary process with `try/after`.

### [P2] The snapshot-facing transaction API does not provide snapshot reads

- `lib/triple_store/transaction.ex:347-470`
- Contract: `specs/contracts/transaction_and_isolation_contract.md:9,14,16,23-25`

The GenServer handles updates and queries synchronously, so queries to the same coordinator wait behind a write. The update path creates a snapshot but never publishes it in `current_snapshot`, passes it to a read context, or changes state. The query path executes against only `%{db: db, dict_manager: dict_manager}`. Existing tests merely assert that `current_snapshot/1` is always `nil`.

Choose and document one model. Either implement concurrent snapshot reads with snapshot/read options propagated through the adapter and executor, or remove the dead snapshot creation and misleading public state until that capability exists. Validate it with a controlled concurrent read/write test without sleeps.

### [P2] Persisted ACL and provenance terms are deserialized without safe mode or shape validation

- `lib/triple_store/sparql/authorization.ex:545-560,753-820`
- `lib/triple_store/reasoner/derivation_provenance.ex:453-463`

These paths call `:erlang.binary_to_term/1` on database bytes. A corrupted, restored, or otherwise untrusted database can reconstruct unsafe term types and allocate atoms. Other project persistence paths already use `binary_to_term(binary, [:safe])`, so the safer local pattern exists.

Decode with `[:safe]`, validate the exact expected map/list/derivation schema, and return a tagged corruption error. Do not silently replace a read error with an empty ACL map during a write.

### [P2] Scheduled backup has a dead store-monitor handler

- `lib/triple_store/scheduled_backup.ex:173-204,252-257`

The server stores the TripleStore handle, which is a map, but never calls `Process.monitor/1`. Its `{:DOWN, ...}` callback can therefore never observe store shutdown. Closing a store while the scheduler remains alive leaves its timer running and turns subsequent backups into recurring failures.

Monitor a real owner process such as the dictionary manager and retain the monitor reference, or require and document explicit scheduler ownership. Add a lifecycle test that closes the store while scheduled backup is active.

### [P2] `QuadLeapfrog`'s public contract, types, tests, and implementation have diverged

- `lib/triple_store/sparql/leapfrog/quad_leapfrog.ex:102-108,213-232,281,323-345,405`
- `test/triple_store/sparql/leapfrog/quad_leapfrog_test.exs:515-1364`

The iterator-plan type and examples describe three-element entries, while current paths return four-element entries. The docs describe one iterator per unbound variable, while three-variable patterns produce one iterator. `from_pattern/2` also raises on malformed input even though the documented API and test expect a tagged error. These inconsistencies make the already complex planner difficult to repair safely.

Specify the planner result and binding contract in one type/module, validate public inputs at `from_pattern/2`, and replace internal-shape assertions with behavioral scans where possible.

## Suggestions

- Decompose `QuadLeapfrog.from_pattern/2` before extending it. Strict Credo reports cyclomatic complexity 29 plus several deeply nested functions in the same module. Extract index selection, prefix construction, iterator ownership, and binding decoding behind small typed functions.
- Reduce the size of the highest-risk modules (`executor.ex` at about 3,953 lines, `optimizer.ex` at about 3,290, `loader.ex` at about 2,684, and `erlang_adapter.ex` at about 2,074). Split by responsibility while preserving the public facade and resource-ownership boundaries.
- Consolidate duplicate cache naming and lifecycle helpers only where semantics match. `SPARQL.PlanCache`, `TripleStore.Query.Cache`, and `SPARQL.QueryCache` have different roles; a small explicit cache registry/facade would reduce accidental cross-use without pretending they are interchangeable.
- Replace hard pattern matches at I/O boundaries with tagged errors. For example, `TripleStore.load_facts_from_db/2` currently matches `{:ok, triples}` and turns a storage error into `MatchError` during materialization.

## Open Questions

- Is the uncommitted `spec_led_ex` dependency intended for the next commit? The checkout is currently absent and `mix.lock` has no entry, so ordinary Mix commands stop before compilation. The review temporarily removed that single dependency declaration to run validation, then restored `mix.exs` byte-for-byte.
- Should transaction snapshots be an actual supported feature, or should the public API describe serialized reads until a future isolation phase? This decision changes both architecture and test strategy.
- Is `QuadLeapfrog` intended to be the default path for all graph-variable queries, or an optimization with a correctness-preserving single-iterator fallback? The current rescue fallback cannot help failures that occur lazily after the stream has been returned.

## Test Gaps And Residual Risk

- Full default suite: `25 doctests, 10 properties, 6728 tests, 36 failures, 53 skipped (345 excluded)` in 313 seconds. The suite was run with Elixir `1.19.5-otp-28` and Erlang `28.3.1` because the exact `.tool-versions` Elixir alias was unavailable locally.
- Fourteen integration-level failures share `Executor.convert_leapfrog_bindings/1`; the remaining failures are concentrated in `QuadLeapfrog` behavior and contract assertions. The first fixes should rerun the dedicated QuadLeapfrog suite, graph query/update/authorization integration tests, and then the default suite.
- Strict Credo analyzed 430 files and exited nonzero with 3 warnings, 40 refactoring opportunities, 24 readability issues, and 34 design suggestions. Most are low-priority style debt; the complexity signals around `QuadLeapfrog` support the correctness findings above.
- The default suite excludes 345 tagged tests, including benchmark, large-dataset, slow, and lifetime-safety coverage. Rust parser checks, Dialyzer, governance scripts, and excluded suites were not run in this review.
- The pre-existing `mix.exs` edit prevents validation without fetching `spec_led_ex`; no dependency fetch was performed and no source changes were made.

## Lane Summaries

### Factual

Implementation and project documentation agree that store-wide coordination, transaction snapshot propagation, and whole-request rollback remain gaps. `QuadLeapfrog` implementation, types, examples, and tests do not agree on iterator-plan or binding shapes.

### QA

The default suite is not green. Failures cluster around one subsystem and provide reproducible starting points. Missing request-rollback, store-wide coordination, backup-lifecycle, unsafe-decode, and atom-exhaustion tests leave important guarantees unprotected.

### Senior Engineering

Query planning and transaction boundaries carry too many responsibilities in large modules. Store lifecycle ownership is explicit for the dictionary but remains fragmented for transaction, cache, statistics, metrics, and scheduled backup helpers.

### Security

Runtime atom creation from external names can terminate the VM, and persisted ACL/provenance bytes are decoded without safe mode or schema validation. Authorization query paths also depend on the currently failing quad execution subsystem.

### Consistency

Tagged error conventions are generally present but break at malformed QuadLeapfrog input, some storage hard matches, and silent ACL read-error fallbacks. Safe persisted-term decoding is used in backup/statistics code but not consistently in authorization/provenance.

### Redundancy

There are overlapping cache implementations and repeated name-generation/term-decoding helpers. Consolidation should target shared mechanics while retaining documented semantic distinctions.

### Elixir

The major Elixir-specific risks are unbounded atom creation, GenServer lifecycle cleanup outside `try/after`, synchronous call semantics that contradict snapshot claims, and high-complexity functions that obscure iterator ownership and result shapes.

## Raw Lane Outputs

### Factual output

- Normative transaction requirements `REQ-TXN-001`, `REQ-TXN-003` through `REQ-TXN-005`, and `REQ-TXN-008` through `REQ-TXN-010` are not all met; the contract records the same observed limitations.
- Root project guidance requires atomic explicit-index fanout and warns that a snapshot's existence does not prove snapshot-aware reads or request-wide rollback.
- Quad Leapfrog's produced binding maps do not match the consumer's tagged-tuple clauses.

### QA output

- Full suite reproduced 36 failures.
- At least 14 failures share the same binding conversion exception across query, update, authorization, and concurrency tests.
- Graph-bound QuadLeapfrog tests return empty results due to the zero-filled prefix; malformed input raises instead of returning an error tuple.

### Senior engineering output

- Independent temporary transaction managers cannot enforce store-wide single-writer behavior.
- Snapshot allocation adds native-resource work without changing read isolation.
- The largest modules and the QuadLeapfrog complexity hotspot should be split around stable contracts rather than mechanically by line count.

### Security output

- SPARQL variables and RDF IRI-derived names reach `String.to_atom/1`.
- ACL and provenance database values reach unsafe `binary_to_term/1`.
- The authorization layer's graph-query enforcement currently depends on a failing executor path.

### Consistency output

- Safe decode helpers already exist elsewhere in the repository and should be reused.
- Planner docs/types/tests and implementation encode different tuple shapes and iterator counts.
- Several error paths raise or silently default instead of returning the project's usual tagged errors.

### Redundancy output

- Cache modules have overlapping mechanics but distinct meanings; shared lifecycle/identity code is a better consolidation target than merging behavior.
- Rule and cache modules repeat runtime atom-based dynamic naming.
- Persistence modules repeat term decoding with inconsistent safety and validation.

### Elixir output

- Credo: 3 warnings, 40 refactoring opportunities, 24 readability issues, and 34 design suggestions across 430 files.
- Highest relevant complexity: `QuadLeapfrog.from_pattern/2` at 29, plus nested stream/binding functions and an 11-argument executor function.
- Temporary GenServer cleanup and native snapshot cleanup need structured ownership on every exit path.
