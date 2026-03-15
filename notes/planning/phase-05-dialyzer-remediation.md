# Phase 5 Companion: Dialyzer Remediation

## Overview

Description: This companion plan tracks the work required to drive `mix dialyzer`
to a clean exit on the current TripleStore codebase. It extends Phase 5
Production Hardening because the current Dialyzer backlog is primarily contract
and type-governance work rather than feature delivery.

Current baseline on this branch after the Phase 1 Section 1.2 shared-type
normalization on 2026-03-15:

- `184` total Dialyzer issues
- Dominant categories: `invalid_contract`, `call`, `no_return`, `pattern_match`,
  `unused_fun`, and `pattern_match_cov`
- Highest-volume files: `graph_scoped_reasoner.ex`, `triple_store.ex`,
  `erlang_adapter.ex`, `quad_index.ex`, and `health.ex`

## Success Criteria

Description: The remediation effort is complete when Dialyzer becomes a stable
quality gate rather than an occasional spot check.

- `mix dialyzer` exits `0` on local development shells and CI
- No project-level ignore file is needed to hide known warnings
- Public `@spec`s match real return shapes across the storage, query, reasoning,
  and statistics layers
- The pre-commit and CI workflows can run Dialyzer as a reliable merge gate

---

## Phase 1: Baseline and Shared Types

Description: Remove toolchain-level Dialyzer noise and establish the canonical
type vocabulary that all later phases depend on.

### Section 1.1: Dialyzer Baseline

Description: Fix the analysis environment so the remaining warnings are true
application warnings rather than tooling false positives.

- [x] **Task 1.1.1** Description: Add `:mix` to the Dialyzer PLT configuration in
  `mix.exs` so Mix task modules do not report false unknown function and callback
  metadata errors. Completed on 2026-03-15, which removed the `Mix` false
  positives and reduced the total Dialyzer count from `321` to `318`.
- [ ] **Task 1.1.2** Description: Document the expected local Dialyzer execution
  flow and capture the before/after warning delta for each remediation batch.

### Section 1.2: Canonical Shared Types

Description: Normalize common database, store, manager, and batch-result types
so subsystem modules stop inventing subtly different aliases.

- [x] **Task 1.2.1** Description: Define or centralize canonical `db_ref`,
  `store`, and manager types that reflect the current runtime handles.
  Completed on 2026-03-15 by centralizing the public aliases in `TripleStore`
  and aligning the RocksDB adapter DB handle type with the current `pid()`-backed
  runtime.
- [x] **Task 1.2.2** Description: Replace stale aliases such as generic
  `reference()` handles and removed dictionary manager types across loader,
  exporter, statistics, and public API modules. Completed on 2026-03-15, which
  reduced the Dialyzer backlog from `318` to `184`.

---

## Phase 2: Storage and Dictionary Contracts

Description: Align the low-level RocksDB and dictionary contracts that higher
layers inherit.

### Section 2.1: RocksDB Adapter and Iterator Contracts

Description: Bring the adapter specs into line with the actual iterator and
prefix-stream behavior.

- [x] **Task 2.1.1** Description: Reconcile `ErlangAdapter` specs with actual
  return values for iterator navigation, snapshots, prefix streams, and write
  helpers. Completed on 2026-03-15 by fixing the snapshot reference type alias,
  aligning snapshot APIs to that alias, and removing unreachable adapter
  fallback branches that Dialyzer no longer considered possible.
- [x] **Task 2.1.2** Description: Remove impossible branches around
  `:iterator_end`, invalid column family paths, and other stale pattern matches
  in the RocksDB adapter and iterator modules. Completed on 2026-03-15 by
  collapsing private iterator/fold catch-all paths to the shapes Dialyzer sees
  from `:rocksdb.iterator_move/2`, which reduced the backlog from `184` to
  `168`.

### Section 2.2: Dictionary and Counter Contracts

Description: Tighten the dictionary and sequence-counter specs to the current
`pid()`-based store interactions.

- [x] **Task 2.2.1** Description: Align `Dictionary.Manager`,
  `Dictionary.IdToString`, and `Dictionary.SequenceCounter` contracts with the
  current lookup and batch-write behavior. Completed on 2026-03-15 by
  normalizing the private dictionary and counter DB-handle specs to the
  canonical `TripleStore.db_ref()` alias introduced in Phase 1.
- [x] **Task 2.2.2** Description: Fix downstream callers that still assume older
  dictionary adapter return shapes. Completed on 2026-03-15 by aligning
  sharded-manager shared-resource contracts with the same store-handle alias,
  which reduced the Dialyzer backlog from `168` to `157`.

---

## Phase 3: Ingestion and Export Pipelines

Description: Remove the contract cascades in the loader and exporter pipelines,
where a small number of stale types currently fan out into many warnings.

### Section 3.1: Loader Pipeline Typing

Description: Make batch-processing, progress-reporting, and parallel-load result
shapes explicit.

- [ ] **Task 3.1.1** Description: Introduce concrete internal types for loader
  progress options, write options, encoded batches, and halt/error results.
- [ ] **Task 3.1.2** Description: Reconcile the public and private loader
  contracts for file, string, stream, sequential, and parallel loading paths.

### Section 3.2: Export Pipeline Typing

Description: Replace vague exporter contracts with the concrete stream and graph
conversion shapes the code actually returns.

- [ ] **Task 3.2.1** Description: Narrow exporter stream contracts to real stream
  shapes and count return types.
- [ ] **Task 3.2.2** Description: Remove stale dictionary-manager type references
  and fix graph/quad lookup assumptions in the exporter code.

---

## Phase 4: Public API and Reasoning Orchestration

Description: Align the top-level API wrappers and graph-scoped reasoning
orchestration with the corrected lower-level contracts.

### Section 4.1: Public API Wrappers

Description: Bring `TripleStore` wrapper specs into line with the actual
delegated return values.

- [ ] **Task 4.1.1** Description: Reconcile public contracts for materialization,
  reasoning status, explanation, and `load_string!` wrapper functions.
- [ ] **Task 4.1.2** Description: Remove wrapper branches and helper functions
  that become unreachable once the delegated contracts are corrected.

### Section 4.2: Graph-Scoped Reasoning and Updates

Description: Fix opaque container use, telemetry contract drift, and update-path
contract mismatches.

- [ ] **Task 4.2.1** Description: Resolve `MapSet` opaque-type issues, quad
  pattern typing, and derived-store write contracts in graph-scoped reasoning.
- [ ] **Task 4.2.2** Description: Align transaction and SPARQL update executor
  contracts so insert/delete/update paths stop producing `call` and `no_return`
  warnings.

---

## Phase 5: Statistics Contract Cleanup

Description: Resolve the concentrated statistics backlog by tightening the core
module, the cache, and the server around their real return shapes.

### Section 5.1: Core Statistics Contracts

Description: Bring the public statistics API into line with the maps and tuples
actually returned by the implementation.

- [ ] **Task 5.1.1** Description: Reconcile `Statistics` contracts for warming,
  collecting, loading, counting, summaries, and histogram builders.
- [ ] **Task 5.1.2** Description: Decide explicitly where APIs should stay broad
  and where the implementation contracts should be narrowed to real shapes.

### Section 5.2: Cache and Server Contracts

Description: Align the deprecated cache and current server-side statistics
implementations with the corrected core contracts.

- [ ] **Task 5.2.1** Description: Reconcile `Statistics.Cache` contracts with the
  current compute and refresh behavior.
- [ ] **Task 5.2.2** Description: Fix stale impossible matches and refresh-path
  contracts in the statistics server code.

---

## Phase 6: Residual Cleanup and Enforcement

Description: Eliminate the remaining tail of dead branches and make Dialyzer a
durable part of local and CI quality gates.

### Section 6.1: Residual Warning Tail

Description: Clear the smaller remaining warning classes that will still exist
after the major contract families are fixed.

- [ ] **Task 6.1.1** Description: Remove the remaining `pattern_match`,
  `pattern_match_cov`, `unused_fun`, `unknown_type`, and `call_without_opaque`
  issues in secondary modules.
- [ ] **Task 6.1.2** Description: Rerun Dialyzer after each remediation batch and
  keep the delta grouped by category to avoid reintroducing drift.

### Section 6.2: Workflow Enforcement

Description: Promote Dialyzer from a manual cleanup tool to a reliable
repository quality gate.

- [ ] **Task 6.2.1** Description: Integrate green Dialyzer runs into the local
  hook and GitHub workflow gates.
- [ ] **Task 6.2.2** Description: Reflect the completed remediation in the
  planning and governance docs so the clean baseline is maintained.
