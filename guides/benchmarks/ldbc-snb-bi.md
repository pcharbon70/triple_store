# LDBC SNB Business Intelligence

TripleStore pins SNB BI v1.0.3 and the SNB specification at v2.2.4. The local
workload package derives 31 read variants from the canonical catalog, keeps
typed parameter and result contracts, and uses explicit extension identifiers
for BI 10, 15, 19, and 20.

The smoke workflow validates workload metadata, parameter provenance, atomic
update batches, closed-store checkpoints, index-backed path traversal, protocol
scheduling, scoring formulas, failure gates, and run artifacts. It is a reduced
diagnostic workflow and never emits an official score.

Comparable execution additionally requires official parameter files bound to
the exact dataset checksum, all canonical dated blocks, at least one hour of
throughput blocks, complete reference-answer validation, and the pinned scoring
rules. Hand-selected parameter rows are rejected for that profile.

## Execution boundaries

- Standard analytical reads enter `SNB.BI.Analytics` through registered engine
  handlers, use typed prepared bindings, and are materialized through the strict
  result contract.
- BI 10, 15, 19, and 20 use registered operation-specific adapters over the
  named `GraphAlgorithms` extensions. Their neighbour providers scan TripleStore
  indices lazily, enforce named-graph authorization, and apply deadlines,
  cancellation, visited-vertex and intermediate-result bounds, and deterministic
  tie handling.
- A BI microbatch is dictionary-encoded before one ordered mixed RocksDB batch.
  The converter emits all 16 canonical record types, validates typed fields,
  and requires every referenced entity to exist in persisted state or an
  earlier insert in the batch. Update files are written and validated as
  replayable streams without retaining the complete source batch in memory.
  Result caches and statistics are invalidated after a successful commit. If
  post-commit maintenance fails, protocol callers that supply the current state
  identity and batch position receive the advanced committed identity in an
  explicitly incomplete, score-ineligible error. If the state probe fails after
  commit, the error marks the committed identity as indeterminate and retry as
  unsafe.
- Initial-load, post-validation, pre-power, and pre-throughput checkpoints are
  copied only while the database is closed. Restore validates each checkpoint's
  own statement count, persisted checksum, update position, and selected BI
  answer hashes.

The deterministic update-state integration covers all eight insert and eight
delete record types. It verifies the `gspo`, `gpos`, `spog`, and `posg` indices
after each accepted microbatch, restores all four named checkpoints, and proves
that translation, dependency, storage, interruption, and restore failures do
not advance persisted state. The complete insert/delete cycle reproduces the
initial state checksum and selected answer hashes exactly.

## Current qualification boundary

All 31 analytical variants now have a registered standard handler or graph
adapter. The reduced smoke integration executes each variant, validates its
typed result contract, and reproduces the same canonical answer digest after a
store reopen. This establishes executable integration coverage, not SNB BI
correctness qualification.

The checked-in BI 1 answer is explicitly a `contract_fixture`, and the other 30
variants have no checked-in answer artifact. A contract fixture cannot satisfy
accepted-answer comparison. Capability entries therefore remain `requires_fix`
or `requires_extension` until every variant is compared with an immutable answer
from the pinned reference implementation. `SNB.BI.Qualification.run/4` records
missing or ineligible answers per variant while preserving successful execution
evidence.

Run the focused validation with:

```sh
mix test test/triple_store/benchmark/ldbc/phase_5_snb_bi_integration_test.exs
mix test test/triple_store/benchmark/ldbc/phase_5b_read_correctness_integration_test.exs
mix test test/triple_store/benchmark/ldbc/phase_5c_update_state_integration_test.exs
mix test test/triple_store/benchmark/ldbc/snb_bi_*_test.exs
```
