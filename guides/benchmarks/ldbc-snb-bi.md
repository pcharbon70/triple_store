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
  Result caches and statistics are invalidated after a successful commit.
- Initial-load, post-validation, pre-power, and pre-throughput checkpoints are
  copied only while the database is closed and are verified before restore.

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
mix test test/triple_store/benchmark/ldbc/snb_bi_*_test.exs
```
