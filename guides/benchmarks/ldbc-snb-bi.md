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
  handlers and are materialized through the strict result contract.
- BI 10, 15, 19, and 20 use the named `GraphAlgorithms` extensions. Their
  neighbour provider scans TripleStore indices lazily, and traversal enforces a
  deadline, cancellation, a visited-vertex bound, and deterministic ties.
- A BI microbatch is dictionary-encoded before one ordered mixed RocksDB batch.
  Result caches and statistics are invalidated after a successful commit.
- Initial-load, post-validation, pre-power, and pre-throughput checkpoints are
  copied only while the database is closed and are verified before restore.

## Current qualification boundary

The Phase 5 infrastructure does not by itself qualify the 20 analytical query
semantics. Each standard read still needs a registered exact engine handler and
full ordered answer comparison against the pinned reference implementation.
Likewise, the four graph extensions need operation-specific edge-weight and
candidate-selection adapters over a generated SNB dataset. Capability entries
therefore remain `requires_fix` or `requires_extension`; the code fails closed
when a handler is absent.

Run the focused validation with:

```sh
mix test test/triple_store/benchmark/ldbc/phase_5_snb_bi_integration_test.exs
mix test test/triple_store/benchmark/ldbc/snb_bi_*_test.exs
```
