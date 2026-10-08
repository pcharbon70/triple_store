# LDBC Semantic Publishing Benchmark

TripleStore's Phase 4 SPB suite packages the LDBC SPB 2.0.2 advanced workload at
commit `ce6323c0936306729408233dc70d26f2389b34c6`. It runs against a quad store
with a union default graph, persistent graph-zero derived facts, and ACL checks
explicitly disabled for benchmark execution.

The checked-in workload contains all 25 aggregation templates plus the insert,
update, delete, and matching validation templates. Original bytes are protected
by `priv/benchmarks/ldbc/spb/checksums.sha256`. The compatibility layer repairs
only invalid comment encoding in queries 11 and 12 and the documented missing
`?` sigil in query 20; every repair is disclosed while the source file remains
unchanged.

Run the deterministic Phase 4 workflow with:

```sh
mix test test/triple_store/benchmark/ldbc/phase_4_spb_integration_test.exs
```

The workflow generates and validates a smoke N-Quads dataset, loads its ontology,
reference, and creative-work graphs, materializes the selected OWL 2 RL profile,
executes all 25 aggregation queries through optimized and reference paths, runs
editorial transitions, runs concurrent agents through the serialized scheduler,
resets the store, verifies exact answer reproduction, performs backup/restore,
and schema-checks the resulting JSON, CSV, Markdown, correctness, and disclosure
artifacts.

Smoke results are diagnostic. They are not official or comparable LDBC results:
the smoke dataset is deliberately small, the canonical Java measurement duration
is not run, externally generated scale tiers are excluded, and the ten catalogued
SPB inference conformance actions remain unqualified. Artifact score gating keeps
`official_score_eligible` false for this workflow. Aggregation and editorial rates
are emitted only when their own complete correctness and scheduling gates pass.

See [the resilience profile](ldbc-spb-resilience.md) for backup semantics and the
explicit exclusion of online replication and failover.
