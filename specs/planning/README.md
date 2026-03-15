# TripleStore Planning Index

This specs system reuses the existing implementation plans in `notes/planning/` rather than duplicating them.

## Canonical Planning References

- [`notes/planning/overview.md`](../../notes/planning/overview.md)
- [`notes/planning/phase-01-storage-foundation.md`](../../notes/planning/phase-01-storage-foundation.md)
- [`notes/planning/phase-02-sparql-query-engine.md`](../../notes/planning/phase-02-sparql-query-engine.md)
- [`notes/planning/phase-03-advanced-query-processing.md`](../../notes/planning/phase-03-advanced-query-processing.md)
- [`notes/planning/phase-04-owl2rl-reasoning.md`](../../notes/planning/phase-04-owl2rl-reasoning.md)
- [`notes/planning/phase-05-production-hardening.md`](../../notes/planning/phase-05-production-hardening.md)
- [`notes/planning/phase-05-dialyzer-remediation.md`](../../notes/planning/phase-05-dialyzer-remediation.md)

## Alignment Rules

- Phase work should continue to follow the existing `notes/planning/` roadmap.
- When a planning phase changes the architecture or contracts, the relevant `specs/` docs SHOULD be updated in the same change set.
- Performance-only work under `notes/planning/performance/` should still respect the storage, query, transaction, reasoning, and observability contracts.
