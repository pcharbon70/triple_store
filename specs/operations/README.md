# TripleStore Operations Index

Operational documentation in this repository is currently centered on runtime modules and implementation notes rather than dedicated runbook files under `specs/operations/`.

## Canonical Operational Surfaces

- `TripleStore.Health`
- `TripleStore.Telemetry`
- `TripleStore.Metrics`
- `TripleStore.Prometheus`
- `TripleStore.Backup`
- `TripleStore.ScheduledBackup`
- `TripleStore.Snapshot`

## Component Specs

- [observability_and_recovery.md](observability_and_recovery.md)

## Related Notes

- [`notes/features/phase-4.3-snapshot-management.md`](../../notes/features/phase-4.3-snapshot-management.md)
- [`notes/features/phase-5-production-hardening-implementation.md`](../../notes/features/phase-5-production-hardening-implementation.md)
- [`notes/reviews/section-5.5-backup-restore-review.md`](../../notes/reviews/section-5.5-backup-restore-review.md)
- [`notes/reviews/section-5.4-telemetry-integration-review.md`](../../notes/reviews/section-5.4-telemetry-integration-review.md)

## Operational Expectations

- Backup and restore behavior must remain consistent with the storage contract.
- Health and telemetry behavior must remain consistent with the observability contract.
- Scheduled maintenance features should not bypass the canonical public API and runtime/data model.
