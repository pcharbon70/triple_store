# TripleStore Operations Index

## Purpose

`Operations Index` is the entry point for observability, health, backup,
recovery, and runtime support documentation in `TripleStore`.

It covers:

- telemetry and metrics surfaces
- health, readiness, and support-process visibility
- backup, restore, graph backup, and scheduled backup
- snapshots, statistics, and alerting thresholds

## Control Plane

Primary control-plane ownership: **Operations Plane**.

## Canonical Operational Surfaces

- `TripleStore.Telemetry`
- `TripleStore.Reasoner.Telemetry`
- `TripleStore.Metrics`
- `TripleStore.Prometheus`
- `TripleStore.Health`
- `TripleStore.Backup`
- `TripleStore.GraphBackup`
- `TripleStore.ScheduledBackup`
- `TripleStore.Snapshot`
- `TripleStore.Statistics`
- `TripleStore.Statistics.Cache`
- `TripleStore.Statistics.Server`
- `TripleStore.AlertThresholds`

## Component Specs

- [observability_and_recovery.md](observability_and_recovery.md)

## Current Codebase Notes

- `Telemetry` is the shared instrumentation surface; metrics and Prometheus consume it rather than defining parallel naming schemes.
- `Metrics` and `Prometheus` are opt-in helpers, not default supervised runtime children.
- `Health` reports on both required and optional runtime helpers, which means degraded states can reflect missing opt-in services.
- `Backup` and `GraphBackup` are both current recovery surfaces: one is full-store oriented, the other is graph-scoped.
- `Snapshot` is globally supervised, while statistics helpers and scheduled backup remain dynamic or caller-managed.

## Acceptance Criteria

| Acceptance ID | Criterion | Related Requirements | Related Scenarios |
|---|---|---|---|
| `AC-OPS-01` | Telemetry remains the shared source for metrics and Prometheus export rather than parallel ad hoc instrumentation stacks. | `REQ-OBS-*` | `SCN-014` |
| `AC-OPS-02` | Health distinguishes liveness, readiness, and full-health status while reporting optional helper state accurately. | `REQ-OBS-*`, `REQ-CP-*` | `SCN-013` |
| `AC-OPS-03` | Full-store backup, graph backup, restore, and scheduled backup remain documented as current runtime features. | `REQ-OBS-*`, `REQ-STO-*` | `SCN-012` |
| `AC-OPS-04` | Snapshot and statistics support remain explicit operational surfaces rather than implicit storage side effects. | `REQ-OBS-*`, `REQ-TXN-*` | `SCN-008`, `SCN-013` |
| `AC-OPS-05` | Operator-facing docs stay aligned with the same schema-aware runtime model used by the storage, query, and reasoning specs. | `REQ-OBS-*`, `REQ-STO-*`, `REQ-RSN-*` | `SCN-012`, `SCN-013`, `SCN-014` |

## Related Notes

- [`notes/features/phase-4.3-snapshot-management.md`](../../notes/features/phase-4.3-snapshot-management.md)
- [`notes/features/phase-5-production-hardening-implementation.md`](../../notes/features/phase-5-production-hardening-implementation.md)
- [`notes/reviews/section-5.5-backup-restore-review.md`](../../notes/reviews/section-5.5-backup-restore-review.md)
- [`notes/reviews/section-5.4-telemetry-integration-review.md`](../../notes/reviews/section-5.4-telemetry-integration-review.md)
