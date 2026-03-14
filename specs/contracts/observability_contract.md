# Observability Contract

This contract defines the normative operational visibility requirements for `TripleStore`.

## Requirement Set

- `REQ-OBS-001`: Telemetry events MUST use stable subsystem and operation naming under the `[:triple_store, ...]` prefix.
- `REQ-OBS-002`: Query, update, load, reasoning, backup, graph-backup, and restore flows SHOULD emit start/stop/exception style instrumentation where applicable.
- `REQ-OBS-003`: Health surfaces MUST distinguish liveness, readiness, and full-health status.
- `REQ-OBS-004`: Operational metadata emitted from telemetry MUST avoid leaking sensitive values such as full paths, raw queries where unsafe, or secret-bearing payloads.
- `REQ-OBS-005`: Backup, restore, graph-backup, and scheduled-backup operations MUST be observable as first-class operational events.
- `REQ-OBS-006`: Reasoning, snapshot, statistics, and cache behavior SHOULD expose enough telemetry or health status to diagnose performance and correctness issues.
- `REQ-OBS-007`: Metrics, Prometheus export, and health checks MUST describe the same canonical schema-aware store/runtime model as the rest of the specs system.
- `REQ-OBS-008`: Operator-facing documentation SHOULD link runtime guarantees back to these contracts and the scenario catalog.
