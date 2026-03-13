# TripleStore Scenario Catalog

Canonical validation scenarios for the TripleStore contract layer.

| Scenario ID | Name | Summary |
|---|---|---|
| `SCN-001` | Control-plane consistency | Baseline and area docs resolve to one canonical plane assignment. |
| `SCN-002` | Store open and close lifecycle | Opening a store creates the required runtime surfaces and closing it releases them safely. |
| `SCN-003` | Dictionary encoding determinism | Equivalent RDF terms encode to stable IDs and decode without type confusion. |
| `SCN-004` | Index fanout atomicity | Triple writes update all explicit index surfaces atomically. |
| `SCN-005` | Triple pattern coverage | All supported triple-pattern shapes resolve through canonical index-selection rules. |
| `SCN-006` | Query parse and execution determinism | Equivalent SPARQL queries produce stable AST-to-execution behavior and typed errors. |
| `SCN-007` | Plan cache invalidation after mutation | Query-plan assumptions are not reused incorrectly after writes. |
| `SCN-008` | Update isolation and snapshot consistency | Reads concurrent with writes do not observe partial multi-index mutation. |
| `SCN-009` | Reasoning fixpoint determinism | Materialization reaches a stable fixpoint with deterministic derived-fact results. |
| `SCN-010` | Explicit versus derived separation | Derived facts remain operationally and logically distinct from explicit triples. |
| `SCN-011` | Incremental reasoning maintenance | Changes to explicit facts propagate through incremental reasoning paths without corrupting derived state. |
| `SCN-012` | Backup and restore recoverability | Backups can be restored into a usable store with expected metadata and safety checks. |
| `SCN-013` | Health probe fidelity | Liveness, readiness, and full-health surfaces reflect the true runtime status. |
| `SCN-014` | Telemetry envelope stability | Major operations emit stable, sanitized telemetry with expected start/stop/exception semantics. |
| `SCN-015` | Native boundary containment | Parser and RocksDB NIFs stay bounded to native capability without taking over semantic control. |
