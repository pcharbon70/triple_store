# TripleStore Scenario Catalog

Canonical validation scenarios for the TripleStore contract layer.

| Scenario ID | Name | Summary |
|---|---|---|
| `SCN-001` | Control-plane consistency | Baseline and area docs resolve to one canonical plane assignment. |
| `SCN-002` | Store open and close lifecycle | Opening a store creates the required runtime surfaces and closing it releases them safely. |
| `SCN-003` | Dictionary encoding and schema metadata determinism | Equivalent RDF terms encode to stable IDs, decode without type confusion, and preserve schema metadata expectations. |
| `SCN-004` | Schema-aware write fanout atomicity | Triple and quad writes update all required explicit index surfaces atomically. |
| `SCN-005` | Triple and quad pattern coverage | Supported triple-pattern and quad-pattern shapes resolve through canonical index-selection rules. |
| `SCN-006` | Query parse and execution determinism | Equivalent SPARQL queries produce stable AST-to-execution behavior and typed errors across triple and quad paths. |
| `SCN-007` | Plan cache invalidation after mutation | Query-plan assumptions are not reused incorrectly after writes. |
| `SCN-008` | Update isolation and explicit snapshot consistency | Reads concurrent with writes do not observe partial multi-index mutation when the transaction coordinator owns the flow. |
| `SCN-009` | Reasoning fixpoint determinism | Materialization reaches a stable fixpoint with deterministic derived-fact results. |
| `SCN-010` | Explicit versus derived separation | Derived facts, provenance, and graph-scoped reasoning state remain operationally and logically distinct from explicit facts. |
| `SCN-011` | Incremental and graph-scoped reasoning maintenance | Changes to explicit facts propagate through incremental and graph-scoped reasoning paths without corrupting derived state. |
| `SCN-012` | Backup, graph backup, and restore recoverability | Store and graph backups can be restored into usable data with expected metadata and safety checks. |
| `SCN-013` | Health probe fidelity | Liveness, readiness, and full-health surfaces reflect the true runtime status, including optional helpers. |
| `SCN-014` | Telemetry envelope stability | Major operations emit stable, sanitized telemetry with expected start/stop/exception semantics. |
| `SCN-015` | Native boundary containment | Parser NIFs and RocksDB adapter-backed native calls stay bounded to native capability without taking over semantic control. |
| `SCN-016` | Schema-aware RDF I/O behavior | Generic graph-oriented APIs, quad-aware loader/exporter APIs, and graph-scoped import/export preserve the documented graph-handling rules. |
| `SCN-017` | Named-graph authorization and operations | Graph clauses, graph management, ACL checks, and graph-scoped storage semantics stay aligned across query, update, and storage layers. |
