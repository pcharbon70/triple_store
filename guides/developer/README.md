# Developer Guides

These guides explain the implementation that backs the public API. Read the
relevant specification or contract alongside them when changing semantics.

## Reading order

1. [Architecture overview](00-architecture-overview.md)
2. [Storage layer](01-storage-layer.md)
3. [SPARQL engine](02-sparql-engine.md)
4. [Reasoning engine](03-reasoning-engine.md)
5. [Query optimization](04-query-optimization.md)
6. [Telemetry and monitoring](05-telemetry-monitoring.md)
7. [Quad-store architecture](06-quad-store-architecture.md)
8. [OTP and concurrency](07-otp-concurrency.md)
9. [Data flow](08-data-flow.md)

The [specification index](https://github.com/pcharbon70/triple_store/blob/main/specs/README.md), [architecture
specification](../../specs/architecture-overview.md), [boundaries
specification](../../specs/boundaries.md), and [control-plane ownership
matrix](../../specs/contracts/control_plane_ownership_matrix.md) define the
expected boundaries. The code and adjacent tests establish current behavior.

## Local validation

Use the toolchain pinned in `.tool-versions`. The main documentation checks are:

~~~sh
./scripts/validate_guides_governance.sh
./scripts/validate_specs_governance.sh
./scripts/validate_code_docs.sh
./scripts/run_conformance.sh
~~~

`mix conformance` checks structure and traceability. It does not execute SPARQL
conformance scenarios. Run focused tests for the behavior being changed, then
the broader checks required by `AGENTS.md` and CI.
