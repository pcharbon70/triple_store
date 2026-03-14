# Reasoning Contract

This contract defines the normative reasoning behavior for `TripleStore`.

## Requirement Set

- `REQ-RSN-001`: Materialization MUST use forward-chaining evaluation over explicit and derived facts.
- `REQ-RSN-002`: Semi-naive delta evaluation is the canonical fixpoint algorithm for full materialization.
- `REQ-RSN-003`: Reasoning profiles and configs MUST determine the active rule set and scope of derivation.
- `REQ-RSN-004`: Rule compilation and rule optimization MUST happen before or during execution through explicit reasoning modules.
- `REQ-RSN-005`: The legacy local `materialize/2` path and the graph-scoped quad reasoning APIs MUST both remain explicitly documented current surfaces while they coexist.
- `REQ-RSN-006`: Derived facts MUST remain logically separable from explicit facts for maintenance and deletion workflows.
- `REQ-RSN-007`: Incremental maintenance, graph-scoped maintenance, and delete-with-reasoning paths MUST preserve the explicit-versus-derived contract.
- `REQ-RSN-008`: Reasoning runs MUST expose typed statistics or status describing progress, outcomes, and graph-scoped state where applicable.
- `REQ-RSN-009`: Optional parallel reasoning MUST remain deterministic with respect to final derived facts.
- `REQ-RSN-010`: Reasoning limits such as maximum iterations or fact counts MUST fail with typed outcomes rather than silent truncation.
- `REQ-RSN-011`: Reasoning telemetry and provenance MUST make iteration, duration, per-graph behavior, and derived-fact lineage observable.
