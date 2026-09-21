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
- `REQ-RSN-011`: Reasoning telemetry and provenance MUST make iteration, duration, per-graph behavior, and derived-fact lineage observable. Persisted provenance MUST be decoded without creating runtime atoms, completely validated before use, and rejected with a tagged corruption error before explanation or maintained deletion changes data.

## Current Implementation Status

The default local `TripleStore.materialize/2` path is an in-memory computation over explicit triple indices. It returns statistics, discards the computed fact set, and neither reads persisted derived facts nor stores new ones. It also does not forward its `parallel` option. This is a limitation of the current facade path, not a general persistence guarantee or a change to the normative requirements above.

Graph-scoped APIs use `GraphScopedReasoner` and storage callbacks; `DerivedStore` exposes persistence APIs for reasoning workflows. Assess `REQ-RSN-001`, `REQ-RSN-005`, `REQ-RSN-006`, and `REQ-RSN-009` separately for each entry point. `SCN-009` fixpoint results alone do not establish `SCN-010` persistence, provenance, or later query visibility. A local facade persistence fix requires implementation and behavior-test evidence, not just a documentation update.

Persistent derivation records retain the existing unversioned map encoding. A
record contains a nonempty atom or binary rule identifier, a list of graph-first
ID quads as premises, binary-keyed bindings, and a nonnegative timestamp; the
optional metadata map may contain graph, scope, and iteration fields. Readers
use safe external-term decoding and validate both the 32-byte fact key and the
complete record. Existing atom rule names remain readable, while newly
specialized rule names may be binaries without requiring a migration.
