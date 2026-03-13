# Reasoning Contract

This contract defines the normative reasoning behavior for `TripleStore`.

## Requirement Set

- `REQ-RSN-001`: Materialization MUST use forward-chaining evaluation over explicit and derived facts.
- `REQ-RSN-002`: Semi-naive delta evaluation is the canonical fixpoint algorithm for full materialization.
- `REQ-RSN-003`: Reasoning profiles MUST determine the active rule set and limit the derivation scope accordingly.
- `REQ-RSN-004`: Rule compilation and rule optimization MUST happen before or during execution through explicit reasoning modules.
- `REQ-RSN-005`: Derived facts MUST remain logically separable from explicit facts for maintenance and deletion workflows.
- `REQ-RSN-006`: Incremental maintenance and delete-with-reasoning paths MUST preserve the explicit-versus-derived contract.
- `REQ-RSN-007`: Reasoning runs MUST expose typed statistics or status describing progress and outcomes.
- `REQ-RSN-008`: Optional parallel reasoning MUST remain deterministic with respect to final derived facts.
- `REQ-RSN-009`: Reasoning limits such as maximum iterations or fact counts MUST fail with typed outcomes rather than silent truncation.
- `REQ-RSN-010`: Reasoning telemetry MUST make iteration, duration, and derived-fact behavior observable.
