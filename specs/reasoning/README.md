# Reasoning Specs Index

## Purpose

`Reasoning Specs Index` is the entry point for OWL 2 RL and derived-fact lifecycle documentation in `TripleStore`.

It covers:

- rule representation and compilation
- semi-naive materialization
- incremental maintenance
- derived-fact storage and reasoning status

## Control Plane

Primary ownership: **Reasoning Plane**.

## Primary Reasoning Components

- `TripleStore.Reasoner.Rule`
- `TripleStore.Reasoner.Rules`
- `TripleStore.Reasoner.RuleCompiler`
- `TripleStore.Reasoner.RuleOptimizer`
- `TripleStore.Reasoner.SemiNaive`
- `TripleStore.Reasoner.Incremental`
- `TripleStore.Reasoner.DerivedStore`
- `TripleStore.Reasoner.DeleteWithReasoning`
- `TripleStore.Reasoner.ReasoningProfile`
- `TripleStore.Reasoner.ReasoningStatus`
- `TripleStore.Reasoner.Telemetry`

## Component Specs

- [materialization_and_maintenance.md](materialization_and_maintenance.md)

## Current Codebase Notes

- The current subsystem includes status tracking, schema-aware helpers, tracing, and rederivation in addition to the main materialization loop.
- `DerivedStore` is already a distinct persisted surface with its own tests and lifecycle.

## Acceptance Criteria

| Acceptance ID | Criterion | Related Requirements | Related Scenarios |
|---|---|---|---|
| `AC-RSN-01` | Materialization uses forward-chaining evaluation with semi-naive delta processing as the canonical full-run algorithm. | `REQ-RSN-*` | `SCN-009` |
| `AC-RSN-02` | Reasoning profiles select bounded rule sets rather than executing an unconstrained inference surface. | `REQ-RSN-*` | `SCN-009` |
| `AC-RSN-03` | Derived facts remain operationally separate from explicit triples for status, deletion, and maintenance flows. | `REQ-RSN-*`, `REQ-STO-*` | `SCN-010`, `SCN-011` |
| `AC-RSN-04` | Incremental reasoning paths preserve fixpoint correctness when explicit facts change. | `REQ-RSN-*` | `SCN-011` |
| `AC-RSN-05` | Reasoning runs emit status or telemetry sufficient to diagnose convergence and failure behavior. | `REQ-RSN-*`, `REQ-OBS-*` | `SCN-009`, `SCN-014` |

## Canonical References

- [../architecture-overview.md](../architecture-overview.md)
- [../topology.md](../topology.md)
- [../contracts/reasoning_contract.md](../contracts/reasoning_contract.md)
