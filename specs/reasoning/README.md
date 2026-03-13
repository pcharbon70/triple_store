# Reasoning Specs Index

## Purpose

`Reasoning Specs Index` is the entry point for OWL 2 RL and derived-fact lifecycle documentation in `TripleStore`.

It covers:

- rule representation and compilation
- legacy triple materialization and graph-scoped quad reasoning
- incremental maintenance
- derived-fact storage, provenance, and reasoning status

## Control Plane

Primary ownership: **Reasoning Plane**.

## Primary Reasoning Components

- `TripleStore.Reasoner.Rule`
- `TripleStore.Reasoner.Rules`
- `TripleStore.Reasoner.RuleCompiler`
- `TripleStore.Reasoner.RuleOptimizer`
- `TripleStore.Reasoner.SemiNaive`
- `TripleStore.Reasoner.Incremental`
- `TripleStore.Reasoner.IncrementalQuad`
- `TripleStore.Reasoner.DerivedStore`
- `TripleStore.Reasoner.DeleteWithReasoning`
- `TripleStore.Reasoner.DeleteWithReasoningQuad`
- `TripleStore.Reasoner.ForwardRederive`
- `TripleStore.Reasoner.ForwardRederiveQuad`
- `TripleStore.Reasoner.BackwardTrace`
- `TripleStore.Reasoner.BackwardTraceQuad`
- `TripleStore.Reasoner.GraphScopedReasoner`
- `TripleStore.Reasoner.GraphReasoningConfig`
- `TripleStore.Reasoner.GraphReasoningStatus`
- `TripleStore.Reasoner.DerivationProvenance`
- `TripleStore.Reasoner.ReasoningProfile`
- `TripleStore.Reasoner.ReasoningStatus`
- `TripleStore.Reasoner.Telemetry`

## Component Specs

- [materialization_and_maintenance.md](materialization_and_maintenance.md)

## Current Codebase Notes

- The current subsystem includes status tracking, schema-aware helpers, tracing, provenance, and rederivation in addition to the main materialization loop.
- `TripleStore.materialize/2` still defaults to a legacy triple-materialization path for local scope, while explicit graph APIs drive the newer graph-scoped quad reasoner.
- `DerivedStore` is a distinct persisted surface with separate triple-mode and quad-mode usage patterns.
- Graph reasoning configuration and status are first-class runtime artifacts in the quad store path.

## Acceptance Criteria

| Acceptance ID | Criterion | Related Requirements | Related Scenarios |
|---|---|---|---|
| `AC-RSN-01` | Materialization uses forward-chaining evaluation with semi-naive delta processing as the canonical full-run algorithm. | `REQ-RSN-*` | `SCN-009` |
| `AC-RSN-02` | Reasoning profiles select bounded rule sets rather than executing an unconstrained inference surface. | `REQ-RSN-*` | `SCN-009` |
| `AC-RSN-03` | Derived facts, provenance, and graph-specific status remain operationally separate from explicit facts for status, deletion, and maintenance flows. | `REQ-RSN-*`, `REQ-STO-*` | `SCN-010`, `SCN-011` |
| `AC-RSN-04` | Incremental reasoning paths preserve fixpoint correctness when explicit facts change in both triple and graph-scoped quad workflows. | `REQ-RSN-*` | `SCN-011` |
| `AC-RSN-05` | Reasoning runs emit status or telemetry sufficient to diagnose convergence, per-graph behavior, and failure behavior. | `REQ-RSN-*`, `REQ-OBS-*` | `SCN-009`, `SCN-014` |

## Canonical References

- [../architecture-overview.md](../architecture-overview.md)
- [../topology.md](../topology.md)
- [../contracts/reasoning_contract.md](../contracts/reasoning_contract.md)
