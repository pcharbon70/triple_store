# TripleStore Specs Getting Started

This guide explains how to use the specs system in this repository.

## 1. Read The Baseline First

Start with:

- [architecture-overview.md](architecture-overview.md)
- [topology.md](topology.md)
- [boundaries.md](boundaries.md)
- [control-planes.md](control-planes.md)

Goal: understand which parts of TripleStore own semantics, coordination, native I/O, and persisted data.

## 2. Lock Ownership In ADR-0001

Resolve control-plane conflicts using the [ownership matrix](contracts/control_plane_ownership_matrix.md) first, then [ADR-0001](adr/ADR-0001-control-plane-authority.md), then baseline and area docs.

When control-plane ownership changes, update ADR-0001 and the ownership matrix in the same change set.

## 3. Update Contracts Before Area Specs Drift

Contracts in [contracts/README.md](https://github.com/pcharbon70/triple_store/blob/main/specs/contracts/README.md) define requirement families (`REQ-*`).

If storage, query, transaction, reasoning, or observability semantics change, update the appropriate contract first or alongside the implementation.

## 4. Use Area Specs For Acceptance Criteria

Area indexes define `AC-*` acceptance criteria for:

- [runtime/README.md](https://github.com/pcharbon70/triple_store/blob/main/specs/runtime/README.md)
- [storage/README.md](https://github.com/pcharbon70/triple_store/blob/main/specs/storage/README.md)
- [query/README.md](https://github.com/pcharbon70/triple_store/blob/main/specs/query/README.md)
- [reasoning/README.md](https://github.com/pcharbon70/triple_store/blob/main/specs/reasoning/README.md)

Each `AC-*` should map back to at least one `REQ-*` family and one `SCN-*` scenario.

## 5. Keep Conformance Traceability Current

Review:

- [conformance/scenario_catalog.md](conformance/scenario_catalog.md)
- [conformance/spec_conformance_matrix.md](conformance/spec_conformance_matrix.md)

If a new behavior matters enough to specify, it should usually matter enough to trace to a scenario.

## 6. Align Planning With Existing Notes

Implementation sequencing remains in:

- [`notes/planning/overview.md`](../notes/planning/overview.md)
- [`notes/planning/phase-01-storage-foundation.md`](../notes/planning/phase-01-storage-foundation.md)
- [`notes/planning/phase-02-sparql-query-engine.md`](../notes/planning/phase-02-sparql-query-engine.md)
- [`notes/planning/phase-03-advanced-query-processing.md`](../notes/planning/phase-03-advanced-query-processing.md)
- [`notes/planning/phase-04-owl2rl-reasoning.md`](../notes/planning/phase-04-owl2rl-reasoning.md)
- [`notes/planning/phase-05-production-hardening.md`](../notes/planning/phase-05-production-hardening.md)

## 7. Manual Review Checklist

Run `mix conformance` (or `./scripts/run_conformance.sh` for the combined governance checks) to validate documentation structure and traceability. These validators do not execute scenario-specific behavior tests. Reviewers must still check:

1. Does the implementation still match the architecture overview and topology?
2. Did control-plane ownership change without updating the matrix or ADR?
3. Did a contract change without updating area specs or conformance docs?
4. Did a new feature or subsystem land without a planning reference or operational note?
