# TripleStore Specs Index

## Architecture Summary

- `TripleStore` is specified as a library-first RDF triple store with a public Elixir API over persistent RocksDB storage.
- Query execution, optimization, transaction coordination, and OWL 2 RL reasoning remain BEAM-owned semantics even when parser/storage operations cross a Rustler NIF boundary.
- Dictionary encoding and triple indices (`spo`, `pos`, `osp`) are canonical storage primitives; `derived` remains a separate persistence surface for inferred facts.
- Runtime coordination is intentionally small: the OTP application owns global cache/snapshot services, while store-local managers are started from `TripleStore.open/2`.
- Operational surfaces such as telemetry, health, backup, restore, and scheduled backup are part of the architecture rather than afterthoughts.
- Specs in this directory define architectural baselines, control-plane ownership, contracts, area-level acceptance criteria, and traceability links into planning and implementation.

This directory is the canonical architecture and governance specification set for `TripleStore`.

Normative language in this directory uses RFC-2119 terms: **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY**.

## Canonical Baselines

- [architecture-overview.md](architecture-overview.md)
- [topology.md](topology.md)
- [boundaries.md](boundaries.md)
- [control-planes.md](control-planes.md)
- [specs-governance-and-compliance-guide.md](specs-governance-and-compliance-guide.md)

## Contract Layer

- [contracts/control_plane_ownership_matrix.md](contracts/control_plane_ownership_matrix.md)
- [contracts/storage_runtime_contract.md](contracts/storage_runtime_contract.md)
- [contracts/query_execution_contract.md](contracts/query_execution_contract.md)
- [contracts/transaction_and_isolation_contract.md](contracts/transaction_and_isolation_contract.md)
- [contracts/reasoning_contract.md](contracts/reasoning_contract.md)
- [contracts/observability_contract.md](contracts/observability_contract.md)

## ADRs

- [adr/ADR-0001-control-plane-authority.md](adr/ADR-0001-control-plane-authority.md)

## Conformance

- [conformance/README.md](conformance/README.md)
- [conformance/scenario_catalog.md](conformance/scenario_catalog.md)
- [conformance/spec_conformance_matrix.md](conformance/spec_conformance_matrix.md)

## Planning

- [planning/README.md](planning/README.md)

## Operations

- [operations/README.md](operations/README.md)

## Component Indexes

- [runtime/README.md](runtime/README.md)
- [storage/README.md](storage/README.md)
- [query/README.md](query/README.md)
- [reasoning/README.md](reasoning/README.md)

## Current Governance Status

- This initial baseline is documentation-first; there is no automated validator wired to these specs yet.
- Until validators exist, architecture, contract, and conformance changes SHOULD be reviewed in the same change set as the corresponding implementation changes.
- Existing project plans under `notes/planning/` remain the executable delivery roadmap and are referenced from this specs system instead of duplicated.
