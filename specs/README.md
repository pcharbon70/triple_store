# TripleStore Specs Index

## Architecture Summary

- `TripleStore` is a library-first RDF store with two persisted schemas: triple store v1 and quad store v2.
- The primary external surface is `TripleStore`, but expert modules such as `TripleStore.Update`, `TripleStore.GraphBackup`, `TripleStore.QuadOperations`, `TripleStore.Health`, and `TripleStore.SPARQL.Authorization` expose narrower capabilities without changing overall control-plane ownership.
- Semantic ownership stays in Elixir: dictionary encoding, index and quad-index selection, SPARQL algebra, query optimization, update semantics, reasoning, and operational policy remain BEAM-owned.
- Native code is bounded to adapter work: `sparql_parser_nif` parses query text, while `erlang-rocksdb` executes storage primitives behind `TripleStore.Backend.RocksDB.ErlangAdapter`.
- The default OTP runtime is intentionally small. `TripleStore.Application` supervises only `TripleStore.SPARQL.PlanCache` and `TripleStore.Snapshot`; store-local managers and most helper services are caller-managed or started dynamically.
- Storage is schema-explicit. Triple stores persist `id2str`, `str2id`, `spo`, `pos`, `osp`, `derived`, and `numeric_range`. Quad stores persist `id2str`, `str2id`, `gspo`, `gpos`, `spog`, `posg`, `derived`, `derivation_provenance`, `numeric_range`, and `acl`.
- Query processing is graph-aware in quad schema and default-graph-only in triple schema. Graph ACL checks exist in lower-level query and update contexts when a `:user` is supplied.
- Reasoning is dual-mode. A legacy triple-materialization path coexists with graph-scoped quad reasoning, per-graph configuration and status, incremental quad maintenance, and derivation provenance.
- Operational surfaces include telemetry, health, statistics, full-store backup and restore, per-graph backup and restore, scheduled backup, metrics, and Prometheus export, with several helpers remaining opt-in.

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

- `mix conformance` validates the current governance and conformance graph for `specs/`.
- `mix conformance --governance-only` checks ownership, required files, acceptance-table shape, and identifier uniqueness.
- `mix conformance --conformance-only` checks `REQ-*`/`SCN-*` coverage, matrix integrity, and acceptance evidence paths.
- Governance validation also rejects tracked generated native binaries under `priv/native`; parser NIF artifacts are local build outputs, not canonical source.
- Shell wrappers are available at `scripts/validate_specs_governance.sh` and `scripts/run_conformance.sh`.
- Architecture, contract, and conformance changes SHOULD still be reviewed in the same change set as the corresponding implementation changes.
- Existing project plans under `notes/planning/` remain the executable delivery roadmap and are referenced from this specs system instead of duplicated.
