# TripleStore Spec Conformance Matrix

This matrix maps requirement families to area specs and canonical scenarios.

| Requirement | Owning Contract | Primary Specs | Scenario |
|---|---|---|---|
| `REQ-CP-001`..`REQ-CP-005` | [../contracts/control_plane_ownership_matrix.md](../contracts/control_plane_ownership_matrix.md) | `specs/architecture-overview.md`, `specs/topology.md`, `specs/control-planes.md`, `specs/runtime/README.md`, `specs/runtime/public_api_and_store_lifecycle.md`, `specs/runtime/application_and_support_services.md`, `specs/storage/README.md`, `specs/query/README.md`, `specs/reasoning/README.md` | `SCN-001`, `SCN-015` |
| `REQ-STO-001`..`REQ-STO-010` | [../contracts/storage_runtime_contract.md](../contracts/storage_runtime_contract.md) | `specs/architecture-overview.md`, `specs/topology.md`, `specs/storage/README.md`, `specs/storage/dictionary_and_index_layer.md`, `specs/storage/native_backend_and_rdf_io.md` | `SCN-003`, `SCN-004`, `SCN-005`, `SCN-010`, `SCN-012`, `SCN-015` |
| `REQ-QRY-001`..`REQ-QRY-010` | [../contracts/query_execution_contract.md](../contracts/query_execution_contract.md) | `specs/architecture-overview.md`, `specs/topology.md`, `specs/query/README.md`, `specs/query/sparql_execution_pipeline.md`, `specs/query/planning_and_cache.md` | `SCN-006`, `SCN-007`, `SCN-015` |
| `REQ-TXN-001`..`REQ-TXN-010` | [../contracts/transaction_and_isolation_contract.md](../contracts/transaction_and_isolation_contract.md) | `specs/architecture-overview.md`, `specs/topology.md`, `specs/runtime/README.md`, `specs/runtime/public_api_and_store_lifecycle.md` | `SCN-002`, `SCN-007`, `SCN-008` |
| `REQ-RSN-001`..`REQ-RSN-010` | [../contracts/reasoning_contract.md](../contracts/reasoning_contract.md) | `specs/architecture-overview.md`, `specs/topology.md`, `specs/reasoning/README.md`, `specs/reasoning/materialization_and_maintenance.md` | `SCN-009`, `SCN-010`, `SCN-011`, `SCN-014` |
| `REQ-OBS-001`..`REQ-OBS-008` | [../contracts/observability_contract.md](../contracts/observability_contract.md) | `specs/architecture-overview.md`, `specs/runtime/application_and_support_services.md`, `specs/storage/native_backend_and_rdf_io.md`, `specs/query/planning_and_cache.md`, `specs/reasoning/materialization_and_maintenance.md`, `specs/operations/README.md`, `specs/operations/observability_and_recovery.md` | `SCN-012`, `SCN-013`, `SCN-014` |

## Acceptance Mapping Rule

Every `AC-*` entry in an area spec SHOULD map to at least one `REQ-*` family and one `SCN-*` scenario.

## Review Gate Policy

Until automated validators exist, reviewers SHOULD enforce:

1. area-spec changes update the conformance matrix when they introduce or remove `AC-*` behavior
2. contract changes update the relevant area docs in the same change set
3. control-plane changes update ADR-0001 and the ownership matrix together
