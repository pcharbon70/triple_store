# TripleStore Contract Layer

Contracts in this directory define the normative requirement families for `TripleStore`.

Each contract uses `REQ-*` identifiers. Area specs and conformance docs should map back to these requirement families.

## Contracts

- [control_plane_ownership_matrix.md](control_plane_ownership_matrix.md)
- [storage_runtime_contract.md](storage_runtime_contract.md)
- [query_execution_contract.md](query_execution_contract.md)
- [transaction_and_isolation_contract.md](transaction_and_isolation_contract.md)
- [reasoning_contract.md](reasoning_contract.md)
- [observability_contract.md](observability_contract.md)

## Traceability Rule

If an implementation change affects one of these semantic areas, the related contract SHOULD be reviewed in the same change set.
