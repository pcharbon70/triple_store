# Query Execution Contract

This contract defines the normative query behavior for `TripleStore`.

## Requirement Set

- `REQ-QRY-001`: SPARQL parsing MAY use a native adapter, but query semantics MUST remain Elixir-owned.
- `REQ-QRY-002`: Query compilation MUST pass through a typed AST and algebra stage before physical execution.
- `REQ-QRY-003`: The optimizer SHOULD apply rule-based and cost-aware rewrites before execution.
- `REQ-QRY-004`: Query execution MUST use dictionary IDs and canonical index-selection behavior rather than raw term scans where an index path exists.
- `REQ-QRY-005`: Query results SHOULD be produced lazily so large result sets do not require eager full materialization.
- `REQ-QRY-006`: Property-path execution MUST remain bounded and typed under failure or limit conditions.
- `REQ-QRY-007`: Query timeout policy MUST be enforceable from the API boundary.
- `REQ-QRY-008`: Plan caching MUST be invalidated or refreshed when writes invalidate cached assumptions.
- `REQ-QRY-009`: Query paths MUST return tagged errors for parse, execution, timeout, and validation failures.
- `REQ-QRY-010`: Advanced join strategies such as Leapfrog Triejoin MUST remain optimizer-selected execution choices, not caller-visible API modes.
