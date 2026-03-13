# Query Execution Contract

This contract defines the normative query behavior for `TripleStore`.

## Requirement Set

- `REQ-QRY-001`: SPARQL parsing MAY use a native adapter, but query semantics MUST remain Elixir-owned.
- `REQ-QRY-002`: Query compilation MUST pass through a typed AST and algebra stage before physical execution.
- `REQ-QRY-003`: The optimizer SHOULD apply rule-based, cost-aware, and schema-aware rewrites before execution.
- `REQ-QRY-004`: Query execution MUST use dictionary IDs and canonical triple or quad index-selection behavior rather than raw term scans where an index path exists.
- `REQ-QRY-005`: Query results SHOULD be produced lazily or streamed where the current API supports it so large result sets do not require eager full materialization.
- `REQ-QRY-006`: Graph clauses, property-path execution, validation, and authorization hooks MUST remain explicit typed execution paths rather than undocumented side behavior.
- `REQ-QRY-007`: Query timeout policy MUST be enforceable from the API boundary for eager execution paths; streaming setup MUST remain explicit about the narrower timeout scope.
- `REQ-QRY-008`: Plan caching MUST be invalidated or refreshed when writes invalidate cached assumptions, and result-cache invalidation MUST remain explicit rather than incidental.
- `REQ-QRY-009`: Query and update paths MUST return tagged errors for parse, execution, timeout, authorization, and validation failures.
- `REQ-QRY-010`: Advanced join strategies such as Leapfrog Triejoin and parallel execution MUST remain optimizer-selected execution choices, not caller-visible API modes.
- `REQ-QRY-011`: Lower-level query and update contexts MAY support actor-aware graph authorization, but facade-level APIs MUST document when that actor context is not surfaced.
