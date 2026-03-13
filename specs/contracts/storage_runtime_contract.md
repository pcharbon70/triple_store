# Storage Runtime Contract

This contract defines the normative storage behavior for `TripleStore`.

## Requirement Set

- `REQ-STO-001`: RDF terms MUST map to tagged 64-bit IDs through the dictionary layer.
- `REQ-STO-002`: Inline encodings for supported numeric or temporal values MUST preserve type-tag separation from sequence-allocated IDs.
- `REQ-STO-003`: Store schema MUST be explicit at open time and MUST determine the persisted column-family layout and key shape for that store.
- `REQ-STO-004`: Explicit writes MUST fan out atomically to the schema-appropriate index set: `spo`/`pos`/`osp` for triple stores and `gspo`/`gpos`/`spog`/`posg` for quad stores.
- `REQ-STO-005`: Derived facts MUST remain logically separate from explicit facts through the `derived` storage surface, and quad-only metadata such as provenance or ACLs MUST remain explicit persistence surfaces.
- `REQ-STO-006`: Pattern lookup MUST use the canonical index-selection rules for both triple and quad storage paths.
- `REQ-STO-007`: Sequence allocation and graph-term allocation MUST avoid ID reuse across process or node restarts.
- `REQ-STO-008`: Storage operations MUST validate paths, bounds, schema, and term inputs before mutation.
- `REQ-STO-009`: Loader, exporter, and graph-backup flows MUST keep their schema-aware graph-preservation behavior explicit rather than silently pretending all APIs are graph-equivalent.
- `REQ-STO-010`: Backup and restore flows MUST operate on canonical persisted bytes, and triple-to-quad migration MUST remain an export/import workflow rather than an in-place schema rewrite.
- `REQ-STO-011`: Storage semantics MUST remain accessible through Elixir modules and MUST NOT require callers to address RocksDB column families directly.
