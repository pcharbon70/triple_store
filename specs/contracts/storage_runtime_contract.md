# Storage Runtime Contract

This contract defines the normative storage behavior for `TripleStore`.

## Requirement Set

- `REQ-STO-001`: RDF terms MUST map to tagged 64-bit IDs through the dictionary layer.
- `REQ-STO-002`: Inline encodings for supported numeric or temporal values MUST preserve type-tag separation from sequence-allocated IDs.
- `REQ-STO-003`: Explicit triple writes MUST fan out atomically to `spo`, `pos`, and `osp`.
- `REQ-STO-004`: Inferred triples MUST remain logically separate from explicit triples through the `derived` storage surface.
- `REQ-STO-005`: Pattern lookup MUST use the canonical index-selection rules for `spo`, `pos`, and `osp`.
- `REQ-STO-006`: Native RocksDB operations expected to exceed BEAM-friendly execution time MUST run on dirty schedulers.
- `REQ-STO-007`: Sequence allocation MUST avoid ID reuse across process or node restarts.
- `REQ-STO-008`: Storage operations MUST validate paths, bounds, and term inputs before mutation.
- `REQ-STO-009`: Backup and restore flows MUST operate on canonical persisted bytes and preserve restore usability.
- `REQ-STO-010`: Storage semantics MUST remain accessible through Elixir modules and MUST NOT require callers to address RocksDB column families directly.
