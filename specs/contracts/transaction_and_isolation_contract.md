# Transaction And Isolation Contract

This contract defines the normative write-coordination behavior for `TripleStore`.

## Requirement Set

- `REQ-TXN-001`: Mutating SPARQL update paths MUST preserve single-writer coordination semantics.
- `REQ-TXN-002`: Update execution MUST use atomic write fanout at the storage boundary.
- `REQ-TXN-003`: Reads concurrent with updates through the transaction coordinator MUST observe a consistent store view rather than partial fanout.
- `REQ-TXN-004`: Temporary transaction coordinators MUST preserve the same semantics as a managed long-lived coordinator.
- `REQ-TXN-005`: Failed updates MUST return tagged errors and MUST NOT leave partial explicit-index mutation behind.
- `REQ-TXN-006`: Update completion MUST trigger plan-cache invalidation and SHOULD trigger result-cache invalidation or statistics refresh behavior when relevant.
- `REQ-TXN-007`: Public mutation surfaces MUST document their coordination differences explicitly; direct load, insert, and delete paths MUST NOT be misrepresented as equivalent to transaction-backed SPARQL update isolation.
- `REQ-TXN-008`: Snapshot-oriented read support MUST remain subordinate to the single-writer model and MUST stay explicit at the transaction-manager boundary.
- `REQ-TXN-009`: Transaction timeouts MUST be explicit and separately configurable for reads and writes when the coordinator owns those flows.
- `REQ-TXN-010`: `Transaction.query/3` MAY provide snapshot-aware reads, but `TripleStore.query/3` MUST NOT be specified as implicitly using that coordinator.
- `REQ-TXN-011`: Transaction coordination semantics MUST remain Elixir-owned even when underlying storage mutation is delegated to RocksDB through the adapter layer.

## Current Implementation Status

The requirements above remain normative targets; this section describes observed implementation limits, not exceptions to those requirements.

- `lib/triple_store/transaction.ex` handles queries and updates synchronously in one GenServer. Calls to the same coordinator are serialized; queries wait behind updates rather than reading a pre-update snapshot concurrently.
- Update execution creates a RocksDB snapshot and releases it in `after`, but does not pass it to the query context or publish it in coordinator state. `Transaction.query/3` prepares and executes against `%{db: db, dict_manager: dict_manager}`. Snapshot lifecycle support is therefore not evidence of snapshot-aware transaction queries (`REQ-TXN-003`, `REQ-TXN-008`, `REQ-TXN-010`).
- `TripleStore.open/2` starts one store-owned coordinator and `TripleStore.update/2` always uses it. Explicit `{:external, manager}` configuration takes precedence and stays caller-owned. Direct loader, insert, and delete paths remain outside this queue (`REQ-TXN-001`, `REQ-TXN-004`, `REQ-TXN-007`).
- `lib/triple_store/sparql/update_executor.ex` executes a request's operations sequentially and stops at the first error. Previously committed operations are not rolled back. Atomic storage fanout applies to each batch, not an entire multi-operation request; whole-request failure atomicity remains a gap against `REQ-TXN-005`.
- One triple or quad MODIFY submits its DELETE-before-INSERT explicit-index work
  as one RocksDB mixed batch. Its affected count preserves template-application
  behavior: duplicate applications are counted, deletes whose terms have never
  been allocated are omitted, and an encoded delete for an absent statement is
  counted as submitted. Dictionary allocation for inserted terms occurs before
  the explicit-index batch, so a failed batch can leave unused dictionary IDs
  while all explicit indices remain unchanged.
- `SCN-008` must be assessed with these boundaries explicit. Documentation validation and individual batch tests do not establish request-wide isolation or rollback.
