# Transaction And Isolation Contract

This contract defines the normative write-coordination behavior for `TripleStore`.

## Requirement Set

- `REQ-TXN-001`: Mutating SPARQL update paths MUST preserve single-writer coordination semantics.
- `REQ-TXN-002`: Update execution MUST use atomic write fanout at the storage boundary.
- `REQ-TXN-003`: Reads concurrent with updates through the transaction coordinator MUST observe a consistent store view rather than partial fanout.
- `REQ-TXN-004`: A caller-supplied coordinator configured at store open MUST preserve the same serialized update semantics as the managed coordinator and MUST remain caller-owned.
- `REQ-TXN-005`: Failed updates MUST return tagged errors and MUST NOT leave partial explicit-index mutation behind.
- `REQ-TXN-006`: Update completion MUST trigger plan-cache invalidation and SHOULD trigger result-cache invalidation or statistics refresh behavior when relevant.
- `REQ-TXN-007`: Public mutation surfaces MUST document their coordination differences explicitly; direct load, insert, and delete paths MUST NOT be misrepresented as equivalent to transaction-backed SPARQL update isolation.
- `REQ-TXN-008`: Transaction APIs MUST NOT claim snapshot isolation unless the
  query execution context actually consumes the snapshot; storage snapshot
  support MUST remain an explicit, separately owned surface.
- `REQ-TXN-009`: Transaction timeouts MUST be explicit and separately configurable for reads and writes when the coordinator owns those flows.
- `REQ-TXN-010`: `Transaction.query/3` MAY provide snapshot-aware reads, but `TripleStore.query/3` MUST NOT be specified as implicitly using that coordinator.
- `REQ-TXN-011`: Transaction coordination semantics MUST remain Elixir-owned even when underlying storage mutation is delegated to RocksDB through the adapter layer.

## Current Implementation Status

The requirements above remain normative targets; this section describes observed implementation limits, not exceptions to those requirements.

- `lib/triple_store/transaction.ex` handles queries and updates synchronously in one GenServer. Calls to the same coordinator are serialized; queries wait behind updates rather than reading a pre-update snapshot concurrently.
- Update execution does not allocate a RocksDB snapshot. `Transaction.query/3`
  waits in the same GenServer queue and reads only after the preceding update
  commits or fails. The deprecated `current_snapshot/1` compatibility function
  returns `nil`, and the former `current_snapshot` and `update_in_progress`
  state fields are absent (`REQ-TXN-003`, `REQ-TXN-008`, `REQ-TXN-010`).
- `TripleStore.open/2` starts one store-owned coordinator and `TripleStore.update/2` always uses it. Explicit `{:external, manager}` configuration takes precedence and stays caller-owned. Direct loader, insert, and delete paths remain outside this queue (`REQ-TXN-001`, `REQ-TXN-004`, `REQ-TXN-007`).
- `lib/triple_store/sparql/update_executor.ex` plans a request's operations sequentially against an overlay. A later operation observes earlier staged puts and deletes. Any planning, authorization, lookup, or conversion error discards the overlay, and successful requests submit one canonical mixed batch (`REQ-TXN-002`, `REQ-TXN-005`).
- INSERT DATA, DELETE DATA, MODIFY, CLEAR, CREATE, DROP, COPY, MOVE, and ADD use
  the staged request view. Unsupported LOAD requests fail before explicit-index
  commit, including when preceded by otherwise valid operations.
- The overlay stores one final mutation per column-family/key pair, preserving
  request order and DELETE-before-INSERT final state. A final batch failure is
  returned as `{:error, {:storage, reason}}` and leaves every explicit index
  unchanged.
- Dictionary allocation for inserted terms occurs before the explicit-index
  batch, so a failed request can leave unused dictionary IDs while all explicit
  indices remain unchanged.
- Result-cache, statistics-cache, and request-level success telemetry effects
  are published only after the final batch succeeds. The coordinator performs
  its configured plan-cache invalidation and statistics callback once for the
  successful request. The managed store coordinator uses the supervised
  `SPARQL.PlanCache`; external coordinators retain caller-supplied cache
  configuration.
- `test/triple_store/phase_2_transaction_integration_test.exs` exercises
  `SCN-008` for both schemas. It verifies failed-batch cache and index state,
  public update ordering, staged-state invisibility, successful cache
  invalidation, and reopen coherence. The direct loader, insert, delete, and
  facade-query paths remain outside the coordinator; those boundaries prevent
  describing the whole store as globally transaction-isolated.
