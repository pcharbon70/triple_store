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
