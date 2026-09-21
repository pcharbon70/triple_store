# SPARQL Updates: Quad Schema

~~~elixir
{:ok, 1} =
  TripleStore.update(store, """
  INSERT DATA {
    GRAPH <http://example.org/people> {
      <http://example.org/alice> <http://example.org/name> "Alice"
    }
  }
  """)
~~~

The update executor handles parsed graph-management operations such as
`CREATE`, `DROP`, `CLEAR`, `COPY`, `MOVE`, and `ADD`, as well as data updates
and DELETE/INSERT WHERE.

~~~elixir
{:ok, count} =
  TripleStore.update(store, """
  DELETE { GRAPH ?g { ?s <http://example.org/status> "old" } }
  INSERT { GRAPH ?g { ?s <http://example.org/status> "current" } }
  WHERE  { GRAPH ?g { ?s <http://example.org/status> "old" } }
  """)
~~~

All operations in one parsed request execute sequentially against a staged
view. Later MODIFY or graph-management operations can read earlier staged
changes. After every operation validates, deletes and inserts for all four
indices are submitted in one ordered RocksDB batch; a planning, authorization,
or batch failure leaves those indices unchanged. Dictionary allocation can
happen before the batch and may leave unused IDs after failure.

## Actor-aware authorization

The facade has no actor option. Use `TripleStore.SPARQL.UpdateExecutor` with a
context carrying `:user` and configured `TripleStore.SPARQL.Authorization`
state. Every graph produced by variable substitution is checked for write
access before the first explicit-index mutation. A denied target returns
`{:error, :unauthorized}`.

The facade's context omits `:user`; the update helpers treat that `:public`
context as an internal/public operation and allow it. Applications that require
per-actor write policy must call the lower-level executor with an actor rather
than exposing the facade update directly.

## Commit and cache boundaries

A multi-operation SPARQL request publishes one explicit-index commit after all
operations succeed. Every successful request with mutations invalidates the
affected open store in active named result caches; failed and denied requests
leave both valid entries and their store generation unchanged. `LOAD` remains
unsupported and rejects the complete request before commit.

Direct `QuadOperations` calls have different coordination and invalidation
responsibilities. Consult the
[transaction contract](../../../specs/contracts/transaction_and_isolation_contract.md).
