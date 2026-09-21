# SPARQL Updates: Triple Schema

Use `TripleStore.update/2` with SPARQL Update text.

~~~elixir
{:ok, 1} =
  TripleStore.update(store, """
  INSERT DATA {
    <http://example.org/alice> <http://example.org/name> "Alice"
  }
  """)

{:ok, count} =
  TripleStore.update(store, """
  DELETE { ?s <http://example.org/status> "old" }
  INSERT { ?s <http://example.org/status> "current" }
  WHERE  { ?s <http://example.org/status> "old" }
  """)
~~~

The executor handles data updates, DELETE/INSERT WHERE, and graph-management
forms parsed by the SPARQL parser. One triple or quad MODIFY submits its
explicit-index deletes and inserts in one mixed RocksDB batch.

## Coordination boundary

An open store has one live transaction coordinator by default.
`TripleStore.update/2` sends every public SPARQL update for that handle through
the same serialized queue. Expert callers can supply an existing coordinator to
`open/2` with `transaction: {:external, manager}`; that coordinator remains
caller-owned and is not stopped by `TripleStore.close/1`. Direct load, insert,
and delete calls do not share the coordinator queue.

A request containing several SPARQL operations is planned sequentially against
a staged view. Later operations see earlier staged inserts and deletes. The
explicit indices are committed once after every operation succeeds; a planning
or final storage error leaves them unchanged. Dictionary IDs allocated during
planning can remain unused after a failed request. `LOAD` remains unsupported
and rejects the full request before explicit-index commit.

## Cache and reasoning behavior

Successful supported updates invalidate every active named materialized-result
cache for the affected open store. Failed and denied writes do not invalidate a
valid result. The managed coordinator also invalidates the application's
supervised plan cache after a successful request with mutations. An external
coordinator invalidates the plan cache configured in its `:plan_cache` option.

Explicit updates do not automatically perform a complete reasoning-maintenance
cycle.
