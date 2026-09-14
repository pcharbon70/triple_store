# Storage Layer

`TripleStore.Backend.RocksDB.ErlangAdapter` is the active storage boundary. It
is a GenServer that owns the RocksDB database and column-family handles. The
older `TripleStore.Backend.RocksDB.NIF` wrapper and `native/rocksdb_nif/` do not
exist in the current tree.

## Opening and point operations

~~~elixir
alias TripleStore.Backend.RocksDB.ErlangAdapter

{:ok, db} = ErlangAdapter.open(path, schema: :triple)
:ok = ErlangAdapter.put(db, :spo, key, value)
{:ok, value} = ErlangAdapter.get(db, :spo, key)
{:ok, true} = ErlangAdapter.exists(db, :spo, key)
:ok = ErlangAdapter.delete(db, :spo, key)
:ok = ErlangAdapter.close(db)
~~~

Applications normally use `TripleStore.open/2` and higher-level APIs. Raw index
keys must use the encoding functions in `Index` or `QuadIndex`.

## Atomic mutations

`write_batch/3`, `delete_batch/3`, and `mixed_batch/3` apply a group of
operations atomically. Triple mutations fan out to `spo`, `pos`, and `osp`;
quad mutations fan out to `gspo`, `gpos`, `spog`, and `posg`. Avoid separate
point writes for a logical RDF mutation.

## Iterators and snapshots

`prefix_iterator/4` and `iterator/3` return iterator processes. Call
`iterator_close/1` on success, exhaustion, early termination, and error.
`prefix_stream/4` and `snapshot_stream/4` provide managed streams where their
lazy lifetime fits the caller. `fold/6` and `fold_keys/6` are convenient for
bounded scans.

~~~elixir
{:ok, snapshot} = ErlangAdapter.snapshot(db)

try do
  ErlangAdapter.snapshot_get(db, snapshot, :spo, key)
after
  ErlangAdapter.release_snapshot(db, snapshot)
end
~~~

`TripleStore.Snapshot.with_snapshot/3` provides ownership and TTL management for
registered snapshots. Snapshot support does not by itself make facade queries
concurrent snapshot reads.

## Dictionary and IDs

`TripleStore.Dictionary` and `Dictionary.Manager` maintain bidirectional
term-to-ID mappings. IDs are tagged 64-bit integers. Supported numeric and
temporal values can be encoded inline; other RDF terms use persistent dictionary
entries. Sequence allocation must remain restart-safe, and the default graph
keeps the reserved ID `0`.

## Bulk loading

`ErlangAdapter.open_for_bulk_load/2` adjusts RocksDB options for initial ingest.
`TripleStore.Loader` supplies schema-aware batching and RDF parsing. Bulk-load
settings change performance and durability tradeoffs; use ordinary open settings
again for normal operation.
