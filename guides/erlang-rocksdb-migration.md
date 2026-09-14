# erlang-rocksdb Migration

The storage migration described by this document is complete in the current
tree. TripleStore uses the Hex `rocksdb` dependency and
`TripleStore.Backend.RocksDB.ErlangAdapter`. The repository no longer contains
`native/rocksdb_nif/` or a storage `TripleStore.Backend.RocksDB.NIF` module.

The Rust NIF under `native/sparql_parser_nif/` remains. It parses SPARQL with
`spargebra` and is unrelated to RocksDB storage.

## Current adapter

~~~elixir
alias TripleStore.Backend.RocksDB.ErlangAdapter

{:ok, db} = ErlangAdapter.open(path, schema: :triple)
{:ok, value} = ErlangAdapter.get(db, :spo, key)
:ok = ErlangAdapter.close(db)
~~~

For application code, prefer `TripleStore.open/2` and the higher-level
dictionary, loader, index, query, and backup APIs. Direct adapter callers are
responsible for correct encoded keys, column families, batch fanout, and
resource cleanup.

`ErlangAdapter` provides point operations, atomic write/delete/mixed batches,
managed iterators, prefix iterators, folds, streams, snapshots, schema
validation, WAL flushing, and a bulk-load open mode. Its iterator references are
processes and must be closed on every path.

## On-disk compatibility

Schema metadata distinguishes triple schema version 1 from quad schema version
2. Existing legacy triple databases without schema metadata have a specific
compatibility path; quad stores require their metadata. A schema mismatch is
rejected during open.

The move from triple to quad storage is not an in-place migration. Export the
source graph, create a new `schema: :quad` store, and import into the intended
default or named graph. Use dataset-aware N-Quads or TriG APIs when source data
already has graph identities.

## Build requirements

The `rocksdb` dependency builds a C++ NIF. Local and CI environments therefore
need C/C++ build tools, CMake, pkg-config, RocksDB, and compression development
libraries. The exact Ubuntu packages and
`ERLANG_ROCKSDB_OPTS=-DCMAKE_POLICY_VERSION_MINIMUM=3.5` setting are recorded in
`.github/workflows/ci.yml`.

The Rust toolchain is still needed to build the SPARQL parser. Generated native
binaries under `priv/native/` are local build artifacts and must not be
committed.
