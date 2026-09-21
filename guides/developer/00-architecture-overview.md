# Architecture Overview

TripleStore is an embedded Elixir/OTP RDF database library. RocksDB persists RDF
term mappings, explicit indices, derived facts, and related metadata. A Rustler
NIF parses SPARQL with `spargebra`; query semantics, optimization, execution,
transactions, and reasoning remain in Elixir.

## Runtime entry points

`TripleStore.open/2` validates the path and schema, opens
`Backend.RocksDB.ErlangAdapter`, and starts a dictionary manager. The returned
handle contains `db`, `dict_manager`, `transaction`, `path`, and `schema`.
`transaction` is `nil` unless the caller supplies a coordinator.

The application supervisor starts:

- `TripleStore.Query.Cache.Registry`
- `TripleStore.SPARQL.PlanCache`
- `TripleStore.Snapshot`

Statistics servers, result caches, metrics, Prometheus collection, and scheduled
backup processes are optional services with their own lifecycle.

## Persistent schemas

The triple schema is version 1 and stores 24-byte keys in `spo`, `pos`, and
`osp`. The quad schema is version 2 and stores 32-byte keys in `gspo`, `gpos`,
`spog`, and `posg`. Components are unsigned big-endian 64-bit dictionary IDs.
Every explicit mutation must update the schema's indices in one RocksDB batch.

The default graph ID is `0`. Schema metadata is persisted, and opening a
database with a different requested schema fails. Moving from triple to quad
storage requires export into a new store and import there.

## Request flows

A query enters `TripleStore.SPARQL.Query`, then passes through the parser,
algebra translation, optimizer, and executor. Execution uses dictionary IDs and
index scans before materializing RDF terms for the result.

The facade's `insert/2`, `delete/2`, and load functions write directly through
the loader. `update/2` uses the store-owned transaction coordinator created by
`open/2`. `query/3` calls the query pipeline directly. These paths do not share
one global lock.

Derived facts use the separate `derived` persistence surface. Deletion,
backup, export, and incremental reasoning must preserve the distinction between
explicit and inferred data, including graph scope and provenance where used.

## Ownership boundaries

`ErlangAdapter` owns storage handles and exposes bounded calls, managed
iterators, folds, snapshots, and batches. The parser NIF owns only parsing.
Elixir owns query and update semantics, authorization decisions, resource
limits, transaction coordination, and reasoning. See
[ADR-0001](../../specs/adr/ADR-0001-control-plane-authority.md) for the normative
authority split.
