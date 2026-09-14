# OTP and Concurrency

TripleStore uses OTP processes for resource ownership and coordination. The
storage adapter owns RocksDB handles; dictionary managers and sequence
allocators serialize term-ID allocation; iterator processes own native
iterators; caches, snapshots, metrics, and scheduled backups each have separate
lifecycles.

## Supervision

The application uses a `:one_for_one` supervisor for the query-cache registry,
`SPARQL.PlanCache`, and `Snapshot`. Opening a database is not equivalent to
adding every store-specific helper to that supervisor. `TripleStore.open/2`
starts the storage adapter and dictionary manager and returns their references
in a handle. Close the handle with `TripleStore.close/1`.

Optional statistics, result-cache, metrics, Prometheus, and scheduled-backup
processes must be started and stopped by the caller or its supervisor.

## Coordination boundaries

`Transaction` serializes requests sent to one coordinator. A facade update uses
the coordinator in the handle or starts a temporary one for that call. Separate
temporary coordinators, direct loader writes, and direct insert/delete calls do
not share a global lock.

Update execution is synchronous, but its created snapshot is not injected into
the query context. A request containing several update operations can commit
multiple storage batches; a later failure does not imply rollback of every
earlier batch. Treat these as current isolation boundaries when designing
concurrent callers.

## Resource ownership

Release snapshots and close iterators on every path. Managed streams retain
resources until exhaustion or halt, so callers must consume them in a scope
whose cleanup is reliable. Do not rely on garbage collection for RocksDB
resource release.

Avoid global names for store-specific processes unless the application truly
requires one instance. Tests that use named services or shared ETS state should
not run asynchronously. Use unique disposable database paths and close all
owners before deleting them.

## Timeouts

Eager query execution runs in timeout isolation. Lazy SELECT streaming performs
work during enumeration and does not apply that timeout to later consumption.
Supervisors and callers should set deadlines at the boundary they actually
control.
