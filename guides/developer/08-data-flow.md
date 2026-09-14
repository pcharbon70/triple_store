# Data Flow

This guide traces the current boundaries for common operations. It complements
the detailed module guides and the normative specifications.

## Open and close

~~~text
TripleStore.open/2
  -> validate path and requested schema
  -> ErlangAdapter opens/creates schema column families
  -> Dictionary.Manager starts
  -> return store handle

TripleStore.close/1
  -> stop dictionary manager
  -> close ErlangAdapter
~~~

Separately started helpers require separate shutdown.

## Query

~~~text
TripleStore.query/3
  -> SPARQL.Query
  -> Parser (Rust NIF parses; Elixir converts)
  -> Algebra
  -> Optimizer / PlanCache
  -> Executor
  -> dictionary and index lookups
  -> typed query result
~~~

Eager execution applies its timeout around the request. A lazy SELECT stream
continues work during enumeration. Quad GRAPH patterns pass through graph
selection and lower-level authorization hooks. ACL-controlled requests skip the
optional result cache.

## Load and update

~~~text
insert/delete/load
  -> Loader
  -> dictionary IDs
  -> one batch across all explicit indices

SPARQL update
  -> UpdateExecutor through a configured or temporary Transaction
  -> pattern evaluation and authorization
  -> one or more storage batches
  -> cache invalidation after supported successful mutations
~~~

Direct writes and independent transaction coordinators do not share a global
lock. A multi-operation SPARQL request is not guaranteed to roll back earlier
batches when a later operation fails.

Dataset import uses dedicated N-Quads or TriG loader functions. The generic
facade load path is graph-oriented and must not be used to claim dataset graph
preservation.

## Reasoning

~~~text
explicit facts + selected rules
  -> RuleCompiler
  -> SemiNaive iterations
  -> in-memory closure or DerivedStore
  -> provenance/rederivation for maintained inferred facts
~~~

The facade's default local materialization reads triple indices and discards the
computed fact set after returning statistics. Persistent quad reasoning uses
`GraphScopedReasoner` and explicit graph-scope configuration.

## Backup and restore

`Backup` creates and restores store-level RocksDB backups and records schema
metadata. `GraphBackup` exports or restores one graph. Verify the backup and
schema before restore. A triple backup cannot be treated as an in-place
quad-schema migration.
