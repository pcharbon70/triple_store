# Configuration and Operations: Quad Schema

Quad stores share the query timeout, loader batching, RocksDB recommendations,
metrics, and scheduled-backup APIs described in the
[triple configuration guide](../triples/06-configuration.md). Quad storage has
four explicit indices and additional ACL, provenance, and derived persistence
surfaces, so measure memory and storage with representative data.

## Graph statistics and health

These expert APIs use internal graph IDs:

~~~elixir
{:ok, summary} = TripleStore.Statistics.graph_summary(store.db, graph_id)
{:ok, health} = TripleStore.Health.graph_health(store, graph_id)
{:ok, all_graphs} = TripleStore.Health.all_graphs_health(store)
~~~

List RDF graph terms with `QuadOperations`, not the `TripleStore` facade:

~~~elixir
{:ok, graph_terms} = TripleStore.QuadOperations.list_graphs(store.db)
~~~

## Full-store and graph backup

~~~elixir
{:ok, metadata} = TripleStore.backup(store, "/backups/quad-store")

{:ok, graph_metadata} =
  TripleStore.GraphBackup.backup_graph(
    store,
    graph_id,
    "/backups/graph.nq"
  )
~~~

`TripleStore.schedule_backup/3` schedules full-store backups. `GraphBackup` has
no graph scheduler; applications needing one must schedule
`GraphBackup.backup_graph/4` themselves.

## Optional services

`TripleStore.Metrics`, `TripleStore.Prometheus`, `TripleStore.Query.Cache`, and
statistics helpers started outside the default supervisor remain caller-owned.
ACL-aware query contexts bypass result caching. Low-level graph, loader,
adapter, and ACL mutations do not receive facade cache invalidation
automatically.
