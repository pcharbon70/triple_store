# Configuration and Operations: Triple Schema

## RocksDB recommendations

`TripleStore.Config.RocksDB` produces configuration maps. The facade's
`TripleStore.open/2` option type does not accept one of these maps as a complete
configuration value;
lower-level adapter callers own translation into adapter options.

~~~elixir
recommended = TripleStore.Config.RocksDB.recommended()
names = TripleStore.Config.RocksDB.preset_names()
small = TripleStore.Config.RocksDB.preset(:development)
bytes = TripleStore.Config.RocksDB.estimate_memory_usage(small)
summary = TripleStore.Config.RocksDB.format_summary(recommended)
~~~

Use `preset_names/0` before choosing a preset. Configuration values are
recommendations, not measured performance guarantees.

## Query and loader controls

Eager query timeout defaults to 30 seconds. Loader batch size defaults to 10,000.
Measure changes with repository benchmarks.

~~~elixir
{:ok, rows} = TripleStore.query(store, query, timeout: 10_000)
{:ok, count} = TripleStore.load(store, "data.nt", batch_size: 5_000)
~~~

Concurrent calls can share a store handle, but this does not create a shared
transaction coordinator.

## Health, statistics, and scheduled backup

~~~elixir
{:ok, health} = TripleStore.health(store)
{:ok, statistics} = TripleStore.stats(store)

{:ok, scheduler} =
  TripleStore.schedule_backup(store, "/backups/triple-store",
    interval: :timer.hours(1),
    max_backups: 24
  )

{:ok, backup_status} = TripleStore.ScheduledBackup.status(scheduler)
:ok = TripleStore.ScheduledBackup.stop(scheduler)
~~~

`TripleStore.Health.health/2` provides the richer component view.
`TripleStore.Metrics` and `TripleStore.Prometheus` are opt-in processes and are
not started by the application supervisor.

~~~elixir
{:ok, metrics_pid} = TripleStore.Metrics.start_link()
metrics = TripleStore.Metrics.get_all()
GenServer.stop(metrics_pid)
~~~

The supervised `SPARQL.PlanCache`, optional `Query.Cache`, legacy
`Statistics.Cache`, and intended successor `Statistics.Server` are distinct
services.
