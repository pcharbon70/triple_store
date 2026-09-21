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

Each opened store has one transaction coordinator, and public SPARQL updates on
that handle share its serialized queue. `Transaction.query/3` calls sent to the
same coordinator wait behind updates. Facade queries and direct load,
insert, and delete calls do not use that queue.

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

The scheduler is opt-in and remains an application-owned process. It monitors
the store's dictionary manager as its lifecycle sentinel: closing the store
cancels the interval timer and any owned backup task, emits a scheduled-backup
stop event, and terminates the scheduler. Explicitly stop the scheduler first
when the application needs to distinguish operator shutdown from store closure.

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
