# Telemetry and Monitoring

TripleStore emits `:telemetry` events for queries, mutations, loading, backup,
caching, quads, and reasoning. Event names and metadata are defined by
`TripleStore.Telemetry`.

## Event handlers

~~~elixir
handler = fn event, measurements, metadata, _config ->
  IO.inspect({event, measurements, metadata})
end

:ok = TripleStore.Telemetry.attach_handler("my-triple-store-handler", handler)

# Later:
:ok = TripleStore.Telemetry.detach_handler("my-triple-store-handler")
~~~

`all_events/0` and category helpers such as `query_events/0` return the current
event list. `sanitize_query/2` produces bounded query metadata; telemetry and
logs should not expose raw query text by default.

## Optional collectors

Metrics are not started by the application supervisor. Start and supervise them
explicitly when needed:

~~~elixir
children = [
  {TripleStore.Metrics, name: MyStoreMetrics},
  {TripleStore.Prometheus, name: MyStorePrometheus}
]
~~~

Pass the matching `name:` option to `Metrics.get_all/1`,
`Metrics.query_metrics/1`, `Prometheus.format/1`, and related calls. The
Prometheus module formats metrics; this library does not create an HTTP
endpoint.

## Health

~~~elixir
{:ok, health} = TripleStore.Health.health(store)
{:ok, summary} = TripleStore.Health.summary(store)
~~~

`liveness/1` checks the database process. `readiness/1` also checks the
dictionary manager. Quad stores have `graph_health/3`,
`all_graphs_health/2`, and `graph_health_alerts/2`. Health output is operational
evidence for the queried store, not a substitute for application-level
authorization or end-to-end service health.

Metric collection and health scans have cost. Choose collection intervals and
graph scan limits based on measured workloads.
