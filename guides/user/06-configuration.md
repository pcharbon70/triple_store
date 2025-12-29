# Configuration & Performance

This guide covers configuring and tuning TripleStore for optimal performance.

## RocksDB Configuration

The storage layer uses RocksDB with configurable memory settings.

### Memory Presets

```elixir
# Get recommended configuration for your system
config = TripleStore.Config.RocksDB.recommended()

# Or use specific presets
config = TripleStore.Config.RocksDB.preset(:development)
config = TripleStore.Config.RocksDB.preset(:production_low_memory)
config = TripleStore.Config.RocksDB.preset(:production_high_memory)
config = TripleStore.Config.RocksDB.preset(:write_heavy)
```

### Preset Details

| Preset | Block Cache | Write Buffer | Best For |
|--------|-------------|--------------|----------|
| `development` | 128 MB | 32 MB × 2 | Local development |
| `production_low_memory` | 256 MB | 32 MB × 2 | Memory-constrained servers |
| `production_high_memory` | 4 GB | 128 MB × 4 | High-performance servers |
| `write_heavy` | 1 GB | 256 MB × 4 | Bulk loading workloads |

### Custom Memory Budget

```elixir
# Configure for specific memory budget
config = TripleStore.Config.RocksDB.for_memory_budget(
  8 * 1024 * 1024 * 1024  # 8 GB
)

# Estimate memory usage
bytes = TripleStore.Config.RocksDB.estimate_memory_usage(config)
IO.puts("Estimated usage: #{bytes / 1_000_000} MB")
```

### Viewing Configuration

```elixir
config = TripleStore.Config.RocksDB.recommended()
IO.puts(TripleStore.Config.RocksDB.format_summary(config))
```

## Query Performance

### Query Timeouts

Set appropriate timeouts:

```elixir
# Default: 30 seconds
{:ok, results} = TripleStore.query(store, sparql)

# Custom timeout
{:ok, results} = TripleStore.query(store, sparql, timeout: 60_000)  # 60 seconds

# Short timeout for interactive queries
{:ok, results} = TripleStore.query(store, sparql, timeout: 5_000)   # 5 seconds
```

### Query Patterns

**Most Efficient (use indices well):**

```elixir
# Fully bound - point lookup
"<http://example.org/alice> <http://example.org/knows> ?who"

# Subject + predicate bound - prefix scan
"?person foaf:name ?name"

# Predicate bound - good for common predicates
"?s rdf:type foaf:Person"
```

**Less Efficient (require more scanning):**

```elixir
# Only object bound - requires OSP index scan
"?s ?p <http://example.org/value>"

# Fully unbound - full table scan
"?s ?p ?o"
```

### LIMIT for Development

Always use LIMIT during development:

```elixir
# While developing queries
"""
SELECT * WHERE { ?s ?p ?o } LIMIT 100
"""

# Remove or increase LIMIT for production
"""
SELECT * WHERE { ?s ?p ?o }
"""
```

### Explain Queries

Understand query plans:

```elixir
{:ok, plan} = TripleStore.query(store, sparql, explain: true)
IO.inspect(plan, pretty: true)
```

## Loading Performance

### Batch Size Tuning

```elixir
# Small files (< 10K triples)
{:ok, _} = TripleStore.load(store, "small.ttl", batch_size: 1_000)

# Medium files (10K - 1M triples)
{:ok, _} = TripleStore.load(store, "medium.nt", batch_size: 10_000)

# Large files (> 1M triples)
{:ok, _} = TripleStore.load(store, "large.nt", batch_size: 50_000)
```

### Format Selection

For bulk loading, prefer N-Triples:

| Format | Parse Speed | Memory Usage |
|--------|-------------|--------------|
| N-Triples | Fastest | Lowest (streaming) |
| Turtle | Medium | Higher (parser state) |
| RDF/XML | Slowest | Highest |

```elixir
# Convert to N-Triples for fastest loading
# (using external tools like rapper, riot)
# $ riot --output=ntriples data.ttl > data.nt

{:ok, count} = TripleStore.load(store, "data.nt")
```

### Parallel Loading (Multiple Files)

Load multiple files concurrently:

```elixir
files = ["file1.nt", "file2.nt", "file3.nt"]

results = files
|> Task.async_stream(fn file ->
  TripleStore.load(store, file, batch_size: 10_000)
end, max_concurrency: 4, timeout: :infinity)
|> Enum.map(fn {:ok, result} -> result end)

total = results |> Enum.map(fn {:ok, count} -> count end) |> Enum.sum()
IO.puts("Loaded #{total} triples")
```

## Reasoning Performance

### Profile Selection

Choose the minimal profile for your needs:

```elixir
# Only need class/property hierarchies
{:ok, _} = TripleStore.materialize(store, profile: :rdfs)

# Need OWL features
{:ok, _} = TripleStore.materialize(store, profile: :owl2rl)
```

### Monitoring Materialization

```elixir
{:ok, stats} = TripleStore.materialize(store)

IO.puts("""
Materialization complete:
  Iterations: #{stats.iterations}
  Derived: #{stats.total_derived}
  Duration: #{stats.duration_ms}ms
  Rate: #{stats.total_derived / (stats.duration_ms / 1000)} triples/sec
""")
```

### Materialization Guidelines

| Dataset Size | Expected Duration | Iterations |
|--------------|-------------------|------------|
| < 10K triples | < 1 second | 2-5 |
| 10K - 100K | 1-10 seconds | 3-8 |
| 100K - 1M | 10-60 seconds | 5-12 |
| > 1M | > 1 minute | 8-20 |

If materialization is slow:
- Reduce schema complexity
- Use simpler profile (:rdfs)
- Check for pathological patterns (deep chains, many sameAs)

## Memory Management

### Monitoring Memory

```elixir
# Get statistics
{:ok, stats} = TripleStore.stats(store)
{:ok, health} = TripleStore.health(store)

IO.puts("Triple count: #{stats.triple_count}")
IO.puts("Health: #{health.status}")
```

### Large Dataset Tips

1. **Use streaming formats** (N-Triples) for loading
2. **Increase batch size** for bulk loading
3. **Avoid holding large result sets** in memory
4. **Use LIMIT and pagination** for queries
5. **Consider server with more RAM** for very large stores

### Pagination for Large Results

```elixir
defmodule Paginator do
  def all_results(store, query, page_size \\ 1000) do
    Stream.resource(
      fn -> 0 end,
      fn offset ->
        paged_query = "#{query} LIMIT #{page_size} OFFSET #{offset}"
        case TripleStore.query(store, paged_query) do
          {:ok, []} -> {:halt, offset}
          {:ok, results} -> {results, offset + page_size}
        end
      end,
      fn _ -> :ok end
    )
  end
end

# Use streaming
Paginator.all_results(store, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
|> Stream.each(&process_row/1)
|> Stream.run()
```

## Backup Configuration

### Scheduled Backup Options

```elixir
# Production: hourly backups, keep 24
{:ok, scheduler} = TripleStore.schedule_backup(store, "/backups/prod",
  interval: :timer.hours(1),
  max_backups: 24
)

# Development: daily backups, keep 7
{:ok, scheduler} = TripleStore.schedule_backup(store, "/backups/dev",
  interval: :timer.hours(24),
  max_backups: 7
)

# Critical: every 15 minutes, keep 96 (24 hours)
{:ok, scheduler} = TripleStore.schedule_backup(store, "/backups/critical",
  interval: :timer.minutes(15),
  max_backups: 96
)
```

## Monitoring

### Health Checks

Regular health monitoring:

```elixir
defmodule HealthChecker do
  use GenServer

  def start_link(store) do
    GenServer.start_link(__MODULE__, store, name: __MODULE__)
  end

  def init(store) do
    schedule_check()
    {:ok, %{store: store}}
  end

  def handle_info(:check, %{store: store} = state) do
    case TripleStore.health(store) do
      {:ok, %{status: :healthy}} ->
        :ok

      {:ok, %{status: status}} ->
        Logger.warn("Store health: #{status}")

      {:error, reason} ->
        Logger.error("Health check failed: #{inspect(reason)}")
    end

    schedule_check()
    {:noreply, state}
  end

  defp schedule_check do
    Process.send_after(self(), :check, :timer.seconds(30))
  end
end
```

### Telemetry Integration

Attach to telemetry events:

```elixir
defmodule MetricsHandler do
  def setup do
    :telemetry.attach_many("metrics-handler", [
      [:triple_store, :query, :execute, :stop],
      [:triple_store, :insert, :stop],
      [:triple_store, :load, :stop]
    ], &handle_event/4, nil)
  end

  def handle_event([:triple_store, :query, :execute, :stop], measurements, _metadata, _config) do
    duration_ms = measurements[:duration_ms] || 0
    StatsD.histogram("triplestore.query.duration", duration_ms)

    if duration_ms > 1000 do
      Logger.warn("Slow query: #{duration_ms}ms")
    end
  end

  def handle_event([:triple_store, :insert, :stop], _measurements, metadata, _config) do
    StatsD.increment("triplestore.insert", metadata[:count] || 1)
  end

  def handle_event([:triple_store, :load, :stop], _measurements, metadata, _config) do
    StatsD.increment("triplestore.load", metadata[:total_count] || 0)
  end
end
```

### Metrics Collection

Use the built-in metrics collector:

```elixir
# Start the metrics collector
{:ok, _} = TripleStore.Metrics.start_link()

# Get all metrics
metrics = TripleStore.Metrics.get_all()

IO.puts("""
Query Metrics:
  Count: #{metrics.query.count}
  Mean: #{metrics.query.mean_duration_ms}ms
  P99: #{metrics.query.percentiles.p99}ms

Cache Metrics:
  Hit Rate: #{Float.round(metrics.cache.hit_rate * 100, 1)}%

Throughput:
  Insert Rate: #{metrics.throughput.insert_rate_per_sec}/sec
""")
```

## Production Checklist

Before going to production:

- [ ] Configure appropriate RocksDB memory settings
- [ ] Set up scheduled backups
- [ ] Test backup and restore procedures
- [ ] Configure monitoring and alerting
- [ ] Set query timeouts appropriate for your use case
- [ ] Test with production-scale data
- [ ] Monitor health during initial deployment
- [ ] Plan for materialization time after data updates

## Troubleshooting

### Slow Queries

1. Check query plan: `TripleStore.query(store, sparql, explain: true)`
2. Add more selective patterns first
3. Use LIMIT during development
4. Check if statistics need refresh

### Memory Issues

1. Reduce batch size for loading
2. Use streaming/pagination for large queries
3. Check RocksDB memory configuration
4. Monitor with `:erlang.memory()`

### Slow Materialization

1. Use simpler reasoning profile
2. Check for circular dependencies in schema
3. Reduce use of transitive properties over large chains
4. Consider incremental updates instead of full rematerialization

## Next Steps

This concludes the user guide series. For implementation details, see the [Developer Guides](../developer/README.md).
