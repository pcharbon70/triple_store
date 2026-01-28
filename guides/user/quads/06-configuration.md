# Configuration & Performance (Quad Store)

This guide covers configuring and tuning TripleStore for optimal performance with a quad store.

> **Note**: This guide is for quad stores (`schema: :quad`) with named graphs. For triple stores, see the [Triple Store Configuration & Performance](../triples/06-configuration.md) guide.

## RocksDB Configuration

Quad stores have different performance characteristics and memory requirements compared to triple stores.

### Quad Store vs Triple Store

| Metric | Triple Store | Quad Store | Change |
|--------|--------------|------------|--------|
| **Key Size** | 24 bytes | 32 bytes | +33% |
| **Indices** | 3 (SPO, POS, OSP) | 4 (GSPO, GPOS, SPOG, POSG) | +33% |
| **Write Amplification** | 3x | 4x | +33% |
| **Block Size** | 8 KB | 16 KB | +100% |
| **Memtable** | 64 MB | 128 MB | +100% |
| **Bloom Filter** | 12 bits/key | 10 bits/key | -17% |

### Memory Budget for Quad Stores

Quad stores require approximately 1.5-2x more memory for the same data size:

```elixir
# For quad stores, allocate more memory
config = TripleStore.Config.RocksDB.for_memory_budget(
  16 * 1024 * 1024 * 1024  # 16 GB recommended for quad (vs 8 GB for triple)
)

# Or use quad-specific presets
config = TripleStore.Config.RocksDB.preset(:quad_production)
config = TripleStore.Config.RocksDB.preset(:quad_write_heavy)
```

### Quad Store Presets

| Preset | Block Cache | Write Buffer | Best For |
|--------|-------------|--------------|----------|
| `quad_development` | 256 MB | 64 MB × 2 | Local development with named graphs |
| `quad_production` | 8 GB | 256 MB × 4 | Production quad stores |
| `quad_write_heavy` | 2 GB | 512 MB × 4 | Bulk loading quads |

### Opening with Configuration

```elixir
# Open with custom config
config = TripleStore.Config.RocksDB.preset(:quad_production)

{:ok, store} = TripleStore.open("./my_database",
  schema: :quad,
  config: config
)
```

## Query Performance

### Query Timeouts

```elixir
# Default: 30 seconds
{:ok, results} = TripleStore.query(store, sparql)

# Custom timeout
{:ok, results} = TripleStore.query(store, sparql, timeout: 60_000)

# Short timeout for interactive queries
{:ok, results} = TripleStore.query(store, sparql, timeout: 5_000)
```

### Query Patterns with Graphs

**Most Efficient (graph-specified):**

```elixir
# Query specific graph - uses GSPO/GPOS indices
"""
SELECT ?s ?p ?o
WHERE {
  GRAPH ex:people { ?s ?p ?o }
}
"""
```

**Less Efficient (graph variable):**

```elixir
# All graphs - scans all graphs
"""
SELECT ?g ?s ?p ?o
WHERE {
  GRAPH ?g { ?s ?p ?o }
}
"""
```

### Graph-Specific Optimization

When querying a specific graph, the query optimizer can use the GSPO and GPOS indices:

```elixir
# More efficient - specifies graph
"""
SELECT ?s ?p ?o
WHERE {
  GRAPH ex:people {
    ?s ?p ?o
  }
}
"""

# Less efficient - filters after scan
"""
SELECT ?s ?p ?o
WHERE {
  ?s ?p ?o
  FILTER (?g = ex:people)
}
"""
```

## Loading Performance

### Batch Size Tuning

```elixir
# Small files (< 10K quads)
{:ok, _} = TripleStore.load(store, "small.nq", batch_size: 1_000)

# Medium files (10K - 1M quads)
{:ok, _} = TripleStore.load(store, "medium.nq", batch_size: 10_000)

# Large files (> 1M quads)
{:ok, _} = TripleStore.load(store, "large.nq", batch_size: 50_000)
```

### Format Selection

For quad stores, prefer N-Quads or TriG:

| Format | Parse Speed | Memory Usage | Graph Support |
|--------|-------------|--------------|---------------|
| N-Quads | Fastest | Lowest (streaming) | Full quad support |
| TriG | Medium | Higher | Full quad support |
| Turtle | Medium | Higher | Default graph only |
| N-Triples | Fastest | Lowest | Default graph only |

```elixir
# N-Quads for streaming with graph context
{:ok, count} = TripleStore.load(store, "data.nq")

# TriG for human-readable named graphs
{:ok, count} = TripleStore.load(store, "data.trig")
```

### Parallel Loading (Multiple Files)

```elixir
files = ["file1.nq", "file2.nq", "file3.nq"]

results = files
|> Task.async_stream(fn file ->
  TripleStore.load(store, file, batch_size: 10_000)
end, max_concurrency: 4, timeout: :infinity)
|> Enum.map(fn {:ok, result} -> result end)

total = results |> Enum.map(fn {:ok, count} -> count end) |> Enum.sum()
IO.puts("Loaded #{total} quads")
```

## Reasoning Performance

### Profile Selection

```elixir
# Only need class/property hierarchies
{:ok, _} = TripleStore.materialize(store, profile: :rdfs)

# Need OWL features
{:ok, _} = TripleStore.materialize(store, profile: :owl2rl)
```

### Scope Selection

```elixir
# Local reasoning - faster for multi-tenant data
config = ReasoningConfig.new(profile: :owl2rl, scope: :local)
{:ok, stats} = TripleStore.materialize(store, config: config)

# Global reasoning - needed for shared schema
config = ReasoningConfig.new(profile: :owl2rl, scope: :global)
{:ok, stats} = TripleStore.materialize(store, config: config)
```

### Monitoring Materialization

```elixir
{:ok, stats} = TripleStore.materialize(store)

IO.puts("""
Materialization complete:
  Iterations: #{stats.iterations}
  Derived: #{stats.total_derived}
  Duration: #{stats.duration_ms}ms
  Rate: #{stats.total_derived / (stats.duration_ms / 1000)} quads/sec
""")
```

## Memory Management

### Monitoring Memory

```elixir
# Get statistics
{:ok, stats} = TripleStore.stats(store)
{:ok, health} = TripleStore.health(store)

IO.puts("Quad count: #{stats.triple_count}")
IO.puts("Health: #{health.status}")
```

### Large Dataset Tips

1. **Use streaming formats** (N-Quads) for loading
2. **Increase batch size** for bulk loading
3. **Avoid holding large result sets** in memory
4. **Use LIMIT and pagination** for queries
5. **Allocate more memory** for quad stores (1.5-2x triple store)

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
Paginator.all_results(store, "SELECT ?g ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o } }")
|> Stream.each(&process_row/1)
|> Stream.run()
```

## Graph-Specific Configuration

### Per-Graph Statistics

```elixir
# Get statistics for a specific graph
{:ok, stats} = TripleStore.Statistics.graph_statistics(store, graph_id)

IO.puts("Graph #{graph_id}:")
IO.puts("  Quad count: #{stats.quad_count}")
IO.puts("  Unique subjects: #{stats.unique_subjects}")
IO.puts("  Unique predicates: #{stats.unique_predicates}")
```

### Graph Enumeration

```elixir
# List all graphs
{:ok, graphs} = TripleStore.list_graphs(store)

# Get statistics for each graph
Enum.each(graphs, fn graph_id ->
  {:ok, stats} = TripleStore.Statistics.graph_statistics(store, graph_id)
  IO.puts("Graph #{graph_id}: #{stats.quad_count} quads")
end)
```

## Backup Configuration

### Scheduled Backup Options

```elixir
# Production: hourly backups, keep 24
{:ok, scheduler} = TripleStore.schedule_backup(store, "/backups/prod",
  interval: :timer.hours(1),
  max_backups: 24
)

# For quad stores, also consider per-graph backups
{:ok, graphs} = TripleStore.list_graphs(store)
Enum.each(graphs, fn graph_id ->
  TripleStore.GraphBackup.schedule_backup(store, graph_id,
    interval: :timer.hours(24),
    max_backups: 7
  )
end)
```

## Monitoring

### Health Checks

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
end
```

## Performance Tuning

### Quad Store Optimization Tips

```elixir
# 1. Increase block cache for quad stores
config = TripleStore.Config.RocksDB.recommended()
config = %{config | block_cache_size_mb: 8192}  # 8 GB for quad

# 2. Increase memtable for better write performance
config = %{config | write_buffer_size_mb: 128}  # 128 MB per column family

# 3. Use larger bloom filters for quad keys
config = %{config | bloom_bits_per_key: 10}  # Slightly lower for quad
```

### Graph-Specific Optimization

```elixir
# When querying specific graphs, use GRAPH clause
# (more efficient than FILTER on graph variable)
query = """
SELECT ?s ?p ?o
WHERE {
  GRAPH ex:target_graph {
    ?s ?p ?o
  }
}
"""
```

## Production Checklist

Before going to production with a quad store:

- [ ] Allocate 1.5-2x memory compared to triple store
- [ ] Use quad-specific RocksDB presets
- [ ] Configure appropriate block cache size (8GB+ for production)
- [ ] Set up scheduled backups (full store + per-graph)
- [ ] Test backup and restore procedures
- [ ] Configure monitoring and alerting
- [ ] Set query timeouts appropriate for your use case
- [ ] Test with production-scale data
- [ ] Monitor health during initial deployment
- [ ] Plan for materialization time (longer than triple store)

## Troubleshooting

### Slow Queries

1. Check query plan: `TripleStore.query(store, sparql, explain: true)`
2. Use specific GRAPH clauses instead of graph variables
3. Use LIMIT during development
4. Check if statistics need refresh

### Memory Issues

1. Increase block cache size (quad stores need more)
2. Reduce batch size for loading
3. Use streaming/pagination for large queries
4. Monitor with `:erlang.memory()`

### Slow Materialization

1. Use simpler reasoning profile (:rdfs)
2. Use local scope for multi-tenant data
3. Check for circular dependencies in schema
4. Reduce use of transitive properties over large chains

## Next Steps

- [Named Graphs](07-named-graphs.md) - Advanced named graph patterns

This concludes the quad store guide series. For implementation details, see the [Developer Guides](../../developer/README.md).
