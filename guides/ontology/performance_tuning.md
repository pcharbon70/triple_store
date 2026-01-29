# Performance Tuning Guide

This guide covers performance optimization strategies for TripleStore, from RocksDB configuration to query optimization.

## Memory Configuration

### Block Cache Size

The block cache is the primary memory consumer. Size it based on available RAM:

```elixir
# In config/config.exs
config :triple_store, :rocksdb,
  block_cache_size: 2 * 1024 * 1024 * 1024  # 2GB
```

**Guidelines:**
- Development: 256MB - 512MB
- Production: 40% of available RAM
- Read-heavy workloads benefit from larger caches

### Write Buffer Size

Controls memory used for buffering writes before flushing to disk:

```elixir
config :triple_store, :rocksdb,
  write_buffer_size: 64 * 1024 * 1024,    # 64MB per buffer
  max_write_buffer_number: 4               # Number of buffers
```

**Guidelines:**
- Larger buffers improve bulk loading performance
- More buffers allow concurrent writes during flush
- Total write memory = write_buffer_size × max_write_buffer_number

## Compression Settings

TripleStore uses tiered compression by default:

```elixir
config :triple_store, :rocksdb,
  # Level 0-1: No compression (hot data)
  # Level 2+: LZ4 compression (good speed/ratio)
  # Level 6+: Zstd compression (archival data)
  compression_per_level: [:none, :none, :lz4, :lz4, :lz4, :zstd, :zstd]
```

**Trade-offs:**
- LZ4: Fast compression, moderate ratio (~2-3x)
- Zstd: Slower compression, better ratio (~3-5x)
- No compression: Fastest but largest storage

## Bulk Loading Optimization

Bulk loading large datasets (>100K triples) can be optimized with the following approaches.

### Open Database for Bulk Load

Use the `open_for_bulk_load/2` function when preparing for a large import:

```elixir
alias TripleStore.Backend.RocksDB.ErlangAdapter

# Open database optimized for bulk loading
{:ok, adapter} = ErlangAdapter.open_for_bulk_load("/path/to/db")
```

**Note:** The erlang-rocksdb library doesn't support runtime configuration changes.
This function documents intent and provides forward compatibility. Actual optimization
comes from the practices below.

### Batch Size

Larger batches reduce per-triple overhead:

```elixir
{:ok, count} = TripleStore.load(store, "large_file.ttl",
  batch_size: 10_000  # Default: 1000
)
```

**Guidelines:**
- Small files: 1,000 (default)
- Large files (>100K triples): 10,000
- Very large files (>1M triples): 50,000

### Disable WAL for Initial Bulk Load

For initial bulk loading into a new database, consider disabling the Write-Ahead Log:

```elixir
# Open without WAL for initial bulk load (use with caution)
# Data may be lost on crash during load
{:ok, adapter} = ErlangAdapter.open(path, disable_wal: true)

# Load data...

# Re-open with WAL enabled for normal operation
:ok = ErlangAdapter.close(adapter)
{:ok, adapter} = ErlangAdapter.open(path)
```

**Warning:** Only disable WAL for initial bulk loads into new databases.
Never disable WAL for databases with important data.

### Parallel Loading with Flow

For multiple files, use Flow for concurrent loading:

```elixir
use Flow

paths = ["file1.nt", "file2.nt", "file3.nt"]

paths
|> Flow.from_enumerable()
|> Flow.map(fn path ->
  Task.async(fn ->
    TripleStore.load(store, path, batch_size: 50_000)
  end)
end)
|> Flow.await()  # Wait for all loads to complete
```

### Manual Compaction After Load

After bulk loading completes, the database will automatically compact in the background.
For large loads, consider allowing extra time for compaction before putting the database
into production use.

The erlang-rocksdb library handles background compaction automatically based on
the configured options.

## Query Optimization

### Query Cache

Enable query result caching for repeated queries:

```elixir
# Start the query cache
{:ok, _} = TripleStore.Query.Cache.start_link(
  max_entries: 10_000,        # Maximum cached queries
  max_result_size: 10_000,    # Skip caching large results
  max_memory_bytes: 100_000_000  # 100MB memory limit
)
```

### Query Timeout

Set appropriate timeouts to prevent runaway queries:

```elixir
{:ok, results} = TripleStore.query(store, sparql,
  timeout: 30_000  # 30 seconds (default)
)
```

### Index Selection

The query optimizer automatically selects optimal indices:

| Pattern | Index Used | Performance |
|---------|-----------|-------------|
| S P O   | SPO       | O(1) lookup |
| S P ?   | SPO       | O(k) prefix scan |
| S ? ?   | SPO       | O(k) prefix scan |
| ? P O   | POS       | O(k) prefix scan |
| ? P ?   | POS       | O(k) prefix scan |
| ? ? O   | OSP       | O(k) prefix scan |
| ? ? ?   | SPO       | O(n) full scan |

Where k is the number of matching triples and n is total triples.

### Complex Join Optimization

For queries with 4+ triple patterns, the Leapfrog Triejoin algorithm activates:

```sparql
# Automatically uses Leapfrog for efficient joins
SELECT ?a ?b ?c ?d
WHERE {
  ?a :knows ?b .
  ?b :knows ?c .
  ?c :knows ?d .
  ?d :knows ?a .
}
```

## Reasoning Performance

### Incremental Materialization

For large ontologies, materialize after loading the TBox (schema):

```elixir
# Load ontology first
{:ok, _} = TripleStore.load(store, "ontology.ttl")
{:ok, _} = TripleStore.materialize(store, profile: :owl2rl)

# Then load instance data - inferences computed incrementally
{:ok, _} = TripleStore.load(store, "data.ttl")
```

### Reasoning Profile Selection

Choose the minimal profile needed:

```elixir
# RDFS only - fastest
{:ok, _} = TripleStore.materialize(store, profile: :rdfs)

# OWL 2 RL - more rules, more inferences
{:ok, _} = TripleStore.materialize(store, profile: :owl2rl)
```

## Compaction Tuning

### Compaction Rate Limiting

Limit I/O impact from background compaction:

```elixir
config :triple_store, :rocksdb,
  rate_limiter_bytes_per_sec: 100 * 1024 * 1024  # 100MB/s
```

### Manual Compaction

Trigger compaction during maintenance windows:

```elixir
# Full compaction - use sparingly
TripleStore.Backend.RocksDB.compact(store.db)
```

## Monitoring Performance

### Telemetry Integration

Attach handlers to monitor performance:

```elixir
:telemetry.attach_many(
  "triple-store-metrics",
  [
    [:triple_store, :query, :stop],
    [:triple_store, :insert, :stop],
    [:triple_store, :loader, :stop]
  ],
  fn event, measurements, metadata, _config ->
    Logger.info("#{inspect(event)}: #{measurements.duration / 1_000_000}ms")
  end,
  nil
)
```

### Prometheus Metrics

Enable Prometheus metrics export:

```elixir
{:ok, _} = TripleStore.Prometheus.start_link(handler_prefix: :prod)

# Get metrics in Prometheus format
metrics = TripleStore.Prometheus.format()
```

### Health Monitoring

Regular health checks identify issues early:

```elixir
{:ok, health} = TripleStore.health(store)
case health.status do
  :healthy -> :ok
  :degraded -> Logger.warn("Store degraded: #{inspect(health)}")
  :unhealthy -> Logger.error("Store unhealthy: #{inspect(health)}")
end
```

## Hardware Recommendations

### Storage

- **SSD required** for production workloads
- NVMe preferred for high-throughput scenarios
- RAID-10 for durability with performance

### Memory

| Dataset Size | Recommended RAM |
|-------------|-----------------|
| < 1M triples | 4GB |
| 1-10M triples | 8-16GB |
| 10-100M triples | 32-64GB |
| > 100M triples | 128GB+ |

### CPU

- More cores help parallel query execution
- Higher clock speed helps single-query latency
- Modern CPUs with good IPC recommended

## Benchmarking

Use the built-in WatDiv benchmark to measure performance:

```bash
# Run the main WatDiv benchmark
mix run scripts/run_benchmarks.exs
```

The benchmark generates test data, loads it, and runs 20 queries across 4 categories (linear, star, snowflake, complex).

### Programmatic Benchmarking

```elixir
alias TripleStore.Benchmark.{WatDiv, WatDivQueries}

# Generate data at scale 10 (~1M triples)
graph = WatDiv.generate(10)

# Run specific query category
{:ok, query} = WatDivQueries.get(:l1)
{:ok, results} = TripleStore.query(store, query.sparql)
```

### Performance Targets

| Metric | Target |
|--------|--------|
| Simple query (L1-L5) | < 10ms p95 |
| Complex query (F1-F5, C1-C3) | < 100ms p95 |
| Query mix aggregate | < 50ms p95 |
| Bulk load | > 100K triples/sec |
| Point lookup | < 1ms p99 |

See [Performance Targets](../benchmarks/performance-targets.md) for detailed WatDiv benchmark documentation.

## Common Performance Issues

### Slow Queries

1. Check query plan with `explain: true` option
2. Ensure indices are being used appropriately
3. Add LIMIT clauses where possible
4. Consider query caching for repeated queries

### High Memory Usage

1. Reduce block_cache_size
2. Lower max_write_buffer_number
3. Enable more aggressive compression
4. Check for memory leaks in query results

### Slow Bulk Loading

1. Increase batch_size
2. Temporarily disable WAL
3. Disable compression during load
4. Use parallel loading with Flow (for multiple files)

### Reasoning Takes Too Long

1. Use minimal reasoning profile needed
2. Pre-materialize before adding instance data
3. Consider hybrid reasoning (materialize + query-time)
4. Profile rule application to find bottlenecks
