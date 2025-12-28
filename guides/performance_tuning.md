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

### Disable WAL for Bulk Loads

For initial bulk loading, consider disabling the Write-Ahead Log:

```elixir
# Warning: Data may be lost on crash during load
config :triple_store, :rocksdb,
  disable_wal: true
```

Re-enable WAL after bulk loading completes.

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

Use the built-in benchmark suite to measure performance:

```elixir
# LUBM benchmark
{:ok, results} = TripleStore.Benchmark.run(store, :lubm,
  scale: 1,           # Number of universities
  warmup: 3,          # Warmup iterations
  iterations: 10      # Measurement iterations
)

# BSBM benchmark
{:ok, results} = TripleStore.Benchmark.run(store, :bsbm,
  scale: 1000,        # Number of products
  query_mix: true     # Run full query mix
)
```

### Performance Targets

| Metric | Target |
|--------|--------|
| Simple BGP query | < 10ms p95 |
| Complex join query | < 100ms p95 |
| Bulk load | > 100K triples/sec |
| Point lookup | < 1ms p99 |

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
