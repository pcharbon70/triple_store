# Performance Targets

> **Last Updated:** 2026-01-28

## Overview

This document defines measurable performance targets for the TripleStore using the WatDiv (Waterloo SPARQL Diversity Test) benchmark suite.

## Running Benchmarks

```bash
# Run the main WatDiv benchmark
mix run scripts/run_benchmarks.exs
```

The benchmark script will:
1. Generate WatDiv test data (scale 1 = ~100K triples)
2. Open a test database
3. Load the data and measure throughput
4. Run all 20 WatDiv queries with warmup
5. Report p50, p95, p99 latencies and result counts

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Bulk Load | > 100K triples/sec | WatDiv data generation + load |
| Simple Query | p95 < 10ms | Linear queries (L1-L5) |
| Complex Query | p95 < 100ms | Snowflake/Complex queries (F1-F5, C1-C3) |
| Query Mix | p95 < 50ms | All queries aggregate |

## WatDiv Query Categories

### Linear Queries (L1-L5)

Single-path queries following linear chains through the graph.

| Query | Description | Target |
|-------|-------------|--------|
| L1 | User likes content with caption | p95 < 10ms |
| L2 | Users who like a product with nationality | p95 < 10ms |
| L3 | User likes and subscribes | p95 < 10ms |
| L4 | Content tagged with topic | p95 < 10ms |
| L5 | Person with job title and nationality | p95 < 10ms |

### Star Queries (S1-S7)

Queries centered on a single entity with many relationships.

| Query | Description | Target |
|-------|-------------|--------|
| S1 | Offer with all properties | p95 < 50ms |
| S2 | User by location, nationality, gender, role | p95 < 50ms |
| S3 | Product by type with caption, genre, publisher | p95 < 50ms |
| S4 | Person by age with name and artist connection | p95 < 50ms |
| S5 | Product by type with description, keywords, language | p95 < 50ms |
| S6 | Musical work with conductor and genre | p95 < 50ms |
| S7 | Product liked by user | p95 < 10ms |

### Snowflake Queries (F1-F5)

Branching patterns from multiple entities.

| Query | Description | Target |
|-------|-------------|--------|
| F1 | Movie with genre tagged with topic | p95 < 100ms |
| F2 | Product with homepage and genre | p95 < 100ms |
| F3 | Product purchase by genre | p95 < 100ms |
| F4 | Product with offer, likes, and language | p95 < 100ms |
| F5 | Offer with product title and type | p95 < 100ms |

### Complex Queries (C1-C3)

Multi-feature queries combining several patterns.

| Query | Description | Target |
|-------|-------------|--------|
| C1 | Review with actor and language | p95 < 100ms |
| C2 | Purchase flow with offers and reviews | p95 < 100ms |
| C3 | User with all profile attributes | p95 < 50ms |

## Benchmark Scales

| Scale | Triples | Use Case |
|-------|---------|----------|
| 1 | ~100K | Quick validation, CI/CD |
| 10 | ~1M | Standard performance testing |
| 100 | ~10M | Large-scale validation |

To run at a different scale, modify the scale parameter in `scripts/run_benchmarks.exs`:

```elixir
# Generate WatDiv data at scale 10 (~1M triples)
{time_us, graph} = :timer.tc(fn -> WatDiv.generate(10) end)
```

## Interpreting Results

The benchmark output provides:

```
>>> WatDiv Benchmark (Scale 1, ~100K triples) <<<

Generated 38947 triples in 45.23ms
Opening database at /tmp/watdiv_bench_12345...
Loading data...
  Loaded in 1234.56ms (31552 triples/sec)

Running WatDiv queries (warmup: 2, iterations: 5)...

  L1: p50=2.3ms, p95=2.8ms, results=42
  L2: p50=1.9ms, p95=2.1ms, results=15
  ...
  S1: p50=8.5ms, p95=12.3ms, results=8
  ...

WatDiv Summary:
  Load throughput: 31552 triples/sec
  Average p50: 5.4ms
  Average p95: 8.2ms
  Max p95: 45.1ms

  By Category:
    LINEAR: avg p50=2.1ms, avg p95=2.8ms
    STAR: avg p50=8.2ms, avg p95=11.5ms
    SNOWFLAKE: avg p50=12.4ms, avg p95=18.2ms
    COMPLEX: avg p50=15.8ms, avg p95=22.1ms
```

### Key Metrics

- **Load throughput**: Higher is better (triples/second)
- **p50 latency**: Median query time
- **p95 latency**: 95th percentile (main target metric)
- **p99 latency**: 99th percentile (tail latency)
- **Result count**: Verify queries return expected data

## Programmatic Validation

The `TripleStore.Benchmark.Targets` module provides validation functions:

```elixir
alias TripleStore.Benchmark.Targets

# Check bulk load performance
{:ok, report} = Targets.validate_bulk_load(100_000, 1000)
# 100K triples loaded in 1000ms = 100K tps
Targets.print_report(report)

# Check simple query performance
case Targets.check_simple_bgp(p95_us: 5000) do
  :pass -> IO.puts("Simple queries meet target")
  {:fail, reason} -> IO.puts("Failed: #{reason}")
end
```

## WatDiv in Research

WatDiv is the current standard for RDF store benchmarking in academic research. Unlike older benchmarks (BSBM, LUBM), WatDiv:

- **Heterogeneous structure**: Same entity types don't always have the same attributes
- **Probabilistic attributes**: Properties appear with specific probabilities, creating realistic data skew
- **Correlated attributes**: The `pgroup` construct creates realistic attribute correlations
- **Diverse query patterns**: 20 queries covering 4 structural categories

This makes WatDiv results more representative of real-world RDF workloads and more effective at exposing optimizer weaknesses.

When publishing performance results, always report:
1. Scale factor (triples generated)
2. Hardware configuration (CPU, RAM, storage)
3. Per-query latencies (p50, p95, p99)
4. Category averages
5. Load throughput

## Additional Benchmarks

### Statistics & Cardinality Benchmarks

Tests for statistics collection and cardinality estimation:

```bash
mix test test/triple_store/benchmark/phase_5_benchmark_test.exs
```

### Quad Store Benchmarks

Tests for quad-specific operations (named graphs, N-Quads/TriG loading):

```bash
mix test test/triple_store/integration/quad_benchmark_test.exs --tag benchmark
```

### Reasoning Benchmarks

Tests for OWL 2 RL reasoning performance:

```bash
mix test test/triple_store/reasoner/reasoning_benchmark_test.exs
```

## References

- [WatDiv Benchmark Module](../../lib/triple_store/benchmark/watdiv.ex)
- [WatDiv Queries Module](../../lib/triple_store/benchmark/watdiv_queries.ex)
- [Benchmark Runner Script](../../scripts/run_benchmarks.exs)
- [Performance Tuning Guide](../ontology/performance_tuning.md)
