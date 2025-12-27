# Task 5.1.1: Benchmark Data Generators

**Date:** 2025-12-27
**Branch:** `feature/task-5.1.1-micro-benchmarks`
**Status:** Complete

## Overview

Implemented data generators for standard RDF benchmarks (LUBM and BSBM). These generators create synthetic datasets for performance testing and benchmarking of the triple store.

## Implementation Details

### Files Created

1. **`lib/triple_store/benchmark/lubm.ex`** - LUBM Data Generator
   - Generates synthetic university data following the LUBM ontology
   - Entities: Universities, Departments, Faculty, Students, Courses, Publications
   - Configurable scale factor (1 university ≈ 80K triples)
   - Supports both graph and stream output modes
   - Deterministic generation with seed support

2. **`lib/triple_store/benchmark/bsbm.ex`** - BSBM Data Generator
   - Generates synthetic e-commerce data following the BSBM vocabulary
   - Entities: Products, Producers, Vendors, Offers, Reviews
   - Configurable product count (100 products ≈ 50K triples)
   - Supports both graph and stream output modes
   - Deterministic generation with seed support

3. **`test/triple_store/benchmark/lubm_test.exs`** - LUBM Tests (10 tests)
4. **`test/triple_store/benchmark/bsbm_test.exs`** - BSBM Tests (11 tests)

### Features Implemented

#### LUBM Generator (5.1.1.1)
- `LUBM.generate(scale, opts)` - Generate complete RDF.Graph
- `LUBM.stream(scale, opts)` - Generate as lazy stream
- `LUBM.estimate_triple_count(scale)` - Estimate output size
- `LUBM.namespace()` - Returns ontology namespace

#### BSBM Generator (5.1.1.2)
- `BSBM.generate(num_products, opts)` - Generate complete RDF.Graph
- `BSBM.stream(num_products, opts)` - Generate as lazy stream
- `BSBM.estimate_triple_count(num_products)` - Estimate output size
- `BSBM.namespace()` - Returns vocabulary namespace

#### Deterministic Generation (5.1.1.3)
Both generators support a `:seed` option for reproducible benchmarks:
```elixir
# Same seed = same output
graph1 = LUBM.generate(1, seed: 12345)
graph2 = LUBM.generate(1, seed: 12345)
# graph1 and graph2 are identical
```

#### RDF.Graph Output (5.1.1.4)
All generators output standard RDF.Graph structures compatible with the loader:
```elixir
graph = LUBM.generate(1)
{:ok, count} = TripleStore.load_graph(store, graph)
```

### Test Results

```
21 tests, 0 failures
```

All tests tagged with `:benchmark` (excluded from normal test runs).

### API Examples

```elixir
# Generate LUBM data for 1 university (~80K triples)
graph = TripleStore.Benchmark.LUBM.generate(1)

# Generate BSBM data for 1000 products (~500K triples)
graph = TripleStore.Benchmark.BSBM.generate(1000)

# Generate with seed for reproducibility
graph = TripleStore.Benchmark.LUBM.generate(5, seed: 42)

# Stream for large datasets
stream = TripleStore.Benchmark.BSBM.stream(100_000)
Enum.each(stream, fn triple -> ... end)

# Estimate size before generating
estimate = TripleStore.Benchmark.LUBM.estimate_triple_count(10)
# => ~320,000
```

## Scale Factors

### LUBM
| Scale | Universities | Approx. Triples |
|-------|-------------|-----------------|
| 1     | 1           | ~80K           |
| 10    | 10          | ~800K          |
| 100   | 100         | ~8M            |

### BSBM
| Products | Approx. Triples |
|----------|-----------------|
| 100      | ~50K           |
| 1000     | ~500K          |
| 10000    | ~5M            |

## Dependencies

No new dependencies added. Uses existing `RDF` library for graph construction.
