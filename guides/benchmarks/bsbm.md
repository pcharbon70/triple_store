# BSBM Benchmark

> **Last Run:** 2025-12-31
> **Dataset:** 1000 products (~141K triples)

## Overview

The Berlin SPARQL Benchmark (BSBM) simulates an e-commerce scenario with products, vendors, offers, and reviews. It's designed to test realistic business intelligence queries against RDF stores.

## Data Model

```mermaid
erDiagram
    Product ||--|| ProductType : "rdf:type"
    Product ||--|| Producer : "producer"
    Product ||--o{ ProductFeature : "productFeature"
    Product ||--o{ Offer : "product"
    Product ||--o{ Review : "reviewFor"
    Offer }o--|| Vendor : "vendor"
    Review }o--|| Person : "reviewer"
    Review }o--|| ReviewSite : "dc:publisher"
    Vendor ||--|| Country : "country"
    Producer ||--|| Country : "country"
```

### Scale Factors

| Products | Est. Triples | Description |
|----------|--------------|-------------|
| 100 | ~50K | Quick test |
| 1,000 | ~140K | Development |
| 10,000 | ~1.4M | Production test |
| 100,000 | ~14M | Stress test |

Each product has:
- 5-20 offers from different vendors
- 3-10 reviews from customers
- Multiple product features

## Queries

The BSBM benchmark includes 12 queries simulating e-commerce operations:

| Query | Description | Operation | Complexity |
|-------|-------------|-----------|------------|
| Q1 | Product type lookup with features | Search | Medium |
| Q2 | Product details for type | Search | Simple |
| Q3 | Product features filtered | Search | Medium |
| Q4 | Product features with UNION | Search | Complex |
| Q5 | Product by label | Search | Simple |
| Q6 | Product details page | Lookup | Simple |
| Q7 | Product with offers | Join | Medium |
| Q8 | Product reviews | Join | Medium |
| Q9 | Describe product | Describe | Simple |
| Q10 | Offers for product | Analytics | Medium |
| Q11 | Offers with conditions | Analytics | Complex |
| Q12 | Export product data | Export | Complex |

## Latest Results

### Configuration

- **Products:** 1,000
- **Triple Count:** 141,084
- **Warmup Iterations:** 2
- **Measurement Iterations:** 5

### Query Performance

| Query | p50 | p95 | p99 | Mean | Results | Status |
|-------|-----|-----|-----|------|---------|--------|
| Q1 | 9.2ms | 9.4ms | 9.4ms | 9.2ms | 10 | Pass |
| Q2 | 19.8ms | 19.9ms | 19.9ms | 19.8ms | 100 | Pass |
| Q3 | 9.0ms | 9.1ms | 9.1ms | 9.0ms | 10 | Pass |
| Q4 | 4.6ms | 4.7ms | 4.7ms | 4.6ms | 10 | Pass |
| Q5 | 0.02ms | 0.02ms | 0.02ms | 0.02ms | error | Fail |
| Q6 | 175.0ms | 177.1ms | 177.1ms | 175.4ms | 1 | Slow |
| Q7 | 1393.2ms | 1406.7ms | 1406.7ms | 1393.6ms | 20 | Very Slow |
| Q8 | 0.5ms | 0.5ms | 0.5ms | 0.5ms | 3 | Pass |
| Q9 | 0.2ms | 0.2ms | 0.2ms | 0.2ms | 13 | Pass |
| Q10 | 1.2ms | 1.2ms | 1.2ms | 1.2ms | 8 | Pass |
| Q11 | 0.03ms | 0.03ms | 0.03ms | 0.03ms | error | Fail |
| Q12 | 27.8ms | 70.4ms | 70.4ms | 36.3ms | 600 | Pass |

### Latency Distribution

```mermaid
xychart-beta
    title "BSBM Query Latency (p50)"
    x-axis [Q1, Q2, Q3, Q4, Q6, Q7, Q8, Q9, Q10, Q12]
    y-axis "Latency (ms)" 0 --> 200
    bar [9.2, 19.8, 9.0, 4.6, 175.0, 200, 0.5, 0.2, 1.2, 27.8]
```

*Note: Q7 (1393ms) exceeds chart scale; Q5 and Q11 failed.*

### Query Categories

```mermaid
pie title Query Performance Distribution
    "Fast (<10ms)" : 5
    "Medium (10-50ms)" : 3
    "Slow (50-200ms)" : 1
    "Very Slow (>200ms)" : 1
    "Failed" : 2
```

### Summary Statistics

| Metric | Value |
|--------|-------|
| Queries Executed | 12 |
| Queries with Results | 10 |
| Average p50 | 164.0ms |
| Average p95 | 169.9ms |
| Max p95 | 1406.7ms |

## Analysis

### Fast Queries (< 10ms)

- **Q1, Q3, Q4** (4.6-9.2ms): Product search with filters and features
- **Q8, Q9, Q10** (0.2-1.2ms): Simple lookups and small result sets

### Medium Queries (10-50ms)

- **Q2** (19.8ms): Product details retrieval (100 results)
- **Q12** (27.8ms): CONSTRUCT query exporting product data

### Slow Queries (> 50ms)

- **Q6** (175ms): Single product detail page - unexpectedly slow for a single result
- **Q7** (1393ms): Product offers join across all products with price filter

### Failed Queries

- **Q5**: Uses typed literal syntax (`"Product1"^^xsd:string`) that may not match the data
- **Q11**: Escaping issue with country URI fragment (`countries#US`)

### Performance Bottlenecks

Q7 is the primary bottleneck. It joins products with offers and filters by price:

```sparql
SELECT ?product ?offer ?price ?vendor
WHERE {
  ?product rdf:type bsbm:Product .
  ?offer bsbm:product ?product .
  ?offer bsbm:price ?price .
  ?offer bsbm:vendor ?vendor .
  FILTER (?price >= 50 && ?price <= 500)
}
ORDER BY ?price LIMIT 20
```

This query scans all products and their offers (~13K offers), making it O(n) in dataset size.

## Running the Benchmark

```elixir
# Generate data
graph = TripleStore.Benchmark.BSBM.generate(1000)

# Load into store
{:ok, store} = TripleStore.open("./tmp/bsbm_bench")
TripleStore.load(store, graph)

# Run benchmark
{:ok, results} = TripleStore.Benchmark.Runner.run(store, :bsbm,
  scale: 1000,
  warmup: 3,
  iterations: 10
)

# Print results
TripleStore.Benchmark.Runner.print_summary(results)
```

## Performance Targets

| Target | Metric | Threshold | Actual | Status |
|--------|--------|-----------|--------|--------|
| BSBM Mix | p95 latency | < 50ms | 169.9ms | Fail |

The BSBM mix target of <50ms p95 is not met, primarily due to Q6 and Q7.

## Optimization Opportunities

1. **Q7 Optimization**: Add an index on `bsbm:price` for faster range queries
2. **Q6 Investigation**: Single-result lookup should be sub-millisecond
3. **Query Planning**: Use cost-based optimizer to choose better join orders

## References

- [BSBM Website](http://wifo5-03.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/)
- [BSBM V3 Specification](http://wifo5-03.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/spec/BenchmarkRules/)
