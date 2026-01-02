# Performance Targets

> **Last Validated:** 2025-12-31

## Overview

This document defines measurable performance targets for the TripleStore and tracks current status against those targets.

## Target Summary

```mermaid
graph LR
    subgraph Targets
        A[Simple BGP<br/>p95 < 10ms]
        B[Complex Join<br/>p95 < 100ms]
        C[Bulk Load<br/>> 100K tps]
        D[BSBM Mix<br/>p95 < 50ms]
    end

    subgraph Status
        A --> A1[Mixed]
        B --> B1[Pass]
        C --> C1[Fail]
        D --> D1[Fail]
    end

    style A1 fill:#ffd700
    style B1 fill:#90EE90
    style C1 fill:#ff6b6b
    style D1 fill:#ff6b6b
```

## Detailed Targets

### 1. Simple BGP Query

**Target:** p95 latency < 10ms on 1M triples

A simple Basic Graph Pattern (BGP) query with a single triple pattern and one bound term.

```sparql
SELECT ?x WHERE { ?x rdf:type ub:UndergraduateStudent }
```

| Dataset | p95 | Status |
|---------|-----|--------|
| LUBM 23K | 11.6ms | Borderline |
| BSBM 141K | 9.4ms | Pass |

**Analysis:** Simple type queries meet the target on moderate datasets. Performance scales linearly with result set size.

---

### 2. Complex Join Query

**Target:** p95 latency < 100ms on 1M triples

Multi-pattern queries with 3+ triple patterns requiring joins.

```sparql
SELECT ?x ?y ?z WHERE {
  ?x rdf:type ub:GraduateStudent .
  ?y rdf:type ub:University .
  ?z rdf:type ub:Department .
  ?x ub:memberOf ?z .
  ?z ub:subOrganizationOf ?y .
}
```

| Dataset | Query | p95 | Status |
|---------|-------|-----|--------|
| LUBM 23K | Q2 | 96.3ms | Pass (borderline) |
| BSBM 141K | Q7 | 1406.7ms | Fail |

**Analysis:** Join performance varies significantly by query structure. LUBM Q2 meets the target, but BSBM Q7 (which joins products with all offers) is 14x over target.

---

### 3. Bulk Load Throughput

**Target:** > 100,000 triples/second

Measure the rate of triple insertion during bulk loading operations.

| Dataset | Triples | Time | Throughput | Status |
|---------|---------|------|------------|--------|
| LUBM Scale 1 | 23,316 | 476ms | 48,983 tps | Fail |
| LUBM Scale 2 | 69,491 | 1,399ms | 49,671 tps | Fail |
| BSBM 500 | 70,457 | 1,873ms | 37,617 tps | Fail |
| BSBM 1000 | 141,084 | 4,332ms | 32,567 tps | Fail |

**Average:** 42,210 triples/sec (42% of target)

```mermaid
xychart-beta
    title "Bulk Load Throughput vs Target"
    x-axis ["LUBM-1", "LUBM-2", "BSBM-500", "BSBM-1000", "Target"]
    y-axis "Triples/sec" 0 --> 120000
    bar [48983, 49671, 37617, 32567, 100000]
```

**Analysis:** Current throughput is ~40-50% of target. Optimization opportunities:
- Larger batch sizes
- Parallel index updates
- Write-ahead logging optimization

---

### 4. BSBM Query Mix

**Target:** p95 latency < 50ms for overall query mix

The aggregate p95 latency across all BSBM queries.

| Metric | Value | Status |
|--------|-------|--------|
| Average p50 | 164.0ms | Fail |
| Average p95 | 169.9ms | Fail |

**Analysis:** The BSBM mix is dominated by Q6 (175ms) and Q7 (1393ms). Excluding these outliers:

| Metric | Value (excl. Q6, Q7) |
|--------|---------------------|
| Average p50 | 6.5ms |
| Average p95 | 11.8ms |

Core search queries perform well; lookup and join queries need optimization.

---

## Performance by Query Type

```mermaid
pie title Performance Distribution (All Queries)
    "< 1ms (Excellent)" : 8
    "1-10ms (Good)" : 5
    "10-50ms (Acceptable)" : 4
    "50-100ms (Slow)" : 2
    "100ms+ (Too Slow)" : 3
    "Error" : 2
```

## Validation Functions

The `TripleStore.Benchmark.Targets` module provides programmatic validation:

```elixir
# Check individual targets
Targets.check_simple_bgp(p95_us: 5000)
# => :pass

Targets.check_bulk_load(triples_per_sec: 50000)
# => {:fail, "throughput 50K triples/sec below target >100K"}

# Validate benchmark results
{:ok, report} = Targets.validate(benchmark_results)
Targets.print_report(report)
```

## Target Definitions

| Target | ID | Metric | Threshold | Unit | Dataset |
|--------|----|--------|-----------|------|---------|
| Simple BGP | `:simple_bgp` | p95 latency | < 10,000 | µs | 1M triples |
| Complex Join | `:complex_join` | p95 latency | < 100,000 | µs | 1M triples |
| Bulk Load | `:bulk_load` | throughput | > 100,000 | triples/sec | any |
| BSBM Mix | `:bsbm_mix` | p95 latency | < 50,000 | µs | 1M triples |

## Improvement Roadmap

### High Priority

1. **Bulk Load Optimization**
   - Implement batch dictionary encoding
   - Use write batches for atomic multi-index updates
   - Consider parallel ingestion

2. **Q7 Join Optimization**
   - Add price index for range queries
   - Implement join reordering based on selectivity
   - Consider materialized views for common patterns

### Medium Priority

3. **Q6 Lookup Investigation**
   - Profile single-product lookup path
   - Check for unnecessary index scans

4. **Query Planning**
   - Implement cost-based optimizer
   - Add statistics-based join ordering

## Historical Trends

*Future benchmark runs will populate this section with trend data.*

| Date | Simple BGP | Complex Join | Bulk Load | BSBM Mix |
|------|------------|--------------|-----------|----------|
| 2025-12-31 | Mixed | Pass | 42K tps | 170ms |

## References

- [LUBM Benchmark Guide](./lubm.md)
- [BSBM Benchmark Guide](./bsbm.md)
- [Targets Module](../../lib/triple_store/benchmark/targets.ex)
