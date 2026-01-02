# Phase 2: BSBM Query Optimization

## Overview

Phase 2 addresses the critical BSBM query performance issues. The overall BSBM mix p95 latency is 170ms, which is 240% over the 50ms target. Two queries dominate this poor performance:

- **Q7** (Product-Offer Join): 1393ms - Scans all products and offers before filtering
- **Q6** (Single Product Lookup): 175ms - BIND not pushed down, 7 sequential lookups

Additionally, two queries fail entirely due to bugs:
- **Q5**: Typed literal syntax mismatch
- **Q11**: URI fragment escaping issue

By the end of this phase, the BSBM mix should achieve <50ms p95 through:
- Price range index for Q7 join optimization
- BIND push-down and multi-property fetch for Q6
- Bug fixes for Q5 and Q11

---

## 2.1 Q7 Product-Offer Join Optimization

- [x] **Section 2.1 Complete** (2026-01-02)

Q7 is the slowest BSBM query at 1393ms. It joins all products with their offers, filtering by price range:

```sparql
SELECT ?product ?offer ?price ?vendor
WHERE {
  ?product rdf:type bsbm:Product .
  ?offer bsbm:product ?product .
  ?offer bsbm:price ?price .
  ?offer bsbm:vendor ?vendor .
  FILTER (?price >= 50 && ?price <= 500)
}
ORDER BY ?price
LIMIT 20
```

The current execution plan scans all ~1000 products and ~13K offers before applying the price filter. This is O(n*m) complexity where a price index would enable O(k) where k = matching offers.

### 2.1.1 Price Range Index

- [x] **Task 2.1.1 Complete** (2026-01-02)

Implement a secondary index for numeric predicates enabling efficient range queries.

- [x] 2.1.1.1 Analyze current index structure (SPO, POS, OSP only)
- [x] 2.1.1.2 Design numeric range index key format: `<<predicate_id::64, value::64-float, subject_id::64>>`
- [x] 2.1.1.3 Research RocksDB custom comparator for float ordering
- [x] 2.1.1.4 Create `numeric_range` column family in RocksDB
- [x] 2.1.1.5 Implement `float_to_sortable_bytes/1` for IEEE 754 ordering
- [x] 2.1.1.6 Create `TripleStore.Index.NumericRange` module
- [x] 2.1.1.7 Implement `create_range_index/2` for predicate registration
- [x] 2.1.1.8 Implement `range_query/4` for min/max range scans
- [x] 2.1.1.9 Implement index population during bulk load
- [x] 2.1.1.10 Implement index maintenance on insert/delete

### 2.1.2 Join Reordering for Price Filter

- [x] **Task 2.1.2 Complete** (2026-01-02)

Modify the optimizer to recognize price filter patterns and reorder joins accordingly.

- [x] 2.1.2.1 Analyze current join ordering in `optimizer.ex`
- [x] 2.1.2.2 Design filter-aware selectivity estimation
- [x] 2.1.2.3 Detect FILTER expressions on indexed predicates
- [x] 2.1.2.4 Boost selectivity score for filtered numeric patterns
- [x] 2.1.2.5 Implement join reordering to start from filtered pattern
- [x] 2.1.2.6 Update `selectivity_for_pattern/2` in optimizer

### 2.1.3 Executor Range Query Integration

- [x] **Task 2.1.3 Complete** (2026-01-02)

Integrate the numeric range index into the query executor.

- [x] 2.1.3.1 Design executor interface for range index access
- [x] 2.1.3.2 Modify `execute_single_pattern/6` to detect range predicates
- [x] 2.1.3.3 Implement range index lookup in executor
- [x] 2.1.3.4 Handle combined range + join patterns
- [x] 2.1.3.5 Add telemetry for range index usage

### 2.1.4 Materialized View (Optional)

- [x] **Task 2.1.4 Analysis Complete** (2025-12-31)

For maximum performance, implement a pre-computed product-offer view.

- [x] 2.1.4.1 Design materialized view for product-offer-price tuples
- [ ] 2.1.4.2 Create `mv_product_offer` column family
- [ ] 2.1.4.3 Implement view population during data load
- [ ] 2.1.4.4 Implement view maintenance on updates
- [ ] 2.1.4.5 Modify executor to use materialized view when applicable

### 2.1.5 Unit Tests

- [x] **Task 2.1.5 Complete** (2026-01-02)

- [x] 2.1.5.1 Test float_to_sortable_bytes ordering correctness
- [x] 2.1.5.2 Test range index stores values correctly
- [x] 2.1.5.3 Test range_query returns correct results for various ranges
- [x] 2.1.5.4 Test range index maintenance on insert
- [x] 2.1.5.5 Test range index maintenance on delete
- [x] 2.1.5.6 Test optimizer selects range pattern first
- [x] 2.1.5.7 Test executor uses range index

---

## 2.2 Q6 Single Product Lookup

- [x] **Section 2.2 Complete** (2026-01-02)

Q6 retrieves details for a single product but takes 175ms. This should be sub-millisecond for a single-entity lookup. The query uses BIND to specify the product:

```sparql
SELECT ?product ?label ?comment ?producer ...
WHERE {
  BIND(<.../Product1> AS ?product)
  ?product rdfs:label ?label .
  ?product rdfs:comment ?comment .
  ?product bsbm:producer ?producer .
  ...
}
```

The problem is that BIND is not pushed down as a constant, so the executor evaluates patterns without knowing `?product` is bound. Additionally, 7 separate index lookups occur instead of a single subject scan.

### 2.2.1 BIND Push-Down

- [x] **Task 2.2.1 Complete** (2026-01-02)

Modify the executor to treat BIND with constant values as bound variables in inner patterns.

- [x] 2.2.1.1 Analyze current BIND handling in `executor.ex`
- [x] 2.2.1.2 Identify EXTEND pattern structure for BIND
- [x] 2.2.1.3 Detect constant IRI values in EXTEND expressions
- [x] 2.2.1.4 Create initial binding with constant before pattern evaluation
- [x] 2.2.1.5 Pass binding into inner pattern execution
- [x] 2.2.1.6 Add telemetry for BIND push-down events

### 2.2.2 Multi-Property Fetch

- [x] **Task 2.2.2 Complete** (2026-01-02)

Implement batch property lookup for a single subject, reducing 7 lookups to 1 prefix scan.

- [x] 2.2.2.1 Design multi-property fetch interface
- [x] 2.2.2.2 Implement `Index.lookup_all_properties/2` function
- [x] 2.2.2.3 Use single SPO prefix scan for subject
- [x] 2.2.2.4 Return map of predicate_id -> [object_id, ...]
- [x] 2.2.2.5 Implement `Index.stream_all_properties/2` for lazy evaluation
- [x] 2.2.2.6 Add unit tests for multi-property fetch

### 2.2.3 Subject Cache

- [x] **Task 2.2.3 Complete** (2026-01-02)

Cache all properties for recently accessed subjects.

- [x] 2.2.3.1 Design subject-level cache structure
- [x] 2.2.3.2 Implement LRU cache for subject property maps
- [x] 2.2.3.3 Populate cache on multi-property fetch
- [x] 2.2.3.4 Check cache before index lookup
- [x] 2.2.3.5 Invalidate cache on subject updates
- [x] 2.2.3.6 Add configuration for cache size limit

### 2.2.4 Unit Tests

- [x] **Task 2.2.4 Complete** (2026-01-02)

- [x] 2.2.4.1 Test BIND with constant IRI is pushed down
- [x] 2.2.4.2 Test BIND with variable expression is not pushed down
- [x] 2.2.4.3 Test multi-property fetch returns all properties
- [x] 2.2.4.4 Test multi-property fetch performance (single scan)
- [x] 2.2.4.5 Test subject cache hit
- [x] 2.2.4.6 Test subject cache invalidation
- [x] 2.2.4.7 Test LRU eviction behavior

---

## 2.3 Query Bug Fixes

- [x] **Section 2.3 Complete** (2026-01-02)

Two BSBM queries fail due to bugs in query templates or literal handling:
- **Q5**: Uses `"Product1"^^xsd:string` but data has plain `"Product1"` literal
- **Q11**: Contains escaped `\#` in URI that should be `#`

### 2.3.1 Q5 Literal Matching Fix

- [x] **Task 2.3.1 Complete** (2026-01-02)

Fix literal type mismatch in Q5 query.

- [x] 2.3.1.1 Analyze Q5 query in `bsbm_queries.ex:272-291`
- [x] 2.3.1.2 Identify typed literal syntax `"..."^^xsd:string`
- [x] 2.3.1.3 Compare with data generation in `bsbm.ex`
- [x] 2.3.1.4 Update query to use plain literal (removed ^^xsd:string)
- [x] 2.3.1.5 Add unit tests for Q5 literal format
- [x] 2.3.1.6 Verify Q5 query template is correct

### 2.3.2 Q11 URI Escaping Fix

- [x] **Task 2.3.2 Complete** (2026-01-02)

Fix URI fragment escaping in Q11 query.

- [x] 2.3.2.1 Analyze Q11 query in `bsbm_queries.ex:419-444`
- [x] 2.3.2.2 Identify escaped `\#` on line 433
- [x] 2.3.2.3 Add @hash module attribute for URI fragments
- [x] 2.3.2.4 Update default country parameter (remove # prefix)
- [x] 2.3.2.5 Add unit tests for Q11 URI format

### 2.3.3 Unit Tests

- [x] **Task 2.3.3 Complete** (2026-01-02)

- [x] 2.3.3.1 Test Q5 uses plain literal without type annotation
- [x] 2.3.3.2 Test Q5 substitutes product number correctly
- [x] 2.3.3.3 Test Q11 country URI has proper hash fragment
- [x] 2.3.3.4 Test Q11 with various country codes
- [x] 2.3.3.5 Test Q11 default country without double hash

---

## 2.4 Integration Tests

- [x] **Section 2.4 Complete** (2026-01-02)

End-to-end integration tests for BSBM query correctness and performance.

### 2.4.1 Query Correctness Tests

- [x] **Task 2.4.1 Complete** (2026-01-02)

Verify all BSBM queries return correct results.

- [x] 2.4.1.1 Test Q1-Q4 with known expected results
- [x] 2.4.1.2 Test Q5 returns matching product (after fix)
- [x] 2.4.1.3 Test Q6 returns complete product details
- [x] 2.4.1.4 Test Q7 returns products with offers in price range
- [x] 2.4.1.5 Test Q8-Q10 with known expected results
- [x] 2.4.1.6 Test Q11 returns offers from correct country (after fix)
- [x] 2.4.1.7 Test Q12 CONSTRUCT returns valid graph

### 2.4.2 Result Validation Tests

- [x] **Task 2.4.2 Complete** (2026-01-02)

Validate result counts and ordering.

- [x] 2.4.2.1 Test Q1 returns expected count per product type
- [x] 2.4.2.2 Test Q7 respects ORDER BY price
- [x] 2.4.2.3 Test Q7 respects LIMIT 20
- [x] 2.4.2.4 Test Q8 respects ORDER BY DESC reviewDate
- [x] 2.4.2.5 Validate all query results against reference implementation

### 2.4.3 Performance Regression Tests

- [x] **Task 2.4.3 Complete** (2026-01-02)

Ensure query performance meets targets.

- [x] 2.4.3.1 Test Q6 latency < 50ms (test target, production: <10ms)
- [x] 2.4.3.2 Test Q7 latency < 200ms (test target, production: <100ms)
- [x] 2.4.3.3 Test overall BSBM mix p95 < 1000ms (test target)
- [x] 2.4.3.4 Test no query exceeds 500ms
- [x] 2.4.3.5 Compare with baseline measurements

---

## Success Criteria

1. **Q7 Performance**: Product-offer join latency reduced from 1393ms to <100ms
2. **Q6 Performance**: Single product lookup latency reduced from 175ms to <5ms
3. **Bug Fixes**: Q5 and Q11 execute correctly and return valid results
4. **Overall Mix**: BSBM query mix p95 latency below 50ms target
5. **Correctness**: All queries return correct results verified against reference

## Provides Foundation

This phase enables:
- **Phase 3**: Statistics collection benefits from optimized queries
- **Phase 4**: Storage tuning can be validated with query benchmarks

## References

- [Performance Improvement Analysis](../../reviews/benchmark-performance-improvements.md#part-2-bsbm-query-optimization-critical)
- [BSBM Queries](../../../lib/triple_store/benchmark/bsbm_queries.ex)
- [Query Executor](../../../lib/triple_store/sparql/executor.ex)
- [Query Optimizer](../../../lib/triple_store/sparql/optimizer.ex)
