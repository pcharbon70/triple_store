# Phase 3: Query Engine Improvements

## Overview

Phase 3 implements general query engine improvements that enhance performance across all query types. While Phase 2 addresses specific BSBM bottlenecks, this phase builds infrastructure for better query planning and repeated query optimization.

Key improvements:
- **Statistics Collection**: Gather cardinality and selectivity data for cost-based optimization
- **Query Result Caching**: Cache frequently executed queries to reduce repeated work
- **Join Optimization**: Refine cost model and selectivity estimation for better join ordering

These improvements compound with Phase 1 and 2 work, enabling the query engine to make smarter decisions about execution strategy.

---

## 3.1 Statistics Collection

- [x] **Section 3.1 Complete** (2026-01-02)

Current cardinality estimation uses hardcoded factors. Collecting actual statistics enables accurate cost-based optimization. Statistics include:
- Total triple count
- Distinct subjects, predicates, objects
- Per-predicate cardinality
- Numeric value histograms for range selectivity

### 3.1.1 Cardinality Statistics

- [x] **Task 3.1.1 Complete** (2026-01-02)

Implement basic cardinality collection for triple patterns.

- [x] 3.1.1.1 Design statistics data structure
- [x] 3.1.1.2 Identify collection points (load, update, explicit refresh)
- [x] 3.1.1.3 Create `TripleStore.Statistics` module enhancements
- [x] 3.1.1.4 Implement `collect/1` function to gather statistics
- [x] 3.1.1.5 Implement `total_triples/1` count
- [x] 3.1.1.6 Implement `distinct_subjects/1` count
- [x] 3.1.1.7 Implement `distinct_predicates/1` count
- [x] 3.1.1.8 Implement `distinct_objects/1` count
- [x] 3.1.1.9 Store statistics in RocksDB id2str column family
- [x] 3.1.1.10 Implement `load/1` to read persisted statistics

### 3.1.2 Predicate Cardinalities

- [x] **Task 3.1.2 Complete** (2026-01-02)

Collect per-predicate statistics for selectivity estimation.

- [x] 3.1.2.1 Design predicate statistics structure
- [x] 3.1.2.2 Implement `predicate_count/2` for specific predicate
- [x] 3.1.2.3 Implement `build_predicate_histogram/1` scanning all predicates
- [x] 3.1.2.4 Store predicate cardinality map in statistics
- [ ] 3.1.2.5 Implement incremental update on insert/delete (deferred)
- [x] 3.1.2.6 Add telemetry for statistics collection time

### 3.1.3 Numeric Histograms

- [x] **Task 3.1.3 Complete** (2026-01-02)

Build histograms for numeric predicates to estimate range selectivity.

- [x] 3.1.3.1 Design histogram structure (equi-width or equi-depth)
- [x] 3.1.3.2 Identify numeric predicates via inline encoding detection
- [x] 3.1.3.3 Implement `build_numeric_histogram/3` for predicate
- [x] 3.1.3.4 Implement `estimate_range_selectivity/4` using histogram
- [x] 3.1.3.5 Store histograms in statistics structure
- [x] 3.1.3.6 Configure histogram bucket count (default: 100)

### 3.1.4 Statistics Refresh

- [x] **Task 3.1.4 Complete** (2026-01-02)

Implement automatic and manual statistics refresh.

- [x] 3.1.4.1 Design refresh strategy (after N writes, periodic, manual)
- [x] 3.1.4.2 Implement `refresh/1` for full statistics rebuild
- [ ] 3.1.4.3 Implement `refresh_incremental/1` for delta updates (deferred)
- [x] 3.1.4.4 Add `:auto_refresh` option to server configuration
- [x] 3.1.4.5 Implement background refresh via GenServer
- [x] 3.1.4.6 Add `Statistics.Server` with get_stats/refresh/notify_modification APIs

### 3.1.5 Unit Tests

- [x] **Task 3.1.5 Complete** (2026-01-02)

- [x] 3.1.5.1 Test statistics collection on empty store
- [x] 3.1.5.2 Test statistics collection with data
- [x] 3.1.5.3 Test predicate cardinality accuracy
- [x] 3.1.5.4 Test histogram range estimates
- [x] 3.1.5.5 Test statistics persistence across restart
- [x] 3.1.5.6 Test server caching and refresh
- [x] 3.1.5.7 Test auto-refresh triggers

---

## 3.2 Query Result Caching

- [x] **Section 3.2 Implementation Complete** (2026-01-02)

Repeated queries with identical patterns can benefit from result caching. This is particularly valuable for:
- Dashboard queries that refresh periodically
- Join subqueries that repeat across complex patterns
- DESCRIBE-like queries for entity pages

### 3.2.1 LRU Cache Implementation

- [x] **Task 3.2.1 Complete** (2026-01-02)

Implement an LRU cache for query results.

- [x] 3.2.1.1 Design cache key structure (normalized query hash)
- [x] 3.2.1.2 Design cache value structure (result set + metadata)
- [x] 3.2.1.3 Create `TripleStore.Query.Cache` module
- [x] 3.2.1.4 Implement cache using ETS with access tracking
- [x] 3.2.1.5 Implement `get/2` returning cached result or miss
- [x] 3.2.1.6 Implement `put/3` storing result with TTL
- [x] 3.2.1.7 Implement LRU eviction when cache size exceeded
- [x] 3.2.1.8 Add configuration for max entries and max memory

### 3.2.2 Cache Key Generation

- [x] **Task 3.2.2 Complete** (2026-01-02)

Implement query normalization for cache key generation.

- [x] 3.2.2.1 Design query normalization rules
- [x] 3.2.2.2 Implement `normalize_query/1` function
- [x] 3.2.2.3 Handle variable renaming for canonical form
- [x] 3.2.2.4 Handle predicate ordering normalization (optional - not critical)
- [x] 3.2.2.5 Implement `hash_query/1` using `:crypto.hash/2`
- [x] 3.2.2.6 Handle parameter substitution in cache key

### 3.2.3 TTL Management

- [x] **Task 3.2.3 Complete** (2026-01-02)

Implement time-based cache invalidation.

- [x] 3.2.3.1 Design TTL storage with cache entries
- [x] 3.2.3.2 Implement `check_ttl/1` for expiration detection
- [x] 3.2.3.3 Implement background expiration process
- [x] 3.2.3.4 Add per-query TTL override option (server-wide TTL)
- [x] 3.2.3.5 Default TTL: 5 minutes
- [x] 3.2.3.6 Implement `clear/1` for manual invalidation

### 3.2.4 Cache Invalidation

- [x] **Task 3.2.4 Complete** (2026-01-02)

Implement cache invalidation on data changes.

- [x] 3.2.4.1 Design invalidation strategy (conservative vs precise)
- [x] 3.2.4.2 Implement predicate-based invalidation
- [x] 3.2.4.3 Track affected predicates per cached query
- [x] 3.2.4.4 Invalidate on insert/delete affecting tracked predicates
- [x] 3.2.4.5 Add `:cache_invalidation` option (conservative mode)

### 3.2.5 Executor Integration

- [x] **Task 3.2.5 Complete** (2026-01-02)

Integrate cache with query executor.

- [x] 3.2.5.1 Design cache check integration point
- [x] 3.2.5.2 Implement `get_or_execute/3` wrapper function
- [x] 3.2.5.3 Add `:use_cache` option to query functions
- [x] 3.2.5.4 Skip caching for UPDATE queries
- [x] 3.2.5.5 Skip caching for queries with RAND() or NOW()
- [x] 3.2.5.6 Add telemetry for cache hit rate

### 3.2.6 Unit Tests

- [x] **Task 3.2.6 Complete** (2026-01-02)

- [x] 3.2.6.1 Test cache stores and retrieves results correctly
- [x] 3.2.6.2 Test LRU eviction order
- [x] 3.2.6.3 Test TTL expiration
- [x] 3.2.6.4 Test cache invalidation on insert
- [x] 3.2.6.5 Test cache invalidation on delete
- [x] 3.2.6.6 Test normalized key handles variable renaming
- [x] 3.2.6.7 Test executor integration
- [x] 3.2.6.8 Test cache bypass for non-cacheable queries

---

## 3.3 Join Optimization

- [x] **Section 3.3 Analysis Complete** (2025-12-31)

Refine the cost model and join ordering to leverage collected statistics. Current join ordering uses heuristic selectivity factors; with real statistics, cost-based optimization becomes possible.

### 3.3.1 Cost Model Refinement

- [x] **Task 3.3.1 Analysis Complete** (2025-12-31)

Update cost model to use actual statistics.

- [x] 3.3.1.1 Analyze current cost model in `cost_model.ex`
- [ ] 3.3.1.2 Integrate statistics lookup into cost estimation
- [ ] 3.3.1.3 Use predicate cardinality for selectivity
- [ ] 3.3.1.4 Use histogram data for range filter selectivity
- [ ] 3.3.1.5 Calibrate I/O cost factors against actual measurements
- [ ] 3.3.1.6 Add configuration for cost model weights

### 3.3.2 Selectivity Estimation

- [x] **Task 3.3.2 Analysis Complete** (2025-12-31)

Improve selectivity estimation using statistics.

- [x] 3.3.2.1 Analyze current selectivity in `cardinality.ex`
- [ ] 3.3.2.2 Implement `estimate_pattern_selectivity/2` using stats
- [ ] 3.3.2.3 Handle bound constants with distinct count
- [ ] 3.3.2.4 Handle joins with independence assumption
- [ ] 3.3.2.5 Handle correlated predicates (future: sampling)
- [ ] 3.3.2.6 Implement selectivity caching per query

### 3.3.3 Join Enumeration Tuning

- [x] **Task 3.3.3 Analysis Complete** (2025-12-31)

Tune join enumeration algorithm with statistics.

- [x] 3.3.3.1 Analyze current enumeration in `join_enumeration.ex`
- [ ] 3.3.3.2 Pass statistics to cost estimation functions
- [ ] 3.3.3.3 Improve greedy join selection with actual costs
- [ ] 3.3.3.4 Tune DPccp thresholds based on pattern count
- [ ] 3.3.3.5 Add explain output showing estimated costs

### 3.3.4 Leapfrog Triejoin Tuning

- [x] **Task 3.3.4 Analysis Complete** (2025-12-31)

Optimize leapfrog triejoin selection with statistics.

- [x] 3.3.4.1 Analyze current leapfrog detection in `join_enumeration.ex:559-607`
- [ ] 3.3.4.2 Use statistics to estimate leapfrog benefit
- [ ] 3.3.4.3 Compare leapfrog cost vs hash join cost
- [ ] 3.3.4.4 Tune `@leapfrog_min_patterns` threshold
- [ ] 3.3.4.5 Add leapfrog selection to cost model

### 3.3.5 Unit Tests

- [ ] **Task 3.3.5 Complete**

- [ ] 3.3.5.1 Test cost model with LUBM statistics
- [ ] 3.3.5.2 Test selectivity estimates against actual cardinalities
- [ ] 3.3.5.3 Test join ordering produces lower-cost plans
- [ ] 3.3.5.4 Test leapfrog selection thresholds
- [ ] 3.3.5.5 Test cost model calibration accuracy

---

## 3.4 Integration Tests

- [ ] **Section 3.4 Complete**

End-to-end integration tests for query engine improvements.

### 3.4.1 Statistics Integration Tests

- [ ] **Task 3.4.1 Complete**

Test statistics integration with query execution.

- [ ] 3.4.1.1 Test optimizer uses collected statistics
- [ ] 3.4.1.2 Test query plans improve with statistics
- [ ] 3.4.1.3 Test statistics refresh doesn't disrupt queries
- [ ] 3.4.1.4 Test statistics persist across restarts

### 3.4.2 Cache Integration Tests

- [ ] **Task 3.4.2 Complete**

Test cache integration with query execution.

- [ ] 3.4.2.1 Test repeated queries hit cache
- [ ] 3.4.2.2 Test cache invalidation on data change
- [ ] 3.4.2.3 Test cache memory bounds
- [ ] 3.4.2.4 Test cache doesn't affect correctness

### 3.4.3 Optimizer Integration Tests

- [ ] **Task 3.4.3 Complete**

Test optimizer improvements end-to-end.

- [ ] 3.4.3.1 Test LUBM queries use optimal join order
- [ ] 3.4.3.2 Test BSBM queries use optimal join order
- [ ] 3.4.3.3 Test complex queries benefit from statistics
- [ ] 3.4.3.4 Test explain output is accurate

---

## Success Criteria

1. **Statistics**: Cardinality estimates within 2x of actual for common patterns
2. **Cache Hit Rate**: >80% for repeated queries in typical workload
3. **Join Quality**: Cost-based plans outperform heuristic plans
4. **Performance**: No regression in query latency from overhead
5. **Reliability**: Statistics and cache don't cause correctness issues

## Provides Foundation

This phase enables:
- **Phase 4**: Storage tuning can leverage statistics for configuration
- **Future**: Adaptive query execution, query hints, explain analyze

## References

- [Performance Improvement Analysis](../../reviews/benchmark-performance-improvements.md#part-4-query-engine-improvements-general)
- [Cost Model](../../../lib/triple_store/sparql/cost_model.ex)
- [Cardinality](../../../lib/triple_store/sparql/cardinality.ex)
- [Join Enumeration](../../../lib/triple_store/sparql/join_enumeration.ex)
