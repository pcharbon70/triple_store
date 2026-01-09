# Phase 5: Statistics and Optimization for Quads

## Overview

Phase 5 extends the statistics and optimization layer to work efficiently with quads. By the end of this phase, the query optimizer will have accurate cardinality estimates for quad patterns and will be able to choose optimal execution plans for graph-aware queries.

The statistics system is extended to track counts per graph, and the optimizer uses this information to select the best index and execution order for quad patterns.

---

## 5.1 Per-Graph Statistics

### 5.1.1 Graph Quad Counts

Implement counting of quads per graph.

- [ ] 5.1.1.1 Implement `graph_quad_count/2` returning count for specific graph
- [ ] 5.1.1.2 Use GSPO prefix scan to count quads in graph
- [ ] 5.1.1.3 Cache per-graph counts in statistics cache
- [ ] 5.1.1.4 Handle default graph count separately
- [ ] 5.1.1.5 Return `:not_found` for non-existent graph

### 5.1.2 Graph Predicate Statistics

Track predicate counts per graph.

- [ ] 5.1.2.1 Implement `graph_predicate_counts/2` for specific graph
- [ ] 5.1.2.2 Scan graph to build predicate frequency map
- [ ] 5.1.2.3 Cache per-graph predicate histograms
- [ ] 5.1.2.4 Update on graph modifications
- [ ] 5.1.2.5 Return map: `%{predicate_id => count}`

### 5.1.3 Graph Subject/Object Statistics

Track subject and object counts per graph.

- [ ] 5.1.3.1 Implement `graph_subject_count/2` for distinct subjects
- [ ] 5.1.3.2 Implement `graph_object_count/2` for distinct objects
- [ ] 5.1.3.3 Use sampling for large graphs (configurable)
- [ ] 5.1.3.4 Cache results in statistics cache
- [ ] 5.1.3.5 Return approximate counts

### 5.1.4 Graph Summary

Provide complete summary for a graph.

- [ ] 5.1.4.1 Implement `graph_summary/2` returning complete stats
- [ ] 5.1.4.2 Include: quad count, distinct predicates, subjects, objects
- [ ] 5.1.4.3 Include: size estimate, last modified time
- [ ] 5.1.4.4 Return structured map for consumption
- [ ] 5.1.4.5 Use cached data when available

### 5.1.5 All Graphs Summary

Provide summary across all graphs.

- [ ] 5.1.5.1 Implement `all_graphs_summary/1` returning summary for all
- [ ] 5.1.5.2 Include: total quads, graph count, largest graph
- [ ] 5.1.5.3 Include: per-graph breakdown
- [ ] 5.1.5.4 Use cached per-graph data
- [ ] 5.1.5.5 Return aggregated statistics

---

## 5.2 Quad Pattern Cardinality Estimation

### 5.2.1 Quad Pattern Estimation

Implement cardinality estimation for quad patterns.

- [ ] 5.2.1.1 Implement `estimate_quad_pattern/2` for quad patterns
- [ ] 5.2.1.2 Use per-graph statistics when graph bound
- [ ] 5.2.1.3 Use aggregate statistics when graph unbound
- [ ] 5.2.1.4 Apply selectivity based on bound positions
- [ ] 5.2.1.5 Return estimated cardinality as float

### 5.2.2 Bound Position Selectivity

Calculate selectivity for each bound position.

- [ ] 5.2.2.1 Calculate subject selectivity: `count / distinct_subjects`
- [ ] 5.2.2.2 Calculate predicate selectivity: `count / predicate_count`
- [ ] 5.2.2.3 Calculate object selectivity: `count / distinct_objects`
- [ ] 5.2.2.4 Calculate graph selectivity: `count / total_graphs` or 1.0 if bound
- [ ] 5.2.2.5 Combine selectivities: `product of all bound positions`

### 5.2.3 Cross-Graph Pattern Estimation

Handle estimation for patterns across graphs.

- [ ] 5.2.3.1 Handle pattern with unbound graph (cross-graph)
- [ ] 5.2.3.2 Sum cardinalities across all graphs
- [ ] 5.2.3.3 Use graph summary data for efficient estimation
- [ ] 5.2.3.4 Account for graphs with no matching patterns
- [ ] 5.2.3.5 Return aggregated estimate

### 5.2.4 Join Cardinality Estimation

Estimate cardinality for quad joins.

- [ ] 5.2.4.1 Extend `estimate_join_cardinality/3` for quads
- [ ] 5.2.4.2 Account for graph variable joining
- [ ] 5.2.4.3 Account for cross-graph joins (when compatible)
- [ ] 5.2.4.4 Use independent join when graphs disjoint
- [ ] 5.2.4.5 Return estimate with confidence interval

---

## 5.3 Query Optimizer Adaptation

### 5.3.1 Quad Pattern Ordering

Extend optimizer to order quad patterns.

- [ ] 5.3.1.1 Update `reorder_bgp_patterns/2` for quad patterns
- [ ] 5.3.1.2 Use `estimate_quad_pattern/2` for selectivity
- [ ] 5.3.1.3 Prefer patterns with bound graph first
- [ ] 5.3.1.4 Within same graph, use existing heuristics
- [ ] 5.3.1.5 Test ordering produces efficient plans

### 5.3.2 Index Selection for Quads

Extend index selection for quad patterns.

- [ ] 5.3.2.1 Use `QuadIndex.select_index_for_quad/1` in optimizer
- [ ] 5.3.2.2 Consider index access pattern in cost calculation
- [ ] 5.3.2.3 Prefer GSPO/GPOS for graph-scoped queries
- [ ] 5.3.2.4 Prefer SPOG/POSG for cross-graph queries
- [ ] 5.3.2.5 Document index selection strategy

### 5.3.3 Graph-Aware Cost Model

Extend cost model for graph operations.

- [ ] 5.3.3.1 Add cost for graph switching in execution
- [ ] 5.3.3.2 Add cost for cross-graph joins
- [ ] 5.3.3.3 Model cost of iterating over graphs
- [ ] 5.3.3.4 Use per-graph statistics for accurate costing
- [ ] 5.3.3.5 Update total plan cost calculation

### 5.3.4 Join Reordering with Graphs

Extend join reordering to consider graph binding.

- [ ] 5.3.4.1 Detect when graph variable is shared across patterns
- [ ] 5.3.4.2 Prefer joining on graph early when beneficial
- [ ] 5.3.4.3 Avoid unnecessary cross-graph joins
- [ ] 5.3.4.4 Group patterns by graph when possible
- [ ] 5.3.4.5 Document optimization strategy

---

## 5.4 Statistics Cache Extension

### 5.4.1 Cache Key Design

Design cache keys for per-graph statistics.

- [ ] 5.4.1.1 Use `{graph_id, :quad_count}` cache key
- [ ] 5.4.1.2 Use `{graph_id, :predicate_counts}` cache key
- [ ] 5.4.1.3 Use `{:all_graphs, :summary}` cache key
- [ ] 5.4.1.4 Ensure cache keys don't collide with triple stats
- [ ] 5.4.1.5 Document cache key structure

### 5.4.2 Cache Invalidation

Invalidate cache on graph modifications.

- [ ] 5.4.2.1 Invalidate graph-specific stats on graph modification
- [ ] 5.4.2.2 Invalidate all-graphs summary on any modification
- [ ] 5.4.2.3 Implement `invalidate_graph/2` for specific graph
- [ ] 5.4.2.4 Implement `invalidate_all/1` for complete invalidation
- [ ] 5.4.2.5 Add telemetry for cache invalidation

### 5.4.3 Cache Warming

Implement cache warming for statistics.

- [ ] 5.4.3.1 Implement `warm_graph_cache/2` for specific graph
- [ ] 5.4.3.2 Implement `warm_all_graphs_cache/1` for all graphs
- [ ] 5.4.3.3 Use parallel warming for multiple graphs
- [ ] 5.4.3.4 Add configurable warming on startup
- [ ] 5.4.3.5 Add telemetry for warming operations

---

## 5.5 Leapfrog Triejoin for Quads

### 5.5.1 Quad Trie Iterator

Extend trie iterator for quad keys.

- [ ] 5.5.1.1 Implement `QuadTrieIterator` for 32-byte keys
- [ ] 5.5.1.2 Support all four quad indices
- [ ] 5.5.1.3 Implement `seek/2` for quad prefix positioning
- [ ] 5.5.1.4 Implement `next/1` advancing to next quad
- [ ] 5.5.1.5 Extract binding from current quad key

### 5.5.2 Quad Leapfrog Algorithm

Extend leapfrog for quad patterns.

- [ ] 5.5.2.1 Implement `leapfrog_quads/2` for quad joins
- [ ] 5.5.2.2 Handle 4-way joins (s, p, o, g)
- [ ] 5.5.2.3 Use appropriate index for each variable
- [ ] 5.5.2.4 Produce complete bindings for all variables
- [ ] 5.5.2.5 Stream results efficiently

### 5.5.3 Variable Ordering for Quads

Determine optimal variable ordering for quad leapfrog.

- [ ] 5.5.3.1 Implement `quad_variable_ordering/2` for quad patterns
- [ ] 5.5.3.2 Use cardinality estimates for ordering
- [ ] 5.5.3.3 Consider index availability for each variable
- [ ] 5.5.3.4 Handle graph variable in ordering
- [ ] 5.5.3.5 Return optimal variable sequence

---

## 5.6 Unit Tests

### 5.6.1 Per-Graph Statistics Tests

- [ ] 5.6.1.1 Test graph_quad_count returns correct count
- [ ] 5.6.1.2 Test graph_predicate_counts returns correct histogram
- [ ] 5.6.1.3 Test graph_subject_count estimates correctly
- [ ] 5.6.1.4 Test graph_object_count estimates correctly
- [ ] 5.6.1.5 Test graph_summary returns complete stats
- [ ] 5.6.1.6 Test all_graphs_summary aggregates correctly
- [ ] 5.6.1.7 Test cache invalidation on modification

### 5.6.2 Cardinality Estimation Tests

- [ ] 5.6.2.1 Test estimate_quad_pattern for fully bound pattern
- [ ] 5.6.2.2 Test estimate_quad_pattern for partially bound
- [ ] 5.6.2.3 Test estimate_quad_pattern for unbound graph
- [ ] 5.6.2.4 Test bound position selectivity calculation
- [ ] 5.6.2.5 Test cross-graph pattern estimation
- [ ] 5.6.2.6 Test join cardinality estimation for quads

### 5.6.3 Optimizer Tests

- [ ] 5.6.3.1 Test quad pattern ordering prefers bound graph
- [ ] 5.6.3.2 Test index selection uses optimal quad index
- [ ] 5.6.3.3 Test graph-aware cost model calculation
- [ ] 5.6.3.4 Test join reordering with graph variable
- [ ] 5.6.3.5 Test cross-graph join optimization

### 5.6.4 Cache Tests

- [ ] 5.6.4.1 Test cache keys don't collide
- [ ] 5.6.4.2 Test cache retrieval returns correct stats
- [ ] 5.6.4.3 Test cache invalidation works correctly
- [ ] 5.6.4.4 Test cache warming populates cache
- [ ] 5.6.4.5 Test parallel cache warming

### 5.6.5 Leapfrog Tests

- [ ] 5.6.5.1 Test QuadTrieIterator seek positions correctly
- [ ] 5.6.5.2 Test QuadTrieIterator next advances correctly
- [ ] 5.6.5.3 Test leapfrog_quads produces correct results
- [ ] 5.6.5.4 Test quad variable ordering selects optimal order
- [ ] 5.6.5.5 Test leapfrog handles 4-way joins

---

## Success Criteria

1. **Per-Graph Stats**: Accurate statistics for each named graph
2. **Cardinality Estimation**: Quad patterns estimated accurately
3. **Optimizer**: Quad patterns ordered optimally
4. **Cost Model**: Graph-aware costs calculated correctly
5. **Cache**: Per-graph statistics cached efficiently
6. **Leapfrog**: Quad leapfrog works for 4-way joins

## Provides Foundation

This phase establishes the infrastructure for:
- **Phase 6**: Integration testing of complete quad functionality
- **Phase 7**: Reasoning with named graphs
- **Phase 8**: Production hardening for quad store

## Key Outputs

- Extended `TripleStore.Statistics` with per-graph stats
- Quad pattern cardinality estimation
- Graph-aware query optimization
- Quad leapfrog triejoin
- Per-graph statistics cache
