# Phase 3: Advanced Query Processing

## Overview

Phase 3 implements advanced query processing features including worst-case optimal joins via Leapfrog Triejoin, cost-based query optimization, full SPARQL UPDATE support, and property path evaluation.

By the end of this phase, complex queries with many join patterns will execute efficiently using Leapfrog Triejoin, which provides worst-case optimal behavior for multi-way joins. The cost-based optimizer will select between nested loop, hash join, and Leapfrog based on query characteristics and statistics.

SPARQL UPDATE support enables INSERT/DELETE operations with transactional semantics using RocksDB snapshots for isolation.

---

## 3.1 Leapfrog Triejoin

- [x] **Section 3.1 Complete**

This section implements the Leapfrog Triejoin algorithm for worst-case optimal multi-way joins. The algorithm processes queries variable-by-variable using sorted iterators that "leapfrog" to find common values.

### 3.1.1 Trie Iterator

- [x] **Task 3.1.1 Complete**

Implement the trie iterator abstraction over RocksDB prefix scans.

- [x] 3.1.1.1 Define `%TrieIterator{db, cf, prefix, current, exhausted}` struct
- [x] 3.1.1.2 Implement `TrieIterator.new(db, cf, prefix)` initialization
- [x] 3.1.1.3 Implement `TrieIterator.seek(iter, target)` positioning to target
- [x] 3.1.1.4 Implement `TrieIterator.next(iter)` advancing to next value
- [x] 3.1.1.5 Implement `TrieIterator.current(iter)` returning current value
- [x] 3.1.1.6 Implement `TrieIterator.exhausted?(iter)` predicate

### 3.1.2 Leapfrog Algorithm

- [x] **Task 3.1.2 Complete**

Implement the core leapfrog join algorithm.

- [x] 3.1.2.1 Implement `leapfrog_init(iterators)` initializing sorted iterator list
- [x] 3.1.2.2 Implement `leapfrog_search(iterators)` finding next common value
- [x] 3.1.2.3 Implement `leapfrog_next(iterators)` advancing past current match
- [x] 3.1.2.4 Return Stream of matching tuples

### 3.1.3 Variable Ordering

- [x] **Task 3.1.3 Complete**

Determine optimal variable ordering for Leapfrog execution.

- [x] 3.1.3.1 Implement `variable_ordering(patterns, stats)` selecting order
- [x] 3.1.3.2 Use cardinality estimates to prefer selective variables first
- [x] 3.1.3.3 Consider index availability for each variable position

### 3.1.4 Multi-Level Iteration

- [x] **Task 3.1.4 Complete**

Extend Leapfrog to handle multiple variables through nested iteration.

- [x] 3.1.4.1 Implement descent to next variable level after match
- [x] 3.1.4.2 Implement ascent on exhaustion of lower level
- [x] 3.1.4.3 Produce complete bindings for all variables

### 3.1.5 Unit Tests

- [x] **Task 3.1.5 Complete**

- [x] Test trie iterator seek positions correctly
- [x] Test trie iterator next advances correctly
- [x] Test leapfrog finds common values across two iterators
- [x] Test leapfrog finds common values across three iterators
- [x] Test leapfrog handles exhausted iterator
- [x] Test multi-level iteration produces all matches
- [x] Test variable ordering prefers selective variables

---

## 3.2 Cost-Based Optimizer

- [x] **Section 3.2 Complete**

This section implements cost-based optimization using cardinality estimation and the DPccp algorithm for join enumeration on complex queries.

### 3.2.1 Cardinality Estimation

- [x] **Task 3.2.1 Complete**

Estimate result cardinality for patterns and joins.

- [x] 3.2.1.1 Implement `estimate_pattern_cardinality(pattern, stats)`
- [x] 3.2.1.2 Apply predicate selectivity from histogram
- [x] 3.2.1.3 Apply bound variable selectivity
- [x] 3.2.1.4 Implement `estimate_join_cardinality(left_card, right_card, join_vars)`

### 3.2.2 Cost Model

- [x] **Task 3.2.2 Complete**

Define cost model for different join strategies.

- [x] 3.2.2.1 Model nested loop join cost: O(left_card * right_card)
- [x] 3.2.2.2 Model hash join cost: O(left_card + right_card)
- [x] 3.2.2.3 Model Leapfrog cost: based on AGM bound
- [x] 3.2.2.4 Model I/O cost for index scans

### 3.2.3 Join Enumeration

- [x] **Task 3.2.3 Complete**

Enumerate join orderings to find optimal plan.

- [x] 3.2.3.1 Implement exhaustive enumeration for small queries (n <= 3)
- [x] 3.2.3.2 Implement DPccp algorithm for larger queries
- [x] 3.2.3.3 Prune invalid orderings (Cartesian products unless necessary)
- [x] 3.2.3.4 Select between nested loop, hash, and Leapfrog

### 3.2.4 Plan Cache

- [x] **Task 3.2.4 Complete**

Cache optimized plans for repeated queries.

- [x] 3.2.4.1 Implement `PlanCache` GenServer with ETS storage
- [x] 3.2.4.2 Key by query hash (parameterized queries share cache)
- [x] 3.2.4.3 Invalidate on statistics change or schema modification
- [x] 3.2.4.4 Implement LRU eviction policy

### 3.2.5 Unit Tests

- [x] **Task 3.2.5 Complete**

- [x] Test cardinality estimation for single pattern
- [x] Test cardinality estimation for join
- [x] Test cost model ranks plans correctly
- [x] Test exhaustive enumeration finds optimal plan
- [x] Test DPccp produces same plan as exhaustive for small queries
- [x] Test plan cache stores and retrieves plans
- [x] Test plan cache invalidates on update

---

## 3.3 SPARQL UPDATE

- [x] **Section 3.3 Complete**

This section implements SPARQL UPDATE operations with transactional semantics. Updates are serialized through a coordinator process and use RocksDB snapshots for isolation.

### 3.3.1 Update Execution

- [x] **Task 3.3.1 Complete**

Implement execution of parsed UPDATE operations.

- [x] 3.3.1.1 Implement `execute_insert_data(db, triples)` for direct insert
- [x] 3.3.1.2 Implement `execute_delete_data(db, triples)` for direct delete
- [x] 3.3.1.3 Implement `execute_delete_where(db, pattern)` for pattern delete
- [x] 3.3.1.4 Implement `execute_insert_where(db, template, pattern)` for templated insert
- [x] 3.3.1.5 Implement `execute_modify(db, delete_tmpl, insert_tmpl, pattern)` for combined

### 3.3.2 Transaction Coordinator

- [x] **Task 3.3.2 Complete**

Implement transaction coordination for serialized writes.

- [x] 3.3.2.1 Create `TripleStore.Transaction` GenServer
- [x] 3.3.2.2 Serialize all writes through coordinator
- [x] 3.3.2.3 Use RocksDB snapshots for read consistency during update
- [x] 3.3.2.4 Invalidate plan cache after successful update
- [x] 3.3.2.5 Handle update failure with rollback semantics

### 3.3.3 Update API

- [x] **Task 3.3.3 Complete**

Implement the public UPDATE API.

- [x] 3.3.3.1 Implement `TripleStore.update(db, sparql)` for SPARQL UPDATE
- [x] 3.3.3.2 Implement `TripleStore.insert(db, triples)` for direct insert
- [x] 3.3.3.3 Implement `TripleStore.delete(db, triples)` for direct delete
- [x] 3.3.3.4 Return affected triple count

### 3.3.4 Unit Tests

- [x] **Task 3.3.4 Complete**

- [x] Test INSERT DATA adds triples
- [x] Test DELETE DATA removes triples
- [x] Test DELETE WHERE removes matching triples
- [x] Test INSERT WHERE adds templated triples
- [x] Test MODIFY combines delete and insert
- [x] Test concurrent reads see consistent snapshot during update
- [x] Test plan cache invalidated after update
- [x] Test update failure leaves database unchanged

---

## 3.4 Property Paths

- [x] **Section 3.4 Complete**

This section implements SPARQL property path evaluation with support for sequence, alternative, and recursive paths.

### 3.4.1 Non-Recursive Paths

- [x] **Task 3.4.1 Complete**

Implement non-recursive property path operators.

- [x] 3.4.1.1 Implement sequence path (p1/p2)
- [x] 3.4.1.2 Implement alternative path (p1|p2)
- [x] 3.4.1.3 Implement inverse path (^p)
- [x] 3.4.1.4 Implement negated property set (!(p1|p2))

### 3.4.2 Recursive Paths

- [x] **Task 3.4.2 Complete**

Implement recursive path evaluation with cycle detection.

- [x] 3.4.2.1 Implement zero-or-more path (p*)
- [x] 3.4.2.2 Implement one-or-more path (p+)
- [x] 3.4.2.3 Implement optional path (p?)
- [x] 3.4.2.4 Implement cycle detection via visited set
- [x] 3.4.2.5 Handle arbitrary-length paths efficiently

### 3.4.3 Path Optimization

- [x] **Task 3.4.3 Complete**

Optimize common property path patterns.

- [x] 3.4.3.1 Detect and optimize fixed-length paths
- [x] 3.4.3.2 Use bidirectional search when both endpoints bound
- [x] 3.4.3.3 Consider materialized path indices (optional)

### 3.4.4 Unit Tests

- [x] **Task 3.4.4 Complete**

- [x] Test sequence path traversal
- [x] Test alternative path branches correctly
- [x] Test inverse path reverses direction
- [x] Test negated property set excludes correctly
- [x] Test zero-or-more includes start node
- [x] Test one-or-more excludes start node
- [x] Test cycle detection prevents infinite loops
- [x] Test path with both endpoints bound

---

## 3.5 Phase 3 Integration Tests

- [ ] **Section 3.5 Complete**

Integration tests validate advanced query processing features working together.

### 3.5.1 Leapfrog Integration Testing

- [x] **Task 3.5.1 Complete**

Test Leapfrog Triejoin on complex queries.

- [x] 3.5.1.1 Test star query with 5+ patterns via Leapfrog
- [x] 3.5.1.2 Compare Leapfrog results to nested loop baseline
- [x] 3.5.1.3 Benchmark Leapfrog vs nested loop on star queries
- [x] 3.5.1.4 Test optimizer selects Leapfrog for appropriate queries

### 3.5.2 Update Integration Testing

- [x] **Task 3.5.2 Complete**

Test SPARQL UPDATE with complex scenarios.

- [x] 3.5.2.1 Test DELETE/INSERT WHERE modifying same triples
- [x] 3.5.2.2 Test concurrent queries during update see consistent state
- [x] 3.5.2.3 Test large batch updates (10K+ triples)
- [x] 3.5.2.4 Test update with inference implications (prepare for Phase 4)

### 3.5.3 Property Path Integration Testing

- [x] **Task 3.5.3 Complete**

Test property paths on real-world patterns.

- [x] 3.5.3.1 Test rdfs:subClassOf* for class hierarchy traversal
- [x] 3.5.3.2 Test foaf:knows+ for social network paths
- [x] 3.5.3.3 Test combined sequence and alternative paths
- [x] 3.5.3.4 Benchmark recursive paths on deep hierarchies

### 3.5.4 Optimizer Integration Testing

- [ ] **Task 3.5.4 Complete**

Test cost-based optimizer selections.

- [ ] 3.5.4.1 Test optimizer chooses hash join for large intermediate results
- [ ] 3.5.4.2 Test optimizer chooses nested loop for small inputs
- [ ] 3.5.4.3 Test optimizer chooses Leapfrog for multi-way joins
- [ ] 3.5.4.4 Test plan cache shows >90% hit rate on repeated queries

---

## Success Criteria

1. **Leapfrog**: Outperforms nested loop on star queries with 5+ patterns
2. **Optimizer**: Selects appropriate join strategy based on statistics
3. **Plan Cache**: >90% hit rate on repeated query workloads
4. **UPDATE**: All operations work correctly with transaction isolation
5. **Property Paths**: Handle cycles without infinite loops
6. **Performance**: Complex queries 2-10x faster than Phase 2 baseline

## Provides Foundation

This phase establishes the infrastructure for:
- **Phase 4**: Reasoner uses UPDATE for materialization; efficient joins for rule evaluation
- **Phase 5**: Plan cache integrates with query telemetry; optimizer tuning

## Key Outputs

- `TripleStore.Query.LeapfrogTriejoin` - Worst-case optimal join implementation
- `TripleStore.SPARQL.CostOptimizer` - Cost-based plan selection
- `TripleStore.Query.PlanCache` - Optimized plan caching
- `TripleStore.SPARQL.Update` - SPARQL UPDATE execution
- `TripleStore.SPARQL.PropertyPath` - Property path evaluation
- `TripleStore.Transaction` - Write coordination
