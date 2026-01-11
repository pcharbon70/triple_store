# Working Plan: Section 3.4 - Graph-Specific Optimizations

## Branch: `feature/section-3.4-graph-specific-optimizations`

## Status: COMPLETED

## Overview

Section 3.4 implements graph-specific query optimizations for quad store queries,
enabling efficient query execution by optimizing pattern ordering, cross-graph
queries, and per-graph statistics.

---

## Part 1: Graph-First Pattern Ordering (3.4.1)

### Task 1.1: Update reorder_bgp_patterns/2 for quad patterns
- [x] Add quad pattern support to pattern reordering logic
- [x] Detect quad patterns vs triple patterns in BGP
- [x] Maintain backward compatibility for triple-only BGPs

**File:** `lib/triple_store/sparql/optimizer.ex` (or `executor.ex` if optimizer doesn't exist)

### Task 1.2: Prefer patterns with bound graph first
- [x] Give highest priority to patterns with bound graph (named or default)
- [x] Patterns with graph variable come after bound graph patterns
- [x] Triple patterns (implicit default graph) treated as bound graph

**File:** `lib/triple_store/sparql/optimizer.ex`

### Task 1.3: Use existing selectivity heuristics within graph
- [x] After graph-based ordering, apply existing selectivity ordering
- [x] Bound positions preferred over variables
- [x] Statistics-based cardinality estimation when available

**File:** `lib/triple_store/sparql/optimizer.ex`

### Task 1.4: Group patterns by graph when possible
- [x] Identify groups of patterns sharing same bound graph
- [x] Execute patterns within same graph together
- [x] Minimize graph context switches during execution

**File:** `lib/triple_store/sparql/optimizer.ex`

### Task 1.5: Add tests for ordering correctness
- [x] Test bound graph patterns ordered before graph variable patterns
- [x] Test triple patterns treated as default graph bound
- [x] Test patterns grouped by graph when multiple graphs present

**File:** `test/triple_store/sparql/graph_optimization_test.exs`

---

## Part 2: Cross-Graph Query Optimization (3.4.2)

### Task 2.1: Detect cross-graph patterns at optimization time
- [x] Analyze BGP to identify patterns spanning multiple graphs
- [x] Detect graph variables that may span multiple graphs
- [x] Distinguish single-graph vs multi-graph queries

**File:** `lib/triple_store/sparql/optimizer.ex`

### Task 2.2: Use SPOG/POSG indices for cross-graph patterns
- [x] When graph is variable, prefer SPOG index (subject-first)
- [x] When predicate is more selective, use POSG index
- [x] Update index selection logic for quad patterns

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.3: Minimize graph switches in execution plan
- [x] Reorder patterns to reduce graph context changes
- [x] Batch patterns by graph when possible
- [x] Document graph switch cost considerations

**File:** `lib/triple_store/sparql/optimizer.ex`

### Task 2.4: Cache graph lookups when iterating over graphs
- [x] In execute_with_graph_variable, cache graph list
- [x] Avoid repeated list_graphs calls during execution
- [x] Document caching behavior

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.5: Document cross-graph performance characteristics
- [x] Add documentation about cross-graph query performance
- [x] Document index selection for different pattern types
- [x] Add examples of optimized cross-graph queries

**File:** `lib/triple_store/sparql/executor.ex` (documentation)

---

## Part 3: Graph Predicate Statistics (3.4.3)

### Task 3.1: Extend statistics to track predicate counts per graph
- [x] Update statistics collection to track graph dimension
- [x] Store predicate counts keyed by graph ID
- [x] Maintain total counts and per-graph counts

**File:** `lib/triple_store/statistics.ex` (or equivalent)

### Task 3.2: Implement graph_statistics/2 returning per-graph stats
- [x] Add function to get statistics for specific graph
- [x] Return predicate counts, triple counts, etc. for graph
- [x] Handle default graph vs named graphs

**File:** `lib/triple_store/statistics.ex`

### Task 3.3: Use graph-specific stats for cardinality estimation
- [x] Update cardinality estimation to consider graph
- [x] When graph is bound, use per-graph statistics
- [x] When graph is variable, estimate across all graphs

**File:** `lib/triple_store/sparql/optimizer.ex`

### Task 3.4: Cache per-graph statistics in Statistics.Cache
- [x] Extend cache to store per-graph statistics
- [x] Invalidate cache on graph modifications
- [x] Implement cache lookup for graph stats

**File:** `lib/triple_store/statistics/cache.ex` (or equivalent)

### Task 3.5: Update statistics on graph modifications
- [x] Hook statistics updates into quad insert/delete
- [x] Update per-graph counts on graph operations
- [x] Handle graph deletion, copy operations

**File:** `lib/triple_store/quad_operations.ex`

---

## Part 4: Unit Tests

### Task 4.1: Graph-first ordering tests
- [x] Test bound graph patterns ordered first
- [x] Test triple patterns treated as default graph
- [x] Test patterns grouped by graph

**File:** `test/triple_store/sparql/graph_optimization_test.exs`

### Task 4.2: Cross-graph optimization tests
- [x] Test single-graph query uses GSPO
- [x] Test cross-graph query uses SPOG/POSG
- [x] Test graph variable optimization

**File:** `test/triple_store/sparql/graph_optimization_test.exs`

### Task 4.3: Statistics tests
- [x] Test per-graph statistics collection
- [x] Test graph_statistics/2 returns correct data
- [x] Test statistics updates on graph modifications

**File:** `test/triple_store/statistics/graph_statistics_test.exs`

---

## Summary

**Status: IN PROGRESS**

This section implements graph-specific optimizations:
1. [ ] Graph-first pattern ordering for quad BGPs
2. [ ] Cross-graph query optimization
3. [ ] Graph predicate statistics collection

## Implementation Notes

### Pattern Ordering Heuristics

For quad BGP patterns, ordering priority (highest to lowest):
1. Bound graph + 2+ bound positions (GSPO/GPOS index scan)
2. Bound graph + 1 bound position (prefix scan)
3. Graph variable + 2+ bound positions (SPOG/POSG index scan)
4. Graph variable + 1 bound position (cross-graph scan)
5. All variables (full scan with graph filtering)

### Index Selection

- **GSPO**: When graph is bound AND subject is bound
- **GPOS**: When graph is bound AND predicate is bound
- **SPOG**: When subject is bound AND graph may vary
- **POSG**: When predicate is bound AND graph may vary

---

## Next Steps

After this section, Phase 3.5 (Solution Modifier Adaptation) will add support for
graph variables in SELECT, GROUP BY, ORDER BY, and CONSTRUCT clauses.

---

## Key Files Modified

1. `lib/triple_store/sparql/optimizer.ex` - Pattern ordering optimization
2. `lib/triple_store/sparql/executor.ex` - Index selection for cross-graph
3. `lib/triple_store/statistics.ex` - Per-graph statistics
4. `test/triple_store/sparql/graph_optimization_test.exs` - New test file
5. `test/triple_store/statistics/graph_statistics_test.exs` - New test file
