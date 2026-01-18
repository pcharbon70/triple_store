# Working Plan: Section 3.2 - GRAPH Clause Execution

## Branch: `feature/section-3.2-graph-clause-execution`

## Status: COMPLETED

## Overview

Section 3.2 implements execution of GRAPH clauses in SPARQL queries. The GRAPH clause
allows queries to be scoped to specific named graphs or to iterate over all graphs
using a graph variable.

---

## Part 1: GRAPH Algebra Node Handler (3.2.1)

### Task 1.1: Add execute_graph/3 to Executor
- [x] Add `execute_graph/3` function to handle `{:graph, graph_spec, pattern}`
- [x] Handle `:default` graph spec (delegate to default graph execution)
- [x] Handle `{:iri, iri}` graph spec (named graph)
- [x] Handle `{:variable, var}` graph spec (graph variable)
- [x] Bind graph term to results when graph is variable

**File:** `lib/triple_store/sparql/executor.ex`

### Task 1.2: Add graph case to execute_pattern in Query
- [x] Add `{:graph, graph_spec, inner}` case to `execute_pattern/3`
- [x] Delegate to `Executor.execute_graph/3`
- [x] Handle errors from graph execution

**File:** `lib/triple_store/sparql/query.ex`

---

## Part 2: Named Graph Execution (3.2.2)

### Task 2.1: Implement execute_in_named_graph/4
- [x] Create function `execute_in_named_graph(ctx, pattern, graph_iri, initial_binding)`
- [x] Convert graph IRI to graph ID using dictionary
- [x] Convert inner triple patterns to quad patterns with bound graph
- [x] Execute converted patterns using existing BGP execution
- [x] Return stream of bindings with graph context

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.2: Use quad storage for graph-scoped queries
- [x] Use QuadOperations for quads lookup
- [x] Use GSPO/GPOS indices for graph-scoped access (via QuadIndex)
- [x] Ensure only quads from specified graph are returned

**File:** `lib/triple_store/sparql/executor.ex`

---

## Part 3: Graph Variable Execution (3.2.3)

### Task 3.1: Implement execute_with_graph_variable/4
- [x] Create function `execute_with_graph_variable(ctx, pattern, var_name, initial_binding)`
- [x] Get list of all graphs using QuadOperations.list_graphs/1
- [x] For each graph, execute pattern with graph bound to that graph
- [x] Bind graph variable to graph IRI in results
- [x] Stream results across all graphs

**File:** `lib/triple_store/sparql/executor.ex`

### Task 3.2: Handle empty graph list
- [x] Return empty stream when no graphs exist
- [x] Handle graphs that may not exist in store

**File:** `lib/triple_store/sparql/executor.ex`

---

## Part 4: Default Graph Execution (3.2.4)

### Task 4.1: Implement execute_in_default_graph/3
- [x] Create function `execute_in_default_graph(ctx, pattern, initial_binding)`
- [x] Use `@default_graph_id` (0) for graph position
- [x] Convert patterns to quad patterns with bound default graph
- [x] Query only quads with default graph ID
- [x] Exclude named graphs from results

**File:** `lib/triple_store/sparql/executor.ex`

---

## Part 5: Nested GRAPH Clauses (3.2.5)

### Task 5.1: Handle nested GRAPH patterns
- [x] Inner GRAPH clause filters outer graph results
- [x] Combine graph bindings appropriately
- [x] Recursive execution through execute_pattern

**File:** `lib/triple_store/sparql/query.ex` (via existing recursion)

---

## Part 6: Unit Tests

### Task 6.1: GRAPH clause tests
- [x] Test GRAPH <iri> { ... } executes in named graph
- [x] Test GRAPH ?g { ... } iterates over all graphs and binds ?g
- [x] Test GRAPH :default { ... } executes in default graph

**File:** `test/triple_store/sparql/graph_clause_test.exs`

### Task 6.2: Pattern conversion tests
- [x] Test triple patterns converted to quads with graph context
- [x] Test mixed patterns in GRAPH clause

**File:** `test/triple_store/sparql/graph_clause_test.exs`

### Task 6.3: Integration tests
- [x] Test GRAPH clause with other operators (JOIN, UNION, FILTER)
- [x] Test nested GRAPH clauses
- [x] Test GRAPH with empty pattern

**File:** `test/triple_store/sparql/graph_clause_test.exs`

---

## Summary

**Status: COMPLETED**

This section implements GRAPH clause execution for SPARQL queries:
1. [x] Execute queries scoped to specific named graphs
2. [x] Execute queries with graph as a variable (iterating over all graphs)
3. [x] Execute queries in default graph context
4. [x] Handle nested GRAPH clauses

## Test Results

```
test/triple_store/sparql/graph_clause_test.exs:8 tests, 0 failures
test/triple_store/sparql/quad_pattern_test.exs:27 tests, 0 failures
Total: 35 tests, 0 failures
```

## Next Steps

After this section, Phase 3.3 (Quad BGP Execution) will extend BGP execution
to handle quad patterns directly.

---

## Key Files to Modify

1. `lib/triple_store/sparql/executor.ex` - Graph execution functions
2. `lib/triple_store/sparql/query.ex` - Add graph case to execute_pattern
3. `test/triple_store/sparql/graph_clause_test.exs` - New test file
