# Phase 3: SPARQL Query Execution with Named Graphs

## Overview

Phase 3 implements SPARQL query execution support for named graphs. By the end of this phase, the query executor will handle GRAPH clauses, graph variables, and all graph-related SPARQL 1.1 features.

The parser already produces `:graph` algebra nodes. This phase focuses on the execution layer to process these nodes correctly against the quad storage.

---

## 3.1 Quad Pattern Representation

### 3.1.1 Pattern Type Extension

Extend pattern representation to include graph position.

- [ ] 3.1.1.1 Define quad pattern type: `quad_pattern() :: {pattern_s, pattern_p, pattern_o, pattern_g}`
- [ ] 3.1.1.2 Update `@type pattern :: triple_pattern() | quad_pattern()`
- [ ] 3.1.1.3 Each position is `:bound | {:var, atom()}`
- [ ] 3.1.1.4 Add `is_quad_pattern?/1` guard function
- [ ] 3.1.1.5 Add `is_triple_pattern?/1` guard function

**Updated Module:** `lib/triple_store/sparql/executor.ex`

### 3.1.2 Pattern Conversion

Convert algebra triples to quad patterns.

- [ ] 3.1.2.1 Implement `triple_pattern_to_quad/2` adding graph context
- [ ] 3.1.2.2 Handle default graph context (bind graph to default_graph_id)
- [ ] 3.1.2.3 Handle named graph context (bind graph to specific graph ID)
- [ ] 3.1.2.4 Handle graph variable context (graph becomes variable)
- [ ] 3.1.2.5 Preserve variable bindings through conversion

### 3.1.3 Variable Binding Extension

Extend variable binding to include graph variable.

- [ ] 3.1.3.1 Update binding type to include graph variable
- [ ] 3.1.3.2 Graph variables bind to graph term IRIs
- [ ] 3.1.3.3 Default graph never appears in bindings (implicit)
- [ ] 3.1.3.4 Add `binding_has_graph?/2` helper

---

## 3.2 GRAPH Clause Execution

### 3.2.1 GRAPH Algebra Node Handler

Implement execution of GRAPH algebra nodes.

- [ ] 3.2.1.1 Implement `execute_graph/3` for `{:graph, graph_spec, pattern}`
- [ ] 3.2.1.2 Handle `{:iri, iri}` graph spec (named graph)
- [ ] 3.2.1.3 Handle `{:var, var}` graph spec (graph variable)
- [ ] 3.2.1.4 Handle `:default` graph spec (default graph)
- [ ] 3.2.1.5 Bind graph term to results when graph is variable

### 3.2.2 Named Graph Execution

Execute queries scoped to a specific named graph.

- [ ] 3.2.2.1 Implement `execute_in_named_graph/4` with graph ID
- [ ] 3.2.2.2 Convert inner patterns to quad patterns with bound graph
- [ ] 3.2.2.3 Use GSPO/GPOS indices for graph-scoped access
- [ ] 3.2.2.4 Stream results with graph bound in all quads
- [ ] 3.2.2.5 Add telemetry for graph-scoped queries

### 3.2.3 Graph Variable Execution

Execute queries with graph as a variable.

- [ ] 3.2.3.1 Implement `execute_with_graph_variable/4`
- [ ] 3.2.3.2 Iterate over all graphs using `list_graphs/1`
- [ ] 3.2.3.3 For each graph, execute pattern with graph bound
- [ ] 3.2.3.4 Bind graph variable to graph IRI in results
- [ ] 3.2.3.5 Stream results across all graphs

### 3.2.4 Default Graph Execution

Execute queries in default graph context.

- [ ] 3.2.4.1 Implement `execute_in_default_graph/3`
- [ ] 3.2.4.2 Use @default_graph_id (0) for graph position
- [ ] 3.2.4.3 Convert patterns to quad patterns with bound default graph
- [ ] 3.2.4.4 Query only quads with default graph ID
- [ ] 3.2.4.5 Exclude named graphs from results

### 3.2.5 Nested GRAPH Clauses

Handle GRAPH clauses within GRAPH clauses.

- [ ] 3.2.5.1 Detect nested GRAPH patterns at parse time
- [ ] 3.2.5.2 Implement `execute_nested_graph/4`
- [ ] 3.2.5.3 Inner GRAPH clause filters outer graph results
- [ ] 3.2.5.4 Combine graph bindings appropriately
- [ ] 3.2.5.5 Return error for invalid nesting (if any)

---

## 3.3 Quad BGP Execution

### 3.3.1 BGP Pattern Extension

Extend BGP execution to handle quad patterns.

- [ ] 3.3.1.1 Update `execute_bgp/3` to accept quad patterns
- [ ] 3.3.1.2 Detect if BGP contains any quad patterns
- [ ] 3.3.1.3 For all-triple BGPs, use existing triple logic
- [ ] 3.3.1.4 For mixed patterns, convert all to quad patterns
- [ ] 3.3.1.5 Add `is_quad_bgp?/1` helper

### 3.3.2 Quad Pattern Execution

Implement execution of individual quad patterns.

- [ ] 3.3.2.1 Implement `execute_quad_pattern/5` for single pattern
- [ ] 3.3.2.2 Use `QuadIndex.select_index_for_quad/1` for index selection
- [ ] 3.3.2.3 Perform prefix scan based on pattern and index
- [ ] 3.3.2.4 Apply post-filtering if needed
- [ ] 3.3.2.5 Return stream of matching quads as bindings

### 3.3.3 Graph Binding in Joins

Handle graph variable in multi-pattern joins.

- [ ] 3.3.3.1 Ensure graph variable consistency across join
- [ ] 3.3.3.2 When graph bound, all patterns share same graph ID
- [ ] 3.3.3.3 When graph variable, cross-graph joining allowed
- [ ] 3.3.3.4 Optimize joins by grouping patterns with same graph
- [ ] 3.3.3.5 Add telemetry for cross-graph joins

### 3.3.4 Default Graph BGP

Execute BGP in implicit default graph context.

- [ ] 3.3.4.1 Detect default graph BGP (no GRAPH clause)
- [ ] 3.3.4.2 Convert all triple patterns to quad with default graph
- [ ] 3.3.4.3 Use @default_graph_id for all pattern graph positions
- [ ] 3.3.4.4 Query only default graph quads
- [ ] 3.3.4.5 Maintain backward compatibility with triple queries

---

## 3.4 Graph-Specific Optimizations

### 3.4.1 Graph-First Pattern Ordering

Optimize pattern ordering for graph-scoped queries.

- [ ] 3.4.1.1 Update `reorder_bgp_patterns/2` for quad patterns
- [ ] 3.4.1.2 Prefer patterns with bound graph first
- [ ] 3.4.1.3 Within same graph, use existing selectivity heuristics
- [ ] 3.4.1.4 Group patterns by graph when possible
- [ ] 3.4.1.5 Add tests for ordering correctness

### 3.4.2 Cross-Graph Query Optimization

Optimize queries that span multiple graphs.

- [ ] 3.4.2.1 Detect cross-graph patterns at optimization time
- [ ] 3.4.2.2 Use SPOG/POSG indices for cross-graph patterns
- [ ] 3.4.2.3 Minimize graph switches in execution plan
- [ ] 3.4.2.4 Cache graph lookups when iterating over graphs
- [ ] 3.4.2.5 Document cross-graph performance characteristics

### 3.4.3 Graph Predicate Statistics

Collect statistics per graph for optimization.

- [ ] 3.4.3.1 Extend statistics to track predicate counts per graph
- [ ] 3.4.3.2 Implement `graph_statistics/2` returning per-graph stats
- [ ] 3.4.3.3 Use graph-specific stats for cardinality estimation
- [ ] 3.4.3.4 Cache per-graph statistics in Statistics.Cache
- [ ] 3.4.3.5 Update statistics on graph modifications

---

## 3.5 Solution Modifier Adaptation

### 3.5.1 Projection with Graph

Handle projection of graph variable.

- [ ] 3.5.1.1 Update `execute_project/3` to include graph variable
- [ ] 3.5.1.2 When graph variable projected, include in results
- [ ] 3.5.1.3 When graph not projected, exclude from results
- [ ] 3.5.1.4 Handle SELECT * with graph variable
- [ ] 3.5.1.5 Update `all_variables/1` to detect graph variables

### 3.5.2 GROUP BY with Graph

Handle GROUP BY with graph variable.

- [ ] 3.5.2.1 Update `execute_group/3` to group by graph variable
- [ ] 3.5.2.2 Group results by graph IRI when graph in GROUP BY
- [ ] 3.5.2.3 Allow aggregates over graph groups
- [ ] 3.5.2.4 Test grouping by graph produces correct groups

### 3.5.3 ORDER BY with Graph

Handle ORDER BY with graph variable.

- [ ] 3.5.3.1 Update `execute_order_by/3` to sort by graph variable
- [ ] 3.5.3.2 Define graph ordering (IRI lexical)
- [ ] 3.5.3.3 Default graph sorts first (or last, define semantics)
- [ ] 3.5.3.4 Test ordering by graph produces correct order

---

## 3.6 Query Results Serialization

### 3.6.1 Graph Variable in Results

Include graph variable in serialized results.

- [ ] 3.6.1.1 Update `to_select_results/2` to include graph bindings
- [ ] 3.6.1.2 Graph IRI serialized as standard RDF term
- [ ] 3.6.1.3 Default graph not included (implicit)
- [ ] 3.6.1.4 Handle CONSTRUCT with graph context

### 3.6.2 CONSTRUCT with Graph

Handle CONSTRUCT queries over named graphs.

- [ ] 3.6.2.1 Implement CONSTRUCT for named graph queries
- [ ] 3.6.2.2 Constructed triples inherit graph from source
- [ ] 3.6.2.3 Return RDF.Dataset instead of RDF.Graph
- [ ] 3.6.2.4 Test CONSTRUCT from single named graph
- [ ] 3.6.2.5 Test CONSTRUCT from multiple graphs

---

## 3.7 Unit Tests

### 3.7.1 Pattern Tests

- [ ] 3.7.1.1 Test triple pattern converts to quad with default graph
- [ ] 3.7.1.2 Test quad pattern preserves graph binding
- [ ] 3.7.1.3 Test quad pattern with graph variable
- [ ] 3.7.1.4 Test is_quad_pattern? detects quad patterns
- [ ] 3.7.1.5 Test is_triple_pattern? detects triple patterns

### 3.7.2 GRAPH Clause Tests

- [ ] 3.7.2.1 Test GRAPH <iri> { ... } queries named graph
- [ ] 3.7.2.2 Test GRAPH ?g { ... } binds graph variable
- [ ] 3.7.2.3 Test GRAPH :default { ... } queries default graph
- [ ] 3.7.2.4 Test GRAPH clause filters results to graph
- [ ] 3.7.2.5 Test GRAPH clause with empty pattern
- [ ] 3.7.2.6 Test nested GRAPH clauses

### 3.7.3 Quad BGP Tests

- [ ] 3.7.3.1 Test BGP with all triple patterns uses default graph
- [ ] 3.7.3.2 Test BGP with quad patterns queries specified graphs
- [ ] 3.7.3.3 Test BGP with mixed patterns works correctly
- [ ] 3.7.3.4 Test BGP with graph variable queries all graphs
- [ ] 3.7.3.5 Test BGP joins respect graph binding

### 3.7.4 Cross-Graph Tests

- [ ] 3.7.4.1 Test query spanning multiple graphs
- [ ] 3.7.4.2 Test pattern with graph variable joins correctly
- [ ] 3.7.4.3 Test UNION of GRAPH clauses
- [ ] 3.7.4.4 Test OPTIONAL with graph context
- [ ] 3.7.4.5 Test FILTER with graph variable

### 3.7.5 Optimization Tests

- [ ] 3.7.5.1 Test pattern ordering prefers bound graph
- [ ] 3.7.5.2 Test graph-scoped query uses GSPO index
- [ ] 3.7.5.3 Test cross-graph query uses SPOG/POSG index
- [ ] 3.7.5.4 Test per-graph statistics used for cardinality
- [ ] 3.7.5.5 Test cross-graph join optimization

### 3.7.6 Solution Modifier Tests

- [ ] 3.7.6.1 Test SELECT with graph variable
- [ ] 3.7.6.2 Test SELECT * includes graph variable
- [ ] 3.7.6.3 Test GROUP BY with graph variable
- [ ] 3.7.6.4 Test ORDER BY with graph variable
- [ ] 3.7.6.5 Test CONSTRUCT returns RDF.Dataset

### 3.7.7 Serialization Tests

- [ ] 3.7.7.1 Test SELECT results include graph binding
- [ ] 3.7.7.2 Test ASK with graph context works
- [ ] 3.7.7.3 Test CONSTRUCT from named graph
- [ ] 3.7.7.4 Test DESCRIBE with graph context

---

## Success Criteria

1. **GRAPH Clause**: All GRAPH clause patterns execute correctly
2. **Graph Variable**: Graph variable binds to correct graph IRIs
3. **Default Graph**: Queries without GRAPH use default graph
4. **Cross-Graph**: Queries can span multiple graphs correctly
5. **Backward Compatible**: Triple-only queries still work on quad store
6. **Performance**: Graph-scoped queries use optimal indices

## Provides Foundation

This phase establishes the infrastructure for:
- **Phase 4**: SPARQL UPDATE with named graphs
- **Phase 5**: Quad-aware statistics and optimization
- **Phase 6**: Integration testing of complete quad functionality

## Key Outputs

- Updated `TripleStore.SPARQL.Executor` with GRAPH clause execution
- Quad pattern execution support
- Graph variable binding in query results
- Graph-specific query optimizations
