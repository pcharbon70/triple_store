# Phase 2: SPARQL Query Engine

## Overview

Phase 2 builds the SPARQL query engine on top of the storage foundation. By the end of this phase, we will have a complete SPARQL 1.1 query processor supporting SELECT, CONSTRUCT, ASK, and DESCRIBE query forms with full algebra operations including JOIN, OPTIONAL, UNION, FILTER, and solution modifiers.

The parser is implemented as a Rustler NIF wrapping the `spargebra` crate from the Oxigraph project, providing battle-tested SPARQL grammar support. The algebra compiler and query executor are implemented in pure Elixir to maintain preemptibility and leverage OTP patterns.

Query execution uses iterator-based lazy evaluation with Elixir Streams, providing natural backpressure and memory-efficient processing of large result sets.

---

## 2.1 SPARQL Parser NIF

- [ ] **Section 2.1 Complete**

This section implements the SPARQL parser as a Rustler NIF using the spargebra crate. The parser returns an Elixir-native AST representation that the algebra compiler can process.

### 2.1.1 Parser Crate Setup

- [x] **Task 2.1.1 Complete**

Set up the Rust crate for SPARQL parsing with spargebra integration.

- [x] 2.1.1.1 Add `spargebra = "0.3"` to `native/sparql_parser_nif/Cargo.toml`
- [x] 2.1.1.2 Create `TripleStore.SPARQL.Parser` module with NIF bindings
- [x] 2.1.1.3 Define Elixir AST representation using tuples/atoms (not structs)
- [x] 2.1.1.4 Implement AST conversion from spargebra types to Elixir terms

### 2.1.2 Query Parsing

- [x] **Task 2.1.2 Complete**

Implement query parsing for all SPARQL query forms.

- [x] 2.1.2.1 Implement `parse_query(sparql)` -> `{:ok, ast}` or `{:error, reason}`
- [x] 2.1.2.2 Support SELECT queries with all projection forms
- [x] 2.1.2.3 Support CONSTRUCT queries with template patterns
- [x] 2.1.2.4 Support ASK queries for boolean results
- [x] 2.1.2.5 Support DESCRIBE queries with IRI expansion

### 2.1.3 Update Parsing

- [ ] **Task 2.1.3 Complete**

Implement SPARQL UPDATE parsing (used in Phase 3).

- [ ] 2.1.3.1 Implement `parse_update(sparql)` -> `{:ok, ast}` or `{:error, reason}`
- [ ] 2.1.3.2 Support INSERT DATA operations
- [ ] 2.1.3.3 Support DELETE DATA operations
- [ ] 2.1.3.4 Support DELETE WHERE / INSERT WHERE operations
- [ ] 2.1.3.5 Support LOAD and CLEAR operations

### 2.1.4 Error Handling

- [ ] **Task 2.1.4 Complete**

Implement comprehensive error handling with informative messages.

- [ ] 2.1.4.1 Return parse position (line, column) on syntax errors
- [ ] 2.1.4.2 Provide descriptive error messages for common mistakes
- [ ] 2.1.4.3 Handle prefix resolution errors
- [ ] 2.1.4.4 Validate variable scoping

### 2.1.5 Unit Tests

- [ ] **Task 2.1.5 Complete**

- [ ] Test simple SELECT query parsing
- [ ] Test SELECT with multiple variables
- [ ] Test SELECT with WHERE clause
- [ ] Test SELECT with FILTER
- [ ] Test SELECT with OPTIONAL
- [ ] Test SELECT with UNION
- [ ] Test SELECT with ORDER BY, LIMIT, OFFSET
- [ ] Test CONSTRUCT query parsing
- [ ] Test ASK query parsing
- [ ] Test DESCRIBE query parsing
- [ ] Test INSERT DATA parsing
- [ ] Test DELETE WHERE parsing
- [ ] Test syntax error reporting with position
- [ ] Test prefix expansion

---

## 2.2 SPARQL Algebra

- [ ] **Section 2.2 Complete**

This section implements the SPARQL algebra representation and compilation from parsed AST. The algebra forms the intermediate representation between parsing and execution, enabling optimization passes.

### 2.2.1 Algebra Node Types

- [ ] **Task 2.2.1 Complete**

Define the algebra node types representing SPARQL operations.

- [ ] 2.2.1.1 Define `%Algebra{type, children, metadata}` base struct
- [ ] 2.2.1.2 Implement `:bgp` node for Basic Graph Patterns
- [ ] 2.2.1.3 Implement `:join` node for inner joins
- [ ] 2.2.1.4 Implement `:left_join` node for OPTIONAL
- [ ] 2.2.1.5 Implement `:union` node for UNION
- [ ] 2.2.1.6 Implement `:filter` node with expression metadata
- [ ] 2.2.1.7 Implement `:extend` node for BIND
- [ ] 2.2.1.8 Implement `:project` node for variable projection
- [ ] 2.2.1.9 Implement `:distinct` and `:reduced` nodes
- [ ] 2.2.1.10 Implement `:order`, `:slice` nodes for solution modifiers

### 2.2.2 AST to Algebra Compilation

- [ ] **Task 2.2.2 Complete**

Implement the compiler transforming parsed AST to algebra trees.

- [ ] 2.2.2.1 Implement `Algebra.from_ast(ast)` main entry point
- [ ] 2.2.2.2 Implement WHERE clause compilation to BGP/join tree
- [ ] 2.2.2.3 Implement solution modifier wrapping (ORDER, LIMIT, OFFSET)
- [ ] 2.2.2.4 Implement projection for SELECT variables
- [ ] 2.2.2.5 Handle SELECT * expansion to all in-scope variables

### 2.2.3 Expression Compilation

- [ ] **Task 2.2.3 Complete**

Compile SPARQL expressions (FILTER conditions, BIND expressions) to evaluable form.

- [ ] 2.2.3.1 Implement arithmetic expressions (+, -, *, /)
- [ ] 2.2.3.2 Implement comparison expressions (=, !=, <, >, <=, >=)
- [ ] 2.2.3.3 Implement logical expressions (&&, ||, !)
- [ ] 2.2.3.4 Implement built-in functions (STR, LANG, DATATYPE, BOUND, etc.)
- [ ] 2.2.3.5 Implement aggregate functions (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT)

### 2.2.4 Unit Tests

- [ ] **Task 2.2.4 Complete**

- [ ] Test BGP algebra node creation
- [ ] Test join algebra from multiple patterns
- [ ] Test left_join algebra from OPTIONAL
- [ ] Test union algebra from UNION
- [ ] Test filter algebra with expression
- [ ] Test complete algebra tree for complex query
- [ ] Test expression compilation for all operators
- [ ] Test aggregate expression compilation

---

## 2.3 Query Optimization

- [ ] **Section 2.3 Complete**

This section implements rule-based query optimization transforms on the algebra tree. Optimizations include filter pushing, constant folding, and join reordering based on selectivity estimates.

### 2.3.1 Filter Push-Down

- [ ] **Task 2.3.1 Complete**

Push filter expressions as close to their data sources as possible to reduce intermediate result sizes.

- [ ] 2.3.1.1 Implement `push_filters_down(algebra)` transformation
- [ ] 2.3.1.2 Push filters past joins when variables allow
- [ ] 2.3.1.3 Split conjunctive filters and push independently
- [ ] 2.3.1.4 Preserve filter semantics across OPTIONAL boundaries

### 2.3.2 Constant Folding

- [ ] **Task 2.3.2 Complete**

Evaluate constant expressions at compile time.

- [ ] 2.3.2.1 Implement `fold_constants(algebra)` transformation
- [ ] 2.3.2.2 Evaluate arithmetic on constant operands
- [ ] 2.3.2.3 Evaluate comparisons on constant operands
- [ ] 2.3.2.4 Simplify always-true/always-false filters

### 2.3.3 BGP Reordering

- [ ] **Task 2.3.3 Complete**

Reorder BGP patterns based on selectivity estimates to reduce intermediate results.

- [ ] 2.3.3.1 Implement `reorder_bgp_patterns(bgp, stats)` transformation
- [ ] 2.3.3.2 Use predicate cardinalities from statistics
- [ ] 2.3.3.3 Prefer patterns with bound subjects/objects
- [ ] 2.3.3.4 Consider variable binding propagation between patterns

### 2.3.4 Optimizer Pipeline

- [ ] **Task 2.3.4 Complete**

Combine optimizations into a pipeline applied to all queries.

- [ ] 2.3.4.1 Implement `Optimizer.optimize(algebra, stats)` entry point
- [ ] 2.3.4.2 Apply optimizations in optimal order
- [ ] 2.3.4.3 Add optimization logging for debugging
- [ ] 2.3.4.4 Handle optimization bypass for EXPLAIN queries

### 2.3.5 Unit Tests

- [ ] **Task 2.3.5 Complete**

- [ ] Test filter push-down past join
- [ ] Test filter push-down stops at OPTIONAL
- [ ] Test conjunctive filter splitting
- [ ] Test constant folding for arithmetic
- [ ] Test constant folding for comparisons
- [ ] Test BGP reordering prefers selective patterns
- [ ] Test optimizer pipeline produces valid algebra

---

## 2.4 Iterator Execution

- [ ] **Section 2.4 Complete**

This section implements the query executor using iterator-based lazy evaluation. Each algebra node type has a corresponding execution strategy that produces a Stream of bindings.

### 2.4.1 BGP Execution

- [ ] **Task 2.4.1 Complete**

Execute Basic Graph Patterns using index nested loop join.

- [ ] 2.4.1.1 Implement `execute_bgp(db, patterns)` returning `Stream.t(bindings)`
- [ ] 2.4.1.2 Order patterns by selectivity before execution
- [ ] 2.4.1.3 Implement variable substitution for bound variables
- [ ] 2.4.1.4 Implement binding extension for matched variables
- [ ] 2.4.1.5 Handle empty pattern (returns single empty binding)

### 2.4.2 Join Execution

- [ ] **Task 2.4.2 Complete**

Execute join operations between result streams.

- [ ] 2.4.2.1 Implement nested loop join for small inputs
- [ ] 2.4.2.2 Implement hash join for larger inputs
- [ ] 2.4.2.3 Implement left outer join for OPTIONAL semantics
- [ ] 2.4.2.4 Handle compatible binding merge

### 2.4.3 Union Execution

- [ ] **Task 2.4.3 Complete**

Execute UNION as concatenation of result streams.

- [ ] 2.4.3.1 Implement `execute_union(db, left, right)` concatenating streams
- [ ] 2.4.3.2 Handle variable alignment across branches
- [ ] 2.4.3.3 Preserve ordering within branches

### 2.4.4 Filter Execution

- [ ] **Task 2.4.4 Complete**

Execute filter expressions against bindings.

- [ ] 2.4.4.1 Implement `execute_filter(stream, expression)` filtering stream
- [ ] 2.4.4.2 Implement expression evaluator for all operators
- [ ] 2.4.4.3 Handle three-valued logic (true, false, error)
- [ ] 2.4.4.4 Implement built-in function evaluation

### 2.4.5 Solution Modifiers

- [ ] **Task 2.4.5 Complete**

Execute solution modifiers (projection, ordering, slicing).

- [ ] 2.4.5.1 Implement `execute_project(stream, vars)` selecting variables
- [ ] 2.4.5.2 Implement `execute_distinct(stream)` removing duplicates
- [ ] 2.4.5.3 Implement `execute_order(stream, comparators)` sorting results
- [ ] 2.4.5.4 Implement `execute_slice(stream, offset, limit)` pagination

### 2.4.6 Result Serialization

- [ ] **Task 2.4.6 Complete**

Serialize execution results to final output format.

- [ ] 2.4.6.1 Implement SELECT result as list of binding maps
- [ ] 2.4.6.2 Implement CONSTRUCT result as `RDF.Graph`
- [ ] 2.4.6.3 Implement ASK result as boolean
- [ ] 2.4.6.4 Implement DESCRIBE result as `RDF.Graph` with CBD

### 2.4.7 Unit Tests

- [ ] **Task 2.4.7 Complete**

- [ ] Test BGP execution with single pattern
- [ ] Test BGP execution with multiple patterns
- [ ] Test BGP with bound variable substitution
- [ ] Test nested loop join produces correct results
- [ ] Test hash join produces same results as nested loop
- [ ] Test left outer join preserves unmatched left rows
- [ ] Test union concatenates both branches
- [ ] Test filter with comparison expressions
- [ ] Test filter with logical expressions
- [ ] Test filter with built-in functions
- [ ] Test projection selects correct variables
- [ ] Test distinct removes duplicates
- [ ] Test order by sorts correctly
- [ ] Test slice applies offset and limit

---

## 2.5 Query API

- [ ] **Section 2.5 Complete**

This section implements the public query API providing a clean interface for executing SPARQL queries against the triple store.

### 2.5.1 Query Function

- [ ] **Task 2.5.1 Complete**

Implement the main query entry point.

- [ ] 2.5.1.1 Implement `TripleStore.query(db, sparql)` returning results
- [ ] 2.5.1.2 Implement `TripleStore.query(db, sparql, opts)` with options
- [ ] 2.5.1.3 Support timeout option for long-running queries
- [ ] 2.5.1.4 Support explain option for query plan inspection

### 2.5.2 Streaming Results

- [ ] **Task 2.5.2 Complete**

Support streaming query results for large result sets.

- [ ] 2.5.2.1 Implement `TripleStore.stream_query(db, sparql)` returning Stream
- [ ] 2.5.2.2 Support backpressure-aware consumption
- [ ] 2.5.2.3 Handle early termination cleanly

### 2.5.3 Prepared Queries

- [ ] **Task 2.5.3 Complete**

Support prepared queries with parameter binding.

- [ ] 2.5.3.1 Implement `TripleStore.prepare_query(sparql)` returning prepared query
- [ ] 2.5.3.2 Implement `TripleStore.execute(db, prepared, params)` with bindings
- [ ] 2.5.3.3 Cache parsed/optimized algebra for prepared queries

### 2.5.4 Unit Tests

- [ ] **Task 2.5.4 Complete**

- [ ] Test query returns correct results
- [ ] Test query with timeout terminates
- [ ] Test streaming query produces all results
- [ ] Test streaming query handles early termination
- [ ] Test prepared query with parameter binding
- [ ] Test prepared query caching

---

## 2.6 Aggregation

- [ ] **Section 2.6 Complete**

This section implements GROUP BY and aggregate functions following SPARQL 1.1 semantics.

### 2.6.1 Group By Execution

- [ ] **Task 2.6.1 Complete**

Implement grouping of solutions by key variables.

- [ ] 2.6.1.1 Implement `execute_group(stream, group_vars)` partitioning by keys
- [ ] 2.6.1.2 Handle implicit grouping (no GROUP BY with aggregates)
- [ ] 2.6.1.3 Support HAVING clause filtering

### 2.6.2 Aggregate Functions

- [ ] **Task 2.6.2 Complete**

Implement standard aggregate functions.

- [ ] 2.6.2.1 Implement `COUNT(*)` and `COUNT(expr)`
- [ ] 2.6.2.2 Implement `SUM(expr)` with type coercion
- [ ] 2.6.2.3 Implement `AVG(expr)` with type coercion
- [ ] 2.6.2.4 Implement `MIN(expr)` and `MAX(expr)`
- [ ] 2.6.2.5 Implement `GROUP_CONCAT(expr; separator=...)`
- [ ] 2.6.2.6 Implement `SAMPLE(expr)` returning arbitrary value
- [ ] 2.6.2.7 Support DISTINCT modifier for aggregates

### 2.6.3 Unit Tests

- [ ] **Task 2.6.3 Complete**

- [ ] Test GROUP BY single variable
- [ ] Test GROUP BY multiple variables
- [ ] Test COUNT aggregate
- [ ] Test SUM aggregate with numeric values
- [ ] Test AVG aggregate
- [ ] Test MIN/MAX aggregates
- [ ] Test GROUP_CONCAT with separator
- [ ] Test DISTINCT within aggregate
- [ ] Test HAVING clause filtering

---

## 2.7 Phase 2 Integration Tests

- [ ] **Section 2.7 Complete**

Integration tests validate the complete query pipeline from SPARQL string through execution to results.

### 2.7.1 Query Pipeline Testing

- [ ] **Task 2.7.1 Complete**

Test complete query pipelines for various query shapes.

- [ ] 2.7.1.1 Test simple SELECT with single pattern
- [ ] 2.7.1.2 Test SELECT with multiple joined patterns (star query)
- [ ] 2.7.1.3 Test SELECT with OPTIONAL producing nulls
- [ ] 2.7.1.4 Test SELECT with UNION combining branches
- [ ] 2.7.1.5 Test SELECT with complex FILTER expressions
- [ ] 2.7.1.6 Test SELECT with ORDER BY, LIMIT, OFFSET

### 2.7.2 Construct/Ask/Describe Testing

- [ ] **Task 2.7.2 Complete**

Test non-SELECT query forms.

- [ ] 2.7.2.1 Test CONSTRUCT produces valid RDF graph
- [ ] 2.7.2.2 Test ASK returns true when matches exist
- [ ] 2.7.2.3 Test ASK returns false when no matches
- [ ] 2.7.2.4 Test DESCRIBE produces CBD for resource

### 2.7.3 Aggregation Testing

- [ ] **Task 2.7.3 Complete**

Test aggregate queries with grouping.

- [ ] 2.7.3.1 Test GROUP BY with COUNT
- [ ] 2.7.3.2 Test GROUP BY with SUM/AVG
- [ ] 2.7.3.3 Test GROUP BY with HAVING filter
- [ ] 2.7.3.4 Test implicit grouping with single aggregate

### 2.7.4 Performance Benchmarking

- [ ] **Task 2.7.4 Complete**

Benchmark query performance on test datasets.

- [ ] 2.7.4.1 Benchmark simple BGP query: target <10ms on 1M triples
- [ ] 2.7.4.2 Benchmark star query (5 patterns): target <100ms on 1M triples
- [ ] 2.7.4.3 Benchmark OPTIONAL query: measure overhead vs inner join
- [ ] 2.7.4.4 Benchmark aggregation: measure grouping cost

---

## Success Criteria

1. **Parser**: All SPARQL 1.1 query forms parse correctly
2. **Algebra**: Correct transformation from AST to algebra
3. **Optimization**: Filter push-down and BGP reordering work correctly
4. **Execution**: All algebra operations execute with correct semantics
5. **Performance**: Simple BGP <10ms on 1M triples
6. **Aggregation**: Full GROUP BY and aggregate function support

## Provides Foundation

This phase establishes the infrastructure for:
- **Phase 3**: Leapfrog Triejoin integrates with executor; UPDATE uses same algebra compilation
- **Phase 4**: Reasoner uses BGP execution for rule body evaluation
- **Phase 5**: Query caching and telemetry hook into execution pipeline

## Key Outputs

- `TripleStore.SPARQL.Parser` - NIF-based SPARQL parser
- `TripleStore.SPARQL.Algebra` - Algebra tree representation
- `TripleStore.SPARQL.Optimizer` - Query optimization transforms
- `TripleStore.SPARQL.Executor` - Iterator-based query execution
- `TripleStore.query/2` - Public query API
