# Phase 2: SPARQL Query Engine

## Overview

Phase 2 builds the SPARQL query engine on top of the storage foundation. By the end of this phase, we will have a complete SPARQL 1.1 query processor supporting SELECT, CONSTRUCT, ASK, and DESCRIBE query forms with full algebra operations including JOIN, OPTIONAL, UNION, FILTER, and solution modifiers.

The parser is implemented as a Rustler NIF wrapping the `spargebra` crate from the Oxigraph project, providing battle-tested SPARQL grammar support. The algebra compiler and query executor are implemented in pure Elixir to maintain preemptibility and leverage OTP patterns.

Query execution uses iterator-based lazy evaluation with Elixir Streams, providing natural backpressure and memory-efficient processing of large result sets.

---

## 2.1 SPARQL Parser NIF

- [x] **Section 2.1 Complete** (including review fixes)

This section implements the SPARQL parser as a Rustler NIF using the spargebra crate. The parser returns an Elixir-native AST representation that the algebra compiler can process.

**Review fixes applied (2025-12-23):**
- Added DirtyCpu scheduler annotations for NIF functions
- Fixed property path atoms (use Elixir atoms instead of strings)
- Added refactoring macros to reduce Rust code duplication
- Added input size limits (1MB) to prevent DoS attacks
- Added catch-all clauses to helper functions
- Added 24 new tests for BIND, MINUS, EXISTS, IN/NOT IN, GRAPH, aggregates
- Total tests: 191 (up from 167)

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

- [x] **Task 2.1.3 Complete**

Implement SPARQL UPDATE parsing (used in Phase 3).

- [x] 2.1.3.1 Implement `parse_update(sparql)` -> `{:ok, ast}` or `{:error, reason}`
- [x] 2.1.3.2 Support INSERT DATA operations
- [x] 2.1.3.3 Support DELETE DATA operations
- [x] 2.1.3.4 Support DELETE WHERE / INSERT WHERE operations
- [x] 2.1.3.5 Support LOAD and CLEAR operations

### 2.1.4 Error Handling

- [x] **Task 2.1.4 Complete**

Implement comprehensive error handling with informative messages.

- [x] 2.1.4.1 Return parse position (line, column) on syntax errors
- [x] 2.1.4.2 Provide descriptive error messages for common mistakes
- [x] 2.1.4.3 Handle prefix resolution errors
- [x] 2.1.4.4 Validate variable scoping

### 2.1.5 Unit Tests

- [x] **Task 2.1.5 Complete**

- [x] Test simple SELECT query parsing
- [x] Test SELECT with multiple variables
- [x] Test SELECT with WHERE clause
- [x] Test SELECT with FILTER
- [x] Test SELECT with OPTIONAL
- [x] Test SELECT with UNION
- [x] Test SELECT with ORDER BY, LIMIT, OFFSET
- [x] Test CONSTRUCT query parsing
- [x] Test ASK query parsing
- [x] Test DESCRIBE query parsing
- [x] Test INSERT DATA parsing
- [x] Test DELETE WHERE parsing
- [x] Test syntax error reporting with position
- [x] Test prefix expansion

---

## 2.2 SPARQL Algebra

- [x] **Section 2.2 Complete** (2025-12-23)

This section implements the SPARQL algebra representation and compilation from parsed AST. The algebra forms the intermediate representation between parsing and execution, enabling optimization passes.

### 2.2.1 Algebra Node Types

- [x] **Task 2.2.1 Complete** (2025-12-23)

Define the algebra node types representing SPARQL operations.

- [x] 2.2.1.1 Define tuple-based algebra nodes (using tagged tuples instead of struct)
- [x] 2.2.1.2 Implement `:bgp` node for Basic Graph Patterns
- [x] 2.2.1.3 Implement `:join` node for inner joins
- [x] 2.2.1.4 Implement `:left_join` node for OPTIONAL
- [x] 2.2.1.5 Implement `:union` node for UNION
- [x] 2.2.1.6 Implement `:filter` node with expression metadata
- [x] 2.2.1.7 Implement `:extend` node for BIND
- [x] 2.2.1.8 Implement `:project` node for variable projection
- [x] 2.2.1.9 Implement `:distinct` and `:reduced` nodes
- [x] 2.2.1.10 Implement `:order_by`, `:slice` nodes for solution modifiers

Additional nodes implemented: `:minus`, `:group`, `:values`, `:graph`, `:service`, `:path`

Test coverage: 72 tests in algebra_test.exs

### 2.2.2 AST to Algebra Compilation

- [x] **Task 2.2.2 Complete** (2025-12-23)

Implement the compiler transforming parsed AST to algebra trees.

- [x] 2.2.2.1 Implement `Algebra.from_ast(ast)` main entry point
- [x] 2.2.2.2 Implement WHERE clause compilation to BGP/join tree (handled by parser)
- [x] 2.2.2.3 Implement solution modifier wrapping (ORDER, LIMIT, OFFSET) (handled by parser)
- [x] 2.2.2.4 Implement projection for SELECT variables (handled by parser)
- [x] 2.2.2.5 Handle SELECT * expansion to all in-scope variables (handled by parser)

Note: The spargebra parser already produces algebra-like nodes, so compilation
focuses on normalization, validation, and analysis functions. Added helper
functions: `extract_pattern/1`, `result_variables/1`, `collect_bgps/1`,
`triple_count/1`, `has_optional?/1`, `has_union?/1`, `has_filter?/1`,
`has_aggregation?/1`, `collect_filters/1`.

Test coverage: 107 tests in algebra_test.exs (35 new for 2.2.2)

### 2.2.3 Expression Compilation

- [x] **Task 2.2.3 Complete** (2025-12-23)

Compile SPARQL expressions (FILTER conditions, BIND expressions) to evaluable form.

- [x] 2.2.3.1 Implement arithmetic expressions (+, -, *, /)
- [x] 2.2.3.2 Implement comparison expressions (=, !=, <, >, <=, >=)
- [x] 2.2.3.3 Implement logical expressions (&&, ||, !)
- [x] 2.2.3.4 Implement built-in functions (STR, LANG, DATATYPE, BOUND, etc.)
- [x] 2.2.3.5 Implement aggregate functions (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT)

Created `lib/triple_store/sparql/expression.ex` with full expression evaluation support
including all SPARQL 1.1 built-in functions. Test coverage: 112 tests in expression_test.exs.

### 2.2.4 Unit Tests

- [x] **Task 2.2.4 Complete** (2025-12-23)

Tests are included in each task's implementation:
- [x] Test BGP algebra node creation (in 2.2.1)
- [x] Test join algebra from multiple patterns (in 2.2.1)
- [x] Test left_join algebra from OPTIONAL (in 2.2.1)
- [x] Test union algebra from UNION (in 2.2.1)
- [x] Test filter algebra with expression (in 2.2.1)
- [x] Test complete algebra tree for complex query (in 2.2.1/2.2.2)
- [x] Test expression compilation for all operators (in 2.2.3)
- [x] Test aggregate expression compilation (in 2.2.3)

Total algebra test coverage: 107 tests (algebra_test.exs) + 112 tests (expression_test.exs) = 219 tests

---

## 2.3 Query Optimization

- [x] **Section 2.3 Complete** (2025-12-23)

This section implements rule-based query optimization transforms on the algebra tree. Optimizations include filter pushing, constant folding, and join reordering based on selectivity estimates.

Implemented in `lib/triple_store/sparql/optimizer.ex` with 113 tests in optimizer_test.exs.

**Review improvements applied (2025-12-23):**
- Added recursion depth limits (max 100) to prevent DoS via deeply nested queries
- All optimization passes now raise ArgumentError on excessively deep nesting
- Added 7 security-focused tests for depth limiting

### 2.3.1 Filter Push-Down

- [x] **Task 2.3.1 Complete** (2025-12-23)

Push filter expressions as close to their data sources as possible to reduce intermediate result sizes.

- [x] 2.3.1.1 Implement `push_filters_down(algebra)` transformation
- [x] 2.3.1.2 Push filters past joins when variables allow
- [x] 2.3.1.3 Split conjunctive filters and push independently
- [x] 2.3.1.4 Preserve filter semantics across OPTIONAL boundaries

Created `lib/triple_store/sparql/optimizer.ex` with filter push-down optimization.
Test coverage: 25 tests in optimizer_test.exs.

### 2.3.2 Constant Folding

- [x] **Task 2.3.2 Complete** (2025-12-23)

Evaluate constant expressions at compile time.

- [x] 2.3.2.1 Implement `fold_constants(algebra)` transformation
- [x] 2.3.2.2 Evaluate arithmetic on constant operands
- [x] 2.3.2.3 Evaluate comparisons on constant operands
- [x] 2.3.2.4 Simplify always-true/always-false filters

Extended `lib/triple_store/sparql/optimizer.ex` with constant folding optimization.
Also implements: logical expression folding with short-circuit, IF/COALESCE folding,
double negation elimination, join/union empty side elimination.
Test coverage: 42 new tests (67 total in optimizer_test.exs).

### 2.3.3 BGP Reordering

- [x] **Task 2.3.3 Complete** (2025-12-23)

Reorder BGP patterns based on selectivity estimates to reduce intermediate results.

- [x] 2.3.3.1 Implement `reorder_bgp_patterns(bgp, stats)` transformation
- [x] 2.3.3.2 Use predicate cardinalities from statistics
- [x] 2.3.3.3 Prefer patterns with bound subjects/objects
- [x] 2.3.3.4 Consider variable binding propagation between patterns

Extended `lib/triple_store/sparql/optimizer.ex` with BGP reordering optimization.
Uses greedy algorithm to select most selective patterns first, considering bound
positions and variable binding propagation. Supports predicate cardinality statistics.
Test coverage: 24 new tests (91 total in optimizer_test.exs).

### 2.3.4 Optimizer Pipeline

- [x] **Task 2.3.4 Complete** (2025-12-23)

Combine optimizations into a pipeline applied to all queries.

- [x] 2.3.4.1 Implement `Optimizer.optimize(algebra, stats)` entry point
- [x] 2.3.4.2 Apply optimizations in optimal order
- [x] 2.3.4.3 Add optimization logging for debugging
- [x] 2.3.4.4 Handle optimization bypass for EXPLAIN queries

Enhanced optimizer with pipeline architecture, debug logging via Logger,
and EXPLAIN mode that returns query analysis without modifying the algebra.
Test coverage: 15 new tests (106 total in optimizer_test.exs).

### 2.3.5 Unit Tests

- [x] **Task 2.3.5 Complete** (2025-12-23)

- [x] Test filter push-down past join
- [x] Test filter push-down stops at OPTIONAL
- [x] Test conjunctive filter splitting
- [x] Test constant folding for arithmetic
- [x] Test constant folding for comparisons
- [x] Test BGP reordering prefers selective patterns
- [x] Test optimizer pipeline produces valid algebra

All optimizer tests are integrated with their respective feature implementations.
Total test coverage: 113 tests in optimizer_test.exs (including 7 security tests).

---

## 2.4 Iterator Execution

- [ ] **Section 2.4 Complete**

This section implements the query executor using iterator-based lazy evaluation. Each algebra node type has a corresponding execution strategy that produces a Stream of bindings.

### 2.4.1 BGP Execution

- [x] **Task 2.4.1 Complete** (2025-12-23)

Execute Basic Graph Patterns using index nested loop join.

- [x] 2.4.1.1 Implement `execute_bgp(db, patterns)` returning `Stream.t(bindings)`
- [x] 2.4.1.2 Order patterns by selectivity before execution
- [x] 2.4.1.3 Implement variable substitution for bound variables
- [x] 2.4.1.4 Implement binding extension for matched variables
- [x] 2.4.1.5 Handle empty pattern (returns single empty binding)

Created `lib/triple_store/sparql/executor.ex` with BGP execution using nested loop join.
Integrates with Index layer for triple lookup and Dictionary for term encoding/decoding.
Test coverage: 13 tests in executor_test.exs.

### 2.4.2 Join Execution

- [x] **Task 2.4.2 Complete** (2025-12-23)

Execute join operations between result streams.

- [x] 2.4.2.1 Implement nested loop join for small inputs
- [x] 2.4.2.2 Implement hash join for larger inputs
- [x] 2.4.2.3 Implement left outer join for OPTIONAL semantics
- [x] 2.4.2.4 Handle compatible binding merge

Extended `lib/triple_store/sparql/executor.ex` with join execution functions:
- `join/3` - Main entry point with strategy selection (nested_loop, hash, auto)
- `nested_loop_join/2` - O(n*m) join for small inputs
- `hash_join/2` - O(n+m) join using hash table for larger inputs
- `left_join/3` - Left outer join for OPTIONAL semantics with filter support
- `merge_bindings/2` - Binding compatibility check and merge
Test coverage: 30 new tests (43 total in executor_test.exs).

### 2.4.3 Union Execution

- [x] **Task 2.4.3 Complete** (2025-12-23)

Execute UNION as concatenation of result streams.

- [x] 2.4.3.1 Implement `execute_union(db, left, right)` concatenating streams
- [x] 2.4.3.2 Handle variable alignment across branches
- [x] 2.4.3.3 Preserve ordering within branches

Extended `lib/triple_store/sparql/executor.ex` with union execution functions:
- `union/2` - Basic UNION concatenation with lazy evaluation
- `union_aligned/2` - UNION with variable alignment (adds `:unbound` for missing vars)
- `union_all/1` - Multi-branch UNION for `{ P1 } UNION { P2 } UNION { P3 }`
- `collect_all_variables/1` - Variable discovery helper
- `align_binding/2` - Binding alignment helper
Test coverage: 27 new tests (70 total in executor_test.exs).

### 2.4.4 Filter Execution

- [x] **Task 2.4.4 Complete** (2025-12-23)

Execute filter expressions against bindings.

- [x] 2.4.4.1 Implement `execute_filter(stream, expression)` filtering stream
- [x] 2.4.4.2 Implement expression evaluator for all operators
- [x] 2.4.4.3 Handle three-valued logic (true, false, error)
- [x] 2.4.4.4 Implement built-in function evaluation

Extended `lib/triple_store/sparql/executor.ex` with filter execution functions:
- `filter/2` - Main filter function with lazy evaluation
- `evaluate_filter/2` - Two-valued evaluation (true/false, errors as false)
- `evaluate_filter_3vl/2` - Three-valued evaluation (true/false/error)
- `filter_all/2` - Conjunctive filters (AND)
- `filter_any/2` - Disjunctive filters (OR)
- `to_effective_boolean/1` - EBV computation per SPARQL spec
Integrates with existing Expression module for full expression evaluation.
Test coverage: 36 new tests (106 total in executor_test.exs).

### 2.4.5 Solution Modifiers

- [x] **Task 2.4.5 Complete** (2025-12-23)

Execute solution modifiers (projection, ordering, slicing).

- [x] 2.4.5.1 Implement `execute_project(stream, vars)` selecting variables
- [x] 2.4.5.2 Implement `execute_distinct(stream)` removing duplicates
- [x] 2.4.5.3 Implement `execute_order(stream, comparators)` sorting results
- [x] 2.4.5.4 Implement `execute_slice(stream, offset, limit)` pagination

Extended `lib/triple_store/sparql/executor.ex` with solution modifier functions:
- `project/2` - Variable projection with lazy evaluation
- `distinct/1` - Duplicate elimination using MapSet tracking
- `reduced/1` - Relaxed duplicate elimination (implements as distinct)
- `order_by/2` - Result ordering with SPARQL ordering rules
- `slice/3` - Pagination with offset and limit
- `offset/2` and `limit/2` - Convenience functions
Test coverage: 37 new tests (143 total in executor_test.exs).

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
