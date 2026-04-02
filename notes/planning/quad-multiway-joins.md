# Multi-Way Quad Join Implementation

## Overview

This plan implements complete multi-way quad joins for the quad store, enabling efficient worst-case optimal joins across all four quad positions (subject, predicate, object, graph). The current implementation handles single-variable patterns via prefix scans but does not fully exploit the Leapfrog Triejoin algorithm for complex quad patterns with multiple unbound variables.

By the end of this plan, quad patterns like `{:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}` will execute efficiently using four coordinated iterators that leapfrog to find intersections where all variables align on the same quad.

The core Leapfrog infrastructure already supports polymorphic iterators via `TrieIteratorProtocol`, so this work focuses on quad-specific iterator creation, coordination, and binding extraction.

---

## Phase 1: Multi-Iterator Pattern Creation

**Phase Goal**: Extend `QuadLeapfrog.create_iterators_for_pattern/2` to create multiple iterators (one per variable position) instead of a single prefix iterator.

### Section 1.1: Quad Index Strategy

**Description**: Define the index selection strategy for creating iterators based on which quad components are bound vs unbound. Each quad index (GSPO, GPOS, SPOG, POSG) provides different prefix capabilities, so we must choose the appropriate index for each iterator position.

#### Task 1.1.1: Index Binding Matrix

Define a matrix mapping which index to use for each iterator position based on bound components.

- [ ] 1.1.1.1 Document which index supports which prefix patterns
- [ ] 1.1.1.2 Create `index_for_variable_pattern/2` function
- [ ] 1.1.1.3 Handle all 16 combinations of bound/unbound for 4 positions
- [ ] 1.1.1.4 Return `:gspo`, `:gpos`, `:spog`, or `:posg` based on pattern

#### Task 1.1.2: Iterator Position Types

Define the type for each iterator position in a multi-iterator join.

- [ ] 1.1.2.1 Define `@type iterator_position :: {:bound, non_neg_integer()} | {:variable, String.t()}`
- [ ] 1.1.2.2 Define `@type iterator_plan :: [{position(), index()}]`
- [ ] 1.1.2.3 Create `plan_iterators/2` returning optimal iterator list

#### Task 1.1.3: Prefix Builder per Position

Implement prefix building for each iterator position based on bound values at that position.

- [ ] 1.1.3.1 Extend `build_prefix_from_components/2` to support per-position prefixes
- [ ] 1.1.3.2 Handle variable positions in prefix (use wildcard/0)
- [ ] 1.1.3.3 Ensure correct byte alignment for multi-component prefixes

#### Task 1.1.4: Unit Tests

- [ ] Test index selection matrix returns correct index for all patterns
- [ ] Test iterator plan handles fully-bound pattern (direct lookup path)
- [ ] Test iterator plan handles fully-unbound pattern (4-way join)
- [ ] Test prefix builder creates correct prefixes for bound components

---

### Section 1.2: Multi-Iterator Creation

**Description**: Implement the creation of multiple `QuadTrieIterator` instances, one for each variable position in the quad pattern, using the index strategy from Section 1.1.

#### Task 1.2.1: Iterator Creation Loop

Implement the loop that creates one iterator per variable position.

- [ ] 1.2.1.1 Modify `create_iterators_for_pattern/2` to support multi-iterator mode
- [ ] 1.2.1.2 Create 4 iterators for fully-unbound pattern (one per variable)
- [ ] 1.2.1.3 Create fewer iterators when some components are bound
- [ ] 1.2.1.4 Handle edge case of single-variable pattern (existing behavior)

#### Task 1.2.2: QuadTrieIterator Initialization

Initialize each iterator with its position-specific prefix and index.

- [ ] 1.2.2.1 Call `QuadTrieIterator.new/4` with correct CF for each position
- [ ] 1.2.2.2 Pass position-specific prefix depth to each iterator
- [ ] 1.2.2.3 Ensure each iterator starts at its first valid position
- [ ] 1.2.2.4 Handle case where no variables are bound (use GSPO prefix scan)

#### Task 1.2.3: Iterator Coordination Metadata

Attach metadata to each iterator for binding extraction later.

- [ ] 1.2.3.1 Store variable name with each iterator
- [ ] 1.2.3.2 Store position index with each iterator
- [ ] 1.2.3.3 Extend `QuadTrieIterator` struct or use wrapper map
- [ ] 1.2.3.4 Update `TrieIteratorProtocol` to handle metadata if needed

#### Task 1.2.4: Unit Tests

- [ ] Test multi-iterator creation produces 4 iterators for 4 variables
- [ ] Test multi-iterator creation produces fewer iterators with bound components
- [ ] Test each iterator has correct prefix for its position
- [ ] Test iterator metadata includes variable name and position

---

### Section 1.3: Leapfrog Integration

**Description**: Pass the multiple created iterators to the core `Leapfrog.new/2` algorithm, which already supports polymorphic iterators via `TrieIteratorProtocol`.

#### Task 1.3.1: Iterator List Validation

Validate the iterator list before passing to Leapfrog.

- [ ] 1.3.1.1 Ensure at least 1 iterator (or handle direct lookup)
- [ ] 1.3.1.2 Ensure all iterators implement `TrieIteratorProtocol`
- [ ] 1.3.1.3 Validate no duplicate iterator positions
- [ ] 1.3.1.4 Return appropriate error for invalid configurations

#### Task 1.3.2: Leapfrog Initialization

Initialize Leapfrog with the multi-iterator list.

- [ ] 1.3.2.1 Pass iterator list to `Leapfrog.new/2`
- [ ] 1.3.2.2 Handle `{:exhausted, lf}` return for empty result sets
- [ ] 1.3.2.3 Handle `{:error, reason}` for initialization failures
- [ ] 1.3.2.4 Wrap Leapfrog struct in `QuadLeapfrog` wrapper

#### Task 1.3.3: Search and Next Delegation

Ensure search and next operations properly delegate to core Leapfrog.

- [ ] 1.3.3.1 Verify `QuadLeapfrog.search/1` calls `Leapfrog.search/1` correctly
- [ ] 1.3.3.2 Verify `QuadLeapfrog.next/1` calls `Leapfrog.next/1` correctly
- [ ] 1.3.3.3 Ensure QuadLeapfrog state is updated after each operation
- [ ] 1.3.3.4 Handle exhausted state propagation correctly

#### Task 1.3.4: Unit Tests

- [ ] Test Leapfrog accepts 4 QuadTrieIterator instances
- [ ] Test Leapfrog searches for intersection across 4 iterators
- [ ] Test Leapfrog handles exhausted iterator in the list
- [ ] Test Leapfrog next advances all iterators correctly

---

### Section 1.4: Binding Extraction from Multi-Iterator Results

**Description**: When Leapfrog finds a match (all iterators aligned), extract the actual quad bindings by reading keys from all four iterators and mapping them to their variable names.

#### Task 1.4.1: Multi-Iterator Key Reading

Read keys from all iterators when at a match.

- [ ] 1.4.1.1 Extend `bindings_from_quad_iterator/2` to handle multiple iterators
- [ ] 1.4.1.2 Read current key from each iterator in the list
- [ ] 1.4.1.3 Decode each key to extract (g, s, p, o) tuple
- [ ] 1.4.1.4 Handle case where iterators have different keys (shouldn't happen)

#### Task 1.4.2: Variable-to-Position Mapping

Map extracted values to their variable names based on iterator position.

- [ ] 1.4.2.1 Store position-to-variable mapping in QuadLeapfrog struct
- [ ] 1.4.2.2 Extract values from decoded keys based on variable positions
- [ ] 1.4.2.3 Build bindings map with variable names as keys
- [ ] 1.4.2.4 Handle bound components (include in bindings with actual value)

#### Task 1.4.3: Quad Reconstruction

Reconstruct the full quad from iterator keys for verification.

- [ ] 1.4.3.1 Combine (g, s, p, o) from all iterators into single quad
- [ ] 1.4.3.2 Validate all iterators agree on the quad value
- [ ] 1.4.3.3 Handle case of disagreement (fallback to first iterator)
- [ ] 1.4.3.4 Include reconstructed quad in bindings for debugging

#### Task 1.4.4: Unit Tests

- [ ] Test binding extraction produces correct variable bindings
- [ ] Test binding extraction includes bound components correctly
- [ ] Test binding extraction handles 4-variable pattern
- [ ] Test binding extraction handles mixed bound/unbound pattern

---

### Section 1.5: Unit Tests

- [ ] Test fully-unbound quad pattern creates 4 iterators
- [ ] Test single-variable pattern uses prefix scan (existing behavior)
- [ ] Test two-variable pattern creates 2 iterators
- [ ] Test three-variable pattern creates 3 iterators
- [ ] Test fully-bound pattern uses direct lookup (existing behavior)
- [ ] Test iterator metadata preserved through Leapfrog operations
- [ ] Test bindings extracted correctly for all variable positions

---

### Section 1.6: Phase 1 Integration Tests

**Description**: End-to-end tests for multi-iterator quad join functionality, verifying that complex quad patterns execute correctly with multiple iterators coordinated via Leapfrog.

#### Integration Test 1.6.1: Four-Way Variable Join

Test fully-unbound quad pattern with 4 variables.

- [ ] 1.6.1.1 Load test data with multiple quads across graphs
- [ ] 1.6.1.2 Execute pattern `{:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}`
- [ ] 1.6.1.3 Verify results include all quads from test data
- [ ] 1.6.1.4 Verify no duplicate results
- [ ] 1.6.1.5 Verify performance is acceptable (should use index scans)

#### Integration Test 1.6.2: Three-Way Join with Bound Graph

Test quad pattern with bound graph and 3 variables.

- [ ] 1.6.2.1 Load test data across multiple graphs
- [ ] 1.6.2.2 Execute pattern `{:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}`
- [ ] 1.6.2.3 Verify results only include quads from graph 0
- [ ] 1.6.2.4 Verify 3 iterators created (not 4)
- [ ] 1.6.2.5 Verify performance uses graph-prefixed index

#### Integration Test 1.6.3: Two-Way Subject-Predicate Join

Test quad pattern with bound object and graph, 2 variables.

- [ ] 1.6.3.1 Load test data with specific object/graph combinations
- [ ] 1.6.3.2 Execute pattern `{:quad, {:variable, "s"}, {:variable, "p"}, 42, 1}`
- [ ] 1.6.3.3 Verify results match only quads with object=42, graph=1
- [ ] 1.6.3.4 Verify 2 iterators created
- [ ] 1.6.3.5 Verify binding extraction returns s and p variables

#### Integration Test 1.6.4: Mixed Bound and Unbound

Test various combinations of bound/unbound components.

- [ ] 1.6.4.1 Test pattern with 1 bound, 3 unbound components
- [ ] 1.6.4.2 Test pattern with 2 bound, 2 unbound components
- [ ] 1.6.4.3 Test pattern with 3 bound, 1 unbound component
- [ ] 1.6.4.4 Verify correct iterator count for each pattern
- [ ] 1.6.4.5 Verify correct results for each pattern

#### Integration Test 1.6.5: Empty Result Handling

Test behavior when no quads match the pattern.

- [ ] 1.6.5.1 Execute pattern that cannot match any existing quads
- [ ] 1.6.5.2 Verify stream completes without returning results
- [ ] 1.6.5.3 Verify no errors or exceptions raised
- [ ] 1.6.5.4 Verify exhausted state is properly reported

---

## Phase 2: Optimization and Edge Cases

**Status**: ✅ **COMPLETED** - See [PR #24](https://github.com/pcharbon70/triple_store/pull/24)

**Branch**: `codex/phase-2-optimization-edge-cases`

**Phase Goal**: Optimize multi-iterator quad joins for performance and handle edge cases that may arise in production use.

**Summary**: Implemented iterator ordering optimization, edge case handling (empty database, large graphs, malformed patterns), QuadTrieIterator protocol enhancements, and stream enumeration support. Added 19 tests covering all implemented functionality.

### Section 2.1: Performance Optimization

**Description**: Optimize the multi-iterator join for common patterns and ensure query performance meets production requirements.

#### Task 2.1.1: Iterator Ordering Optimization

Order iterators by selectivity before passing to Leapfrog.

- [x] 2.1.1.1 Implement `order_iterators_by_selectivity/2` using cardinality estimates
- [x] 2.1.1.2 Prefer bound components first (most selective)
- [x] 2.1.1.3 Consider prefix position selectivity
- [x] 2.1.1.4 Fall back to default ordering for unknown cardinalities

#### Task 2.1.2: Short-Circuit for Direct Lookup

Optimize fully-bound and nearly-bound patterns to avoid iterator overhead.

- [x] 2.1.2.1 Detect when all components are bound (use direct lookup)
- [x] 2.1.2.2 Detect when 3 components are bound (use prefix scan + filter)
- [x] 2.1.2.3 Only create multiple iterators when 2+ components are unbound
- [x] 2.1.2.4 Benchmark to verify optimization provides benefit

#### Task 2.1.3: Iterator Reuse

Reuse iterators across multiple join operations when possible.

- [ ] 2.1.3.1 Identify when iterator can be reused across patterns
- [ ] 2.1.3.2 Implement iterator pooling for common patterns
- [ ] 2.1.3.3 Ensure proper cleanup on stream completion
- [ ] 2.1.3.4 Add telemetry for iterator pool hit/miss rate

#### Task 2.1.4: Unit Tests

- [x] Test iterator ordering improves performance on selective patterns
- [x] Test short-circuit avoids iterator creation for bound patterns
- [ ] Test iterator reuse reduces allocation overhead
- [x] Benchmark comparing before/after optimization

---

### Section 2.2: Edge Case Handling

**Description**: Handle edge cases that may arise in production use, including empty databases, very large graphs, and malformed patterns.

#### Task 2.2.1: Empty Database

Ensure correct behavior when database has no quads.

- [x] 2.2.1.1 Test pattern on empty database returns exhausted immediately
- [x] 2.2.1.2 Verify no errors or crashes
- [x] 2.2.1.3 Verify stream terminates without blocking
- [x] 2.2.1.4 Verify iterators properly closed

#### Task 2.2.2: Large Graph Handling

Handle patterns where one graph has significantly more data than others.

- [x] 2.2.2.1 Test pattern with one large graph and small graphs
- [x] 2.2.2.2 Verify iterator doesn't get stuck scanning large graph
- [x] 2.2.2.3 Implement timeout or max iteration safeguard
- [x] 2.2.2.4 Add telemetry for iteration count

#### Task 2.2.3: Malformed Pattern Handling

Handle invalid or malformed quad patterns gracefully.

- [x] 2.2.3.1 Validate pattern structure before iterator creation
- [x] 2.2.3.2 Return helpful error for invalid patterns
- [x] 2.2.3.3 Handle patterns with all variables (same as all unbound)
- [x] 2.2.3.4 Handle patterns with no variables (use direct lookup)

#### Task 2.2.4: Unit Tests

- [x] Test empty database returns exhausted immediately
- [x] Test large graph doesn't cause timeout or excessive iteration
- [x] Test malformed patterns return helpful errors
- [x] Test max iteration safeguard prevents infinite loops

---

### Section 2.3: QuadTrieIterator Protocol Enhancements

**Description**: Ensure `QuadTrieIterator` fully implements `TrieIteratorProtocol` with correct semantics for multi-iterator coordination.

#### Task 2.3.1: Protocol Compliance Verification

Verify QuadTrieIterator implements all required protocol functions.

- [x] 2.3.1.1 Verify `current/1` returns correct key for position
- [x] 2.3.1.2 Verify `next/1` advances to next key for position
- [x] 2.3.1.3 Verify `seek/2` seeks to target in position's domain
- [x] 2.3.1.4 Verify `exhausted?/1` correctly reports position exhaustion

#### Task 2.3.2: Position-Aware Operations

Enhance QuadTrieIterator operations to be aware of their position.

- [x] 2.3.2.1 Store position index in QuadTrieIterator struct
- [x] 2.3.2.2 Use position to decode key correctly (4 different schemas)
- [x] 2.3.2.3 Handle prefix depth based on position
- [x] 2.3.2.4 Update documentation for position-aware behavior

#### Task 2.3.3: Key Encoding Consistency

Ensure all iterators use consistent key encoding for the same quad.

- [x] 2.3.3.1 Verify all 4 position iterators encode the same quad to same key
- [x] 2.3.3.2 Test encoding/decoding round-trip for each position
- [x] 2.3.3.3 Document key encoding scheme for each index
- [x] 2.3.3.4 Add unit tests for encoding consistency

#### Task 2.3.4: Unit Tests

- [x] Test protocol functions work correctly for each position type
- [x] Test position-aware operations return correct results
- [x] Test key encoding consistency across all positions
- [x] Test protocol works with Leapfrog for multi-iterator joins

---

### Section 2.4: Stream and Enumeration

**Description**: Ensure `QuadLeapfrog.stream/1` works correctly with multi-iterator joins and produces a clean enumeration API.

#### Task 2.4.1: Stream Implementation

Verify stream yields all matches correctly.

- [x] 2.4.1.1 Test `QuadLeapfrog.stream/1` yields binding maps
- [x] 2.4.1.2 Verify stream terminates when exhausted
- [x] 2.4.1.3 Verify stream is lazy (doesn't materialize all results upfront)
- [x] 2.4.1.4 Verify stream can be halted mid-execution

#### Task 2.4.2: Resource Cleanup

Ensure iterators are properly closed when stream is terminated.

- [x] 2.4.2.1 Verify iterators closed when stream completes
- [x] 2.4.2.2 Verify iterators closed when stream is halted
- [x] 2.4.2.3 Verify iterators closed on error/exception
- [ ] 2.4.2.4 Add telemetry for resource cleanup

#### Task 2.4.3: Backpressure Handling

Ensure stream handles backpressure correctly.

- [x] 2.4.3.1 Test slow consumer doesn't cause issues
- [x] 2.4.3.2 Test stream works with Enum.take for limiting results
- [x] 2.4.3.3 Test stream works with Stream.transform for processing
- [x] 2.4.3.4 Verify memory usage is bounded

#### Task 2.4.4: Unit Tests

- [x] Test stream yields all matches in correct order
- [x] Test stream terminates without leaks
- [x] Test stream cleanup on halt
- [x] Test stream handles backpressure correctly

---

### Section 2.5: Unit Tests

- [x] Test iterator ordering improves join performance
- [x] Test short-circuit optimization works for bound patterns
- [x] Test empty database handled correctly
- [x] Test large graph doesn't cause timeout
- [x] Test malformed patterns return helpful errors
- [x] Test QuadTrieIterator protocol compliance
- [x] Test key encoding consistency across positions
- [x] Test stream yields all results correctly
- [x] Test stream cleanup on normal completion
- [x] Test stream cleanup on early termination

**Status**: ✅ 19 tests added and passing

---

### Section 2.6: Phase 2 Integration Tests

**Status**: ⏸️ **DEFERRED** - Integration tests deferred to Phase 3 (SPARQL Integration)

**Description**: Integration tests for optimized multi-iterator joins with real-world data patterns and edge cases.

**Note**: These integration tests are better suited for Phase 3 when the multi-iterator joins are integrated into the SPARQL query engine, allowing for end-to-end testing with actual SPARQL queries.

#### Integration Test 2.6.1: Real-World Query Patterns

Test patterns based on real SPARQL graph queries.

- [ ] 2.6.1.1 Test graph enumeration: `SELECT ?g WHERE { GRAPH ?g { ?s a ?type } }`
- [ ] 2.6.1.2 Test cross-graph query: `SELECT ?s ?o WHERE { GRAPH ?g1 { ?s ?p ?o } GRAPH ?g2 { ?s ?p ?o } }`
- [ ] 2.6.1.3 Test graph-scoped pattern: `SELECT ?s WHERE { GRAPH :named { ?s ?p ?o } }`
- [ ] 2.6.1.4 Verify results match expected SPARQL semantics

#### Integration Test 2.6.2: Performance Benchmarks

Benchmark multi-iterator joins against single-iterator patterns.

- [ ] 2.6.2.1 Benchmark 4-variable join on 10K quads
- [ ] 2.6.2.2 Benchmark 4-variable join on 100K quads
- [ ] 2.6.2.3 Benchmark 4-variable join on 1M quads
- [ ] 2.6.2.4 Compare performance against equivalent triple pattern

#### Integration Test 2.6.3: Stress Testing

Test the system under heavy load with complex patterns.

- [ ] 2.6.3.1 Execute 100 concurrent quad pattern queries
- [ ] 2.6.3.2 Test with very large result sets (100K+ matches)
- [ ] 2.6.3.3 Test with deep nesting (multiple quad patterns)
- [ ] 2.6.3.4 Verify no resource leaks or crashes

#### Integration Test 2.6.4: Error Recovery

Test error handling and recovery in multi-iterator joins.

- [ ] 2.6.4.1 Test behavior when database closes mid-query
- [ ] 2.6.4.2 Test behavior when compaction interferes with iteration
- [ ] 2.6.4.3 Test timeout handling on long-running queries
- [ ] 2.6.4.4 Verify clean error messages for failures

---

## Phase 3: SPARQL Integration

**Status**: ✅ **COMPLETED** - See [PR #24](https://github.com/pcharbon70/triple_store/pull/24)

**Branch**: `codex/phase-2-optimization-edge-cases`

**Phase Goal**: Integrate multi-iterator quad joins into the SPARQL query engine so that quad patterns in SPARQL queries automatically use the optimized multi-iterator approach.

**Summary**: Implemented pattern recognition module, executor integration hooks, GRAPH clause optimization, comprehensive unit tests (20 tests), and integration tests (20 tests) covering end-to-end functionality.

### Section 3.1: Pattern Recognition

**Description**: Enhance the SPARQL algebra executor to recognize when a quad pattern would benefit from multi-iterator joins.

#### Task 3.1.1: Quad Pattern Detection

Detect quad patterns in SPARQL algebra.

- [x] 3.1.1.1 Identify GRAPH clauses with triple patterns
- [x] 3.1.1.2 Identify quad patterns in query WHERE clause
- [x] 3.1.1.3 Determine if pattern has multiple unbound variables
- [x] 3.1.1.4 Flag patterns suitable for multi-iterator join

#### Task 3.1.2: Cost-Based Decision

Decide when to use multi-iterator vs single-iterator approach.

- [x] 3.1.2.1 Define threshold for number of unbound variables
- [x] 3.1.2.2 Use cardinality estimates to inform decision
- [x] 3.1.2.3 Fall back to single-iterator for simple patterns
- [ ] 3.1.2.4 Add telemetry for decision tracking

#### Task 3.1.3: Pattern Translation

Translate SPARQL quad patterns to QuadLeapfrog patterns.

- [x] 3.1.3.1 Map SPARQL variables to QuadLeapfrog variable names
- [x] 3.1.3.2 Map bound values to appropriate integer IDs
- [x] 3.1.3.3 Handle graph variable vs named graph IRI
- [x] 3.1.3.4 Translate default graph case correctly

#### Task 3.1.4: Unit Tests

- [x] Test quad pattern detection identifies correct patterns
- [x] Test cost-based decision makes optimal choices
- [x] Test pattern translation produces correct QuadLeapfrog patterns
- [x] Test default graph handled correctly in translation

---

### Section 3.2: Executor Integration

**Description**: Modify the SPARQL executor to use QuadLeapfrog for appropriate quad patterns.

#### Task 3.2.1: BGP Execution Hook

Add hook to basic graph pattern execution for quad patterns.

- [x] 3.2.1.1 Modify BGP executor to check for quad patterns
- [x] 3.2.1.2 Route quad patterns to QuadLeapfrog execution
- [x] 3.2.1.3 Fall back to existing execution for non-quad patterns
- [x] 3.2.1.4 Ensure compatibility with existing query semantics

#### Task 3.2.2: Result Integration

Integrate QuadLeapfrog results with SPARQL result set.

- [x] 3.2.2.1 Convert QuadLeapfrog bindings to SPARQL binding format
- [x] 3.2.2.2 Merge with bindings from other pattern types
- [x] 3.2.2.3 Handle variable naming consistency
- [x] 3.2.2.4 Ensure result ordering matches SPARQL semantics

#### Task 3.2.3: Stream Integration

Support streaming results from QuadLeapfrog in SPARQL queries.

- [x] 3.2.3.1 Convert QuadLeapfrog stream to SPARQL result stream
- [x] 3.2.3.2 Support LIMIT and OFFSET on QuadLeapfrog results
- [x] 3.2.3.3 Support ORDER BY on QuadLeapfrog results
- [x] 3.2.3.4 Ensure lazy evaluation is preserved

#### Task 3.2.4: Unit Tests

- [x] Test BGP executor routes quad patterns correctly
- [x] Test result integration produces correct SPARQL results
- [x] Test stream integration preserves laziness
- [x] Test LIMIT/OFFSET work correctly

---

### Section 3.3: GRAPH Clause Optimization

**Description**: Optimize SPARQL GRAPH clauses that can benefit from multi-iterator joins.

#### Task 3.3.1: Static GRAPH Detection

Optimize queries with static named graph in GRAPH clause.

- [x] 3.3.1.1 Detect `GRAPH <iri> { ?s ?p ?o }` patterns
- [x] 3.3.1.2 Use graph-prefixed iterator for better performance
- [x] 3.3.1.3 Reduce iterator count by 1 for static graph
- [ ] 3.3.1.4 Benchmark vs generic approach

#### Task 3.3.2: Variable GRAPH Detection

Optimize queries with variable in GRAPH clause.

- [x] 3.3.2.1 Detect `GRAPH ?g { ?s ?p ?o }` patterns
- [x] 3.3.2.2 Create 4-iterator join for full enumeration
- [x] 3.3.2.3 Ensure all 4 variables appear in results
- [x] 3.3.2.4 Verify correct semantics for variable graph

#### Task 3.3.3: Multi-GRAPH Patterns

Optimize queries with multiple GRAPH clauses.

- [x] 3.3.3.1 Detect `GRAPH ?g1 { ... } GRAPH ?g2 { ... }` patterns
- [x] 3.3.3.2 Use union or intersection of iterators as appropriate
- [x] 3.3.3.3 Handle same variable across multiple graphs
- [x] 3.3.3.4 Verify correct duplicate handling

#### Task 3.3.4: Unit Tests

- [x] Test static GRAPH uses optimized path
- [x] Test variable GRAPH uses 4-iterator join
- [x] Test multi-GRAPH patterns handled correctly
- [x] Test GRAPH clause semantics match SPARQL spec

---

### Section 3.4: Unit Tests

- [x] Test SPARQL executor integrates QuadLeapfrog correctly
- [x] Test quad patterns in WHERE clause use multi-iterator
- [x] Test GRAPH clause optimization works correctly
- [x] Test result integration preserves query semantics
- [x] Test streaming results work with LIMIT/OFFSET
- [x] Test ORDER BY works with QuadLeapfrog results

**Status**: ✅ 20 tests passing

---

### Section 3.5: Phase 3 Integration Tests

**Description**: End-to-end SPARQL queries using multi-iterator quad joins, verifying complete integration with the query engine.

#### Integration Test 3.5.1: Complete SPARQL Queries

Test full SPARQL queries with quad patterns.

- [x] 3.5.1.1 Test `SELECT * WHERE { GRAPH ?g { ?s ?p ?o } }`
- [x] 3.5.1.2 Test `SELECT * WHERE { GRAPH <iri> { ?s a :type } }`
- [x] 3.5.1.3 Test `SELECT ?s WHERE { GRAPH ?g1 { ?s ?p ?o } GRAPH ?g2 { ?s ?p2 ?o2 } }`
- [x] 3.5.1.4 Test complex queries with multiple quad and triple patterns

#### Integration Test 3.5.2: Mixed Triple/Quad Queries

Test queries mixing triple store and quad store patterns.

- [x] 3.5.2.1 Test query with both triple patterns and GRAPH clauses
- [x] 3.5.2.2 Test query joins across triple and quad patterns
- [x] 3.5.2.3 Test query with UNION of triple and quad patterns
- [x] 3.5.2.4 Verify correct results across mixed patterns

#### Integration Test 3.5.3: UPDATE with Quad Patterns

Test SPARQL UPDATE operations with quad patterns.

- [ ] 3.5.3.1 Test `INSERT DATA { GRAPH ?g { ?s ?p ?o } }` with variables
- [ ] 3.5.3.2 Test `DELETE WHERE { GRAPH ?g { ?s ?p ?o } }` with variables
- [ ] 3.5.3.3 Test UPDATE modifies quads correctly
- [ ] 3.5.3.4 Verify query plan invalidation after update

#### Integration Test 3.5.4: Performance Validation

Validate query performance meets production requirements.

- [x] 3.5.4.1 Benchmark GRAPH clause query vs manual triple equivalent
- [x] 3.5.4.2 Benchmark multi-variable GRAPH query performance
- [x] 3.5.4.3 Verify query cost within acceptable bounds
- [ ] 3.5.4.4 Compare against baseline (single-iterator) performance

**Status**: ✅ 20 tests passing (UPDATE operations deferred)

---

## Success Criteria

1. **Functionality**: Multi-iterator quad joins produce correct results for all variable binding patterns
2. **Performance**: 4-variable quad joins execute efficiently using Leapfrog Triejoin
3. **Integration**: SPARQL queries with GRAPH clauses automatically use multi-iterator joins
4. **Stability**: Edge cases handled gracefully without resource leaks
5. **Test Coverage**: Comprehensive unit and integration tests ensure correctness

## Key Outputs

- Enhanced `QuadLeapfrog.create_iterators_for_pattern/2` supporting multiple iterators
- Position-aware `QuadTrieIterator` with correct key encoding per index
- SPARQL executor integration for automatic multi-iterator selection
- Comprehensive test coverage for multi-iterator quad joins
- Performance benchmarks demonstrating optimization benefits
