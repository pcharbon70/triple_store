# Section 6.3: Query Integration Tests

## Overview

This feature implements Section 6.3 of the quad store integration tests, focusing on GRAPH clause queries, cross-graph queries, and result serialization.

## Implementation Plan

### 6.3.1 GRAPH Clause Queries (8 tests) - 7/19 passing

- [x] 6.3.1.1 Test SELECT from single named graph
- [x] 6.3.1.2 Test SELECT from multiple named graphs (UNION)
- [ ] 6.3.1.3 Test SELECT with graph variable (graph variable tests failing - Stream.resource issue)
- [x] 6.3.1.4 Test SELECT from default graph (implicit)
- [ ] 6.3.1.5 Test nested GRAPH clauses (nested GRAPH tests failing)
- [ ] 6.3.1.6 Test GRAPH with OPTIONAL (OPTIONAL within GRAPH failing - LEFT_JOIN not supported)
- [x] 6.3.1.7 Test GRAPH with UNION
- [ ] 6.3.1.8 Test GRAPH with FILTER (FILTER tests failing)

### 6.3.2 Cross-Graph Queries (5 tests) - Not Started

- [ ] 6.3.2.1 Test query patterns across two graphs
- [ ] 6.3.2.2 Test query with graph variable in join
- [ ] 6.3.2.3 Test query comparing graphs via FILTER
- [ ] 6.3.2.4 Test query aggregating across graphs
- [ ] 6.3.2.5 Test subquery across graphs

### 6.3.3 Result Serialization (6 tests) - Not Started

- [ ] 6.3.3.1 Test SELECT returns graph variable binding
- [ ] 6.3.3.2 Test SELECT * includes graph
- [ ] 6.3.3.3 Test CONSTRUCT returns RDF.Dataset
- [ ] 6.3.3.4 Test ASK with graph context
- [ ] 6.3.3.5 Test ORDER BY with graph variable
- [ ] 6.3.3.6 Test GROUP BY with graph variable

## Files Created

1. `test/triple_store/integration/graph_clause_query_test.exs` - GRAPH clause query tests (7/19 passing)

## Test Structure

Tests follow the existing integration test patterns:
- Use `ExUnit.Case, async: false` for database operations
- Create temporary databases with unique IDs
- Use `on_exit` for cleanup
- Load test data using Loader module
- Execute queries using SPARQL query module
- Verify results with assertions

## Dependencies

- `TripleStore.Loader` - Loading test data
- `TripleStore.Backend.RocksDB.NIF` - Database operations
- `TripleStore.Dictionary.Manager` - Dictionary encoding
- `TripleStore.SPARQL` - Query execution
- `TripleStore.QuadOperations` - Quad CRUD operations
- `RDF.NQuads` / `RDF.TriG` - RDF format handling

## Status

**Partially Complete** - 7/19 tests passing for section 6.3.1. See summary document for details on remaining issues.
