# Phase 6: Quad Store Integration Tests

## Overview

Phase 6 implements comprehensive integration tests for the quad store. By the end of this phase, the entire quad storage, loading, query, and update pipeline will be validated with real-world workloads.

Tests cover end-to-end scenarios from loading N-Quads/TriG files through querying with GRAPH clauses to updating named graphs.

---

## 6.1 Quad Storage Integration Tests

### 6.1.1 Database Lifecycle

Test complete database lifecycle with quads.

- [ ] 6.1.1.1 Test create new quad store database
- [ ] 6.1.1.2 Test open quad store with four indices
- [ ] 6.1.1.3 Test schema version 2 detected on open
- [ ] 6.1.1.4 Test triple store database rejected (version mismatch)
- [ ] 6.1.1.5 Test close and reopen persists quads
- [ ] 6.1.1.6 Test multiple quad stores in same process

### 6.1.2 Quad Insert and Lookup

Test quad insertion and retrieval end-to-end.

- [ ] 6.1.2.1 Test insert single quad retrieves correctly
- [ ] 6.1.2.2 Test insert batch quads retrieves all
- [ ] 6.1.2.3 Test insert to default graph works
- [ ] 6.1.2.4 Test insert to named graph works
- [ ] 6.1.2.5 Test quad exists in all four indices
- [ ] 6.1.2.6 Test duplicate insert is idempotent

### 6.1.3 Quad Delete

Test quad deletion end-to-end.

- [ ] 6.1.3.1 Test delete single quad removes from all indices
- [ ] 6.1.3.2 Test delete batch quads removes all
- [ ] 6.1.3.3 Test delete from default graph works
- [ ] 6.1.3.4 Test delete from named graph works
- [ ] 6.1.3.5 Test delete non-existent quad is no-op
- [ ] 6.1.3.6 Test delete all quads from graph

---

## 6.2 Loading Integration Tests

### 6.2.1 N-Quads Loading

Test loading N-Quads files end-to-end.

- [ ] 6.2.1.1 Test load N-Quads file with single graph
- [ ] 6.2.1.2 Test load N-Quads file with multiple graphs
- [ ] 6.2.1.3 Test load N-Quads with default graph
- [ ] 6.2.1.4 Test load N-Quads with blank node graphs
- [ ] 6.2.1.5 Test load large N-Quads file (1M+ quads)
- [ ] 6.2.1.6 Test load N-Quads from string

### 6.2.2 TriG Loading

Test loading TriG files end-to-end.

- [ ] 6.2.2.1 Test load TriG file with single named graph
- [ ] 6.2.2.2 Test load TriG file with multiple graphs
- [ ] 6.2.2.3 Test load TriG with default graph block
- [ ] 6.2.2.4 Test load TriG with nested graphs
- [ ] 6.2.2.5 Test load large TriG file
- [ ] 6.2.2.6 Test load TriG from string

### 6.2.3 Roundtrip Tests

Test load/export roundtrip preserves data.

- [ ] 6.2.3.1 Test N-Quads load/export roundtrip
- [ ] 6.2.3.2 Test TriG load/export roundtrip
- [ ] 6.2.3.3 Test N-Quads to TriG conversion
- [ ] 6.2.3.4 Test TriG to N-Quads conversion
- [ ] 6.2.3.5 Test roundtrip preserves all graphs
- [ ] 6.2.3.6 Test roundtrip preserves blank node IDs

### 6.2.4 Format Conversion

Test converting between quad and triple formats.

- [ ] 6.2.4.1 Test load Turtle to named graph
- [ ] 6.2.4.2 Test export single graph as Turtle
- [ ] 6.2.4.3 Test export default graph as N-Triples
- [ ] 6.2.4.4 Test convert N-Quads to Turtle (per graph)
- [ ] 6.2.4.5 Test convert TriG to N-Quads

---

## 6.3 Query Integration Tests

### 6.3.1 GRAPH Clause Queries

Test GRAPH clause execution end-to-end.

- [ ] 6.3.1.1 Test SELECT from single named graph
- [ ] 6.3.1.2 Test SELECT from multiple named graphs (UNION)
- [ ] 6.3.1.3 Test SELECT with graph variable
- [ ] 6.3.1.4 Test SELECT from default graph (implicit)
- [ ] 6.3.1.5 Test nested GRAPH clauses
- [ ] 6.3.1.6 Test GRAPH with OPTIONAL
- [ ] 6.3.1.7 Test GRAPH with UNION
- [ ] 6.3.1.8 Test GRAPH with FILTER

### 6.3.2 Cross-Graph Queries

Test queries spanning multiple graphs.

- [ ] 6.3.2.1 Test query patterns across two graphs
- [ ] 6.3.2.2 Test query with graph variable in join
- [ ] 6.3.2.3 Test query comparing graphs via FILTER
- [ ] 6.3.2.4 Test query aggregating across graphs
- [ ] 6.3.2.5 Test subquery across graphs

### 6.3.3 Result Serialization

Test query result serialization with graphs.

- [ ] 6.3.3.1 Test SELECT returns graph variable binding
- [ ] 6.3.3.2 Test SELECT * includes graph
- [ ] 6.3.3.3 Test CONSTRUCT returns RDF.Dataset
- [ ] 6.3.3.4 Test ASK with graph context
- [ ] 6.3.3.5 Test ORDER BY with graph variable
- [ ] 6.3.3.6 Test GROUP BY with graph variable

---

## 6.4 Update Integration Tests

### 6.4.1 Graph Management Updates

Test CREATE/DROP/CLEAR GRAPH operations.

- [ ] 6.4.1.1 Test CREATE GRAPH then query returns empty
- [ ] 6.4.1.2 Test DROP GRAPH removes all data
- [ ] 6.4.1.3 Test CLEAR GRAPH empties graph
- [ ] 6.4.1.4 Test CREATE SILENT on existing graph
- [ ] 6.4.1.5 Test DROP SILENT on missing graph
- [ ] 6.4.1.6 Test CLEAR ALL empties database

### 6.4.2 INSERT/DELETE with Graphs

Test INSERT DATA and DELETE DATA with graphs.

- [ ] 6.4.2.1 Test INSERT DATA to named graph
- [ ] 6.4.2.2 Test INSERT DATA with multiple GRAPH blocks
- [ ] 6.4.2.3 Test DELETE DATA from named graph
- [ ] 6.4.2.4 Test DELETE DATA with multiple GRAPH blocks
- [ ] 6.4.2.5 Test INSERT then DELETE same quad
- [ ] 6.4.2.6 Test INSERT creates graph if needed

### 6.4.3 MODIFY Operations

Test MODIFY (DELETE/INSERT WHERE) with graphs.

- [ ] 6.4.3.1 Test MODIFY in named graph
- [ ] 6.4.3.2 Test MODIFY with WHERE across graphs
- [ ] 6.4.3.3 Test MODIFY WITH graph context
- [ ] 6.4.3.4 Test MODIFY atomicity (all or nothing)
- [ ] 6.4.3.5 Test MODIFY returns correct counts

### 6.4.4 COPY/MOVE/ADD Operations

Test graph copy, move, and add operations.

- [ ] 6.4.4.1 Test COPY GRAPH duplicates graph
- [ ] 6.4.4.2 Test MOVE GRAPH moves and deletes source
- [ ] 6.4.4.3 Test ADD merges source into target
- [ ] 6.4.4.4 Test operations with SILENT modifier
- [ ] 6.4.4.5 Test operations on non-existent graphs

---

## 6.5 Real-World Scenarios

### 6.5.1 RDF Datasets

Test common RDF dataset patterns.

- [ ] 6.5.1.1 Test VoID dataset description
- [ ] 6.5.1.2 Test named graph for provenance
- [ ] 6.5.1.3 Test named graph for access control
- [ ] 6.5.1.4 Test named graph for temporal data
- [ ] 6.5.1.5 Test union graph via query

### 6.5.2 SPARQL 1.1 Graph Tests

Test SPARQL 1.1 graph management test cases.

- [ ] 6.5.2.1 Test Graph Management Protocol (DAV)
- [ ] 6.5.2.2 Test graph selection in FROM/FROM NAMED
- [ ] 6.5.2.3 Test GRAPH clause with subqueries
- [ ] 6.5.2.4 Test GRAPH with EXISTS/NOT EXISTS
- [ ] 6.5.2.5 Test GRAPH with property paths

### 6.5.3 Performance Benchmarks

Benchmark quad operations for performance validation.

- [ ] 6.5.3.1 Benchmark loading N-Quads (1M quads target <30s)
- [ ] 6.5.3.2 Benchmark loading TriG (1M quads target <30s)
- [ ] 6.5.3.3 Benchmark graph-scoped query (<10ms for simple pattern)
- [ ] 6.5.3.4 Benchmark cross-graph query (<100ms for moderate complexity)
- [ ] 6.5.3.5 Benchmark graph enumeration (<100ms for 100 graphs)
- [ ] 6.5.3.6 Benchmark INSERT/DELETE with graphs

---

## 6.6 Error Handling Tests

### 6.6.1 Invalid Data Handling

Test handling of invalid quad data.

- [ ] 6.6.1.1 Test load N-Quads with syntax errors
- [ ] 6.6.1.2 Test load TriG with syntax errors
- [ ] 6.6.1.3 Test load with invalid IRIs
- [ ] 6.6.1.4 Test load with invalid literals
- [ ] 6.6.1.5 Test load with malformed quads

### 6.6.2 Constraint Violations

Test handling of constraint violations.

- [ ] 6.6.2.1 Test INSERT duplicate quad (idempotent)
- [ ] 6.6.2.2 Test DELETE non-existent quad (no-op)
- [ ] 6.6.2.3 Test operation on non-existent graph
- [ ] 6.6.2.4 Test DROP non-existent graph with SILENT
- [ ] 6.6.2.5 Test CREATE existing graph (fails)

### 6.6.3 Query Errors

Test handling of query errors with graphs.

- [ ] 6.6.3.1 Test GRAPH with non-existent graph (empty result)
- [ ] 6.6.3.2 Test invalid graph IRI in query
- [ ] 6.6.3.3 Test malformed GRAPH clause
- [ ] 6.6.3.4 Test query timeout with cross-graph scan
- [ ] 6.6.3.5 Test memory limit with large graph scan

---

## 6.7 Concurrency Tests

### 6.7.1 Concurrent Reads

Test concurrent read operations.

- [x] 6.7.1.1 Test concurrent queries on different graphs
- [x] 6.7.1.2 Test concurrent queries on same graph
- [x] 6.7.1.3 Test concurrent reads during load
- [x] 6.7.1.4 Test concurrent graph enumeration
- [x] 6.7.1.5 Test concurrent statistics access

### 6.7.2 Concurrent Writes

Test concurrent write operations.

- [x] 6.7.2.1 Test concurrent inserts to different graphs
- [x] 6.7.2.2 Test concurrent inserts to same graph
- [x] 6.7.2.3 Test concurrent updates on different graphs
- [x] 6.7.2.4 Test concurrent CREATE GRAPH
- [x] 6.7.2.5 Test concurrent DELETE on same graph

### 6.7.3 Mixed Read/Write

Test concurrent read and write operations.

- [x] 6.7.3.1 Test read during INSERT to different graph
- [x] 6.7.3.2 Test read during INSERT to same graph (snapshot isolation)
- [x] 6.7.3.3 Test read during DELETE
- [x] 6.7.3.4 Test read during CLEAR GRAPH
- [x] 6.7.3.5 Test query during DROP GRAPH

---

## 6.8 Migration Tests

### 6.8.1 Triple to Quad Migration

Test migration from triple to quad store.

- [ ] 6.8.1.1 Test export triple store as N-Triples
- [ ] 6.8.1.2 Test convert N-Triples to N-Quads (add default graph)
- [ ] 6.8.1.3 Test load N-Quads to new quad store
- [ ] 6.8.1.4 Test query migrated data returns same results
- [ ] 6.8.1.5 Test all data preserved in migration

### 6.8.2 Migration Tooling

Test migration tool functionality.

- [ ] 6.8.2.1 Test migration tool handles large datasets
- [ ] 6.8.2.2 Test migration tool reports progress
- [ ] 6.8.2.3 Test migration tool handles errors gracefully
- [ ] 6.8.2.4 Test migration tool validates output
- [ ] 6.8.2.5 Test migration tool can resume on failure

---

## Success Criteria

1. **Loading**: N-Quads/TriG files load correctly
2. **Querying**: All GRAPH clause patterns execute correctly
3. **Updating**: All UPDATE operations with graphs work
4. **Roundtrip**: Load/export preserves all data
5. **Performance**: Benchmarks meet targets
6. **Concurrency**: Concurrent operations work correctly

## Provides Foundation

This phase establishes validation for:
- **Phase 7**: Reasoning with named graphs
- **Phase 8**: Production deployment of quad store

## Key Outputs

- Comprehensive integration test suite for quad store
- Real-world scenario tests
- Performance benchmarks for quad operations
- Migration test coverage
