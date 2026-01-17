# Section 6.7: Concurrency Tests

## Overview

Implement Section 6.7 of the quad store integration tests, focusing on concurrent read operations, concurrent write operations, and mixed read/write scenarios to validate thread safety and isolation properties.

## Feature Branch

`feature/section-6.7-concurrency-tests`

## Implementation Plan

### 6.7.1 Concurrent Reads

Test concurrent read operations to validate thread safety and consistency.

- [x] 6.7.1.1 Test concurrent queries on different graphs
- [x] 6.7.1.2 Test concurrent queries on same graph
- [x] 6.7.1.3 Test concurrent reads during load
- [x] 6.7.1.4 Test concurrent graph enumeration
- [x] 6.7.1.5 Test concurrent statistics access

### 6.7.2 Concurrent Writes

Test concurrent write operations to validate atomicity and consistency.

- [x] 6.7.2.1 Test concurrent inserts to different graphs
- [x] 6.7.2.2 Test concurrent inserts to same graph
- [x] 6.7.2.3 Test concurrent updates on different graphs
- [x] 6.7.2.4 Test concurrent CREATE GRAPH
- [x] 6.7.2.5 Test concurrent DELETE on same graph

### 6.7.3 Mixed Read/Write

Test concurrent read and write operations to validate isolation levels.

- [x] 6.7.3.1 Test read during INSERT to different graph
- [x] 6.7.3.2 Test read during INSERT to same graph (snapshot isolation)
- [x] 6.7.3.3 Test read during DELETE
- [x] 6.7.3.4 Test read during CLEAR GRAPH
- [x] 6.7.3.5 Test query during DROP GRAPH

## File Structure

```
test/triple_store/integration/
└── concurrency_test.exs          # All concurrency tests
```

## Implementation Notes

### Concurrency Testing Approach

1. **Use Elixir Tasks**: Leverage `Task.async_stream/5` for concurrent execution
2. **Synchronization**: Use `Agent` or `GenServer` for coordinating test state
3. **Assertions**: Verify consistency, atomicity, and isolation properties
4. **Timing**: Use appropriate timeouts to avoid hanging tests

### Test Isolation

Each test should:
- Create a unique database path to avoid conflicts
- Set up test data in isolation
- Clean up properly in `on_exit` callbacks
- Use deterministic seed values for reproducibility

### Expected Behaviors

**RocksDB Guarantees**:
- Reads are consistent (snapshot isolation)
- Writes are atomic per batch operation
- Concurrent writes to same key are serialized by RocksDB

**Our Implementation**:
- Each INSERT/DELETE is a WriteBatch (atomic across all indices)
- Queries see consistent snapshot at query start time
- Graph enumeration is consistent within query

## Dependencies

- Existing integration test infrastructure
- Quad store with all indices (GSPO, GPOS, SPOG, POSG)
- SPARQL UPDATE operations
- SPARQL QUERY operations

## Success Criteria

1. All concurrent read tests pass with consistent results
2. All concurrent write tests maintain data consistency
3. Mixed read/write tests demonstrate proper isolation
4. No deadlocks or race conditions detected
5. Tests complete within reasonable time limits

## Status

**Complete** - All 15 tests passing successfully
