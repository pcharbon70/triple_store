# Section 6.2: Loading Integration Tests

## Overview

This feature implements Section 6.2 of the quad store integration tests, focusing on loading N-Quads and TriG files, roundtrip preservation, and format conversion.

## Implementation Plan

### 6.2.1 N-Quads Loading (6 tests)

- [x] 6.2.1.1 Test load N-Quads file with single graph
- [x] 6.2.1.2 Test load N-Quads file with multiple graphs
- [x] 6.2.1.3 Test load N-Quads with default graph
- [x] 6.2.1.4 Test load N-Quads with blank node graphs
- [x] 6.2.1.5 Test load large N-Quads file (10k+ quads for testing)
- [x] 6.2.1.6 Test load N-Quads from string

### 6.2.2 TriG Loading (6 tests)

- [x] 6.2.2.1 Test load TriG file with single named graph
- [x] 6.2.2.2 Test load TriG file with multiple graphs
- [x] 6.2.2.3 Test load TriG with default graph block
- [x] 6.2.2.4 Test load TriG with nested graphs (multiple graphs in one file)
- [x] 6.2.2.5 Test load large TriG file (10k+ quads for testing)
- [x] 6.2.2.6 Test load TriG from string

### 6.2.3 Roundtrip Tests (6 tests)

- [x] 6.2.3.1 Test N-Quads load/export roundtrip
- [x] 6.2.3.2 Test TriG load/export roundtrip
- [x] 6.2.3.3 Test N-Quads to TriG conversion
- [x] 6.2.3.4 Test TriG to N-Quads conversion
- [x] 6.2.3.5 Test roundtrip preserves all graphs
- [x] 6.2.3.6 Test roundtrip preserves blank node IDs

### 6.2.4 Format Conversion (5 tests)

- [x] 6.2.4.1 Test load Turtle to named graph
- [x] 6.2.4.2 Test export single graph as Turtle
- [x] 6.2.4.3 Test export default graph as N-Triples
- [x] 6.2.4.4 Test convert N-Quads to Turtle (per graph)
- [x] 6.2.4.5 Test convert TriG to N-Quads

## Files Created

1. `test/triple_store/integration/nquads_loading_test.exs` - N-Quads loading tests (15 tests)
2. `test/triple_store/integration/trig_loading_test.exs` - TriG loading tests (15 tests)
3. `test/triple_store/integration/roundtrip_test.exs` - Roundtrip tests (14 tests)
4. `test/triple_store/integration/format_conversion_test.exs` - Format conversion tests (13 tests)

**Total: 57 integration tests**

## Test Structure

Tests follow the existing integration test patterns:
- Use `ExUnit.Case, async: false` for database operations
- Create temporary databases with unique IDs using `System.system_time(:microsecond)`
- Use `on_exit` for cleanup
- Test both success and error cases
- Verify data preservation across load/export cycles

## Dependencies

- `TripleStore.Loader` - Loading operations for N-Quads/TriG/Turtle
- `TripleStore.Exporter` - Export operations
- `TripleStore.Backend.RocksDB.NIF` - Database operations
- `TripleStore.Dictionary.Manager` - Dictionary encoding
- `TripleStore.QuadOperations` - Quad CRUD operations
- `RDF.Turtle` / `RDF.NQuads` / `RDF.TriG` - RDF format handling

## Test Results

All 57 tests pass:
- N-Quads Loading: 15 tests
- TriG Loading: 15 tests
- Roundtrip: 14 tests
- Format Conversion: 13 tests

## Status

**Complete** - All tests implemented and passing
