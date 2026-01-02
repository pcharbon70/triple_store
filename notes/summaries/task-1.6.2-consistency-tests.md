# Task 1.6.2: Consistency Tests - Summary

**Date:** 2026-01-02

## Overview

Implemented consistency tests to verify data integrity after parallel bulk loading operations. These tests ensure that the parallel loading pipeline maintains correctness across the dictionary and index layers.

## Test File Created

`test/triple_store/loader/consistency_test.exs`

17 consistency tests covering all task requirements.

## Test Categories

### 1.6.2.1 Dictionary Consistency (No Duplicate IDs)

- **bulk load assigns unique IDs to each unique term** - Loads 5000 triples, verifies all term IDs are unique
- **parallel loading produces unique IDs across concurrent batches** - 10 concurrent processes creating 500 terms each, verifies no ID collisions
- **repeated terms in different batches get same ID** - Verifies idempotent ID assignment across batches

### 1.6.2.2 Dictionary Bidirectionality (Encode/Decode Roundtrip)

- **str2id and id2str are inverse operations after bulk load** - Verifies 1000 terms roundtrip correctly through str2id and id2str
- **URIs, blank nodes, and literals all roundtrip correctly** - Tests various RDF term types maintain integrity
- **inline numeric literals preserve values** - Tests XSD.Integer values are stored correctly

### 1.6.2.3 Index Consistency (All Three Indices Match)

- **SPO, POS, and OSP indices contain identical triples** - Loads 2000 triples, scans all three indices, verifies they match
- **parallel bulk load maintains index consistency** - Loads 5000 triples with parallel stages, verifies index consistency
- **each triple is findable via all indices** - Tests specific triples exist in SPO, POS, and OSP indices

### 1.6.2.4 Query Correctness After Bulk Load

- **subject-based lookups return correct triples** - Creates 100 subjects × 5 triples, queries specific subject
- **predicate-based lookups return correct triples** - Loads 500 triples with 10 predicates, queries specific predicate
- **object-based lookups return correct triples** - Tests shared object lookups return correct count
- **full scan returns all loaded triples** - Loads 1000 triples, verifies full scan count

### 1.6.2.5 Persistence Survives Restart

- **data persists after close and reopen** - Loads 500 triples, closes db, reopens, verifies data
- **dictionary state persists correctly** - Creates terms, closes, verifies same IDs after reopen
- **index consistency maintained after restart** - Loads data, closes, verifies all three indices match after reopen
- **sequence counter resumes correctly after restart** - Verifies new sequence numbers are greater than old ones

## Test Tags

- `@moduletag :integration` - All tests marked as integration tests

## Running the Tests

```bash
# Run all consistency tests
mix test test/triple_store/loader/consistency_test.exs
```

## Files Created

1. `test/triple_store/loader/consistency_test.exs` (NEW)
   - 17 comprehensive consistency tests
   - ~680 lines of test code

2. `notes/planning/performance/phase-01-bulk-load-optimization.md`
   - Marked task 1.6.2 as complete

## Test Results

All 17 tests pass:
- 3 tests for dictionary consistency
- 3 tests for dictionary bidirectionality
- 3 tests for index consistency
- 4 tests for query correctness
- 4 tests for persistence

## Key Implementation Details

### Scan Index Helper
Uses `NIF.prefix_stream/3` with empty prefix to scan all entries from an index and convert to canonical `{subject, predicate, object}` triples for comparison.

### Persistence Test Pattern
Tests follow the pattern:
1. Close setup database first
2. Create fresh test path
3. Open, load data, close
4. Call `:erlang.garbage_collect()` and `Process.sleep(200)` to ensure db is fully released
5. Reopen and verify

## Verified Properties

1. **No duplicate IDs** - Each unique term gets exactly one ID
2. **Bidirectional mapping** - str2id and id2str are inverses
3. **Index consistency** - All three indices (SPO, POS, OSP) contain the same triples
4. **Query correctness** - Pattern matching works correctly after bulk load
5. **Persistence** - Data survives database close and reopen
6. **Sequence continuity** - Sequence counter resumes from last used value
