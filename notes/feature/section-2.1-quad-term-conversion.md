# Working Plan: Section 2.1 - Quad Term Conversion

## Branch: `feature/section-2.1-quad-term-conversion`

## Status: COMPLETED

## Overview

Section 2.1 implements the conversion layer between RDF.ex quad representation (RDF.Quad) and the internal quad storage format. This is the foundation for loading and storing quad formats (N-Quads, TriG) in Phase 2.

The plan mentions updating `lib/triple_store/rdf_adapter.ex`, but we need to check if this file exists or if we should extend `lib/triple_store/adapter.ex` instead.

---

## Part 1: Analysis (Completed)

- [x] Check if `lib/triple_store/rdf_adapter.ex` exists or should be created
  - Confirmed: File doesn't exist, extending existing `adapter.ex`
- [x] Review existing `TripleStore.Adapter` module for triple conversion patterns
- [x] Review RDF.Quad structure from RDF.ex library
  - Confirmed: Quads are 4-tuples {s, p, o, g}, graph can be nil for default
- [x] Understand how `@default_graph_id` (0) should be handled
  - Confirmed with user: nil graph ↔ ID 0

---

## Part 2: Implementation Tasks (Completed)

### 2.1.1 Quad Type Definition

**File:** `lib/triple_store/adapter.ex` (extended existing)

**Tasks:**
- [x] 2.1.1.1 Define `@type rdf_quad` and `@type internal_quad`
- [x] 2.1.1.2 Document quad ordering: subject, predicate, object, graph
- [x] 2.1.1.3 Add type specs for quad functions
- [x] 2.1.1.4 Document that graph position nil = default graph

### 2.1.2 RDF.ex Quad to Internal Conversion

**File:** `lib/triple_store/adapter.ex` (extended existing)

**Tasks:**
- [x] 2.1.2.1 Implement `from_rdf_quad/2` converting RDF.Quad to internal quad
- [x] 2.1.2.2 Handle `RDF.Quad` with `RDF.IRI` graph names
- [x] 2.1.2.3 Handle `RDF.Quad` with `RDF.BlankNode` graph names
- [x] 2.1.2.4 Handle default graph (nil graph in RDF.Quad) → @default_graph_id (0)
- [x] 2.1.2.5 Use existing term conversion functions for S/P/O components

### 2.1.3 Internal Quad to RDF.ex Conversion

**File:** `lib/triple_store/adapter.ex` (extended existing)

**Tasks:**
- [x] 2.1.3.1 Implement `to_rdf_quad/3` converting internal quad to RDF.Quad
- [x] 2.1.3.2 Handle @default_graph_id (0) → nil graph in RDF.Quad
- [x] 2.1.3.3 Use existing ID-to-term conversion functions
- [x] 2.1.3.4 Preserve graph term type (IRI vs BlankNode)

### 2.1.4 Batch Quad Conversion

**File:** `lib/triple_store/adapter.ex` (extended existing)

**Tasks:**
- [x] 2.1.4.1 Implement `from_rdf_quads/2` for stream conversion
- [x] 2.1.4.2 Implement `to_rdf_quads/2` for reverse stream conversion
- [x] 2.1.4.3 Use batch operations for efficiency
- [x] 2.1.4.4 Handle errors in individual quads gracefully

---

## Part 3: Tests (Completed)

### Test File: `test/triple_store/adapter/quad_conversion_test.exs`

- [x] 2.8.1.1 Test RDF.Quad with IRI graph converts correctly
- [x] 2.8.1.2 Test RDF.Quad with blank node graph converts correctly
- [x] 2.8.1.3 Test RDF.Quad with nil graph uses default_graph_id
- [x] 2.8.1.4 Test internal quad converts to RDF.Quad correctly
- [x] 2.8.1.5 Test default_graph_id converts to nil graph in RDF.Quad
- [x] 2.8.1.6 Test batch conversion handles multiple quads

**Test Results: 25 tests, 0 failures**

---

## Dependencies

- Existing `TripleStore.Adapter` for term conversion patterns
- Existing `TripleStore.Dictionary` for graph ID handling
- `RDF.Quad` from RDF.ex library
- `@default_graph_id` constant (0) from Dictionary module

---

## Success Criteria

1. ✅ RDF.Quad with named graph converts to internal quad with correct graph ID
2. ✅ RDF.Quad with nil graph converts to internal quad with graph ID 0
3. ✅ Internal quad with graph ID 0 converts to RDF.Quad with nil graph
4. ✅ Internal quad with graph ID > 0 converts to RDF.Quad with proper graph term
5. ✅ Batch conversion handles multiple quads efficiently
6. ✅ All 25 unit tests pass

---

## Notes

- The plan mentions `lib/triple_store/rdf_adapter.ex` but this file doesn't exist
- We extended the existing `lib/triple_store/adapter.ex` module instead
- The existing Adapter already has `from_rdf_*` and `to_rdf_*` functions for triples
- We followed the same pattern for quads
- Batch conversion processes quads individually to handle nil graphs correctly
- This could be optimized in the future by collecting non-nil terms

---

## Summary

See `notes/summaries/section-2.1-quad-term-conversion.md` for full implementation summary.
