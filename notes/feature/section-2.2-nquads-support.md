# Working Plan: Section 2.2 - N-Quads Format Support

## Branch: `feature/section-2.2-nquads-support`

## Status: COMPLETED

## Overview

Section 2.2 implements full N-Quads format support for loading and exporting quads. Previously, the loader only extracted the default graph from N-Quads files, discarding named graphs. This section enables loading all quads (including named graphs) and exporting back to N-Quads format.

---

## Part 1: Analysis (Completed)

- [x] Review current loader implementation
  - Current: `parse_nquads_file/1` extracts only default graph via `extract_default_graph/1`
  - Current: Loader uses `load_triples/5` which only handles 3-tuple triples
- [x] Review current exporter implementation
  - Has `export_file/3` function that supports N-Quads format
  - Uses `export_graph/2` which only exports triples (no graph context)
- [x] Review `QuadOperations.insert_quads/3` for quad insertion
  - Accepts list of quads: `{subject_id, predicate_id, object_id, graph_id}`
  - Returns `:ok` on success
- [x] Review `Adapter.from_rdf_quads/2` for quad conversion
  - Converts RDF.Quad tuples to internal quad format
  - Handles nil graph → ID 0 correctly

---

## Part 2: Implementation Tasks (Completed)

### 2.2.1 N-Quads Loading

**File:** `lib/triple_store/loader.ex`

**Tasks:**
- [x] 2.2.1.1 Add `load_nquads_file/3` function for N-Quads file loading
- [x] 2.2.1.2 Parse file using `RDF.NQuads.read_file/1` (returns RDF.Dataset)
- [x] 2.2.1.3 Extract all quads from dataset using `RDF.Dataset.quads/1`
- [x] 2.2.1.4 Convert quads using `Adapter.from_rdf_quads/2`
- [x] 2.2.1.5 Load quads using `QuadOperations.insert_quads/3`
- [x] 2.2.1.6 Handle batch processing with progress reporting
- [x] 2.2.1.7 Return count of quads loaded

**Design Decision:**
- Created new `load_nquads_file/3` function to avoid breaking existing triple-loading behavior
- Added `load_quads/5` private function (similar to `load_triples/5` but uses QuadOperations)
- Added `load_quads_sequential/6` and `load_quads_parallel/9` for batch processing

### 2.2.2 N-Quads Export

**File:** `lib/triple_store/exporter.ex`

**Tasks:**
- [x] 2.2.2.1 Add `export_nquads_file/3` function for N-Quads export
- [x] 2.2.2.2 Add `export_nquads_string/2` for string export
- [x] 2.2.2.3 Retrieve all quads using `QuadOperations.lookup_quads/3`
- [x] 2.2.2.4 Convert internal quads to RDF.Quad using `Adapter.to_rdf_quads/2`
- [x] 2.2.2.5 Write to file using `RDF.NQuads.write_file/2`
- [x] 2.2.2.6 Handle default graph (no graph name in output for graph ID 0)

**Implementation Notes:**
- Used `QuadOperations.lookup_quads/3` instead of stream API due to timeout issues
- Added `extract_bound_values/2` helper function for pattern-based filtering
- Added `quad_pattern` type to exporter types

### 2.2.3 N-Quads String Loading

**File:** `lib/triple_store/loader.ex`

**Tasks:**
- [x] 2.2.3.1 Add `load_nquads_string/3` function for string loading
- [x] 2.2.3.2 Parse string using `RDF.NQuads.read_string/1`
- [x] 2.2.3.3 Extract quads and convert to internal format
- [x] 2.2.3.4 Load quads using `QuadOperations.insert_quads/3`
- [x] 2.2.3.5 Return count of quads loaded

---

## Part 3: Tests (Completed)

### Test File: `test/triple_store/nquads_test.exs` (new file)

- [x] 2.2.4.1 Test loading N-Quads file with named graphs
- [x] 2.2.4.2 Test loading N-Quads file with default graph only
- [x] 2.2.4.3 Test loading N-Quads file with mixed graphs
- [x] 2.2.4.4 Test export to N-Quads preserves graph names
- [x] 2.2.4.5 Test N-Quads roundtrip (load + export)
- [x] 2.2.4.6 Test N-Quads string loading
- [x] 2.2.4.7 Test N-Quads export to string
- [x] 2.2.4.8 Test loading empty N-Quads file
- [x] 2.2.4.9 Test progress callback during N-Quads loading
- [x] 2.2.4.10 Test batch processing with large N-Quads file

**Test Results:** 18 tests, 0 failures

---

## Dependencies

- `TripleStore.Adapter.from_rdf_quads/2` - Convert RDF.Quad to internal format
- `TripleStore.Adapter.to_rdf_quads/2` - Convert internal to RDF.Quad
- `TripleStore.QuadOperations.insert_quads/3` - Insert quads into store
- `TripleStore.QuadOperations.lookup_quads/3` - Look up quads from store
- `RDF.NQuads.read_file/1` - Parse N-Quads files
- `RDF.NQuads.write_file/2` - Write N-Quads files
- `RDF.NQuads.read_string/1` - Parse N-Quads strings
- `RDF.Dataset.quads/1` - Extract quads from dataset

---

## Success Criteria

1. ✅ N-Quads file loading preserves all named graphs
2. ✅ N-Quads export produces valid N-Quads format
3. ✅ N-Quads roundtrip preserves all data (including named graphs)
4. ✅ String loading works correctly
5. ✅ Progress reporting works for quad loading
6. ✅ All 18 tests pass

---

## Notes

- Created new functions for quad loading to avoid breaking existing triple-loading behavior
- Existing `load_file/3` behavior remains unchanged for backward compatibility
- N-Quads format uses 4-tuple: subject, predicate, object, graph
- Default graph in N-Quads has no graph name (represented as nil in RDF.Quad, ID 0 in storage)
- Pattern-based lookup uses `:bound`/`:var` pattern format, not `{:bound, value}` tuples
- For progress callback halting, use `parallel: false` for deterministic behavior

---

## Summary

See `notes/summaries/section-2.2-nquads-support.md` for full implementation summary.
