# Section 2.1: Quad Term Conversion - Implementation Summary

## Branch: `feature/section-2.1-quad-term-conversion`

## Date Completed: 2025-01-10

## Overview

Section 2.1 implements the conversion layer between RDF.ex quad representation (RDF.Quad) and the internal quad storage format. This is the foundation for loading and storing quad formats (N-Quads, TriG) in Phase 2.

---

## Changes Implemented

### Part 1: Quad Type Definitions (2.1.1)

**File:** `lib/triple_store/adapter.ex`

Added type definitions for quad representation:

```elixir
@type rdf_quad :: {RDF.IRI.t() | RDF.BlankNode.t(), RDF.IRI.t(), rdf_term(), RDF.IRI.t() | RDF.BlankNode.t() | nil}
@type internal_quad :: {term_id(), term_id(), term_id(), term_id()}
```

- `rdf_quad`: 4-tuple of RDF terms (subject, predicate, object, graph)
- `internal_quad`: 4-tuple of dictionary-encoded term IDs
- Graph position can be IRI, BlankNode, or nil (for default graph)

### Part 2: RDF.ex to Internal Conversion (2.1.2)

**File:** `lib/triple_store/adapter.ex`

Implemented `from_rdf_quad/2`:

```elixir
@spec from_rdf_quad(manager(), rdf_quad()) :: {:ok, internal_quad()} | {:error, term()}
```

- Converts RDF.Quad tuples to internal quad representation
- Handles IRI, BlankNode, and nil graph names
- Nil graph converts to ID 0 (default graph)
- Uses existing term conversion functions for S/P/O components

### Part 3: Internal to RDF.ex Conversion (2.1.3)

**File:** `lib/triple_store/adapter.ex`

Implemented `to_rdf_quad/2`:

```elixir
@spec to_rdf_quad(db_ref(), internal_quad()) :: {:ok, rdf_quad()} | :not_found | {:error, term()}
```

- Converts internal quad to RDF.Quad representation
- ID 0 converts to nil graph (default graph)
- Preserves graph term type (IRI vs BlankNode)

### Part 4: Batch Quad Conversion (2.1.4)

**File:** `lib/triple_store/adapter.ex`

Implemented batch conversion functions:

- `from_rdf_quads/2`: Converts list of RDF quads to internal representation
- `to_rdf_quads/2`: Converts list of internal quads to RDF representation

**Implementation Note:** Batch conversion processes quads individually to properly handle nil graphs. This is a simplification that could be optimized in the future by collecting non-nil terms and tracking nil positions.

### Part 5: Helper Functions

**File:** `lib/triple_store/adapter.ex`

Added private helper functions:

- `graph_to_id/2`: Converts RDF graph term to graph ID (nil → 0)
- `id_to_graph/2`: Converts graph ID to RDF graph term (0 → nil)

---

## Files Modified

### Source Files
1. `lib/triple_store/adapter.ex` - Added quad type definitions and conversion functions

### Test Files (New)
1. `test/triple_store/adapter/quad_conversion_test.exs` - 25 tests for quad conversion

---

## Test Results

**All tests passing:** 25 tests, 0 failures

### Test Coverage

1. **from_rdf_quad/2** (6 tests):
   - Quad with IRI graph
   - Quad with blank node graph
   - Quad with nil graph → default graph ID 0
   - Quad with blank node subject and named graph
   - Quad with inline-encoded integer object
   - Same quad converts to same IDs

2. **to_rdf_quad/2** (6 tests):
   - Internal quad with IRI graph roundtrip
   - Internal quad with blank node graph roundtrip
   - Default graph ID (0) → nil graph
   - Quad with blank node subject and object
   - Quad with inline-encoded integer roundtrip
   - Unknown term ID returns :not_found

3. **from_rdf_quads/2** (2 tests):
   - Empty list
   - Multiple quads with mixed graph types
   - Handles quads with shared terms

4. **to_rdf_quads/2** (2 tests):
   - Empty list
   - Multiple internal quads back to RDF
   - Handles quads with inline-encoded values

5. **Roundtrip conversion** (4 tests):
   - Quad with IRI graph
   - Quad with blank node graph
   - Quad with nil graph
   - Batch quads roundtrip

6. **Graph handling** (3 tests):
   - Different graph names get different IDs
   - Same graph name gets same ID across quads
   - Default graph ID is always 0

---

## Success Criteria Met

1. ✅ RDF.Quad with named graph converts to internal quad with correct graph ID
2. ✅ RDF.Quad with nil graph converts to internal quad with graph ID 0
3. ✅ Internal quad with graph ID 0 converts to RDF.Quad with nil graph
4. ✅ Internal quad with graph ID > 0 converts to RDF.Quad with proper graph term
5. ✅ Batch conversion handles multiple quads efficiently
6. ✅ All 25 unit tests pass

---

## Design Decisions

1. **Extended existing adapter.ex**: The plan mentioned `lib/triple_store/rdf_adapter.ex` but this file doesn't exist. We extended the existing `lib/triple_store/adapter.ex` module following the same pattern as triple conversion.

2. **Nil graph ↔ ID 0 mapping**: Confirmed with user that nil graph in RDF.Quad should map to ID 0 (default graph).

3. **Batch conversion implementation**: Implemented batch conversion by processing quads individually to properly handle nil graphs. This is simpler than collecting non-nil terms and tracking positions, and is sufficient for the current use case.

4. **Type specs**: Added comprehensive type specs for all public functions following the existing pattern in the adapter.

---

## Breaking Changes

None - this is new functionality added to the existing adapter module.

---

## Dependencies

- Existing `TripleStore.Adapter` for term conversion patterns
- Existing `TripleStore.Dictionary` for graph ID handling
- `RDF.Quad` from RDF.ex library (quads represented as 4-tuples)
- `@default_graph_id` constant (0) from Dictionary module

---

## Next Steps

After merging to `quad` branch, proceed to:
- Section 2.2: N-Quads Format Support
- Section 2.3: TriG Format Support
- Section 2.4: Quad Loading Operations

---

## References

- Plan: `notes/planning/quad/phase-02-rdf-integration-and-loading.md`
- Working Plan: `notes/feature/section-2.1-quad-term-conversion.md`
