# Working Plan: Section 2.7 - Exporter Module Refactoring

## Branch: `feature/section-2.7-exporter-refactoring`

## Status: COMPLETED

## Overview

Section 2.7 refactors the Exporter module to provide a unified API for both triple and quad export. The goal is to make the quad store export functionality first-class while maintaining backward compatibility with existing triple-exporting code.

**Note**: Many quad export features have already been implemented in sections 2.2-2.3 (N-Quads and TriG export). This section focuses on:
1. Adding graph-scoped export functions
2. Adding RDF.Dataset return types for quad stores
3. Adding convenience functions for common export patterns
4. Improving format detection

---

## Part 1: Analysis (COMPLETED)

- [x] Review current Exporter module public API
- [x] Identify functions that need graph parameter support
- [x] Review existing quad export implementations (N-Quads, TriG)
- [x] Determine backward compatibility requirements

---

## Part 2: Implementation Tasks (COMPLETED)

### 2.7.1 Export API Updates

**Module:** `lib/triple_store/exporter.ex`

**Tasks:**
- [x] 2.7.1.1 Add `export_dataset/2` returning RDF.Dataset for quad stores
- [x] 2.7.1.2 Add `export_graphs/4` for specific graph selection (with manager)
- [x] 2.7.1.3 Add `export_default_graph/1` convenience function
- [x] 2.7.1.4 Add `export_single_graph/4` for one named graph (with manager)
- [x] 2.7.1.5 Add `export_multiple_graphs/4` for list of graphs (with manager)
- [x] 2.7.1.6 Update function specifications for quad types
- [x] 2.7.1.7 Update documentation with graph export examples

**Functions added:**
- `export_dataset/2` - Export all quads as RDF.Dataset
- `export_graphs/4` - Export specific graphs as RDF.Dataset (requires manager)
- `export_default_graph/1` - Export only default graph as RDF.Graph
- `export_single_graph/4` - Export one named graph (requires manager)
- `export_multiple_graphs/4` - Export list of named graphs (alias, requires manager)

**API Design:**
```elixir
# Export all quads as RDF.Dataset (for quad stores)
@spec export_dataset(db_ref(), keyword()) :: {:ok, RDF.Dataset.t()} | {:error, term()}

# Export specific graphs as RDF.Dataset (requires manager for term-to-ID conversion)
@spec export_graphs(db_ref(), TripleStore.Dictionary.manager(), [RDF.IRI.t()], keyword()) :: {:ok, RDF.Dataset.t()} | {:error, term()}

# Export only default graph as RDF.Graph
@spec export_default_graph(db_ref(), keyword()) :: {:ok, RDF.Graph.t()} | {:error, term()}

# Export single named graph as RDF.Graph (requires manager)
@spec export_single_graph(db_ref(), TripleStore.Dictionary.manager(), RDF.IRI.t(), keyword()) :: {:ok, RDF.Graph.t()} | {:error, term()}

# Export multiple named graphs as RDF.Dataset (alias, requires manager)
@spec export_multiple_graphs(db_ref(), TripleStore.Dictionary.manager(), [RDF.IRI.t()], keyword()) :: {:ok, RDF.Dataset.t()} | {:error, term()}
```

### 2.7.2 Graph-Scoped Export Implementation (COMPLETED)

**Module:** `lib/triple_store/exporter.ex`

**Tasks:**
- [x] 2.7.2.1 Implement graph filtering using QuadOperations
- [x] 2.7.2.2 Handle default graph parameter correctly
- [x] 2.7.2.3 Return RDF.Dataset with specified graphs
- [x] 2.7.2.4 Add batch processing for large datasets
- [x] 2.7.2.5 Add telemetry support for RDF.Dataset

### 2.7.3 Documentation Updates (COMPLETED)

**Module:** `lib/triple_store/exporter.ex`

**Tasks:**
- [x] 2.7.3.1 Update moduledoc to mention quad export support
- [x] 2.7.3.2 Add examples for exporting with named graphs
- [x] 2.7.3.3 Document the graph export behavior
- [x] 2.7.3.4 Add migration guide from triples to quads

---

## Part 3: Tests (COMPLETED)

### Test File: `test/triple_store/exporter_refactoring_test.exs` (created)

- [x] 2.7.4.1 Test export_dataset returns RDF.Dataset with all graphs
- [x] 2.7.4.2 Test export_graphs filters to specified graphs
- [x] 2.7.4.3 Test export_default_graph returns only default graph
- [x] 2.7.4.4 Test export_single_graph returns single named graph
- [x] 2.7.4.5 Test export_multiple_graphs returns specified graphs
- [x] 2.7.4.6 Test backward compatibility (existing export functions)
- [x] 2.7.4.7 Test error handling for non-existent graphs

**Test Results: 14 tests, 0 failures**

---

## Dependencies

- `TripleStore.Exporter` - Module being refactored
- `TripleStore.Adapter` - For quad conversion (to_rdf_quads/2, term_to_id/2)
- `TripleStore.QuadOperations` - For graph filtering and lookup
- `RDF.Dataset` - For quad containers
- `RDF.Graph` - For triple containers (backward compatibility)

---

## Success Criteria

1. ✅ New export functions support named graphs
2. ✅ Backward compatibility maintained (existing functions unchanged)
3. ✅ export_dataset returns RDF.Dataset for quad stores
4. ✅ Documentation updated with examples
5. ✅ All 14 tests pass

---

## Notes

- This is a refactoring section, focused on API consistency
- N-Quads and TriG export already exist from sections 2.2-2.3
- Focus is on graph-scoped export and convenience functions
- Existing tests continue to pass without modification
- New tests verify the enhanced API

## Key Implementation Decisions

1. **Manager Parameter**: Functions that need term-to-ID conversion (export_graphs, export_single_graph, export_multiple_graphs) require manager as a parameter, following the pattern used in QuadOperations.

2. **Telemetry Support**: Added support for RDF.Dataset in the with_telemetry function to properly track dataset export operations.

3. **Return Values**:
   - export_dataset returns RDF.Dataset containing all graphs
   - export_graphs returns RDF.Dataset with specified graphs
   - export_default_graph returns RDF.Graph (no graph name)
   - export_single_graph returns RDF.Graph with the graph's name
