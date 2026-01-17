# Working Plan: Section 2.5 - Graph-Scoped Loading

## Branch: `feature/section-2.5-graph-scoped-loading`

## Status: COMPLETED

## Overview

Section 2.5 implements graph-scoped loading functionality. This allows loading RDF data (Turtle, N-Triples) directly into specific named graphs, overriding the default graph behavior of the source files. It also supports loading multiple files to separate graphs in a single operation.

---

## Part 1: Analysis (COMPLETED)

- [x] Review current Loader module for existing load functions
- [x] Understand how file formats are detected and parsed
- [x] Review Adapter module for graph term handling
- [x] Determine the best module to place these operations (Loader)

---

## Part 2: Implementation Tasks (COMPLETED)

### 2.5.1 Load to Specific Graph (COMPLETED)

**Module:** `lib/triple_store/loader.ex`

**Tasks:**
- [x] 2.5.1.1 Implement `load_to_graph/5` with explicit graph parameter
- [x] 2.5.1.2 Support Turtle, N-Triples loaded to named graph
- [x] 2.5.1.3 Override default graph in source with target graph
- [x] 2.5.1.4 Create graph if it doesn't exist
- [x] 2.5.1.5 Return count of quads loaded

**API Design:**
```elixir
@spec load_to_graph(db_ref(), TripleStore.Dictionary.Manager.manager(), Path.t(), RDF.IRI.t() | RDF.BlankNode.t(), keyword()) ::
        {:ok, non_neg_integer()} | {:error, term()}
# Options: format (auto-detect), clear_graph (false), batch_size, progress_callback
```

**Key Design Decisions:**
- For formats without native graph support (Turtle, N-Triples), override the default graph
- Use existing format detection from Loader
- Support clear_graph option to replace existing data in target graph
- Use existing batch loading and progress reporting infrastructure

### 2.5.2 Multi-Graph Loading (COMPLETED)

**Module:** Same as above

**Tasks:**
- [x] 2.5.2.1 Implement `load_files_to_graphs/3` with graph map
- [x] 2.5.2.2 Accept `%{graph_term => file_path}` mapping
- [x] 2.5.2.3 Load each file to its designated graph
- [x] 2.5.2.4 Support parallel loading (configurable)
- [x] 2.5.2.5 Return summary of quads loaded per graph

**API Design:**
```elixir
@spec load_files_to_graphs(db_ref(), TripleStore.Dictionary.Manager.manager(), %{RDF.IRI.t() | RDF.BlankNode.t() => Path.t()}, keyword()) ::
        {:ok, %{RDF.IRI.t() | RDF.BlankNode.t() => non_neg_integer()}} | {:error, term()}
# Options: parallel (false), on_conflict (:continue), progress_callback
```

**Key Design Decisions:**
- Map keys are graph terms, values are file paths
- :parallel option enables concurrent loading (default false for safety)
- :on_conflict determines behavior on error: :continue, :stop, or :abort
- Returns summary map with counts per graph
- Telemetry events for each file loaded

---

## Part 3: Tests (COMPLETED)

### Test File: `test/triple_store/graph_scoped_loading_test.exs` (new file)

- [x] 2.5.3.1 Test load_to_graph with Turtle file to named graph
- [x] 2.5.3.2 Test load_to_graph with N-Triples file to named graph
- [x] 2.5.3.3 Test load_to_graph overrides default graph from source
- [x] 2.5.3.4 Test load_to_graph with clear_graph option
- [x] 2.5.3.5 Test load_to_graph with non-existent file
- [x] 2.5.3.6 Test load_to_graph with invalid format
- [x] 2.5.3.7 Test load_files_to_graphs with multiple files
- [x] 2.5.3.8 Test load_files_to_graphs sequential loading
- [x] 2.5.3.9 Test load_files_to_graphs parallel loading
- [x] 2.5.3.10 Test load_files_to_graphs with on_conflict: :stop
- [x] 2.5.3.11 Test load_files_to_graphs with on_conflict: :abort
- [x] 2.5.3.12 Test load_files_to_graphs returns correct summary
- [x] 2.5.3.13 Test load_files_to_graphs with progress callback
- [x] 2.5.3.14 Test load_to_graph preserves graph term type (IRI vs BlankNode)
- [x] 2.5.3.15 Test load_to_graph creates graph if it doesn't exist

**Test Results: 19 tests, 0 failures**

---

## Dependencies

- `TripleStore.Loader` - For existing load infrastructure
- `TripleStore.Adapter` - For quad conversion (from_rdf_quads/2)
- `TripleStore.QuadOperations` - For inserting quads
- `TripleStore.Dictionary.Manager` - For term-to-ID conversion
- `RDF.Turtle` - For Turtle format parsing
- `RDF.NTriples` - For N-Triples format parsing
- File format detection utilities from Loader

---

## Success Criteria

1. load_to_graph loads Turtle/N-Triples files to specified named graph
2. Default graph in source file is overridden with target graph
3. clear_graph option replaces existing data in target graph
4. load_files_to_graphs loads multiple files to separate graphs
5. Parallel loading option works correctly
6. All tests pass

---

## Notes

- Turtle and N-Triples don't have native graph support (all data goes to default graph)
- Graph-scoped loading overrides this by forcing all triples into the specified graph
- Need to handle RDF.IRI and RDF.BlankNode graph terms
- Parallel loading requires careful coordination of Dictionary Manager access
- Progress callback should report per-file progress in multi-graph loading
