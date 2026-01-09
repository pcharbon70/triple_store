# Phase 2: RDF.ex Integration and Quad Loading

## Overview

Phase 2 implements the integration with RDF.ex for loading and storing quad formats (N-Quads, TriG). By the end of this phase, the quad store will support loading from all standard RDF serializations including those with named graphs.

The RDF.ex library already supports quad formats. This phase focuses on adapting our loader and exporter to handle the fourth graph component correctly.

---

## 2.1 Quad Term Conversion

### 2.1.1 Quad Type Definition

Define the internal quad representation.

- [ ] 2.1.1.1 Define `@type quad :: {term(), term(), term(), term()}`
- [ ] 2.1.1.2 Define `@type term :: term_id()` (dictionary IDs for storage)
- [ ] 2.1.1.3 Document quad ordering: subject, predicate, object, graph
- [ ] 2.1.1.4 Add guards for valid quad tuples

**New/Updated Module:** `lib/triple_store/rdf_adapter.ex`

### 2.1.2 RDF.ex Quad to Internal Conversion

Implement conversion from RDF.ex quad representation.

- [ ] 2.1.2.1 Implement `from_rdf_quad/2` converting RDF.Quad to internal quad
- [ ] 2.1.2.2 Handle `RDF.Quad` with `RDF.IRI` graph names
- [ ] 2.1.2.3 Handle `RDF.Quad` with `RDF.BlankNode` graph names
- [ ] 2.1.2.4 Handle default graph (nil graph in RDF.Quad) → @default_graph_id
- [ ] 2.1.2.5 Use existing `term_to_id/2` for term encoding

### 2.1.3 Internal Quad to RDF.ex Conversion

Implement conversion to RDF.ex quad representation.

- [ ] 2.1.3.1 Implement `to_rdf_quad/3` converting internal quad to RDF.Quad
- [ ] 2.1.3.2 Handle @default_graph_id → nil graph in RDF.Quad
- [ ] 2.1.3.3 Use existing `id_to_term/2` for term decoding
- [ ] 2.1.3.4 Preserve graph term type (IRI vs BlankNode)

### 2.1.4 Batch Quad Conversion

Implement efficient batch conversion operations.

- [ ] 2.1.4.1 Implement `from_rdf_quads/2` for stream conversion
- [ ] 2.1.4.2 Implement `to_rdf_quads/2` for reverse stream conversion
- [ ] 2.1.4.3 Optimize dictionary lookups via batch operations
- [ ] 2.1.4.4 Handle errors in individual quads gracefully

---

## 2.2 N-Quads Format Support

### 2.2.1 N-Quads Loading

Implement loading of N-Quads format files.

- [ ] 2.2.1.1 Extend `load_file/3` to recognize `.nq` extension
- [ ] 2.2.1.2 Use `RDF.Serializations.read_nquads/1` from RDF.ex
- [ ] 2.2.1.3 Extract graph name from each quad (4th component)
- [ ] 2.2.1.4 Load quads to appropriate named graphs
- [ ] 2.2.1.5 Handle default graph (no graph name) correctly

### 2.2.2 N-Quads Export

Implement export to N-Quads format.

- [ ] 2.2.2.1 Implement `export_nquads/2` for N-Quads serialization
- [ ] 2.2.2.2 Use `RDF.Serializations.write_nquads/2` from RDF.ex
- [ ] 2.2.2.3 Convert internal quads to RDF.Quad format
- [ ] 2.2.2.4 Handle default graph (no graph name in output)
- [ ] 2.2.2.5 Stream export for large datasets

### 2.2.3 N-Quads String Loading

Implement loading N-Quads from strings.

- [ ] 2.2.3.1 Implement `load_nquads_string/2` parsing N-Quads from string
- [ ] 2.2.3.2 Use `RDF.Serializations.read_nquads_string!/1`
- [ ] 2.2.3.3 Load parsed quads to database
- [ ] 2.2.3.4 Return count of quads loaded

---

## 2.3 TriG Format Support

### 2.3.1 TriG Loading

Implement loading of TriG format files.

- [ ] 2.3.1.1 Extend `load_file/3` to recognize `.trig` extension
- [ ] 2.3.1.2 Use `RDF.Serializations.read_trig/1` from RDF.ex
- [ ] 2.3.1.3 Load each named graph in dataset to corresponding graph
- [ ] 2.3.1.4 Handle default graph block in TriG
- [ ] 2.3.1.5 Clear existing data before load (configurable)

### 2.3.2 TriG Export

Implement export to TriG format.

- [ ] 2.3.2.1 Implement `export_trig/2` for TriG serialization
- [ ] 2.3.2.2 Use `RDF.Serializations.write_trig/2` from RDF.ex
- [ ] 2.3.2.3 Group quads by graph name for output
- [ ] 2.3.2.4 Write default graph without GRAPH wrapper
- [ ] 2.3.2.5 Stream export for large datasets

### 2.3.3 TriG String Loading

Implement loading TriG from strings.

- [ ] 2.3.3.1 Implement `load_trig_string/2` parsing TriG from string
- [ ] 2.3.3.2 Use `RDF.Serializations.read_trig_string!/1`
- [ ] 2.3.3.3 Load each graph to database
- [ ] 2.3.3.4 Return count of quads loaded per graph

---

## 2.4 Dataset Operations

### 2.4.1 Graph Enumeration

Implement listing of all named graphs in database.

- [ ] 2.4.1.1 Implement `list_graphs/1` returning list of graph names
- [ ] 2.4.1.2 Use GSPO index to find distinct graph IDs
- [ ] 2.4.1.3 Exclude default graph from listing (configurable)
- [ ] 2.4.1.4 Convert graph IDs to terms via dictionary
- [ ] 2.4.1.5 Return as list of RDF.IRI terms

### 2.4.2 Graph Existence

Implement checking for graph existence.

- [ ] 2.4.2.1 Implement `graph_exists?/2` for named graph check
- [ ] 2.4.2.2 Implement `default_graph_exists?/1` for default graph
- [ ] 2.4.2.3 Use GSPO prefix scan to check for any quad in graph
- [ ] 2.4.2.4 Return boolean result

### 2.4.3 Graph Deletion

Implement deletion of entire named graphs.

- [ ] 2.4.3.1 Implement `delete_graph/2` removing all quads from graph
- [ ] 2.4.3.2 Use GSPO prefix scan to find all quads in graph
- [ ] 2.4.3.3 Delete from all four indices atomically
- [ ] 2.4.3.4 Handle default graph deletion (clear all data)
- [ ] 2.4.3.5 Return count of quads deleted

### 2.4.4 Graph Copying

Implement copying quads between graphs.

- [ ] 2.4.4.1 Implement `copy_graph/3` copying quads from source to target graph
- [ ] 2.4.4.2 Use GSPO prefix scan on source graph
- [ ] 2.4.4.3 Insert quads with new graph ID
- [ ] 2.4.4.4 Handle graph already exists (merge/replace options)
- [ ] 2.4.4.5 Return count of quads copied

### 2.4.5 Graph Statistics

Implement per-graph statistics.

- [ ] 2.4.5.1 Implement `graph_quad_count/2` returning count of quads in graph
- [ ] 2.4.5.2 Implement `graphs_summary/1` returning stats for all graphs
- [ ] 2.4.5.3 Return map: `%{graph_term => quad_count}`
- [ ] 2.4.5.4 Include default graph in summary (configurable)

---

## 2.5 Graph-Scoped Loading

### 2.5.1 Load to Specific Graph

Implement loading data to a specific named graph.

- [ ] 2.5.1.1 Implement `load_to_graph/4` with explicit graph parameter
- [ ] 2.5.1.2 Support Turtle, N-Triples loaded to named graph
- [ ] 2.5.1.3 Override default graph in source with target graph
- [ ] 2.5.1.4 Create graph if it doesn't exist
- [ ] 2.5.1.5 Return count of quads loaded

### 2.5.2 Multi-Graph Loading

Implement loading multiple files to separate graphs.

- [ ] 2.5.2.1 Implement `load_files_to_graphs/2` with graph map
- [ ] 2.5.2.2 Accept `%{graph_name => file_path}` mapping
- [ ] 2.5.2.3 Load each file to its designated graph
- [ ] 2.5.2.4 Support parallel loading (configurable)
- [ ] 2.5.2.5 Return summary of quads loaded per graph

---

## 2.6 Loader Module Refactoring

### 2.6.1 API Updates for Quads

Update the Loader module public API.

- [ ] 2.6.1.1 Change `load_graph/2` to handle RDF.Dataset (quad-capable)
- [ ] 2.6.1.2 Add optional `graph:` parameter to all load functions
- [ ] 2.6.1.3 Default `graph:` to `:default` for backward compatibility
- [ ] 2.6.1.4 Update function specifications for quad types
- [ ] 2.6.1.5 Update documentation with graph parameter examples

### 2.6.2 Internal Adaptation

Adapt internal loader functions for quads.

- [ ] 2.6.2.1 Update `do_load/4` to handle 4-component quads
- [ ] 2.6.2.2 Use `QuadIndex.insert_quad/3` instead of `Index.insert_triple/3`
- [ ] 2.6.2.3 Pass graph ID through loading pipeline
- [ ] 2.6.2.4 Update batching logic for 32-byte keys
- [ ] 2.6.2.5 Update progress reporting for quads

### 2.6.3 Error Handling

Add graph-specific error handling.

- [ ] 2.6.3.1 Add `{:error, :invalid_graph_term}` for invalid graph names
- [ ] 2.6.3.2 Add `{:error, :graph_not_found}` for operations on missing graphs
- [ ] 2.6.3.3 Add `{:error, :default_graph_protected}` for protected operations
- [ ] 2.6.3.4 Document error conditions in @moduledoc

---

## 2.7 Exporter Module Refactoring

### 2.7.1 Export API Updates

Update the Exporter module for quad formats.

- [ ] 2.7.1.1 Add `export_nquads/2` for N-Quads export
- [ ] 2.7.1.2 Add `export_trig/2` for TriG export
- [ ] 2.7.1.3 Update `export_graph/2` to return RDF.Dataset
- [ ] 2.7.1.4 Add `export_graphs/2` for specific graph selection
- [ ] 2.7.1.5 Add `export_default_graph/1` convenience function

### 2.7.2 Graph-Scoped Export

Implement exporting specific graphs.

- [ ] 2.7.2.1 Implement `export_single_graph/3` for one named graph
- [ ] 2.7.2.2 Implement `export_multiple_graphs/3` for list of graphs
- [ ] 2.7.2.3 Use GSPO prefix scan for graph filtering
- [ ] 2.7.2.4 Return RDF.Dataset with specified graphs
- [ ] 2.7.2.5 Handle default graph parameter correctly

### 2.7.3 Format Detection

Add automatic format detection for export.

- [ ] 2.7.3.1 Detect `.nq` extension for N-Quads format
- [ ] 2.7.3.2 Detect `.trig` extension for TriG format
- [ ] 2.7.3.3 Fall back to N-Triples for `.nt` (default graph only)
- [ ] 2.7.3.4 Fall back to Turtle for `.ttl` (default graph only)
- [ ] 2.7.3.5 Document format selection in @moduledoc

---

## 2.8 Unit Tests

### 2.8.1 Term Conversion Tests

- [ ] 2.8.1.1 Test RDF.Quad with IRI graph converts correctly
- [ ] 2.8.1.2 Test RDF.Quad with blank node graph converts correctly
- [ ] 2.8.1.3 Test RDF.Quad with nil graph uses default_graph_id
- [ ] 2.8.1.4 Test internal quad converts to RDF.Quad correctly
- [ ] 2.8.1.5 Test default_graph_id converts to nil graph in RDF.Quad
- [ ] 2.8.1.6 Test batch conversion handles multiple quads

### 2.8.2 N-Quads Tests

- [ ] 2.8.2.1 Test loading N-Quads file with named graphs
- [ ] 2.8.2.2 Test loading N-Quads file with default graph
- [ ] 2.8.2.3 Test loading N-Quads file with mixed graphs
- [ ] 2.8.2.4 Test export to N-Quads preserves graph names
- [ ] 2.8.2.5 Test N-Quads roundtrip (load + export)
- [ ] 2.8.2.6 Test N-Quads string loading

### 2.8.3 TriG Tests

- [ ] 2.8.3.1 Test loading TriG file with multiple graphs
- [ ] 2.8.3.2 Test loading TriG with default graph block
- [ ] 2.8.3.3 Test export to TriG preserves graph structure
- [ ] 2.8.3.4 Test TriG roundtrip (load + export)
- [ ] 2.8.3.5 Test TriG string loading

### 2.8.4 Dataset Operations Tests

- [ ] 2.8.4.1 Test list_graphs returns all named graphs
- [ ] 2.8.4.2 Test list_graphs excludes default graph by default
- [ ] 2.8.4.3 Test graph_exists? returns true for existing graph
- [ ] 2.8.4.4 Test graph_exists? returns false for non-existent graph
- [ ] 2.8.4.5 Test delete_graph removes all quads from graph
- [ ] 2.8.4.6 Test delete_default_graph clears all data
- [ ] 2.8.4.7 Test copy_graph duplicates quads correctly
- [ ] 2.8.4.8 Test copy_graph to existing graph merges correctly
- [ ] 2.8.4.9 Test graph_quad_count returns accurate count
- [ ] 2.8.4.10 Test graphs_summary returns correct statistics

### 2.8.5 Graph-Scoped Loading Tests

- [ ] 2.8.5.1 Test load_to_graph puts data in specified graph
- [ ] 2.8.5.2 Test load_to_graph with :default puts in default graph
- [ ] 2.8.5.3 Test load_to_graph creates graph if needed
- [ ] 2.8.5.4 Test load_files_to_graphs loads each file correctly
- [ ] 2.8.5.5 Test load_files_to_graphs handles parallel loading

### 2.8.6 Export Tests

- [ ] 2.8.6.1 Test export_graph returns RDF.Dataset with all graphs
- [ ] 2.8.6.2 Test export_graphs filters to specified graphs
- [ ] 2.8.6.3 Test export_single_graph returns single graph dataset
- [ ] 2.8.6.4 Test export_default_graph returns only default graph
- [ ] 2.8.6.5 Test format detection selects correct format
- [ ] 2.8.6.6 Test N-Quads export format is valid
- [ ] 2.8.6.7 Test TriG export format is valid

### 2.8.7 Error Handling Tests

- [ ] 2.8.7.1 Test invalid graph term returns error
- [ ] 2.8.7.2 Test operation on non-existent graph returns error
- [ ] 2.8.7.3 Test protected default graph operations handled
- [ ] 2.8.7.4 Test corrupt N-Quads file returns error
- [ ] 2.8.7.5 Test corrupt TriG file returns error

---

## Success Criteria

1. **N-Quads**: Load and export N-Quads format correctly
2. **TriG**: Load and export TriG format correctly
3. **Graph Operations**: List, delete, copy graphs correctly
4. **Graph-Scoped Load**: Load data to specific named graphs
5. **Roundtrip**: N-Quads/TriG roundtrip preserves all data
6. **API**: Loader and Exporter handle quads transparently

## Provides Foundation

This phase establishes the infrastructure for:
- **Phase 3**: Query execution with GRAPH clause
- **Phase 4**: SPARQL UPDATE with graph operations
- **Phase 5**: Quad-aware statistics and optimization

## Key Outputs

- Updated `TripleStore.RDFAdapter` with quad conversion
- N-Quads and TriG format support in Loader/Exporter
- Dataset operations (list, delete, copy graphs)
- Graph-scoped loading capabilities
