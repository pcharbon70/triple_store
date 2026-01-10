# Section 2.4: Dataset Operations - Implementation Summary

## Branch: `feature/section-2.4-dataset-operations`

## Status: COMPLETED

## Overview

Section 2.4 implements dataset operations for managing named graphs in the quad store. These operations enable listing graphs, checking existence, deleting graphs, copying between graphs, and getting per-graph statistics. All operations work with the quad storage format that supports subject-predicate-object-graph tuples.

## Implementation Details

### 1. Graph Enumeration

**Module:** `lib/triple_store/quad_operations.ex`

**Functions Added:**
- `list_graphs/2` - Returns list of all named graphs as RDF.IRI or RDF.BlankNode terms
- `scan_distinct_graph_ids/1` - Scans GSPO index to find distinct graph IDs
- `convert_graph_ids_to_terms/2` - Converts graph IDs to RDF terms
- `graph_id_to_term/2` - Converts single graph ID to term (ID 0 maps to :default)
- `lookup_graph_term/2` - Looks up graph term by ID from dictionary

**Key Design Decisions:**
- Uses GSPO index prefix scan to find distinct graph values
- Excludes default graph (ID 0) by default, configurable with `include_default: true`
- Returns list of RDF terms (IRI or BlankNode) for named graphs
- Handles missing graph terms gracefully by returning placeholder IRIs

### 2. Graph Existence Checking

**Functions Added:**
- `graph_exists?/3` - Checks if named graph exists (requires manager for term-to-ID conversion)
- `default_graph_exists?/1` - Checks if default graph has any quads
- `graph_id_exists?/2` - Internal helper that checks if any quad exists for graph ID

**Key Design Decisions:**
- `graph_exists?` requires manager parameter to convert graph term to ID via Adapter
- Uses efficient count_quads_in_graph for existence check
- Returns boolean directly (not tuple) for convenience

### 3. Graph Deletion

**Functions Added:**
- `delete_graph/3` - Deletes all quads from specified graph
- `delete_all_quads_in_graph/2` - Internal helper using GSPO prefix scan

**Key Design Decisions:**
- Accepts :default, RDF.IRI, or RDF.BlankNode as graph term
- Requires manager parameter for term-to-ID conversion
- Deletes from all four indices (GSPO, GPOS, SPOG, POSG) atomically via WriteBatch
- Returns count of quads deleted
- Returns {:ok, 0} for non-existent graphs (idempotent)

### 4. Graph Copying

**Functions Added:**
- `copy_graph/5` - Copies quads from source to target graph
- `handle_copy_conflict/3` - Handles target graph existence based on conflict mode
- `do_copy_graph/3` - Performs actual copy operation

**Key Design Decisions:**
- Accepts :default, RDF.IRI, or RDF.BlankNode for both source and target
- Requires manager parameter for term-to-ID conversion
- `on_conflict` option: :merge (default), :replace, :error
- :merge - adds quads to existing target (idempotent insert handles duplicates)
- :replace - clears target first, then copies
- :error - fails with {:error, :graph_exists} if target exists
- Returns {:ok, 0} if source and target are the same graph
- Telemetry event with :copy_graph action

### 5. Graph Statistics

**Functions Added:**
- `graph_quad_count/3` - Returns count of quads in specified graph
- `graphs_summary/2` - Returns map of all graphs to their quad counts
- `count_quads_in_graph/2` - Counts quads for a specific graph ID
- `count_quads_by_graph_id/2` - Counts quads for multiple graph IDs

**Key Design Decisions:**
- `graph_quad_count` requires manager parameter for term-to-ID conversion
- `graphs_summary` returns map with graph terms as keys
- Default graph (ID 0) included by default, configurable with `include_default: false`
- Uses GSPO prefix scan for efficient counting

## Files Modified

1. `lib/triple_store/quad_operations.ex` - Added ~540 lines for dataset operations
2. `test/triple_store/dataset_operations_test.exs` - Created comprehensive test suite

## Test Results

**Test Coverage: 31 tests, 0 failures**

Test groups:
- list_graphs/2 tests (4 tests)
- graph_exists?/3 tests (3 tests)
- default_graph_exists?/1 tests (2 tests)
- delete_graph/3 tests (4 tests)
- copy_graph/4 tests (9 tests)
- graph_quad_count/3 tests (4 tests)
- graphs_summary/2 tests (4 tests)
- Integration tests (1 test)

## Dependencies

- `TripleStore.QuadOperations` - For quad storage operations
- `TripleStore.Adapter` - For term-to-ID conversion (requires manager)
- `TripleStore.Backend.RocksDB.NIF` - For RocksDB operations
- Quad index structure (GSPO, GPOS, SPOG, POSG) for prefix scans
- `RDF.IRI`, `RDF.BlankNode` for graph term types

## API Examples

### Listing Graphs

```elixir
# List all named graphs
{:ok, graphs} = QuadOperations.list_graphs(db)
# => {:ok, [%RDF.IRI{value: "http://example.org/g1"}, ...]}

# Include default graph in listing
{:ok, graphs} = QuadOperations.list_graphs(db, include_default: true)
# => {:ok, [:default, %RDF.IRI{value: "http://example.org/g1"}, ...]}
```

### Checking Graph Existence

```elixir
# Check named graph
QuadOperations.graph_exists?(db, manager, RDF.iri("http://example.org/g1"))
# => true

# Check default graph
QuadOperations.default_graph_exists?(db)
# => true
```

### Deleting Graphs

```elixir
# Delete named graph
{:ok, count} = QuadOperations.delete_graph(db, manager, RDF.iri("http://example.org/g1"))
# => {:ok, 42}

# Delete default graph (clears all data)
{:ok, count} = QuadOperations.delete_graph(db, manager, :default)
# => {:ok, 100}
```

### Copying Graphs

```elixir
# Merge into existing graph (default)
{:ok, count} = QuadOperations.copy_graph(db, manager, source_graph, target_graph)

# Replace target first
{:ok, count} = QuadOperations.copy_graph(db, manager, source_graph, target_graph, on_conflict: :replace)

# Fail if target exists
{:error, :graph_exists} = QuadOperations.copy_graph(db, manager, source, target, on_conflict: :error)
```

### Getting Statistics

```elixir
# Count quads in graph
{:ok, count} = QuadOperations.graph_quad_count(db, manager, RDF.iri("http://example.org/g1"))
# => {:ok, 42}

# Summary of all graphs
{:ok, summary} = QuadOperations.graphs_summary(db)
# => {:ok, %{%RDF.IRI{value: "http://example.org/g1"} => 42, :default => 100}}
```

## Success Criteria Met

1. ✅ list_graphs returns all named graphs
2. ✅ list_graphs excludes default graph by default, includes with option
3. ✅ graph_exists? correctly reports graph existence
4. ✅ default_graph_exists? checks default graph
5. ✅ delete_graph removes all quads from specified graph
6. ✅ delete_graph handles default graph deletion
7. ✅ copy_graph copies quads between graphs with conflict modes
8. ✅ copy_graph handles :merge, :replace, and :error options
9. ✅ graph_quad_count returns accurate counts
10. ✅ graphs_summary returns per-graph statistics
11. ✅ All 31 tests pass

## Notes

- Manager parameter required for graph term to ID conversion (uses Adapter.term_to_id)
- Default graph has ID 0 in storage, maps to :default atom in API
- Operations are atomic where possible (using WriteBatch for multi-index deletes)
- Insert is idempotent - copying graphs with overlapping quads doesn't create duplicates
- GSPO index used for all graph-scoped operations (prefix scans)
- Telemetry events emitted for copy operations

## Next Steps

Ready to merge `feature/section-2.4-dataset-operations` into `quad` branch.
