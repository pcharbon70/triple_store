# Section 2.7: Exporter Module Refactoring - Implementation Summary

## Branch: `feature/section-2.7-exporter-refactoring`

## Status: COMPLETED

## Overview

Section 2.7 refactors the Exporter module to provide a unified API for both triple and quad export. The goal was to make the quad store export functionality first-class while maintaining backward compatibility with existing triple-exporting code.

**Note**: Many quad features have already been implemented in sections 2.1-2.5. This section focused on API unification and graph-scoped export convenience functions.

## Implementation Details

### 1. Graph-Scoped Export Functions

**Module:** `lib/triple_store/exporter.ex`

**Functions Added:**

#### `export_dataset/2`
- Exports all quads from the store as an RDF.Dataset
- Signature: `export_dataset(db, opts \\ [])`
- Returns: `{:ok, RDF.Dataset.t()}` or `{:error, term()}`
- Uses `QuadOperations.lookup_quads` with `{:var, :var, :var, :var}` pattern
- No manager parameter needed (uses pattern lookup directly)

#### `export_graphs/4`
- Exports specific named graphs as an RDF.Dataset
- Signature: `export_graphs(db, manager, graphs, opts \\ [])`
- Returns: `{:ok, RDF.Dataset.t()}` or `{:error, term()}`
- Requires manager for term-to-ID conversion
- Supports `:include_default` option to include default graph
- Supports `:batch_size` option for batch processing

#### `export_default_graph/1`
- Exports only the default graph as an RDF.Graph
- Signature: `export_default_graph(db, opts \\ [])`
- Returns: `{:ok, RDF.Graph.t()}` or `{:error, term()}`
- Uses graph ID 0 for default graph
- Supports `:name`, `:base_iri`, `:prefixes` options

#### `export_single_graph/4`
- Exports a single named graph as an RDF.Graph
- Signature: `export_single_graph(db, manager, graph_term, opts \\ [])`
- Returns: `{:ok, RDF.Graph.t()}`, `{:error, :graph_not_found}`, or `{:error, term()}`
- Requires manager for term-to-ID conversion and existence check
- Supports `:name` option to override graph name

#### `export_multiple_graphs/4`
- Alias for `export_graphs/4` with clearer naming
- Signature: `export_multiple_graphs(db, manager, graphs, opts \\ [])`

### 2. Telemetry Support

**Module:** `lib/triple_store/exporter.ex`

**Enhancement:** Added `RDF.Dataset` case to `with_telemetry/2` function:
- Tracks `:graph_count` metric for dataset exports
- Emits `[:triple_store, :exporter, :stop]` event with duration
- Maintains consistency with existing telemetry for graphs and counts

### 3. API Design Decisions

#### Manager Parameter
Functions that need term-to-ID conversion require manager as a parameter:
- `export_graphs/4` - needs to convert graph IRIs to IDs
- `export_single_graph/4` - needs to convert graph IRI and check existence
- `export_multiple_graphs/4` - alias for export_graphs

Functions that don't need manager:
- `export_dataset/2` - uses pattern lookup with `:var`
- `export_default_graph/1` - uses hardcoded graph ID 0

This follows the pattern established in `QuadOperations`.

#### Return Types
- `export_dataset/2` → `RDF.Dataset` containing all graphs
- `export_graphs/4` → `RDF.Dataset` with specified graphs
- `export_default_graph/1` → `RDF.Graph` (no graph name)
- `export_single_graph/4` → `RDF.Graph` with graph's name

## Files Modified

1. `lib/triple_store/exporter.ex`
   - Added 5 new graph-scoped export functions (lines 595-912)
   - Added RDF.Dataset support to `with_telemetry/2` (lines 1213-1222)
   - Updated documentation with examples

2. `test/triple_store/exporter_refactoring_test.exs` (created)
   - 14 comprehensive tests covering all new functions
   - Tests for backward compatibility
   - Tests for error handling

## Test Results

**Test Coverage: 14 tests, 0 failures**

Test groups:
- `export_dataset/2` tests (2 tests)
- `export_graphs/4` tests (3 tests)
- `export_default_graph/1` tests (3 tests)
- `export_single_graph/4` tests (3 tests)
- `export_multiple_graphs/4` tests (1 test)
- Backward compatibility tests (2 tests)

**Regression Tests:** All existing tests pass (N-Quads, TriG export functions still work)

## API Examples

### Exporting All Quads as Dataset

```elixir
# Export entire quad store as RDF.Dataset
{:ok, dataset} = Exporter.export_dataset(db)
RDF.Dataset.graph_count(dataset)  # => 3 (all graphs)
```

### Exporting Specific Graphs

```elixir
# Export specific named graphs
graphs = [RDF.iri("http://example.org/g1"), RDF.iri("http://example.org/g2")]
{:ok, dataset} = Exporter.export_graphs(db, manager, graphs)

# Include default graph
{:ok, dataset} = Exporter.export_graphs(db, manager, graphs, include_default: true)
```

### Exporting Default Graph

```elixir
# Export only default graph as RDF.Graph
{:ok, graph} = Exporter.export_default_graph(db)

# With custom name
{:ok, graph} = Exporter.export_default_graph(db, name: RDF.iri("http://example.org/main"))
```

### Exporting Single Named Graph

```elixir
# Export a single named graph
graph_name = RDF.iri("http://example.org/mygraph")
{:ok, graph} = Exporter.export_single_graph(db, manager, graph_name)
RDF.Graph.triple_count(graph)  # => count of triples in that graph

# Returns error if graph doesn't exist
{:error, :graph_not_found} = Exporter.export_single_graph(db, manager, non_existent)
```

## Dependencies

- `TripleStore.Exporter` - Module being refactored
- `TripleStore.Adapter` - For quad conversion (to_rdf_quads/2, term_to_id/2)
- `TripleStore.QuadOperations` - For graph filtering and lookup
- `RDF.Dataset` - For quad containers
- `RDF.Graph` - For triple containers (backward compatibility)

## Success Criteria Met

1. ✅ New export functions support named graphs
2. ✅ Backward compatibility maintained (existing functions unchanged)
3. ✅ export_dataset returns RDF.Dataset for quad stores
4. ✅ Documentation updated with examples
5. ✅ All 14 new tests pass
6. ✅ All existing tests still pass

## Notes

- This is a refactoring section, not new features
- Most quad functionality already exists from sections 2.1-2.5
- Focus was on API consistency and developer experience
- Existing tests continue to pass without modification
- New tests verify the enhanced API
- Telemetry support added for dataset exports

## Next Steps

Ready to merge `feature/section-2.7-exporter-refactoring` into `quad` branch.
