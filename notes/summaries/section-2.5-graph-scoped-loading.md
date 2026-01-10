# Section 2.5: Graph-Scoped Loading - Implementation Summary

## Branch: `feature/section-2.5-graph-scoped-loading`

## Status: COMPLETED

## Overview

Section 2.5 implements graph-scoped loading functionality. This allows loading RDF data (Turtle, N-Triples, etc.) directly into specific named graphs, overriding the default graph behavior of the source files. It also supports loading multiple files to separate graphs in a single operation.

## Implementation Details

### 1. Load to Specific Graph

**Module:** `lib/triple_store/loader.ex`

**Functions Added:**
- `load_to_graph/5` - Load a file to a specific named graph

**Key Design Decisions:**
- For Turtle/N-Triples files (which don't have native graph support), all triples are converted to quads with the target graph
- Uses existing RDF.ex parsing functions (Turtle, N-Triples, etc.)
- Supports `clear_graph` option to replace existing data in target graph
- Telemetry events with `format: :graph_scoped` metadata
- Uses existing quad loading infrastructure (load_quads, encode_quad_batch)

**API Design:**
```elixir
@spec load_to_graph(db_ref(), manager(), Path.t(), RDF.IRI.t() | RDF.BlankNode.t(), keyword()) ::
        {:ok, non_neg_integer()} | {:error, term()} | {:halted, non_neg_integer()}
# Options: format, batch_size, bulk_mode, parallel, clear_graph, progress_callback, max_file_size
```

### 2. Multi-Graph Loading

**Functions Added:**
- `load_files_to_graphs/3` - Load multiple files to multiple graphs
- `do_load_files_to_graphs/4` - Internal implementation
- `load_files_to_graphs_sequential/8` - Sequential loading implementation
- `load_files_to_graphs_parallel/5` - Parallel loading implementation
- `emit_start_telemetry/1` - Emit telemetry start event
- `triples_to_quads/2` - Convert triples to quads with graph term

**Key Design Decisions:**
- Accepts map of `%{graph_term => file_path}` as input
- `:parallel` option enables concurrent loading (default: false)
- `:on_conflict` option controls error handling:
  - `:continue` - Continue loading other files on error (default)
  - `:stop` - Stop loading on first error, return partial results
  - `:abort` - Abort entire operation, return error tuple
- `:clear_graphs` option clears all target graphs before loading
- Progress callback receives per-file progress information
- Returns summary map of `%{graph_term => quad_count}`

**API Design:**
```elixir
@spec load_files_to_graphs(db_ref(), manager(), %{(RDF.IRI.t() | RDF.BlankNode.t()) => Path.t()}, keyword()) ::
        {:ok, %{(RDF.IRI.t() | RDF.BlankNode.t()) => non_neg_integer()}} | {:error, term()}
# Options: parallel, on_conflict, batch_size, bulk_mode, clear_graphs, progress_callback
```

### 3. Helper Functions

**Added:**
- `triples_to_quads/2` - Converts stream of triples to quads with specific graph term
- `emit_start_telemetry/1` - Emits telemetry start event for multi-graph loading

## Files Modified

1. `lib/triple_store/loader.ex` - Added graph-scoped loading section (~220 lines)
2. `test/triple_store/graph_scoped_loading_test.exs` - Created comprehensive test suite

## Test Results

**Test Coverage: 19 tests, 0 failures**

Test groups:
- load_to_graph/5 tests (8 tests)
- load_files_to_graphs/3 tests (10 tests)
- Integration tests (1 test)

## Dependencies

- `TripleStore.Loader` - For existing load infrastructure
- `TripleStore.Adapter` - For quad conversion (from_rdf_quads/2)
- `TripleStore.QuadOperations` - For quad storage and graph operations
- `TripleStore.Dictionary.Manager` - For term-to-ID conversion
- `RDF.Turtle` - For Turtle format parsing
- `RDF.NTriples` - For N-Triples format parsing
- `Flow` - For parallel processing
- `Task.async_stream` - For parallel multi-graph loading

## API Examples

### Loading to a Specific Graph

```elixir
# Load Turtle file to named graph
{:ok, 42} = Loader.load_to_graph(db, manager, "data.ttl",
  RDF.iri("http://example.org/graph1"))

# Load with graph clearing
{:ok, count} = Loader.load_to_graph(db, manager, "update.nt",
  RDF.iri("http://example.org/graph1"), clear_graph: true)

# Load to blank node graph
{:ok, 10} = Loader.load_to_graph(db, manager, "data.ttl",
  RDF.bnode("temp_graph"))
```

### Loading Multiple Files to Multiple Graphs

```elixir
# Define file-to-graph mapping
graph_files = %{
  RDF.iri("http://example.org/g1") => "data1.ttl",
  RDF.iri("http://example.org/g2") => "data2.nt"
}

# Load all files
{:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files)
# => {:ok, %{%RDF.IRI{value: "http://example.org/g1"} => 42, ...}}

# With parallel loading
{:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files,
  parallel: true)

# With conflict handling
{:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files,
  on_conflict: :continue)

# Clear graphs before loading
{:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files,
  clear_graphs: true)
```

### Progress Callback for Multi-Graph Loading

```elixir
progress_callback = fn info ->
  IO.puts("Loaded #{info.loaded_so_far}/#{info.total_files} files")
  IO.puts("Current: #{info.file} to #{inspect(info.graph)}")
  :continue
end

{:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files,
  progress_callback: progress_callback)
```

## Success Criteria Met

1. ✅ load_to_graph loads Turtle/N-Triples files to specified named graph
2. ✅ Default graph in source is overridden with target graph
3. ✅ clear_graph option works correctly
4. ✅ load_files_to_graphs loads multiple files to separate graphs
5. ✅ Sequential and parallel loading modes work
6. ✅ Conflict handling options (:continue, :stop, :abort) work correctly
7. ✅ Progress callback works for multi-graph loading
8. ✅ All 19 tests pass

## Notes

- Graph terms (IRI or BlankNode) are preserved through the loading process
- Turtle and N-Triples don't have native graph support - all triples go to default graph in source
- Graph-scoped loading converts these triples to quads with the target graph term
- The Adapter.from_rdf_quads function handles graph term to ID conversion
- Parallel loading uses Task.async_stream with limited concurrency
- Halt callback stops before loading the next file (current summary is returned)

## Next Steps

Ready to merge `feature/section-2.5-graph-scoped-loading` into `quad` branch.
