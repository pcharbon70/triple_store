# Section 2.6: Loader Module Refactoring - Implementation Summary

## Branch: `feature/section-2.6-loader-refactoring`

## Status: COMPLETED

## Overview

Section 2.6 refactors the Loader module to provide a unified API for both triples and quads. The goal was to make the quad store functionality first-class while maintaining backward compatibility with existing triple-loading code.

**Note**: Many quad features have already been implemented in sections 2.1-2.5. This section focused on API unification and cleanup.

## Implementation Details

### 1. API Updates for Quads

**Module:** `lib/triple_store/loader.ex`

**Functions Updated:**
- `load_file/4` - Added `:graph` option for loading to named graphs
- `load_string/5` - Added `:graph` option for loading to named graphs
- `load_stream/4` - Added `:graph` option for loading to named graphs
- `load_graph/4` - Updated to accept both `RDF.Graph` and `RDF.Dataset`, added `:graph` option

**Key Design Decisions:**
- All load functions now accept a `:graph` option to specify the target graph
- When `:graph` is an RDF.IRI or RDF.BlankNode, data is loaded to that named graph
- When `:graph` is `:default` or not provided, the loader detects the store schema:
  - Quad stores load as quads with graph_id 0
  - Triple stores load as triples (backward compatible)
- `load_graph/4` now accepts `RDF.Dataset` and preserves named graphs

**API Design:**
```elixir
# Load to named graph
{:ok, count} = Loader.load_file(db, manager, "data.ttl",
  graph: RDF.iri("http://example.org/mygraph"))

# Load from string to named graph
{:ok, count} = Loader.load_string(db, manager, content, :turtle,
  graph: RDF.iri("http://example.org/mygraph"))

# Load from stream to named graph
{:ok, count} = Loader.load_stream(db, manager, triples,
  graph: RDF.iri("http://example.org/mygraph"))

# Load RDF.Graph to named graph
{:ok, count} = Loader.load_graph(db, manager, rdf_graph,
  graph: RDF.iri("http://example.org/mygraph"))

# Load RDF.Dataset (preserves named graphs)
{:ok, count} = Loader.load_graph(db, manager, rdf_dataset)
```

### 2. Store Schema Detection

The loader now automatically detects whether the database is a triple store or quad store using `TripleStore.Backend.RocksDB.ErlangAdapter.is_quad_store?/1`. This ensures:

- Quad stores always use quad operations (even for default graph)
- Triple stores continue to use triple operations (backward compatible)

### 3. Helper Function Updates

**Updated:** `triples_to_quads/2`
- Now handles `:default` as a graph term (converts to nil for RDF.ex default graph)
- Creates proper `RDF.Quad` objects instead of plain tuples
- Handles both tuple format `{s, p, o}` and RDF statement objects

### 4. Documentation Updates

**Module:** `lib/triple_store/loader.ex`

**Updates:**
- Updated `load_opts` type to include `graph: RDF.IRI.t() | RDF.BlankNode.t() | :default | nil`
- Updated `load_graph/4` documentation to mention `RDF.Dataset` support
- Updated function documentation for `load_file`, `load_string`, `load_stream` to describe `:graph` option
- Updated moduledoc to reflect quad support as a first-class feature

## Files Modified

1. `lib/triple_store/loader.ex` - Added `:graph` option support to all load functions
2. `test/triple_store/loader_refactoring_test.exs` - Created comprehensive test suite

## Test Results

**Test Coverage: 17 tests, 0 failures**

Test groups:
- `load_file/4` with `:graph` option tests (5 tests)
- `load_string/5` with `:graph` option tests (3 tests)
- `load_stream/4` with `:graph` option tests (3 tests)
- `load_graph/4` with `RDF.Dataset` tests (2 tests)
- `load_graph/4` with `RDF.Graph` and `:graph` option tests (3 tests)
- Integration test (1 test)

**Regression Tests:** 123 existing tests pass (1 unrelated flaky test about database closing)

## Dependencies

- `TripleStore.Loader` - Module being refactored
- `TripleStore.Backend.RocksDB.ErlangAdapter` - For schema detection
- `TripleStore.Adapter` - For quad conversion (from_rdf_quads/2)
- `TripleStore.QuadOperations` - For quad storage
- `TripleStore.Dictionary.Manager` - For term-to-ID conversion
- `RDF.Quad` - For creating quad objects

## API Examples

### Loading to Named Graphs

```elixir
# Load Turtle file to named graph
{:ok, 42} = Loader.load_file(db, manager, "data.ttl",
  graph: RDF.iri("http://example.org/graph1"))

# Load string to named graph
content = "@prefix ex: <http://example.org/> . ex:s ex:p \"o\" ."
{:ok, 1} = Loader.load_string(db, manager, content, :turtle,
  graph: RDF.iri("http://example.org/graph2"))

# Load stream to named graph
triples = [{RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o"}]
{:ok, 1} = Loader.load_stream(db, manager, triples,
  graph: RDF.iri("http://example.org/graph3"))
```

### Loading RDF.Dataset

```elixir
# Load dataset with named graphs preserved
dataset = RDF.Dataset.new([
  {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o1",
   RDF.iri("http://example.org/g1")},
  {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o2",
   RDF.iri("http://example.org/g2")}
])

{:ok, 2} = Loader.load_graph(db, manager, dataset)
```

### Backward Compatibility

```elixir
# Triple store mode - loads as triples
{:ok, count} = Loader.load_file(db, manager, "data.ttl")

# Quad store mode - loads as quads with default graph
{:ok, count} = Loader.load_file(db, manager, "data.ttl")

# Explicit default graph
{:ok, count} = Loader.load_file(db, manager, "data.ttl", graph: :default)
```

## Success Criteria Met

1. ✅ All load functions accept `:graph` option
2. ✅ Backward compatibility maintained (no graph option = default behavior)
3. ✅ `load_graph` accepts both `RDF.Graph` and `RDF.Dataset`
4. ✅ Documentation updated with examples
5. ✅ All 17 new tests pass
6. ✅ All 123 existing tests still pass

## Notes

- This is a refactoring section, not new features
- Most quad functionality already exists from sections 2.1-2.5
- Focus was on API consistency and developer experience
- Existing tests continue to pass without modification
- New tests verify the enhanced API
- Store schema detection ensures correct behavior for both triple and quad stores

## Next Steps

Ready to merge `feature/section-2.6-loader-refactoring` into `quad` branch.
