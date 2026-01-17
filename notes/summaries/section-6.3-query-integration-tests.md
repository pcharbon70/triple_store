# Section 6.3: Query Integration Tests - Summary

**Date:** 2026-01-16
**Feature:** Section 6.3 Query Integration Tests
**Branch:** `feature/section-6.3-query-integration-tests`
**Status:** Partially Complete

## Overview

Implemented Section 6.3.1 of the quad store integration tests, focusing on GRAPH clause query execution. Currently 7 out of 19 tests are passing.

## Files Created

| File | Description | Status |
|------|-------------|--------|
| `test/triple_store/integration/graph_clause_query_test.exs` | GRAPH clause query tests | 7/19 passing |

## Test Results

### Passing Tests (7/19)

1. **6.3.1.1.2** - Returns empty results for non-existent graph
2. **6.3.1.1.3** - Supports IRI prefix in GRAPH clause
3. **6.3.1.2.1** - Executes SELECT from multiple graphs with UNION
4. **6.3.1.4.1** - SELECT without GRAPH clause queries default graph
5. **6.3.1.6.2** - OPTIONAL across graphs
6. **6.3.1.7.2** - UNION across different GRAPH clauses
7. **6.3.1.1.1** - Executes SELECT from single named graph using GRAPH clause

### Failing Tests (12/19)

**Graph Variable Tests (3 tests):**
- 6.3.1.3.1 - GRAPH clause with variable binds graph name
- 6.3.1.3.2 - Can filter by specific graph using graph variable
- 6.3.1.3.3 - Graph variable appears in result bindings

**FILTER Tests (3 tests):**
- 6.3.1.8.1 - FILTER within GRAPH clause
- 6.3.1.8.2 - FILTER on graph variable
- 6.3.1.8.3 - FILTER with regex in GRAPH clause

**OPTIONAL Tests (1 test):**
- 6.3.1.6.1 - GRAPH clause with OPTIONAL pattern

**UNION Tests (2 tests):**
- 6.3.1.2.2 - UNION returns distinct results
- 6.3.1.7.1 - UNION within GRAPH clause

**Nested GRAPH Tests (2 tests):**
- 6.3.1.5.1 - Supports GRAPH within GRAPH pattern
- 6.3.1.5.2 - Nested pattern with shared subject across graphs

**Default Graph Test (1 test):**
- 6.3.1.4.2 - Can explicitly query default graph with DEFAULT keyword (FILTER NOT EXISTS)

## Implementation Changes

### 1. Executor Enhancements (`lib/triple_store/sparql/executor.ex`)

**Added support for `{:named_node, iri}` format in `execute_graph/4`:**
```elixir
def execute_graph(ctx, {:named_node, iri}, pattern, initial_binding) do
  execute_in_named_graph(ctx, pattern, {:named_node, iri}, initial_binding)
end
```

**Fixed authorization handling for non-existent graphs:**
- Returns `{:ok, []}` (empty stream) instead of `{:error, :unauthorized}` when graph doesn't exist
- Checks `QuadOperations.graph_exists?/3` before returning unauthorized

**Improved `execute_quad_pattern/3` for proper quad BGP execution:**
```elixir
def execute_quad_pattern(ctx, {:bgp, quad_patterns}, initial_binding) do
  initial_stream = Stream.iterate(initial_binding, & &1) |> Stream.take(1)
  Enum.reduce_while(quad_patterns, {:ok, initial_stream}, fn quad_pattern, {:ok, stream} ->
    case extend_bindings(ctx, stream, quad_pattern) do
      {:ok, new_stream} -> {:cont, {:ok, new_stream}}
      {:error, _reason} = error -> {:halt, error}
    end
  end)
end
```

**Fixed `execute_with_graph_variable/4` Stream.resource format:**
- Changed return format from `{{:cont, values}, state}` to `{:cont, values, state}`
- Fixed RDF.IRI to internal format conversion: `%RDF.IRI{value: value}` -> `{:named_node, value}`

### 2. Test Enhancements (`test/triple_store/integration/graph_clause_query_test.exs`)

**Added PREFIX declarations to all queries:**
- All test queries now include `PREFIX ex: <http://example.org/>` declarations

**Added public permission setup:**
```elixir
:ok = Authorization.set_public(ctx, "#{@ex}graph1")
:ok = Authorization.set_public(ctx, "#{@ex}graph2")
:ok = Authorization.set_public(ctx, "#{@ex}graph3")
```

## Known Issues

### 1. Graph Variable Queries (GRAPH ?g)

The `execute_with_graph_variable/4` function has issues with the Stream.resource implementation. The error shows:

```
** (TryClauseError) no try clause matching: {:cont, [...], state}
```

**Root Cause:** Complex interaction between `take_batch/2` function and Stream.resource. The `take_batch` function may be returning problematic stream states when the underlying stream is exhausted.

**Potential Fix:** Simplify the stream handling by avoiding the complex batching logic and use a more straightforward stream concatenation approach.

### 2. FILTER within GRAPH Clause

FILTER expressions within GRAPH clauses may not be executing correctly. This requires investigation into how FILTER patterns are converted when wrapped in GRAPH contexts.

### 3. OPTIONAL within GRAPH Clause

Error: `{:error, {:unsupported_quad_pattern, {:left_join, ...}}}`

The LEFT_JOIN pattern (used for OPTIONAL) is not supported for quad patterns. The executor would need to be extended to handle LEFT_JOIN with quad patterns.

### 4. UNION Issues

The "UNION returns distinct results" test expects 3 results but gets 6, suggesting duplicate results are not being properly deduplicated.

### 5. FILTER NOT EXISTS

The FILTER NOT EXISTS pattern with default graph may not be implemented yet in the algebra.

## Next Steps

1. **Fix graph variable queries** - Simplify Stream.resource implementation or use alternative approach
2. **Implement LEFT_JOIN for quad patterns** - Extend executor to support OPTIONAL within GRAPH
3. **Fix FILTER execution** - Ensure FILTER works correctly within GRAPH clauses
4. **Fix UNION deduplication** - Investigate why UNION is returning duplicates
5. **Implement FILTER NOT EXISTS** - Add support for this pattern if not already present

## Dependencies

- `TripleStore.Loader` - Loading test data
- `TripleStore.Backend.RocksDB.NIF` - Database operations
- `TripleStore.Dictionary.Manager` - Dictionary encoding
- `TripleStore.SPARQL.Authorization` - Access control
- `TripleStore.SPARQL.Query` - Query execution
- `TripleStore.SPARQL.Executor` - Query execution engine
- `TripleStore.QuadOperations` - Quad CRUD operations

## Notes

- The Rust NIF parser (`sparql_parser_nif`) already has GraphPattern::Graph support via spargebra
- The Elixir executor needed updates to handle `{:named_node, iri}` format (previously only handled `{:iri, iri}`)
- Authorization checks now properly distinguish between "graph doesn't exist" (empty results) and "access denied" (error)
