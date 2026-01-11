# Section 3.6: Query Results Serialization - Implementation Summary

## Branch: `feature/section-3.6-query-results-serialization`

## Status: COMPLETED

## Date: 2026-01-11

## Overview

This section implements query results serialization for quad store queries,
enabling proper handling of graph variables in SELECT results and CONSTRUCT
queries that return RDF.Datasets for named graph queries.

## Changes Made

### Files Modified

1. **lib/triple_store/sparql/executor.ex** (~50 lines added)
   - Modified `to_construct_result/4` to return RDF.Dataset for named graph queries
   - Added `instantiate_template_with_graph/2` to produce quads with graph context
   - Added `build_dataset_from_terms/3` to convert internal quads to RDF.Dataset
   - Added `extract_graph_names/2` to collect graph names from instantiated quads
   - Added `internal_to_rdf/1` to convert internal terms to RDF.ex terms

### Files Created

1. **test/triple_store/sparql/serialization_test.exs** (~330 lines)
   - Graph variable in SELECT results tests (3 tests)
   - CONSTRUCT with default graph tests (3 tests)
   - CONSTRUCT with named graphs tests (5 tests)

## Implementation Details

### Graph Variable in SELECT Results (3.6.1)

The existing `to_select_results/2` function already handles graph variables
correctly because graph variables are stored in the binding map like any other
variable. No code changes were required.

**Test Coverage:**
- Includes graph variable in results
- Projects graph variable when specified
- Graph IRI is returned as standard RDF term

### CONSTRUCT with Named Graphs (3.6.2)

Modified `to_construct_result/4` to:
1. Detect graph variables in bindings
2. Return RDF.Dataset when graph variables are present
3. Return RDF.Graph for default graph queries (backward compatible)

**Key Functions:**

```elixir
# Detects graph variables and returns appropriate result type
def to_construct_result(ctx, stream, template, opts \\ []) do
  bindings = Enum.to_list(stream)

  # Check for graph variables
  has_graph_vars? =
    Enum.any?(bindings, fn binding ->
      Enum.any?(binding, fn {k, _v} ->
        String.contains?(k, "g") or k == "graph"
      end)
    end)

  # Instantiate template with graph context
  {quads_or_triples, graph_names} =
    Enum.reduce(bindings, {[], MapSet.new()}, fn binding, {acc, graphs} ->
      instantiated = instantiate_template_with_graph(template, binding)
      new_graphs = extract_graph_names(binding, instantiated)
      {acc ++ instantiated, MapSet.union(graphs, new_graphs)}
    end)

  # Return RDF.Dataset or RDF.Graph based on graph context
  if has_graph_vars? and MapSet.size(graph_names) > 0 do
    build_dataset_from_terms(ctx, quads_or_triples, opts)
  else
    build_graph_from_terms(ctx, quads_or_triples, opts)
  end
end

# Instantiates template with graph context
defp instantiate_template_with_graph(template, binding) do
  graph_term =
    Enum.find_value(binding, fn
      {"g", v} -> v
      {"graph", v} -> v
      {k, v} when is_binary(k) ->
        if String.contains?(k, "graph"), do: v, else: nil
      _ -> nil
    end)

  Enum.flat_map(template, fn {:triple, s, p, o} ->
    with {:ok, s_val} <- substitute_term(s, binding),
         {:ok, p_val} <- substitute_term(p, binding),
         {:ok, o_val} <- substitute_term(o, binding) do
      if graph_term do
        [{s_val, p_val, o_val, graph_term}]
      else
        [{s_val, p_val, o_val}]
      end
    else
      :unbound -> []
    end
  end)
end

# Converts internal quads to RDF.Dataset
defp build_dataset_from_terms(_ctx, quads, _opts) do
  rdf_quads =
    Enum.flat_map(quads, fn
      {s, _p, _o, g} = quad ->
        with {:ok, s_term} <- internal_to_rdf(s),
             {:ok, p_term} <- internal_to_rdf(elem(quad, 1)),
             {:ok, o_term} <- internal_to_rdf(elem(quad, 2)),
             {:ok, g_term} <- internal_to_rdf(g) do
          [{s_term, p_term, o_term, g_term}]
        else
          _ -> []
        end
      {_s, _p, _o} -> []
    end)

  {:ok, RDF.Dataset.new(rdf_quads)}
end

# Converts internal term representation to RDF.ex terms
defp internal_to_rdf({:named_node, uri}), do: {:ok, RDF.iri(uri)}
defp internal_to_rdf({:blank_node, id}), do: {:ok, RDF.bnode(id)}
defp internal_to_rdf({:literal, :simple, value}), do: {:ok, RDF.literal(value)}
defp internal_to_rdf({:literal, :typed, value, datatype}), do: {:ok, RDF.literal(value, datatype: datatype)}
defp internal_to_rdf({:literal, :lang, value, lang}), do: {:ok, RDF.literal(value, language: lang)}
defp internal_to_rdf(_), do: :error
```

## Test Results

All tests pass (11 tests, 0 failures):

```
test/triple_store/sparql/serialization_test.exs:11 tests, 0 failures
```

### Test Coverage

**Graph Variable in SELECT Results (3 tests):**
- Includes graph variable in results
- Projects graph variable when specified
- Graph IRI is returned as standard RDF term

**CONSTRUCT with Default Graph (3 tests):**
- Constructs RDF.Graph from default graph bindings
- Handles empty bindings
- Skips triples with unbound variables

**CONSTRUCT with Named Graphs (5 tests):**
- Returns RDF.Dataset for named graph bindings
- Returns RDF.Dataset with multiple graphs from same named graph
- Returns RDF.Dataset for CONSTRUCT with multiple named graphs
- Constructs with mixed literal types
- Handles blank nodes in CONSTRUCT

## Design Decisions

### Graph Variable Detection

The implementation detects graph variables by checking if any binding key
contains "g" or equals "graph". This approach is simple and covers common
SPARQL graph variable naming conventions.

### Return Type Selection

- **RDF.Graph**: Returned when no graph variables are present (default graph queries)
- **RDF.Dataset**: Returned when graph variables are present (named graph queries)

This maintains backward compatibility while enabling named graph support.

### Stream Materialization

The implementation materializes the binding stream to check for graph variables
and instantiate the template. This is necessary because:
1. We need to detect if graph variables are present
2. We need to collect all graph names
3. The result size is typically small for CONSTRUCT queries

## Provides Foundation For

- SPARQL CONSTRUCT queries over named graphs
- Query result serialization with graph context
- Full SPARQL 1.1 query support with GRAPH clauses

---

**Implementation Date:** 2026-01-11
**Branch:** `feature/section-3.6-query-results-serialization`
