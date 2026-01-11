# Working Plan: Section 3.6 - Query Results Serialization

## Branch: `feature/section-3.6-query-results-serialization`

## Status: COMPLETED

## Overview

Section 3.6 implements query results serialization for quad store queries,
enabling proper handling of graph variables in SELECT results and CONSTRUCT
queries that return RDF.Datasets for named graph queries.

---

## Part 1: Graph Variable in Results (3.6.1)

### Task 1.1: Understand current serialization
- [x] Review `to_select_results/2` in query.ex or executor
- [x] Understand how bindings are converted to results
- [x] Check if graph variables are already handled

**File:** `lib/triple_store/sparql/executor.ex`

**Result:** Graph variables are already handled correctly. The `to_select_results/2` function includes all variables from the binding map, including graph variables.

### Task 1.2: Verify graph variable serialization
- [x] Test that graph IRIs are serialized as standard RDF terms
- [x] Verify graph variable appears in SELECT results
- [x] Verify default graph is implicit (not included in results)

**File:** `test/triple_store/sparql/serialization_test.exs`

**Result:** 3 tests verify graph variable serialization in SELECT results.

### Task 1.3: Document graph variable serialization behavior
- [x] Add documentation about graph variable in results
- [x] Document default graph handling
- [x] Add examples of graph variable serialization

**File:** Documentation in executor.ex

---

## Part 2: CONSTRUCT with Graph (3.6.2)

### Task 2.1: Understand current CONSTRUCT implementation
- [x] Review how CONSTRUCT queries are executed
- [x] Check current return type (RDF.Graph vs RDF.Dataset)
- [x] Understand how graph context is handled

**File:** `lib/triple_store/sparql/executor.ex`

**Result:** The `to_construct_result/4` function returns RDF.Graph for default graph queries. Modified to return RDF.Dataset for named graph queries.

### Task 2.2: Implement CONSTRUCT for named graphs
- [x] Return RDF.Dataset when query has named graphs
- [x] Include graph context for each triple
- [x] Handle queries with multiple named graphs

**File:** `lib/triple_store/sparql/executor.ex`

**Changes:**
- Modified `to_construct_result/4` to detect graph variables in bindings
- Added `instantiate_template_with_graph/2` to produce quads with graph context
- Added `build_dataset_from_terms/3` to convert internal quads to RDF.Dataset
- Added `extract_graph_names/2` to collect graph names from instantiated quads
- Added `internal_to_rdf/1` to convert internal terms to RDF.ex terms

### Task 2.3: Add tests for CONSTRUCT with graphs
- [x] Test CONSTRUCT from single named graph
- [x] Test CONSTRUCT from multiple graphs
- [x] Test CONSTRUCT with graph variable
- [x] Verify RDF.Dataset is returned

**File:** `test/triple_store/sparql/serialization_test.exs`

**Result:** 8 tests verify CONSTRUCT behavior with default graph (RDF.Graph) and named graphs (RDF.Dataset).

---

## Part 3: Unit Tests

### Task 3.1: Serialization tests
- [x] Test SELECT results include graph binding
- [x] Test SELECT * with graph variable
- [x] Test graph IRI serialized correctly

**File:** `test/triple_store/sparql/serialization_test.exs`

### Task 3.2: CONSTRUCT tests
- [x] Test CONSTRUCT from default graph returns RDF.Graph
- [x] Test CONSTRUCT from named graph returns RDF.Dataset
- [x] Test CONSTRUCT from multiple graphs returns RDF.Dataset with all graphs
- [x] Test CONSTRUCT with graph variable

**File:** `test/triple_store/sparql/serialization_test.exs`

---

## Summary

**Status: COMPLETED**

This section implements query results serialization:
1. [x] Graph variable in SELECT results
2. [x] CONSTRUCT queries returning RDF.Dataset for named graphs

**Test Results:** 11 tests, 0 failures

## Implementation Notes

### Graph Variable Serialization

When a query includes a graph variable (from GRAPH ?g { ... }), the graph
IRI should be serialized in the SELECT results like any other variable.

Example:
```sparql
SELECT ?g ?s WHERE {
  GRAPH ?g { ?s a ex:Person }
}
```

Should return results where `?g` is bound to graph IRIs.

### CONSTRUCT with Named Graphs

CONSTRUCT queries over named graphs should return RDF.Dataset instead
of RDF.Graph. Each triple should retain its graph context.

```sparql
CONSTRUCT { ?s ex:name ?o }
WHERE {
  GRAPH ?g { ?s ex:name ?o }
}
```

Should return an RDF.Dataset with triples in their respective graphs.

---

## Next Steps

After this section, the next phase would be to continue with additional SPARQL
query features or move to integration testing.

---

## Key Files Modified

1. `lib/triple_store/sparql/executor.ex` - CONSTRUCT execution with named graphs
2. `test/triple_store/sparql/serialization_test.exs` - New test file with 11 tests
