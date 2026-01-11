# Working Plan: Section 3.5 - Solution Modifier Adaptation

## Branch: `feature/section-3.5-solution-modifier-adaptation`

## Status: COMPLETED

## Overview

Section 3.5 implements solution modifier (SELECT, GROUP BY, ORDER BY) adaptation
for quad store queries with graph variables. This enables proper handling of
graph variables in query results and aggregation.

---

## Part 1: Projection with Graph (3.5.1)

### Task 1.1: Update execute_project/3 to include graph variable
- [x] Modify projection to handle graph variable in bindings
- [x] When graph variable is projected, include it in results
- [x] When graph is not projected, exclude from results
- [x] Handle SELECT * expansion to include graph variable

**File:** `lib/triple_store/sparql/executor.ex`

### Task 1.2: Update all_variables/1 to detect graph variables
- [x] Scan patterns for graph variables
- [x] Include graph variables in SELECT * variable list
- [x] Distinguish between bound graphs and graph variables

**File:** `lib/triple_store/sparql/executor.ex` (or `algebra.ex`)

### Task 1.3: Add tests for projection with graph
- [x] Test SELECT with explicit graph variable
- [x] Test SELECT * includes graph variable
- [x] Test graph variable excluded when not projected
- [x] Test mixed projection (some vars, not graph)

**File:** `test/triple_store/sparql/solution_modifier_test.exs`

---

## Part 2: GROUP BY with Graph (3.5.2)

### Task 2.1: Update execute_group/3 to group by graph variable
- [x] Add graph variable to grouping key computation
- [x] Handle graph IRI in grouping key comparison
- [x] Support default graph in GROUP BY (if allowed)

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.2: Group results by graph IRI
- [x] Create grouping keys from graph variable values
- [x] Ensure consistent IRI comparison for grouping
- [x] Handle blank node graphs in grouping

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.3: Allow aggregates over graph groups
- [x] Support COUNT, SUM, etc. over graph groups
- [x] Ensure aggregates compute per graph group correctly

**File:** `lib/triple_store/sparql/executor.ex`

### Task 2.4: Add tests for GROUP BY with graph
- [x] Test GROUP BY ?g groups by graph
- [x] Test GROUP BY graph + other variables
- [x] Test aggregates with graph grouping
- [x] Test COUNT per graph

**File:** `test/triple_store/sparql/solution_modifier_test.exs`

---

## Part 3: ORDER BY with Graph (3.5.3)

### Task 3.1: Update execute_order_by/3 to sort by graph variable
- [x] Add graph variable to sort key extraction
- [x] Handle graph IRI comparison for sorting

**File:** `lib/triple_store/sparql/executor.ex`

### Task 3.2: Define graph ordering semantics
- [x] IRI lexical ordering for named graphs
- [x] Default graph position in ordering (define semantics)
- [x] Document ordering behavior

**File:** `lib/triple_store/sparql/executor.ex` (documentation)

### Task 3.3: Add tests for ORDER BY with graph
- [x] Test ORDER BY ?g sorts by graph IRI
- [x] Test ORDER BY with graph + other variables
- [x] Test ASC/DESC with graph variable
- [x] Test default graph position in ordering

**File:** `test/triple_store/sparql/solution_modifier_test.exs`

---

## Part 4: Unit Tests

### Task 4.1: Projection tests
- [x] Test SELECT with graph variable
- [x] Test SELECT * with graph variable
- [x] Test graph excluded when not in projection

**File:** `test/triple_store/sparql/solution_modifier_test.exs`

### Task 4.2: GROUP BY tests
- [x] Test GROUP BY graph variable
- [x] Test aggregates per graph
- [x] Test GROUP BY graph + other variables

**File:** `test/triple_store/sparql/solution_modifier_test.exs`

### Task 4.3: ORDER BY tests
- [x] Test ORDER BY graph variable
- [x] Test graph ordering direction
- [x] Test mixed graph/variable ordering

**File:** `test/triple_store/sparql/solution_modifier_test.exs`

---

## Summary

**Status: COMPLETED**

This section implements solution modifier adaptation:
1. [ ] Projection with graph variable
2. [ ] GROUP BY with graph variable
3. [ ] ORDER BY with graph variable

## Implementation Notes

### Graph Variable in Projection

Graph variables from GRAPH clauses should be included in SELECT results:

```sparql
SELECT ?g ?s WHERE {
  GRAPH ?g { ?s a ex:Person }
}
```

Should return bindings containing both `?g` (graph IRI) and `?s` (subject).

### Graph Variable in GROUP BY

```sparql
SELECT ?g (COUNT(?s) AS ?count)
WHERE {
  GRAPH ?g { ?s a ex:Person }
}
GROUP BY ?g
```

Should count persons per graph.

### Graph Variable in ORDER BY

```sparql
SELECT ?g ?s WHERE {
  GRAPH ?g { ?s a ex:Person }
}
ORDER BY ?g
```

Should order results by graph IRI (lexical ordering).

---

## Next Steps

After this section, Phase 3.6 (Query Results Serialization) will add support
for serializing query results containing graph variables.

---

## Key Files Modified

1. `lib/triple_store/sparql/executor.ex` - Solution modifier execution
2. `test/triple_store/sparql/solution_modifier_test.exs` - New test file
