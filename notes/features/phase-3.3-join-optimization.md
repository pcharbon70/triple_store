# Phase 3.3 Join Optimization - Feature Plan

**Date:** 2026-01-02
**Branch:** `feature/phase-3.3-join-optimization`
**Source:** `notes/planning/performance/phase-03-query-engine-improvements.md`

## Overview

Refine the cost model and join ordering to leverage collected statistics from Phase 3.1. The current implementation uses heuristic selectivity factors; with real statistics, cost-based optimization becomes more accurate.

## Existing Implementation Status

| Component | Status | Location |
|-----------|--------|----------|
| CostModel | Exists | `lib/triple_store/sparql/cost_model.ex` |
| Cardinality | Exists | `lib/triple_store/sparql/cardinality.ex` |
| JoinEnumeration | Exists | `lib/triple_store/sparql/join_enumeration.ex` |
| Statistics | Complete | `lib/triple_store/statistics.ex` |
| Numeric Histograms | Complete | In Statistics module |

## Implementation Plan

### Task 1: Cost Model Refinement (3.3.1)

- [x] 1.1 Add `with_weights/2` function to CostModel for weight configuration
- [x] 1.2 Use predicate cardinality from stats for selectivity
- [x] 1.3 Use numeric histograms for range filter selectivity via `range_filter_cost/5`
- [x] 1.4 Add configurable cost weights module attribute (`@default_weights`)
- [x] 1.5 Add explain output with cost breakdown via `explain_cost/2`

### Task 2: Selectivity Estimation (3.3.2)

- [x] 2.1 Add `estimate_predicate_selectivity/2` to use stats properly
- [x] 2.2 Add `estimate_range_selectivity/4` using histograms
- [x] 2.3 Add `estimate_pattern_with_range/5` for pattern + range filter cardinality
- [x] 2.4 Statistics module already provides histogram-based selectivity

### Task 3: Join Enumeration Tuning (3.3.3)

- [x] 3.1 Statistics already passed to all cost estimation functions
- [x] 3.2 Greedy selection uses actual costs from CostModel
- [x] 3.3 Add `explain_plan/1` with cost breakdown to explain output

### Task 4: Leapfrog Triejoin Tuning (3.3.4)

- [x] 4.1 `leapfrog_cost/3` uses statistics to estimate benefit
- [x] 4.2 `should_use_leapfrog?/3` compares leapfrog vs hash join using costs
- [x] 4.3 `@leapfrog_min_patterns` threshold configured for statistics-based selection

### Task 5: Unit Tests (3.3.5)

- [x] 5.1 Test cost model with statistics (default_weights, with_weights, explain_cost)
- [x] 5.2 Test selectivity estimates (filter_cost, range_filter_cost)
- [x] 5.3 Test join ordering improvement (explain_plan tests)
- [x] 5.4 Test leapfrog selection (existing tests cover this)
- [x] 5.5 Test cardinality range functions (estimate_range_selectivity, estimate_pattern_with_range)

## Files Changed

| File | Changes |
|------|---------|
| `lib/triple_store/sparql/cost_model.ex` | Added `@default_weights`, `default_weights/0`, `with_weights/2`, `explain_cost/2`, `filter_cost/2`, `range_filter_cost/5` |
| `lib/triple_store/sparql/cardinality.ex` | Added `estimate_range_selectivity/4`, `estimate_pattern_with_range/5`, `estimate_predicate_selectivity/2` |
| `lib/triple_store/sparql/join_enumeration.ex` | Added `explain_plan/1` with `describe_tree/1` helpers |
| `test/triple_store/sparql/cost_model_test.exs` | Added tests for configuration and explain functions |
| `test/triple_store/sparql/cardinality_test.exs` | Added tests for range selectivity functions |
| `test/triple_store/sparql/join_enumeration_test.exs` | Added tests for explain_plan function |

## Current Status

**Started:** 2026-01-02
**Completed:** 2026-01-02
**Status:** Complete

All 163 SPARQL optimization tests pass. Full test suite runs clean (4493 tests, 0 failures).
