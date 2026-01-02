# Task 2.1.2: Join Reordering for Price Filter - Summary

**Date:** 2026-01-02

## Overview

Implemented filter-aware BGP pattern reordering in the query optimizer. The optimizer now detects FILTER expressions with range comparisons (>=, <=, >, <) and boosts the selectivity of patterns that bind the filtered variables, especially when those patterns use predicates with range indices.

This is a key optimization for BSBM Q7, which filters offers by price range. Without this optimization, the executor would scan all products and offers before filtering. With this optimization, the price pattern is prioritized and can use the range index for efficient lookup.

## Implementation

### 1. Range Filter Extraction

Added `extract_range_filters/1` function that scans the algebra tree and extracts:
- Variable names that have range filters
- The min/max bounds for each filtered variable

**Supported filter patterns:**
- `?var >= value` / `value <= ?var` → min bound
- `?var <= value` / `value >= ?var` → max bound
- `?var > value` / `value < ?var` → min bound (exclusive)
- `?var < value` / `value > ?var` → max bound (exclusive)
- Conjunctive (AND) combinations are merged
- OR expressions are handled conservatively

### 2. Selectivity Boost

Modified `estimate_selectivity/3` to apply a selectivity boost when:
1. The pattern's object variable has a range filter
2. The pattern's predicate has a range index (100x boost)
3. The pattern's predicate doesn't have a range index but variable is filtered (10x boost)

### 3. Optimization Pipeline Integration

Added new option `:range_indexed_predicates` to `optimize/2`:
```elixir
Optimizer.optimize(algebra,
  range_indexed_predicates: MapSet.new(["http://ex.org/price"])
)
```

The pipeline now:
1. Extracts filter context before BGP reordering
2. Passes filter context and range index info to selectivity estimation
3. Patterns with range-filtered objects are prioritized in BGP ordering

## Key Functions Added

| Function | Description |
|----------|-------------|
| `extract_range_filters/1` | Scans algebra tree for range filters |
| `collect_filter_expressions/2` | Collects all FILTER expressions |
| `extract_range_comparisons/1` | Parses comparison expressions |
| `extract_numeric_value/1` | Extracts float from typed literals |
| `apply_range_filter_boost/5` | Applies selectivity boost |
| `binds_range_filtered_variable?/2` | Checks if pattern binds filtered var |
| `get_variable_range/2` | Gets min/max bounds for variable |

## Files Modified

| File | Changes |
|------|---------|
| `lib/triple_store/sparql/optimizer.ex` | +280 lines - range filter extraction and selectivity boost |
| `test/triple_store/sparql/optimizer_test.exs` | +130 lines - 12 new tests |
| `notes/planning/performance/phase-02-bsbm-query-optimization.md` | Mark task complete |

## Test Coverage

Added 12 new tests in 3 describe blocks:

**extract_range_filters/1 (9 tests):**
- Extracts greater-or-equal, less-or-equal filters
- Extracts conjunctive (AND) range filters
- Extracts strict greater/less comparisons
- Handles reversed comparisons (value <= var)
- Extracts decimal values
- Handles multiple filtered variables
- Returns empty for non-range filters

**selectivity boost (2 tests):**
- Pattern with range filter has lower selectivity score
- Boost is stronger with range index than without

**BGP reordering (1 test):**
- Places range-filtered pattern first in BGP

## Example: BSBM Q7 Optimization

**Before:**
```sparql
SELECT ?product ?offer ?price ?vendor WHERE {
  ?product rdf:type bsbm:Product .          # 1st: All products
  ?offer bsbm:product ?product .            # 2nd: All offers
  ?offer bsbm:price ?price .                # 3rd: All prices
  FILTER (?price >= 50 && ?price <= 500)    # 4th: Filter results
}
```

**After (with range index on bsbm:price):**
```sparql
SELECT ?product ?offer ?price ?vendor WHERE {
  ?offer bsbm:price ?price .                # 1st: Range index lookup
  FILTER (?price >= 50 && ?price <= 500)    # 2nd: Applied during scan
  ?offer bsbm:product ?product .            # 3rd: Bound by offer
  ?product rdf:type bsbm:Product .          # 4th: Type check
}
```

## Next Steps

Task 2.1.2 provides the optimizer reordering. Follow-up tasks will:
- 2.1.3: Integrate range index into executor for actual range lookups
- 2.1.5: Add integration tests verifying end-to-end optimization
