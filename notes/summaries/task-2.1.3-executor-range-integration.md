# Task 2.1.3: Executor Range Query Integration - Summary

**Date:** 2026-01-02

## Overview

Integrated the numeric range index (from Task 2.1.1) into the SPARQL query executor. When a BGP pattern matches a range-filtered variable with a range-indexed predicate, the executor now uses `NumericRange.range_query/4` instead of the regular triple index, enabling efficient range scans directly from RocksDB.

This completes the Q7 Product-Offer Join optimization chain:
1. Task 2.1.1: Created NumericRange infrastructure
2. Task 2.1.2: Optimizer reorders BGP to prioritize range-filtered patterns
3. Task 2.1.3: Executor uses range index for actual queries

## Implementation

### 1. Extended Context Type

Added optional `range_context` to the executor context type:

```elixir
@type context :: %{
  :db => db(),
  :dict_manager => dict_manager(),
  optional(:range_context) => range_context()
}

@type range_context :: %{
  optional(:filter_context) => map(),
  optional(:range_indexed_predicates) => MapSet.t()
}
```

The range context contains:
- `filter_context`: Map with `:range_filtered_vars` (MapSet) and `:variable_ranges` (map of var_name -> {min, max})
- `range_indexed_predicates`: MapSet of predicate URIs that have range indices

### 2. Range Query Opportunity Detection

Added `check_range_query_opportunity/5` function that determines whether a pattern can use the range index:

```elixir
defp check_range_query_opportunity(ctx, _binding, _s, p, {:variable, var_name}) do
  range_context = Map.get(ctx, :range_context, %{})
  filter_context = Map.get(range_context, :filter_context, %{})
  range_indexed = Map.get(range_context, :range_indexed_predicates, MapSet.new())
  range_vars = Map.get(filter_context, :range_filtered_vars, MapSet.new())

  if MapSet.member?(range_vars, var_name) do
    case p do
      {:named_node, predicate_uri} ->
        if MapSet.member?(range_indexed, predicate_uri) do
          {:use_range_index, predicate_id, var_name, min_bound, max_bound}
        else
          :use_regular_index
        end
      _ -> :use_regular_index
    end
  else
    :use_regular_index
  end
end
```

**Conditions for range index usage:**
1. Object position is a variable (not bound)
2. Variable has a range filter (in `range_filtered_vars`)
3. Predicate is a named node with a range index (in `range_indexed_predicates`)

### 3. Range Pattern Execution

Added `execute_range_pattern/9` function that performs the actual range query:

```elixir
defp execute_range_pattern(ctx, binding, s, _p, _o, predicate_id, var_name, min_val, max_val) do
  %{db: db, dict_manager: dict_manager} = ctx

  :telemetry.execute(
    [:triple_store, :sparql, :executor, :range_index_used],
    %{count: 1},
    %{predicate_id: predicate_id, var_name: var_name, min: min_val, max: max_val}
  )

  case NumericRange.range_query(db, predicate_id, min_val, max_val) do
    {:ok, range_results} ->
      binding_stream =
        Stream.flat_map(range_results, fn {subject_id, float_value} ->
          value_term = {:literal, :typed, Float.to_string(float_value),
            "http://www.w3.org/2001/XMLSchema#decimal"}

          case maybe_bind(binding, s, subject_id, dict_manager) do
            {:ok, binding_with_subject} ->
              final_binding = Map.put(binding_with_subject, var_name, value_term)
              [final_binding]
            {:error, _} -> []
          end
        end)
      {:ok, binding_stream}
    {:error, _} = error -> error
  end
end
```

### 4. Telemetry

Added telemetry event for range index usage monitoring:

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:triple_store, :sparql, :executor, :range_index_used]` | `%{count: 1}` | `%{predicate_id, var_name, min, max}` |

## Key Functions Added

| Function | Description |
|----------|-------------|
| `check_range_query_opportunity/5` | Detects if pattern can use range index |
| `execute_range_pattern/9` | Executes range index query |
| `execute_regular_pattern/5` | Refactored regular index execution |

## Files Modified

| File | Changes |
|------|---------|
| `lib/triple_store/sparql/executor.ex` | +65 lines - range context types, detection, execution |
| `test/triple_store/sparql/executor_test.exs` | +160 lines - 4 integration tests |
| `notes/planning/performance/phase-02-bsbm-query-optimization.md` | Mark task complete |

## Test Coverage

Added 4 integration tests in `describe "range index integration"`:

| Test | Description |
|------|-------------|
| BGP without range context | Verifies fallback to regular index |
| BGP with non-matching predicate | Verifies fallback when predicate not indexed |
| Range index telemetry | Verifies telemetry event emission |
| Range query correct results | Verifies filtering by bounds |

## Usage Example

```elixir
# Create context with range information
ctx = %{
  db: db,
  dict_manager: dict_manager,
  range_context: %{
    filter_context: %{
      range_filtered_vars: MapSet.new(["price"]),
      variable_ranges: %{"price" => {50.0, 500.0}}
    },
    range_indexed_predicates: MapSet.new(["http://example.org/price"])
  }
}

# Execute BGP - will use range index for price pattern
Executor.execute(ctx, bgp_algebra)
```

## Performance Impact

With this integration complete, BSBM Q7 can now:
1. Start execution from the price pattern (via optimizer reordering from 2.1.2)
2. Use range index to get only offers with price in [50, 500] range
3. Join to products using already-bound offer IDs

Expected improvement: 1393ms → <100ms (target)

## Next Steps

Section 2.1 is now complete. The next section is:

**Section 2.2: Q6 Single Product Lookup**
- 2.2.1: BIND push-down for constant values
- 2.2.2: Multi-property fetch for single subject
- 2.2.3: Subject cache for frequently accessed entities
