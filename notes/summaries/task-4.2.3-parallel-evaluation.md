# Task 4.2.3: Parallel Rule Evaluation Summary

**Date:** 2025-12-25
**Branch:** feature/4.2.3-parallel-evaluation

## Overview

This task adds parallel rule evaluation to the semi-naive evaluation loop. Rules within a stratum are independent and can be evaluated concurrently using `Task.async_stream`, improving performance on multi-core systems.

## Implementation

### Changes to `TripleStore.Reasoner.SemiNaive`

Location: `lib/triple_store/reasoner/semi_naive.ex`

#### New Options

Added two new options to `materialize/5`:

- `:parallel` - Enable parallel rule evaluation (default: false)
- `:max_concurrency` - Maximum parallel tasks (default: `System.schedulers_online()`)

#### New Functions

**`default_concurrency/0`**

Returns the default max concurrency (number of schedulers online).

```elixir
@spec default_concurrency() :: pos_integer()
def default_concurrency, do: @default_max_concurrency
```

**`materialize_parallel/5`**

Convenience function that enables parallel rule evaluation.

```elixir
@spec materialize_parallel(lookup_fn, store_fn, rules, initial_facts, opts) ::
  {:ok, stats()} | {:error, term()}
def materialize_parallel(lookup_fn, store_fn, rules, initial_facts, opts \\ [])
```

#### Implementation Details

**`apply_stratum/5`** now routes to either:
- `apply_stratum_sequential/4` - Original implementation
- `apply_stratum_parallel/5` - New parallel implementation

```elixir
defp apply_stratum(lookup_fn, %{rules: rules}, state, already_derived, parallel_opts) do
  if parallel_opts.parallel and length(rules) > 1 do
    apply_stratum_parallel(lookup_fn, rules, state, all_existing, parallel_opts.max_concurrency)
  else
    apply_stratum_sequential(lookup_fn, rules, state, all_existing)
  end
end
```

**`apply_stratum_parallel/5`** uses `Task.async_stream`:

```elixir
defp apply_stratum_parallel(lookup_fn, rules, state, all_existing, max_concurrency) do
  results =
    rules
    |> Task.async_stream(
      fn rule ->
        DeltaComputation.apply_rule_delta(lookup_fn, rule, state.delta, all_existing)
      end,
      max_concurrency: max_concurrency,
      ordered: false,  # Order doesn't matter for set union
      timeout: :infinity
    )
    |> Enum.to_list()

  merge_parallel_results(results, state.all_facts, length(rules))
end
```

### Determinism

Parallel evaluation is deterministic because:

1. **Set Union is Commutative**: Results are merged using `MapSet.union/2`, which produces the same output regardless of order
2. **Rules are Independent**: Each rule application reads from the same delta and existing facts
3. **No Shared Mutable State**: Each task works with immutable data structures

### Error Handling

The `collect_parallel_results/1` function handles:
- Successful results: `{:ok, {:ok, facts}}`
- Rule errors: `{:ok, {:error, reason}}`
- Task crashes: `{:exit, reason}`

## Test Coverage

Location: `test/triple_store/reasoner/semi_naive_test.exs`

6 new tests added:

| Test | Description |
|------|-------------|
| parallel produces same results as sequential | Verifies determinism |
| parallel with max_concurrency option | Tests custom concurrency |
| materialize_parallel convenience function works | Tests convenience API |
| default_concurrency returns positive integer | Tests default value |
| parallel handles many rules efficiently | Tests with multiple rules |
| parallel is deterministic across multiple runs | Runs 5x and compares |

## Files Modified

| File | Changes |
|------|---------|
| `lib/triple_store/reasoner/semi_naive.ex` | Added parallel evaluation (+120 lines) |
| `test/triple_store/reasoner/semi_naive_test.exs` | Added 6 parallel tests |

## Test Results

```
4 properties, 2636 tests, 0 failures
```

- Previous test count: 2630
- New tests added: 6
- All tests pass

## Usage Examples

```elixir
# Enable parallel evaluation
{:ok, stats} = SemiNaive.materialize(lookup_fn, store_fn, rules, facts,
  parallel: true
)

# With custom concurrency
{:ok, stats} = SemiNaive.materialize(lookup_fn, store_fn, rules, facts,
  parallel: true,
  max_concurrency: 4
)

# Convenience function
{:ok, stats} = SemiNaive.materialize_parallel(lookup_fn, store_fn, rules, facts)

# Check default concurrency
IO.puts("Using #{SemiNaive.default_concurrency()} cores")
```

## Performance Considerations

- Parallel execution adds overhead; only beneficial with multiple rules
- Single rule falls back to sequential (no overhead)
- `max_concurrency` defaults to `System.schedulers_online()`
- `ordered: false` allows tasks to complete in any order (faster)
- `timeout: :infinity` prevents timeouts during long rule evaluations

## Design Decisions

1. **Opt-in Parallelism**: Disabled by default to avoid overhead for small rule sets
2. **Single Rule Optimization**: Falls back to sequential for single rule (no task overhead)
3. **Unordered Results**: Uses `ordered: false` since set union is commutative
4. **Infinite Timeout**: Rule evaluation time is unbounded, so no timeout
5. **Task.async_stream**: Better than manual Task spawning for backpressure control

## Next Steps

Task 4.2.4 (Derived Fact Storage) will add:
- Store derived facts distinctly from explicit facts
- Use separate column family or tagging for derived facts
- Support incremental deletion on retraction
- Track provenance/justification for derived facts
