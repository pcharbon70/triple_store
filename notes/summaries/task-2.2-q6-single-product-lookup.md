# Task 2.2: Q6 Single Product Lookup - Summary

**Date:** 2026-01-02

## Overview

Implemented three optimizations to improve BSBM Q6 single-product lookup performance from 175ms towards <5ms target:

1. **BIND Push-Down**: Pre-binds constant IRIs before executing inner patterns
2. **Multi-Property Fetch**: Single SPO prefix scan instead of N separate lookups
3. **Subject Cache**: LRU cache for frequently accessed subject property maps

## 2.2.1 BIND Push-Down

### Problem

Q6 uses `BIND(<uri> AS ?product)` but the executor evaluated inner patterns without knowing `?product` was bound, resulting in inefficient lookups.

### Solution

Modified `query.ex` to detect constant expressions in EXTEND (BIND) and pre-populate bindings before inner pattern execution.

```elixir
# New helper function
defp try_constant_bind_pushdown(ctx, inner, var_name, expr, depth) do
  case extract_constant_value(expr) do
    {:ok, constant_value} ->
      :telemetry.execute([:triple_store, :sparql, :query, :bind_pushdown], ...)
      execute_with_initial_binding(ctx, inner, var_name, constant_value, depth)
    :not_constant ->
      :not_constant
  end
end

# Constant detection
defp extract_constant_value({:named_node, _} = term), do: {:ok, term}
defp extract_constant_value({:blank_node, _} = term), do: {:ok, term}
defp extract_constant_value({:literal, _, _} = term), do: {:ok, term}
defp extract_constant_value({:literal, _, _, _} = term), do: {:ok, term}
defp extract_constant_value(_), do: :not_constant
```

### Telemetry

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:triple_store, :sparql, :query, :bind_pushdown]` | `%{count: 1}` | `%{variable: var_name}` |

## 2.2.2 Multi-Property Fetch

### Problem

Q6 fetches 7 properties from the same subject, resulting in 7 separate index lookups.

### Solution

Added `Index.lookup_all_properties/2` for single SPO prefix scan:

```elixir
@spec lookup_all_properties(NIF.db_ref(), term_id()) ::
        {:ok, %{term_id() => [term_id()]}} | {:error, term()}
def lookup_all_properties(db, subject_id) do
  prefix = spo_prefix(subject_id)

  case NIF.prefix_stream(db, :spo, prefix) do
    {:ok, stream} ->
      properties =
        stream
        |> Enum.reduce(%{}, fn {key, _value}, acc ->
          {_s, p, o} = decode_spo_key(key)
          Map.update(acc, p, [o], fn objects -> [o | objects] end)
        end)
        |> Map.new(fn {p, objects} -> {p, Enum.reverse(objects)} end)
      {:ok, properties}
    {:error, _} = error -> error
  end
end
```

Also added `stream_all_properties/2` for lazy evaluation.

### Performance

- Before: N separate lookups (O(N * log M))
- After: Single prefix scan (O(M) where M = properties per subject)

## 2.2.3 Subject Cache

### Problem

Repeated queries for the same subject (common in BSBM) resulted in redundant index lookups.

### Solution

Created `TripleStore.Index.SubjectCache` with ETS-based LRU caching:

```elixir
# Initialize
SubjectCache.init()

# Configure cache size
SubjectCache.configure(max_entries: 1000)

# Get or fetch
{:ok, properties} = SubjectCache.get_or_fetch(db, subject_id)

# Invalidate on update
SubjectCache.invalidate(subject_id)

# Clear entire cache
SubjectCache.clear()
```

### Features

- **ETS-based**: Fast concurrent reads
- **LRU eviction**: Automatically evicts oldest entries at capacity
- **Telemetry**: Hit/miss/eviction events for monitoring
- **Configurable**: Cache size via `configure/1`

### Telemetry Events

| Event | Measurements | Metadata |
|-------|-------------|----------|
| `[:triple_store, :index, :subject_cache, :hit]` | `%{count: 1}` | `%{subject_id: id}` |
| `[:triple_store, :index, :subject_cache, :miss]` | `%{count: 1}` | `%{subject_id: id, property_count: n}` |
| `[:triple_store, :index, :subject_cache, :eviction]` | `%{count: 1}` | `%{subject_id: id}` |

## Files Created/Modified

| File | Action | Description |
|------|--------|-------------|
| `lib/triple_store/sparql/query.ex` | Modified | BIND push-down implementation (+80 lines) |
| `lib/triple_store/index.ex` | Modified | Multi-property fetch functions (+100 lines) |
| `lib/triple_store/index/subject_cache.ex` | Created | LRU cache module (~280 lines) |
| `test/triple_store/sparql/query_test.exs` | Modified | BIND push-down tests (+100 lines) |
| `test/triple_store/index/multi_property_test.exs` | Created | Multi-property tests (~140 lines) |
| `test/triple_store/index/subject_cache_test.exs` | Created | Cache tests (~320 lines) |

## Test Coverage

- **BIND push-down**: 3 tests (constant IRI push-down, variable expression not pushed, telemetry)
- **Multi-property fetch**: 8 tests (basic, multi-valued, empty, performance)
- **Subject cache**: 14 tests (init, configure, get/put, invalidate, clear, LRU eviction, stats)

All 25 new tests pass.

## Usage Example

```elixir
# BIND with constant IRI automatically pushed down
Query.query(ctx, """
  SELECT ?product ?name ?age WHERE {
    BIND(<http://example.org/Product1> AS ?product)
    ?product <http://ex.org/name> ?name .
    ?product <http://ex.org/age> ?age .
  }
""")
# ?product is now bound BEFORE inner BGP executes,
# resulting in efficient point lookups

# Multi-property fetch for subject details
{:ok, properties} = Index.lookup_all_properties(db, subject_id)
# => %{predicate_id_1 => [obj1], predicate_id_2 => [obj2, obj3], ...}

# Subject cache for repeated access
SubjectCache.configure(max_entries: 1000)
{:ok, props} = SubjectCache.get_or_fetch(db, subject_id)  # Cache miss, fetches
{:ok, props} = SubjectCache.get_or_fetch(db, subject_id)  # Cache hit, instant
```

## Expected Performance Impact

- **BIND push-down**: Converts full scans to point lookups
- **Multi-property fetch**: 7x fewer index operations for Q6-style queries
- **Subject cache**: O(1) for repeated subject access

Combined, these optimizations should reduce Q6 from 175ms towards <5ms target.

## Next Steps

**Section 2.3: Query Bug Fixes** is the next upcoming task:
- 2.3.1: Q5 Literal Matching Fix (typed literal mismatch)
- 2.3.2: Q11 URI Escaping Fix (fragment escaping issue)
