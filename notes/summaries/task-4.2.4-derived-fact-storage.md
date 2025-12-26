# Task 4.2.4: Derived Fact Storage Summary

**Date:** 2025-12-26
**Branch:** feature/4.2.4-derived-fact-storage

## Overview

This task implements the storage layer for derived (inferred) facts from OWL 2 RL reasoning. Derived facts are stored separately from explicit facts in a dedicated `derived` column family, enabling:

1. **Incremental rematerialization**: Clear all derived facts and recompute
2. **Provenance tracking**: Distinguish explicit from inferred knowledge
3. **Query optimization**: Query only explicit, only derived, or both

## Implementation

### New Module: `TripleStore.Reasoner.DerivedStore`

Location: `lib/triple_store/reasoner/derived_store.ex`

#### Storage Operations

| Function | Description |
|----------|-------------|
| `insert_derived/2` | Batch insert derived facts |
| `insert_derived_single/2` | Insert a single derived fact |
| `delete_derived/2` | Batch delete derived facts |
| `derived_exists?/2` | Check if a derived fact exists |
| `clear_all/1` | Clear all derived facts (for rematerialization) |
| `count/1` | Count total derived facts |

#### Query Operations

| Function | Description |
|----------|-------------|
| `lookup_derived/2` | Query derived facts matching a pattern |
| `lookup_explicit/2` | Query explicit facts (delegates to Index) |
| `lookup_all/2` | Query both explicit and derived facts |
| `lookup_derived_all/2` | Collect all matching derived facts into a list |

#### SemiNaive Integration

| Function | Description |
|----------|-------------|
| `make_lookup_fn/2` | Create lookup function for SemiNaive with source selection |
| `make_store_fn/1` | Create store function for SemiNaive to persist derived facts |

### Pattern Conversion

The module handles conversion between Rule pattern format and Index pattern format:

```elixir
# Rule patterns use:
{:pattern, [{:var, :x}, {:const, value}, {:var, :y}]}

# Index patterns use:
{:var, {:bound, value}, :var}
```

The `convert_rule_pattern/1` function handles this translation automatically in the lookup functions.

### Storage Design

Derived facts use a single `derived` column family with the same key encoding as the SPO index:

```
Key format: <<subject::64-big, predicate::64-big, object::64-big>>
Value: <<>> (empty - presence indicates fact exists)
```

This provides O(log n) lookups for any pattern starting with bound subject.

## Usage Examples

```elixir
# Store derived facts
DerivedStore.insert_derived(db, [{s1, p1, o1}, {s2, p2, o2}])

# Query derived facts only
{:ok, stream} = DerivedStore.lookup_derived(db, {{:bound, 123}, :var, :var})

# Query both explicit and derived
{:ok, stream} = DerivedStore.lookup_all(db, pattern)

# Clear all derived facts for rematerialization
{:ok, count} = DerivedStore.clear_all(db)

# Integration with SemiNaive
lookup_fn = DerivedStore.make_lookup_fn(db, :both)
store_fn = DerivedStore.make_store_fn(db)
{:ok, stats} = SemiNaive.materialize(lookup_fn, store_fn, rules, initial_facts)
```

## Test Coverage

Location: `test/triple_store/reasoner/derived_store_test.exs`

36 new tests covering:

| Category | Tests |
|----------|-------|
| Storage operations | 12 tests |
| Query operations | 12 tests |
| SemiNaive integration | 8 tests |
| Integration with reasoning | 4 tests |

## Files Modified

| File | Changes |
|------|---------|
| `lib/triple_store/reasoner/derived_store.ex` | New module (475 lines) |
| `test/triple_store/reasoner/derived_store_test.exs` | New tests (460 lines) |

## Test Results

```
4 properties, 2672 tests, 0 failures
```

- Previous test count: 2636
- New tests added: 36
- All tests pass

## Key Design Decisions

1. **Existing Column Family**: Leveraged the pre-existing `:derived` column family in the RocksDB NIF, avoiding schema changes.

2. **Same Key Encoding**: Uses identical SPO key encoding as explicit facts for consistent behavior.

3. **Pattern Conversion**: Automatic translation between Rule patterns (`{:const, v}`) and Index patterns (`{:bound, v}`) in lookup functions.

4. **Source Selection**: The `make_lookup_fn/2` function accepts a source parameter (`:explicit`, `:derived`, `:both`) for flexible querying during reasoning.

5. **Stream-based Queries**: Lookup functions return streams for lazy evaluation, with convenience functions that collect to lists.

## Integration Points

The DerivedStore integrates with:

- **SemiNaive**: Through `make_lookup_fn/2` and `make_store_fn/1` callbacks
- **Index**: For explicit fact lookups via `lookup_explicit/2`
- **RocksDB NIF**: For low-level storage operations

## Next Steps

Task 4.2.5 (Unit Tests) will add additional tests:
- Test delta computation finds new facts only
- Test fixpoint terminates correctly
- Test fixpoint produces complete inference closure
- Test parallel evaluation produces same results as sequential
- Test derived facts stored separately
- Test clear derived removes only inferred triples
