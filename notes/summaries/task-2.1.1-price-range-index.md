# Task 2.1.1: Price Range Index - Summary

**Date:** 2026-01-02

## Overview

Implemented a numeric range index infrastructure to enable efficient range queries on numeric predicates. This is the foundation for optimizing BSBM Q7, which currently takes 1393ms due to full table scans before applying price filters.

## Implementation

### 1. RocksDB Column Family

Added a new `numeric_range` column family to store range index entries:

**Files Modified:**
- `native/rocksdb_nif/src/lib.rs`
  - Added `"numeric_range"` to `CF_NAMES` array (now 7 column families)
  - Added `numeric_range` atom
  - Updated `cf_atom_to_name/1` function
  - Updated `list_column_families/0` function

- `lib/triple_store/backend/rocksdb/nif.ex`
  - Added `:numeric_range` to `column_family` type
  - Updated documentation

### 2. Float-to-Sortable-Bytes Encoding

Implemented IEEE 754 float conversion for lexicographic ordering:

```elixir
# Positive floats: flip sign bit (0 -> 1)
# Negative floats: flip all bits

def float_to_sortable_bytes(value) do
  <<bits::64-unsigned-big>> = <<value::64-float-big>>
  sorted_bits =
    if (bits >>> 63) == 1 do
      bxor(bits, 0xFFFFFFFFFFFFFFFF)
    else
      bxor(bits, 0x8000000000000000)
    end
  <<sorted_bits::64-unsigned-big>>
end
```

This ensures:
- Negative floats sort before positive floats
- Larger negative floats sort after smaller negative floats
- Proper ordering for all IEEE 754 double-precision values

### 3. NumericRange Module

Created `lib/triple_store/index/numeric_range.ex` with the following functions:

| Function | Description |
|----------|-------------|
| `float_to_sortable_bytes/1` | Converts float to sortable 8-byte binary |
| `sortable_bytes_to_float/1` | Inverse conversion for decoding |
| `init/0` | Initializes ETS table for predicate registration |
| `create_range_index/2` | Registers a predicate for range indexing |
| `has_range_index?/1` | Checks if predicate is registered |
| `list_range_predicates/0` | Lists all registered predicates |
| `range_query/4` | Queries range with min/max bounds |
| `index_value/4` | Indexes a single value |
| `delete_value/4` | Removes a value from index |
| `build_index_operation/3` | Creates batch put operation |
| `build_delete_operation/3` | Creates batch delete operation |

### 4. Key Format

Index keys are 24 bytes in the format:

```
<<predicate_id::64-big, sortable_value::64-big, subject_id::64-big>>
```

This enables:
- Prefix scans by predicate
- Range scans by value within a predicate
- Multiple subjects with the same value

### 5. Unit Tests

Created `test/triple_store/index/numeric_range_test.exs` with 22 tests covering:

- Float-to-sortable-bytes ordering correctness
- Positive/negative float sorting
- Roundtrip encoding/decoding
- Predicate registration
- Range queries with bounds
- Unbounded min/max queries
- Index value insertion and deletion
- Batch operation building
- Stress test with 1000 random values

## Files Created/Modified

| File | Action | Lines |
|------|--------|-------|
| `native/rocksdb_nif/src/lib.rs` | Modified | +10 |
| `lib/triple_store/backend/rocksdb/nif.ex` | Modified | +3 |
| `lib/triple_store/index/numeric_range.ex` | Created | ~320 |
| `test/triple_store/index/numeric_range_test.exs` | Created | ~280 |
| `test/triple_store/backend/rocksdb/lifecycle_test.exs` | Modified | +2 |

## Test Results

```
22 tests, 0 failures
```

All NumericRange tests pass. Full test suite has 4290 tests with 1 pre-existing flaky failure unrelated to this change.

## Usage Example

```elixir
# Initialize the range index system
NumericRange.init()

# Register a predicate for range indexing
price_predicate_id = 12345
{:ok, _} = NumericRange.create_range_index(db, price_predicate_id)

# Index some values during data load
:ok = NumericRange.index_value(db, price_predicate_id, subject1, 99.99)
:ok = NumericRange.index_value(db, price_predicate_id, subject2, 149.99)

# Query a price range
{:ok, results} = NumericRange.range_query(db, price_predicate_id, 50.0, 500.0)
# Returns [{subject_id, price}, ...] sorted by price
```

## Next Steps

Task 2.1.1 provides the infrastructure. Follow-up tasks will:
- 2.1.2: Integrate with query optimizer for join reordering
- 2.1.3: Integrate with executor for range lookups
- 2.1.5: Add integration tests with BSBM data
