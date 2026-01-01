# Task 1.1.3: Batch Sequence Allocation Implementation

**Date**: 2026-01-01
**Branch**: `feature/batch-sequence-allocation`
**Status**: Complete

## Overview

Implemented batch sequence allocation to reduce contention on the SequenceCounter during bulk loading. Instead of allocating IDs one at a time via GenServer calls, the system now allocates ranges of IDs atomically.

## Changes Made

### Modified Files

1. **`lib/triple_store/dictionary/sequence_counter.ex`**
   - Added `allocate_range/3` function for atomic range allocation
   - Uses `:atomics.add_get/3` to atomically reserve N sequential IDs
   - Returns starting sequence number; caller uses start..start+count-1
   - Overflow detection with rollback on failure
   - Telemetry event `[:triple_store, :dictionary, :range_allocated]`
   - Added `maybe_flush_counter_range/4` for batch flush tracking

2. **`lib/triple_store/dictionary/manager.ex`**
   - Rewrote `do_get_or_create_ids/4` with optimized batch processing
   - Phase 1: Encode all terms and check cache/RocksDB for existing IDs
   - Phase 2: Group terms needing new IDs by type (uri, bnode, literal)
   - Phase 3: Allocate ranges for each type in single call per type
   - Phase 4: Assign IDs from ranges and store in RocksDB/cache
   - Removed unused `Batch` alias

### New Files

1. **`test/triple_store/dictionary/batch_sequence_test.exs`**
   - 19 comprehensive unit tests covering:
     - Range allocation in SequenceCounter
     - Manager batch processing with range allocation
     - Mixed existing and new terms
     - Large batch allocation
     - Concurrent range allocations (non-overlapping)
     - Crash recovery with safety margin
     - Telemetry integration

## Architecture

```
Before (per-term allocation):
┌──────────────────────────────────────────────────────────────┐
│  Batch of 1000 terms                                         │
│                                                              │
│  for term <- terms do                                        │
│    GenServer.call(counter, {:next_id, type})  # 1000 calls   │
│  end                                                         │
└──────────────────────────────────────────────────────────────┘

After (batch range allocation):
┌──────────────────────────────────────────────────────────────┐
│  Batch of 1000 terms                                         │
│                                                              │
│  # Phase 1: Check existing                                   │
│  # Phase 2: Count by type: {uri: 800, bnode: 100, literal: 100} │
│  # Phase 3: Allocate ranges (3 GenServer calls)              │
│  {:ok, uri_start} = allocate_range(:uri, 800)                │
│  {:ok, bnode_start} = allocate_range(:bnode, 100)            │
│  {:ok, literal_start} = allocate_range(:literal, 100)        │
│  # Phase 4: Assign from ranges (no GenServer calls)          │
└──────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

1. **Atomic Range Allocation**: Uses `:atomics.add_get/3` which atomically adds `count` and returns the new value. Start sequence is calculated as `new_value - count + 1`.

2. **Per-Type Allocation**: Each term type (uri, bnode, literal) gets its own range allocation, allowing the SequenceCounter to track and persist each independently.

3. **Overflow Protection**: Before committing a range, checks if end sequence exceeds max. On overflow, rolls back via `:atomics.sub/3` and returns error.

4. **Crash Recovery**: Existing safety margin mechanism (add 1000 on startup) already handles crash recovery. Range allocation doesn't change this - it just reduces the number of GenServer calls.

## API Changes

New function in `SequenceCounter`:

```elixir
@spec allocate_range(counter(), dict_type(), pos_integer()) ::
        {:ok, non_neg_integer()} | {:error, atom()}
def allocate_range(counter, type, count)

# Example:
{:ok, start} = SequenceCounter.allocate_range(counter, :uri, 100)
# Use sequences: start, start+1, ..., start+99
```

## Expected Performance Impact

For a batch of 1000 terms with typical distribution (800 URIs, 100 bnodes, 100 literals):

- **Before**: 1000 GenServer calls to SequenceCounter
- **After**: 3 GenServer calls (one per type with terms)

This reduces GenServer contention by ~99.7% for batch operations.

Combined with sharding (Task 1.1.1) and caching (Task 1.1.2):
- Repeated terms: Cache hit, no GenServer call
- New terms: Single range allocation per type, then local assignment

## Test Results

```
Finished in 3.8 seconds
336 dictionary tests, 0 failures, 2 excluded
```

## Next Steps

The next task in the plan is **1.1.4 Unit Tests** for Section 1.1:
- Test sharded manager distributes terms across shards evenly
- Test concurrent `get_or_create_id` from multiple processes
- Test ETS cache hit returns correct ID
- Test cache miss falls through to GenServer
- Test cache population on term creation
- Test batch operations partition correctly by shard
- Test sequence block allocation under contention
- Test dictionary consistency after parallel operations
