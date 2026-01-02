# Phase 3.1 Statistics Collection Summary

**Date:** 2026-01-02
**Branch:** `feature/phase-3-statistics-collection`
**Source:** `notes/planning/performance/phase-03-query-engine-improvements.md`

## Overview

Implemented statistics collection for cost-based query optimization. The Statistics module now collects comprehensive statistics including predicate histograms and numeric histograms for range selectivity estimation.

## Key Features Implemented

### 1. Enhanced Statistics Collection

**File:** `lib/triple_store/statistics.ex`

Added `collect/1` function that gathers all statistics in a single operation:

```elixir
{:ok, stats} = Statistics.collect(db)
# Returns:
%{
  triple_count: 10000,
  distinct_subjects: 1000,
  distinct_predicates: 50,
  distinct_objects: 2000,
  predicate_histogram: %{42 => 500, 43 => 1500},
  numeric_histograms: %{price_id => %{...}},
  collected_at: ~U[2026-01-02 12:00:00Z],
  version: 1
}
```

### 2. Predicate Histogram

Added `build_predicate_histogram/1` to count triples per predicate:

```elixir
{:ok, histogram} = Statistics.build_predicate_histogram(db)
# Returns: %{predicate_id => count, ...}
```

### 3. Numeric Histograms for Range Selectivity

Added equi-width histograms for predicates with numeric values:

```elixir
# Build histogram for a predicate
{:ok, histogram} = Statistics.build_numeric_histogram(db, predicate_id, bucket_count)

# Estimate range selectivity
selectivity = Statistics.estimate_range_selectivity(stats, predicate_id, min, max)
```

Histograms automatically detect inline-encoded numeric types (integer, decimal, datetime).

### 4. Statistics Persistence

Statistics are persisted to RocksDB and can be reloaded:

```elixir
# Save statistics
:ok = Statistics.save(db, stats)

# Load persisted statistics
{:ok, stats} = Statistics.load(db)

# Get (load or collect if not saved)
{:ok, stats} = Statistics.get(db)

# Force refresh
{:ok, stats} = Statistics.refresh(db)
```

### 5. Statistics Server (GenServer)

**File:** `lib/triple_store/statistics/server.ex`

New GenServer for in-memory caching with auto-refresh:

```elixir
# Start server
{:ok, pid} = Statistics.Server.start_link(
  db: db,
  auto_refresh: true,
  refresh_threshold: 10_000,    # Refresh after 10K modifications
  refresh_interval: :timer.hours(1)  # Or every hour
)

# Get cached statistics
{:ok, stats} = Statistics.Server.get_stats()

# Force refresh
{:ok, stats} = Statistics.Server.refresh()

# Notify of modifications (triggers refresh at threshold)
Statistics.Server.notify_modification(count: 100)
```

### 6. Telemetry

Added telemetry events:

- `[:triple_store, :statistics, :collect]` - Collection timing and counts
- `[:triple_store, :statistics, :server, :refresh]` - Server refresh timing

## Implementation Details

### Statistics Storage

Statistics are stored in the `id2str` column family using a reserved key prefix (`<<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01>>`) that cannot conflict with term IDs.

### Numeric Type Detection

Inline-encoded numeric types are detected by their type tag (high 4 bits):
- `0b0100` - Integer
- `0b0101` - Decimal
- `0b0110` - DateTime

### Histogram Range Estimation

Range selectivity estimation uses fractional bucket counting:
1. Clamp query range to histogram bounds
2. Sum bucket counts weighted by overlap fraction
3. Return count / total as selectivity

## Files Changed

| File | Lines | Changes |
|------|-------|---------|
| `lib/triple_store/statistics.ex` | +500 | collect, save, load, histograms |
| `lib/triple_store/statistics/server.ex` | +330 | New GenServer |
| `test/triple_store/statistics_test.exs` | +340 | 21 new tests |
| `test/triple_store/statistics/server_test.exs` | +270 | 12 new tests |

## Test Results

All 4424 tests pass (49 statistics-related tests).

## Deferred Items

- **Incremental updates**: Currently uses full refresh; incremental delta updates deferred
- **Optimizer integration**: Statistics available but optimizer integration is Section 3.3

## Usage Example

```elixir
# Collect and save statistics
{:ok, db} = TripleStore.open("/path/to/db")
{:ok, stats} = TripleStore.Statistics.collect(db)
:ok = TripleStore.Statistics.save(db, stats)

# Use with cardinality estimation
stats.predicate_histogram[rdf_type_id]  # Get count for rdf:type

# Estimate range selectivity for price predicate
selectivity = Statistics.estimate_range_selectivity(stats, price_id, 10.0, 100.0)
```

## Next Steps

Phase 3 continues with:
- **Section 3.2**: Query Result Caching
- **Section 3.3**: Join Optimization (integrates statistics with cost model)
