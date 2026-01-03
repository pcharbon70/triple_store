# Phase 4.3 Snapshot Management - Feature Plan

**Date:** 2026-01-03
**Branch:** `feature/phase-4.3-snapshot-management`
**Source:** `notes/planning/performance/phase-04-storage-layer-tuning.md`

## Overview

Long-lived snapshots prevent RocksDB compaction and retain old data, causing storage bloat and performance degradation. This feature implements TTL-based snapshot management with automatic cleanup.

## Current State

- Snapshots created via `NIF.snapshot/1` in `lib/triple_store/backend/rocksdb/nif.ex`
- Manual release required via `NIF.release_snapshot/1`
- No tracking of snapshot lifecycle or ownership
- No automatic cleanup mechanism

## Solution

Implement a three-layer snapshot management system:

### 1. Snapshot Registry (GenServer)
- Tracks all active snapshots with metadata (owner PID, creation time, TTL)
- Monitors owner processes for automatic cleanup on termination
- Periodic sweep for expired snapshots (every minute)

### 2. TTL Support
- Default TTL: 5 minutes
- Configurable per-snapshot TTL
- Soft TTL warnings before expiry

### 3. Safe Wrapper API
- `with_snapshot/2` ensuring cleanup on success/exception
- Automatic registration and cleanup

## Implementation Plan

### Task 1: Create Snapshot module with registry
- [x] Create `lib/triple_store/snapshot.ex` module
- [x] Implement GenServer for snapshot registry
- [x] Track: snapshot_ref, owner_pid, created_at, ttl, warned?

### Task 2: Implement snapshot lifecycle management
- [x] `create/2` - Create snapshot with TTL, register, monitor owner
- [x] `release/1` - Explicit release and unregister
- [x] Auto-release on owner process termination
- [x] Periodic TTL check every 60 seconds

### Task 3: Implement safe wrapper
- [x] `with_snapshot/3` - Execute function with automatic cleanup
- [x] Handle exceptions, ensure release in all cases
- [x] Return result or re-raise exception

### Task 4: Add telemetry
- [x] snapshot.created event
- [x] snapshot.released event (with reason: manual/expired/owner_down)
- [x] snapshot.expired_warning event (at 80% TTL threshold)

### Task 5: Add tests
- [x] Test TTL expiration
- [x] Test owner process termination cleanup
- [x] Test with_snapshot success/exception paths
- [x] Test registry cleanup
- [x] Test telemetry events (16 tests total)

## API Design

```elixir
# Create snapshot with default 5-minute TTL
{:ok, snapshot} = TripleStore.Snapshot.create(db_ref)

# Create snapshot with custom TTL
{:ok, snapshot} = TripleStore.Snapshot.create(db_ref, ttl: :timer.minutes(10))

# Safe wrapper - automatically releases snapshot
TripleStore.Snapshot.with_snapshot(db_ref, fn snapshot ->
  # Use snapshot here
  NIF.snapshot_get(snapshot, :spo, key)
end)

# Explicit release
:ok = TripleStore.Snapshot.release(snapshot)

# Get info about active snapshots
info = TripleStore.Snapshot.info()
count = TripleStore.Snapshot.count()
```

## Current Status

**Started:** 2026-01-03
**Completed:** 2026-01-03
**Status:** Complete
