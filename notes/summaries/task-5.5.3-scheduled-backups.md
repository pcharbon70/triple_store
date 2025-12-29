# Task 5.5.3: Scheduled Backups

**Date:** 2025-12-29
**Branch:** feature/task-5.5.3-scheduled-backups
**Tests:** 15 new tests, 4006 total tests pass

## Overview

This task completes the deferred scheduled backup functionality from Phase 5.5. The implementation provides a GenServer-based scheduler that runs periodic backups with automatic rotation.

## Implementation

### New Files

- `lib/triple_store/scheduled_backup.ex` - GenServer for scheduled backups

### Modified Files

- `lib/triple_store.ex` - Added `schedule_backup/3` public API function
- `lib/triple_store/backup.ex` - Added millisecond timestamps to `rotate/3`
- `lib/triple_store/telemetry.ex` - Added scheduled backup event definitions
- `notes/planning/phase-05-production-hardening.md` - Marked task complete

### Test Files

- `test/triple_store/scheduled_backup_test.exs` - 15 tests for scheduler

## API

### TripleStore.schedule_backup/3

```elixir
@spec schedule_backup(store(), Path.t(), keyword()) :: {:ok, pid()} | {:error, term()}
def schedule_backup(store, backup_dir, opts \\ [])
```

**Options:**
- `:interval` - Backup interval in milliseconds (default: 1 hour)
- `:max_backups` - Maximum backups to keep (default: 5)
- `:prefix` - Backup name prefix (default: "scheduled")
- `:run_immediately` - Run first backup immediately (default: false)

**Example:**
```elixir
# Start hourly backups, keeping last 24
{:ok, scheduler} = TripleStore.schedule_backup(store, "/backups/mydb",
  interval: :timer.hours(1),
  max_backups: 24
)

# Check status
{:ok, status} = TripleStore.ScheduledBackup.status(scheduler)

# Trigger immediate backup
{:ok, metadata} = TripleStore.ScheduledBackup.trigger_backup(scheduler)

# Stop scheduler
:ok = TripleStore.ScheduledBackup.stop(scheduler)
```

### TripleStore.ScheduledBackup

The GenServer provides additional functions:

- `status/1` - Get scheduler status (running, backup_count, last_backup, etc.)
- `trigger_backup/1` - Trigger immediate backup, resetting interval timer
- `stop/1` - Stop the scheduler gracefully

## Features

1. **Configurable Intervals**: Default 1 hour, can be set to any millisecond value
2. **Automatic Rotation**: Uses `Backup.rotate/3` to keep only N most recent backups
3. **Status Monitoring**: Track backup count, last backup time, errors
4. **Manual Trigger**: Force immediate backup when needed
5. **Telemetry Integration**: Emits events for monitoring

## Telemetry Events

### [:triple_store, :scheduled_backup, :tick]

Emitted on each scheduled backup attempt.

**Measurements:**
- `count` - Number of successful backups so far

**Metadata:**
- `backup_dir` - Backup directory path
- `interval_ms` - Configured interval

### [:triple_store, :scheduled_backup, :error]

Emitted when a scheduled backup fails.

**Measurements:** (empty)

**Metadata:**
- `reason` - Error reason
- `backup_dir` - Backup directory path

## Bug Fix: Backup Timestamp Collisions

Fixed an issue where rapid backups (faster than 1 second) could collide due to second-precision timestamps. The `Backup.rotate/3` function now includes milliseconds in backup names:

```
# Before: backup_20251229_093651
# After:  backup_20251229_093651_123
```

## Test Coverage

15 tests covering:
- Start/stop lifecycle
- Missing required options handling
- Status reporting
- Backup execution at intervals
- Immediate backup on start (`run_immediately: true`)
- Multiple backups over time
- Manual trigger functionality
- Backup rotation
- Telemetry event emission
- Main API integration
