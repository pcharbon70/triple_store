# Task 5.5.2.4: Atomics Counter Restoration

**Date:** 2025-12-28
**Branch:** `feature/task-5.5.2.4-atomics-counter-restoration`

## Overview

This task implements atomics counter persistence and restoration for backup/restore operations. The sequence counter is critical for generating unique term IDs (URIs, blank nodes, literals) and must be properly handled during backup and restore to prevent ID collisions.

## Problem Statement

When restoring from a backup, the sequence counters for term ID generation need to be restored to values at least as high as the backed-up state (plus a safety margin) to ensure:

1. No ID collisions with existing data
2. New data inserted after restore gets unique IDs
3. The system can handle both new backups (with counter files) and legacy backups (without counter files)

## Implementation

### 1. SequenceCounter Export/Import Functions

Added new functions to `lib/triple_store/dictionary/sequence_counter.ex`:

| Function | Purpose |
|----------|---------|
| `export/1` | Returns map of current counter values `%{uri: N, bnode: N, literal: N}` |
| `import_values/2` | Sets counters to `max(current, imported + safety_margin)` |
| `export_to_file/2` | Exports counter state to binary file |
| `import_from_file/2` | Imports counter state from file |
| `read_from_file/1` | Reads counter values without applying them |

### 2. Dictionary Manager Counter Access

Added to `lib/triple_store/dictionary/manager.ex`:

- `get_counter/1` - Returns the sequence counter process reference for backup/restore operations

### 3. Backup Module Updates

Updated `lib/triple_store/backup.ex`:

- **On backup creation**: Exports counter state to `.counter_state` file in backup directory
- **On restore**: Imports counter state from backup and applies with safety margin
- **Legacy support**: Falls back to RocksDB-persisted counters if no counter file exists

### File Format

Counter state is saved as Erlang binary term format:

```elixir
%{
  version: 1,
  counters: %{uri: N, bnode: N, literal: N},
  exported_at: "2025-12-28T..."
}
```

## Files Modified

| File | Changes |
|------|---------|
| `lib/triple_store/dictionary/sequence_counter.ex` | Added export/import functions and GenServer handlers |
| `lib/triple_store/dictionary/manager.ex` | Added `get_counter/1` function |
| `lib/triple_store/backup.ex` | Added counter state save/restore in backup/restore operations |
| `test/triple_store/dictionary/sequence_counter_test.exs` | Added 7 new tests for export/import functionality |
| `test/triple_store/backup_test.exs` | Added 4 new tests for counter persistence in backups |

## Test Coverage

### New SequenceCounter Tests

- `export/1` - exports current counter values
- `import_values/2` - imports counter values with safety margin
- `import_values/2` - takes max of current and imported values
- `export_to_file/2 and import_from_file/2` - round-trips counter state through file
- `import_from_file/2` - returns error for invalid file
- `read_from_file/1` - reads counter values without applying them

### New Backup Tests

- `backup includes counter state file` - verifies `.counter_state` file is created
- `restore applies counter state from backup` - verifies counters are restored with safety margin
- `restore works without counter file (legacy backup)` - verifies backward compatibility
- `new data after restore gets unique IDs` - end-to-end test for ID uniqueness

## Key Design Decisions

1. **Non-blocking backup**: Counter export failure doesn't fail the backup (logged as warning)
2. **Non-blocking restore**: Counter import failure doesn't fail the restore (uses RocksDB values)
3. **Safety margin**: Always applies safety margin (1000) when importing to prevent collisions
4. **Max semantics**: Import takes max of current value and imported value to never decrease
5. **Backward compatible**: Handles legacy backups without counter files gracefully

## Test Results

- All tests pass: 3834 tests, 0 failures
- New tests: 11 additional tests (7 in sequence_counter, 4 in backup)

## Summary

Task 5.5.2.4 is complete. The atomics counter restoration feature ensures that:

1. Backups include sequence counter state in a dedicated file
2. Restores properly initialize counters from backup state
3. New data after restore always gets unique, non-colliding IDs
4. Legacy backups without counter files are handled gracefully
