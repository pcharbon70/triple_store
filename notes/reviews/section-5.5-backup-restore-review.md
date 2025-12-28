# Section 5.5 Backup and Restore - Comprehensive Review

**Date:** 2025-12-28
**Reviewers:** Factual, QA, Senior Engineer, Security, Consistency, Redundancy, Elixir Expert

---

## Executive Summary

Section 5.5 (Backup and Restore) is **substantially complete** with solid implementation quality. All planned features are implemented except for scheduled backups (5.5.3.1), which is explicitly deferred. The main issues requiring attention are:

1. **Security (Critical):** Unsafe `binary_to_term` in backup metadata reading
2. **Telemetry Gap:** Backup events not registered in telemetry module
3. **Test Coverage:** Incremental backup tests are skipped

---

## Blockers (Must Fix Before Merge)

### B1. Unsafe Deserialization in Backup Metadata

**File:** `lib/triple_store/backup.ex:724`

```elixir
defp read_metadata_from_file(path, metadata_path) do
  case File.read(metadata_path) do
    {:ok, content} ->
      build_metadata_from_stored(path, :erlang.binary_to_term(content))  # UNSAFE!
```

**Issue:** Uses `:erlang.binary_to_term/1` without the `:safe` option. A malicious backup could execute arbitrary code during deserialization.

**Contrast:** `SequenceCounter.safe_binary_to_term/1` correctly uses `[:safe]` option.

**Fix:** Add `:safe` option:
```elixir
:erlang.binary_to_term(content, [:safe])
```

### B2. No Path Traversal Protection

**File:** `lib/triple_store/backup.ex` (multiple locations)

**Issue:** Backup paths are used directly without validation for path traversal attacks (`..` components).

**Fix:** Implement path validation similar to Query Cache:
```elixir
defp validate_path(path) do
  expanded = Path.expand(path)
  # Validate path is within allowed directory
end
```

### B3. Backup Telemetry Events Not Registered

**File:** `lib/triple_store/telemetry.ex`

**Issue:** The Backup module emits telemetry events via `Telemetry.span/4`, but these events are NOT registered in `all_events/0`. Handlers attached via `Telemetry.attach_handler/3` won't receive backup events, and Prometheus won't track backup metrics.

**Fix:** Add `backup_events/0` to Telemetry module:
```elixir
def backup_events do
  [
    @prefix ++ [:backup, :create, :start],
    @prefix ++ [:backup, :create, :stop],
    @prefix ++ [:backup, :create_incremental, :start],
    @prefix ++ [:backup, :create_incremental, :stop],
    @prefix ++ [:backup, :restore, :start],
    @prefix ++ [:backup, :restore, :stop]
  ]
end
```

---

## Concerns (Should Address)

### C1. Incremental Backup Tests Skipped

**File:** `test/triple_store/backup_test.exs:77, 105`

Two critical tests are marked `@tag :skip`:
- "creates an incremental backup based on full backup"
- "incremental backup is independently restorable"

**Impact:** Core incremental backup functionality is not tested in CI.

### C2. No Empty Database Backup/Restore Tests

**Impact:** Backup of database with zero triples is untested.

### C3. Silent Failure of Counter Operations

**Files:** `lib/triple_store/backup.ex:599-604, 321-326`

Counter save/restore failures log warnings but return `:ok`:
```elixir
{:error, reason} ->
  Logger.warning("Failed to save counter state to backup: #{inspect(reason)}")
  :ok  # Silent success despite failure
```

**Recommendation:** Return `{:ok, :partial}` or add `:strict` option.

### C4. Race Condition: Counter Import After Store Open

**File:** `lib/triple_store/backup.ex:303-310`

The restore flow opens the store (which initializes counters) before importing backup counters. A window exists for race conditions.

### C5. Incremental Backup Only Compares File Size

**File:** `lib/triple_store/backup.ex:529-536`

Files with same size but different content would be incorrectly linked.

**Recommendation:** Compare `mtime` in addition to `size`.

### C6. No Symlink Protection

**Issue:** `File.cp_r/2` follows symlinks by default, potentially overwriting arbitrary files during restore.

### C7. Documentation Error in SequenceCounter

**File:** `lib/triple_store/dictionary/sequence_counter.ex:208`

Doc example shows `SequenceCounter.import(counter, values)` but function is named `import_values/2`.

### C8. Bare Rescue in `has_required_files?/1`

**File:** `lib/triple_store/backup.ex:696-707`

```elixir
rescue
  _ -> false
```

Catches all exceptions, masking legitimate errors.

---

## Suggestions (Nice to Have)

### S1. Add Backup Size Limits
Prevent DoS via extremely large backups by adding configurable limits.

### S2. Add Backup Integrity Checksums
SHA-256 checksums in metadata would detect tampering.

### S3. Add File Permission Restrictions
Set restrictive permissions (0600) on backup files.

### S4. Extract Common Metadata Writing Logic
`write_metadata/3` and `write_incremental_metadata/4` have duplicated code.

### S5. Use Safe Binary Serialization Helper
Create shared utility for versioned, safe binary file I/O.

### S6. Add Progress Callbacks for Large Backups
```elixir
Backup.create(store, path, progress_callback: fn bytes, total -> ... end)
```

### S7. Add Backup Prometheus Metrics
- `triple_store_backup_total` (counter)
- `triple_store_backup_duration_seconds` (histogram)
- `triple_store_backup_size_bytes` (gauge)

### S8. Document Incremental Backup Limitations
Note that incremental backups work best when store is closed.

---

## Good Practices Observed

### Implementation Quality

1. **Counter State Backup/Restore**: Excellent implementation of atomics counter restoration with safety margin to prevent ID collisions.

2. **Versioned File Format**: Counter file includes version field for forward compatibility.

3. **Backward Compatibility**: Restore handles legacy backups without counter files gracefully.

4. **Safe Deserialization in SequenceCounter**: Correctly uses `[:safe]` option.

5. **Clean API Design**: Consistent `{:ok, result} | {:error, reason}` returns.

6. **Good Separation of Concerns**: Backup handles orchestration, SequenceCounter owns serialization.

7. **Proper `:atomics` Usage**: Correct use of `add_get/3` for atomic increment.

8. **Idiomatic `with` Chains**: Clean sequential operations with early error return.

9. **Comprehensive Typespecs**: All public functions have `@spec` annotations.

10. **Telemetry Integration**: All major operations emit telemetry events.

### Test Quality

1. **Core Functionality Tested**: Create, restore, verify, list, delete, rotate all covered.

2. **Counter Persistence Well-Tested**: Safety margin, legacy compatibility, ID uniqueness verified.

3. **Concurrent Access Tested**: 100 concurrent tasks in SequenceCounter tests.

4. **Error Conditions Tested**: Path exists, invalid backup, destination exists scenarios.

---

## Summary Table

| Category | Count | Priority Items |
|----------|-------|----------------|
| Blockers | 3 | Unsafe binary_to_term, path traversal, telemetry registration |
| Concerns | 8 | Skipped tests, silent failures, race conditions |
| Suggestions | 8 | Size limits, checksums, permissions, code dedup |
| Good Practices | 10+ | Counter restoration, API design, atomics usage |

---

## Recommended Fix Priority

1. **Immediate (Security):**
   - Add `:safe` option to `binary_to_term` in backup metadata reading
   - Add path traversal protection

2. **High:**
   - Register backup events in Telemetry module
   - Enable or fix skipped incremental backup tests

3. **Medium:**
   - Add symlink detection
   - Fix documentation error (import vs import_values)
   - Add empty database test

4. **Low:**
   - Refactor duplicated metadata writing code
   - Add backup size limits
   - Add progress callbacks

---

## Files Reviewed

| File | Lines | Purpose |
|------|-------|---------|
| `lib/triple_store/backup.ex` | 813 | Main backup module |
| `lib/triple_store/dictionary/sequence_counter.ex` | 577 | Counter export/import |
| `lib/triple_store/dictionary/manager.ex` | 273 | Counter access |
| `test/triple_store/backup_test.exs` | 407 | Backup tests |
| `test/triple_store/dictionary/sequence_counter_test.exs` | 520+ | Counter tests |
