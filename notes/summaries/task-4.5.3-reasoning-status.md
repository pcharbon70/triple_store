# Task 4.5.3: Reasoning Status - Summary

**Date:** 2025-12-26
**Branch:** feature/4.5.3-reasoning-status

## Overview

Implemented reasoning status tracking for monitoring and debugging the OWL 2 RL reasoning subsystem. The status module tracks configuration, derived triple counts, materialization history, and error states.

## Implementation

### New Module: `TripleStore.Reasoner.ReasoningStatus`

**Location:** `lib/triple_store/reasoner/reasoning_status.ex`

Provides comprehensive status tracking for the reasoning subsystem.

#### Status Structure

| Field | Type | Description |
|-------|------|-------------|
| `config` | ReasoningConfig.t | Active reasoning configuration |
| `derived_count` | integer | Number of derived triples |
| `explicit_count` | integer | Number of explicit triples |
| `last_materialization` | DateTime | Last materialization timestamp |
| `materialization_count` | integer | Total materializations performed |
| `last_materialization_stats` | map | Stats from last materialization |
| `state` | atom | Current state (:initialized, :materialized, :stale, :error) |
| `error` | term | Error details if in error state |
| `created_at` | DateTime | Status creation timestamp |
| `updated_at` | DateTime | Last update timestamp |

#### Status API

| Function | Description |
|----------|-------------|
| `new/1` | Create new status with config |
| `record_materialization/2` | Record materialization event with stats |
| `update_explicit_count/2` | Update explicit triple count |
| `update_derived_count/2` | Update derived triple count |
| `mark_stale/1` | Mark status as needing rematerialization |
| `record_error/2` | Record an error |
| `update_config/2` | Update the configuration |

#### Query API

| Function | Description |
|----------|-------------|
| `summary/1` | Get complete status summary |
| `profile/1` | Get active profile |
| `mode/1` | Get active mode |
| `derived_count/1` | Get derived triple count |
| `explicit_count/1` | Get explicit triple count |
| `total_count/1` | Get total triple count |
| `last_materialization/1` | Get last materialization time |
| `last_materialization_stats/1` | Get last materialization stats |
| `state/1` | Get current state |
| `needs_rematerialization?/1` | Check if rematerialization needed |
| `error?/1` | Check if in error state |
| `error/1` | Get error details |
| `time_since_materialization/1` | Get seconds since last materialization |

#### Storage API

| Function | Description |
|----------|-------------|
| `store/2` | Store status in persistent_term |
| `load/1` | Load status from persistent_term |
| `remove/1` | Remove status from persistent_term |
| `exists?/1` | Check if status exists |
| `list_stored/0` | List all stored status keys |
| `clear_all/0` | Remove all stored statuses |

### Status States

| State | Description |
|-------|-------------|
| `:initialized` | Newly created, no materialization yet |
| `:materialized` | Successfully materialized |
| `:stale` | Needs rematerialization (TBox changed) |
| `:error` | Error occurred during reasoning |

### Key Design Decisions

- **Immutable Updates**: All updates return new status structs
- **Timestamp Tracking**: Created/updated timestamps for debugging
- **Statistics Recording**: Full materialization stats preserved
- **Error Tracking**: Error state with detailed error information
- **Persistent Storage**: Optional persistent_term storage for fast access
- **Integration Ready**: Works with ReasoningConfig for full context

## Test Coverage

**Location:** `test/triple_store/reasoner/reasoning_status_test.exs`

| Test Category | Test Count |
|--------------|------------|
| Creation | 5 |
| Recording Materialization | 6 |
| Count Updates | 3 |
| State Changes | 4 |
| Status Queries | 14 |
| Persistent Term Storage | 7 |
| **Total** | **49** |

## Files Changed

| File | Change |
|------|--------|
| `lib/triple_store/reasoner/reasoning_status.ex` | New module (+350 lines) |
| `test/triple_store/reasoner/reasoning_status_test.exs` | New tests (+320 lines) |

## Test Results

```
768 tests, 0 failures
```

All existing tests continue to pass with the new status module.

## Usage Examples

```elixir
# Create status with configuration
config = ReasoningConfig.preset(:full_materialization)
{:ok, status} = ReasoningStatus.new(config)

# Record materialization
status = ReasoningStatus.record_materialization(status, %{
  derived_count: 1500,
  iterations: 5,
  duration_ms: 250
})

# Update counts
status = ReasoningStatus.update_explicit_count(status, 5000)

# Get summary
summary = ReasoningStatus.summary(status)
# => %{
#      state: :materialized,
#      profile: :owl2rl,
#      mode: :materialized,
#      derived_count: 1500,
#      explicit_count: 5000,
#      total_count: 6500,
#      ...
#    }

# Check if rematerialization needed
ReasoningStatus.needs_rematerialization?(status)  # => false

# Mark as stale after TBox change
status = ReasoningStatus.mark_stale(status)
ReasoningStatus.needs_rematerialization?(status)  # => true

# Store for later access
ReasoningStatus.store(status, :my_ontology)
{:ok, loaded} = ReasoningStatus.load(:my_ontology)
```

## Next Steps

- **Task 4.5.4**: Unit tests for complete reasoning configuration (already covered)
- **Section 4.6**: Phase 4 Integration Tests
