# Task 5.3.3: Cache Warming

**Date:** 2025-12-28
**Branch:** `feature/task-5.3.3-cache-warming`
**Status:** Complete

## Overview

Implemented cache warming functionality for the query result cache. The system can persist cache contents to disk, restore from disk on startup, and pre-execute common queries to warm the cache.

## Implementation Details

### Files Modified

1. **`lib/triple_store/query/cache.ex`**
   - Added persistence options: `:persistence_path`, `:warm_on_start`
   - Added `persist_to_file/2` - Saves cache to binary file
   - Added `warm_from_file/2` - Restores cache from file
   - Added `warm_queries/2` - Pre-executes and caches query list
   - Added `get_all_entries/1` - Returns all entries for custom persistence
   - Added telemetry events for persist and warm operations
   - Modified `init/1` to support `warm_on_start` option

2. **`test/triple_store/query/cache_test.exs`**
   - Added 12 new tests for cache warming (46 tests total, up from 34)

### Features Implemented

| Feature | Description |
|---------|-------------|
| Disk Persistence | Save cache to compressed binary file using `:erlang.term_to_binary` |
| File Restoration | Load cache from previously saved file |
| Warm on Start | Automatically warm cache when GenServer starts |
| Query Pre-execution | Execute a list of queries to pre-populate cache |
| Version Control | File format versioned for future compatibility |
| Telemetry | Events for persist and warm operations |

### API

```elixir
# Persist cache to file
{:ok, count} = Query.Cache.persist_to_file("/var/cache/queries.bin")

# Warm from file
{:ok, count} = Query.Cache.warm_from_file("/var/cache/queries.bin")

# Pre-execute queries
queries = [
  {"SELECT ?s WHERE { ?s a ?type }", fn -> execute(...) end, [predicates: [:rdf_type]]},
  {"SELECT ?name WHERE { ?s <name> ?name }", fn -> execute(...) end, []}
]
{:ok, %{cached: 2, failed: 0}} = Query.Cache.warm_queries(queries)

# Start with warm_on_start
{:ok, _pid} = Query.Cache.start_link(
  persistence_path: "/var/cache/queries.bin",
  warm_on_start: true
)

# Get all entries for custom persistence
entries = Query.Cache.get_all_entries()
```

### File Format

The cache is persisted as a compressed Erlang term with the following structure:

```elixir
%{
  version: 1,
  timestamp: 1735355234,  # Unix timestamp
  entry_count: 42,
  entries: [
    %{
      key: <<SHA256 hash>>,
      result: [...],
      result_size: 100,
      predicates: [:pred1, :pred2]
    },
    ...
  ]
}
```

### Telemetry Events

| Event | Measurements | Metadata |
|-------|--------------|----------|
| `[:triple_store, :cache, :query, :persist]` | `%{count: n}` | `%{}` |
| `[:triple_store, :cache, :query, :warm]` | `%{count: n}` | `%{}` |

### Test Coverage

```
46 tests, 0 failures
```

New test categories:
- **Persistence tests** (5 tests): persist/restore cycle, error handling, get_all_entries
- **Warm on start tests** (2 tests): automatic warming, missing file handling
- **Query pre-execution tests** (3 tests): warm_queries, failure handling, skip cached
- **Telemetry tests** (2 tests): persist and warm events

### Key Design Decisions

1. **Compressed Binary Format**: Uses `:erlang.term_to_binary(data, [:compressed])` for efficient storage. Typical compression ratios of 3-5x for query results.

2. **Timestamp Reset on Load**: Monotonic timestamps are reset when loading from disk since they're process-specific. All loaded entries get fresh timestamps.

3. **Graceful Missing File Handling**: When `warm_on_start: true` but file doesn't exist, cache starts empty without error.

4. **Respect Size Limits**: When loading from disk, entries exceeding `max_result_size` or `max_entries` are skipped.

5. **Version Field**: File format includes version number to support future format changes without breaking existing cache files.

## Usage Examples

### Periodic Backup

```elixir
# In a GenServer or scheduled task
defmodule CacheBackup do
  use GenServer

  def handle_info(:backup, state) do
    Query.Cache.persist_to_file("/var/cache/query_cache.bin")
    Process.send_after(self(), :backup, :timer.minutes(5))
    {:noreply, state}
  end
end
```

### Application Startup Warming

```elixir
# In application.ex
def start(_type, _args) do
  children = [
    {TripleStore.Query.Cache, [
      max_entries: 1000,
      persistence_path: "/var/cache/query_cache.bin",
      warm_on_start: true
    ]},
    # ... other children
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
end
```

### Pre-warming Common Queries

```elixir
# After application starts
def warm_common_queries(db) do
  queries = [
    {"SELECT ?type (COUNT(?s) AS ?count) WHERE { ?s a ?type } GROUP BY ?type",
     fn -> TripleStore.SPARQL.Query.execute(db, ...) end,
     [predicates: [RDF.type()]]},
    # ... more frequent queries
  ]

  Query.Cache.warm_queries(queries)
end
```

## Relationship to Other Tasks

- **Task 5.3.1 (Result Cache)**: Built on the cache infrastructure from 5.3.1
- **Task 5.3.2 (Cache Invalidation)**: Persisted entries include predicates for invalidation
- **Task 5.4 (Telemetry)**: New events ready for Prometheus integration

## Dependencies

No new dependencies added.
