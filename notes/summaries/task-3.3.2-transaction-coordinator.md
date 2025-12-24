# Task 3.3.2: Transaction Coordinator - Summary

## Overview

Implemented the Transaction Coordinator GenServer that provides serialized write access to the triple store with snapshot-based read isolation during updates.

## Files Created

### Implementation
- `lib/triple_store/transaction.ex` (~590 lines)
  - GenServer for coordinating all write operations
  - Snapshot-based read isolation during updates
  - Plan cache invalidation after data changes
  - Statistics callback support

### Tests
- `test/triple_store/transaction_test.exs` (~520 lines)
  - 32 comprehensive tests covering all functionality

### Minor Fix
- `lib/triple_store/sparql/update_executor.ex`
  - Added support for `:language_tagged` literal format from parser

## Key Functions

### Client API

```elixir
@spec start_link(keyword()) :: GenServer.on_start()
def start_link(opts)
```
Starts the transaction manager with required `:db` and `:dict_manager` options.

```elixir
@spec update(manager(), String.t(), keyword()) :: update_result()
def update(manager, sparql, opts \\ [])
```
Executes a SPARQL UPDATE operation, serialized through the coordinator.

```elixir
@spec query(manager(), String.t(), keyword()) :: query_result()
def query(manager, sparql, opts \\ [])
```
Executes a SPARQL query. Uses snapshot for consistent reads during concurrent updates.

```elixir
@spec insert(manager(), [{term(), term(), term()}], keyword()) :: update_result()
def insert(manager, triples, opts \\ [])
```
Direct insert API using RDF.ex terms.

```elixir
@spec delete(manager(), [{term(), term(), term()}], keyword()) :: update_result()
def delete(manager, triples, opts \\ [])
```
Direct delete API using RDF.ex terms.

```elixir
@spec execute_update(manager(), term(), keyword()) :: update_result()
def execute_update(manager, ast, opts \\ [])
```
Executes a pre-parsed UPDATE AST.

### Monitoring

```elixir
@spec update_in_progress?(manager()) :: boolean()
@spec current_snapshot(manager()) :: reference() | nil
@spec get_context(manager()) :: context()
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    Client Requests                   │
│    (query/1, update/1, insert/2, delete/2, etc.)    │
└─────────────┬─────────────────────────┬─────────────┘
              │                         │
              │ Writes (serialized)     │ Reads (direct)
              ▼                         ▼
┌─────────────────────┐   ┌─────────────────────────────┐
│ Transaction Manager │   │      Direct DB Access       │
│    (GenServer)      │   │  (snapshot during updates)  │
└─────────────────────┘   └─────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────┐
│                    RocksDB                           │
│         (WriteBatch for atomicity)                   │
└─────────────────────────────────────────────────────┘
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `@update_timeout` | 300,000ms (5 min) | Timeout for update operations |
| `@query_timeout` | 120,000ms (2 min) | Timeout for query operations |

## Isolation Semantics

1. **Write Serialization**: All writes go through GenServer, executed one at a time
2. **Read Isolation**: Snapshots created during updates for consistent reads
3. **Atomicity**: RocksDB WriteBatch ensures all-or-nothing updates

## Plan Cache Integration

After successful updates (count > 0):
1. Plan cache is invalidated via `PlanCache.invalidate/1`
2. Optional stats callback is invoked for statistics refresh
3. No invalidation when no changes made (count == 0)

## Test Coverage

### Lifecycle (4 tests)
- Start and stop
- Named registration
- Plan cache configuration
- Stats callback invocation

### SPARQL UPDATE (6 tests)
- INSERT DATA (single, multiple, typed, language literals)
- DELETE DATA (existing, non-existent)
- Parse error handling

### SPARQL Query (3 tests)
- Query after insert
- Empty results
- Parse error handling

### Direct API (5 tests)
- Insert with RDF.ex terms
- Delete with RDF.ex terms
- Empty list handling

### Pre-parsed AST (1 test)
- Execute UPDATE from parsed AST

### Monitoring (2 tests)
- update_in_progress?/1
- get_context/1

### Plan Cache Invalidation (2 tests)
- Invalidation after successful update
- No invalidation when no changes

### Serialization (1 test)
- Concurrent updates are serialized

### Snapshot Isolation (1 test)
- Snapshots created and released

### Error Handling (1 test)
- Invalid update recovery

### Timeouts (3 tests)
- Default timeout values
- Custom timeout support

### Data Integrity (2 tests)
- Insert then query
- Delete removes from results

## Usage Example

```elixir
# Start the transaction manager
{:ok, txn} = Transaction.start_link(
  db: db,
  dict_manager: manager,
  plan_cache: PlanCache,
  stats_callback: fn -> refresh_stats() end
)

# Execute SPARQL UPDATE
{:ok, 1} = Transaction.update(txn, """
  INSERT DATA {
    <http://example.org/alice> <http://example.org/name> "Alice" .
  }
""")

# Query the data
{:ok, results} = Transaction.query(txn, """
  SELECT ?name WHERE {
    <http://example.org/alice> <http://example.org/name> ?name .
  }
""")

# Direct API
{:ok, 1} = Transaction.insert(txn, [
  {RDF.iri("http://example.org/bob"),
   RDF.iri("http://example.org/name"),
   RDF.literal("Bob")}
])

# Stop
Transaction.stop(txn)
```

## Planning Checklist

- [x] 3.3.2.1 Create `TripleStore.Transaction` GenServer
- [x] 3.3.2.2 Serialize all writes through coordinator
- [x] 3.3.2.3 Use RocksDB snapshots for read consistency during update
- [x] 3.3.2.4 Invalidate plan cache after successful update
- [x] 3.3.2.5 Handle update failure with rollback semantics

## Next Steps

Task 3.3.3 (Update API) will add:
- Public `TripleStore.update/2` function
- Public `TripleStore.insert/2` function
- Public `TripleStore.delete/2` function
- Affected triple count return values
