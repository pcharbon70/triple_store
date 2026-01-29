# OTP & Concurrency Model

This document describes the OTP-based process architecture, including GenServers, supervision trees, and concurrency patterns used in TripleStore.

## Overview

TripleStore uses OTP (Open Telecom Platform) for fault tolerance and concurrency:

- **GenServers** for long-running services with state
- **Supervision trees** for process isolation and restarts
- **Dynamic supervision** for per-database processes
- **Registry** for process naming and discovery

```mermaid
graph TB
    subgraph "Application Supervision Tree"
        APP[TripleStore.Application]

        subgraph "Global Services"
            METRICS[Metrics GenServer]
            PROM[Prometheus Exporter]
        end

        subgraph "Per-Store Processes"
            DICT[Dictionary.Manager]
            SEQ[SequenceCounter]
            TXN[Transaction Coordinator]
            CACHE[Plan Cache]
        end

        subgraph "Scheduled Services"
            BACKUP[Scheduled Backup]
        end
    end

    APP --> METRICS
    APP --> PROM
    APP --> DICT
    APP --> SEQ
    APP --> TXN
    APP --> CACHE
    APP --> BACKUP
```

## GenServer Components

### Dictionary.Manager

The `TripleStore.Dictionary.Manager` GenServer manages term encoding:

```mermaid
graph TB
    subgraph "Dictionary.Manager State"
        COUNTER[next_id: integer]
        CACHE[batch_cache: map]
        DB[db_ref: reference]
        PERSIST[persisted_at: integer]
    end

    subgraph "Client Operations"
        GET[get_or_create_id/1]
        BATCH[batch_create_ids/1]
        DECODE[decode_term/1]
    end

    subgraph "Internal Operations"
        FLUSH[flush_batch/0]
        CHECKPOINT[checkpoint/0]
    end

    GET --> COUNTER
    BATCH --> COUNTER
    DECODE --> CACHE
    FLUSH --> DB
    CHECKPOINT --> DB
```

**Responsibilities:**
- Allocate sequential IDs for new terms (URIs, blank nodes, literals)
- Maintain batch cache for recently created terms
- Periodically flush ID sequences to RocksDB
- Coordinate with sharded manager for bulk loading

**State:**
```elixir
%{
  db: db_ref,
  next_id: current_sequence,
  batch_cache: %{term_binary => id},
  persisted_at: last_flushed_sequence,
  shard_count: number_of_shards
}
```

**Key Operations:**
- `get_or_create_id(term)` - Get existing ID or allocate new one
- `batch_create_ids(terms)` - Allocate multiple IDs efficiently
- `decode_term(id)` - Look up term string from ID

### SequenceCounter

The `TripleStore.Dictionary.SequenceCounter` GenServer provides atomic ID generation:

```mermaid
sequenceDiagram
    participant Client
    participant Counter as SequenceCounter
    participant RocksDB

    Client->>Counter: allocate_id(type, count)
    Counter->>Counter: Increment local counter
    Counter-->>Client: {:ok, start_id, count}

    Note over Counter: Every flush_interval:
    Counter->>RocksDB: Persist current value
    Counter->>Counter: Reset safety margin
```

**Responsibilities:**
- Atomic ID allocation within type ranges
- Periodic persistence to RocksDB
- Recovery with safety margin (1000 IDs) on restart

### Transaction Coordinator

The `TripleStore.Transaction` GenServer serializes write operations:

```mermaid
graph TB
    subgraph "Transaction Coordinator"
        QUEUE[Request Queue]
        EXEC[Execute One-by-One]
        CACHE[Plan Cache]
        SNAPSHOT[Read Snapshot]
    end

    subgraph "Request Types"
        INSERT[insert/2]
        DELETE[delete/2]
        UPDATE[update/2]
        BATCH[batch_operation/2]
    end

    INSERT --> QUEUE
    DELETE --> QUEUE
    UPDATE --> QUEUE
    BATCH --> QUEUE

    QUEUE --> EXEC
    EXEC --> CACHE
    EXEC --> SNAPSHOT
```

**Responsibilities:**
- Serialize write operations (prevents write conflicts)
- Create snapshots for read isolation during writes
- Invalidate plan cache after successful writes
- Coordinate with dictionary for term encoding

**Concurrency Guarantees:**

| Operation | Isolation | Guarantees |
|-----------|-----------|-------------|
| Single write | Atomic | All indices updated or none |
| Multiple writes | Serialized | Processed in order received |
| Reads during write | Snapshot | See pre-write state |
| Concurrent reads | Uncoordinated | Direct database access |

### Plan Cache

The `TripleStore.SPARQL.PlanCache` GenServer (or ETS-based implementation) caches optimized query plans:

```mermaid
graph TB
    subgraph "Plan Cache Structure"
        ETS[ETS Table: plans]
        LRU[Ordered Set: LRU tracking]
        META[Metadata: size, hits, misses]
    end

    subgraph "Operations"
        GET[get_or_compute/2]
        PUT[put/2]
        INVALIDATE[invalidate/0]
        STATS[stats/0]
    end

    GET --> ETS
    PUT --> ETS
    PUT --> LRU
    INVALIDATE --> ETS
    STATS --> META
```

**Responsibilities:**
- Store optimized query plans by normalized query hash
- Track LRU for cache eviction
- Maintain hit/miss statistics
- Support invalidation on data changes

## Supervision Tree

### Application Supervisor

The `TripleStore.Application` module defines the supervision tree:

```elixir
defmodule TripleStore.Application do
  use Application

  def start(_type, _args) do
    children = [
      # Global services
      {TripleStore.Metrics, name: TripleStore.Metrics},
      {TripleStore.Prometheus, port: prometheus_port()},

      # Per-store processes started dynamically
      # Dictionary managers, transaction coordinators, etc.
    ]

    opts = [strategy: :one_for_one, name: TripleStore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

### Dynamic Supervision

Per-database processes are started under a `DynamicSupervisor`:

```mermaid
graph TB
    subgraph "DynamicSupervisor: database_processes"
        DICT_MGR[Dictionary.Manager]
        SEQ_COUNTER[SequenceCounter]
        TXN_COORD[Transaction Coordinator]
    end

    subgraph "Store Registration"
        REGISTRY[Registry: triple_store_stores]
    end

    DICT_MGR -.->|registered| REGISTRY
    SEQ_COUNTER -.->|registered| REGISTRY
    TXN_COORD -.->|registered| REGISTRY
```

**Starting per-database processes:**

```elixir
# When opening a database
def open(path, opts) do
  # Start dictionary manager
  {:ok, _dict_pid} = DynamicSupervisor.start_child(
    TripleStore.DictionarySupervisor,
    {Dictionary.Manager, db: db_ref, name: via_name(db_ref, :dict)}
  )

  # Start sequence counter
  {:ok, _seq_pid} = DynamicSupervisor.start_child(
    TripleStore.DictionarySupervisor,
    {SequenceCounter, db: db_ref, name: via_name(db_ref, :seq)}
  )
end
```

### Process Registration

Processes are registered using `Registry` for discovery:

```mermaid
graph LR
    subgraph "Registry: triple_store_stores"
        STORE1[{store: "/data/db1"}]
        STORE2[{store: "/data/db2"}]
    end

    subgraph "Associated Processes"
        DICT1["Dictionary.Manager"]
        TXN1["Transaction Coordinator"]
        DICT2["Dictionary.Manager"]
        TXN2["Transaction Coordinator"]
    end

    STORE1 --> DICT1
    STORE1 --> TXN1
    STORE2 --> DICT2
    STORE2 --> TXN2
```

**Registry keys:**

```elixir
# Store entry
{:store, "/data/db"} => store_pid

# Dictionary manager
{:dictionary_manager, "/data/db"} => dict_pid

# Sequence counter
{:sequence_counter, "/data/db"} => seq_pid

# Transaction coordinator
{:transaction, "/data/db"} => txn_pid
```

## Concurrency Patterns

### Read-Write Concurrency

```mermaid
sequenceDiagram
    participant R1 as Reader 1
    participant R2 as Reader 2
    participant W1 as Writer
    participant DB as RocksDB

    R1->>DB: Read query 1
    R2->>DB: Read query 2
    W1->>W1: Begin write (serialized)

    Note over DB: Readers proceed concurrently

    W1->>DB: Write batch
    DB-->>W1: Committed
    W1->>R1: Invalidate cache

    Note over DB: New readers see updated data
```

### Bulk Loading with Sharding

For bulk loading, a sharded dictionary manager enables parallel encoding:

```mermaid
graph TB
    subgraph "Sharded Dictionary"
        SHARD0[Shard 0<br/>Terms hash % 8 = 0]
        SHARD1[Shard 1<br/>Terms hash % 8 = 1]
        SHARD2[Shard 2...]
        SHARD7[Shard 7<br/>Terms hash % 8 = 7]
    end

    subgraph "Coordinator"
        COORD[ShardedManager<br/>routes to shards]
    end

    subgraph "Worker Pool"
        W1[Worker 1]
        W2[Worker 2]
        W3[Worker 3]
        W4[Worker 4]
    end

    COORD --> SHARD0
    COORD --> SHARD1
    COORD --> SHARD2
    COORD --> SHARD7

    W1 --> COORD
    W2 --> COORD
    W3 --> COORD
    W4 --> COORD
```

**Sharding strategy:**

```elixir
defmodule TripleStore.Dictionary.ShardedManager do
  @shard_count 8

  def get_or_create_id(shards, term) do
    shard_index = :erlang.phash2(term, @shard_count)
    shard = Enum.at(shards, shard_index)
    Dictionary.Manager.get_or_create_id(shard, term)
  end
end
```

### Parallel Query Execution

Independent query operations can run in parallel:

```mermaid
graph TB
    subgraph "Query Execution"
        MAIN[Main Process]
        TASK1[Task 1: BGP 1]
        TASK2[Task 2: BGP 2]
        TASK3[Task 3: BGP 3]
        JOIN[Join Results]
    end

    MAIN --> TASK1
    MAIN --> TASK2
    MAIN --> TASK3

    TASK1 --> JOIN
    TASK2 --> JOIN
    TASK3 --> JOIN
```

**Parallel processing example:**

```elixir
defmodule ParallelExecutor do
  def execute_parallel_patterns(patterns, ctx) do
    patterns
    |> Task.async_stream(
      fn pattern -> execute_pattern(pattern, ctx) end,
      max_concurrency: System.schedulers_online(),
      timeout: :infinity
    )
    |> Enum.flat_map(fn {:ok, results} -> results end)
  end
end
```

## State Management

### GenServer State Patterns

#### Dictionary Manager State

```elixir
%{
  # Database reference
  db: db_ref,

  # ID allocation state
  next_id: current_sequence,
  persisted_at: last_flushed_sequence,

  # Batch cache for recent allocations
  batch_cache: %{
    term_binary => id
  },

  # Sharding configuration
  shard_count: 8,
  shard_index: 0
}
```

#### Transaction Coordinator State

```elixir
%{
  # Dependencies
  db: db_ref,
  dict_manager: dict_pid,
  plan_cache: cache_module,

  # Request queue (for serialization)
  queue: :queue.from_list([]),
  current: nil,

  # Snapshot for reads
  snapshot: nil
}
```

#### Metrics Collector State

```elixir
%{
  # Counters
  query_count: 0,
  insert_count: 0,
  delete_count: 0,

  # Duration tracking
  query_durations: :queue.new(),

  # Cache metrics
  cache_hits: 0,
  cache_misses: 0,

  # Config
  histogram_buckets: [1, 5, 10, 25, 50, 100, 250, 500, 1000]
}
```

## Process Lifecycle

### Starting a Store

```mermaid
sequenceDiagram
    participant Client
    participant App as Application
    participant DynSup as DynamicSupervisor
    participant Dict as Dictionary.Manager
    participant Seq as SequenceCounter

    Client->>App: open(path, schema: :quad)
    App->>App: Create RocksDB reference
    App->>DynSup: start_child(Dictionary.Manager)
    DynSup-->>Dict: Started
    App->>DynSup: start_child(SequenceCounter)
    DynSup-->>Seq: Started
    App-->>Client: {:ok, store_handle}
```

### Graceful Shutdown

```mermaid
sequenceDiagram
    participant Client
    participant Store
    participant Dict as Dictionary.Manager
    participant Seq as SequenceCounter
    participant DB as RocksDB

    Client->>Store: close(store)
    Store->>Dict: checkpoint()  # Save ID state
    Dict->>DB: Persist sequence
    Dict-->>Store: :ok
    Store->>Dict: stop()
    Store->>Seq: stop()
    Store->>DB: close()
    Store-->>Client: :ok
```

## Error Handling

### Supervision Strategies

```mermaid
graph TB
    subgraph "Supervision Strategies"
        ONE_FOR_ONE[one_for_one<br/>Restart only failed child]
        ONE_FOR_ALL[one_for_all<br/>Restart all children]
        REST_FOR_ONE[rest_for_one<br/>Restart failed + after]
    end

    subgraph "TripleStore Strategy"
        APP["Application<br/>one_for_one"]
        DYN["DynamicSupervisor<br/>one_for_one"]
    end

    APP --> ONE_FOR_ONE
    DYN --> ONE_FOR_ONE
```

**Strategy by process:**

| Process | Strategy | Reason |
|---------|----------|--------|
| Dictionary.Manager | `:permanent` | Must always be running |
| SequenceCounter | `:permanent` | Must always be running |
| Transaction Coordinator | `:permanent` | Must always be running |
| Metrics | `:transient` | Can be restarted |
| Scheduled Backup | `:transient` | Can be restarted |

### Restart Scenarios

```mermaid
graph TB
    subgraph "Failure Scenarios"
        CRASH[Process Crash]

        subgraph "Dictionary.Manager Failure"
            DICT_CRASH[Dictionary.Manager crashes]
            DICT_RESTART[Supervisor restarts it]
            DICT_RECOVER[Recovers from RocksDB]
        end

        subgraph "Transaction Coordinator Failure"
            TXN_CRASH[Coordinator crashes mid-transaction]
            TXN_RESTART[Supervisor restarts it]
            TXN_RECOVER[In-flight write abandoned]
        end
    end

    CRASH --> DICT_CRASH
    CRASH --> TXN_CRASH
```

## Module Reference

| Module | Type | Purpose |
|--------|------|---------|
| `TripleStore.Application` | Application | Root supervisor |
| `TripleStore.Dictionary.Manager` | GenServer | Term encoding coordination |
| `TripleStore.Dictionary.SequenceCounter` | GenServer | Atomic ID allocation |
| `TripleStore.Transaction` | GenServer | Write serialization |
| `TripleStore.SPARQL.PlanCache` | GenServer/ETS | Query plan caching |
| `TripleStore.Metrics` | GenServer | Metrics aggregation |
| `TripleStore.ScheduledBackup` | GenServer | Backup scheduling |

## Best Practices

### When to Use GenServers

**Use GenServers for:**
- Long-lived state (dictionary, caches)
- Coordinating access to shared resources (transactions)
- Periodic background tasks (backups, metrics)

**Avoid GenServers for:**
- Query execution (use Task or Stream)
- One-off operations (use direct function calls)
- CPU-bound work (use Task with dirty schedulers)

### Process Naming

```elixir
# Good: Registry-based naming
{:via, Registry, {TripleStore.Registry, {Dictionary.Manager, "/data/db"}}}

# Avoid: Global names
:name: :my_dictionary_manager  # Can't have multiple instances
```

### Timeout Handling

```elixir
# For long-running operations, use :infinity cautiously
GenServer.call(server_pid, :long_operation, 30_000)  # 30 second timeout

# Or use cast + handle_info for async operations
GenServer.cast(server_pid, :async_operation)
```

## Next Steps

- [Architecture Overview](00-architecture-overview.md) - For high-level component interaction
- [Storage Layer](01-storage-layer.md) - For RocksDB integration details
- [Data Flow](08-data-flow.md) - For end-to-end request processing
