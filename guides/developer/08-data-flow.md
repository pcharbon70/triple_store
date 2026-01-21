# Data Flow

This document describes the end-to-end data flows for all major operations in TripleStore.

## Overview

Data flows through several layers from API to storage:

```mermaid
graph TB
    subgraph "API Layer"
        API[TripleStore API]
    end

    subgraph "Processing Layer"
        QUERY[SPARQL Engine]
        REASON[Reasoner]
        TXN[Transaction Manager]
        LOAD[Loader]
    end

    subgraph "Index Layer"
        IDX[Index/QuadIndex]
        DICT[Dictionary]
    end

    subgraph "Storage Layer"
        ADAPTER[ErlangAdapter]
        ROCKSDB[(RocksDB)]
    end

    API --> QUERY
    API --> REASON
    API --> TXN
    API --> LOAD

    QUERY --> IDX
    REASON --> IDX
    TXN --> IDX
    TXN --> DICT
    LOAD --> DICT
    LOAD --> IDX

    IDX --> ADAPTER
    DICT --> ADAPTER

    ADAPTER --> ROCKSDB
```

## Query Processing Flow

### SELECT Query

```mermaid
sequenceDiagram
    participant Client
    participant API as TripleStore
    participant Cache as PlanCache
    participant Parser as SPARQL.Parser
    participant Opt as Optimizer
    participant Exec as Executor
    participant Idx as Index
    participant Dict as Dictionary

    Client->>API: query(store, sparql)
    API->>Cache: lookup(query_hash)

    alt Cache Hit
        Cache-->>API: cached_plan
    else Cache Miss
        API->>Parser: parse(sparql)
        Parser-->>API: algebra_ast
        API->>Opt: optimize(algebra)
        Opt-->>API: execution_plan
        API->>Cache: store(query_hash, plan)
    end

    API->>Exec: execute(plan)
    Exec->>Idx: lookup_pattern(pattern)

    loop For each binding
        Idx-->>Exec: term_ids
        Exec->>Dict: decode_ids(term_ids)
        Dict-->>Exec: rdf_terms
    end

    Exec-->>API: result_bindings
    API-->>Client: {:ok, results}
```

### Query Flow Stages

| Stage | Module | Input | Output |
|-------|--------|-------|--------|
| 1. Parse | `SPARQL.Parser` | SPARQL string | AST |
| 2. Compile | `SPARQL.Algebra` | AST | Algebra tree |
| 3. Optimize | `SPARQL.Optimizer` | Algebra | Optimized plan |
| 4. Execute | `SPARQL.Executor` | Plan | Bindings |
| 5. Decode | `Dictionary` | Term IDs | RDF terms |

### ASK Query

```mermaid
sequenceDiagram
    participant Client
    participant API as TripleStore
    participant Exec as Executor
    participant Idx as Index

    Client->>API: query(store, "ASK { ... }")
    API->>Exec: execute(ask_query)

    Exec->>Idx: lookup_pattern(pattern)

    alt Result found
        Idx-->>Exec: at least one binding
        Exec-->>API: {:ok, true}
    else No results
        Idx-->>Exec: empty stream
        Exec-->>API: {:ok, false}
    end

    API-->>Client: {:ok, boolean}
```

### CONSTRUCT Query

```mermaid
sequenceDiagram
    participant Client
    participant API as TripleStore
    participant Exec as Executor
    participant Builder as Graph.Builder

    Client->>API: query(store, "CONSTRUCT { ... }")
    API->>Exec: execute(construct_query)

    Exec->>Exec: execute WHERE clause
    Exec->>Builder: build_triples(template, bindings)

    loop For each binding
        Builder-->>Exec: triple
    end

    Exec-->>API: {:ok, rdf_graph}
    API-->>Client: {:ok, RDF.Graph}
```

## Data Loading Flow

### Load File

```mermaid
sequenceDiagram
    participant Client
    participant API as TripleStore
    participant Loader as Loader
    participant Parser as RDF.Parser
    participant Dict as Dictionary
    participant Idx as Index
    participant NIF as ErlangAdapter
    participant RocksDB

    Client->>API: load(store, "data.ttl")
    API->>Loader: load_file(path)

    Loader->>Parser: parse_file(path)
    Parser-->>Loader: stream_of_triples

    loop For each batch
        Loader->>Dict: batch_encode_terms(triples)
        Dict->>NIF: get_or_create(term)
        NIF-->>Dict: term_id
        Dict-->>Loader: term_ids

        Loader->>Idx: batch_insert_quads(term_ids)
        Idx->>NIF: write_batch(operations)
        NIF->>RocksDB: Atomic write
    end

    Loader-->>API: {:ok, total_count}
    API-->>Client: {:ok, count}
```

### Load with Named Graph

```mermaid
sequenceDiagram
    participant Client
    participant Loader
    participant Dict
    participant Idx as QuadIndex
    participant NIF

    Client->>Loader: load(store, "data.nq", graph: iri)
    Loader->>Loader: Parse N-Quads

    Note over Loader: Each line: "subject predicate object graph ."

    loop For each quad
        Loader->>Dict: encode_graph_iri(graph_iri)
        Dict-->>Loader: graph_id
        Loader->>Dict: encode_terms(s, p, o)
        Dict-->>Loader: s_id, p_id, o_id
        Loader->>Idx: insert_quad(graph_id, s_id, p_id, o_id)
    end
```

### Batch Size Selection

| File Size | Recommended Batch | Rationale |
|-----------|-------------------|-----------|
| < 1 MB | 100-500 | Low overhead, commit frequently |
| 1-100 MB | 1,000-5,000 | Balance throughput and memory |
| 100 MB - 1 GB | 5,000-10,000 | Larger writes, amortize overhead |
| > 1 GB | 10,000-50,000 | Maximize throughput |

## Update Operations Flow

### INSERT DATA

```mermaid
sequenceDiagram
    participant Client
    participant API as TripleStore
    participant TXN as Transaction
    participant Dict as Dictionary
    participant Idx as Index
    participant Cache as PlanCache

    Client->>API: update(store, "INSERT DATA { ... }")
    API->>TXN: update(update_string)

    TXN->>TXN: Parse UPDATE

    loop For each triple
        TXN->>Dict: get_or_create_id(subject)
        Dict-->>TXN: s_id
        TXN->>Dict: get_or_create_id(predicate)
        Dict-->>TXN: p_id
        TXN->>Dict: get_or_create_id(object)
        Dict-->>TXN: o_id

        TXN->>Idx: insert_triple(s_id, p_id, o_id)
    end

    TXN->>TXN: write_batch_commit()
    TXN->>Cache: invalidate()
    TXN-->>API: {:ok, count}
    API-->>Client: {:ok, count}
```

### DELETE ... WHERE

```mermaid
sequenceDiagram
    participant Client
    participant API as TripleStore
    participant TXN as Transaction
    participant Query as SPARQL.Executor
    participant Idx as Index

    Client->>API: update(store, "DELETE WHERE { ... }")
    API->>TXN: update(update_string)

    TXN->>Query: execute(WHERE clause)
    Query-->>TXN: matching_bindings

    loop For each binding
        TXN->>Idx: delete_triple(s, p, o)
    end

    TXN->>TXN: write_batch_commit()
    TXN-->>API: {:ok, deleted_count}
    API-->>Client: {:ok, count}
```

### DELETE/INSERT ... WHERE

```mermaid
sequenceDiagram
    participant Client
    participant TXN as Transaction
    participant Idx as Index
    participant Snap as Snapshot

    Client->>TXN: update("DELETE { ... } INSERT { ... } WHERE { ... }")

    TXN->>Snap: create_snapshot()
    Snap-->>TXN: snapshot_ref

    par Delete phase
        TXN->>Idx: delete_using_snapshot(snapshot, WHERE)
    and Insert phase
        TXN->>Idx: insert_using_bindings(WHERE)
    end

    TXN->>TXN: atomic_commit()
    TXN->>Snap: release()
```

## Reasoning Flow

### Full Materialization

```mermaid
sequenceDiagram
    participant Client
    participant API as TripleStore
    participant Compiler as RuleCompiler
    participant SemiNaive as SemiNaive
    participant Rules as Compiled Rules
    participant Idx as Index
    participant Derived as DerivedStore

    Client->>API: materialize(store, profile: :owl2rl)
    API->>Compiler: compile(store, profile)

    Compiler->>Compiler: Extract schema info
    Compiler->>Compiler: Filter applicable rules
    Compiler-->>API: compiled_rules

    API->>SemiNaive: materialize(lookup, store, rules)

    SemiNaive->>Idx: get_all_explicit()
    Idx-->>SemiNaive: explicit_facts

    SemiNaive->>SemiNaive: delta = explicit_facts

    loop Until fixpoint
        SemiNaive->>Rules: apply_rules(delta)
        Rules-->>SemiNaive: new_facts

        SemiNaive->>Idx: check_exists(new_facts)
        Idx-->>SemiNaive: existing_facts

        SemiNaive->>SemiNaive: delta = new_facts - existing
        SemiNaive->>Derived: insert_derived(new_facts)
    end

    SemiNaive-->>API: {:ok, stats}
    API-->>Client: {:ok, stats}
```

### Semi-Naive Evaluation

```mermaid
graph TB
    subgraph "Iteration 1"
        E1["Explicit facts"]
        D1["Δ1 = Explicit"]
        R1["Apply rules(Δ1)"]
        N1["New facts"]
    end

    subgraph "Iteration 2"
        D2["Δ2 = New facts"]
        R2["Apply rules(Δ2)"]
        N2["New facts"]
    end

    subgraph "Iteration 3"
        D3["Δ3 = New facts"]
        R3["Apply rules(Δ3)"]
        N3["New facts = ∅"]
    end

    subgraph "Complete"
        DONE["Fixpoint reached"]
    end

    E1 --> D1
    D1 --> R1
    R1 --> N1
    N1 --> D2
    D2 --> R2
    R2 --> N2
    N2 --> D3
    D3 --> R3
    R3 --> N3
    N3 --> DONE
```

### Incremental Addition

```mermaid
sequenceDiagram
    participant Client
    participant API as TripleStore
    participant Incr as Incremental
    participant SemiNaive as SemiNaive
    participant Idx as Index

    Client->>API: add_with_reasoning(store, new_triples)
    API->>Incr: add_with_reasoning(db, triples, rules)

    Incr->>Idx: check_exists(triples)
    Idx-->>Incr: existing_triples

    Incr->>Incr: delta = new_triples - existing
    Incr->>Idx: insert_explicit(delta)

    Incr->>SemiNaive: materialize(delta, rules)
    SemiNaive-->>Incr: derived_facts

    Incr->>Idx: insert_derived(derived_facts)
    Incr-->>API: {:ok, stats}
    API-->>Client: {:ok, stats}
```

## GRAPH Clause Flow

### Query Specific Graph

```mermaid
sequenceDiagram
    participant Client
    participant Exec as SPARQL.Executor
    participant QuadIdx as QuadIndex
    participant Dict as Dictionary

    Client->>Exec: query("SELECT ... WHERE { GRAPH ex:g1 { ?s ?p ?o } }")
    Exec->>Exec: resolve_graph_id("ex:g1")
    Exec->>QuadIdx: lookup_pattern({g: bound, s: var, p: var, o: var})
    QuadIdx-->>Exec: stream_of_quads

    loop For each quad
        Exec->>Dict: decode_ids([s_id, p_id, o_id])
        Dict-->>Exec: [s_term, p_term, o_term]
        Exec-->>Client: binding_row
    end
```

### Query All Graphs

```mermaid
sequenceDiagram
    participant Client
    participant Exec as SPARQL.Executor
    participant QuadIdx as QuadIndex
    participant Dict as Dictionary

    Client->>Exec: query("SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o } }")
    Exec->>QuadIdx: list_graphs()
    QuadIdx-->>Exec: [g1, g2, g3]

    loop For each graph
        Exec->>QuadIdx: lookup_pattern({g: bound, ...})
        QuadIdx-->>Exec: quads_in_graph

        loop For each quad
            Exec->>Dict: decode_ids
            Exec-->>Client: binding_with_graph
        end
    end
```

## Backup Flow

### Create Backup

```mermaid
sequenceDiagram
    participant Client
    participant API as TripleStore
    participant Backup as Backup
    participant Snap as RocksDB.Snapshot
    participant Files as File.System

    Client->>API: backup(store, "/backup/path")
    API->>Backup: create(db, backup_path)

    Backup->>Snap: create_snapshot()
    Snap-->>Backup: snapshot_ref

    Backup->>Files: create_backup_dir()
    Backup->>Snap: iterate_all_column_families()

    loop For each CF
        Snap-->>Backup: key_value_pairs
        Backup->>Files: write_sst_file(cf, data)
    end

    Backup->>Files: write_metadata(manifest)
    Backup->>Snap: release_snapshot()
    Backup-->>API: {:ok, backup_metadata}
    API-->>Client: {:ok, metadata}
```

### Restore Backup

```mermaid
sequenceDiagram
    participant Client
    participant API as TripleStore
    participant Backup as Backup
    participant RocksDB as Database
    participant Files as File.System

    Client->>API: restore(backup_path, target_path)
    API->>Backup: restore(backup_path, target_path)

    Backup->>Files: read_metadata(manifest)
    Backup->>RocksDB: open_new(target_path)

    loop For each SST file
        Backup->>Files: read_sst_file(cf)
        Backup->>RocksDB: ingest_sst_file(cf, sst_path)
    end

    Backup->>RocksDB: close()
    Backup-->>API: {:ok, stats}
    API-->>Client: {:ok, stats}
```

## Cache Invalidation Flow

```mermaid
sequenceDiagram
    participant Write as Write Operation
    participant TXN as Transaction
    participant PlanCache as Plan Cache
    participant Stats as Statistics
    participant Query as Next Query

    Write->>TXN: execute_write()
    TXN->>TXN: write_batch_commit()

    alt Write successful
        TXN->>PlanCache: invalidate()
        PlanCache-->>TXN: :ok
        TXN->>Stats: mark_stale()
        Stats-->>TXN: :ok
    end

    TXN-->>Write: {:ok, count}

    Note over Query: Subsequent query
    Query->>PlanCache: lookup(query_hash)
    PlanCache-->>Query: :miss (cache invalidated)
    Query->>Stats: get_fresh_statistics()
```

## Module Reference

| Module | Role in Data Flow |
|--------|-------------------|
| `TripleStore` | API entry point, routes to appropriate subsystem |
| `TripleStore.SPARQL.Parser` | Converts SPARQL strings to AST |
| `TripleStore.SPARQL.Algebra` | Converts AST to algebra representation |
| `TripleStore.SPARQL.Optimizer` | Transforms algebra to execution plan |
| `TripleStore.SPARQL.Executor` | Executes plan against indices |
| `TripleStore.Transaction` | Coordinates write operations |
| `TripleStore.Loader` | Handles bulk data loading |
| `TripleStore.Dictionary` | Encodes/decodes RDF terms |
| `TripleStore.Index` / `TripleStore.QuadIndex` | Pattern matching lookup |
| `TripleStore.Backend.RocksDB.ErlangAdapter` | RocksDB operations |
| `TripleStore.Reasoner` | OWL 2 RL materialization |

## Performance Considerations

### Query Flow Optimization

| Stage | Optimization Technique |
|-------|----------------------|
| Parsing | NIF-based (Rust) for speed |
| Plan caching | Normalize variable names for cache hits |
| Join ordering | Cost-based enumeration |
| Index selection | Pattern-based optimal index |
| Result streaming | Iterator-based, lazy evaluation |

### Load Flow Optimization

| Stage | Optimization Technique |
|-------|----------------------|
| File parsing | Streaming parsers |
| Term encoding | Batch allocation, sharded dictionary |
| Index writes | Write batching, atomic commits |
| Memtable tuning | Larger memtables for bulk load |

## Next Steps

- [Architecture Overview](00-architecture-overview.md) - For component interaction
- [Storage Layer](01-storage-layer.md) - For RocksDB operations
- [OTP & Concurrency](07-otp-concurrency.md) - For process architecture
