# TripleStore Topology

## Purpose

This document defines the canonical topology for `TripleStore`.

It covers:

- caller entry points
- the small default OTP supervision tree
- store-local managers and optional helpers
- query, update, reasoning, and operations flow
- persisted schema topology for triple and quad stores

## Core Topology Decisions

- `TripleStore` is the primary caller-facing entry point, but expert modules are legitimate topology nodes rather than hidden internals.
- `TripleStore.Application` supervises only process-global helpers that do not require a store-specific DB reference.
- Store-local managers are created at `open/2` time, not pre-created globally.
- SPARQL query, update, graph-management, and reasoning meaning remain Elixir-owned even when parser or storage work crosses a native boundary.
- Triple and quad stores are different persisted topologies, not cosmetic variations over the same column-family layout.
- Named graphs, ACLs, and provenance are topology nodes in quad schema.
- Optional runtime helpers such as `Query.Cache`, `Metrics`, `Prometheus`, `Statistics.Server`, and `ScheduledBackup` are opt-in topology additions, not guaranteed baseline children.

## Runtime Topology

```mermaid
graph TD
  HOST["Embedding Application"] --> API["TripleStore Facade"]
  HOST --> EXPERT["Expert Modules"]

  API --> APP["TripleStore.Application Supervisor"]
  API --> STORE["Store Handle"]
  EXPERT --> STORE

  APP --> PLAN["SPARQL.PlanCache"]
  APP --> SNAP["Snapshot Registry"]

  STORE --> DICT["Dictionary.Manager or ShardedManager"]
  STORE -. optional .-> TXN["Transaction Coordinator"]
  STORE -. optional .-> STATS["Statistics.Cache or Statistics.Server"]
  STORE -. optional .-> QC["Query.Cache"]
  STORE -. optional .-> MET["Metrics / Prometheus"]
  STORE -. optional .-> SCHED["ScheduledBackup"]

  STORE --> LOAD["Loader / Adapter"]
  STORE --> QRY["SPARQL.Query"]
  STORE --> RSN["Reasoning APIs"]
  STORE --> OPS["Health / Backup / GraphBackup / Exporter"]

  QRY --> PARSE["Parser Facade + Parser NIF"]
  QRY --> ALG["Algebra / Term / Expression"]
  QRY --> OPT["Optimizer / Cost / Cardinality / PlanCache"]
  QRY --> EXEC["Executor / Validation / Authorization / PropertyPath"]

  EXEC --> TRIPLEIDX["Index + NumericRange + SubjectCache"]
  EXEC --> QUADIDX["QuadIndex + QuadOperations"]

  TXN --> UPD["SPARQL.UpdateExecutor"]
  UPD --> EXEC
  UPD --> TRIPLEIDX
  UPD --> QUADIDX

  RSN --> RULES["ReasoningProfile / RuleCompiler / RuleOptimizer"]
  RSN --> ENGINE["SemiNaive / Incremental / GraphScopedReasoner"]
  ENGINE --> DERIVED["DerivedStore / DerivationProvenance / Status"]
  ENGINE --> TRIPLEIDX
  ENGINE --> QUADIDX

  LOAD --> TRIPLEIDX
  LOAD --> QUADIDX
  OPS --> ERL["RocksDB ErlangAdapter"]
  TRIPLEIDX --> ERL
  QUADIDX --> ERL
  DERIVED --> ERL

  ERL --> TRIPLECF["Triple CFs: id2str, str2id, spo, pos, osp, derived, numeric_range"]
  ERL --> QUADCF["Quad CFs: id2str, str2id, gspo, gpos, spog, posg, derived, derivation_provenance, numeric_range, acl"]
```

## Component Classes

| Class | Primary Modules | Responsibility |
|---|---|---|
| Primary facade | `TripleStore` | Main lifecycle, query, update, reasoning, export, health, backup, and scheduling API |
| Expert modules | `TripleStore.Update`, `TripleStore.GraphBackup`, `TripleStore.QuadOperations`, `TripleStore.Health`, `TripleStore.SPARQL.Authorization` | Narrower APIs for direct update contexts, graph backup, quad storage, rich health, and graph ACLs |
| Global runtime services | `TripleStore.Application`, `TripleStore.SPARQL.PlanCache`, `TripleStore.Snapshot` | Process-global support services |
| Store-local coordination | `TripleStore.Dictionary.Manager`, `TripleStore.Dictionary.ShardedManager`, `TripleStore.Transaction`, `TripleStore.Statistics.Cache`, `TripleStore.Statistics.Server` | ID allocation, optional serialized SPARQL update coordination, statistics support |
| Storage semantics | `TripleStore.Adapter`, `TripleStore.Dictionary.*`, `TripleStore.Index`, `TripleStore.QuadIndex`, `TripleStore.QuadOperations`, `TripleStore.Loader`, `TripleStore.Exporter` | RDF conversion, dictionary encoding, index fanout, loading, export |
| Query execution | `TripleStore.SPARQL.*`, `TripleStore.Query.Cache`, `TripleStore.SPARQL.QueryCache` | Parse, compile, optimize, execute, authorize, validate, and optionally cache query work |
| Reasoning execution | `TripleStore.Reasoner.*` | Compile rules, materialize, maintain derived facts, track graph reasoning status and provenance |
| Native boundaries | `TripleStore.Backend.RocksDB.ErlangAdapter`, `TripleStore.SPARQL.Parser.NIF` | Bounded storage and parser capability |
| Operations | `TripleStore.Telemetry`, `TripleStore.Health`, `TripleStore.Backup`, `TripleStore.GraphBackup`, `TripleStore.ScheduledBackup`, `TripleStore.Metrics`, `TripleStore.Prometheus` | Observability, recovery, backup, and scheduling |

## Query Flow Topology

```mermaid
flowchart LR
  A["Caller"] --> B["TripleStore.query/3 or SPARQL.Query"]
  B --> C["Parser NIF"]
  C --> D["Algebra"]
  D --> E["Optimizer / CostModel / Cardinality / PlanCache"]
  E --> F["Executor / Validation / Authorization / PropertyPath"]
  F --> G{"Schema"}
  G -->|triple| H["Index / NumericRange / SubjectCache"]
  G -->|quad| I["QuadIndex / QuadOperations"]
  H --> J["RocksDB ErlangAdapter"]
  I --> J
  F --> K["Streaming or materialized results"]
```

## Update And Mutation Flow Topology

```mermaid
flowchart LR
  A["Caller"] --> B{"Mutation Surface"}
  B -->|SPARQL UPDATE| C["TripleStore.update/2 or TripleStore.Update"]
  B -->|load/insert/delete| D["Loader / Adapter"]
  B -->|quad expert ops| E["QuadOperations / GraphBackup"]

  C --> F["Managed or temporary Transaction"]
  F --> G["Parser NIF"]
  G --> H["UpdateExecutor"]
  H --> I["Executor for WHERE patterns"]
  H --> J["Index or QuadOperations write batches"]
  J --> K["PlanCache / Query.Cache invalidation"]

  D --> J
  E --> J
```

## Reasoning Flow Topology

```mermaid
flowchart LR
  A["Caller"] --> B{"Reasoning Entry Point"}
  B -->|materialize/2 local| C["Legacy triple materialization"]
  B -->|materialize_graph* / materialize_all| D["GraphScopedReasoner"]
  B -->|add/delete quads with reasoning| E["IncrementalQuad / DeleteWithReasoningQuad"]

  C --> F["ReasoningProfile / SemiNaive"]
  D --> G["GraphReasoningConfig / RuleCompiler / SemiNaive"]
  E --> H["BackwardTrace / ForwardRederive / Provenance"]

  F --> I["DerivedStore"]
  G --> I
  H --> I
  I --> J["ReasoningStatus / GraphReasoningStatus / DerivationProvenance"]
  I --> K["RocksDB ErlangAdapter"]
```

## Topology Constraints

- The default application runtime MUST remain smaller than the full set of optional helper modules.
- Schema choice at `open/2` time MUST determine the active persisted topology for that store.
- Public mutation semantics MUST distinguish between direct batch-mutation helpers and SPARQL update coordination through `Transaction`.
- Public `TripleStore.query/3` MUST NOT be described as implicitly using transaction-query snapshots, because the current code does not route it that way.
- Graph clauses, graph management, ACL checks, and graph-scoped reasoning MUST be documented as quad-schema behavior.
- `TripleStore.materialize/2` local mode MUST be treated as the legacy triple-materialization path; graph-aware reasoning lives in the explicit graph APIs.
- Operational modules MUST observe the same canonical runtime and data topology used by query, update, and reasoning code.

## Current Codebase Notes

- The default topology includes only global plan-cache and snapshot services.
- `Query.Cache`, `SPARQL.QueryCache`, `Metrics`, `Prometheus`, `Statistics.Server`, and `ScheduledBackup` are opt-in additions.
- `TripleStore.Health` provides richer health topology than the compact `TripleStore.health/1` facade.
- `GraphBackup` and dataset export make named-graph topology a current operational concern, not a future design placeholder.
