---
title: SPARQL query path
description: Trace SPARQL text through parsing, algebra, optimization, and execution
chapter_step: "2"
inspected_revision: "29608a6217a7db4204fc2f583978451805afd8fb"
chapter_state: current
---

# SPARQL query path

## Chapter record

| Field | Value |
| --- | --- |
| Step | `2` |
| Inspected revision | `29608a6217a7db4204fc2f583978451805afd8fb` |
| State | `current` |
| Scope | Trace query text through parsing, planning, execution, and result boundaries. |

## Problem and mental model

SPARQL text is not a RocksDB scan plan. It crosses a native parsing boundary,
then Elixir-owned algebra, planning, execution, and RDF result conversion.

```mermaid
flowchart LR
  Text["SPARQL text"] --> Facade["TripleStore.query"]
  Facade --> Query["SPARQL.Query"]
  Query --> Parser["Parser facade / Rust NIF"]
  Parser --> Algebra["Algebra"]
  Algebra --> Optimizer["Optimizer"]
  Optimizer --> Executor["Executor"]
  Executor --> Indices["Dictionary IDs + indices"]
  Indices --> Results["RDF results or stream"]
```

## Important files

- `lib/triple_store/sparql/query.ex` orchestrates validation, timeout,
  parsing, planning, optional result cache, eager results, and streams.
- `lib/triple_store/sparql/parser.ex` normalizes parser calls and typed errors.
- `lib/triple_store/sparql/parser/nif.ex` is the Elixir Rustler adapter.
- `native/sparql_parser_nif/src/lib.rs` parses SPARQL with `spargebra` and
  converts native AST data for Elixir.
- `lib/triple_store/sparql/algebra.ex` represents executable query operations.
- `lib/triple_store/sparql/optimizer.ex` applies logical and cost-aware
  rewrites before execution.
- `lib/triple_store/sparql/executor.ex` evaluates patterns and graph clauses
  using dictionary IDs and schema-appropriate indices.
- `test/triple_store/sparql/query_test.exs` has query API cases.
- `test/triple_store/sparql/leapfrog/quad_iterator_cleanup_test.exs` tests
  resource cleanup for a lazy quad join path.

## Execution flow

1. `TripleStore.query/3` constructs a context containing the DB and dictionary
   manager, sanitizes telemetry metadata, and calls `SPARQL.Query.query/3`.
   It does not enqueue the query on the store transaction coordinator.
2. `SPARQL.Query` validates options. The eager path runs under timeout
   isolation, parses text through the Rust-backed parser facade, compiles
   algebra, optionally optimizes it, and executes it through Elixir modules.
3. The executor uses integer dictionary IDs and triple or quad index lookup.
   Graph clauses and `FROM` datasets affect which quad graph IDs are scanned.
   Final bindings become RDF terms for the public result shape.
4. Optional result caching belongs to `TripleStore.Query.Cache`; it is
   distinct from the automatically supervised `SPARQL.PlanCache` and from the
   separate `SPARQL.QueryCache` implementation. Named-graph queries governed
   by ACLs bypass result caching until authorization has a stable revision
   identity.
5. `stream_query/3` builds a lazy stream. Timeout protection does not extend
   over arbitrary subsequent consumption. The consumer's exit path matters
   because a partially read stream may own native iterators.

## Data, ownership, and lifetime

The parser NIF returns AST-shaped data, not authoritative query results.
Algebra and execution remain Elixir-owned. The optimizer may select join
strategies such as Leapfrog; callers do not choose a physical index directly.
The quad Leapfrog stream closes owned iterators on exhaustion, early halt, and
failure after enumeration starts; callers constructing a stream without
enumerating it must use the lower-level close API.

## Failure and trust boundaries

Parse, validation, execution, timeout, and authorization failures propagate as
tagged results. A native parser success does not mean the query is authorized
or executable. The facade does not expose an actor-aware query option;
user-aware ACL checks exist in lower-level contexts. Named graphs by
themselves therefore do not provide tenant authorization.

## Tests and evidence limits

The mapped query tests cover public query behavior; the mapped iterator
regression covers cleanup of the quad Leapfrog path. Other relevant cases
live in `test/triple_store/sparql/parser_test.exs` and
`test/triple_store/sparql/query_dataset_property_path_test.exs`. These are
test-file references, not a claim that the suite passed during chapter
authoring. Eager timeout tests do not prove a timeout for later lazy stream
consumption, and lower-level ACL tests do not add an actor option to the facade.

## Normative connections

The [query contract](https://github.com/pcharbon70/triple_store/blob/main/specs/contracts/query_execution_contract.md#requirement-set)
sets the parser/Elixir authority boundary, index-backed execution, timeout,
cache, and authorization requirements. The
[query area spec](https://github.com/pcharbon70/triple_store/blob/main/specs/query/sparql_execution_pipeline.md#current-codebase-notes)
records current limitations. `REQ-QRY-007` explicitly distinguishes eager
execution from stream consumption; `REQ-QRY-011` records the facade actor
boundary.

## Revision and maintenance

Revisit this chapter and its coverage rows when parser AST conversion,
algebra, optimizer selection, graph dataset semantics, cache identity,
authorization context, or stream lifetime changes.
