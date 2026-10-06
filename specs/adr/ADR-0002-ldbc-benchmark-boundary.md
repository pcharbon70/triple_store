# ADR-0002: LDBC Benchmark Adapter and RDF Mapping Boundary

## Status

Accepted

## Context

SPB is an RDF/SPARQL workload, while SNB BI and Interactive are language-neutral
graph workloads with Java drivers and reference implementations commonly expressed
in Cypher or SQL. TripleStore is an embedded Elixir library, not an HTTP service.
The benchmark integration needs canonical-driver control without adding a product
server or moving query, update, transaction, reasoning, or graph-algorithm semantics
out of their Elixir-owned control planes.

The Interactive v1 driver supports Java 8. Java's Unix-domain socket API is therefore
not a portable baseline. A native Java-to-BEAM NIF would enlarge the trusted native
boundary and couple benchmark code to application runtime code. An unframed stdio
protocol would make cancellation, partial reads, and shutdown ambiguous.

SNB also requires one explicit property-graph-to-RDF mapping. Letting each query or
reference adapter invent its own representation would make cross-validation and
performance results incomparable.

## Decision

1. Canonical Java drivers integrate through a benchmark-owned Erlang Port child
   process using four-byte length framing and UTF-8 JSON payloads.
2. Every scalar crossing the bridge uses an explicit type tag, including IDs,
   32-bit and 64-bit integers, floats, booleans, strings, dates, datetimes, IRIs,
   lists, records, and unbound values. Numeric IDs are not transported as JSON
   numbers when precision could be lost.
3. Frames carry protocol version, request ID, profile ID, operation ID, typed
   parameters or results, deadline, and one of request, success, error,
   cancellation, health, or shutdown kinds.
4. The bridge lives under `TripleStore.Benchmark.LDBC` and is started explicitly
   by benchmark tooling. It is absent from `TripleStore.Application`, the public
   `TripleStore` facade, and normal store lifecycle.
5. One bridge owner owns one open store, dictionary manager, store transaction
   coordinator, benchmark caches, statistics helpers, Java Port, and outstanding
   requests. It closes these resources deterministically in reverse ownership order.
6. The Java adapter maps driver operations to stable catalog IDs. It does not
   contain query semantics, result post-processing that repairs engine answers,
   scheduling substitutions, or hidden retries.
7. TripleStore-owned translations, graph algorithms, and result codecs remain in
   Elixir benchmark and query modules. Native parsing and RocksDB remain bounded
   adapters under the existing ownership matrix and control-plane authority decision.
8. Phase 2 defines one versioned SNB RDF mapping. The mapping owns IRIs, entity and
   relationship representation, relationship properties, datatype encodings,
   default or named graph placement, and deterministic serialization. Dataset
   conversion, operation translations, expected-answer conversion, and artifacts
   must cite the same mapping version.
9. Any shortest-path or weighted-path behavior absent from SPARQL 1.1 is implemented
   as an explicit engine-owned extension with tests and capability metadata.
   Application-side filtering or post-processing may format results but may not
   hide missing joins, paths, aggregates, ordering, limits, or update effects.
10. The bridge binds no listening network socket. A future transport change requires
    a new ADR and cannot silently turn TripleStore into a service.

## Protocol failure rules

- Unknown protocol, profile, operation, or type versions fail before execution.
- Duplicate active request IDs, malformed frames, oversized frames, and responses
  for unknown requests terminate the affected run.
- Deadlines cover queueing and operation execution. Cancellation has an explicit
  acknowledgement and a late success cannot enter benchmark metrics.
- Driver termination, Port exit, store failure, and graceful shutdown fail all
  outstanding requests with typed errors and release every owned resource.
- Only complete successful typed results may reach correctness comparison or timing.

## Consequences

- Existing Java drivers can control an embedded TripleStore process without a
  general-purpose HTTP or socket service.
- Benchmark adapters remain removable tooling and do not become public API.
- SNB translations and datasets share one reviewable semantic representation.
- Missing engine capabilities remain visible in the capability matrix instead of
  being concealed in driver code.
- Phase 3 must implement and test framing, typed codecs, backpressure, cancellation,
  deadlines, health checks, and ownership cleanup.

## Related authority

- [Control-plane ownership matrix](../contracts/control_plane_ownership_matrix.md)
- [Transaction and isolation contract](../contracts/transaction_and_isolation_contract.md)
- [Query execution contract](../contracts/query_execution_contract.md)
- [Reasoning contract](../contracts/reasoning_contract.md)
- [LDBC Phase 1 plan](../../notes/planning/ldbc/phase-01-authority-and-capability-baseline.md)
