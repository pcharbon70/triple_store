# LDBC Phase 1 Capability Baseline

`priv/benchmarks/ldbc/capabilities.exs` classifies every pinned catalog operation as
`supported`, `requires_fix`, `requires_extension`, or `profile_exclusion`. Parser
support and end-to-end execution support are recorded separately. A parsed query is
not considered implemented until its typed, ordered result matches accepted answers.

## Main findings

- The current parser and executor cover broad SPARQL 1.1 syntax, aggregation,
  OPTIONAL, UNION, ordering, slicing, and property paths. Full LDBC translations
  and typed answer comparisons remain Phase 4-6 work.
- BI 10, 15, 19, and 20 and Interactive 13 and 14 require path length, returned
  paths, or weighted path cost unavailable from standard SPARQL property paths.
  Phase 5 adds bounded index-backed traversal primitives for BI, but the four
  operation-specific graph adapters and accepted answer comparisons remain open.
- The SNB BI protocol, scoring, atomic microbatch, checkpoint, and artifact
  boundaries are implemented. Analytical reads remain fail-closed until exact
  engine handlers pass reference-answer validation; see
  [the SNB BI guide](ldbc-snb-bi.md).
- `TripleStore.query/3` does not enter the store transaction coordinator. Direct
  insert, delete, and loader paths also remain outside its queue. Interactive must
  use one benchmark-owned read/write visibility boundary.
- SPB now uses quad-schema context preservation and a persistent graph-aware
  materialization path for its selected reasoning profile. The ten separately
  catalogued inference conformance actions remain unqualified.
- Coordinated backup and fresh-path restore are integrated with the SPB smoke
  workflow. Online replication and failover remain excluded because TripleStore
  has no such product surface.
- Text and geospatial SPB options are excluded from current profiles.
- The generic benchmark runner is not eligible for LDBC scoring because its error
  handling is not the Phase 3 fail-fast measurement contract.

The selected bridge and RDF-mapping ownership are recorded in
[ADR-0002](../../specs/adr/ADR-0002-ldbc-benchmark-boundary.md). Every unresolved
finding names its implementation owner and target phase.
