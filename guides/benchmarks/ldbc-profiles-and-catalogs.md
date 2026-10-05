# LDBC Profiles and Operation Catalogs

TripleStore separates execution profiles from operation catalogs. Profiles define
which upstream protocol guarantees are preserved and which report language is
allowed. Catalogs inventory canonical operations without claiming that the engine
implements them yet; implementation status belongs to the capability matrix.

## Claim levels

| Level | Purpose | Scoring and reporting |
| --- | --- | --- |
| `smoke` | Offline fixtures and representative operations | Diagnostic metrics only; never comparable |
| `development` | Selected operation families or shortened execution | Diagnostic metrics only; explicitly non-comparable |
| `comparable` | Complete pinned operation mix, parameters, scheduling, validation, and scoring | Canonical score namespace; may say comparable but not official, certified, or audited |
| `audit_preparation` | Comparable protocol plus configuration, pricing, provenance, and full disclosure evidence | Protected audit terminology remains forbidden until completed external-audit metadata is attached |

`TripleStore.Benchmark.LDBC.Profile.validate_report_claim/3` enforces the protected
terms `official`, `certified`, and `audited`. A profile name alone never grants
those claims.

## Catalog scope

- SPB v2.0.2 records 25 aggregation queries, three editorial operations, three
  editorial validations, ten named OWL 2 RL conformance checks, nine lifecycle
  phases, and five audit-only resilience actions.
- SNB BI v1.0.3 records all 20 reads, their parameter variants, typed result and
  ordering contracts, choke points, and the ordered update-microbatch operation.
- SNB Interactive v1.2.0 records 14 complex reads, seven short reads, and eight
  inserts with their typed parameters, results, limits, ordering, driver frequency,
  and stream dependencies.
- Interactive v2 records its version-specific read variants and eight deletes in
  a separate work-in-progress delta catalog. It does not inherit v1 comparable or
  audit claims.

Every operation has a stable local ID containing its benchmark, canonical upstream
identifier, and pinned profile version. `TripleStore.Benchmark.LDBC.Catalog`
requires the expected operation-ID set to match the actual catalog exactly and
rejects duplicate or unversioned IDs.
