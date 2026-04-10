# Phase 2: Query Corpus Construction

Description: Phase 2 builds the benchmark query corpora that will run against TripleStore. By the end of this phase, we should have imported and normalized public workload families, created a Scholia-derived benchmark corpus, and validated that every benchmark query is packaged as a runnable, categorized artifact.

---

## 2.1 Public Workload Importers

Description: This section imports benchmark workloads inspired by public Wikidata evaluations and converts them into benchmark-ready query suites for TripleStore.

- [x] **Section 2.1 Complete** (2026-04-10)

### 2.1.1 WGPB and WDQS Import

Description: This task imports simple graph-pattern workloads and user-facing query workloads into a consistent local benchmark representation.

- [x] 2.1.1.1 Import `WGPB` query sets and preserve original pattern grouping
- [x] 2.1.1.2 Import `WDQS` query sets and capture stable benchmark IDs and names
- [x] 2.1.1.3 Annotate imported queries with shape metadata, feature tags, and expected answer-size class
- [x] 2.1.1.4 Define corpus-level metadata for source origin, license notes, and preprocessing history
- [x] 2.1.1.5 Add normalization hooks for LIMIT policy and standards-compliant rewrites where needed

### 2.1.2 WDBench Import and Expansion

Description: This task imports fragment-oriented workloads derived from query logs and expands them into complete benchmark queries that TripleStore can execute directly.

- [x] 2.1.2.1 Import the five `WDBench` families: single BGP, multiple BGPs, OPTIONAL, property paths, and other
- [x] 2.1.2.2 Expand fragments into full SPARQL queries with stable identifiers
- [x] 2.1.2.3 Apply configurable LIMIT policies by benchmark tier
- [x] 2.1.2.4 Preserve family-level grouping so reports can summarize by workload shape
- [x] 2.1.2.5 Record any fragments that cannot be converted cleanly and classify them as exclusions

## 2.2 Scholia-Derived Workload

Description: This section builds a Scholia-style benchmark corpus from reusable query templates while keeping the resulting workload standards-compliant and friendly to TripleStore.

- [x] **Section 2.2 Complete** (2026-04-10)

### 2.2.1 Template Normalization

Description: This task converts Scholia-oriented query templates into standard SPARQL and prepares them for deterministic benchmark instantiation.

- [x] 2.2.1.1 Extract Scholia template classes and template metadata into local benchmark assets
- [x] 2.2.1.2 Replace engine-specific constructs with standards-compliant SPARQL equivalents
- [x] 2.2.1.3 Implement label-service and convenience-template rewrites where needed
- [x] 2.2.1.4 Add parameter-substitution helpers for entities, classes, and common template variables
- [x] 2.2.1.5 Tag templates by complexity, feature usage, and likely execution stress points

### 2.2.2 Template Instantiation Strategy

Description: This task defines how Scholia templates are instantiated into a stable benchmark corpus for each dataset tier.

- [x] 2.2.2.1 Select representative item sets per class for `smoke`, `medium`, `large`, and `full_dump` tiers
- [x] 2.2.2.2 Materialize instantiated queries with stable benchmark IDs and suite membership
- [x] 2.2.2.3 Generate query variants for count-only and distinct-only execution where appropriate
- [x] 2.2.2.4 Record per-template instantiation metadata so results can be traced back to the original template
- [x] 2.2.2.5 Define fallback behavior when a template has no valid instantiations for a smaller dataset tier

## 2.3 Integration Tests

Description: This section verifies that all benchmark corpora parse, normalize, and execute as valid workload packages against prepared benchmark stores.

- [x] **Section 2.3 Complete** (2026-04-10)

### 2.3.1 Corpus Validation Integration

Description: This task validates the structural integrity of imported and generated query corpora before runtime benchmarking begins.

- [x] 2.3.1.1 Verify every benchmark query parses successfully after normalization
- [x] 2.3.1.2 Verify every parameterized template instantiates into valid SPARQL
- [x] 2.3.1.3 Verify every benchmark query belongs to a named suite and category
- [x] 2.3.1.4 Verify excluded queries are tracked explicitly rather than being dropped silently

### 2.3.2 Query Execution Smoke Integration

Description: This task validates that representative queries from every workload family execute successfully against benchmark fixtures.

- [x] 2.3.2.1 Run a smoke sample from `wgpb`, `wdbench`, `wdqs`, and `scholia` against the `smoke` dataset tier
- [x] 2.3.2.2 Verify count-only and distinct-only variants compile and execute correctly
- [x] 2.3.2.3 Verify query metadata survives from corpus construction to runtime execution
- [x] 2.3.2.4 Verify unsupported constructs surface explicit benchmark errors or exclusions
