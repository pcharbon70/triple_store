# Phase 1: Benchmark Authority and Capability Baseline

Description: Phase 1 establishes exactly which LDBC specifications, generators,
drivers, operation catalogs, and scoring rules TripleStore will implement. It also
measures the gap between those requirements and current storage, SPARQL, update,
transaction, and reasoning behavior. By the end of the phase, later work should
have immutable upstream pins and an accepted architecture rather than relying on
benchmark names or moving default branches.

---

## Section 1.1: Upstream Authority and Version Pins

Description: This section creates the source-of-truth manifest for all upstream
software, specifications, datasets, licenses, and generated assets used by the
three benchmark families.

### Task 1.1.1: Pin benchmark specifications and implementations

Description: Select immutable versions for SPB, SNB BI, and SNB Interactive so
query catalogs, parameters, data formats, and scoring formulas cannot drift between
runs.

- [x] 1.1.1.1 Pin the SPB v2.0 specification and a specific commit or release of
  `ldbc_spb_bm_2.0`.
- [x] 1.1.1.2 Pin a stable SNB specification release rather than the latest
  `SNAPSHOT` document.
- [x] 1.1.1.3 Pin compatible releases or commits of SNB Datagen, BI parameter
  generation, BI scoring, and the selected BI reference implementations.
- [x] 1.1.1.4 Pin the latest audited-stable SNB Interactive driver and reference
  implementation versions.
- [x] 1.1.1.5 Record the newer Interactive/deep-delete driver as a separate profile
  with its own version and readiness status.
- [x] 1.1.1.6 Record source URLs, commit SHAs, release tags, checksums, licenses,
  notices, required runtimes, and supported platforms in a machine-readable manifest.

### Task 1.1.2: Define source update and provenance policy

Description: Establish how upstream benchmark updates enter the repository and
how transformations remain traceable to their original artifacts.

- [x] 1.1.2.1 Define a review process for updating any pinned specification,
  generator, driver, parameter set, or reference implementation.
- [x] 1.1.2.2 Require regenerated catalogs and fixtures to record their source pin
  and transformation tool version.
- [x] 1.1.2.3 Define which small upstream-derived assets may be committed and which
  must be downloaded or generated outside Git.
- [x] 1.1.2.4 Add license and notice validation for committed benchmark assets.
- [x] 1.1.2.5 Define offline behavior for clean-checkout smoke tests and explicit
  errors for missing large external assets.

## Section 1.2: Profiles, Claims, and Operation Catalogs

Description: This section converts the pinned source material into explicit
implementation profiles and prevents partial or diagnostic runs from being
reported as complete LDBC results.

### Task 1.2.1: Define benchmark profiles and claim levels

Description: Define named profiles for smoke, development, comparable, and audit
preparation runs, including the language permitted in reports for each profile.

- [x] 1.2.1.1 Define `smoke` profiles that use reduced fixtures and representative
  operations without emitting official score names.
- [x] 1.2.1.2 Define `development` profiles that may run selected operation families
  or shortened durations and are explicitly non-comparable.
- [x] 1.2.1.3 Define `comparable` profiles that preserve the selected specification's
  complete operation mix, parameters, scheduling, validation, and scoring rules.
- [x] 1.2.1.4 Define `audit_preparation` profiles that also capture disclosure,
  pricing, configuration, and provenance evidence required for external review.
- [x] 1.2.1.5 Add report validation that rejects `official`, `certified`, and
  `audited` labels unless explicit audit metadata is present.

### Task 1.2.2: Build canonical operation catalogs

Description: Extract every benchmark operation and its semantic contract into a
versioned local catalog without yet translating it into TripleStore execution code.

- [x] 1.2.2.1 Catalog SPB aggregation operations, editorial CRUD operations,
  inference checks, validation actions, and resilience actions.
- [x] 1.2.2.2 Catalog all SNB BI reads, variants, parameter types, limits, ordering,
  result schemas, choke points, and update-batch operations.
- [x] 1.2.2.3 Catalog all selected SNB Interactive complex reads, short reads,
  update operations, frequencies, dependencies, result schemas, and ordering rules.
- [x] 1.2.2.4 Record optional, mandatory, version-specific, and audit-only operations
  explicitly instead of omitting them.
- [x] 1.2.2.5 Give every catalog item a stable local ID that retains its canonical
  upstream identifier and profile version.

## Section 1.3: TripleStore Capability and Architecture Decision

Description: This section executes a feature-by-feature gap analysis and chooses
where benchmark translation, driver bridging, and any required engine semantics
will live.

### Task 1.3.1: Audit query and result semantics

Description: Parse and classify the canonical read operations against the current
TripleStore query stack, distinguishing parser support from correct end-to-end
execution.

- [x] 1.3.1.1 Map SPB and SNB operations to SPARQL features including aggregates,
  subqueries, expressions, OPTIONAL, UNION, MINUS, ordering, slicing, and property paths.
- [x] 1.3.1.2 Identify BI operations that require weighted shortest paths or other
  semantics not expressible in standard SPARQL 1.1.
- [x] 1.3.1.3 Verify datatype, collation, date arithmetic, integer-width, null or
  unbound, duplicate, and tie-breaking behavior against benchmark result schemas.
- [x] 1.3.1.4 Identify operations whose correct execution requires materialization
  or algorithms outside the existing streaming query path.
- [x] 1.3.1.5 Produce a machine-readable capability matrix with `supported`,
  `requires_fix`, `requires_extension`, and `profile_exclusion` states.

### Task 1.3.2: Audit update, isolation, and reasoning semantics

Description: Compare benchmark concurrency and inference requirements with the
current embedded store lifecycle and documented transaction boundaries.

- [x] 1.3.2.1 Map SPB editorial operations and SNB update operations to current
  public and expert update APIs.
- [x] 1.3.2.2 Identify where direct queries bypass the store transaction coordinator
  and whether benchmark reads can observe partially ordered workload state.
- [x] 1.3.2.3 Determine the isolation and atomicity required for Interactive driver
  concurrency and BI microbatches.
- [x] 1.3.2.4 Map the SPB RDFS/OWL rules to implemented reasoning profiles and record
  gaps in persisted, query-visible, and incrementally maintained derived facts.
- [x] 1.3.2.5 Classify SPB context, text, geospatial, backup, replication, and
  failover requirements as mandatory, optional, or unsupported for each profile.

### Task 1.3.3: Approve the benchmark adapter architecture

Description: Define a benchmark-only boundary that lets upstream Java drivers
control an embedded Elixir store without turning TripleStore into a network service.

- [x] 1.3.3.1 Compare a framed local socket bridge, port protocol, and driver-port
  implementation for upstream Java integration.
- [x] 1.3.3.2 Keep the selected bridge under benchmark tooling and outside the
  `TripleStore` public API and default OTP supervision tree.
- [x] 1.3.3.3 Define request IDs, operation IDs, typed parameters, typed results,
  error frames, cancellation, timeout, and graceful shutdown behavior.
- [x] 1.3.3.4 Define how one benchmark store, dictionary manager, transaction
  coordinator, caches, and statistics services are owned and released.
- [x] 1.3.3.5 Write an ADR for RDF mapping ownership, benchmark-only query extensions,
  and the rule that application-side post-processing may not hide missing engine work.

## Section 1.4: Integration Tests

Description: This final section proves that the pinned sources, profiles, catalogs,
and capability decisions form a coherent foundation before data or runner code is built.

### Task 1.4.1: Validate manifests and catalogs end to end

Description: Exercise manifest loading and catalog generation using checked-in
metadata and small license-compatible fixtures.

- [ ] 1.4.1.1 Verify every source entry has an immutable version, checksum, license,
  and owning benchmark profile.
- [ ] 1.4.1.2 Verify every canonical operation in the pinned specifications appears
  exactly once in a local operation catalog.
- [ ] 1.4.1.3 Verify catalog regeneration is deterministic for a fixed source pin.
- [ ] 1.4.1.4 Verify moving branches, unpinned URLs, missing notices, and unknown
  operation versions fail validation.

### Task 1.4.2: Pass the Phase 1 capability gate

Description: Confirm that every discovered gap has an implementation owner and
that no later phase relies on an undocumented assumption.

- [ ] 1.4.2.1 Parse representative operations from all three suites through the
  real parser and record end-to-end support separately from parse support.
- [ ] 1.4.2.2 Exercise representative update and reasoning paths through the real
  store lifecycle and record their observed isolation and visibility.
- [ ] 1.4.2.3 Verify the capability matrix has no unclassified canonical operation.
- [ ] 1.4.2.4 Review and accept the adapter and RDF-mapping ADR before Phase 2 begins.
- [ ] 1.4.2.5 Record exact commands, toolchain versions, test counts, exclusions,
  and unresolved blocking capabilities in the phase pull request.
