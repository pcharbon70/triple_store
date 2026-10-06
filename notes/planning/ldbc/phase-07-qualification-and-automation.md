# Phase 7: Cross-Suite Qualification and Automation

Description: Phase 7 turns the three correctness-qualified implementations into a
maintainable benchmark program. It adds clean-checkout smoke coverage, scheduled
scale runs, regression policy, historical baselines, documentation, and disclosure
artifacts. It also establishes the final terminology gates separating local diagnostics,
comparable runs, audit preparation, and externally audited results.

---

## Section 7.1: Continuous Integration and Scheduled Execution

Description: This section assigns each workload to an execution environment that
matches its cost while keeping core correctness visible on every relevant change.

### Task 7.1.1: Add clean-checkout CI smoke jobs

Description: Run deterministic offline fixtures that cover every mandatory operation
family without downloading large datasets or emitting official scores.

- [ ] 7.1.1.1 Add manifest, catalog, parser, transformation, and license-validation jobs.
- [ ] 7.1.1.2 Run SPB aggregation, editorial, inference, and mixed-workload smoke coverage.
- [ ] 7.1.1.3 Run all BI operation translations against the BI smoke fixture and golden answers.
- [ ] 7.1.1.4 Run all Interactive operation bindings against the stable-profile smoke fixture.
- [ ] 7.1.1.5 Upload correctness and diagnostic artifacts on failure while excluding
  environment-sensitive performance thresholds from hosted CI.

### Task 7.1.2: Add scheduled benchmark-runner workflows

Description: Run larger protocol-faithful profiles on labeled self-hosted hardware
with explicit dataset provisioning and artifact retention.

- [ ] 7.1.2.1 Define runner labels, hardware requirements, storage capacity, dataset
  locations, secrets policy, and cleanup behavior.
- [ ] 7.1.2.2 Add independent schedules for SPB, BI, and Interactive so one long suite
  does not suppress the others.
- [ ] 7.1.2.3 Verify dataset and store manifests before every scheduled run.
- [ ] 7.1.2.4 Upload raw, normalized, correctness, score, environment, resource, and
  disclosure artifacts with stable retention names.
- [ ] 7.1.2.5 Prevent overlapping jobs from sharing mutable stores, caches, or output paths.

## Section 7.2: Baselines and Regression Policy

Description: This section defines how correctness and performance history are accepted,
compared, and invalidated when workloads, hardware, or engine semantics change.

### Task 7.2.1: Manage correctness baselines

Description: Preserve accepted answers and state probes as immutable evidence keyed
by suite, profile, source pins, mapping version, scale, and parameter set.

- [ ] 7.2.1.1 Store small golden answers and hashes for smoke profiles in the repository.
- [ ] 7.2.1.2 Store large validation artifacts outside Git with checksummed manifests.
- [ ] 7.2.1.3 Require an explicit baseline-update command, source diff, and reviewer-visible
  divergence report for every answer change.
- [ ] 7.2.1.4 Invalidate baselines automatically when source pins, mapping versions,
  operation translations, or result codecs change.

### Task 7.2.2: Define performance comparison rules

Description: Compare like-for-like runs and distinguish statistically credible
regressions from environment drift or incomplete protocols.

- [ ] 7.2.2.1 Key performance baselines by hardware identity, software environment,
  suite profile, scale, dataset checksum, and engine configuration.
- [ ] 7.2.2.2 Define minimum samples, accepted variance, percentile comparisons, and
  confidence or repeated-run requirements per profile.
- [ ] 7.2.2.3 Separate load, warmup, power, throughput, update, backup, and restore metrics.
- [ ] 7.2.2.4 Block comparisons when correctness, completeness, environment compatibility,
  or state-reset checks fail.
- [ ] 7.2.2.5 Define alert thresholds without turning synthetic targets into production claims.

## Section 7.3: Documentation and Disclosure

Description: This section documents exact workflows, supported capabilities, known
limits, source provenance, and the evidence required to interpret a result.

### Task 7.3.1: Publish the LDBC benchmark guide

Description: Create one user-facing guide with suite-specific commands and explicit
distinctions among smoke, development, comparable, and audit-preparation runs.

- [ ] 7.3.1.1 Document prerequisites, pinned external tools, dataset acquisition,
  generation, checksums, disk requirements, and cleanup.
- [ ] 7.3.1.2 Document Mix tasks for generate, validate, load, smoke, run, compare,
  score, and baseline management.
- [ ] 7.3.1.3 Document supported profiles, operation coverage, RDF mapping, reasoning,
  transaction, path-extension, and resilience behavior.
- [ ] 7.3.1.4 Document artifact layouts and how to reproduce a reported run.
- [ ] 7.3.1.5 Remove or correct conflicting benchmark claims in README, guides,
  module documentation, and older performance plans.

### Task 7.3.2: Generate disclosure and audit-preparation packages

Description: Assemble the configuration, provenance, results, costs, and limitations
needed for review while avoiding unauthorized claims of certification.

- [ ] 7.3.2.1 Generate a complete configuration and environment disclosure from run artifacts.
- [ ] 7.3.2.2 Include source pins, transformations, operation coverage, deviations,
  parameter files, raw samples, correctness evidence, and score inputs.
- [ ] 7.3.2.3 Capture hardware and software cost inputs only for profiles whose
  specification defines price-adjusted metrics.
- [ ] 7.3.2.4 Validate disclosure completeness against the pinned specification.
- [ ] 7.3.2.5 Label packages `audit preparation` until an external audit has completed.

## Section 7.4: Release and Operational Controls

Description: This section prepares the benchmark tooling for versioned release and
safe repeated operation on development and benchmark-runner machines.

### Task 7.4.1: Version benchmark tooling and schemas

Description: Give adapters, mappings, manifests, catalogs, artifacts, and baselines
independent compatibility versions tied to a TripleStore release.

- [ ] 7.4.1.1 Define compatibility rules among tooling, mapping, catalog, artifact,
  baseline, and TripleStore versions.
- [ ] 7.4.1.2 Reject incompatible combinations before dataset mutation or measurement.
- [ ] 7.4.1.3 Include migration tools only for metadata formats where conversion can
  preserve meaning; require regeneration otherwise.
- [ ] 7.4.1.4 Publish release notes describing benchmark source and behavior changes.

### Task 7.4.2: Add operational safeguards

Description: Prevent large benchmark runs from damaging user stores, exhausting
shared hosts silently, or leaving stale external processes.

- [ ] 7.4.2.1 Require dedicated disposable benchmark paths and refuse known production paths.
- [ ] 7.4.2.2 Preflight free disk, memory, file descriptors, tool versions, dataset
  checksums, and writable output paths.
- [ ] 7.4.2.3 Add interrupt-safe cleanup, resumable acquisition, and explicit preservation
  of failed-run artifacts.
- [ ] 7.4.2.4 Verify external Java, Spark, container, and bridge processes terminate
  after success, failure, timeout, and user cancellation.

## Section 7.5: Integration Tests

Description: This final section validates the entire three-suite program from clean
checkout through scheduled-style execution, comparison, documentation, and release gates.

### Task 7.5.1: Run the clean-checkout benchmark matrix

Description: Exercise every offline smoke path using only committed fixtures,
manifests, pinned tooling metadata, and locally built TripleStore code.

- [ ] 7.5.1.1 Run SPB, BI, and Interactive smoke workflows from fresh output directories.
- [ ] 7.5.1.2 Verify every mandatory operation family executes and matches its baseline.
- [ ] 7.5.1.3 Verify all stores, external processes, snapshots, iterators, ports, and
  temporary paths are released.
- [ ] 7.5.1.4 Verify repeated runs produce identical correctness artifacts and compatible
  provenance while keeping raw timings distinct.

### Task 7.5.2: Run scheduled-style qualification

Description: Exercise representative external datasets and the same orchestration
used by self-hosted scheduled runners without shortening protocol rules silently.

- [ ] 7.5.2.1 Run one externally provisioned scale for each suite on a labeled runner.
- [ ] 7.5.2.2 Verify profile prerequisites, state reset, correctness, complete duration,
  score gating, artifact upload, and cleanup.
- [ ] 7.5.2.3 Compare a candidate run with a compatible baseline and verify regression decisions.
- [ ] 7.5.2.4 Simulate missing data, incompatible pins, insufficient disk, driver crash,
  and artifact-upload failure with actionable tagged outcomes.

### Task 7.5.3: Pass the final quality and governance gate

Description: Confirm implementation quality, documentation accuracy, specification
traceability, and release readiness across the repository.

- [ ] 7.5.3.1 Run strict compilation, formatting, default tests, strict Credo,
  Dialyzer, and documentation/governance validators.
- [ ] 7.5.3.2 Run all benchmark-tagged LDBC tests and record any intentionally excluded
  large, slow, or external-tool profiles.
- [ ] 7.5.3.3 Verify every operation catalog entry links to implementation tests,
  correctness evidence, and a supported profile.
- [ ] 7.5.3.4 Review all user-facing performance claims against generated artifacts
  and remove unsupported comparative language.
- [ ] 7.5.3.5 Record exact commands, counts, environment, source pins, limitations,
  and remaining audit steps in the final phase pull request.
