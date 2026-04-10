# Phase 4: Correctness, Comparison, and Automation

Description: Phase 4 turns the benchmark suite into a durable engineering tool by adding answer validation, divergence analysis, CI workflows, and baseline management. By the end of this phase, TripleStore should be able to use the benchmark for both performance tracking and correctness-oriented regression detection.

---

## 4.1 Answer Validation and Divergence Analysis

Description: This section adds correctness-aware benchmark analysis so speed measurements can be interpreted alongside result quality and standards behavior.

### 4.1.1 Answer Normalization

Description: This task defines how benchmark answers are normalized so cross-run and cross-engine comparisons are stable and interpretable.

- [ ] 4.1.1.1 Define canonical serialization for bindings, literals, IRIs, and ordering-sensitive result sets
- [ ] 4.1.1.2 Define normalization rules for count-only and distinct-only variants
- [ ] 4.1.1.3 Define handling for blank-node-related comparisons on benchmark tiers where they appear
- [ ] 4.1.1.4 Define allowable non-deterministic variations versus true divergences
- [ ] 4.1.1.5 Add stable hashing for normalized benchmark answers

### 4.1.2 Divergence Classification

Description: This task turns mismatched answers into actionable benchmark signals by classifying them into meaningful categories.

- [ ] 4.1.2.1 Classify divergences by likely cause: parser, optimizer, paths, duplicates, datatype handling, and LIMIT or DISTINCT semantics
- [ ] 4.1.2.2 Add support for reference-answer baselines on smaller dataset tiers
- [ ] 4.1.2.3 Produce per-query correctness summaries alongside timing summaries
- [ ] 4.1.2.4 Record divergence exemplars so failures are diagnosable without rerunning the whole suite
- [ ] 4.1.2.5 Add a benchmark-facing API for marking accepted divergences when justified

## 4.2 Developer Workflow and Benchmark Operations

Description: This section integrates the benchmark suite into normal developer workflows, CI, and scheduled performance checks.

### 4.2.1 Benchmark Commands and CI Tiers

Description: This task exposes the benchmark suite through standard repository commands and automation-friendly run modes.

- [ ] 4.2.1.1 Add mix tasks or scripts for `smoke`, `medium`, and `full` benchmark runs
- [ ] 4.2.1.2 Add CI jobs for parser-only validation, query-corpus smoke runs, and benchmark smoke execution
- [ ] 4.2.1.3 Add scheduled jobs for larger workstation or server benchmark runs
- [ ] 4.2.1.4 Add threshold gates for runtime regressions, error rate regressions, and divergence regressions
- [ ] 4.2.1.5 Define failure messages that point directly to the offending suite and query ID

### 4.2.2 Documentation and Baseline Management

Description: This task documents benchmark usage and creates the baseline-management workflow needed for long-lived regression tracking.

- [ ] 4.2.2.1 Add a benchmark guide covering datasets, workloads, commands, and report interpretation
- [ ] 4.2.2.2 Add initial accepted baseline reports for TripleStore benchmark runs
- [ ] 4.2.2.3 Define a process for updating baselines after accepted engine changes
- [ ] 4.2.2.4 Define an update cadence for new Wikidata dumps and regenerated query corpora
- [ ] 4.2.2.5 Document which benchmark artifacts belong in version control versus generated output directories

## 4.3 Integration Tests

Description: This section validates the correctness-analysis and automation layers end to end so the benchmark can be trusted as an operational regression tool.

### 4.3.1 Correctness Integration

Description: This task validates the answer-normalization and divergence-detection pipeline on controlled benchmark workloads.

- [ ] 4.3.1.1 Verify known result mismatches are classified into the expected divergence buckets
- [ ] 4.3.1.2 Verify canonical normalization yields stable comparisons across repeated runs
- [ ] 4.3.1.3 Verify count-only and distinct-only validation paths work end to end
- [ ] 4.3.1.4 Verify accepted-divergence metadata is honored without masking unrelated failures

### 4.3.2 Automation Integration

Description: This task validates that benchmark commands, CI jobs, and baseline workflows operate safely and produce expected outputs.

- [ ] 4.3.2.1 Verify smoke benchmark tasks run successfully from a clean checkout with one command
- [ ] 4.3.2.2 Verify CI and scheduled jobs publish benchmark artifacts with the correct provenance metadata
- [ ] 4.3.2.3 Verify regression thresholds fail clearly when suite metrics move past configured limits
- [ ] 4.3.2.4 Verify accepted baselines can be compared against fresh runs without corrupting stored benchmark history
