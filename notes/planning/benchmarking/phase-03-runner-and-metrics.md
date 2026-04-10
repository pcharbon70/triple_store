# Phase 3: Runner and Metrics

Description: Phase 3 implements the benchmark execution harness, timing model, and artifact pipeline. By the end of this phase, TripleStore should be able to run individual workloads or full benchmark matrices, capture stable performance metrics, and emit durable report artifacts for regression tracking.

---

## 3.1 Benchmark Runner

Description: This section creates the runtime harness that executes benchmark suites with consistent warmup, measurement, timeout, and teardown behavior.

### 3.1.1 Execution Orchestration

Description: This task defines the control flow for suite execution so every benchmark run follows the same lifecycle regardless of workload family or dataset tier.

- [x] 3.1.1.1 Implement suite runner with configurable warmup, iterations, and timeout budgets
- [x] 3.1.1.2 Support single-query, suite-level, and full-matrix benchmark runs
- [x] 3.1.1.3 Support execution variants for raw, count-only, and distinct-only queries
- [x] 3.1.1.4 Add per-tier defaults for timeout, warmup, and iteration counts
- [x] 3.1.1.5 Add run-level provenance capture for dataset manifest, runtime config, and git SHA

### 3.1.2 Error and Resource Capture

Description: This task ensures benchmark failures and runtime instability are captured as structured outputs rather than ad hoc logs.

- [x] 3.1.2.1 Capture parser, execution, timeout, cancellation, and out-of-memory errors separately
- [x] 3.1.2.2 Capture result counts, elapsed time, and any available peak-memory metrics
- [x] 3.1.2.3 Implement adjusted timing penalties for failures and unbounded long-running queries
- [x] 3.1.2.4 Classify partial failures so suite summaries can distinguish flaky runs from hard incompatibilities
- [x] 3.1.2.5 Preserve raw query text and normalized query text for failed executions

## 3.2 Metrics and Result Artifacts

Description: This section computes the summary statistics and output artifacts needed for analysis, documentation, and regression comparison.

### 3.2.1 Statistics Computation

Description: This task computes the timing and stability metrics used by the benchmark suite at both query and suite levels.

- [x] 3.2.1.1 Compute min, max, median, quartiles, mean, and adjusted mean for each query
- [x] 3.2.1.2 Compute per-suite totals for errors, timeouts, completion rate, and divergence placeholders
- [x] 3.2.1.3 Compute per-tier and per-suite throughput summaries where applicable
- [x] 3.2.1.4 Compute aggregate views grouped by workload family and query-shape category
- [x] 3.2.1.5 Define stable report schemas so later phases can compare results across runs

### 3.2.2 Report Generation

Description: This task emits the benchmark outputs in formats suitable for local inspection, automated analysis, and checked-in documentation.

- [x] 3.2.2.1 Generate JSON outputs for machine consumption and baseline comparison
- [x] 3.2.2.2 Generate CSV outputs for spreadsheet and notebook analysis
- [x] 3.2.2.3 Generate Markdown summaries aligned with existing benchmark docs in this repository
- [x] 3.2.2.4 Include report sections for dataset provenance, runtime configuration, and hardware metadata
- [x] 3.2.2.5 Add report versioning so regenerated benchmark outputs can coexist with prior baselines

## 3.3 Integration Tests

Description: This section verifies that the runner and artifact pipeline work together on real benchmark fixtures and produce reproducible outputs.

### 3.3.1 Runner Integration

Description: This task validates the full benchmark execution lifecycle from warmup through measured runs and teardown.

- [ ] 3.3.1.1 Verify warmup runs do not pollute measured timing output
- [ ] 3.3.1.2 Verify timeout and error cases produce adjusted timings and structured errors
- [ ] 3.3.1.3 Verify single-query, suite-level, and multi-suite runs all complete through the same runner interface
- [ ] 3.3.1.4 Verify repeated runner execution leaves benchmark stores and temp artifacts in a clean state

### 3.3.2 Artifact Integration

Description: This task validates that every benchmark run emits complete and internally consistent result artifacts.

- [ ] 3.3.2.1 Verify JSON, CSV, and Markdown outputs are generated together for benchmark runs
- [ ] 3.3.2.2 Verify report metadata matches the executed dataset tier and runtime configuration
- [ ] 3.3.2.3 Verify aggregate statistics match the underlying per-query results
- [ ] 3.3.2.4 Verify reruns append or version artifacts without clobbering accepted baselines unintentionally
