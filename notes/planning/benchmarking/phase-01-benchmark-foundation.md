# Phase 1: Benchmark Foundation

Description: Phase 1 establishes the benchmark contract, dataset provenance model, and fixture-loading workflow needed for all later benchmark work. By the end of this phase, TripleStore should be able to prepare benchmark datasets reproducibly, load them through a consistent helper layer, and validate that the resulting stores are suitable for running benchmark suites.

---

## 1.1 Benchmark Contract

Description: This section defines the canonical benchmark inputs, execution modes, result artifacts, and dataset tiers so the benchmark suite has a stable contract before implementation expands.

- [x] **Section 1.1 Complete** (2026-04-10)

### 1.1.1 Benchmark Manifest and Workload Matrix

Description: This task defines the machine-readable manifest format that describes benchmark workloads, dataset provenance, execution variants, and expected outputs.

- [x] 1.1.1.1 Define workload families: `wgpb`, `wdbench`, `wdqs`, and `scholia`
- [x] 1.1.1.2 Define execution variants: unmodified query, count-only query, and distinct-only query
- [x] 1.1.1.3 Define benchmark metadata fields: benchmark ID, suite, category, source, and tags
- [x] 1.1.1.4 Define required result artifacts: raw timings, adjusted timings, errors, timeouts, divergences, and metadata
- [x] 1.1.1.5 Define benchmark manifest versioning rules for future corpus changes

### 1.1.2 Dataset Tiers and Success Criteria

Description: This task defines the dataset tiers and initial acceptance criteria so the benchmark remains useful in both local development and larger scheduled runs.

- [x] 1.1.2.1 Define dataset tiers: `smoke`, `medium`, `large`, and `full_dump`
- [x] 1.1.2.2 Define per-tier intended use: CI, local debugging, workstation benchmarking, and long-running validation
- [x] 1.1.2.3 Define reproducibility metadata: dump version, checksum, subset generation seed, and normalization flags
- [x] 1.1.2.4 Define initial benchmark success criteria: load completion, query completion rate, and report generation
- [x] 1.1.2.5 Define hardware and runtime metadata that must be captured with every benchmark run

## 1.2 Wikidata Fixture Pipeline

Description: This section creates the dataset acquisition, subset generation, and load-helper infrastructure required to prepare TripleStore for benchmark execution.

- [x] **Section 1.2 Complete** (2026-04-10)

### 1.2.1 Dump Acquisition and Manifesting

Description: This task creates the reproducible pipeline for acquiring Wikidata RDF sources and turning them into benchmark-ready dataset manifests.

- [x] 1.2.1.1 Implement dump manifest generation with source URL, date, checksum, and triple count
- [x] 1.2.1.2 Implement local fixture registration for prepared benchmark datasets
- [x] 1.2.1.3 Implement subset generation for `smoke` and `medium` tiers from larger RDF sources
- [x] 1.2.1.4 Store subset-generation metadata so benchmark tiers can be regenerated deterministically
- [x] 1.2.1.5 Add validation that fixture manifests match on-disk dataset contents

### 1.2.2 TripleStore Load Fixtures

Description: This task standardizes how benchmark datasets are loaded into TripleStore so later benchmark phases can reuse one fixture pipeline instead of duplicating setup logic.

- [x] 1.2.2.1 Define load presets for benchmark ingestion modes, including truthy-oriented and full-RDF-oriented presets
- [x] 1.2.2.2 Implement benchmark setup helpers for open, load, warmup, compact, and teardown
- [x] 1.2.2.3 Capture load metrics: elapsed time, throughput, warning count, and failure class
- [x] 1.2.2.4 Add fixture helpers for reopening previously loaded benchmark stores without reimporting data
- [x] 1.2.2.5 Document the expected directory layout for benchmark datasets and generated stores

## 1.3 Integration Tests

Description: This section verifies that benchmark datasets, manifests, and load helpers work together end to end and provide stable inputs for later phases.

### 1.3.1 Dataset Pipeline Integration

Description: This task validates that benchmark manifests and subset generation produce deterministic, reusable fixtures.

- [ ] 1.3.1.1 Verify fixture manifests round-trip correctly from source metadata to local fixture registration
- [ ] 1.3.1.2 Verify subset generation is deterministic for a fixed seed and source version
- [ ] 1.3.1.3 Verify recorded triple counts and checksums match generated benchmark fixtures
- [ ] 1.3.1.4 Verify invalid or partial fixture metadata fails with explicit benchmark-pipeline errors

### 1.3.2 Load Pipeline Integration

Description: This task validates that prepared benchmark fixtures can be loaded, reopened, and cleaned up reliably through the benchmark helper layer.

- [ ] 1.3.2.1 Verify each dataset tier loads successfully into a fresh TripleStore instance
- [ ] 1.3.2.2 Verify post-load store stats match the benchmark fixture manifest
- [ ] 1.3.2.3 Verify repeated setup and teardown leave no leaked state across benchmark runs
- [ ] 1.3.2.4 Verify benchmark store reopen logic works without requiring a reload
