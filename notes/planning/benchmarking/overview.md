# Wikidata Benchmarking Plan

## Overview

This planning directory defines a phased roadmap for building a Wikidata-style benchmark suite for TripleStore. The benchmark is inspired by recent public evaluations of SPARQL engines on Wikidata workloads and adapts that pattern to the needs of this repository.

The plan focuses on four benchmark workload families:

- `WGPB` for simple graph pattern queries
- `WDBench` for query-log-derived workload fragments
- `WDQS` for end-user query workloads
- `Scholia` for template-driven scholarly queries

The benchmark suite is intended to measure more than raw query speed. It should also make dataset provenance, execution stability, answer-set divergence, and report generation first-class outputs so we can use it as a long-term regression harness.

## Goals

- Build a reproducible Wikidata benchmark workflow for TripleStore
- Support small local runs and larger scheduled runs from the same corpus
- Measure load throughput, query latency, completion rate, and correctness
- Produce durable JSON, CSV, and Markdown artifacts for regression tracking
- Align with the repository's existing planning and benchmark conventions

## Phase Overview

| Phase | Focus | Key Deliverables |
|-------|-------|------------------|
| 1 | Benchmark Foundation | Dataset manifests, fixture tiers, load helpers |
| 2 | Query Corpus Construction | WGPB, WDBench, WDQS, Scholia workload packaging |
| 3 | Runner and Metrics | Execution harness, timing model, report artifacts |
| 4 | Correctness, Comparison, and Automation | Divergence analysis, CI workflows, baseline management |

## Planning Principles

- Prefer reproducible benchmark inputs over ad hoc scripts
- Separate dataset preparation, query corpus construction, and execution logic
- Treat correctness and standards behavior as benchmark outputs, not side notes
- Keep benchmark tiers explicit so CI and local development remain practical
- End every phase with integration tests that exercise the delivered workflow end to end

## Phase Documents

- [Phase 1: Benchmark Foundation](./phase-01-benchmark-foundation.md)
- [Phase 2: Query Corpus Construction](./phase-02-query-corpus-construction.md)
- [Phase 3: Runner and Metrics](./phase-03-runner-and-metrics.md)
- [Phase 4: Correctness, Comparison, and Automation](./phase-04-correctness-comparison-and-automation.md)
