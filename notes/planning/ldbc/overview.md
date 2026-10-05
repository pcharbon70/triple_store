# LDBC Benchmark Family Implementation Plan

## Overview

Description: This planning directory defines the phased implementation of three
Graph Data Council/LDBC benchmark families for TripleStore:

- LDBC Semantic Publishing Benchmark (`SPB`)
- LDBC Social Network Benchmark Business Intelligence (`SNB BI`)
- LDBC Social Network Benchmark Interactive (`SNB Interactive`)

The plan targets reproducible, correctness-aware benchmark integrations. It does
not treat a locally runnable workload as an official or audited LDBC result.
Official-result terminology, scoring, execution rules, and disclosure claims may
be used only when the implemented profile follows the pinned specification and
driver exactly and the required audit has occurred.

SPB is an RDF/SPARQL benchmark and maps directly to TripleStore's data model.
SNB BI and Interactive are language-neutral graph workloads with public reference
implementations primarily expressed in Cypher and SQL. Their integration therefore
requires a documented RDF mapping, operation translations, parameter handling,
and cross-validation against a reference implementation.

## Source Authority

Description: The benchmark implementation must pin immutable upstream versions
rather than following moving branches or documentation snapshots.

| Benchmark | Initial authority to pin | Initial implementation profile |
| --- | --- | --- |
| SPB | LDBC SPB v2.0 specification, generator, driver, queries, ontologies, and validation assets | Complete local RDF engine profile, including reasoning and mixed read/write execution; replication/failover claims gated by an explicit capability decision |
| SNB BI | Latest stable SNB specification, Datagen, BI parameter generator, scoring tools, and reference implementations selected during Phase 1 | All canonical BI reads, update batches, validation parameters, power and throughput protocols |
| SNB Interactive | Latest audited stable Interactive specification and driver selected during Phase 1 | Complete audited-stable read/update workload first; newer deep-delete profile added only as a separately versioned extension |

Canonical upstream references:

- [LDBC benchmark catalog](https://ldbcouncil.org/benchmarks/)
- [Semantic Publishing Benchmark](https://ldbcouncil.org/benchmarks/spb/)
- [Social Network Benchmark](https://ldbcouncil.org/benchmarks/snb/)
- [SNB Business Intelligence](https://ldbcouncil.org/benchmarks/snb/bi/)
- [SNB Interactive](https://ldbcouncil.org/benchmarks/snb/interactive/)
- [SNB specification repository](https://github.com/ldbc/ldbc_snb_docs)
- [SPB v2.0 implementation](https://github.com/ldbc/ldbc_spb_bm_2.0)
- [SNB BI reference implementations](https://github.com/ldbc/ldbc_snb_bi)

## Goals

Description: The completed work should provide defensible internal comparisons,
repeatable regression measurements, and a clear path toward externally comparable
results without overstating conformance.

- Reproduce benchmark inputs from pinned upstream artifacts and immutable manifests.
- Preserve canonical operation IDs, parameter distributions, ordering, limits,
  result types, and update ordering.
- Load SPB RDF and mapped SNB datasets without holding full datasets in memory.
- Cross-validate every operation against accepted answers or a reference engine
  before measuring performance.
- Fail benchmark runs on parse, execution, timeout, or answer errors rather than
  including failed operations in latency statistics.
- Run official scheduling and scoring protocols when the TripleStore adapter meets
  their prerequisites; label reduced smoke or diagnostic profiles explicitly.
- Produce JSON, CSV, Markdown, environment, provenance, correctness, and disclosure
  artifacts for every run.
- Keep benchmark adapters and external-driver bridges outside TripleStore's public
  embedded-library API.

## Non-Goals

Description: These boundaries keep the implementation focused and prevent local
benchmark conveniences from being mistaken for engine features or certified results.

- Do not add a general-purpose HTTP service to TripleStore solely for benchmark use.
- Do not copy query text, data, or expected answers without recording its upstream
  version, license, and transformation history.
- Do not call a translated SNB workload conformant until reference cross-validation
  passes for all operations and parameter sets in the selected profile.
- Do not publish `official`, `certified`, or `audited` LDBC results without the
  Graph Data Council process required for those terms.
- Do not use the legacy `TripleStore.Benchmark.Runner` behavior that records failed
  operations as timed measurements.

## Phase Overview

Description: Phase boundaries follow implementation dependencies. The common
contract, data pipeline, and runner precede suite-specific work; final automation
and disclosure depend on all three suites producing correct results.

| Phase | Focus | Depends on | Exit result |
| --- | --- | --- | --- |
| 1 | Authority, scope, and capability baseline | None | Pinned versions, claim levels, operation catalogs, and an accepted gap/architecture decision |
| 2 | Reproducible datasets and load fixtures | Phase 1 | SPB and SNB fixtures can be generated or acquired, mapped, loaded, reopened, and verified |
| 3 | Common driver, correctness, and artifact foundation | Phases 1-2 | One fail-fast execution contract supports native and upstream-driver-controlled runs |
| 4 | Semantic Publishing Benchmark | Phases 1-3 | SPB query, editorial, reasoning, and selected resilience profiles execute correctly |
| 5 | SNB Business Intelligence | Phases 1-3 | BI reads, update batches, scoring inputs, and reference comparisons are complete |
| 6 | SNB Interactive | Phases 1-3 and transaction capability findings from Phase 1 | Interactive reads, updates, scheduling, and validation are complete for the pinned profile |
| 7 | Cross-suite qualification and automation | Phases 4-6 | CI smoke coverage, scheduled runs, baselines, guides, and disclosure artifacts are operational |

## Planning and Delivery Rules

Description: These rules apply to every phase and govern how benchmark behavior,
source changes, and performance claims are reviewed.

- Every phase, section, and task begins with a description of its purpose and exit behavior.
- Every phase ends with an integration-test section that acts as its phase gate.
- Section counts, task counts, and sub-task counts follow the actual work rather
  than a fixed template.
- Treat each section as a reviewable commit boundary and each phase as the natural
  pull-request boundary unless an implementation dependency requires a smaller PR.
- Preserve benchmark source artifacts as immutable external inputs; commit only
  small license-compatible smoke fixtures and manifests.
- Separate source acquisition, transformation, load, validation, warmup, measurement,
  and reporting time.
- Record the exact query text or native operation plan that was executed.
- Keep result caching disabled unless the selected benchmark profile explicitly
  permits it; record plan-cache and storage-cache state in every run.
- Review and update the relevant storage, query, transaction, reasoning, and
  observability contracts when benchmark implementation exposes a semantic gap.
- Preserve the distinction between TripleStore's triple and quad schemas. The
  canonical benchmark profile must state which schema is measured.

## Completion Criteria

Description: The three benchmark integrations are complete only when correctness,
protocol fidelity, reproducibility, and reporting all pass together.

- All selected upstream sources are pinned by version and checksum.
- Every canonical operation in each selected profile is accounted for as implemented,
  unsupported with a blocking issue, or excluded by an upstream-defined optional profile.
- Smoke fixtures run from a clean checkout without network access.
- Larger datasets are reproducibly acquired or generated from manifests.
- TripleStore results match accepted baselines or a designated reference implementation.
- No failed, timed-out, or incorrect operation contributes to latency or throughput scores.
- Official score names are emitted only by protocol-faithful runs.
- Scheduled benchmark artifacts include environment and full-disclosure metadata.
- The benchmark guide explains how local, comparable, and audited runs differ.

## Phase Documents

Description: Each linked phase document contains sections, tasks, sub-tasks, and
an integration-test gate.

- [Phase 1: Benchmark Authority and Capability Baseline](phase-01-authority-and-capability-baseline.md)
- [Phase 2: Dataset Generation, RDF Mapping, and Loading](phase-02-datasets-and-loading.md)
- [Phase 3: Common Driver, Correctness, and Metrics Foundation](phase-03-driver-correctness-and-metrics.md)
- [Phase 4: Semantic Publishing Benchmark](phase-04-semantic-publishing-benchmark.md)
- [Phase 5: SNB Business Intelligence](phase-05-snb-business-intelligence.md)
- [Phase 6: SNB Interactive](phase-06-snb-interactive.md)
- [Phase 7: Cross-Suite Qualification and Automation](phase-07-qualification-and-automation.md)
