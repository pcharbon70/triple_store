# Wikidata Benchmarking

This guide describes the repository-supported Wikidata benchmark workflow for TripleStore. The benchmark suite is designed to be reproducible, correctness-aware, and usable from both a clean checkout and larger externally provisioned datasets.

## Datasets

The benchmark supports four dataset tiers:

| Tier | Purpose | Data Source |
| --- | --- | --- |
| `smoke` | CI, quick local validation, task-level regression checks | Built-in fixture under `priv/benchmarks/wikidata/fixtures/smoke.nt` |
| `medium` | Workstation benchmarking and pre-merge validation | User-provided source file |
| `full` | Publication-grade or server benchmarking | User-provided source file |

The built-in smoke fixture is intentionally small and checked in with the repository so the smoke benchmark can run from a clean checkout.

## Workloads

The benchmark covers four workload families:

| Suite | Description |
| --- | --- |
| `WGPB` | Simple graph-pattern queries |
| `WDBench` | Query-log-derived workload fragments |
| `WDQS` | User query workload samples |
| `Scholia` | Template-driven scholarly queries |

The smoke benchmark uses one representative query from each suite so CI remains fast and stable. Medium and full runs default to the full corpus for each selected suite.

## Commands

Use the repository task `mix benchmark.wikidata`:

```bash
mix benchmark.wikidata parser
mix benchmark.wikidata corpus-smoke
mix benchmark.wikidata smoke
mix benchmark.wikidata medium --source /path/to/wikidata-medium.nt --source-url https://example.org/wikidata-medium.nt
mix benchmark.wikidata full --source /path/to/wikidata-full.nt --source-url https://example.org/wikidata-full.nt
```

Useful options:

```bash
--fixture-root PATH
--output-root PATH
--report-id ID
--answer-baseline PATH
--accepted-divergences PATH
--max-adjusted-p95-us N
--max-failure-rate FLOAT
--max-divergence-rate FLOAT
--write-answer-baseline PATH
--write-accepted-report DIR
```

Examples:

```bash
mix benchmark.wikidata smoke \
  --report-id wikidata-smoke-local \
  --max-adjusted-p95-us 500000 \
  --max-failure-rate 0.0

mix benchmark.wikidata smoke \
  --write-answer-baseline priv/benchmarks/wikidata/baselines/smoke/reference_answers.json \
  --write-accepted-report priv/benchmarks/wikidata/baselines/smoke/accepted_report
```

## Report Artifacts

Benchmark runs emit:

| Artifact | Purpose |
| --- | --- |
| `summary.json` | Machine-readable run report |
| `query_summaries.csv` | Query-by-query spreadsheet export |
| `summary.md` | Human-readable benchmark summary |

Correctness-aware runs also use:

| File | Purpose |
| --- | --- |
| `reference_answers.json` | Accepted answer baseline for a benchmark tier |
| `accepted_divergences.json` | Explicitly accepted divergences |

## Report Interpretation

The JSON, CSV, and Markdown reports include:

- dataset provenance and runtime configuration
- query-level latency summaries
- failure counts and completion rates
- divergence status and divergence classification
- accepted-divergence counts at aggregate levels

The main correctness fields are:

| Field | Meaning |
| --- | --- |
| `divergence_status` | `match`, `divergent`, `accepted_divergence`, `missing_reference`, or `not_comparable` |
| `divergence_classification` | Likely cause such as `paths`, `duplicates`, `datatype_handling`, or `optimizer` |
| `answer_fingerprint` | Stable hash of the normalized answer |
| `reference_fingerprint` | Stable hash from the accepted baseline |

## CI and Scheduled Runs

The repository wires the benchmark into two automation paths:

- `CI`: parser validation, query-corpus smoke execution, and smoke benchmark execution
- scheduled workflow: medium and full benchmark runs intended for a benchmark-capable runner

The smoke job uploads the generated report artifacts so regressions can be inspected without rerunning locally.
The scheduled workflow reads its dataset configuration from workflow-dispatch inputs or the repository variables `WIKIDATA_BENCHMARK_SOURCE_PATH`, `WIKIDATA_BENCHMARK_SOURCE_URL`, and optionally `WIKIDATA_BENCHMARK_DUMP_VERSION`.

## Baseline Workflow

Use this process when updating accepted smoke baselines:

1. Run the smoke benchmark against `main` and review the report artifacts.
2. If the behavior change is accepted, rewrite `reference_answers.json` with `--write-answer-baseline`.
3. If a divergence is intentional and should remain accepted, update `accepted_divergences.json`.
4. Copy the accepted JSON, CSV, and Markdown report artifacts into `priv/benchmarks/wikidata/baselines/smoke/accepted_report`.
5. Verify the checked-in baseline artifacts remain portable by keeping fixture-local paths out of the accepted JSON artifacts.
6. Include the baseline update and the accepted report artifacts in the same review.

## Baseline Update Cadence

Follow this cadence:

- smoke fixture baselines: update only when the smoke query set or expected behavior changes
- medium and full datasets: update after accepted engine changes that materially alter results or timings
- new Wikidata dump generations: evaluate quarterly or when corpus regeneration is already planned

## Version Control Policy

Keep these artifacts in version control:

- smoke fixture source data
- accepted smoke answer baselines
- accepted divergence metadata
- accepted smoke report artifacts
- planning and guide documents

Do not keep these generated outputs in version control:

- ad hoc local benchmark runs under `tmp/`
- workstation or server benchmark runs for one-off investigation
- regenerated report directories created only for review or experimentation
