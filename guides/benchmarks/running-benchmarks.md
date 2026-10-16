# Running Benchmarks

Each currently executable benchmark family has a repository-owned script. Run
the scripts from any directory; they resolve the repository root before invoking
Mix.

| Benchmark | Command | Current boundary |
| --- | --- | --- |
| WatDiv | `./scripts/prepare_watdiv_benchmark.sh`, then `./scripts/generate_watdiv_reference_answers.sh` and `./scripts/run_watdiv_benchmark.sh` | Official v0.6 artifacts with independent answer validation |
| gMark | `./scripts/run_gmark_benchmark.sh --scale 1` | Simplified Elixir Bib scenario |
| Wikidata | `./scripts/run_wikidata_benchmark.sh smoke` | Reproducible parser, corpus, smoke, medium, and full modes |
| LDBC SPB | `./scripts/run_ldbc_spb_benchmark.sh` | Deterministic diagnostic smoke workflow |
| LDBC SNB BI | `./scripts/run_ldbc_snb_bi_benchmark.sh` | Reduced diagnostic qualification with 31 accepted-answer comparisons |

Use `--help` on any script for its accepted arguments. The
[WatDiv guide](watdiv-benchmarking.md) covers official artifact preparation,
Apache Jena ARQ reference answers, provenance checks, and the rule that no
timing artifact is written after a correctness mismatch. The older Elixir
WatDiv-shaped generator and query modules remain diagnostic fixtures and are not
the inputs to this script. gMark creates and removes a temporary store and
writes JSON and CSV results under `tmp/gmark_benchmark_runs/` unless
`--output-dir` is given.
Its default run covers ten bounded query templates. The Q3 transitive-closure
stress query is available through `--include-q3`; it can exceed the query timeout
at scale 1 and is therefore not silently included in the bounded default run.
Wikidata forwards its mode and options to `mix benchmark.wikidata`.

The SPB and SNB BI scripts execute their complete deterministic integration
workflows. Those workflows verify correctness, state transitions, and artifact
gates in addition to collecting diagnostic measurements. Their reduced fixtures
do not produce official, certified, audited, or comparable LDBC results.

SNB Interactive has an operation catalog and shared data infrastructure, but its
Phase 6 runtime is not complete. It deliberately has no run script yet; adding
one before the reads, updates, driver schedule, and correctness oracle exist
would misrepresent catalog coverage as an executable benchmark.
