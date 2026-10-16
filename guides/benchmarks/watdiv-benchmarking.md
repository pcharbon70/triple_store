# WatDiv Benchmarking

TripleStore's valid WatDiv workflow uses datasets and instantiated queries from
the official WatDiv v0.6 generator. It verifies the downloaded source archive,
records dataset provenance, compares every TripleStore result with an
independently generated Apache Jena ARQ answer, and only records timings after
all queries pass that correctness gate.

The modules `TripleStore.Benchmark.WatDiv` and
`TripleStore.Benchmark.WatDivQueries` remain small Elixir fixtures for local
diagnostics and tests. They do not reproduce the official generator's data
distributions or parameter selection and must not be used for cross-engine
WatDiv comparisons.

## Prerequisites

Artifact preparation requires:

- `g++`, `make`, `wget`, `tar`, `md5sum`, and `sha256sum`
- Boost development headers and libraries
- a word list at `/usr/share/dict/words`
- Apache Jena's `arq` command for independent reference answers
- the normal TripleStore Elixir, Erlang, Rust, RocksDB, and build dependencies

Set `BOOST_HOME` if Boost is installed under a prefix other than `/usr`.
Generated datasets, workloads, references, stores, and reports live below
`tmp/` by default and are not source artifacts.

## 1. Prepare official artifacts

From the repository root, run:

```sh
./scripts/prepare_watdiv_benchmark.sh \
  --workspace tmp/watdiv_official \
  --scale 1 \
  --queries-per-template 1 \
  --recurrence 1
```

The script downloads the official `watdiv_v06.tar` archive and requires its MD5
digest to equal `9eac247dfdec044d7fa0141ea3ad361f` before extraction. It compiles
the upstream tool, generates the dataset from `wsdbm-data-model.txt`, and asks
the upstream query generator to instantiate the Basic Testing, Incremental
Linear, and Mixed Linear templates. Concatenated generator output is split into
one query per `.rq` file without rewriting its SPARQL.

The generated `dataset-manifest.json` binds the dataset to:

- WatDiv version and official archive URL and digest
- generator identity and scale factor
- pinned SHA-256 of the upstream data model
- SHA-256 and statement count of the generated N-Triples file

The runner re-hashes and recounts the local dataset before loading it. A copied,
truncated, or replaced file therefore fails before timing begins.

## 2. Generate independent reference answers

Use Apache Jena ARQ against that exact dataset and workload:

```sh
./scripts/generate_watdiv_reference_answers.sh \
  --dataset tmp/watdiv_official/watdiv-sf1.nt \
  --dataset-manifest tmp/watdiv_official/dataset-manifest.json \
  --workload-dir tmp/watdiv_official/workload \
  --output-dir tmp/watdiv_official/reference
```

Each answer is stored as W3C SPARQL Results JSON. The reference manifest records
the ARQ version and binds the answer set to the exact dataset and a stable
SHA-256 fingerprint of every query ID and query body. Adding, removing, or
editing a query invalidates the reference set.

Reference answers preserve duplicate solution rows. Row order is ignored,
which matches SPARQL result semantics for queries without `ORDER BY`.

## 3. Run the benchmark

```sh
./scripts/run_watdiv_benchmark.sh \
  --dataset tmp/watdiv_official/watdiv-sf1.nt \
  --dataset-manifest tmp/watdiv_official/dataset-manifest.json \
  --workload-dir tmp/watdiv_official/workload \
  --reference-dir tmp/watdiv_official/reference \
  --output-dir tmp/watdiv_official/results \
  --warmup 2 \
  --iterations 5 \
  --timeout-ms 30000
```

The runner creates a fresh triple-schema store and streams the N-Triples input
in bounded batches. It then:

1. verifies every query result against the independent reference answer;
2. repeats the comparison during every warmup iteration;
3. repeats the comparison during every measured iteration; and
4. writes `results.json` and `results.csv` only after all checks pass.

A mismatch, timeout, query error, provenance failure, or invalid reference file
aborts the run without publishing result artifacts. The temporary database is
removed unless `--keep-store` is supplied. The output directory must not already
exist, which prevents a failed or repeated run from being confused with earlier
results. A successful run promotes both result files together. Use
`--store-path` to choose the database location and `--batch-size` to tune load
batches.

The equivalent Mix entry point is `mix benchmark.watdiv`. Run
`mix help benchmark.watdiv` for the `dataset-manifest`, `split`,
`reference-manifest`, and `run` commands.

## Comparing results

The correctness gate makes a successful run suitable for engine-performance
comparison over the exact recorded dataset and workload. A defensible report
must also keep the scale factor, query count and recurrence, TripleStore commit,
toolchain, hardware, operating system, storage medium, warmup count, measured
iterations, timeout, and cache state fixed or disclose their differences.
The JSON result records the available Git revision and dirty state, application
and language versions, runtime architecture, scheduler count, and run options;
record the remaining hardware and storage details alongside the artifact.

This repository does not claim that a local run is an audited or certified
benchmark result. Official source provenance and matching answers establish
what was executed; comparable experimental controls still depend on the person
running and reporting the benchmark.
