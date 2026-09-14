# Performance Targets

`TripleStore.Benchmark.Targets` defines goals for the repository's synthetic
benchmarks:

| Target | Metric | Threshold |
| --- | --- | --- |
| Simple WatDiv query | p95 latency | less than 10 ms |
| Complex WatDiv query | p95 latency | less than 100 ms |
| Bulk load | throughput | greater than 100,000 triples/second |
| WatDiv query mix | p95 latency | less than 50 ms |

These values are acceptance targets. They are not measurements of release
`v0.1.0` or guarantees for another machine, dataset, schema, cache state, or
query mix.

## Running benchmarks

The standalone WatDiv script generates scale 1 data, loads it into a temporary
store, and measures every query in `TripleStore.Benchmark.WatDivQueries`:

~~~sh
mix run scripts/run_benchmarks.exs
~~~

Relevant excluded test suites can be selected explicitly:

~~~sh
mix test --include benchmark test/triple_store/benchmark/phase_5_benchmark_test.exs
mix test --include benchmark test/triple_store/benchmark/phase_8_1_quad_performance_test.exs
mix test --include benchmark test/triple_store/integration/quad_benchmark_test.exs
mix test --include benchmark test/triple_store/reasoner/reasoning_benchmark_test.exs
~~~

`test/test_helper.exs` excludes `:benchmark`, `:large_dataset`, `:slow`, and
`:lifetime_safety` by default. A plain `mix test` therefore does not run all
performance or lifetime coverage.

For reproducible corpus benchmarking, use the
[Wikidata workflow](wikidata-benchmarking.md).

## Programmatic validation

~~~elixir
alias TripleStore.Benchmark.Targets

:pass = Targets.check_simple_query(p95_us: 5_000)
:pass = Targets.check_complex_query(p95_us: 50_000)
:pass = Targets.check_bulk_load(triples_per_sec: 125_000)
:pass = Targets.check_query_mix(p95_us: 25_000)

{:ok, report} = Targets.validate_bulk_load(125_000, 1_000)
report_text = Targets.format_report(report)
~~~

Latency inputs are microseconds. `validate_bulk_load/2` accepts a triple count
and elapsed milliseconds. Threshold comparisons are strict: exactly 10 ms does
not pass the “less than 10 ms” target, and exactly 100,000 triples/second does
not pass the “greater than” target.

## Reporting measurements

Record the commit, toolchain, CPU, memory, storage, schema, dataset provenance,
warmup count, measurement count, timeout, and cache state. Report p50, p95, p99,
failures, and answer correctness alongside throughput. Avoid copying historical
numbers into a guide as if they describe the current checkout.
