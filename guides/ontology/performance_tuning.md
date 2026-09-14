# Ontology Performance Tuning

Tune TripleStore from measurements taken on the target workload. RocksDB,
dictionary allocation, RDF parsing, join planning, result materialization, and
reasoning can each be the limiting stage.

## Establish a baseline

Record dataset provenance, schema, commit, toolchain, hardware, cache state,
query mix, and correctness results. Run the
[repository benchmarks](../benchmarks/performance-targets.md) and keep generated
numbers with the environment that produced them.

## RocksDB configuration

`TripleStore.Config.RocksDB` produces and validates configuration maps:

~~~elixir
alias TripleStore.Config.RocksDB

config = RocksDB.preset(:production_low_memory)
:ok = RocksDB.validate(config)
summary = RocksDB.format_summary(config)
estimated_bytes = RocksDB.estimate_memory_usage(config)
~~~

Available presets can be read with `preset_names/0`. `recommended/1` and
`for_memory_budget/2` calculate settings from workload or memory inputs.
These maps are planning helpers; `TripleStore.open/2` does not accept the
configuration map as a plug-in replacement for all adapter options. Verify the
actual adapter option keys at the call site.

`ErlangAdapter.open_for_bulk_load/2` and loader batch-size controls can improve
initial ingest. Bulk-load settings trade durability and background work for
throughput. Close and reopen with normal settings after the initial load. There
is no public `Backend.RocksDB.compact/1` API in this release.

## Query tuning

Collect or refresh statistics through `TripleStore.Statistics` after large data
changes when plans depend on representative distributions. The application
supervises `SPARQL.PlanCache`. The result cache in `TripleStore.Query.Cache` is
optional and must be started explicitly; enable it per query with the matching
cache name.

Use eager query `:timeout` for request isolation. Lazy stream consumption needs
a caller-controlled deadline. Leapfrog execution applies only to supported
algebra and pattern shapes, and the v0.1.0 quad path has documented limitations;
measure and verify answers before relying on it.

## Reasoning tuning

Choose an RDFS or OWL 2 RL profile that supplies the required semantics.
`ReasoningConfig` exposes materialized, query-time, hybrid, and disabled modes,
along with local/global/hybrid graph scope and derived-storage strategies.
These configuration structures do not cause direct loads to infer facts
automatically.

For maintained inference, use the explicit incremental and derived-store paths.
Track iteration count, derived count, lookup time, and provenance cost. Global
quad reasoning can increase both fact volume and cross-graph visibility, so
review authorization and graph scope together.

## Operations

`TripleStore.Telemetry` exposes the current event catalog. `Metrics` and
`Prometheus` are opt-in processes. `Health.health/2` can include statistics,
index sizes, memory estimates, and compaction status. Sampling and graph-wide
health scans also consume resources; set intervals from observed overhead.
