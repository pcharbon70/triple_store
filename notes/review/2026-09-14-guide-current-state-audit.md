# Guide Current-State Audit

**Date:** 2026-09-14
**Code baseline:** `dc4555344ae445b7d805a78e13834f45e014f017` (`main`, release `v0.1.0`)
**Scope:** every Markdown file under `guides/` (29 files)

## Method

Each guide was checked against the public facade, the implementation modules it
describes, adjacent tests, the repository scripts, CI workflows, and the
normative specifications named in `AGENTS.md`. Planning notes, old comments,
README claims, and benchmark targets were not treated as proof of current
behavior.

The audit included:

- an inventory of every guide and every Elixir code block;
- syntax parsing of all 52 Elixir blocks;
- resolution of project module calls against the compiled application's exported
  functions and arities;
- focused execution of the shared triple, quad, query, configuration, and
  benchmark examples;
- local-link and required-index validation;
- searches for removed or renamed public APIs and stale architecture terms.

## Results by file

| Guide | Review result and evidence |
| --- | --- |
| `guides/README.md` | Rebuilt as the current guide index; identifies specs as normative and plans/reviews as context. |
| `guides/benchmarks/performance-targets.md` | Replaced historical sample measurements and invalid target calls with the exact `Benchmark.Targets` API, strict thresholds, current test paths, and benchmark-tag behavior. |
| `guides/benchmarks/wikidata-benchmarking.md` | Verified task modes, switches, artifacts, workflows, and baseline paths against `Mix.Tasks.Benchmark.Wikidata`, Wikidata operations, CI, and scheduled workflow; corrected the tier count from four to three. |
| `guides/developer/README.md` | Rebuilt the reading order and current governance commands. |
| `guides/developer/00-architecture-overview.md` | Reconciled facade, OTP, storage, query, update, reasoning, and ownership boundaries with current source and ADR-0001. |
| `guides/developer/01-storage-layer.md` | Replaced removed storage-NIF calls with `ErlangAdapter`; documented current schemas, batches, dictionary IDs, iterators, folds, streams, and snapshots. |
| `guides/developer/02-sparql-engine.md` | Reconciled parser ownership, query options, result shapes, prepared queries, streaming timeout limits, GRAPH authorization, and the three distinct cache modules. |
| `guides/developer/03-reasoning-engine.md` | Reconciled compilation, semi-naive evaluation, in-memory facade behavior, persistent derived data, incremental maintenance, graph scope, and quad tuple order. |
| `guides/developer/04-query-optimization.md` | Removed nonexistent algebra helpers and fixed statistics, plan-cache, result-cache, Leapfrog, and invalidation claims. |
| `guides/developer/05-telemetry-monitoring.md` | Replaced invalid telemetry/health examples; documented the current event API and opt-in metrics/Prometheus lifecycle. |
| `guides/developer/06-quad-store-architecture.md` | Replaced obsolete `QuadIndex` CRUD calls with current `QuadOperations`; reconciled indices, graph IDs, tuple orders, ACLs, dataset I/O, reasoning, and backup. |
| `guides/developer/07-otp-concurrency.md` | Reconciled the actual supervision tree, store lifecycle, coordinator scope, snapshot propagation gap, multi-batch rollback boundary, and lazy-resource ownership. |
| `guides/developer/08-data-flow.md` | Rebuilt query, load, update, reasoning, and backup flows around current modules and explicit isolation limits. |
| `guides/erlang-rocksdb-migration.md` | Converted obsolete migration instructions into an accurate completed-migration note; distinguished the C++ RocksDB NIF dependency from the retained Rust parser NIF. |
| `guides/ontology/getting_started.md` | Replaced stale result, health, backup, and materialization examples with a tested triple-store walkthrough and the current result representation. |
| `guides/ontology/performance_tuning.md` | Removed nonexistent compaction calls and unmeasured claims; documented current configuration maps, bulk-load controls, cache lifecycle, query limits, and explicit reasoning paths. |
| `guides/user/README.md` | Rebuilt the triple/quad guide index and shared entry point. |
| `guides/user/01-getting-started.md` | Reconciled toolchain requirements, handle shape, persisted schemas, executable triple/quad examples, public graph ACL setup, dataset-I/O caveat, and cleanup. |
| `guides/user/triples/02-data-management.md` | Reconciled loader arities, the 10,000-item default batch, direct mutations, export targets, backup, cache invalidation, and coordination limits. |
| `guides/user/triples/03-sparql-queries.md` | Reconciled SELECT/ASK/CONSTRUCT shapes, options, explain output, preparation, streaming, and lazy timeout behavior. |
| `guides/user/triples/04-sparql-updates.md` | Reconciled supported update flow, per-operation commits, dictionary allocation, mixed batches, result-cache invalidation, and optional plan-cache invalidation. |
| `guides/user/triples/05-reasoning.md` | Corrected the default local materialization claim: it computes in memory, returns statistics, and discards the closure. |
| `guides/user/triples/06-configuration.md` | Corrected RocksDB helper maps, facade integration limits, timeout and batch defaults, health/stats, scheduled backup, optional metrics, and cache distinctions. |
| `guides/user/quads/02-data-management.md` | Reconciled graph-aware insertion, graph enumeration, tuple order, dataset-preserving Loader/Exporter APIs, and low-level cache responsibilities. |
| `guides/user/quads/03-sparql-queries.md` | Added the required public ACL setup, actor-aware lower-level example, cache bypass behavior, variable graphs, and current quad Leapfrog caveat. |
| `guides/user/quads/04-sparql-updates.md` | Reconciled graph update authorization, facade public/internal behavior, variable-target preauthorization, one-MODIFY batching, per-operation commits, and cache invalidation. |
| `guides/user/quads/05-reasoning.md` | Reconciled graph-ID inputs, local/global materialization, graph-zero GSPO derived storage, tuple order, and malformed-key recovery guidance. |
| `guides/user/quads/06-configuration.md` | Replaced nonexistent graph health/statistics/backup methods with current functions and clarified full-store versus graph backup scheduling. |
| `guides/user/quads/07-named-graphs.md` | Reconciled implicit graph creation, public ACL requirements, list behavior for empty graphs, graph-copy signatures, actor-aware authorization, dataset I/O, and graph backup. |

## Material corrections

The former guides referred to APIs that are absent from this release, including
`TripleStore.schema/1`, `TripleStore.list_graphs/1`,
`TripleStore.refresh_statistics/1`, old `QuadIndex` CRUD calls,
`QuadOperations.count_graph_quads/2`, `Health.check/1`,
graph/full-store scheduled-backup helpers on the wrong modules, a public RocksDB
compaction call, `Paginator.all_results/1`, the removed storage NIF wrapper,
`Executor.execute/2`, `Algebra.extract_patterns/1`, and
`Targets.check_simple_bgp/1`. Those examples and the prose built around them
were removed or replaced.

Runtime verification also caught two behavior-level discrepancies that an
exported-function check cannot find:

1. SELECT returns a list of binding maps, not a map containing `variables` and
   `results`.
2. The query facade evaluates named graphs as `:public`; an existing graph
   must have a public read ACL or the query returns `{:error, :unauthorized}`.

The updated examples and focused tests cover both behaviors.

## Validation

The following checks passed on the audited content:

```text
Guide example validation passed (52 Elixir blocks).
Guides governance validation passed.
3 tests, 0 failures
Wikidata parser validation passed (15 queries).
Wikidata corpus smoke passed (4 queries).
Wikidata smoke benchmark passed and emitted its artifact bundle.
git diff --check
```

The focused test is
`test/triple_store/guides/current_examples_test.exs`. The static exported-API
and syntax check is `scripts/validate_guide_examples.exs`.

The focused tests were run against the committed dependency definition because
the working tree already contained an unrelated, unfetched `spec_led_ex`
dependency edit in `mix.exs`. That file was restored byte-for-byte after the
test.

`mix docs` generated HTML and EPUB output without warnings after all guides,
specifications, production runbooks, and the license were registered as extras
with stable filenames. Historical planning references remain outside the
published documentation set and are explicitly excluded from undefined-link
warnings. The malformed inline-code backquote in the `SPARQL.Executor`
moduledoc was also corrected.
