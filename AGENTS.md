# Working on TripleStore

## Project and documentation authority

TripleStore is an embedded Elixir/OTP RDF database library with persistent
triple and quad (named graph) schemas, SPARQL query/update execution, OWL 2 RL
reasoning, and operational/benchmark tooling. It is not a Phoenix application
or an HTTP service. RocksDB storage uses the `rocksdb` dependency's C++ NIF;
the repository's Rust NIF parses SPARQL using `spargebra` and Rustler.
RDF document parsing and data structures use the Elixir `rdf` dependency.

Start with these sources, then inspect the implementation and adjacent tests:

- [Specs index](specs/README.md), [architecture](specs/architecture-overview.md),
  [topology](specs/topology.md), and [boundaries](specs/boundaries.md).
- [Ownership matrix](specs/contracts/control_plane_ownership_matrix.md) and
  [ADR-0001](specs/adr/ADR-0001-control-plane-authority.md) define semantic authority.
- [Contracts](specs/contracts/README.md) define required behavior;
  [governance](specs/specs-governance-and-compliance-guide.md) explains traceability.
- [User guides](guides/user/README.md) split triple and quad workflows.
  [Developer guides](guides/developer/README.md) explain internals and data flow.
- `notes/planning/` holds delivery plans; `notes/research/` is background;
  `notes/review/` contains historical reviews. Neither proves current behavior.
- `docs/production/` contains operational checklists/runbooks;
  `guides/benchmarks/` describes benchmark workflows and targets.

Treat specs as normative intent and executable code/tests as evidence of current
behavior. Report discrepancies rather than assuming either is synchronized.
README feature claims, old comments, and completed planning checkboxes are not
proof of complete standards conformance. `CLAUDE.md` and `CONTRIBUTING.md` contain
older architecture/path references; verify them before following examples.
The separate `.claude/AGENTS.md` describes Claude-specific orchestration and is
not the root project architecture guide.

## Source map and runtime flow

| Area | Entry points and responsibilities |
| --- | --- |
| Public API | `lib/triple_store.ex`: lifecycle, query, RDF I/O, updates, reasoning, operations; `lib/triple_store/update.ex`: expert update API |
| OTP runtime | `lib/triple_store/application.ex`: default supervision of `SPARQL.PlanCache` and `Snapshot` |
| Storage boundary | `lib/triple_store/backend/rocksdb/erlang_adapter.ex`: DB/column-family handles, schema validation, batches, iterators, snapshots; `backend/rocksdb/iterator.ex`: iterator process lifetime |
| RDF terms | `lib/triple_store/dictionary.ex`, `dictionary/`, `adapter.ex`: tagged IDs, bidirectional mapping, sequence allocation, sharded encoding, RDF conversion |
| Explicit indices | `lib/triple_store/index.ex`, `quad_index.ex`, `quad_operations.ex`, `index_protocol.ex`: key encoding, schema-specific lookup, atomic index fanout |
| RDF I/O | `lib/triple_store/loader.ex`, `exporter.ex`: graph/dataset ingestion and export; preserve graph context at each boundary |
| Query pipeline | `lib/triple_store/sparql/query.ex`, `parser.ex`, `algebra.ex`, `optimizer.ex`, `executor.ex`: parse, algebra, optimize, execute, materialize/stream results |
| Planning | `sparql/cardinality.ex`, `quad_cardinality.ex`, `cost_model.ex`, `join_enumeration.ex`, `leapfrog/`, `parallel_executor.ex`, plus `statistics/` |
| Updates | `lib/triple_store/transaction.ex`, `sparql/update_executor.ex`, `sparql/update/`: coordination and typed update operations |
| Reasoning | `lib/triple_store/reasoner/`: profiles/rules, compilation, semi-naive evaluation, derived storage, graph scope, provenance, incremental maintenance and rederivation |
| Operations | `backup.ex`, `graph_backup.ex`, `scheduled_backup.ex`, `health.ex`, `snapshot.ex`, `telemetry.ex`, `metrics.ex`, `prometheus.ex`, `config/` under `lib/triple_store/` |
| Native parser | `native/sparql_parser_nif/src/lib.rs` and `lib/triple_store/sparql/parser/nif.ex`: Rust-to-Elixir AST boundary; query/update parsing uses dirty CPU scheduling |
| Governance/benchmarks | `lib/mix/tasks/`, `lib/triple_store/specs/validator.ex`, `lib/triple_store/benchmark/`, `priv/benchmarks/`, `scripts/` |

`TripleStore.open/2` validates the path, opens the selected schema, starts a
dictionary manager (optionally sharded), and returns a map containing `db`,
`dict_manager`, `transaction`, `path`, and `schema`. `transaction` starts as `nil`.
`close/1` stops the dictionary manager and closes storage. Separately started
statistics, result-cache, metrics, and scheduled-backup helpers need explicit
lifecycle management; do not assume the facade starts or stops them.

Queries pass through `SPARQL.Query`, the parser/AST/algebra boundary, optimizer,
and Elixir executor. Execution uses dictionary IDs and indices. Preserve typed
results/errors and lazy resource cleanup. Native adapters execute bounded work;
query semantics, optimization, transaction coordination, and reasoning remain
Elixir-owned and preemptible.

## Storage and semantic invariants

- Terms use tagged 64-bit IDs; preserve inline numeric/temporal encodings,
  bidirectional dictionary consistency, and restart-safe sequence allocation.
- The default schema is `:triple` (v1): `spo`, `pos`, `osp`, with 24-byte keys.
  `schema: :quad` (v2) uses `gspo`, `gpos`, `spog`, `posg`, with 32-byte keys.
  Keys concatenate big-endian 64-bit IDs. Every explicit mutation must update
  all relevant indices atomically through storage batches.
- Default graph ID is `0`; named graph IDs must not collide with it. Schema
  metadata is persisted and mismatched opens are rejected. Triple-to-quad
  migration is export/import into a new store, not an in-place schema switch.
- **Quad tuple order differs across APIs.** `QuadOperations` accepts/returns
  `{s, p, o, g}`; `Reasoner.DerivedStore` quad APIs use `{g, s, p, o}`.
  `QuadIndex.key_to_quad/2` returns canonical `{s, p, o, g}`, while raw
  `decode_gspo_key/1` is graph-first. Check the actual callee before converting.
- Derived facts have their own `derived` persistence surface. Preserve explicit
  versus inferred distinctions, provenance, and graph scope during deletion,
  incremental reasoning, backup, and export. Quad ACL/provenance column families
  are additional persisted metadata; do not assume index lists are all CFs.
- Use `ErlangAdapter` for storage I/O. The old `native/rocksdb_nif/` and storage
  `RocksDB.NIF` wrapper are absent; do not recreate them from older guidance.
- Close iterators and release snapshots on success, exhaustion, early stream
  halt, exceptions, and failed initialization. Prefer existing managed streams
  or folds where appropriate; do not rely on garbage collection for cleanup.

## API distinctions and current caveats

- `insert/2`, `delete/2`, and load APIs use direct loader/storage writes.
  `update/2` uses the handle's transaction manager or creates a temporary one
  per call. `query/3` goes directly to `SPARQL.Query`, not `Transaction.query/3`.
  Serialization through one coordinator does not establish a shared lock
  across independent temporary coordinators or direct writes.
- Review isolation claims against `transaction.ex` implementation: update
  handling is synchronous, and the query context does not receive the snapshot
  created during update execution. Snapshot lifecycle support alone does not
  prove concurrent snapshot reads or whole-request rollback across batches.
- `SPARQL.PlanCache` is automatically supervised. `TripleStore.Query.Cache`
  is the optional result cache used by `SPARQL.Query`. `SPARQL.QueryCache` is a
  separate tested implementation. Production result keys include the open-store
  identity, and successful supported mutations invalidate that store in every
  active named result cache. ACL-governed quad queries bypass result caching
  until authorization has a stable revision identity. Do not treat these three
  cache modules as interchangeable.
- Eager queries use timeout isolation. Lazy query streaming does not provide
  a timeout over subsequent stream consumption; preserve this distinction.
- The facade does not expose actor-aware query options. Named-graph ACL hooks
  live in lower-level execution contexts. Named graphs alone do not establish
  tenant authorization; follow existing authorization paths when changing them.
- Generic `export/3` is graph-oriented. Use `Exporter` dataset/named-graph APIs
  or `GraphBackup` when all graph identities must survive a round trip.
- Default `materialize/2` (`scope: :local`) reads triple indices, invokes
  `SemiNaive.materialize_in_memory`, and returns statistics while discarding
  the returned fact set. Do not describe this path as persisting inferences or
  as schema-neutral. Graph-scoped APIs route through `GraphScopedReasoner`.
  Fully ground premises are checked through the configured lookup provider;
  lookup failures abort materialization with a tagged error. Global
  `:per_graph_cf` materialization stores canonical GSPO derived keys in graph 0.
- `Statistics.Cache` remains a legacy integration; `Statistics.Server` is its
  intended successor. Metrics and Prometheus are opt-in services.

These are navigation cautions grounded in source, not permission to silently
change API semantics. Pair fixes with focused behavior tests and spec updates.

## Build and validation

Check `.tool-versions`, `mix.exs`, and `.github/workflows/ci.yml` before setup.
At this review, local pins are Elixir 1.19.5, OTP 28.3, Rust 1.93.1; CI uses
Elixir 1.19.5, OTP 27.3, Rust 1.93.1. `mix.exs` requires Elixir `~> 1.18`.
Native builds also need C/C++ tools, CMake, pkg-config, RocksDB and compression
development libraries; the CI file contains the complete Ubuntu package list.
CI and wrappers use `ERLANG_ROCKSDB_OPTS=-DCMAKE_POLICY_VERSION_MINIMUM=3.5`.

```sh
mix deps.get
./scripts/compile_strict.sh
mix format --check-formatted
mix test test/triple_store/api_test.exs
mix test path/to/affected_test.exs:LINE
mix test
mix credo --strict
mix dialyzer --format short
./scripts/validate_specs_governance.sh
./scripts/validate_guides_governance.sh
./scripts/validate_rfc_governance.sh
./scripts/validate_code_docs.sh
./scripts/run_conformance.sh
```

`compile_strict.sh` compiles dependencies separately so third-party compiler
warnings are not promoted to application errors. `.githooks/pre-commit` runs
governance, compilation, tests, strict Credo, and Dialyzer. Check the workflow
files for CI's separate jobs; do not assume CI runs every hook check.

`mix conformance` validates documentation structure, identifiers, links, and
requirement/acceptance/scenario traceability. It does **not** execute scenario
tests or certify SPARQL standards compliance. The RFC validator intentionally
skips when no `rfcs/` directory exists.

Tests mirror implementation under `test/triple_store/`; shared helpers are in
`test/support/`. Follow nearby fixtures and ownership/cleanup patterns. Use
unique disposable DB paths; release managers/iterators before deleting files.
Avoid asynchronous tests that mutate shared globally named services or ETS.
`test/test_helper.exs` excludes `:benchmark`, `:large_dataset`, `:slow`, and
`:lifetime_safety` by default; use `mix test --include TAG` deliberately for
relevant excluded coverage. A default suite run is not all test coverage.

Prioritize regression tests for both schemas, encoding/reopen compatibility,
graph preservation, index fanout, cache invalidation, and resource lifetime
when those behaviors change. The dedicated iterator regression is
`test/triple_store/sparql/leapfrog/quad_iterator_cleanup_test.exs`.

For Rust changes:

```sh
cargo fmt --manifest-path native/sparql_parser_nif/Cargo.toml -- --check
cargo clippy --manifest-path native/sparql_parser_nif/Cargo.toml
cargo test --manifest-path native/sparql_parser_nif/Cargo.toml
mix test test/triple_store/sparql/parser_test.exs
```

`mix compile` builds the parser into `priv/native/`. Generated native binaries
must remain untracked. `RUSTLER_SKIP_COMPILATION=1` skips compilation only; it
does not supply a working parser. Rebuild stale local parser artifacts after
toolchain/branch changes rather than committing a platform-specific binary.

Existing benchmark entry points include `mix benchmark.wikidata parser`,
`mix benchmark.wikidata corpus-smoke`, `mix benchmark.wikidata smoke`, and
`mix run scripts/run_benchmarks.exs` (WatDiv). See
[Wikidata benchmarking](guides/benchmarks/wikidata-benchmarking.md) for baselines,
correctness comparisons, and artifacts. README's `bench/bsbm.exs` is absent.
Do not equate synthetic benchmark targets with measured production performance.

## Change discipline

- Inspect `git status` and preserve pre-existing user edits. Do not update
  dependencies, locks, toolchain pins, or unrelated code just to write docs.
- Follow existing Elixir formatting, tagged error conventions, typespecs, and
  neighboring module organization. Production modules require meaningful
  `@moduledoc`; `scripts/validate_code_docs.exs` rejects missing/false module docs.
- For semantic changes, review the relevant contract and area spec in the same
  change. Keep `REQ-*`, `AC-*`, and `SCN-*` mappings synchronized. Ownership
  changes must update the ownership matrix and ADR-0001. Update guides when
  caller/operator behavior changes; keep delivery plans under `notes/planning/`.
- Run checks appropriate to the change and report exact failures or skipped
  checks. Distinguish source inspection from executed behavior verification.
- If asdf cannot resolve the pins, inspect installed versions first. A temporary
  environment override can support local analysis without editing project pins;
  report that override. Missing dependencies/NIFs preclude claiming a passing
  application suite.
