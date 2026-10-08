# LDBC Phase 4 Validation Record

## Scope and qualification result

Phase 4 now provides a deterministic SPB smoke workflow for the pinned LDBC SPB
2.0.2 source at commit `ce6323c0936306729408233dc70d26f2389b34c6`. It covers
checksum-protected operation packaging, typed parameter binding, quad-context
loading, persistent reasoning, all 25 aggregation templates, editorial and
validation transitions, mixed scheduling, reset, full backup/restore, and
versioned result artifacts.

The smoke workflow is correctness-qualified for its implemented aggregation,
editorial, validation, lifecycle, and coordinated-backup surfaces. It is not a
comparable or official SPB run. The scheduler suppresses rates unless the caller
explicitly supplies a completed qualification gate, and the artifact gate keeps
`official_score_eligible` false for the smoke workflow.

## Operation and integration evidence

- All 25 aggregation templates instantiate and pass the native parser and
  algebra pipeline. Each operation executes through optimized and non-optimized
  paths, and canonical answers retain duplicates and unbound values.
- Insert, update, and delete use the store-owned transaction coordinator. Every
  successful commit rederives persistent inference and runs its canonical graph
  validation probe. Invalid typed input fails before mutation.
- The mixed scheduler runs aggregation agents concurrently with an ordered
  editorial agent, separates warmup from measured records, reconciles all agent
  counts, and suppresses rates until qualification is explicit.
- Reset reproduces all 25 initial answer counts and digests exactly.
- Coordinated backup and fresh-path restore preserve the manifest identity,
  explicit graph digest, derived-fact count, and accepted query answer. The
  restored store is closed before the action returns.
- Online replication and failover fail at the profile gate before measurement.
  Backup is never labeled as replication or failover.
- The integration run writes and decodes manifest, environment, catalog, raw
  sample, error, correctness, disclosure, resource, and summary JSON files, plus
  CSV and Markdown outputs with checksums.

## Executed validation

Validation ran on 2026-10-08 with installed Elixir `1.19.5-otp-28` and Erlang
`28.3.1` through temporary asdf environment overrides. No toolchain file changed.

| Command | Result |
| --- | --- |
| `./scripts/compile_strict.sh` | Passed. TripleStore compiled cleanly; existing dependency compiler warnings were reported separately. |
| `mix format --check-formatted` | Passed. |
| `mix credo --strict` | Passed across 508 files and 8,915 modules/functions with no issues. |
| `mix dialyzer --format short` | Passed with zero errors, skipped warnings, or unnecessary skips. |
| `mix test test/triple_store/benchmark/ldbc` | Passed: 78 tests, 0 failures. |
| Affected SPARQL update and reasoning regression files | Passed: 117 tests, 0 failures, 19 skipped, 4 excluded. The run emitted pre-existing warnings in `section_7_8_3_global_materialization_test.exs`. |
| `./scripts/validate_guides_governance.sh` | Passed. |
| `./scripts/validate_code_docs.sh` | Passed. |
| `./scripts/validate_specs_governance.sh` | Passed with 58 requirements, 69 acceptance criteria, 17 scenarios, 2 ADRs, and 6 matrix rows. |

The standard test helper excluded `:benchmark`, `:large_dataset`, `:slow`, and
`:lifetime_safety` tags. No external Java generator, canonical measurement
duration, production-scale dataset, or multiple generated scale tier was run.

## Remaining Phase 4 qualification gaps

Four plan items remain open and are not hidden by smoke success:

- The pinned upstream `checkConformance` surface is a multi-file enterprise
  workflow. The local catalog currently reduces it to ten rule labels, while the
  checked-in workload does not contain the corresponding conformance datasets
  and query archives. Those actions remain `requires_fix` and block performance
  scoring.
- Complete answer cross-validation has used multiple parameter bindings on the
  deterministic smoke dataset, but not multiple generated scale tiers.
- Explain evidence is captured for every aggregation operation, but no optimizer
  or index change was justified by this small smoke dataset.
- Because mandatory conformance actions remain unqualified, the complete SPB
  comparable-operation gate is not satisfied.

The capability matrix therefore marks 42 operations supported, 54 requiring a
fix, 6 requiring an extension, and 17 excluded by profile. The supported count
includes 25 aggregation, 3 editorial, 3 validation, 9 lifecycle, and 2
coordinated-backup operations. The ten conformance operations remain unverified.
