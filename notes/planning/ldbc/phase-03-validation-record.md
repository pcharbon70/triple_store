# LDBC Phase 3 Validation Record

## Scope

Phase 3 implements the suite-neutral operation model, strict codecs, benchmark-only
Erlang Port bridge, owned runtime lifecycle, fail-fast measurements, correctness
comparison, score gating, and versioned evidence artifacts. The bridge remains
outside the public TripleStore API and default supervision tree, as required by
ADR-0002.

## Executed validation

Validation used installed Elixir `1.19.5-otp-28` and Erlang `28.3.1` as temporary
asdf overrides because the repository's bare `1.19.5` and `28.3` identifiers are
not installed locally. No toolchain file was changed.

- `./scripts/compile_strict.sh`: passed. Dependency compilation emitted existing
  third-party warnings; TripleStore compiled cleanly.
- `mix format --check-formatted`: passed.
- `mix credo --strict`: passed with no findings across 493 source files.
- `mix dialyzer`: passed with zero errors after checking the current PLT.
- Phase 3 focused and integration files together: 23 tests, zero failures.
- The final affected rerun after the Dialyzer cleanup: 9 tests, zero failures.
- `validate_specs_governance.sh`: passed with 58 requirements, 69 acceptance
  criteria, 17 scenarios, 2 ADRs, and 6 matrix rows.
- `validate_guides_governance.sh`: passed.
- `validate_rfc_governance.sh`: skipped as designed because no `rfcs/` directory exists.
- `validate_code_docs.sh`: passed.
- `mix conformance`: passed with the same governance counts.

The full default suite completed 6,817 tests with 53 skipped and 345 excluded. It
found one race in the new Port-exit assertion; the test observed an exit-status
message just before process termination. The assertion now monitors the process
and waits for its exact `:DOWN` message, and the corrected integration suite passes.
A second full-suite start was prevented by a transient RocksDB `ENOSPC` response
for `/dev/shm`, although `df` reported the 32 GiB tmpfs empty immediately afterward.

## Excluded profiles and remaining capability work

The standard test helper excluded `:benchmark`, `:large_dataset`, `:slow`, and
`:lifetime_safety` coverage. No externally downloaded full-scale LDBC dataset or
canonical Java distribution was executed in this phase.

The common runner is ready for suite work, but comparable results still require
the complete SPB, SNB BI, and SNB Interactive translations, reference answers,
canonical schedules, and full-scale protocol runs planned in Phases 4 through 6.
The three checked-in executable operations are smoke representatives and cannot
be reported as complete or official LDBC scores.
