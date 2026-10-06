# Phase 1 Validation Record

Description: This record captures the executable evidence used to close Phase 1
of the LDBC benchmark plan. It separates the metadata and architecture foundation
proved in this phase from benchmark execution and conformance work assigned to
later phases.

## Foundation inventory

The Phase 1 gate validates these checked-in artifacts as one coherent unit:

- 8 immutable upstream source entries with commit SHAs, release identifiers,
  checksums, licenses, notices, runtimes, platforms, and owning profiles.
- 13 benchmark profiles spanning smoke, development, comparable, and audit
  preparation claim levels.
- 4 versioned operation catalogs containing 119 unique operations: 55 SPB, 21
  SNB BI, 29 SNB Interactive v1, and 14 Interactive v2 delta operations.
- 4 accepted SHA-256 catalog digests used to detect nondeterministic or
  unreviewed catalog changes.
- 119 capability entries and 12 system findings, each with an owner,
  implementation phase, rationale, and evidence.
- Accepted architecture decision ADR-0002 for the benchmark bridge, RDF mapping,
  lifecycle ownership, and query-extension boundary.

The capability baseline classifies 96 operations as `requires_fix`, 6 as
`requires_extension`, and 17 as `profile_exclusion`. No canonical operation is
currently classified as fully supported because Phase 1 does not claim complete
dataset translation, expected-answer comparison, or official driver execution.

## Toolchain

The repository pins Elixir 1.19.5, Erlang/OTP 28.3, and Rust 1.93.1. Validation
used Elixir 1.19.5 for OTP 28 and the installed compatible Erlang/OTP 28.3.1
patch release through these temporary environment overrides:

```sh
ASDF_ELIXIR_VERSION=1.19.5-otp-28 ASDF_ERLANG_VERSION=28.3.1
```

No project toolchain files were changed.

## Commands and results

All commands ran from the repository root on 2026-10-05.

| Command | Result |
| --- | --- |
| `./scripts/compile_strict.sh` | Passed; application compilation was clean. Existing dependency warnings from `rdf`/`protocol_ex` and `erlex` were not promoted to application failures. |
| `mix format --check-formatted` | Passed. |
| `mix test test/triple_store/benchmark/ldbc` | Passed: 16 tests, 0 failures. |
| `mix test` | Passed: 3,761 tests, 0 failures, 1 skipped, 8 excluded. |
| `mix credo --strict` | Passed: 455 files, 8,168 functions/macros, no issues. |
| `mix dialyzer --format short` | Passed: 0 errors, 0 skipped, 0 unnecessary skips. |
| `mix conformance` | Passed: 58 requirements, 69 acceptance criteria, 17 scenarios, 2 ADRs, and 6 matrix rows. |
| `./scripts/validate_specs_governance.sh` | Passed with the same governance counts. |
| `./scripts/validate_guides_governance.sh` | Passed. |
| `./scripts/validate_code_docs.sh` | Passed. |

The default test run excluded the repository's `:benchmark`, `:large_dataset`,
`:slow`, and `:lifetime_safety` tags. Phase 1 did not execute external Java
drivers, data generators, large datasets, or official benchmark measurement
periods; those belong to later phases.

## Integration evidence and remaining blockers

The Phase 1 integration suite proves that all source pins, profiles, catalogs,
catalog digests, capability entries, and ADR links pass one executable gate. It
also verifies that moving source refs and operation IDs with unknown versions
fail closed. Representative SPB, BI, Interactive, and update operations reach
the real native parser.

A real quad store test exercises a coordinated SPARQL update, direct and
transaction-coordinated reads, and graph-scoped reasoning. Both read paths see
the explicit facts. The graph reasoning path reports zero derived facts for a
simple RDFS subclass case, matching capability finding `LDBC-CAP-005`; Phase 4
owns the required persisted, query-visible reasoning remediation.

The other blocking categories remain assigned by the capability matrix. They
include exact result semantics, benchmark transaction ordering, SNB RDF
translation, shortest-path extensions for BI reads 10, 15, 19, and 20 and
Interactive reads 13 and 14, plus benchmark-family lifecycle and reporting work.
