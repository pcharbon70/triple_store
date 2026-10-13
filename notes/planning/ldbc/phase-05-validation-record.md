# Phase 5 SNB BI Validation Record

## Implemented boundary

- 31 pinned read variants are derived from the canonical catalog without
  duplicating parameter, result, ordering, limit, or choke-point metadata.
- Parameter bundles bind generator identity, dataset checksum, scale factor,
  provenance, row order, and typed schemas. Comparable mode rejects smoke or
  hand-selected provenance.
- Four named graph extensions provide bounded, cancellable, deterministic BFS
  and Dijkstra execution through lazy TripleStore index providers.
- Insert and delete records preserve sequence order and commit through one mixed
  RocksDB batch. Cache and statistics work begins only after commit.
- Four protocol checkpoints are immutable, closed-store copies protected by a
  content fingerprint and carry update position/checksum metadata.
- Validation, power, and throughput blocks preserve the pinned variant order.
  The local scorer implements the pinned power and throughput formulas and
  rejects incorrect, incomplete, unpaired, or too-short comparable inputs.

## Validation performed

The phase integration test converts the deterministic SNB BI fixture, opens the
quad store, creates and restores a checkpoint, applies the update stream, walks
the `knows` graph through store indices, executes two reduced dated blocks,
calculates a diagnostic score, and validates the complete artifact set. It also
injects an update failure, query timeout, incorrect answer, and incomplete power
block; every case blocks score eligibility.

## Phase 5A correctness qualification increment

The executable corpus gate covers all 31 canonical variants. Twenty-two
standard variants each resolve to one checksummed, semantically validated,
parseable SPARQL artifact. The nine BI 10, 15, 19, and 20 variants each resolve
to their operation's single registered extension identifier and have no SPARQL
artifact. Missing artifacts, malformed SPARQL, changed checksums, and wrong
placeholders all produce operation-scoped errors.

All 31 smoke parameter rows pass their catalog type contracts. The mapped-store
check validates every store-backed field selected by the `snb_bi_smoke`
reference profile, including all nine variant-level person-ID references. A
missing entity or dataset checksum mismatch invalidates the parameter bundle.
The smoke fixture does not contain the complete entity-name vocabulary required
by the `official` profile, so this evidence does not qualify official parameter
coverage.

Reference-answer coverage is recorded explicitly rather than inferred:

| Coverage kind | Canonical variants | Evidence |
| --- | ---: | --- |
| Checked-in | 1 | BI 1 `default`, parameter row 0 |
| External | 0 | No immutable external answer bundle is registered |
| Missing | 30 | Lookup returns `reference_answer_not_found` |

The checked-in BI 1 tuple is a deterministic contract fixture bound to the
smoke dataset, parameter row, operation source, and pinned reference-source
identity. It supplies the single deterministic tuple required by the Phase 5A
exit gate; it is not represented as complete reference-engine output. Synthetic
test-only contract fixtures prove empty, duplicate-bearing, ordered, timestamp,
32/64-bit integer, and floating-point round trips, along with missing-row,
unexpected-row, ordering, type, provenance, and deterministic-hash failures.
They are not accepted benchmark answers.

## Qualification boundary

The checked-in smoke fixture is not an official scale-factor dataset and has no
accepted answer corpus for all BI operations. Thirty canonical variants still
lack registered reference answers. Exact analytical handlers for BI
1-9, 11-14, and 16-18 and operation-specific graph adapters for BI 10, 15, 19,
and 20 remain open. The current capability matrix is intentionally unchanged,
and the reduced workflow exposes only diagnostic scores.

No claim of official, certified, audited, or comparable SNB BI performance is
made by this phase record.

## Phase 5B semantic-gap baseline

The generated qualification matrix now records all 31 canonical variants with
separate parse, algebra, optimized-plan, execution, typed-result, and expected
answer evidence. All 22 standard translations parse and prepare, including the
BI 12 list-valued `VALUES` parameter. Each standard variant resolves to the
registered typed prepared-query handler. Quad default-dataset execution and
property paths preserve the active named graphs and enforce their authorization
boundary.

All nine graph variants resolve to operation-specific BI 10, 15, 19, or 20
adapters. The adapters perform lazy index-backed candidate and edge lookup,
preserve the operation-specific unweighted or weighted semantics, retain global
equal-cost ties, enforce timeout, cancellation, visited and intermediate bounds,
and reject malformed or non-positive weights through tagged errors.

The contextual qualification integration executes all 31 variants successfully
against the reduced mapped smoke store and reproduces the same typed result
digests after close and reopen. The ordered aggregate digest is
`sha256:02cd4914ca255871dc36732eb406cc3b96ad09f92426ce62078fe112bbd9cfa1`.
Timeout, cancellation, and result-contract
failures remain per-variant evidence and prevent qualification without erasing
the other results.

The reference manifest uses schema version 2 to distinguish an
`accepted_answer` from a `contract_fixture`. The one embedded BI 1 artifact is a
contract fixture and is ineligible for accepted-answer comparison; 30 variants
have no artifact. Consequently all 20 BI capability entries remain unpromoted,
and the Sub-Phase 5B exit gate is blocked on a complete immutable accepted-answer
bundle from the pinned reference implementation. The reduced smoke data and
synthetic parameter rows also remain diagnostic rather than comparable-run
evidence.

## Executed checks

- Phase 5B contextual qualification and adjacent SNB BI regressions: 61 tests,
  0 failures
- Phase 5B reduced smoke qualification: 31 execution passes, 1 ineligible
  contract fixture, 30 missing accepted answers, 0 capability promotions
- Phase 5B `mix credo --strict`: no issues
- Phase 5B strict application compilation: passed; warnings came from third-party
  dependencies
- Phase 5B specs, guides, and code-doc governance validators: passed; RFC
  governance skipped because the repository has no `rfcs/` directory
- Phase 5A corpus/reference integration and adjacent correctness tests: 27
  tests, 0 failures
- `mix format --check-formatted`
- `./scripts/compile_strict.sh`
- Phase 5 and affected fixture tests: 18 tests, 0 failures
- `mix credo --strict`: no issues
- specs, guides, RFC, and code-doc validators: passed (RFC skipped because the
  repository has no `rfcs/` directory)
- `mix test`: 25 doctests, 10 properties, 6,849 tests, 0 failures, 53 skipped,
  345 excluded by the default test configuration

Strict compilation reported warnings from third-party dependencies. The
TripleStore application compiled without warnings.
