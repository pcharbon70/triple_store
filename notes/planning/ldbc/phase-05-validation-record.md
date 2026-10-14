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

## Phase 5A qualification boundary

The checked-in smoke fixture is not an official scale-factor dataset and has no
accepted answer corpus for all BI operations. Thirty canonical variants still
lack registered reference answers. At the end of Phase 5A, exact analytical
handlers for BI 1-9, 11-14, and 16-18 and operation-specific graph adapters for
BI 10, 15, 19, and 20 remained open. Phase 5B implements those handlers, as
recorded below. The capability matrix remains intentionally unchanged because
implementation availability alone does not establish accepted-answer
correctness, and the reduced workflow exposes only diagnostic scores.

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

## Phase 5C update and state qualification

The complete deterministic smoke sequence now covers all eight insert and all
eight delete record types. Inserts run in dependency order; relationship deletes
precede deep entity deletes. Every accepted microbatch is submitted as one mixed
RocksDB batch and produces matching `gspo`, `gpos`, `spog`, and `posg` contents.
The three updated checkpoints have these deterministic persisted states:

| Checkpoint | Statements | Entities | Relationships | Properties | Graphs | State checksum |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `post_validation` | 118 | 7 | 21 | 52 | 2 | `sha256:16a13d0c4d0037f18ff199e3edf61369a1dce6514739d645a9e2c58c73cd0102` |
| `pre_power` | 94 | 7 | 17 | 48 | 2 | `sha256:5be8fd1ab530aef984d0ef09fe8a8961c7abf198243ffba2602c8c7d9518250a` |
| `pre_throughput` | 42 | 3 | 3 | 24 | 1 | `sha256:4e699f53e569540a0c9be3d5524fa6763a02863d5467c44ac7c1c0a72e5d0385` |

All four named checkpoints restore their recorded counts, state checksum, and
selected affected BI answer hashes. BI 1 changes from
`sha256:3255eb8b221ef966e538e2172935bd856d8e7a0f48f57b66592a969453d91424`
to
`sha256:058df443305256117a13ed7d0f921b16f7869c6f1b7d09c003724010e672328c`
after inserts, and the complete delete sequence reproduces the initial hash.
These are deterministic smoke-state oracles; they do not replace the missing
accepted-answer corpus described in the Phase 5B boundary.

Injected translation, dependency, storage, interruption, and checkpoint
restore failures preserve the pre-commit state and all explicit indices.
Post-commit cache invalidation failure returns the full committed state identity
with `complete?: false` and `score_eligible?: false`; the scorer rejects that
record. Checkpoint restore validates the statement count recorded by the
selected checkpoint instead of applying the initial-load count to every update
state.

Sub-Phase 5C's update and state exit gate is complete. Phase 5 remains open:
Sub-Phase 5D protocol qualification, scoring equivalence, optimization, and
final evidence have not been completed, and analytical capability promotion is
still blocked on accepted reference answers.

## Executed checks

- Phase 5C update-state integration: 3 tests, 0 failures
- Phase 5C integration and adjacent LDBC regressions: 41 tests, 0 failures
- Final Phase 5B focused regression set after review remediation: 166 tests,
  0 failures, 4 excluded by the default test configuration
- Final default suite: 25 doctests, 10 properties, 6,901 tests, 0 failures,
  53 skipped, 345 excluded by the default test configuration
- Phase 5B `mix dialyzer --format short`: 0 errors and 0 skipped warnings
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
