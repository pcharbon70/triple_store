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

## Qualification boundary

The checked-in smoke fixture is not an official scale-factor dataset and has no
accepted answer corpus for all BI operations. Exact analytical handlers for BI
1-9, 11-14, and 16-18 and operation-specific graph adapters for BI 10, 15, 19,
and 20 remain open. The current capability matrix is intentionally unchanged,
and the reduced workflow exposes only diagnostic scores.

No claim of official, certified, audited, or comparable SNB BI performance is
made by this phase record.

## Executed checks

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
