# Phase 1 Test Strategy (Balanced)

## Scope

Phase 1 only: RocksDB NIFs, dictionary encoding, triple indices, and RDF adapter.
No SPARQL parser/algebra/executor or OWL reasoning coverage yet.

## Goals

- Keep the Rust bridge safe under lifecycle misuse (no use-after-free).
- Confirm DB correctness (CRUD, batch atomicity, iterators, snapshots).
- Validate dictionary encoding/decoding and adapter conversions.
- Prove index insert/lookup correctness for all core patterns.
- Include a minimal end-to-end roundtrip for integration confidence.

## Coverage Layers

1) Rust unit tests (native/rocksdb_nif)
- CF names are unique and non-empty.
- Basic put/get on a CF.
- Prefix iteration stays within bounds.
- Snapshot isolation is respected.
- WriteBatch writes are visible as a unit.

2) NIF contract tests (Elixir)
- open/close/is_open/get_path.
- read/write/delete/exists with invalid CF and already_closed errors.
- batch APIs with atomicity on error (no partial writes/deletes).
- iterator and snapshot APIs including lifetime safety after close.
- crash-harness subprocess tests to prevent VM-crashing regressions.

3) Dictionary + Adapter tests (Elixir)
- term ID encoding/decoding and inline numeric behavior.
- input validation and sequence counter persistence.
- manager get_or_create correctness and concurrency behavior.
- RDF term conversion roundtrips.

4) Index tests (Elixir)
- key encoding/decoding and lexicographic ordering.
- insert/delete and lookup across all pattern shapes.
- consistency across SPO/POS/OSP indices.

5) Full stack sanity (Elixir)
- RDF term -> ID -> index insert -> index lookup -> ID -> RDF term roundtrip.

## Out of Scope (defer to later phases)

- SPARQL parser/algebra/optimizer/executor tests (Phase 2+).
- OWL reasoning tests (Phase 4+).
- Long-running stress/soak tests or fuzzing.

## Commands

- Elixir tests: `mix test`
- Rust NIF tests: `cd native/rocksdb_nif && cargo test`
- All tests: `mix test && (cd native/rocksdb_nif && cargo test)`

## Notes

- **IMPORTANT**: Always run `mix compile` before `mix test` if you've modified
  any Rust code. If the NIF is recompiled mid-test, the loaded .so becomes
  invalid and tests will fail with `:nif_not_loaded`.
- Crash harness tests use `mix run --no-compile` in a subprocess to safely
  test NIF lifetime scenarios that could crash the VM.
- The full-stack integration test uses an isolated DB (not the pool) to
  avoid coupling with other test infrastructure.

## Test Counts (as of Phase 1 completion)

- Elixir tests: 664
- Rust unit tests: 5
