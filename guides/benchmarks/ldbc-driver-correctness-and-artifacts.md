# LDBC Driver, Correctness, and Artifacts

Phase 3 provides a common execution boundary for SPB, SNB BI, and SNB
Interactive. It is benchmark tooling, not a public TripleStore server. The bridge
must be started explicitly with `benchmark_mode: true`; it is absent from the
application supervision tree and binds no network socket.

## Driver boundary

`TripleStore.Benchmark.LDBC.Bridge.PortOwner` starts the upstream driver as an
Erlang Port child with four-byte packet framing. Each packet is UTF-8 JSON. A run
begins with a handshake containing protocol version, profile ID, dataset manifest
checksum, and executable operation-catalog checksum. Requests carry unique IDs and
use `execute`, `batch`, `reset`, `checkpoint`, `health`, `cancel`, or `shutdown`.

The bridge enforces frame, parameter, batch, concurrency, and deadline limits.
Errors have a stable class such as `parse`, `validation`, `execution`, `timeout`,
`cancellation`, `storage`, `reasoning`, or `bridge`. The bridge and runtime own
request tasks, the driver Port, optional services, and the embedded store so they
can release them deterministically.

## Operation and result contracts

Executable definitions are in `priv/benchmarks/ldbc/operations.exs`. Each one
links to a Phase 1 catalog operation, profile, pinned source checksum, and local
transformation version. SPARQL strategies keep `$parameter` placeholders in query
text and pass values through the prepared-query API. Raw interpolation is not part
of the execution model.

Parameters and results use strict codecs. Narrowing overflow, floating-point
precision loss, non-UTC timestamps, invalid UTF-8, missing columns, unknown
parameters, and null/unbound confusion are correctness failures. Typed rows retain
column order and duplicate multiplicity. Canonical sorting is allowed only for an
unordered result contract.

## Measurement and correctness

Execution records setup, parse, plan, execute, materialize, validation, and
teardown boundaries where available. Deadlines cover lazy stream consumption.
Warmup records remain separate. A measured latency is eligible only after the full
answer or update effect passes validation; a failed operation invalidates the
enclosing score gate.

Small answers are compared in full. Large answers compare row counts and SHA-256
ordered or multiset hashes. Accepted divergences require explicit specification
permission, a reason, and a pinned source version.

## Artifacts and baselines

`TripleStore.Benchmark.LDBC.Artifacts.write/2` emits versioned manifest,
environment, catalog, raw-sample, error, correctness, resource, and summary JSON,
plus normalized CSV and Markdown. An official score appears only when profile,
correctness, scheduling, duration, and completeness gates all pass.

Baseline acceptance is a separate review action:

```sh
mix benchmark.ldbc.baseline.accept \
  --input results/correctness.json \
  --output priv/benchmarks/ldbc/baselines/run.json \
  --source-version v1.0.3 \
  --reason "Reviewed against the pinned reference output"
```

The command rejects incorrect inputs and records the source checksum, reason, and
acceptance time. Measurement never invokes it automatically.
