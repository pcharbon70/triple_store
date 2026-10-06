# LDBC datasets and loading

Phase 2 introduces one provenance and lifecycle boundary for SPB, SNB BI, and
SNB Interactive data. The APIs live under `TripleStore.Benchmark.LDBC`; they are
benchmark tooling and do not extend the public embedded-database API.

## Manifest boundary

`TripleStore.Benchmark.LDBC.DatasetManifest` stores three independent records:

- source identity: suite, profile, scale factor, immutable generator pin,
  settings, seed, source syntax, checksum, and license;
- transformation identity: converter and RDF-mapping versions, output checksum,
  source and statement counts, and update-stream components;
- store identity: triple or quad schema, loader settings, path identity, and
  post-load statistics.

The supported manifest schema is version 1. Manifests use deterministic Erlang
external-term encoding and safe decoding; newer schema versions fail explicitly.
Parameter sets must carry the dataset identity returned by
`DatasetManifest.identity/1`.

## External artifact cache

`TripleStore.Benchmark.LDBC.ArtifactCache` accepts only files matching a
canonical SHA-256 digest. Registrations and downloads use partial paths and are
promoted only after verification. Cache reads validate the completion marker,
file size, artifact identity, and checksum, so interrupted, stale, and manually
replaced files fail closed.

Network acquisition and upstream generator execution require explicit options.
The cache's acquisition command uses resumable `curl` transfers. External
generator commands run without a shell and require either an exact Git commit
checkout or an immutable container digest. Tests never enable those options.

Large datasets, generator checkouts, store images, and partial downloads belong
in an external cache and must not be committed. Small license-compatible smoke
inputs and their expected provenance may be committed under
`priv/benchmarks/ldbc/`.

## SPB pipeline

`TripleStore.Benchmark.LDBC.SPB.Pipeline` registers the reference datasets,
ontologies, rule configuration, generator definitions, graph identities,
editorial inputs, and optional text and geospatial inputs from the pinned SPB
2.0.2 source. The deterministic smoke generator emits the same N-Quads boundary
used by external runs.

SPB normalization validates one N-Quads statement at a time and copies accepted
bytes unchanged. It does not rewrite IRIs, blank nodes, language tags,
datatypes, graph names, or statement direction. A malformed line aborts with
its line number. Substitution parameters are generated only after the complete
dataset passes syntax validation and are bound to its output checksum.

For external generation, `SPB.Pipeline.external_generator_spec/3` creates a
properties file containing the explicit dataset size, seed, N-Quads syntax,
output path, and parameter count. `run_external/3` additionally verifies that
the checkout is at the pinned commit and requires `allow_external: true`.
