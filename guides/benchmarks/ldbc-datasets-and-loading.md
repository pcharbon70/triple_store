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

## SNB pipeline

The [canonical SNB RDF mapping](ldbc-snb-rdf-mapping.md) is shared by BI and
Interactive. `SNB.Converter` streams the suites' distinct CSV layouts into
N-Quads, validates relationship endpoints with a disk-backed index, preserves
relationship properties, and emits separate initial, parameter, and ordered
update components. Every component is checksum-addressed in its dataset
manifest.

## Loading and reset lifecycle

`TripleStore.Benchmark.LDBC.StoreFixture.setup/3` validates every manifest
component checksum before opening a store. It acquires an exclusive lock for the
manifest's `path_identity`, loads initial N-Quads in bounded batches through the
normal dictionary and four-index write paths, closes the store, creates a
fingerprinted pristine copy, and verifies every explicit quad index after reopen.

Load metrics keep parse, RDF-shape mapping, dictionary, and write timings
separate. They also report statement throughput, the BEAM memory high-water
mark, warnings, and final on-disk store bytes. `cancel?: fn -> boolean end`
allows cooperative cancellation between batches; failed or cancelled setup
closes the store and removes its staging directory and lock.

Stateful SNB runs use `StoreFixture.apply_updates/2` for an ordered update
component and `StoreFixture.reset/2` before the next measured run. Reset closes
all store resources, verifies that the pristine copy has not changed, copies it
to a sibling staging directory, atomically exchanges directories, reopens the
store, and checks manifest counts plus any caller-supplied answer probes. Finish
with `StoreFixture.teardown/2` to release the run lock; pass `delete: true` to
remove both mutable and pristine copies.

## Reproducing external generator inputs

The automated Phase 2 tests use only checked-in smoke inputs. They do not run
Java, Spark, Hadoop, Docker, or network acquisition. The last verified upstream
pins recorded by the manifests are:

- SPB 2.0.2: `ce6323c0936306729408233dc70d26f2389b34c6`
- SNB BI Datagen 0.5.1: `2459f4e45834c78902a50511fc64a05c48dd4029`
- SNB Interactive v1 Datagen 1.0.0:
  `37d35f40f5023fcf1afd3b6d0984f71c202f4bca`

After checking out the matching commit and installing its documented runtime,
the exact TripleStore entry points are:

```sh
mix run -e 'alias TripleStore.Benchmark.LDBC.SPB.Pipeline; IO.inspect(Pipeline.run_external("/path/to/ldbc_spb_bm_2.0.2", "tmp/ldbc/spb", jar: "/path/to/spb-driver.jar", dataset_size: 1000000, seed: 42, parameter_count: 100000, allow_external: true))'

mix run -e 'alias TripleStore.Benchmark.LDBC.SNB.Generator; IO.inspect(Generator.run(:snb_bi_smoke, "/path/to/ldbc_snb_datagen_spark", "tmp/ldbc/snb-bi", cores: 8, memory: "16G", allow_external: true))'

mix run -e 'alias TripleStore.Benchmark.LDBC.SNB.Generator; IO.inspect(Generator.run(:snb_interactive_smoke, "/path/to/ldbc_snb_datagen_hadoop", "tmp/ldbc/snb-interactive", update_partitions: 1, allow_external: true))'
```

Each entry point verifies the pinned checkout before execution. The resulting
SPB N-Quads or SNB CSV directories must then pass the same validation and
conversion boundaries used by the smoke fixtures before store creation.
