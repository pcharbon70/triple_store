# Phase 2: Dataset Generation, RDF Mapping, and Loading

Description: Phase 2 builds reproducible data pipelines for SPB and the two SNB
workloads. SPB data remains in its canonical RDF representation. SNB data receives
one documented RDF mapping shared by BI and Interactive while preserving the fact
that their generated datasets and update layouts differ. By the end of the phase,
small fixtures and external scale-factor datasets can be acquired or generated,
validated, streamed into TripleStore, reopened, and updated without losing source
provenance.

---

## Section 2.1: Dataset Manifests and External Artifact Cache

Description: This section establishes one suite-neutral format for dataset sources,
generated outputs, transformations, and local cache state.

### Task 2.1.1: Implement dataset manifests

Description: Define the machine-readable record that connects an upstream source
to the exact RDF fixture and store image used in a benchmark run.

- [ ] 2.1.1.1 Record suite, profile, scale factor, generator pin, generator settings,
  seed, source format, source checksum, and license metadata.
- [ ] 2.1.1.2 Record transformation version, RDF mapping version, output checksum,
  triple count, entity and relationship counts, and expected update-stream files.
- [ ] 2.1.1.3 Record storage schema, loader settings, store path identity, and
  post-load store statistics separately from source metadata.
- [ ] 2.1.1.4 Version the manifest schema and provide explicit errors for newer or
  incompatible versions.
- [ ] 2.1.1.5 Reuse suite-neutral provenance primitives from the Wikidata benchmark
  only after removing Wikidata-specific assumptions.

### Task 2.1.2: Implement acquisition and cache controls

Description: Provide deterministic local handling of generated and downloaded
artifacts without committing large benchmark datasets to Git.

- [ ] 2.1.2.1 Implement checksum-verified registration of pre-generated official datasets.
- [ ] 2.1.2.2 Implement generator invocation from pinned source or a pinned container image.
- [ ] 2.1.2.3 Use resumable acquisition for large external datasets and atomic promotion
  from partial downloads into the artifact cache.
- [ ] 2.1.2.4 Reject stale, partial, mismatched, or unmanifested cached artifacts.
- [ ] 2.1.2.5 Keep network acquisition out of tests and require explicit user commands
  for large downloads or generation jobs.

## Section 2.2: SPB RDF Dataset Pipeline

Description: This section integrates SPB reference data, ontologies, synthetic
creative-work generation, and update inputs using the pinned SPB driver and generator.

### Task 2.2.1: Package SPB source inputs

Description: Register every input needed to recreate the SPB dataset and query
parameter population.

- [ ] 2.2.1.1 Register the pinned reference datasets, ontologies, rule configuration,
  generator definitions, and scale settings.
- [ ] 2.2.1.2 Preserve graph or context identity when the SPB source distinguishes contexts.
- [ ] 2.2.1.3 Record optional text and geospatial source inputs separately from the
  core RDF profile.
- [ ] 2.2.1.4 Add a minimal smoke fixture derived through the same generator path as
  larger SPB datasets.

### Task 2.2.2: Generate and normalize SPB datasets

Description: Run the upstream generator reproducibly and normalize only the
transport details needed by TripleStore's RDF loader.

- [ ] 2.2.2.1 Invoke the pinned SPB generator with explicit scale, seed, and output paths.
- [ ] 2.2.2.2 Stream-validate RDF syntax and count statements without materializing
  the complete dataset in BEAM memory.
- [ ] 2.2.2.3 Preserve IRIs, blank nodes, language tags, datatypes, named graphs,
  and statement direction byte-for-byte where format permits.
- [ ] 2.2.2.4 Generate query substitution parameters only after the corresponding
  dataset has passed validation.
- [ ] 2.2.2.5 Record normalization steps and reject silent repairs of malformed RDF.

## Section 2.3: SNB-to-RDF Mapping and Generation

Description: This section defines and implements the canonical RDF representation
used by both SNB suites while retaining each suite's distinct data and update files.

### Task 2.3.1: Specify the SNB RDF mapping

Description: Map the SNB property-graph schema into stable RDF terms with explicit
rules for identity, labels, properties, relationships, datatypes, and ordering.

- [ ] 2.3.1.1 Define stable IRIs for every entity type and ID without depending on
  load order or dictionary IDs.
- [ ] 2.3.1.2 Define RDF classes and predicates for entity labels, scalar properties,
  and relationship types.
- [ ] 2.3.1.3 Define exact mappings for dates, timestamps, integers, strings, arrays,
  country and language values, and optional properties.
- [ ] 2.3.1.4 Define relationship-property representation where an SNB edge carries
  data not representable as a single RDF triple.
- [ ] 2.3.1.5 Define message, place, organisation, and other subtype handling without
  changing canonical operation semantics.
- [ ] 2.3.1.6 Define whether benchmark data uses the triple schema or a documented
  quad partitioning; prohibit switching mappings between runs.

### Task 2.3.2: Integrate SNB data generation and conversion

Description: Produce BI and Interactive RDF datasets from the pinned Datagen
outputs while preserving their required initial data, parameters, and update streams.

- [ ] 2.3.2.1 Invoke pinned SNB Datagen profiles for BI and Interactive with exact
  scale-factor and serializer settings.
- [ ] 2.3.2.2 Convert CSV or Parquet entity files to RDF as bounded streams.
- [ ] 2.3.2.3 Convert relationship files and relationship properties using the
  approved mapping.
- [ ] 2.3.2.4 Preserve the BI initial/update-batch split and the Interactive
  initial/update-stream split as separate manifest components.
- [ ] 2.3.2.5 Integrate official parameter generators and bind their output to the
  dataset manifest checksum and scale factor.
- [ ] 2.3.2.6 Validate source row counts, mapped statement counts, referential
  integrity, update ordering, and deterministic output checksums.

## Section 2.4: TripleStore Load and Store-Fixture Lifecycle

Description: This section provides a common lifecycle for loading benchmark data,
preparing store state, reopening stores, applying update streams, and cleaning up.

### Task 2.4.1: Implement streaming benchmark loads

Description: Load all benchmark formats through bounded-memory adapters while
measuring load behavior separately from query performance.

- [ ] 2.4.1.1 Add benchmark load adapters for SPB RDF and mapped SNB RDF streams.
- [ ] 2.4.1.2 Use existing loader and storage batches without bypassing dictionary
  or index invariants.
- [ ] 2.4.1.3 Capture parse time, mapping time, dictionary time, write time, throughput,
  warnings, memory high-water mark, and final store size.
- [ ] 2.4.1.4 Verify all relevant indices and schema metadata after load and reopen.
- [ ] 2.4.1.5 Add cancellation and tagged cleanup behavior for interrupted or failed loads.

### Task 2.4.2: Implement immutable fixture and reset controls

Description: Provide fast, reproducible reset behavior for workloads whose updates
make the store stateful.

- [ ] 2.4.2.1 Create a validated pristine-store snapshot or filesystem copy after
  each initial load.
- [ ] 2.4.2.2 Restore the pristine state atomically before stateful validation or
  measured runs that require it.
- [ ] 2.4.2.3 Prevent concurrent runs from sharing mutable store directories.
- [ ] 2.4.2.4 Release managers, transactions, iterators, snapshots, and RocksDB
  handles before copying, restoring, or deleting store fixtures.
- [ ] 2.4.2.5 Verify reset state using manifest counts and selected answer probes.

## Section 2.5: Integration Tests

Description: This final section proves that source inputs, transformations,
manifests, loading, reopen behavior, and reset behavior compose end to end.

### Task 2.5.1: Exercise the SPB data pipeline

Description: Validate SPB generation and load behavior using the offline smoke fixture.

- [ ] 2.5.1.1 Regenerate or validate the smoke dataset from pinned SPB inputs.
- [ ] 2.5.1.2 Verify RDF terms, contexts, ontologies, counts, and checksums survive
  load and reopen.
- [ ] 2.5.1.3 Verify generated substitution parameters refer to terms present in the store.
- [ ] 2.5.1.4 Verify malformed RDF, missing ontology inputs, and mismatched manifests
  fail before store promotion.

### Task 2.5.2: Exercise both SNB data pipelines

Description: Validate the shared RDF mapping against distinct BI and Interactive
dataset layouts and update inputs.

- [ ] 2.5.2.1 Convert and load smoke-scale BI and Interactive source files.
- [ ] 2.5.2.2 Compare mapped RDF entity, relationship, datatype, and statement counts
  with source counts.
- [ ] 2.5.2.3 Verify representative parameter files bind to existing mapped values.
- [ ] 2.5.2.4 Apply representative BI batches and Interactive update events, restore
  pristine state, and confirm exact pre-run answers return.
- [ ] 2.5.2.5 Run repeated load, reopen, reset, and teardown cycles and assert no
  leaked processes, iterators, snapshots, or filesystem locks.

### Task 2.5.3: Pass the Phase 2 reproducibility gate

Description: Establish that identical inputs produce identical benchmark-ready
stores and that different profiles cannot be confused.

- [ ] 2.5.3.1 Build each smoke fixture twice and compare manifests and RDF checksums.
- [ ] 2.5.3.2 Verify BI and Interactive manifests cannot be interchanged despite
  sharing the SNB ontology mapping.
- [ ] 2.5.3.3 Run formatting, strict compilation, focused loader tests, and the new
  LDBC data-pipeline integration tests.
- [ ] 2.5.3.4 Record external generators not executed in CI, their last verified
  pins, and the exact reproduction commands in the phase pull request.
