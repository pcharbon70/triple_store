# Native Backend And RDF I/O

## Purpose

This document backfills the storage-adjacent execution surfaces implemented by:

- `TripleStore.Backend`
- `TripleStore.Backend.RocksDB`
- `TripleStore.Backend.RocksDB.ErlangAdapter`
- `TripleStore.Config` and `TripleStore.Config.*`
- `TripleStore.Adapter`
- `TripleStore.Loader`
- `TripleStore.Exporter`
- `TripleStore.GraphBackup`

## Control Plane

Mixed ownership:

- **Storage Plane** for Elixir-owned validation, batching, graph handling, and conversion policy
- **Native Adapter Plane** for RocksDB execution
- **Data Plane** for persisted bytes and export artifacts

## Dependency View

```mermaid
graph TD
  A["Adapter"] --> B["Dictionary Managers"]
  A --> C["RocksDB ErlangAdapter"]

  D["Loader"] --> A
  D --> E["Flow / batching / bulk mode"]
  D --> C

  F["Exporter"] --> G["Index lookup_all / streams"]
  F --> A
  H["GraphBackup"] --> F
  H --> D

  I["Config / ColumnFamily / Compression / Compaction / Runtime"] --> C
  C --> J["RocksDB files, schema metadata, and snapshots"]
```

## Current Codebase Notes

- The storage backend is not just a thin wrapper; the Elixir side still owns path validation, schema selection, option handling, batch shaping, telemetry, and security constraints.
- The loader currently supports Flow-based parallel ingestion, dynamic batch sizing, progress callbacks, and a bulk-mode durability tradeoff.
- The SPARQL parser NIF is built locally from `native/sparql_parser_nif` into `priv/native/`; generated binaries are operational artifacts rather than canonical source files.
- RDF graph preservation depends on the entry point. Generic `Loader.load_file/4` without a named target and `Loader.load_string/5` use graph-oriented parsing: for N-Quads/TriG, `parse_file`/`parse_string` extract only the dataset's default graph, including when the destination schema is quad.
- Dedicated `Loader.load_nquads_file/4`, `load_nquads_string/4`, `load_trig_file/4`, and `load_trig_string/4` parse full datasets and load their quads. `Loader.load_graph/4` with an `RDF.Dataset` also preserves dataset graph identities through quad loading. Use these quad-store surfaces when named graphs must survive ingestion; selecting `schema: :quad` alone does not make every loader dataset-preserving.
- The generic `TripleStore.export/3` facade remains graph-oriented. `Exporter` additionally provides quad-aware N-Quads/TriG, dataset, default-graph, and selected named-graph exports; `GraphBackup` provides graph-scoped recovery. Graph-oriented and dataset-oriented APIs are not interchangeable.
- The config surface is split across general config plus RocksDB-specific modules (`column_family`, `compression`, `compaction`, `runtime`).

## Acceptance Criteria

| Acceptance ID | Criterion | Related Tests |
|---|---|---|
| `AC-STO-11` | The Elixir backend layer keeps validation, option shaping, and telemetry outside the native-adapter boundary. | `test/triple_store/backend/rocksdb_test.exs`, `test/triple_store/config/rocksdb_test.exs` |
| `AC-STO-12` | Loader batching, parallelization, bulk-mode behavior, and graph-preservation behavior remain explicit and testable runtime choices. | `test/triple_store/loader/batch_size_test.exs`, `test/triple_store/loader/parallel_loading_test.exs`, `test/triple_store/loader/pipeline_integration_test.exs`, `test/triple_store/integration/nquads_loading_test.exs`, `test/triple_store/integration/trig_loading_test.exs`, `test/triple_store/graph_scoped_loading_test.exs` |
| `AC-STO-13` | RDF adaptation remains the canonical bridge between RDF.ex terms and internal IDs for both triples and quads. | `test/triple_store/adapter/term_conversion_test.exs`, `test/triple_store/adapter/triple_graph_conversion_test.exs`, `test/triple_store/adapter/quad_conversion_test.exs` |
| `AC-STO-14` | Export paths preserve the current split between graph-oriented facade exports and quad-aware dataset or named-graph export surfaces. | `test/triple_store/exporter_test.exs`, `test/triple_store/exporter_refactoring_test.exs`, `test/triple_store/integration/rdf_roundtrip_test.exs`, `test/triple_store/dataset_operations_test.exs` |
| `AC-STO-15` | Graph backup, restore, and schema-aware RDF I/O remain documented as explicit expert workflows rather than being implied by the generic facade alone. | `test/triple_store/graph_backup_test.exs`, `test/triple_store/nquads_test.exs`, `test/triple_store/trig_test.exs` |
