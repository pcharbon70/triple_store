# Guides Index

The guides describe the current public workflows and the implementation behind
them. The [specification index](https://github.com/pcharbon70/triple_store/blob/main/specs/README.md) and its contracts remain the
authority for required behavior; source and tests show what this release
currently implements.

## Choose a path

- [User guides](https://github.com/pcharbon70/triple_store/blob/main/guides/user/README.md) cover opening a store, RDF I/O, SPARQL,
  reasoning, backup, and separate triple/quad workflows.
- [Developer guides](https://github.com/pcharbon70/triple_store/blob/main/guides/developer/README.md) cover runtime structure, storage,
  query execution, optimization, reasoning, telemetry, and data flow.
- [Performance targets](benchmarks/performance-targets.md) explains the
  repository's benchmark goals and executable benchmark suites.
- [Wikidata benchmarking](benchmarks/wikidata-benchmarking.md) documents the
  current corpus tasks and generated reports.
- [Ontology getting started](ontology/getting_started.md) is a short triple-store
  walkthrough. [Ontology performance tuning](ontology/performance_tuning.md)
  describes measured tuning controls.
- [erlang-rocksdb migration](erlang-rocksdb-migration.md) records the completed
  storage-adapter migration and the remaining compatibility boundaries.

Operational material starts with the
[pre-production checklist](../docs/production/pre-production-checklist.md) and
the other runbooks under `docs/production/`.
Delivery plans and historical reviews under `notes/` are useful context, but
they are not evidence of current behavior.
