# Reasoning: Quad Schema

Graph-aware reasoning routes through
`TripleStore.Reasoner.GraphScopedReasoner` and persists inferred facts through
`TripleStore.Reasoner.DerivedStore`.

## Local graph materialization

`materialize_graph/3` takes an internal graph ID, not an RDF graph IRI.

~~~elixir
{:ok, stats} =
  TripleStore.materialize_graph(store, graph_id,
    profile: :owl2rl,
    parallel: true
  )
~~~

`materialize_graphs/3` accepts several graph IDs and materializes them
independently. `tbox_graph:` can supply a shared schema graph.

## Global materialization

~~~elixir
{:ok, stats} =
  TripleStore.materialize_all(store,
    profile: :owl2rl,
    storage_strategy: :per_graph_cf
  )
~~~

Global evaluation merges facts from all graphs. With `:per_graph_cf`, inferred
triples are stored as canonical GSPO keys in the `derived` column family under
graph ID 0. Global evaluation does not retain a unique premise graph for each
derivation, so it cannot place each fact back into its source graph.

`DerivedStore` quad APIs use
`{graph_id, subject_id, predicate_id, object_id}`. This differs from
`QuadOperations`, whose tuples are `{s, p, o, g}`.

## Recovery and maintenance

Older malformed derived data may contain SPOG-ordered bytes. Both layouts are
32 bytes, so format cannot be inferred from key length. Follow the
[derived-quad recovery runbook](../../../docs/production/derived-quad-recovery.md)
for a backup-first clear and rebuild.

The facade also exposes graph-aware incremental additions, deletions, and
inference explanation. These APIs take internal graph IDs and RDF statements;
review their typespecs and nearby tests before integrating them.

The default `TripleStore.materialize/2` local path remains triple-oriented and
does not persist its returned closure.
