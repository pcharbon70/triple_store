# Quad-Store Architecture

A quad store persists RDF datasets with default and named graphs. Open one with
`TripleStore.open(path, schema: :quad)`. The persisted schema is version 2 and
cannot be switched in place.

## Indices and tuple order

Quad keys are 32-byte concatenations of four big-endian 64-bit IDs:

| Index | Key order |
| --- | --- |
| `gspo` | graph, subject, predicate, object |
| `gpos` | graph, predicate, object, subject |
| `spog` | subject, predicate, object, graph |
| `posg` | predicate, object, subject, graph |

`QuadIndex.key_to_quad/2` returns canonical `{s, p, o, g}`, while
`decode_gspo_key/1` exposes the raw graph-first order.
`QuadOperations` accepts and returns `{s, p, o, g}`. `DerivedStore` quad
functions accept `{g, s, p, o}`.

## Graph operations

~~~elixir
alias TripleStore.QuadOperations

{:ok, graphs} = QuadOperations.list_graphs(store.db)
exists? = QuadOperations.graph_exists?(store.db, store.dict_manager, graph)

{:ok, count} =
  QuadOperations.copy_graph(
    store.db,
    store.dict_manager,
    source_graph,
    destination_graph
  )
~~~

Graph names are RDF terms resolved through the dictionary. The default graph ID
is `0`, and named graph IDs must not collide with it. Graph metadata, ACLs,
provenance, derived facts, and statistics are additional persisted concerns
beyond the four explicit indices.

## Query and authorization

GRAPH clauses become quad algebra and run through the SPARQL executor. Named
graphs provide data partitioning; they do not by themselves authorize a tenant.
Actor-aware authorization hooks are available in lower-level contexts. The
public facade does not expose actor options and queries named graphs as
`:public`. Existing non-public graphs are denied. ACL-controlled queries bypass
the result cache.

## I/O, reasoning, and backup

The generic facade load/export path is graph-oriented. Use the dedicated
`Loader` N-Quads/TriG functions and `Exporter` dataset functions when every
graph identity must survive. `GraphBackup` handles one named graph;
`Backup` handles a store. There is no graph-specific scheduled-backup API.

`GraphScopedReasoner` controls local, global, and hybrid materialization.
Preserve explicit versus derived data, graph scope, provenance, and tuple order
at every boundary.
