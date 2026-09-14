# Named-Graph Workflows

Named graphs require `schema: :quad`. Graph identity is persisted in every quad
index. The default graph uses ID 0; named graph IDs come from the dictionary and
must not collide with it.

## Create and query a graph

Graphs containing data are created implicitly by SPARQL insertion:

~~~elixir
graph = "http://example.org/tenant/acme"

{:ok, 1} =
  TripleStore.update(store, """
  INSERT DATA {
    GRAPH <#{graph}> {
      <http://example.org/alice> <http://example.org/name> "Alice"
    }
  }
  """)

ctx = %{db: store.db, dict_manager: store.dict_manager}
:ok = TripleStore.SPARQL.Authorization.set_public(ctx, graph)

{:ok, rows} =
  TripleStore.query(store, """
  SELECT ?name WHERE {
    GRAPH <#{graph}> {
      <http://example.org/alice> <http://example.org/name> ?name
    }
  }
  """)
~~~

An empty graph can reserve a dictionary ID through
`QuadOperations.create_graph/3`, but `list_graphs/2` discovers graphs by scanning
stored quads and therefore does not list an empty reservation.

~~~elixir
{:ok, graphs} = TripleStore.QuadOperations.list_graphs(store.db)
~~~

## Manage graph contents

Use SPARQL `CLEAR`, `DROP`, `COPY`, `MOVE`, and `ADD` through
`TripleStore.update/2`, or use corresponding expert functions on
`TripleStore.QuadOperations`. Expert functions require both `store.db` and
`store.dict_manager`; cache invalidation is caller-owned.

~~~elixir
source = RDF.iri("http://example.org/source")
target = RDF.iri("http://example.org/archive")

{:ok, copied} =
  TripleStore.QuadOperations.copy_graph(
    store.db,
    store.dict_manager,
    source,
    target,
    on_conflict: :replace
  )
~~~

## Authorization

Named graphs do not create tenant isolation by themselves. Facade queries run
as `:public` and can read a named graph only when its public ACL is set. The
public facade has no actor option. Facade updates omit a user and are treated as
internal/public updates; applications enforcing actor writes must use
`UpdateExecutor` with a user. Resolved variable graph targets are authorized
before actor-aware mutation, and ACL-governed queries bypass the materialized
result cache.

## Import, export, and recovery

Use dataset-aware `Loader` and `Exporter` functions for N-Quads or TriG round
trips. Generic facade load/export remains graph-oriented. Use
`GraphBackup.backup_graph/4` and `restore_graph/4` for one internal graph ID.

Graph-scoped reasoning and derived data have separate storage and recovery rules;
see the [quad reasoning guide](05-reasoning.md).
