# Working with Named Graphs

## Overview

Named graphs in the quad store allow you to partition RDF data into separate, addressable graphs. This enables use cases like multi-tenancy, temporal versioning, access control, and provenance tracking.

## What is a Named Graph?

A named graph is a collection of RDF triples identified by an IRI (Internationalized Resource Identifier). In a quad store, each quad consists of:

```
{subject, predicate, object, graph}
```

The `graph` component is the named graph IRI, or `0` for the default graph.

## Creating and Managing Graphs

### Using SPARQL UPDATE

```elixir
# Create a new named graph
query = "CREATE GRAPH <http://example.org/my-graph>"
TripleStore.SPARQL.Update.execute(ctx, query)

# Insert data into a named graph
query = """
  INSERT DATA {
    GRAPH <http://example.org/my-graph> {
      <http://example.org/resource1>
        <http://example.org/name>
        "Example Resource" .
    }
  }
"""
TripleStore.SPARQL.Update.execute(ctx, query)

# Clear a graph (removes all triples, keeps graph)
query = "CLEAR GRAPH <http://example.org/my-graph>"
TripleStore.SPARQL.Update.execute(ctx, query)

# Drop a graph (removes all triples and the graph itself)
query = "DROP GRAPH <http://example.org/my-graph>"
TripleStore.SPARQL.Update.execute(ctx, query)
```

### Using Direct Quad Operations

```elixir
# Get graph ID from IRI
{:ok, graph_id} = TripleStore.Dictionary.Manager.encode(
  ctx.dict_manager,
  {:named_node, "http://example.org/my-graph"}
)

# Insert quad to named graph
TripleStore.QuadOperations.insert_quad(
  ctx.db,
  {subject_id, predicate_id, object_id, graph_id}
)
```

## Querying Named Graphs

### Querying a Single Named Graph

```elixir
query = """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o WHERE {
    GRAPH ex:my-graph {
      ?s ?p ?o .
    }
  }
"""
{:ok, results} = TripleStore.SPARQL.Query.query(ctx, query)
```

### Querying Multiple Named Graphs with UNION

```elixir
query = """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o ?g WHERE {
    { GRAPH ex:graph1 { ?s ?p ?o } }
    UNION
    { GRAPH ex:graph2 { ?s ?p ?o } }
  }
"""
{:ok, results} = TripleStore.SPARQL.Query.query(ctx, query)
```

### Querying with a Graph Variable

```elixir
query = """
  PREFIX ex: <http://example.org/>

  SELECT ?g ?count WHERE {
    {
      SELECT ?g (COUNT(?s) AS ?count) WHERE {
        GRAPH ?g {
          ?s a ex:Resource .
        }
      }
      GROUP BY ?g
    }
  }
  ORDER BY DESC(?count)
"""
{:ok, results} = TripleStore.SPARQL.Query.query(ctx, query)
```

### Cross-Graph Joins

```elixir
query = """
  PREFIX ex: <http://example.org/>

  SELECT ?resource ?provenance WHERE {
    # Get resources from data graph
    GRAPH ex:data {
      ?resource a ex:Resource .
    }
    # Join with provenance graph
    GRAPH ex:provenance {
      ?resource ex:source ?provenance .
    }
  }
"""
{:ok, results} = TripleStore.SPARQL.Query.query(ctx, query)
```

## Loading Data into Named Graphs

### Loading N-Quads

```elixir
# Load N-Quads file (each line has graph IRI)
{:ok, count} = TripleStore.Loader.load_file(
  ctx.db,
  ctx.dict_manager,
  "data.nq"
)
```

### Loading TriG

```elixir
# Load TriG file (explicit GRAPH blocks)
{:ok, count} = TripleStore.Loader.load_file(
  ctx.db,
  ctx.dict_manager,
  "data.trig",
  format: :trig
)
```

### Loading from String

```elixir
nquads = """
  <http://example.org/s1> <http://example.org/p> "o1" <http://example.org/graph1> .
  <http://example.org/s2> <http://example.org/p> "o2" <http://example.org/graph2> .
"""

{:ok, count} = TripleStore.Loader.load_nquads_string(
  ctx.db,
  ctx.dict_manager,
  nquads
)
```

## Common Patterns

### Multi-Tenant Data Isolation

```elixir
# Each tenant gets their own named graph
tenant_graph = "http://example.org/tenant/#{tenant_id}"

# Create tenant graph
TripleStore.SPARQL.Update.execute(ctx, "CREATE GRAPH <#{tenant_graph}>")

# Query only tenant's data
query = """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o WHERE {
    GRAPH <#{tenant_graph}> {
      ?s ?p ?o .
    }
  }
"""
```

### Temporal Versioning

```elixir
# Each time period gets its own graph
version_graph = "http://example.org/data/version/#{date}"

# Load data into versioned graph
TripleStore.Loader.load_file(
  ctx.db,
  ctx.dict_manager,
  "data-#{date}.nq"
)

# Compare versions
query = """
  PREFIX ex: <http://example.org/>

  SELECT ?resource ?v1_value ?v2_value WHERE {
    GRAPH <http://example.org/data/version/#{date1}> {
      ?resource ex:value ?v1_value .
    }
    GRAPH <http://example.org/data/version/#{date2}> {
      ?resource ex:value ?v2_value .
    }
    FILTER(?v1_value != ?v2_value)
  }
"""
```

### Access Control Lists

```elixir
# Store ACL in separate graph
acl_query = """
  PREFIX acl: <http://www.w3.org/ns/auth/acl#>
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:acls {
      ex:data acl:read "public" .
      ex:data acl:write "admin" .
      ex:provenance acl:read "admin" .
    }
  }
"""
TripleStore.SPARQL.Update.execute(ctx, acl_query)

# Check permissions before query
query = """
  PREFIX ex: <http://example.org/>

  SELECT ?graph ?permission WHERE {
    GRAPH ex:acls {
      ?graph ex:readAccess ?permission .
    }
  }
"""
```

## Listing Graphs

```elixir
# List all named graphs (excluding default)
{:ok, graphs} = TripleStore.QuadOperations.list_graphs(
  ctx.db,
  include_default: false
)

# graphs is a list of {:named_node, iri} tuples
Enum.each(graphs, fn {:named_node, iri} ->
  IO.puts("Graph: #{iri}")
end)
```

## Checking if Graph Exists

```elixir
# Check if a graph has any data
{:ok, has_data} = TripleStore.QuadOperations.graph_has_data?(
  ctx.db,
  ctx.dict_manager,
  "http://example.org/my-graph"
)
```

## Best Practices

1. **Use Descriptive Graph IRIs**: Make graph IRIs meaningful and stable
   - Good: `http://example.org/data/2024-01`
   - Bad: `http://example.org/graph1`

2. **Organize by Purpose**: Separate graphs by data purpose
   - `ex:data` - Main data
   - `ex:provenance` - Provenance metadata
   - `ex:acls` - Access control

3. **Consider Query Patterns**: Design graph structure to optimize queries
   - Put frequently joined data in adjacent graphs
   - Use graph prefixes for filtering

4. **Document Your Schema**: Maintain documentation of graph purposes
   - What data goes in each graph
   - How graphs relate to each other
   - Expected access patterns

## Performance Considerations

- **Graph-Scoped Queries**: Queries within a single GRAPH clause are fastest
- **Cross-Graph Queries**: Joins across graphs have overhead
- **Graph Count**: More graphs = more index overhead, consider consolidation
- **Batch Operations**: Use `INSERT DATA` with multiple GRAPH blocks for efficiency
