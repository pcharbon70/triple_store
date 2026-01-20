# Named Graphs

This guide covers TripleStore's support for RDF named graphs (quads), also known as the quad store.

> **Recommendation**: Quad store is the recommended format for new projects. It supports everything triple store does plus named graphs for data isolation and provenance tracking.

## Overview

TripleStore supports both traditional triple stores and quad stores with named graphs:

| Feature | Triple Store (v1) | Quad Store (v2) |
|---------|-------------------|-----------------|
| **Data Model** | `{subject, predicate, object}` | `{graph, subject, predicate, object}` |
| **Indices** | 3 (SPO, POS, OSP) | 4 (GSPO, GPOS, SPOG, POSG) |
| **Graph Support** | Implicit default graph | Explicit named graphs |
| **Default Graph ID** | N/A | 0 |
| **Use Case** | Simple datasets | Multi-tenant, provenance, data isolation |

## When to Use Named Graphs

Named graphs are useful when you need to:

- **Multi-tenancy**: Separate data per tenant, user, or organization
- **Provenance tracking**: Track where data came from
- **Data isolation**: Keep different datasets separate
- **Incremental loading**: Add new data sources without affecting existing data
- **Graph-scoped reasoning**: Apply reasoning rules to specific graphs

## Opening a Quad Store

To use named graphs, open a store with `schema: :quad`:

```elixir
# Open a quad store
{:ok, store} = TripleStore.open("./my_database", schema: :quad)
```

The default graph is represented by graph ID `0`. All graphs use numeric IDs internally for efficiency.

## Loading Data into Named Graphs

### Using SPARQL UPDATE

Load data into a specific graph using the `GRAPH` keyword:

```elixir
# Load into default graph
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:alice ex:name "Alice" .
  }
""")

# Load into named graph
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:graph1 {
      ex:bob ex:name "Bob" .
    }
  }
""")
```

### Loading from N-Quads Files

N-Quads format includes the graph context:

```elixir
# Load N-Quads file (graph is specified in the data)
{:ok, count} = TripleStore.load(store, "data.nq")
```

Example N-Quads format:
```
<http://example.org/alice> <http://example.org/name> "Alice" <http://example.org/graph1> .
<http://example.org/bob> <http://example.org/name> "Bob" <http://example.org/graph2> .
```

### Loading from TriG Files

TriG is Turtle with named graph support:

```elixir
{:ok, count} = TripleStore.load(store, "data.trig")
```

Example TriG format:
```
@prefix ex: <http://example.org/>.

ex:graph1 {
  ex:alice ex:name "Alice" .
}

ex:graph2 {
  ex:bob ex:name "Bob" .
}
```

## Querying Named Graphs

### GRAPH Clause

Use the `GRAPH` keyword to query a specific graph:

```elixir
# Query a specific named graph
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o
  WHERE {
    GRAPH ex:graph1 {
      ?s ?p ?o
    }
  }
""")
```

### Query All Graphs

Use a graph variable to iterate over all graphs:

```elixir
# Get triples from all graphs
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?g ?s ?p ?o
  WHERE {
    GRAPH ?g {
      ?s ?p ?o
    }
  }
""")
```

### Filter by Graph

Combine graph patterns with filters:

```elixir
# Query specific graphs
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o
  WHERE {
    GRAPH ?g {
      ?s ex:name ?o .
      FILTER (?g IN (ex:graph1, ex:graph2))
    }
  }
""")
```

### Default Graph Queries

Queries without `GRAPH` clause use the default graph (union of all unnamed triples):

```elixir
# Queries default graph only
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o
  WHERE {
    ?s ?p ?o
  }
""")
```

To include named graphs in the default graph query, use `UNION`:

```elixir
# Union of default and named graphs
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o
  WHERE {
    { ?s ?p ?o }
    UNION
    { GRAPH ?g { ?s ?p ?o } }
  }
""")
```

## Updating Data in Named Graphs

### INSERT into Named Graph

```elixir
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:graph1 {
      ex:charlie ex:name "Charlie" ;
                 ex:age 35 .
    }
  }
""")
```

### DELETE from Named Graph

```elixir
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE DATA {
    GRAPH ex:graph1 {
      ex:charlie ex:age 35 .
    }
  }
""")
```

### DELETE Entire Graph

To clear all data from a graph:

```elixir
# Method 1: DELETE all triples in the graph
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    GRAPH ex:graph1 {
      ?s ?p ?o
    }
  }
  WHERE {
    GRAPH ex:graph1 {
      ?s ?p ?o
    }
  }
""")
```

### COPY Graphs

Copy data between graphs:

```elixir
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT {
    GRAPH ex:graph_backup {
      ?s ?p ?o
    }
  }
  USING {
    GRAPH ex:graph1 {
      ?s ?p ?o
    }
  }
""")
```

### MOVE Graphs

Move data from one graph to another (deletes from source):

```elixir
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    GRAPH ex:graph1 {
      ?s ?p ?o
    }
  }
  INSERT {
    GRAPH ex:graph_archive {
      ?s ?p ?o
    }
  }
  WHERE {
    GRAPH ex:graph1 {
      ?s ?p ?o
    }
  }
""")
```

## Listing Graphs

Get all graphs in the store:

```elixir
{:ok, graphs} = TripleStore.list_graphs(store)

# graphs is a list of graph IDs or URIs
# For example: [0, 1, 2] or ["http://example.org/graph1", ...]
```

## Graph Statistics

Get statistics for a specific graph:

```elixir
# Count quads in a graph
{:ok, count} = TripleStore.graph_quad_count(store, "http://example.org/graph1")
IO.puts("Graph has #{count} quads")
```

## Graph-Scoped Reasoning

When using reasoning with named graphs, you can control the scope:

```elixir
alias TripleStore.Reasoner.GraphScopedReasoner

# Local reasoning - each graph reasons independently
config = ReasoningConfig.new(
  profile: :owl2rl,
  scope: :local
)

# Global reasoning - merge all graphs for reasoning
config = ReasoningConfig.new(
  profile: :owl2rl,
  scope: :global
)

# Materialize inferences for specific graph
{:ok, stats} = GraphScopedReasoner.materialize_graph(
  store,
  graph_id: 1,
  config: config
)
```

See [Reasoning](05-reasoning.md) for more details on graph-scoped reasoning.

## Performance Considerations

### Quad Store vs Triple Store

Quad stores have different performance characteristics:

| Metric | Triple Store | Quad Store |
|--------|--------------|-------------|
| Key Size | 24 bytes | 32 bytes (+33%) |
| Write Amplification | 3x | 4x (+33%) |
| Block Size | 8KB | 16KB |
| Memtable | 64MB | 128MB |
| Bloom Filter | 12 bits/key | 10 bits/key |

### When to Use Each

**Use Triple Store** when:
- You don't need named graphs
- Maximum performance is critical
- Your data is in a single context

**Use Quad Store** when:
- You need data isolation (multi-tenancy)
- You need provenance tracking
- You have multiple data sources
- You need graph-scoped reasoning

## Best Practices

### 1. Use Consistent Graph URIs

Define your graph URIs consistently:

```elixir
# Good: Clear, structured URIs
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>
  PREFIX prov: <http://www.w3.org/ns/prov#>

  INSERT DATA {
    GRAPH prov:Collection1/Import2024-01-20 {
      ?s a ex:Item .
    }
  }
""")
```

### 2. Group Related Data

Keep related data in the same graph:

```elixir
# Group source data together
# All triples from source A go in graph:source:a
# All triples from source B go in graph:source:b
```

### 3. Use Graph Variables Sparingly

Querying all graphs can be expensive:

```elixir
# Expensive: Scans all graphs
GRAPH ?g { ?s ?p ?o }

# Better: Query specific graphs
{
  GRAPH ex:graph1 { ?s ?p ?o }
  UNION
  GRAPH ex:graph2 { ?s ?p ?o }
}
```

### 4. Consider Graph Size

Larger graphs take longer to query. For very large datasets, consider splitting into multiple graphs.

## Migration from Triple Store

If you're migrating from a triple store, all existing triples will be assigned to the default graph (ID 0).

```elixir
# 1. Open triple store
{:ok, triple_store} = TripleStore.open("./old_db", schema: :triple)

# 2. Export to N-Triples
{:ok, _} = TripleStore.export(triple_store, "export.nt")

# 3. Open quad store
{:ok, quad_store} = TripleStore.open("./new_db", schema: :quad)

# 4. Import (all triples go to default graph)
{:ok, count} = TripleStore.load(quad_store, "export.nt")
```

## Complete Example

```elixir
defmodule MyApp.NamedGraphsExample do
  import RDF.Sigils

  def run do
    # 1. Open quad store
    {:ok, store} = TripleStore.open("./named_graphs_db", schema: :quad)

    # 2. Load data into different graphs
    load_data_by_source(store)

    # 3. Query across graphs
    query_all_graphs(store)

    # 4. Query specific graph
    query_specific_graph(store)

    # 5. Update specific graph
    update_graph(store)

    # 6. List all graphs
    list_graphs(store)

    # 7. Clean up
    :ok = TripleStore.close(store)
  end

  defp load_data_by_source(store) do
    # Source 1 data
    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>

      INSERT DATA {
        GRAPH ex:source1 {
          ex:alice ex:name "Alice" ;
                   ex:email "alice@example.org" .
        }
      }
    """)

    # Source 2 data
    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>

      INSERT DATA {
        GRAPH ex:source2 {
          ex:bob ex:name "Bob" ;
                 ex:email "bob@example.org" .
        }
      }
    """)
  end

  defp query_all_graphs(store) do
    {:ok, results} = TripleStore.query(store, """
      PREFIX ex: <http://example.org/>

      SELECT ?g ?name
      WHERE {
        GRAPH ?g {
          ?s ex:name ?name .
        }
      }
    """)

    IO.puts("\nAll graphs:")
    for row <- results do
      IO.puts("  #{row["g"]}: #{row["name"]}")
    end
  end

  defp query_specific_graph(store) do
    {:ok, results} = TripleStore.query(store, """
      PREFIX ex: <http://example.org/>

      SELECT ?s ?p ?o
      WHERE {
        GRAPH ex:source1 {
          ?s ?p ?o
        }
      }
    """)

    IO.puts("\nSource 1 triples:")
    for row <- results do
      IO.puts("  #{row["s"]} #{row["p"]} #{row["o"]}")
    end
  end

  defp update_graph(store) do
    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      INSERT DATA {
        GRAPH ex:source1 {
          ex:alice foaf:knows ex:bob .
        }
      }
    """)
  end

  defp list_graphs(store) do
    {:ok, graphs} = TripleStore.list_graphs(store)
    IO.puts("\nGraphs in store: #{inspect(graphs)}")
  end
end
```

## Next Steps

- [Configuration & Performance](06-configuration.md) - Tuning your quad store
- [Reasoning](05-reasoning.md) - Graph-scoped reasoning with OWL 2 RL
- [Data Management](02-data-management.md) - Backing up and restoring quad stores
