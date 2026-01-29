# SPARQL Queries (Quad Store)

This guide covers querying data with SPARQL in a quad store with named graphs.

> **Note**: This guide is for quad stores (`schema: :quad`) with GRAPH clauses. For triple stores, see the [Triple Store SPARQL Queries](../triples/03-sparql-queries.md) guide.

## Query Basics

### Executing Queries

```elixir
# Open quad store
{:ok, store} = TripleStore.open("./my_database", schema: :quad)

# Execute query
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?subject ?predicate ?object
  WHERE {
    GRAPH ex:people {
      ?subject ?predicate ?object
    }
  }
  LIMIT 10
""")

for row <- results do
  IO.inspect(row)
end
```

### Query Types

TripleStore supports all SPARQL query types:

| Type | Returns | Use Case |
|------|---------|----------|
| SELECT | List of binding maps | Retrieving specific data |
| ASK | Boolean | Checking existence |
| CONSTRUCT | RDF.Graph | Creating new graphs |
| DESCRIBE | RDF.Graph | Getting resource descriptions |

## GRAPH Clause (Named Graphs)

The `GRAPH` keyword allows you to query specific named graphs in a quad store.

### Query a Specific Graph

```elixir
# Open quad store first
{:ok, store} = TripleStore.open("./my_database", schema: :quad)

# Query a specific named graph
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o
  WHERE {
    GRAPH ex:source1 {
      ?s ?p ?o
    }
  }
  LIMIT 100
""")
```

### Query All Graphs

Use a graph variable to iterate over all graphs:

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?g ?s ?p ?o
  WHERE {
    GRAPH ?g {
      ?s ?p ?o
    }
  }
  ORDER BY ?g
""")
```

### Combine Graph Patterns

Query multiple specific graphs with UNION:

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o
  WHERE {
    { GRAPH ex:graph1 { ?s ?p ?o } }
    UNION
    { GRAPH ex:graph2 { ?s ?p ?o } }
  }
""")
```

### Graph Variable in Patterns

Use graph variables to correlate data across graphs:

```elixir
# Find quads that appear in multiple graphs
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o
  WHERE {
    GRAPH ex:graph1 { ?s ?p ?o }
    GRAPH ex:graph2 { ?s ?p ?o }
  }
""")
```

### Filter by Graph

Combine graph patterns with FILTER:

```elixir
# Query specific graphs from a set
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?g ?s ?p ?o
  WHERE {
    GRAPH ?g {
      ?s ex:name ?o .
      FILTER (?g IN (ex:graph1, ex:graph2, ex:graph3))
    }
  }
""")
```

### Graph with Subpatterns

Combine graph patterns with other patterns:

```elixir
# Match pattern in graph, then join with default graph
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?person ?department
  WHERE {
    GRAPH ex:employees {
      ?person a ex:Employee .
    }
    ?person ex:worksIn ?department .
  }
""")
```

### Count per Graph

Get statistics for each graph:

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?g (COUNT(*) AS ?tripleCount)
  WHERE {
    GRAPH ?g {
      ?s ?p ?o
    }
  }
  GROUP BY ?g
  ORDER BY DESC(?tripleCount)
""")
```

### Default Graph vs Named Graphs

```elixir
# Default graph (no GRAPH keyword) - queries default graph only
{:ok, results} = TripleStore.query(store, """
  SELECT ?s ?p ?o
  WHERE { ?s ?p ?o }
""")

# All graphs (with GRAPH variable)
{:ok, results} = TripleStore.query(store, """
  SELECT ?g ?s ?p ?o
  WHERE { GRAPH ?g { ?s ?p ?o } }
""")

# Union of default and all named graphs
{:ok, results} = TripleStore.query(store, """
  SELECT ?s ?p ?o
  WHERE {
    { ?s ?p ?o }
    UNION
    { GRAPH ?g { ?s ?p ?o } }
  }
""")
```

## SELECT Queries

### Basic SELECT with GRAPH

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  SELECT ?name
  WHERE {
    GRAPH <http://example.org/people> {
      ?person foaf:name ?name
    }
  }
""")
```

### Select with Graph Variable

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  SELECT ?g ?name
  WHERE {
    GRAPH ?g {
      ?person foaf:name ?name
    }
  }
""")
```

## FILTER Expressions

### FILTER within GRAPH

```elixir
# Filter within a specific graph
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?person
  WHERE {
    GRAPH ex:people {
      ?person ex:age ?age .
      FILTER (?age >= 18)
    }
  }
""")
```

### FILTER across Graphs

```elixir
# Filter by graph name
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?person
  WHERE {
    GRAPH ?g {
      ?person ex:name ?name .
    }
    FILTER (STRSTARTS(STR(?g), ex:public))
  }
""")
```

## OPTIONAL with GRAPH

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  SELECT ?name ?email
  WHERE {
    GRAPH <http://example.org/people> {
      ?person foaf:name ?name .
      OPTIONAL { ?person foaf:mbox ?email }
    }
  }
""")
```

## UNION with GRAPH

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o
  WHERE {
    { GRAPH ex:graph1 { ?s ?p ?o } }
    UNION
    { GRAPH ex:graph2 { ?s ?p ?o } }
    UNION
    { GRAPH ex:graph3 { ?s ?p ?o } }
  }
""")
```

## Property Paths with GRAPH

Property paths work within GRAPH clauses:

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?person ?manager
  WHERE {
    GRAPH ex:organization {
      ?person ex:reportsTo+ ?manager
    }
  }
""")
```

## Subqueries with GRAPH

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?g (AVG(?age) AS ?avgAge)
  WHERE {
    {
      SELECT ?g (AVG(?age) AS ?avgAge)
      WHERE {
        GRAPH ?g {
          ?person ex:age ?age
        }
      }
      GROUP BY ?g
    }
    FILTER (?avgAge > 30)
  }
""")
```

## ASK Queries with GRAPH

```elixir
{:ok, exists} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  ASK {
    GRAPH ex:people {
      ex:alice ex:knows ex:bob
    }
  }
""")

if exists do
  IO.puts("Alice knows Bob in the people graph")
end
```

## CONSTRUCT Queries with GRAPH

```elixir
{:ok, graph} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  CONSTRUCT {
    GRAPH ex:output {
      ?person ex:hasFullName ?fullName
    }
  }
  WHERE {
    GRAPH ex:input {
      ?person ex:firstName ?first .
      ?person ex:lastName ?last .
    }
    BIND(CONCAT(?first, " ", ?last) AS ?fullName)
  }
""")
```

## DESCRIBE Queries with GRAPH

```elixir
{:ok, graph} = TripleStore.query(store, """
  DESCRIBE <http://example.org/alice>
""")

# Returns all triples where alice is subject or object, from all graphs
```

## Query Options

### Timeout

```elixir
# 5 second timeout
{:ok, results} = TripleStore.query(store, sparql, timeout: 5_000)

# Handle timeout
case TripleStore.query(store, sparql, timeout: 5_000) do
  {:ok, results} -> results
  {:error, :timeout} -> fallback_results()
end
```

### Explain (Query Plan)

```elixir
{:ok, plan} = TripleStore.query(store, sparql, explain: true)
IO.inspect(plan)
```

## Common Patterns

### Find All Graphs

```elixir
"""
SELECT DISTINCT ?g
WHERE {
  GRAPH ?g { ?s ?p ?o }
}
ORDER BY ?g
"""
```

### Find Graphs Matching Pattern

```elixir
"""
SELECT ?g
WHERE {
  GRAPH ?g {
    ?s a ex:Person
  }
}
"""
```

### Cross-Graph Joins

```elixir
# Find resources that appear in multiple graphs with different properties
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?resource ?prop1 ?prop2
  WHERE {
    GRAPH ex:graph1 {
      ?resource ?prop1 ?value1
    }
    GRAPH ex:graph2 {
      ?resource ?prop2 ?value2
    }
  }
""")
```

### Graph Pattern Matching

```elixir
# Match specific pattern across graphs
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?person ?department
  WHERE {
    # Get person from employee graph
    GRAPH ex:employees {
      ?person a ex:Employee .
    }
    # Get department from org chart graph
    GRAPH ex:orgchart {
      ?person ex:worksIn ?department .
    }
  }
""")
```

### Graph Provenance

```elixir
# Track where data came from
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?fact ?source
  WHERE {
    GRAPH ?source {
      ?s ?p ?o
    }
    BIND(CONCAT(STR(?s), " ", STR(?p), " ", STR(?o)) AS ?fact)
  }
""")
```

## Tips

### Use Graph Variables for Discovery

```elixir
# Discover all graphs and their contents
"""
SELECT ?g (COUNT(*) AS ?count)
WHERE {
  GRAPH ?g { ?s ?p ?o }
}
GROUP BY ?g
ORDER BY DESC(?count)
"""
```

### Prefix Graph Names

```elixir
# Use readable graph names
"""
PREFIX ex: <http://example.org/>
PREFIX prov: <http://www.w3.org/ns/prov#>

INSERT DATA {
  GRAPH prov:Collection1/Import2024-01-20 {
    ?s a ex:Item .
  }
}
"""
```

### Limit Early

Always use LIMIT during development:

```elixir
"""
SELECT ?g ?s ?p ?o
WHERE { GRAPH ?g { ?s ?p ?o } }
LIMIT 100
"""
```

### Graph-Specific Optimization

When querying a specific graph, the query optimizer can use the GSPO and GPOS indices:

```elixir
# More efficient - specifies graph
"""
SELECT ?s ?p ?o
WHERE {
  GRAPH ex:people {
    ?s ?p ?o
  }
}
"""

# Less efficient - scans all graphs
"""
SELECT ?s ?p ?o
WHERE {
  ?s ?p ?o
  FILTER (?g = ex:people)
}
"""
```

## Next Steps

- [SPARQL Updates](04-sparql-updates.md) - Modify data in named graphs
- [Reasoning](05-reasoning.md) - Graph-scoped inference
- [Configuration & Performance](06-configuration.md) - Quad-specific tuning
- [Named Graphs](07-named-graphs.md) - Advanced named graph patterns
