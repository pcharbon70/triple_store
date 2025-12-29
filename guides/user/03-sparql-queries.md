# SPARQL Queries

This guide covers querying data with SPARQL, including syntax, patterns, and examples.

## Query Basics

### Executing Queries

```elixir
{:ok, results} = TripleStore.query(store, """
  SELECT ?subject ?predicate ?object
  WHERE {
    ?subject ?predicate ?object
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

## SELECT Queries

### Basic SELECT

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  SELECT ?name ?email
  WHERE {
    ?person foaf:name ?name .
    ?person foaf:mbox ?email .
  }
""")

# Results: [%{"name" => ..., "email" => ...}, ...]
```

### Select All Variables

```elixir
{:ok, results} = TripleStore.query(store, """
  SELECT *
  WHERE {
    ?s ?p ?o
  }
  LIMIT 100
""")
```

### Distinct Results

```elixir
{:ok, results} = TripleStore.query(store, """
  SELECT DISTINCT ?type
  WHERE {
    ?subject a ?type
  }
""")
```

## Triple Patterns

### Basic Pattern

```elixir
# Find all people
"""
?person a foaf:Person
"""

# Find names
"""
?person foaf:name ?name
"""
```

### Chained Patterns (Same Subject)

Use semicolon to add multiple predicates for the same subject:

```elixir
"""
?person a foaf:Person ;
        foaf:name ?name ;
        foaf:age ?age .
"""
```

### Multiple Objects

Use comma for multiple objects with the same predicate:

```elixir
"""
?person foaf:knows ?friend1, ?friend2, ?friend3 .
"""
```

## FILTER Expressions

### Comparison Operators

```elixir
# Numeric comparison
"""
FILTER (?age >= 18 && ?age < 65)
"""

# String comparison
"""
FILTER (?name = "Alice")
"""

# Not equal
"""
FILTER (?status != "inactive")
"""
```

### String Functions

```elixir
# Contains
"""
FILTER (CONTAINS(?name, "Smith"))
"""

# Starts with
"""
FILTER (STRSTARTS(?email, "admin@"))
"""

# Case-insensitive
"""
FILTER (LCASE(?name) = "alice")
"""

# Regular expression
"""
FILTER (REGEX(?email, "@example\\.org$", "i"))
"""
```

### Type Checking

```elixir
# Check if value is IRI
"""
FILTER (isIRI(?resource))
"""

# Check if value is literal
"""
FILTER (isLiteral(?value))
"""

# Check if bound
"""
FILTER (BOUND(?optionalValue))
"""
```

### Logical Operators

```elixir
# AND
"""
FILTER (?age > 18 && ?status = "active")
"""

# OR
"""
FILTER (?role = "admin" || ?role = "moderator")
"""

# NOT
"""
FILTER (!BOUND(?deletedAt))
"""
```

## OPTIONAL Patterns

Handle missing data gracefully:

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  SELECT ?name ?email ?phone
  WHERE {
    ?person foaf:name ?name .
    OPTIONAL { ?person foaf:mbox ?email }
    OPTIONAL { ?person foaf:phone ?phone }
  }
""")

# Results include rows where email or phone may be nil
```

## UNION Patterns

Combine alternative patterns:

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX dc: <http://purl.org/dc/elements/1.1/>
  PREFIX dcterms: <http://purl.org/dc/terms/>

  SELECT ?resource ?title
  WHERE {
    {
      ?resource dc:title ?title
    }
    UNION
    {
      ?resource dcterms:title ?title
    }
  }
""")
```

## VALUES (Inline Data)

Provide explicit value sets:

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?person ?name
  WHERE {
    VALUES ?person { ex:alice ex:bob ex:charlie }
    ?person foaf:name ?name .
  }
""")
```

## MINUS (Negation)

Exclude matching patterns:

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?person
  WHERE {
    ?person a ex:User .
    MINUS {
      ?person ex:banned true .
    }
  }
""")
```

## Solution Modifiers

### ORDER BY

```elixir
# Ascending (default)
"""
ORDER BY ?name
"""

# Descending
"""
ORDER BY DESC(?age)
"""

# Multiple criteria
"""
ORDER BY ?lastName ?firstName
"""
```

### LIMIT and OFFSET

```elixir
# First 10 results
"""
LIMIT 10
"""

# Pagination (page 3, 20 per page)
"""
LIMIT 20
OFFSET 40
"""
```

## Aggregates

### COUNT

```elixir
{:ok, results} = TripleStore.query(store, """
  SELECT (COUNT(?person) AS ?count)
  WHERE {
    ?person a foaf:Person
  }
""")

count = hd(results)["count"]
```

### GROUP BY

```elixir
{:ok, results} = TripleStore.query(store, """
  SELECT ?type (COUNT(?resource) AS ?count)
  WHERE {
    ?resource a ?type
  }
  GROUP BY ?type
  ORDER BY DESC(?count)
""")
```

### Other Aggregates

```elixir
"""
SELECT
  (SUM(?amount) AS ?total)
  (AVG(?amount) AS ?average)
  (MIN(?amount) AS ?minimum)
  (MAX(?amount) AS ?maximum)
WHERE {
  ?order ex:amount ?amount
}
"""
```

### HAVING

Filter groups:

```elixir
"""
SELECT ?type (COUNT(?item) AS ?count)
WHERE {
  ?item a ?type
}
GROUP BY ?type
HAVING (COUNT(?item) > 10)
"""
```

## Subqueries

```elixir
{:ok, results} = TripleStore.query(store, """
  SELECT ?person ?avgScore
  WHERE {
    ?person a foaf:Person .
    {
      SELECT ?person (AVG(?score) AS ?avgScore)
      WHERE {
        ?person ex:hasTest/ex:score ?score
      }
      GROUP BY ?person
    }
  }
""")
```

## Property Paths

Navigate graph structures:

### Sequence

```elixir
# Follow a path
"""
?person foaf:knows/foaf:knows ?friend_of_friend
"""
```

### Alternative

```elixir
# Either predicate
"""
?resource (dc:title|dcterms:title) ?title
"""
```

### Inverse

```elixir
# Reverse direction
"""
?person ^foaf:knows ?knownBy
"""
```

### Transitive Closure

```elixir
# Zero or more
"""
?child ex:parentOf* ?ancestor
"""

# One or more
"""
?employee ex:reportsTo+ ?manager
"""

# Zero or one
"""
?item ex:relatedTo? ?related
"""
```

### Negation

```elixir
# Any predicate except rdf:type
"""
?s !(rdf:type) ?o
"""
```

## ASK Queries

Check existence:

```elixir
{:ok, exists} = TripleStore.query(store, """
  ASK {
    <http://example.org/alice> foaf:knows <http://example.org/bob>
  }
""")

if exists do
  IO.puts("Alice knows Bob")
end
```

## CONSTRUCT Queries

Create new graphs:

```elixir
{:ok, graph} = TripleStore.query(store, """
  CONSTRUCT {
    ?person ex:hasFullName ?fullName
  }
  WHERE {
    ?person foaf:firstName ?first .
    ?person foaf:lastName ?last .
    BIND(CONCAT(?first, " ", ?last) AS ?fullName)
  }
""")
```

## DESCRIBE Queries

Get resource descriptions:

```elixir
{:ok, graph} = TripleStore.query(store, """
  DESCRIBE <http://example.org/alice>
""")

# Returns all triples where alice is subject or object
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

### Find All Types

```elixir
"""
SELECT DISTINCT ?type
WHERE {
  [] a ?type
}
ORDER BY ?type
"""
```

### Find All Predicates

```elixir
"""
SELECT DISTINCT ?predicate
WHERE {
  [] ?predicate []
}
ORDER BY ?predicate
"""
```

### Resource Description

```elixir
"""
SELECT ?property ?value
WHERE {
  <http://example.org/resource> ?property ?value
}
"""
```

### Path Finding

```elixir
"""
SELECT ?start ?end (COUNT(?mid) AS ?distance)
WHERE {
  ?start ex:connectedTo+ ?end .
  ?start ex:connectedTo* ?mid .
  ?mid ex:connectedTo* ?end .
}
GROUP BY ?start ?end
"""
```

## Tips

### Use Prefixes

Makes queries more readable and maintainable:

```elixir
"""
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ex: <http://example.org/>

SELECT ?name
WHERE { ?person foaf:name ?name }
"""
```

### Limit Early, Optimize Later

Start with LIMIT during development:

```elixir
"""
SELECT * WHERE { ?s ?p ?o } LIMIT 10
"""
```

### Put Most Selective Patterns First

The query optimizer reorders patterns, but hints help:

```elixir
# Good: specific pattern first
"""
?person a ex:VIP .       # Few results
?person ex:orders ?order  # Filters by the few VIPs
"""
```

### Use BIND for Computed Values

```elixir
"""
SELECT ?person ?ageGroup
WHERE {
  ?person foaf:age ?age .
  BIND(
    IF(?age < 18, "minor",
      IF(?age < 65, "adult", "senior"))
    AS ?ageGroup
  )
}
"""
```

## Next Steps

- [SPARQL Updates](04-sparql-updates.md) - Modify data with SPARQL
- [Reasoning](05-reasoning.md) - Enable inference for richer queries
- [Configuration & Performance](06-configuration.md) - Query optimization
