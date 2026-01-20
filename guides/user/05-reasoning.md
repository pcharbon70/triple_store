# Reasoning

This guide covers using OWL 2 RL reasoning to infer new knowledge from your data, with emphasis on graph-scoped reasoning for quad stores.

> **Note**: Graph-scoped reasoning requires opening the store with `schema: :quad`. See [Named Graphs](07-named-graphs.md) for details.

## What is Reasoning?

Reasoning (inference) automatically derives new facts from explicit data based on ontology rules. For example:

```
Explicit fact:   Alice is a Student
Schema rule:     Student is a subclass of Person
Inferred fact:   Alice is a Person
```

Without reasoning, queries for "all Persons" would miss Alice. With reasoning, Alice is automatically included.

## Reasoning Profiles

TripleStore supports three reasoning profiles:

| Profile | Description | Use Case |
|---------|-------------|----------|
| `:rdfs` | RDFS entailment only | Simple class/property hierarchies |
| `:owl2rl` | OWL 2 RL (includes RDFS) | Full semantic reasoning |
| `:all` | All available rules | Maximum inference |

### RDFS Profile

Includes rules for:
- Subclass transitivity (`rdfs:subClassOf`)
- Subproperty transitivity (`rdfs:subPropertyOf`)
- Domain/range inference (`rdfs:domain`, `rdfs:range`)
- Type inheritance

### OWL 2 RL Profile

Includes RDFS rules plus:
- Transitive properties (`owl:TransitiveProperty`)
- Symmetric properties (`owl:SymmetricProperty`)
- Inverse properties (`owl:inverseOf`)
- Functional properties (`owl:FunctionalProperty`)
- Same-as reasoning (`owl:sameAs`)
- Restriction-based inference

## Materializing Inferences

Run reasoning to compute inferences:

```elixir
# Default profile (owl2rl)
{:ok, stats} = TripleStore.materialize(store)

# Specific profile
{:ok, stats} = TripleStore.materialize(store, profile: :rdfs)

# Check results
IO.puts("Iterations: #{stats.iterations}")
IO.puts("Derived triples: #{stats.total_derived}")
IO.puts("Duration: #{stats.duration_ms}ms")
```

Materialization computes all derivable facts and stores them. Queries then see both explicit and derived facts without runtime overhead.

## Practical Examples

### Class Hierarchies

Define a class hierarchy in your ontology:

```turtle
@prefix ex: <http://example.org/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:GraduateStudent rdfs:subClassOf ex:Student .
ex:Student rdfs:subClassOf ex:Person .
ex:Person rdfs:subClassOf ex:Agent .
```

Add instance data:

```elixir
{:ok, _} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:alice a ex:GraduateStudent .
    ex:bob a ex:Student .
  }
""")
```

Materialize and query:

```elixir
{:ok, _} = TripleStore.materialize(store, profile: :rdfs)

# Query all Persons - includes Alice and Bob
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?person
  WHERE {
    ?person a ex:Person
  }
""")
# Results: [alice, bob]

# Query all Agents - still includes them
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?agent
  WHERE {
    ?agent a ex:Agent
  }
""")
# Results: [alice, bob]
```

### Property Hierarchies

Define property hierarchy:

```turtle
ex:hasChild rdfs:subPropertyOf ex:hasDescendant .
ex:hasGrandchild rdfs:subPropertyOf ex:hasDescendant .
```

Data and inference:

```elixir
{:ok, _} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:alice ex:hasChild ex:bob .
    ex:bob ex:hasChild ex:charlie .
  }
""")

{:ok, _} = TripleStore.materialize(store, profile: :rdfs)

# Query all descendants
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?person ?descendant
  WHERE {
    ?person ex:hasDescendant ?descendant
  }
""")
# Results include: alice->bob, bob->charlie
```

### Transitive Properties

Define transitive property:

```turtle
@prefix owl: <http://www.w3.org/2002/07/owl#> .

ex:contains a owl:TransitiveProperty .
```

Data:

```elixir
{:ok, _} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:country ex:contains ex:state .
    ex:state ex:contains ex:city .
    ex:city ex:contains ex:neighborhood .
  }
""")

{:ok, _} = TripleStore.materialize(store, profile: :owl2rl)

# Transitivity is inferred
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?container
  WHERE {
    ?container ex:contains ex:neighborhood
  }
""")
# Results: city, state, country (transitive closure)
```

### Symmetric Properties

Define symmetric property:

```turtle
ex:knows a owl:SymmetricProperty .
```

Data:

```elixir
{:ok, _} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:alice ex:knows ex:bob .
  }
""")

{:ok, _} = TripleStore.materialize(store, profile: :owl2rl)

# Inverse is inferred
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  ASK {
    ex:bob ex:knows ex:alice
  }
""")
# Returns: true
```

### Inverse Properties

Define inverse properties:

```turtle
ex:hasParent owl:inverseOf ex:hasChild .
```

Data:

```elixir
{:ok, _} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:alice ex:hasChild ex:bob .
  }
""")

{:ok, _} = TripleStore.materialize(store, profile: :owl2rl)

# Inverse is inferred
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?parent
  WHERE {
    ex:bob ex:hasParent ?parent
  }
""")
# Results: [alice]
```

### Domain and Range

Define domain and range:

```turtle
ex:teaches rdfs:domain ex:Teacher .
ex:teaches rdfs:range ex:Course .
```

Data:

```elixir
{:ok, _} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:alice ex:teaches ex:math101 .
  }
""")

{:ok, _} = TripleStore.materialize(store, profile: :rdfs)

# Types are inferred
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?type
  WHERE {
    ex:alice a ?type
  }
""")
# Results include: Teacher

{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?type
  WHERE {
    ex:math101 a ?type
  }
""")
# Results include: Course
```

## Graph-Scoped Reasoning

When using a quad store with named graphs, you can control the scope of reasoning. This allows you to reason independently within each graph, or merge all graphs for global reasoning.

> **Note**: Graph-scoped reasoning requires opening the store with `schema: :quad`. See [Named Graphs](07-named-graphs.md) for details.

### Local vs Global Reasoning

```elixir
alias TripleStore.Reasoner.ReasoningConfig

# Local reasoning - each graph reasons independently
local_config = ReasoningConfig.new(
  profile: :owl2rl,
  scope: :local
)

# Global reasoning - merge all graphs for reasoning
global_config = ReasoningConfig.new(
  profile: :owl2rl,
  scope: :global
)
```

**Local (`:local`)**: Each named graph is reasoned independently. Inferences stay within their source graph.

**Global (`:global`)**: All graphs are merged before reasoning. Inferences may derive from data across multiple graphs.

### When to Use Each Scope

| Scope | Use Case |
|-------|----------|
| `:local` | Multi-tenant data, isolated datasets, provenance tracking |
| `:global` | Unified ontology, shared schema across graphs |

### Local Reasoning Example

```elixir
# Open quad store
{:ok, store} = TripleStore.open("./my_database", schema: :quad)

# Load schema into graph1 (shared ontology)
TripleStore.update(store, """
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:schema {
      ex:Student rdfs:subClassOf ex:Person .
    }
  }
""")

# Load data into different graphs
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:graph1 {
      ex:alice a ex:Student .
    }
    GRAPH ex:graph2 {
      ex:bob a ex:Student .
    }
  }
""")

# Materialize with local scope
config = ReasoningConfig.new(profile: :owl2rl, scope: :local)
{:ok, stats} = TripleStore.materialize(store, config: config)

# Query graph1 - alice is now also a Person in graph1
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?type
  WHERE {
    GRAPH ex:graph1 {
      ex:alice a ?type
    }
  }
""")
# Results: [Student, Person]
```

### Global Reasoning Example

```elixir
# With global scope, all graphs are merged for reasoning
config = ReasoningConfig.new(profile: :owl2rl, scope: :global)
{:ok, stats} = TripleStore.materialize(store, config: config)

# Inferences can derive from data across any graph
# Useful when schema is in one graph and data in others
```

### Materializing Specific Graphs

Materialize inferences for a specific graph:

```elixir
alias TripleStore.Reasoner.GraphScopedReasoner

# Materialize inferences for graph ID 1
{:ok, stats} = GraphScopedReasoner.materialize_graph(
  store,
  graph_id: 1,
  config: ReasoningConfig.new(profile: :owl2rl, scope: :local)
)
```

### Graph-Scoped Best Practices

**1. Schema Placement**

For local reasoning, place schema triples in the same graph as the data:

```elixir
# Good: Schema with data
TripleStore.update(store, """
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:tenant1 {
      ex:Student rdfs:subClassOf ex:Person .
      ex:alice a ex:Student .
    }
  }
""")
```

For global reasoning, place schema in a dedicated graph:

```elixir
# Good: Dedicated schema graph
TripleStore.update(store, """
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:schema {
      ex:Student rdfs:subClassOf ex:Person .
    }
  }
""")

# Data in separate graphs
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:tenant1 { ex:alice a ex:Student . }
    GRAPH ex:tenant2 { ex:bob a ex:Student . }
  }
""")

# Use global scope to apply schema to all graphs
config = ReasoningConfig.new(profile: :owl2rl, scope: :global)
{:ok, _} = TripleStore.materialize(store, config: config)
```

**2. Multi-Tenancy**

For multi-tenant data, use local scope:

```elixir
# Each tenant's data is isolated
# Tenant 1 cannot derive facts from Tenant 2's data
config = ReasoningConfig.new(profile: :owl2rl, scope: :local)
{:ok, _} = TripleStore.materialize(store, config: config)
```

**3. Performance Considerations**

Local reasoning is generally faster for large multi-tenant datasets:

```elixir
# Local reasoning processes each graph independently
# Can be parallelized across graphs
config = ReasoningConfig.new(profile: :owl2rl, scope: :local)
```

Global reasoning requires merging all graphs first:

```elixir
# Global reasoning merges all graphs before reasoning
# Use when you need cross-graph inferences
config = ReasoningConfig.new(profile: :owl2rl, scope: :global)
```

## When to Materialize

### After Loading Data

```elixir
# Load your data
{:ok, _} = TripleStore.load(store, "data.ttl")
{:ok, _} = TripleStore.load(store, "ontology.ttl")

# Then materialize once
{:ok, stats} = TripleStore.materialize(store, profile: :owl2rl)
```

### After Schema Changes

If you modify schema triples (subclass, subproperty, etc.):

```elixir
# Add new schema triple
{:ok, _} = TripleStore.update(store, """
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:Manager rdfs:subClassOf ex:Employee .
  }
""")

# Rematerialize to derive new inferences
{:ok, stats} = TripleStore.materialize(store, profile: :owl2rl)
```

### Checking Reasoning Status

```elixir
{:ok, status} = TripleStore.reasoning_status(store)

IO.puts("State: #{status.state}")
IO.puts("Profile: #{status.profile}")
IO.puts("Derived facts: #{status.derived_count}")
IO.puts("Needs rematerialization: #{status.needs_rematerialization}")
```

## Best Practices

### 1. Load Ontology First

```elixir
# 1. Load schema/ontology
{:ok, _} = TripleStore.load(store, "ontology.ttl")

# 2. Load instance data
{:ok, _} = TripleStore.load(store, "data.ttl")

# 3. Materialize once
{:ok, _} = TripleStore.materialize(store)
```

### 2. Choose the Right Profile

```elixir
# If you only use rdfs:subClassOf and rdfs:subPropertyOf
{:ok, _} = TripleStore.materialize(store, profile: :rdfs)  # Faster

# If you use OWL features (transitive, symmetric, inverse, etc.)
{:ok, _} = TripleStore.materialize(store, profile: :owl2rl)
```

### 3. Avoid Frequent Rematerialization

Materialization can be expensive for large datasets. Batch your updates:

```elixir
# Bad: materialize after each insert
for triple <- triples do
  TripleStore.insert(store, triple)
  TripleStore.materialize(store)  # Expensive!
end

# Good: batch inserts, then materialize once
TripleStore.insert(store, triples)
TripleStore.materialize(store)  # Once
```

### 4. Monitor Materialization Performance

```elixir
{:ok, stats} = TripleStore.materialize(store)

if stats.iterations > 20 do
  Logger.warn("Many reasoning iterations - check for cycles or complex rules")
end

if stats.duration_ms > 60_000 do
  Logger.warn("Materialization took over 1 minute")
end
```

## Common Ontology Patterns

### Type Hierarchy

```turtle
# Define hierarchy
ex:CEO rdfs:subClassOf ex:Executive .
ex:Executive rdfs:subClassOf ex:Employee .
ex:Employee rdfs:subClassOf ex:Person .

# Query at any level works
SELECT ?employee WHERE { ?employee a ex:Employee }
# Finds CEOs, Executives, and direct Employees
```

### Roles and Permissions

```turtle
# Schema
ex:AdminRole rdfs:subClassOf ex:Role .
ex:AdminRole ex:grants ex:FullAccess .
ex:Role rdfs:domain ex:User .

# Data
ex:alice ex:hasRole ex:AdminRole .

# After materialization:
# - alice is a User (from domain)
# - alice's role grants FullAccess
```

### Part-Whole Relationships

```turtle
# Schema
ex:hasPart a owl:TransitiveProperty .
ex:partOf owl:inverseOf ex:hasPart .

# Data
ex:car ex:hasPart ex:engine .
ex:engine ex:hasPart ex:piston .

# After materialization:
# - car hasPart piston (transitive)
# - piston partOf car (inverse)
```

## Troubleshooting

### No Inferences Generated

Check that:
1. Schema triples are loaded (rdfs:subClassOf, etc.)
2. Correct profile is used
3. Data matches schema terms exactly (IRI spelling)

```elixir
# Verify schema is loaded
{:ok, results} = TripleStore.query(store, """
  SELECT ?sub ?super
  WHERE {
    ?sub rdfs:subClassOf ?super
  }
""")
IO.inspect(results)
```

### Too Many Inferences

Some patterns can explode:

```elixir
# Check total triples after materialization
{:ok, stats} = TripleStore.stats(store)
IO.puts("Total triples: #{stats.triple_count}")

# If unexpectedly high, review schema for:
# - Circular class hierarchies
# - owl:sameAs creating many equivalences
# - Transitive properties over large chains
```

### Slow Materialization

For large datasets:

```elixir
# Check iteration count
{:ok, stats} = TripleStore.materialize(store)
IO.puts("Iterations: #{stats.iterations}")

# High iteration count suggests:
# - Deep hierarchies
# - Many transitive relationships
# - Consider using simpler profile (:rdfs)
```

## Next Steps

- [Configuration & Performance](06-configuration.md) - Tuning reasoning performance
