# Reasoning (Quad Store)

This guide covers using OWL 2 RL reasoning with graph-scoped inference in a quad store.

> **Note**: This guide is for quad stores (`schema: :quad`) with graph-scoped reasoning. For triple stores, see the [Triple Store Reasoning](../triples/05-reasoning.md) guide.

## What is Graph-Scoped Reasoning?

In a quad store, reasoning can be scoped to control how inferences are computed:

- **Local reasoning**: Each graph reasons independently. Inferences stay within their source graph.
- **Global reasoning**: All graphs are merged before reasoning. Inferences can derive from data across multiple graphs.

## Reasoning Profiles

TripleStore supports three reasoning profiles:

| Profile | Description | Use Case |
|---------|-------------|----------|
| `:rdfs` | RDFS entailment only | Simple class/property hierarchies |
| `:owl2rl` | OWL 2 RL (includes RDFS) | Full semantic reasoning |
| `:all` | All available rules | Maximum inference |

## Local vs Global Reasoning

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

## When to Use Each Scope

| Scope | Use Case |
|-------|----------|
| `:local` | Multi-tenant data, isolated datasets, provenance tracking |
| `:global` | Unified ontology, shared schema across graphs |

## Local Reasoning Example

```elixir
# Open quad store
{:ok, store} = TripleStore.open("./my_database", schema: :quad)

# Load schema into separate graphs
TripleStore.update(store, """
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:tenant1_schema {
      ex:Student rdfs:subClassOf ex:Person .
    }

    GRAPH ex:tenant2_schema {
      ex:Student rdfs:subClassOf ex:Person .
    }
  }
""")

# Load data into different graphs
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:tenant1 {
      ex:alice a ex:Student .
    }

    GRAPH ex:tenant2 {
      ex:bob a ex:Student .
    }
  }
""")

# Materialize with local scope
config = ReasoningConfig.new(profile: :owl2rl, scope: :local)
{:ok, stats} = TripleStore.materialize(store, config: config)

# Query tenant1 - alice is now also a Person in tenant1's graph
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?type
  WHERE {
    GRAPH ex:tenant1 {
      ex:alice a ?type
    }
  }
""")
# Results: [Student, Person]
```

## Global Reasoning Example

```elixir
# With global scope, all graphs are merged for reasoning
config = ReasoningConfig.new(profile: :owl2rl, scope: :global)
{:ok, stats} = TripleStore.materialize(store, config: config)

# Inferences can derive from data across any graph
# Useful when schema is in one graph and data in others
```

## Shared Schema Pattern

For global reasoning, place schema in a dedicated graph:

```elixir
# Load shared schema
TripleStore.update(store, """
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:schema {
      ex:Student rdfs:subClassOf ex:Person .
      ex:Employee rdfs:subClassOf ex:Person .
      ex:Manager rdfs:subClassOf ex:Employee .
    }
  }
""")

# Load data into separate graphs
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:tenant1 {
      ex:alice a ex:Student .
      ex:charlie a ex:Manager .
    }

    GRAPH ex:tenant2 {
      ex:bob a ex:Student .
      ex:david a ex:Employee .
    }
  }
""")

# Use global scope to apply schema to all graphs
config = ReasoningConfig.new(profile: :owl2rl, scope: :global)
{:ok, _} = TripleStore.materialize(store, config: config)

# All graphs now have access to the shared schema inferences
```

## Multi-Tenant Reasoning Pattern

```elixir
defmodule TenantReasoner do
  def materialize_tenant(store, tenant_id) do
    graph_iri = "http://example.org/tenants/#{tenant_id}"

    # Materialize only this tenant's graph
    {:ok, stats} = TripleStore.materialize_graph(store,
      graph_id: tenant_id,
      config: ReasoningConfig.new(
        profile: :owl2rl,
        scope: :local
      )
    )

    {:ok, stats}
  end

  def materialize_all_tenants(store) do
    # Each tenant reasons independently
    config = ReasoningConfig.new(profile: :owl2rl, scope: :local)
    {:ok, stats} = TripleStore.materialize(store, config: config)
  end
end
```

## Materializing Specific Graphs

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

## Schema Placement Patterns

### Local Reasoning Schema

Place schema triples in the same graph as the data:

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

### Global Reasoning Schema

Place schema in a dedicated graph:

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

## Hybrid Reasoning

Combine TBox sharing with local reasoning:

```elixir
# Shared TBox (terminology) graph
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:tbox {
      ex:Person rdfs:subClassOf ex:Agent .
      ex:Employee rdfs:subClassOf ex:Person .
    }
  }
""")

# Configure with TBox sharing
config = ReasoningConfig.new(
  profile: :owl2rl,
  scope: :local,
  tbox_graph: "http://example.org/tbox"
)

{:ok, _} = TripleStore.materialize(store, config: config)
```

## Materializing Inferences

### Basic Materialization

```elixir
# Default (global scope)
{:ok, stats} = TripleStore.materialize(store)

# Specific profile
{:ok, stats} = TripleStore.materialize(store, profile: :rdfs)

# With configuration
{:ok, stats} = TripleStore.materialize(store,
  config: ReasoningConfig.new(
    profile: :owl2rl,
    scope: :local
  )
)
```

### Checking Results

```elixir
IO.puts("Iterations: #{stats.iterations}")
IO.puts("Derived quads: #{stats.total_derived}")
IO.puts("Duration: #{stats.duration_ms}ms")
```

## Checking Reasoning Status

```elixir
# Get overall status
{:ok, status} = TripleStore.reasoning_status(store)

IO.puts("State: #{status.state}")
IO.puts("Profile: #{status.profile}")
IO.puts("Scope: #{status.scope}")
IO.puts("Derived facts: #{status.derived_count}")

# Get per-graph status
{:ok, graph_statuses} = TripleStore.reasoning_status(store, graph_id: 1)
```

## Best Practices

### 1. Choose the Right Scope

```elixir
# Multi-tenant data - use local
config = ReasoningConfig.new(profile: :owl2rl, scope: :local)

# Shared ontology - use global
config = ReasoningConfig.new(profile: :owl2rl, scope: :global)
```

### 2. Schema Placement

**For local reasoning**, place schema with data:

```elixir
# Each tenant has their own schema
GRAPH ex:tenant1 {
  ex:LocalClass rdfs:subClassOf ex:Thing .
  ex:alice a ex:LocalClass .
}
```

**For global reasoning**, use a shared schema graph:

```elixir
# Shared schema
GRAPH ex:schema {
  ex:Class rdfs:subClassOf ex:Thing .
}

# Data in separate graphs
GRAPH ex:tenant1 {
  ex:instance a ex:Class .
}
```

### 3. Performance Considerations

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

### 4. Monitor Performance

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

### Multi-Tenant Type Hierarchy

```turtle
# Tenant 1 schema
@prefix ex: <http://example.org/tenant1/>

GRAPH ex:tenant1_schema {
  ex:Customer rdfs:subClassOf ex:Person .
  ex:VIPCustomer rdfs:subClassOf ex:Customer .
}

# Tenant 2 schema
GRAPH ex:tenant2_schema {
  ex:Client rdfs:subClassOf ex:Person .
  ex:EnterpriseClient rdfs:subClassOf ex:Client .
}
```

### Provenance Tracking

```turtle
# Track where inferences came from
@prefix prov: <http://www.w3.org/ns/prov#>
@prefix ex: <http://example.org/>

GRAPH ex:data_source1 {
  ex:alice a ex:Student .
}

GRAPH ex:inferred {
  ex:alice a ex:Person ;
         prov:wasDerivedFrom ex:data_source1 .
}
```

## Next Steps

- [Configuration & Performance](06-configuration.md) - Tuning reasoning performance
- [Named Graphs](07-named-graphs.md) - Advanced named graph patterns
