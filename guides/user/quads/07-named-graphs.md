# Named Graphs (Quad Store)

This guide covers advanced named graph patterns and usage in a quad store.

> **Note**: This guide is for quad stores (`schema: :quad`) with named graphs. For triple stores, see the [Triple Store Data Management](../triples/02-data-management.md) guide.

## Overview

Named graphs (quads) add a fourth dimension to RDF data: the graph context. Each quad is `{graph, subject, predicate, object}`.

## When to Use Named Graphs

Named graphs are useful when you need to:

- **Multi-tenancy**: Separate data per tenant, user, or organization
- **Provenance tracking**: Track where data came from and when
- **Data isolation**: Keep different datasets separate for security
- **Incremental loading**: Add new data sources without affecting existing data
- **Graph-scoped reasoning**: Apply reasoning rules to specific graphs
- **Versioning**: Track different versions of the same data

## Opening a Quad Store

```elixir
# Open a quad store
{:ok, store} = TripleStore.open("./my_database", schema: :quad)
```

The default graph is represented by graph ID `0`. All graphs use numeric IDs internally for efficiency.

## Named Graph Patterns

### Multi-Tenancy Pattern

Separate tenant data into isolated graphs:

```elixir
defmodule TenantGraphs do
  @tenant_base "http://example.org/tenants/"

  def create_tenant_graph(store, tenant_id) do
    graph_iri = "#{@tenant_base}#{tenant_id}"

    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>

      INSERT DATA {
        GRAPH <#{graph_iri}> {
          ex:tenant_#{tenant_id} a ex:Tenant ;
                                ex:id "#{tenant_id}" ;
                                ex:createdAt "#{DateTime.utc_now()}"^^xsd:dateTime .
        }
      }
    """)
  end

  def get_tenant_data(store, tenant_id) do
    graph_iri = "#{@tenant_base}#{tenant_id}"

    TripleStore.query(store, """
      PREFIX ex: <http://example.org/>

      SELECT ?s ?p ?o
      WHERE {
        GRAPH <#{graph_iri}> {
          ?s ?p ?o
        }
      }
    """)
  end

  def delete_tenant_graph(store, tenant_id) do
    graph_iri = "#{@tenant_base}#{tenant_id}"

    TripleStore.update(store, """
      DROP GRAPH <#{graph_iri}>
    """)
  end
end
```

### Provenance Tracking Pattern

Track data sources and import history:

```elixir
defmodule ProvenanceGraphs do
  @provenance_base "http://example.org/provenance/"

  def import_with_provenance(store, data_file, source_id) do
    # Create provenance graph
    provenance_graph = "#{@provenance_base}import/#{source_id}"
    timestamp = DateTime.utc_now() |> DateTime.to_iso8601()

    # Record import metadata
    TripleStore.update(store, """
      PREFIX prov: <http://www.w3.org/ns/prov#>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      INSERT DATA {
        GRAPH <#{provenance_graph}> {
          <#{provenance_graph}> a prov:Activity ;
                                prov:startedAtTime "#{timestamp}"^^xsd:dateTime ;
                                prov:used "#{data_file}" ;
                                prov:wasAssociatedWith ex:ImportScript .
        }
      }
    """)

    # Load data into its own graph
    data_graph = "#{@provenance_base}data/#{source_id}"
    {:ok, count} = TripleStore.load(store, data_file)

    # Link data to provenance
    TripleStore.update(store, """
      PREFIX prov: <http://www.w3.org/ns/prov#>

      INSERT DATA {
        GRAPH <#{data_graph}> {
          ?data prov:wasGeneratedBy <#{provenance_graph}> .
        }
      }
    """)
  end

  def query_provenance(store, data_graph) do
    TripleStore.query(store, """
      PREFIX prov: <http://www.w3.org/ns/prov#>

      SELECT ?activity ?timestamp ?source
      WHERE {
        GRAPH ?data_graph {
          ?data prov:wasGeneratedBy ?activity .
        }
        GRAPH ?activity {
          ?activity prov:startedAtTime ?timestamp ;
                    prov:used ?source .
        }
      }
    """)
  end
end
```

### Versioning Pattern

Track different versions of data:

```elixir
defmodule VersionedGraphs do
  @version_base "http://example.org/data/"

  def save_version(store, dataset_id, data_triples) do
    timestamp = DateTime.utc_now()
    version = DateTime.to_unix(timestamp, :microsecond)
    version_graph = "#{@version_base}#{dataset_id}/v#{version}"

    # Insert versioned data
    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      INSERT DATA {
        GRAPH <#{version_graph}> {
          #{data_triples}
          <#{version_graph}> ex:version "#{version}" ;
                             ex:createdAt "#{DateTime.to_iso8601(timestamp)}"^^xsd:dateTime ;
                             ex:dataset "#{dataset_id}" .
        }
      }
    """)
  end

  def get_latest_version(store, dataset_id) do
    TripleStore.query(store, """
      PREFIX ex: <http://example.org/>

      SELECT ?graph ?createdAt
      WHERE {
        GRAPH ?graph {
          ?graph ex:dataset "#{dataset_id}" ;
                 ex:createdAt ?createdAt .
        }
      }
      ORDER BY DESC(?createdAt)
      LIMIT 1
    """)
  end

  def get_version_at_time(store, dataset_id, datetime) do
    TripleStore.query(store, """
      PREFIX ex: <http://example.org/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      SELECT ?graph
      WHERE {
        GRAPH ?graph {
          ?graph ex:dataset "#{dataset_id}" ;
                 ex:createdAt ?createdAt .
          FILTER (?createdAt <= "#{DateTime.to_iso8601(datetime)}"^^xsd:dateTime)
        }
      }
      ORDER BY DESC(?createdAt)
      LIMIT 1
    """)
  end
end
```

### Staging Pattern

Use staging graphs for data validation:

```elixir
defmodule StagingGraphs do
  def stage_data(store, staging_id, data_sparql) do
    staging_graph = "http://example.org/staging/#{staging_id}"

    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>

      INSERT DATA {
        GRAPH <#{staging_graph}> {
          #{data_sparql}
        }
      }
    """)
  end

  def validate_staged(store, staging_id, validation_query) do
    staging_graph = "http://example.org/staging/#{staging_id}"

    case TripleStore.query(store, validation_query) do
      {:ok, []} -> {:ok, :valid}
      {:ok, errors} -> {:error, errors}
      error -> error
    end
  end

  def promote_staged(store, staging_id, target_graph) do
    staging_graph = "http://example.org/staging/#{staging_id}"

    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>

      DELETE {
        GRAPH <#{target_graph}> {
          ?s ?p ?o
        }
      }
      INSERT {
        GRAPH <#{target_graph}> {
          ?s ?p ?o
        }
      }
      WHERE {
        GRAPH <#{staging_graph}> {
          ?s ?p ?o
        }
      }
    """)
  end

  def discard_staged(store, staging_id) do
    staging_graph = "http://example.org/staging/#{staging_id}"

    TripleStore.update(store, """
      DROP GRAPH <#{staging_graph}>
    """)
  end
end
```

### Access Control Pattern

Graph-level access control:

```elixir
defmodule AccessControlledGraphs do
  def create_user_graph(store, user_id) do
    user_graph = "http://example.org/users/#{user_id}"

    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>
      PREFIX acl: <http://www.w3.org/ns/auth/acl#>

      INSERT DATA {
        GRAPH <#{user_graph}> {
          <#{user_graph}> acl:owner "#{user_id}" ;
                         acl:created_at "#{DateTime.utc_now()}" .
        }
      }
    """)
  end

  def check_access(store, user_id, graph_iri) do
    TripleStore.query(store, """
      PREFIX acl: <http://www.w3.org/ns/auth/acl#>

      ASK {
        GRAPH <#{graph_iri}> {
          <#{graph_iri}> acl:owner "#{user_id}"
        }
      }
    """)
  end

  def grant_access(store, graph_iri, user_id, permission) do
    acl_graph = "http://example.org/acl/#{graph_iri}"

    TripleStore.update(store, """
      PREFIX acl: <http://www.w3.org/ns/auth/acl#>

      INSERT DATA {
        GRAPH <#{acl_graph}> {
          _:auth acl:accessTo <#{graph_iri}> ;
                 acl:agent "#{user_id}" ;
                 acl:mode #{permission} .
        }
      }
    """)
  end
end
```

## Cross-Graph Queries

### Join Across Graphs

Combine data from multiple graphs:

```elixir
# Join person data from employee graph with department from org chart
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?person ?name ?department
  WHERE {
    GRAPH ex:employees {
      ?person ex:name ?name .
    }
    GRAPH ex:orgchart {
      ?person ex:worksIn ?department .
    }
  }
""")
```

### Aggregate Across Graphs

Get statistics per graph:

```elixir
# Count triples per graph
{:ok, results} = TripleStore.query(store, """
  SELECT ?g (COUNT(*) AS ?count)
  WHERE {
    GRAPH ?g {
      ?s ?p ?o
    }
  }
  GROUP BY ?g
  ORDER BY DESC(?count)
""")
```

### Graph Pattern Matching

Find graphs matching specific patterns:

```elixir
# Find all graphs containing Person instances
{:ok, results} = TripleStore.query(store, """
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  SELECT ?g
  WHERE {
    GRAPH ?g {
      ?s a foaf:Person .
    }
  }
  GROUP BY ?g
""")
```

### Detect Duplicate Data Across Graphs

Find quads that appear in multiple graphs:

```elixir
{:ok, results} = TripleStore.query(store, """
  PREFIX ex: <http://example.org/>

  SELECT ?s ?p ?o (COUNT(?g) AS ?graph_count)
  WHERE {
    GRAPH ?g {
      ?s ?p ?o
    }
  }
  GROUP BY ?s ?p ?o
  HAVING (?graph_count > 1)
""")
```

## Graph Management Operations

### List All Graphs

```elixir
{:ok, graphs} = TripleStore.list_graphs(store)

Enum.each(graphs, fn graph_id ->
  IO.puts("Graph: #{graph_id}")
end)
```

### Get Graph Statistics

```elixir
{:ok, stats} = TripleStore.Statistics.graph_statistics(store, graph_id)

IO.puts("""
Graph #{graph_id}:
  Quad count: #{stats.quad_count}
  Unique subjects: #{stats.unique_subjects}
  Unique predicates: #{stats.unique_predicates}
""")
```

### Copy Graph

```elixir
def copy_graph(store, source_graph, target_graph) do
  TripleStore.update(store, """
    PREFIX ex: <http://example.org/>

    INSERT {
      GRAPH <#{target_graph}> {
        ?s ?p ?o
      }
    }
    WHERE {
      GRAPH <#{source_graph}> {
        ?s ?p ?o
      }
    }
  """)
end
```

### Merge Graphs

```elixir
def merge_graphs(store, source_graphs, target_graph) do
  graph_patterns = source_graphs
    |> Enum.map(fn g -> "GRAPH <#{g}> { ?s ?p ?o }" end)
    |> Enum.join(" UNION\n")

  TripleStore.update(store, """
    INSERT {
      GRAPH <#{target_graph}> {
        ?s ?p ?o
      }
    }
    WHERE {
      #{graph_patterns}
    }
  """)
end
```

### Archive Old Graphs

```elixir
def archive_graph_by_date(store, graph_pattern, days_old) do
  cutoff_date = DateTime.add(DateTime.utc_now(), -days_old, :day)

  TripleStore.query(store, """
    PREFIX ex: <http://example.org/>

    SELECT ?g
    WHERE {
      GRAPH ?g {
        ?g ex:createdAt ?date .
        FILTER (?date < "#{DateTime.to_iso8601(cutoff_date)}"^^xsd:dateTime)
      }
    }
  """)
  |> then(fn {:ok, graphs} ->
    Enum.each(graphs, fn %{"g" => graph_iri} ->
      archive_graph(store, graph_iri)
    end)
  end)
end

defp archive_graph(store, graph_iri) do
  archive_graph = String.replace(graph_iri, "/data/", "/archive/")

  TripleStore.update(store, """
    PREFIX ex: <http://example.org/>

    DELETE {
      GRAPH <#{graph_iri}> {
        ?s ?p ?o
      }
    }
    INSERT {
      GRAPH <#{archive_graph}> {
        ?s ?p ?o
      }
    }
    WHERE {
      GRAPH <#{graph_iri}> {
        ?s ?p ?o
      }
    }
  """)
end
```

## Default Graph Handling

### Query Default Graph

```elixir
# Queries only the default graph (graph ID 0)
{:ok, results} = TripleStore.query(store, """
  SELECT ?s ?p ?o
  WHERE { ?s ?p ?o }
""")
```

### Union of All Graphs

```elixir
# Query all graphs including default
{:ok, results} = TripleStore.query(store, """
  SELECT ?s ?p ?o
  WHERE {
    { ?s ?p ?o }
    UNION
    { GRAPH ?g { ?s ?p ?o } }
  }
""")
```

### Set Default Graph

When loading data without a graph context, it goes to the default graph:

```elixir
# Turtle/N-Triples load into default graph
{:ok, count} = TripleStore.load(store, "data.ttl")

# SPARQL UPDATE without GRAPH goes to default graph
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:alice ex:name "Alice" .
  }
""")
```

## Best Practices

### 1. Use Structured Graph URIs

```elixir
# Good: Hierarchical, readable
"http://example.org/tenants/acme/data"
"http://example.org/provenance/import/2024-01-20"

# Avoid: Random or meaningless URIs
"http://example.org/graph_abc123"
```

### 2. Separate Data from Metadata

```elixir
# Data graph
"http://example.org/data/source1"

# Metadata/provenance graph
"http://example.org/metadata/source1"
```

### 3. Use Graph Prefixes for Organization

```elixir
@base "http://example.org/"

# Data graphs
"/data/tenant1"
"/data/tenant2"

# Staging graphs
"/staging/import_001"
"/staging/import_002"

# Archive graphs
"/archive/2024/01"
"/archive/2024/02"
```

### 4. Document Your Graph Structure

```elixir
# Create a graph registry
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:graph_registry {
      ex:graph_registry a ex:Registry ;
                       ex:graphType "data" ;
                       ex:graphPrefix "http://example.org/data/" .

      ex:staging_registry a ex:Registry ;
                       ex:graphType "staging" ;
                       ex:graphPrefix "http://example.org/staging/" .
    }
  }
""")
```

### 5. Query Specific Graphs When Possible

```elixir
# Better: Specific graph
GRAPH ex:source1 { ?s ?p ?o }

# Avoid: All graphs (slower)
GRAPH ?g { ?s ?p ?o }
```

## Complete Example

```elixir
defmodule MyApp.NamedGraphsApp do
  alias TripleStore

  def setup do
    {:ok, store} = TripleStore.open("./app_database", schema: :quad)

    # Create graph structure
    create_graph_structure(store)

    # Load data
    load_sample_data(store)

    # Query examples
    query_examples(store)

    :ok = TripleStore.close(store)
  end

  defp create_graph_structure(store) do
    # Schema graph
    TripleStore.update(store, """
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      PREFIX ex: <http://example.org/>

      INSERT DATA {
        GRAPH ex:schema {
          ex:Person rdfs:subClassOf ex:Agent .
          ex:Employee rdfs:subClassOf ex:Person .
        }
      }
    """)

    # Tenant graphs
    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>

      INSERT DATA {
        GRAPH ex:tenant_a {
          ex:tenant_a a ex:Tenant ;
                     ex:name "Tenant A" .
        }

        GRAPH ex:tenant_b {
          ex:tenant_b a ex:Tenant ;
                     ex:name "Tenant B" .
        }
      }
    """)
  end

  defp load_sample_data(store) do
    # Load data per tenant
    TripleStore.update(store, """
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>
      PREFIX ex: <http://example.org/>

      INSERT DATA {
        GRAPH ex:tenant_a {
          ex:alice a foaf:Person ;
                  foaf:name "Alice" ;
                  ex:department "Engineering" .
        }

        GRAPH ex:tenant_b {
          ex:bob a foaf:Person ;
                foaf:name "Bob" ;
                ex:department "Sales" .
        }
      }
    """)
  end

  defp query_examples(store) do
    # Query specific tenant
    {:ok, tenant_a_people} = TripleStore.query(store, """
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      SELECT ?name
      WHERE {
        GRAPH ex:tenant_a {
          ?s foaf:name ?name .
        }
      }
    """)

    IO.puts("Tenant A people: #{inspect(tenant_a_people)}")

    # Query all tenants
    {:ok, all_people} = TripleStore.query(store, """
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      SELECT ?g ?name
      WHERE {
        GRAPH ?g {
          ?s foaf:name ?name .
        }
      }
    """)

    IO.puts("All people: #{inspect(all_people)}")

    # List all graphs
    {:ok, graphs} = TripleStore.list_graphs(store)
    IO.puts("All graphs: #{inspect(graphs)}")
  end
end
```

## Next Steps

This concludes the quad store guide series. For implementation details, see:

- [Developer Guides](../../developer/README.md) - Implementation details
- [API Reference](../../api/README.md) - Complete API documentation
