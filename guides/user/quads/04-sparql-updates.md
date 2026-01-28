# SPARQL Updates (Quad Store)

This guide covers modifying data using SPARQL UPDATE operations in a quad store with named graphs.

> **Note**: This guide is for quad stores (`schema: :quad`) with named graphs. For triple stores, see the [Triple Store SPARQL Updates](../triples/04-sparql-updates.md) guide.

## Update Basics

### Executing Updates

```elixir
# Open quad store
{:ok, store} = TripleStore.open("./my_database", schema: :quad)

# Execute update
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:people {
      ex:alice ex:name "Alice" .
    }
  }
""")

IO.puts("Modified #{count} quads")
```

### Update Types

| Operation | Description |
|-----------|-------------|
| INSERT DATA | Add specific quads |
| DELETE DATA | Remove specific quads |
| INSERT ... WHERE | Add quads based on patterns |
| DELETE ... WHERE | Remove quads based on patterns |
| DELETE/INSERT ... WHERE | Modify quads atomically |
| CLEAR | Remove all quads |
| CREATE/DROP GRAPH | Graph management |

## INSERT DATA

Add explicit quads:

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  INSERT DATA {
    GRAPH ex:people {
      ex:alice a foaf:Person ;
               foaf:name "Alice Smith" ;
               foaf:age 30 ;
               foaf:mbox <mailto:alice@example.org> .

      ex:bob a foaf:Person ;
             foaf:name "Bob Jones" ;
             foaf:age 25 .
    }
  }
""")
```

### INSERT DATA into Named Graph

```elixir
# Open quad store first
{:ok, store} = TripleStore.open("./my_database", schema: :quad)

# Insert into default graph
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:alice ex:name "Alice" .
  }
""")

# Insert into named graph
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:source1 {
      ex:bob ex:name "Bob" ;
             ex:email "bob@example.org" .
    }
  }
""")
```

## DELETE DATA

Remove explicit quads:

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE DATA {
    GRAPH ex:source1 {
      ex:bob ex:email "bob@example.org" .
    }
  }
""")
```

### DELETE DATA from Named Graph

```elixir
# Delete from a specific graph
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE DATA {
    GRAPH ex:source1 {
      ex:bob ex:email "bob@example.org" .
    }
  }
""")
```

## INSERT ... WHERE with GRAPH

```elixir
# Copy triples that match a pattern into a named graph
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  INSERT {
    GRAPH ex:managers {
      ?person ex:isManager true .
    }
  }
  WHERE {
    GRAPH ex:employees {
      ?person foaf:holdsPosition "Manager" .
    }
  }
""")
```

## DELETE ... WHERE with GRAPH

```elixir
# Delete triples from a specific graph matching a pattern
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    GRAPH ex:temp {
      ?s ?p ?o
    }
  }
  WHERE {
    GRAPH ex:temp {
      ?s ex:createdAt ?ts .
      ?s ?p ?o .
      FILTER (?ts < "2024-01-01T00:00:00Z"^^xsd:dateTime)
    }
  }
""")
```

## DELETE/INSERT ... WHERE with GRAPH

Move data between graphs atomically:

```elixir
# Move data from staging to production
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    GRAPH ex:staging {
      ?s ?p ?o
    }
  }
  INSERT {
    GRAPH ex:production {
      ?s ?p ?o
    }
  }
  WHERE {
    GRAPH ex:staging {
      ?s ex:approved true .
      ?s ?p ?o
    }
  }
""")
```

## Graph Management

### CREATE GRAPH

Create an empty named graph:

```elixir
{:ok, _} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  CREATE GRAPH ex:new_graph
""")
```

Note: In TripleStore, graphs are created implicitly when you insert data into them. CREATE GRAPH ensures the graph exists even if empty.

### DROP GRAPH

Remove a named graph and all its quads:

```elixir
{:ok, _} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DROP GRAPH ex:old_graph
""")
```

### CLEAR GRAPH

Remove all quads from a graph but keep the graph:

```elixir
# Method 1: DELETE all triples in the graph
{:ok, count} = TripleStore.update(store, """
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

# Method 2: CLEAR specific graph
{:ok, _} = TripleStore.update(store, "CLEAR GRAPH ex:graph1")
```

### COPY Graphs

Copy all quads from one graph to another:

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT {
    GRAPH ex:graph_backup {
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

### MOVE Graphs

Move all quads from one graph to another (source is cleared):

```elixir
{:ok, count} = TripleStore.update(store, """
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

### ADD Graphs

Copy quads from one graph to another (source retained):

```elixir
# ADD is equivalent to COPY - both graphs have the data after
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT {
    GRAPH ex:graph_target {
      ?s ?p ?o
    }
  }
  WHERE {
    GRAPH ex:graph_source {
      ?s ?p ?o
    }
  }
""")
```

## INSERT into Multiple Graphs

```elixir
# Distribute data to multiple graphs based on conditions
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  INSERT {
    GRAPH ?targetGraph {
      ?person foaf:name ?name .
    }
  }
  WHERE {
    ?person foaf:name ?name ;
            ex:role ?role .

    BIND(
      IF(?role = "admin", ex:admin_users,
        IF(?role = "manager", ex:manager_users, ex:regular_users))
      AS ?targetGraph
    )
  }
""")
```

## Graph-Scoped Update Patterns

### Archive Old Data by Date

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>
  PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

  DELETE {
    GRAPH ex:active {
      ?log ?p ?o
    }
  }
  INSERT {
    GRAPH ex:archive {
      ?log ?p ?o
    }
  }
  WHERE {
    GRAPH ex:active {
      ?log a ex:LogEntry ;
           ex:timestamp ?ts ;
           ?p ?o .
      FILTER (?ts < "2023-01-01T00:00:00Z"^^xsd:dateTime)
    }
  }
""")
```

### Merge Data from Multiple Graphs

```elixir
# Combine data from multiple source graphs
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT {
    GRAPH ex:merged {
      ?s ?p ?o
    }
  }
  WHERE {
    { GRAPH ex:source1 { ?s ?p ?o } }
    UNION
    { GRAPH ex:source2 { ?s ?p ?o } }
    UNION
    { GRAPH ex:source3 { ?s ?p ?o } }
  }
""")
```

### Cross-Graph Updates

```elixir
# Update based on data from multiple graphs
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT {
    GRAPH ex:enriched {
      ?person ex:hasManager ?manager .
    }
  }
  WHERE {
    GRAPH ex:employees {
      ?person a ex:Employee .
    }
    GRAPH ex:orgchart {
      ?person ex:reportsTo ?manager .
      ?manager a ex:Manager .
    }
  }
""")
```

## Safe Update Patterns

### Preview Before Delete

```elixir
defmodule SafeUpdater do
  def safe_delete_from_graph(store, graph_iri, pattern_sparql) do
    # Preview
    {:ok, preview} = TripleStore.query(store, """
      SELECT (COUNT(*) AS ?count)
      WHERE {
        GRAPH #{graph_iri} {
          #{pattern_sparql}
        }
      }
    """)

    count = hd(preview)["count"]
    IO.puts("Will delete #{count} quads from #{graph_iri}")

    if count > 0 and confirm?() do
      {:ok, deleted} = TripleStore.update(store, """
        DELETE {
          GRAPH #{graph_iri} {
            #{pattern_sparql}
          }
        }
        WHERE {
          GRAPH #{graph_iri} {
            #{pattern_sparql}
          }
        }
      """)

      # Rematerialize if using reasoning
      {:ok, _} = TripleStore.materialize(store)

      {:ok, deleted}
    else
      {:cancelled, 0}
    end
  end

  defp confirm? do
    IO.gets("Proceed? [y/N] ") |> String.trim() |> String.downcase() == "y"
  end
end
```

### Backup Before Update

```elixir
defmodule UpdateWithBackup do
  def update_safely(store, update_sparql, backup_dir) do
    # 1. Create backup
    {:ok, backup} = TripleStore.Backup.create(
      store,
      "#{backup_dir}/pre_update_#{DateTime.utc_now() |> DateTime.to_iso8601()}"
    )

    try do
      # 2. Execute update
      {:ok, count} = TripleStore.update(store, update_sparql)
      {:ok, %{updated: count, backup_path: backup.path}}
    rescue
      e ->
        IO.puts("Update failed! Backup available at: #{backup.path}")
        reraise e, __STACKTRACE__
    end
  end
end
```

## Example Workflows

### Multi-Tenant Data Loading

```elixir
def load_tenant_data(store, tenant_id, file_path) do
  graph_iri = "http://example.org/tenants/#{tenant_id}"

  # Load data into tenant-specific graph
  {:ok, content} = File.read(file_path)

  {:ok, count} = TripleStore.load_string(store, content, :nquads)

  # Verify data was loaded to the correct graph
  {:ok, results} = TripleStore.query(store, """
    PREFIX ex: <http://example.org/>

    SELECT (COUNT(*) AS ?count)
    WHERE {
      GRAPH <#{graph_iri}> {
        ?s ?p ?o
      }
    }
  """)

  IO.puts("Loaded #{count} quads to #{graph_iri}")
end
```

### Provenance Tracking

```elixir
def load_with_provenance(store, data_file, source_id) do
  # Create a provenance graph for this import
  provenance_graph = "http://example.org/provenance/import#{source_id}"

  # Load data with provenance information
  TripleStore.update(store, """
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {
      GRAPH #{provenance_graph} {
        <#{provenance_graph}> prov:wasGeneratedBy "ImportScript" .
        <#{provenance_graph}> prov:endedAtTime "#{DateTime.utc_now()}"^^xsd:dateTime .
        <#{provenance_graph}> prov:used "#{data_file}" .
      }
    }
  """)

  # Load actual data into a separate graph
  TripleStore.load(store, data_file)
end
```

### Graph Isolation

```elixir
defmodule TenantManager do
  def create_tenant(store, tenant_id) do
    graph_iri = "http://example.org/tenants/#{tenant_id}"

    # Initialize tenant graph with schema
    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

      INSERT DATA {
        GRAPH <#{graph_iri}> {
          ex:Resource rdfs:subClassOf ex:BaseResource .
        }
      }
    """)

    {:ok, graph_iri}
  end

  def delete_tenant(store, tenant_id) do
    graph_iri = "http://example.org/tenants/#{tenant_id}"

    # Drop entire tenant graph
    TripleStore.update(store, """
      DROP GRAPH <#{graph_iri}>
    """)

    :ok
  end

  def archive_tenant(store, tenant_id) do
    source_graph = "http://example.org/tenants/#{tenant_id}"
    archive_graph = "http://example.org/archive/tenants/#{tenant_id}"

    TripleStore.update(store, """
      PREFIX ex: <http://example.org/>

      DELETE {
        GRAPH <#{source_graph}> {
          ?s ?p ?o
        }
      }
      INSERT {
        GRAPH <#{archive_graph}> {
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
end
```

## Handling Errors

```elixir
case TripleStore.update(store, update_sparql) do
  {:ok, count} ->
    Logger.info("Updated #{count} quads")

  {:error, {:parse_error, message}} ->
    Logger.error("SPARQL syntax error: #{message}")

  {:error, :timeout} ->
    Logger.error("Update timed out")

  {:error, reason} ->
    Logger.error("Update failed: #{inspect(reason)}")
end
```

## Reasoning Considerations

After significant updates, you may need to rematerialize inferences:

```elixir
# After adding schema-affecting triples
{:ok, _} = TripleStore.update(store, """
  INSERT DATA {
    GRAPH ex:schema {
      ex:Manager rdfs:subClassOf ex:Employee .
    }
  }
""")

# Rematerialize to derive new inferences
{:ok, stats} = TripleStore.materialize(store,
  profile: :owl2rl,
  scope: :global
)
IO.puts("Derived #{stats.total_derived} new quads")
```

## Next Steps

- [Reasoning](05-reasoning.md) - Graph-scoped inference
- [Configuration & Performance](06-configuration.md) - Quad-specific tuning
- [Named Graphs](07-named-graphs.md) - Advanced named graph patterns
