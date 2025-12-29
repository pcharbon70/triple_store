# SPARQL Updates

This guide covers modifying data using SPARQL UPDATE operations.

## Update Basics

### Executing Updates

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:alice ex:name "Alice" .
  }
""")

IO.puts("Modified #{count} triples")
```

### Update Types

| Operation | Description |
|-----------|-------------|
| INSERT DATA | Add specific triples |
| DELETE DATA | Remove specific triples |
| INSERT ... WHERE | Add triples based on patterns |
| DELETE ... WHERE | Remove triples based on patterns |
| DELETE/INSERT ... WHERE | Modify triples atomically |
| CLEAR | Remove all triples |
| DROP | Remove graph (same as CLEAR for default graph) |

## INSERT DATA

Add explicit triples:

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  INSERT DATA {
    ex:alice a foaf:Person ;
             foaf:name "Alice Smith" ;
             foaf:age 30 ;
             foaf:mbox <mailto:alice@example.org> .

    ex:bob a foaf:Person ;
           foaf:name "Bob Jones" ;
           foaf:age 25 .
  }
""")
```

### Tips for INSERT DATA

- No variables allowed - only explicit values
- Multiple triples can be inserted at once
- Use semicolon for multiple predicates on same subject

## DELETE DATA

Remove explicit triples:

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  DELETE DATA {
    ex:alice foaf:age 30 .
  }
""")
```

### Tips for DELETE DATA

- Triple must exist exactly as specified
- No variables allowed
- Returns count of triples actually deleted

## INSERT ... WHERE

Add triples based on patterns:

```elixir
# Create inverse relationships
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT {
    ?b ex:isKnownBy ?a .
  }
  WHERE {
    ?a ex:knows ?b .
  }
""")
```

### Use Cases

**Add computed values:**

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT {
    ?person ex:fullName ?fullName .
  }
  WHERE {
    ?person ex:firstName ?first .
    ?person ex:lastName ?last .
    BIND(CONCAT(?first, " ", ?last) AS ?fullName)
  }
""")
```

**Add type based on property:**

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT {
    ?person a ex:Employee .
  }
  WHERE {
    ?person ex:employeeId ?id .
    FILTER NOT EXISTS { ?person a ex:Employee }
  }
""")
```

## DELETE ... WHERE

Remove triples matching patterns:

```elixir
# Delete all triples about inactive users
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    ?user ?p ?o .
  }
  WHERE {
    ?user ex:status "inactive" ;
          ?p ?o .
  }
""")
```

### Common Patterns

**Delete all triples for a subject:**

```elixir
{:ok, count} = TripleStore.update(store, """
  DELETE WHERE {
    <http://example.org/alice> ?p ?o
  }
""")
```

**Delete by type:**

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    ?item ?p ?o
  }
  WHERE {
    ?item a ex:TemporaryData ;
          ?p ?o .
  }
""")
```

**Delete old data:**

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>
  PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

  DELETE {
    ?log ?p ?o
  }
  WHERE {
    ?log a ex:LogEntry ;
         ex:timestamp ?ts ;
         ?p ?o .
    FILTER (?ts < "2024-01-01T00:00:00Z"^^xsd:dateTime)
  }
""")
```

## DELETE/INSERT ... WHERE

Modify data atomically (update in place):

```elixir
# Change email address
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    ex:alice ex:email ?oldEmail .
  }
  INSERT {
    ex:alice ex:email "newalice@example.org" .
  }
  WHERE {
    ex:alice ex:email ?oldEmail .
  }
""")
```

### Update Patterns

**Update a property value:**

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE { ?person ex:age ?oldAge }
  INSERT { ?person ex:age ?newAge }
  WHERE {
    ?person ex:age ?oldAge .
    BIND(?oldAge + 1 AS ?newAge)
  }
""")
```

**Replace type:**

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE { ?item a ex:Draft }
  INSERT { ?item a ex:Published }
  WHERE {
    ?item a ex:Draft ;
          ex:approved true .
  }
""")
```

**Rename predicate:**

```elixir
{:ok, count} = TripleStore.update(store, """
  PREFIX old: <http://old.example.org/>
  PREFIX new: <http://new.example.org/>

  DELETE { ?s old:name ?value }
  INSERT { ?s new:label ?value }
  WHERE {
    ?s old:name ?value
  }
""")
```

## CLEAR and DROP

Remove all triples:

```elixir
# Clear default graph
{:ok, _} = TripleStore.update(store, "CLEAR DEFAULT")

# CLEAR ALL is equivalent for single-graph stores
{:ok, _} = TripleStore.update(store, "CLEAR ALL")
```

> **Warning**: CLEAR operations remove all triples. Use with caution!

## Shorthand: DELETE WHERE

When DELETE and WHERE patterns are identical:

```elixir
# Longhand
"""
DELETE { ?s ?p ?o }
WHERE { ?s ?p ?o . FILTER ... }
"""

# Shorthand (equivalent)
"""
DELETE WHERE { ?s ?p ?o . FILTER ... }
"""
```

## Batch Updates

### Processing in Batches

For large updates, process in batches to avoid memory issues:

```elixir
defmodule BatchUpdater do
  def delete_old_logs(store, cutoff_date) do
    batch_size = 10_000
    total_deleted = delete_batch(store, cutoff_date, batch_size, 0)
    {:ok, total_deleted}
  end

  defp delete_batch(store, cutoff_date, batch_size, acc) do
    {:ok, count} = TripleStore.update(store, """
      PREFIX ex: <http://example.org/>
      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

      DELETE {
        ?log ?p ?o
      }
      WHERE {
        SELECT ?log ?p ?o
        WHERE {
          ?log a ex:LogEntry ;
               ex:timestamp ?ts ;
               ?p ?o .
          FILTER (?ts < "#{cutoff_date}"^^xsd:dateTime)
        }
        LIMIT #{batch_size}
      }
    """)

    if count > 0 do
      IO.puts("Deleted #{count} triples...")
      delete_batch(store, cutoff_date, batch_size, acc + count)
    else
      acc
    end
  end
end
```

## Safe Update Patterns

### Preview Before Delete

```elixir
defmodule SafeUpdater do
  def safe_delete(store, where_clause) do
    # Preview
    {:ok, preview} = TripleStore.query(store, """
      SELECT (COUNT(*) AS ?count)
      WHERE { #{where_clause} }
    """)

    count = hd(preview)["count"]
    IO.puts("Will delete #{count} triples")

    if count > 0 and confirm?() do
      {:ok, deleted} = TripleStore.update(store, """
        DELETE WHERE { #{where_clause} }
      """)
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
    {:ok, backup} = TripleStore.backup(store, "#{backup_dir}/pre_update")

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

### Transactional Updates

Multiple updates that should succeed or fail together:

```elixir
defmodule TransactionalUpdate do
  def transfer_ownership(store, from, to, item) do
    # Verify preconditions
    {:ok, check} = TripleStore.query(store, """
      ASK {
        <#{from}> ex:owns <#{item}>
      }
    """)

    unless check do
      {:error, :not_owned_by_source}
    else
      # Perform atomic update
      {:ok, _} = TripleStore.update(store, """
        PREFIX ex: <http://example.org/>

        DELETE { <#{from}> ex:owns <#{item}> }
        INSERT { <#{to}> ex:owns <#{item}> }
        WHERE { <#{from}> ex:owns <#{item}> }
      """)

      {:ok, :transferred}
    end
  end
end
```

## Handling Errors

```elixir
case TripleStore.update(store, update_sparql) do
  {:ok, count} ->
    Logger.info("Updated #{count} triples")

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
    ex:Manager rdfs:subClassOf ex:Employee .
  }
""")

# Rematerialize to derive new inferences
{:ok, stats} = TripleStore.materialize(store, profile: :owl2rl)
IO.puts("Derived #{stats.total_derived} new triples")
```

See the [Reasoning guide](05-reasoning.md) for more details.

## Example Workflows

### User Registration

```elixir
def register_user(store, username, email) do
  user_uri = "http://example.org/users/#{username}"

  {:ok, _} = TripleStore.update(store, """
    PREFIX ex: <http://example.org/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT DATA {
      <#{user_uri}> a foaf:Person, ex:User ;
                    foaf:accountName "#{username}" ;
                    foaf:mbox <mailto:#{email}> ;
                    ex:createdAt "#{DateTime.utc_now()}"^^xsd:dateTime .
    }
  """)

  {:ok, user_uri}
end
```

### Soft Delete

```elixir
def soft_delete(store, resource_uri) do
  {:ok, _} = TripleStore.update(store, """
    PREFIX ex: <http://example.org/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

    INSERT {
      <#{resource_uri}> ex:deletedAt ?now .
    }
    WHERE {
      <#{resource_uri}> a ?type .
      BIND(NOW() AS ?now)
      FILTER NOT EXISTS { <#{resource_uri}> ex:deletedAt ?existing }
    }
  """)
end

def restore(store, resource_uri) do
  {:ok, _} = TripleStore.update(store, """
    PREFIX ex: <http://example.org/>

    DELETE WHERE {
      <#{resource_uri}> ex:deletedAt ?timestamp
    }
  """)
end
```

### Data Migration

```elixir
def migrate_v1_to_v2(store) do
  # Rename predicates
  {:ok, _} = TripleStore.update(store, """
    PREFIX v1: <http://example.org/v1/>
    PREFIX v2: <http://example.org/v2/>

    DELETE { ?s v1:userName ?value }
    INSERT { ?s v2:accountName ?value }
    WHERE { ?s v1:userName ?value }
  """)

  # Add version marker
  {:ok, _} = TripleStore.update(store, """
    PREFIX ex: <http://example.org/>

    INSERT DATA {
      ex:schema ex:version "2.0" .
    }
  """)

  :ok
end
```

## Next Steps

- [Reasoning](05-reasoning.md) - Work with inferred data
- [Configuration & Performance](06-configuration.md) - Optimize updates
