# Data Management

This guide covers loading, exporting, and backing up data in TripleStore, with focus on quad store operations and named graphs.

## Loading Data

### From Files

Load RDF data from files:

```elixir
# Load N-Quads (recommended for quad store)
{:ok, count} = TripleStore.load(store, "data.nq")

# Load TriG (human-readable quad format)
{:ok, count} = TripleStore.load(store, "data.trig")

# Load Turtle (loaded into default graph)
{:ok, count} = TripleStore.load(store, "ontology.ttl")

# Load N-Triples
{:ok, count} = TripleStore.load(store, "data.nt")

# Explicit format specification
{:ok, count} = TripleStore.load(store, "data.xml", format: :rdfxml)
```

### Loading into Named Graphs

Use SPARQL UPDATE to load data into specific graphs:

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
    GRAPH ex:people {
      ex:bob ex:name "Bob" .
    }
  }
""")
```

### From Strings

Load RDF content from a string:

```elixir
# N-Quads string
nquads = """
<http://example.org/alice> <http://xmlns.com/foaf/0.1/name> "Alice" <http://example.org/people> .
<http://example.org/bob> <http://xmlns.com/foaf/0.1/name> "Bob" <http://example.org/people> .
"""

{:ok, count} = TripleStore.load_string(store, nquads, :nquads)

# TriG string
trig = """
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

<http://example.org/people> {
  <http://example.org/charlie> foaf:name "Charlie" .
}
"""

{:ok, count} = TripleStore.load_string(store, trig, :trig)

# Turtle string (goes to default graph)
turtle = """
@prefix ex: <http://example.org/> .
ex:david ex:name "David" .
"""

{:ok, count} = TripleStore.load_string(store, turtle, :turtle)
```

### From RDF.Graph

Load an RDF.ex Graph directly:

```elixir
import RDF.Sigils

graph = RDF.Graph.new([
  {~I<http://example.org/alice>, ~I<http://example.org/name>, ~L"Alice"},
  {~I<http://example.org/alice>, ~I<http://example.org/age>, 30}
])

{:ok, count} = TripleStore.load_graph(store, graph)
```

### Batch Loading Options

For large files, tune the batch size:

```elixir
# Larger batches = fewer commits, faster loading
{:ok, count} = TripleStore.load(store, "large_dataset.nq", batch_size: 10_000)

# Smaller batches = lower memory usage
{:ok, count} = TripleStore.load(store, "data.nq", batch_size: 500)
```

**Recommended batch sizes:**

| Dataset Size | Batch Size | Notes |
|--------------|------------|-------|
| < 10,000 quads | 1,000 (default) | Works well for most cases |
| 10,000 - 1M quads | 5,000 - 10,000 | Better throughput |
| > 1M quads | 10,000 - 50,000 | Maximum performance |

### Loading Multiple Data Sources

```elixir
# Load different sources into different graphs
sources = [
  {"source1.nq", "http://example.org/source1"},
  {"source2.nq", "http://example.org/source2"},
  {"source3.nq", "http://example.org/source3"}
]

Enum.each(sources, fn {file, graph_name} ->
  # Load file into a named graph for that source
  {:ok, content} = File.read(file)

  # Parse and load with graph context
  TripleStore.load_string(store, content, :nquads)
end)
```

## Inserting Quads

### Single Quad

```elixir
import RDF.Sigils

# Insert with explicit graph
{:ok, 1} = TripleStore.insert(store, {
  ~I<http://example.org/alice>,
  ~I<http://example.org/knows>,
  ~I<http://example.org/bob>,
  ~I<http://example.org/social>
})

# Insert into default graph (omit graph)
{:ok, 1} = TripleStore.insert(store, {
  ~I<http://example.org/alice>,
  ~I<http://example.org/name>,
  ~L"Alice"
})
```

### Multiple Quads

```elixir
quads = [
  {~I<http://example.org/alice>, ~I<http://example.org/knows>, ~I<http://example.org/bob>, ~I<http://example.org/social>},
  {~I<http://example.org/alice>, ~I<http://example.org/name>, ~L"Alice", ~I<http://example.org/people>},
  {~I<http://example.org/bob>, ~I<http://example.org/name>, ~L"Bob", ~I<http://example.org/people>}
]

{:ok, 3} = TripleStore.insert(store, quads)
```

### Using SPARQL UPDATE

```elixir
# Insert into specific graph
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:people {
      ex:charlie ex:name "Charlie" ;
                 ex:age 35 .
    }
  }
""")
```

## Deleting Quads

### Single Quad

```elixir
{:ok, 1} = TripleStore.delete(store, {
  ~I<http://example.org/alice>,
  ~I<http://example.org/knows>,
  ~I<http://example.org/bob>,
  ~I<http://example.org/social>
})
```

### Delete from Specific Graph

```elixir
# Delete all quads in a graph matching a pattern
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    GRAPH ex:people {
      ex:alice ?p ?o .
    }
  }
  WHERE {
    GRAPH ex:people {
      ex:alice ?p ?o .
    }
  }
""")
```

### Delete Entire Graph

```elixir
# Clear all data from a specific graph
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    GRAPH ex:temp {
      ?s ?p ?o .
    }
  }
  WHERE {
    GRAPH ex:temp {
      ?s ?p ?o .
    }
  }
""")
```

## Exporting Data

### Per-Graph Export

Export a single graph to N-Quads format:

```elixir
# Export a specific graph as N-Quads string
{:ok, nquads} = TripleStore.GraphBackup.export_graph(store, graph_id)

# Export to file
{:ok, metadata} = TripleStore.GraphBackup.backup_graph(
  store,
  graph_id,
  "/backups/people_graph.nq"
)
```

### To RDF.Graph

```elixir
# Export default graph
{:ok, graph} = TripleStore.export(store, :graph)

# Export specific graph pattern
{:ok, graph} = TripleStore.export(store, :graph,
  pattern: {:var, :var, :var, {:bound, graph_id}}
)
```

### To File

Export to various formats:

```elixir
# N-Quads (includes graph context)
{:ok, count} = TripleStore.export(store, {:file, "backup.nq", :nquads})

# TriG (human-readable with graphs)
{:ok, count} = TripleStore.export(store, {:file, "backup.trig", :trig})

# Turtle (default graph only)
{:ok, count} = TripleStore.export(store, {:file, "backup.ttl", :turtle})

# N-Triples
{:ok, count} = TripleStore.export(store, {:file, "backup.nt", :ntriples})
```

### Export with Prefixes

```elixir
# TriG with custom prefixes
{:ok, count} = TripleStore.export(store, {:file, "data.trig", :trig},
  prefixes: %{
    "ex" => "http://example.org/",
    "foaf" => "http://xmlns.com/foaf/0.1/"
  }
)
```

### Partial Export

Export only specific patterns:

```elixir
# Export only quads with a specific predicate
pattern = {:var, {:bound, predicate_id}, :var, :var}
{:ok, graph} = TripleStore.export(store, :graph, pattern: pattern)

# Export from specific graph
pattern = {:var, :var, :var, {:bound, graph_id}}
{:ok, graph} = TripleStore.export(store, :graph, pattern: pattern)
```

## Backup and Restore

### Full Store Backup

Create a complete backup of the database:

```elixir
{:ok, metadata} = TripleStore.Backup.create(store, "/backups/mydb_20240120")

IO.puts("Backup created:")
IO.puts("  Path: #{metadata.path}")
IO.puts("  Schema: #{metadata.schema}")
IO.puts("  Size: #{metadata.size_bytes} bytes")
IO.puts("  Time: #{metadata.created_at}")
```

### Per-Graph Backup

Backup individual graphs using the GraphBackup module:

```elixir
# Backup a specific graph to N-Quads file
{:ok, metadata} = TripleStore.GraphBackup.backup_graph(
  store,
  graph_id,
  "/backups/graph_#{graph_id}.nq"
)

# Metadata includes:
# - graph_id: Which graph was backed up
# - graph_name: IRI of the graph (if available)
# - quad_count: Number of quads in the graph
# - created_at: Timestamp of backup
# - file_size: Size of the backup file
```

### Backup with Graph Statistics

Include per-graph statistics in the backup:

```elixir
{:ok, metadata} = TripleStore.Backup.create_with_graph_stats(
  store,
  "/backups/mydb_with_stats"
)

# Access per-graph statistics
metadata.statistics.per_graph
# => %{
#   0 => %{quad_count: 1000, predicate_counts: %{...}},
#   1 => %{quad_count: 500, predicate_counts: %{...}},
#   ...
# }
```

### Restoring from Backup

```elixir
# Restore full database
{:ok, restored_store} = TripleStore.Backup.restore(
  "/backups/mydb_20240120",
  "/data/restored_db"
)

# Restore specific graph
{:ok, stats} = TripleStore.GraphBackup.restore_graph(
  restored_store,
  "/backups/graph_1.nq",
  target_graph_id
)
```

### Restore with Options

```elixir
# Append to existing graph instead of clearing
{:ok, stats} = TripleStore.GraphBackup.restore_graph(
  store,
  "/backups/graph.nq",
  graph_id,
  append: true
)

# Skip validation for faster restore
{:ok, stats} = TripleStore.GraphBackup.restore_graph(
  store,
  "/backups/graph.nq",
  graph_id,
  validate: false
)
```

### Validating Backups

Validate backup files before restoring:

```elixir
# Validate a graph backup file
case TripleStore.GraphBackup.validate_backup("/backups/graph_1.nq") do
  {:ok, :valid} ->
    IO.puts("Backup is valid")

  {:ok, :valid_with_metadata} ->
    IO.puts("Backup is valid with metadata file")

  {:error, :not_found} ->
    IO.puts("Backup file not found")

  {:error, :invalid_format} ->
    IO.puts("Backup file is corrupted or invalid")
end
```

### Listing Backups

List all graph backups in a directory:

```elixir
{:ok, backups} = TripleStore.GraphBackup.list_backups("/backups")

# Returns list of backup metadata
Enum.each(backups, fn backup ->
  IO.puts("#{backup.graph_name}: #{backup.quad_count} quads")
end)
```

### Verifying Quad Store Backup

For quad stores, verify all 4 indices are present:

```elixir
# Check if backup is from a quad store
{:ok, :quad} = TripleStore.Backup.get_backup_schema("/backups/mydb")

# Verify quad store has all required indices
{:ok, :valid} = TripleStore.Backup.verify_quad_backup("/backups/mydb")
```

### Scheduled Backups

Set up automatic periodic backups:

```elixir
# Hourly backups, keep last 24
{:ok, scheduler} = TripleStore.Backup.schedule_backup(store, "/backups/mydb",
  interval: :timer.hours(1),
  max_backups: 24,
  prefix: "hourly"
)

# Check status
{:ok, status} = TripleStore.ScheduledBackup.status(scheduler)
IO.puts("Backups completed: #{status.backup_count}")
IO.puts("Last backup: #{status.last_backup}")

# Trigger immediate backup
{:ok, metadata} = TripleStore.ScheduledBackup.trigger_backup(scheduler)

# Stop scheduled backups
:ok = TripleStore.ScheduledBackup.stop(scheduler)
```

### Backup Rotation

Automatically remove old backups:

```elixir
# Keep only the 5 most recent daily backups
{:ok, metadata} = TripleStore.Backup.rotate(store, "/backups/mydb",
  max_backups: 5,
  prefix: "daily"
)

# Creates backup named: daily_20240120_103000_123
# Removes oldest backups beyond max_backups
```

## Graph Management

### Listing Graphs

Get all graphs in the store:

```elixir
{:ok, graphs} = TripleStore.list_graphs(store)

# Returns list of graph IDs or IRIs
# Example: [0, 5, 12] or ["http://example.org/g1", ...]
```

### Getting Graph Statistics

```elixir
# Get statistics for a specific graph
{:ok, stats} = TripleStore.Statistics.graph_statistics(store, graph_id)

IO.puts("Graph #{graph_id}:")
IO.puts("  Quad count: #{stats.quad_count}")
IO.puts("  Unique subjects: #{stats.unique_subjects}")
IO.puts("  Unique predicates: #{stats.unique_predicates}")
```

### Creating Named Graphs

Named graphs are created implicitly when you insert data into them:

```elixir
# This creates the graph if it doesn't exist
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    GRAPH ex:new_graph {
      ex:subject ex:predicate ex:object .
    }
  }
""")
```

### Copying Graphs

```elixir
# Copy data from one graph to another
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT {
    GRAPH ex:backup_graph {
      ?s ?p ?o .
    }
  }
  WHERE {
    GRAPH ex:source_graph {
      ?s ?p ?o .
    }
  }
""")
```

### Moving Graphs

```elixir
# Move (copy + delete) data between graphs
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    GRAPH ex:source {
      ?s ?p ?o .
    }
  }
  INSERT {
    GRAPH ex:archive {
      ?s ?p ?o .
    }
  }
  WHERE {
    GRAPH ex:source {
      ?s ?p ?o .
    }
  }
""")
```

## Best Practices

### Loading Large Datasets

```elixir
defmodule DataLoader do
  def load_large_dataset(store, file_path) do
    # 1. Check store health
    {:ok, health} = TripleStore.health(store)
    unless health.status == :healthy do
      raise "Store not healthy: #{health.status}"
    end

    # 2. Load with optimal batch size
    {:ok, count} = TripleStore.load(store, file_path,
      batch_size: 10_000
    )

    # 3. Refresh statistics for query optimization
    TripleStore.refresh_statistics(store)

    # 4. Materialize if using reasoning
    {:ok, reason_stats} = TripleStore.materialize(store)

    {:ok, %{
      loaded: count,
      derived: reason_stats.total_derived
    }}
  end
end
```

### Safe Delete Pattern

```elixir
defmodule DataManager do
  def safe_delete_from_graph(store, graph_iri, pattern_sparql) do
    # 1. Preview what will be deleted
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

    # 2. Confirm before proceeding
    if confirm?() do
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

      # 3. Rematerialize if using reasoning
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

### Backup Before Bulk Operations

```elixir
defmodule SafeUpdater do
  def update_with_backup(store, update_sparql, backup_dir) do
    # 1. Create pre-update backup
    timestamp = DateTime.utc_now() |> DateTime.to_iso8601()
    {:ok, backup} = TripleStore.Backup.create(
      store,
      "#{backup_dir}/pre_update_#{timestamp}"
    )

    try do
      # 2. Perform update
      {:ok, count} = TripleStore.update(store, update_sparql)
      {:ok, %{updated: count, backup: backup.path}}
    rescue
      e ->
        IO.puts("Update failed, restore from: #{backup.path}")
        reraise e, __STACKTRACE__
    end
  end
end
```

### Per-Graph Backup Strategy

```elixir
defmodule GraphBackupStrategy do
  def backup_all_graphs(store, backup_dir) do
    with {:ok, graphs} <- TripleStore.list_graphs(store) do
      Enum.reduce(graphs, {:ok, []}, fn graph_id, {:ok, results} ->
        backup_path = Path.join(backup_dir, "graph_#{graph_id}.nq")

        case TripleStore.GraphBackup.backup_graph(store, graph_id, backup_path) do
          {:ok, metadata} ->
            {:ok, [metadata | results]}

          {:error, reason} ->
            {:error, {:graph_backup_failed, graph_id, reason}}
        end
      end)
    end
  end
end
```

## Troubleshooting

### "File not found" errors

Check that the file path is correct and accessible:

```elixir
path = "data.nq"
unless File.exists?(path) do
  IO.puts("File not found: #{path}")
end
```

### Parse errors

For parsing issues, check the file format:

```elixir
case TripleStore.load(store, "data.nq") do
  {:ok, count} ->
    IO.puts("Loaded #{count} quads")

  {:error, {:parse_error, message}} ->
    IO.puts("Parse error: #{message}")
    # Check file encoding, N-Quads syntax, etc.

  {:error, reason} ->
    IO.puts("Error: #{inspect(reason)}")
end
```

### Memory issues with large files

For very large files, use N-Quads format which is streamed:

```elixir
# N-Quads is streamed line-by-line, using less memory
{:ok, count} = TripleStore.load(store, "large.nq")
```

## Next Steps

- [SPARQL Queries](03-sparql-queries.md) - Query your data with GRAPH clauses
- [SPARQL Updates](04-sparql-updates.md) - Modify data with SPARQL
- [Named Graphs](07-named-graphs.md) - Advanced graph management
- [Reasoning](05-reasoning.md) - Enable inference
- [Configuration & Performance](06-configuration.md) - Optimize loading
