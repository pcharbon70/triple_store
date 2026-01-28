# Data Management (Triple Store)

This guide covers loading, exporting, and backing up data in a triple store.

> **Note**: This guide is for triple stores (`schema: :triple`). For quad stores with named graphs, see the [Quad Store Data Management](../quads/02-data-management.md) guide.

## Loading Data

### From Files

Load RDF data from files:

```elixir
# Load Turtle (recommended for human-readable data)
{:ok, count} = TripleStore.load(store, "data.ttl")

# Load N-Triples (fastest for loading)
{:ok, count} = TripleStore.load(store, "data.nt")

# Load RDF/XML
{:ok, count} = TripleStore.load(store, "data.rdf")

# Explicit format specification
{:ok, count} = TripleStore.load(store, "data.dat", format: :ntriples)
```

The format is auto-detected from the file extension:

| Extension | Format | Description |
|-----------|--------|-------------|
| `.ttl` | Turtle | Human-readable, compact |
| `.nt` | N-Triples | Simple line-based, fast parsing |
| `.rdf` | RDF/XML | XML-based |

### Loading via SPARQL UPDATE

```elixir
# Insert data directly
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  INSERT DATA {
    ex:alice ex:name "Alice" .
    ex:bob ex:name "Bob" .
  }
""")
```

### From Strings

Load RDF content from a string:

```elixir
# Turtle string
turtle = """
@prefix ex: <http://example.org/>.
@prefix foaf: <http://xmlns.com/foaf/0.1/>.

ex:alice a foaf:Person ;
         foaf:name "Alice" .
"""

{:ok, count} = TripleStore.load_string(store, turtle, :turtle)

# N-Triples string
ntriples = """
<http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
<http://example.org/alice> <http://xmlns.com/foaf/0.1/name> "Alice" .
"""

{:ok, count} = TripleStore.load_string(store, ntriples, :ntriples)
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
{:ok, count} = TripleStore.load(store, "large_dataset.nt", batch_size: 10_000)

# Smaller batches = lower memory usage
{:ok, count} = TripleStore.load(store, "data.nt", batch_size: 500)
```

**Recommended batch sizes:**

| Dataset Size | Batch Size | Notes |
|--------------|------------|-------|
| < 10,000 triples | 1,000 (default) | Works well for most cases |
| 10,000 - 1M triples | 5,000 - 10,000 | Better throughput |
| > 1M triples | 10,000 - 50,000 | Maximum performance |

## Inserting Triples

### Single Triple

```elixir
import RDF.Sigils

{:ok, 1} = TripleStore.insert(store, {
  ~I<http://example.org/alice>,
  ~I<http://example.org/knows>,
  ~I<http://example.org/bob>
})
```

### Multiple Triples

```elixir
triples = [
  {~I<http://example.org/alice>, ~I<http://example.org/knows>, ~I<http://example.org/bob>},
  {~I<http://example.org/alice>, ~I<http://example.org/name>, ~L"Alice"},
  {~I<http://example.org/bob>, ~I<http://example.org/name>, ~L"Bob"}
]

{:ok, 3} = TripleStore.insert(store, triples)
```

## Deleting Triples

### Single Triple

```elixir
{:ok, 1} = TripleStore.delete(store, {
  ~I<http://example.org/alice>,
  ~I<http://example.org/knows>,
  ~I<http://example.org/bob>
})
```

### Delete by Pattern

```elixir
# Delete all triples for a subject
TripleStore.update(store, """
  PREFIX ex: <http://example.org/>

  DELETE {
    ex:alice ?p ?o .
  }
  WHERE {
    ex:alice ?p ?o .
  }
""")
```

### Delete All Data

```elixir
# Clear all triples
{:ok, _} = TripleStore.update(store, "CLEAR DEFAULT")
```

## Exporting Data

### To RDF.Graph

```elixir
# Export entire store
{:ok, graph} = TripleStore.export(store, :graph)

# Export with pattern
{:ok, graph} = TripleStore.export(store, :graph,
  pattern: {{:bound, subject_id}, :var, :var}
)
```

### To File

Export to various formats:

```elixir
# Turtle (human-readable)
{:ok, count} = TripleStore.export(store, {:file, "backup.ttl", :turtle})

# N-Triples (fast to parse)
{:ok, count} = TripleStore.export(store, {:file, "backup.nt", :ntriples})
```

### Export with Prefixes

```elixir
# Turtle with custom prefixes
{:ok, count} = TripleStore.export(store, {:file, "data.ttl", :turtle},
  prefixes: %{
    "ex" => "http://example.org/",
    "foaf" => "http://xmlns.com/foaf/0.1/"
  }
)
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
```

### Restoring from Backup

```elixir
# Restore full database
{:ok, restored_store} = TripleStore.Backup.restore(
  "/backups/mydb_20240120",
  "/data/restored_db"
)
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

# Stop scheduled backups
:ok = TripleStore.ScheduledBackup.stop(scheduler)
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
  def safe_delete(store, pattern_sparql) do
    # 1. Preview what will be deleted
    {:ok, preview} = TripleStore.query(store, """
      SELECT (COUNT(*) AS ?count)
      WHERE { #{pattern_sparql} }
    """)

    count = hd(preview)["count"]
    IO.puts("Will delete #{count} triples")

    # 2. Confirm before proceeding
    if confirm?() do
      {:ok, deleted} = TripleStore.update(store, """
        DELETE WHERE { #{pattern_sparql} }
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

## Troubleshooting

### "File not found" errors

```elixir
path = "data.nt"
unless File.exists?(path) do
  IO.puts("File not found: #{path}")
end
```

### Parse errors

```elixir
case TripleStore.load(store, "data.ttl") do
  {:ok, count} ->
    IO.puts("Loaded #{count} triples")

  {:error, {:parse_error, message}} ->
    IO.puts("Parse error: #{message}")
    # Check file encoding, Turtle syntax, etc.

  {:error, reason} ->
    IO.puts("Error: #{inspect(reason)}")
end
```

### Memory issues with large files

```elixir
# For very large files, use N-Triples format (streaming)
{:ok, count} = TripleStore.load(store, "large.nt")
```

## Next Steps

- [SPARQL Queries](03-sparql-queries.md) - Query your triple store
- [SPARQL Updates](04-sparql-updates.md) - Modify data with SPARQL
- [Reasoning](05-reasoning.md) - Enable inference
- [Configuration & Performance](06-configuration.md) - Optimize loading
