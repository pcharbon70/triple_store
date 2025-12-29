# Data Management

This guide covers loading, exporting, and backing up data in TripleStore.

## Loading Data

### From Files

Load RDF data from files on disk:

```elixir
# Load Turtle file
{:ok, count} = TripleStore.load(store, "ontology.ttl")

# Load N-Triples
{:ok, count} = TripleStore.load(store, "data.nt")

# Explicit format
{:ok, count} = TripleStore.load(store, "data.xml", format: :rdfxml)
```

### From Strings

Load RDF content from a string:

```elixir
turtle_content = """
@prefix ex: <http://example.org/> .

ex:alice ex:knows ex:bob .
ex:bob ex:knows ex:charlie .
"""

{:ok, count} = TripleStore.load_string(store, turtle_content, :turtle)
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
{:ok, count} = TripleStore.load(store, "data.ttl", batch_size: 500)
```

**Recommended batch sizes:**

| Dataset Size | Batch Size | Notes |
|--------------|------------|-------|
| < 10,000 triples | 1,000 (default) | Works well for most cases |
| 10,000 - 1M triples | 5,000 - 10,000 | Better throughput |
| > 1M triples | 10,000 - 50,000 | Maximum performance |

### Tips for Loading

1. **Use N-Triples for bulk loading** - It's the fastest format to parse
2. **Load before enabling reasoning** - Materialize after all data is loaded
3. **Monitor memory** - Large Turtle files require more memory than N-Triples

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

### From RDF.Description

```elixir
import RDF.Sigils

description = RDF.Description.new(~I<http://example.org/alice>)
|> RDF.Description.add({~I<http://example.org/name>, ~L"Alice"})
|> RDF.Description.add({~I<http://example.org/age>, 30})

{:ok, 2} = TripleStore.insert(store, description)
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

### Multiple Triples

```elixir
# Delete all provided triples
{:ok, count} = TripleStore.delete(store, triples)
```

### Using SPARQL DELETE

For pattern-based deletion, use SPARQL UPDATE:

```elixir
# Delete all triples about alice
{:ok, count} = TripleStore.update(store, """
  DELETE WHERE {
    <http://example.org/alice> ?p ?o
  }
""")
```

> **Note**: After significant deletions, you may need to rematerialize inferences. See the [Reasoning guide](05-reasoning.md).

## Exporting Data

### To RDF.Graph

Get all triples as an RDF.Graph:

```elixir
{:ok, graph} = TripleStore.export(store, :graph)

# Use the graph
IO.puts("Triples: #{RDF.Graph.triple_count(graph)}")
```

### To File

Export to a file in various formats:

```elixir
# Turtle (human-readable)
{:ok, count} = TripleStore.export(store, {:file, "backup.ttl", :turtle})

# N-Triples (streaming-friendly)
{:ok, count} = TripleStore.export(store, {:file, "backup.nt", :ntriples})

# With prefixes (Turtle only)
{:ok, count} = TripleStore.export(store, {:file, "data.ttl", :turtle},
  prefixes: %{
    "ex" => "http://example.org/",
    "foaf" => "http://xmlns.com/foaf/0.1/"
  }
)
```

### To String

Get serialized RDF as a string:

```elixir
{:ok, turtle_string} = TripleStore.export(store, {:string, :turtle})
IO.puts(turtle_string)
```

### Partial Export

Export only specific patterns:

```elixir
# Export only triples with a specific predicate
{:ok, graph} = TripleStore.export(store, :graph,
  pattern: {:var, {:bound, rdf_type_id}, :var}
)
```

## Backup and Restore

### Creating Backups

Create a complete backup of the database:

```elixir
{:ok, metadata} = TripleStore.backup(store, "/backups/mydb_20241229")

IO.puts("Backup created:")
IO.puts("  Path: #{metadata.path}")
IO.puts("  Size: #{metadata.size_bytes} bytes")
IO.puts("  Time: #{metadata.created_at}")
```

### Restoring from Backup

Restore a database from a backup:

```elixir
{:ok, restored_store} = TripleStore.restore(
  "/backups/mydb_20241229",
  "/data/restored_db"
)

# The restored store is ready to use
{:ok, stats} = TripleStore.stats(restored_store)
IO.puts("Restored #{stats.triple_count} triples")
```

### Scheduled Backups

Set up automatic periodic backups:

```elixir
# Hourly backups, keep last 24
{:ok, scheduler} = TripleStore.schedule_backup(store, "/backups/mydb",
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

### Backup with Rotation

Automatically remove old backups:

```elixir
# Uses TripleStore.Backup.rotate/3 internally
{:ok, metadata} = TripleStore.Backup.rotate(store, "/backups/mydb",
  max_backups: 5,
  prefix: "daily"
)

# Creates backup named: daily_20241229_103000_123
# Removes oldest backups beyond max_backups
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

### Backup Before Updates

```elixir
defmodule SafeUpdater do
  def update_with_backup(store, update_sparql, backup_dir) do
    # 1. Create pre-update backup
    timestamp = DateTime.utc_now() |> DateTime.to_unix()
    {:ok, backup} = TripleStore.backup(store, "#{backup_dir}/pre_update_#{timestamp}")

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

Check that the file path is correct and accessible:

```elixir
path = "data.ttl"
unless File.exists?(path) do
  IO.puts("File not found: #{path}")
end
```

### Parse errors

For parsing issues, check the file format:

```elixir
case TripleStore.load(store, "data.ttl") do
  {:ok, count} ->
    IO.puts("Loaded #{count} triples")

  {:error, {:parse_error, message}} ->
    IO.puts("Parse error: #{message}")
    # Check file encoding, syntax, etc.
end
```

### Memory issues with large files

For very large files, use N-Triples format and streaming:

```elixir
# N-Triples is streamed line-by-line, using less memory
{:ok, count} = TripleStore.load(store, "large.nt")
```

## Next Steps

- [SPARQL Queries](03-sparql-queries.md) - Query your data
- [SPARQL Updates](04-sparql-updates.md) - Modify data with SPARQL
- [Reasoning](05-reasoning.md) - Enable inference
- [Configuration & Performance](06-configuration.md) - Optimize loading
