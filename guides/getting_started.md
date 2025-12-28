# Getting Started with TripleStore

This guide will walk you through the basics of using TripleStore, a high-performance RDF triple store with SPARQL 1.1 and OWL 2 RL reasoning support.

## Installation

Add `triple_store` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:triple_store, "~> 0.1.0"}
  ]
end
```

Then fetch dependencies:

```bash
mix deps.get
```

## Opening a Store

Create or open a triple store by specifying a path:

```elixir
{:ok, store} = TripleStore.open("./my_database")
```

The database directory will be created if it doesn't exist. The store handle can be shared between processes.

### Options

```elixir
{:ok, store} = TripleStore.open("./my_database",
  create_if_missing: true  # Default: true
)
```

## Loading Data

### From a File

Load RDF data from Turtle, N-Triples, or other formats:

```elixir
{:ok, count} = TripleStore.load(store, "data.ttl")
# => {:ok, 1000}  # Number of triples loaded
```

Supported formats are auto-detected from file extension:
- `.ttl`, `.turtle` - Turtle
- `.nt`, `.ntriples` - N-Triples
- `.nq`, `.nquads` - N-Quads
- `.rdf`, `.xml` - RDF/XML

### From an RDF.Graph

```elixir
graph = RDF.Graph.new([
  {~I<http://example.org/alice>, ~I<http://example.org/knows>, ~I<http://example.org/bob>},
  {~I<http://example.org/alice>, RDF.type(), ~I<http://example.org/Person>}
])

{:ok, 2} = TripleStore.load_graph(store, graph)
```

### From a String

```elixir
ttl = """
@prefix ex: <http://example.org/> .

ex:alice a ex:Person ;
         ex:knows ex:bob .
"""

{:ok, 2} = TripleStore.load_string(store, ttl, :turtle)
```

## Inserting and Deleting Triples

### Insert

```elixir
# Single triple
{:ok, 1} = TripleStore.insert(store,
  {~I<http://example.org/alice>, ~I<http://example.org/age>, RDF.literal(30)}
)

# Multiple triples
{:ok, 2} = TripleStore.insert(store, [
  {~I<http://example.org/bob>, RDF.type(), ~I<http://example.org/Person>},
  {~I<http://example.org/bob>, ~I<http://example.org/age>, RDF.literal(25)}
])
```

### Delete

```elixir
{:ok, 1} = TripleStore.delete(store,
  {~I<http://example.org/alice>, ~I<http://example.org/age>, RDF.literal(30)}
)
```

## Querying with SPARQL

### SELECT Queries

```elixir
sparql = """
SELECT ?person ?name
WHERE {
  ?person a <http://example.org/Person> ;
          <http://example.org/name> ?name .
}
"""

{:ok, results} = TripleStore.query(store, sparql)
# => {:ok, [
#      %{"person" => ~I<http://example.org/alice>, "name" => ~L"Alice"},
#      %{"person" => ~I<http://example.org/bob>, "name" => ~L"Bob"}
#    ]}
```

### ASK Queries

```elixir
{:ok, true} = TripleStore.query(store,
  "ASK { <http://example.org/alice> a <http://example.org/Person> }"
)
```

### CONSTRUCT Queries

```elixir
{:ok, graph} = TripleStore.query(store, """
  CONSTRUCT { ?s <http://example.org/friendOf> ?o }
  WHERE { ?s <http://example.org/knows> ?o }
""")
# => {:ok, #RDF.Graph<...>}
```

### Query Options

```elixir
{:ok, results} = TripleStore.query(store, sparql,
  timeout: 5000,     # Maximum execution time in ms
  optimize: true     # Enable query optimization (default)
)
```

## SPARQL UPDATE

Modify data using SPARQL UPDATE operations:

```elixir
# INSERT DATA
{:ok, 1} = TripleStore.update(store, """
  INSERT DATA {
    <http://example.org/charlie> a <http://example.org/Person> .
  }
""")

# DELETE DATA
{:ok, 1} = TripleStore.update(store, """
  DELETE DATA {
    <http://example.org/charlie> a <http://example.org/Person> .
  }
""")

# DELETE/INSERT WHERE
{:ok, count} = TripleStore.update(store, """
  DELETE { ?s <http://example.org/status> ?old }
  INSERT { ?s <http://example.org/status> "active" }
  WHERE { ?s <http://example.org/status> ?old }
""")
```

## Exporting Data

### As RDF.Graph

```elixir
{:ok, graph} = TripleStore.export(store, :graph)
```

### To File

```elixir
{:ok, count} = TripleStore.export(store, {:file, "export.ttl", :turtle})
```

### As String

```elixir
{:ok, ntriples} = TripleStore.export(store, {:string, :ntriples})
```

## Reasoning

Enable OWL 2 RL reasoning to infer additional triples:

```elixir
# Load an ontology
{:ok, _} = TripleStore.load(store, "ontology.ttl")

# Materialize inferences
{:ok, stats} = TripleStore.materialize(store, profile: :owl2rl)
# => {:ok, %{iterations: 5, total_derived: 1500, duration_ms: 250}}
```

### Reasoning Profiles

- `:rdfs` - RDFS entailment only (subclass, subproperty, domain, range)
- `:owl2rl` - Full OWL 2 RL profile (default)
- `:all` - All available rules

### Check Reasoning Status

```elixir
{:ok, status} = TripleStore.reasoning_status(store)
# => {:ok, %{
#      state: :materialized,
#      derived_count: 1500,
#      needs_rematerialization: false
#    }}
```

## Backup and Restore

### Create Backup

```elixir
{:ok, metadata} = TripleStore.backup(store, "/backups/mydb_20251228")
# => {:ok, %{
#      path: "/backups/mydb_20251228",
#      timestamp: ~U[2025-12-28 10:00:00Z],
#      size_bytes: 1048576
#    }}
```

### Restore from Backup

```elixir
{:ok, restored_store} = TripleStore.restore(
  "/backups/mydb_20251228",
  "./restored_database"
)
```

## Monitoring

### Health Check

```elixir
{:ok, health} = TripleStore.health(store)
# => {:ok, %{
#      status: :healthy,
#      triple_count: 10000,
#      database_open: true,
#      dict_manager_alive: true
#    }}
```

### Statistics

```elixir
{:ok, stats} = TripleStore.stats(store)
# => {:ok, %{
#      triple_count: 10000,
#      distinct_subjects: 500,
#      distinct_predicates: 50,
#      distinct_objects: 3000
#    }}
```

## Closing the Store

Always close the store when done to ensure data is flushed:

```elixir
:ok = TripleStore.close(store)
```

## Error Handling

All functions return tagged tuples for pattern matching:

```elixir
case TripleStore.query(store, sparql) do
  {:ok, results} ->
    process_results(results)

  {:error, {:parse_error, message}} ->
    Logger.error("Invalid SPARQL: #{message}")

  {:error, :timeout} ->
    Logger.error("Query timed out")

  {:error, reason} ->
    Logger.error("Query failed: #{inspect(reason)}")
end
```

## Next Steps

- See the [Performance Tuning Guide](performance_tuning.md) for optimization tips
- Explore the `TripleStore.Telemetry` module for monitoring integration
- Check `TripleStore.Config.RocksDB` for database tuning options
