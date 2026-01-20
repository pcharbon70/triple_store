# Getting Started

This guide will help you get up and running with TripleStore quickly.

## Triple Store vs Quad Store

TripleStore supports two storage schemas:

| Feature | Triple Store (v1) | Quad Store (v2) |
|---------|-------------------|-----------------|
| **Data Model** | `{subject, predicate, object}` | `{graph, subject, predicate, object}` |
| **Named Graphs** | No (implicit default) | Yes (explicit graphs) |
| **Indices** | 3 (SPO, POS, OSP) | 4 (GSPO, GPOS, SPOG, POSG) |
| **Use Case** | Simple datasets | Multi-tenancy, provenance, data isolation |

**Recommendation**: Use quad store (`schema: :quad`) for new projects. It supports everything triple store does plus named graphs for data isolation and provenance tracking.

Use triple store only if you have a simple dataset without named graph requirements and maximum performance is critical.

## Installation

Add `triple_store` to your dependencies in `mix.exs`:

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

The first compilation will build the native dependencies:

```bash
mix compile
```

> **Note**: You need librocksdb-dev installed:
> - Ubuntu/Debian: `sudo apt-get install librocksdb-dev`
> - macOS: `brew install rocksdb`

## Quick Start

### Opening a Store

```elixir
# Recommended: Open or create a quad store
{:ok, store} = TripleStore.open("./my_database", schema: :quad)

# Alternative: Triple store (for simple datasets without named graphs)
{:ok, store} = TripleStore.open("./my_database", schema: :triple)
```

The database directory will be created if it doesn't exist.

> **Important**: Once a database is created with a schema, it cannot be changed. Choose the correct schema when first opening the database.

### Loading Data

Load RDF data from a file:

```elixir
# Load N-Quads (recommended for quad store - includes graph context)
{:ok, count} = TripleStore.load(store, "data.nq")
IO.puts("Loaded #{count} quads")

# Load TriG (human-readable quad format)
{:ok, count} = TripleStore.load(store, "data.trig")

# Load Turtle (loaded into default graph)
{:ok, count} = TripleStore.load(store, "data.ttl")

# Load N-Triples
{:ok, count} = TripleStore.load(store, "data.nt")
```

### Inserting Data

```elixir
import RDF.Sigils

# Insert a quad (with graph context)
{:ok, 1} = TripleStore.insert(store, {
  ~I<http://example.org/alice>,
  ~I<http://example.org/knows>,
  ~I<http://example.org/bob>,
  ~I<http://example.org/social>
})

# Or insert into default graph
{:ok, 1} = TripleStore.insert(store, {
  ~I<http://example.org/alice>,
  ~I<http://example.org/name>,
  ~L"Alice"
})
```

### Querying with SPARQL

Execute SPARQL queries:

```elixir
# Query default graph
{:ok, results} = TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  SELECT ?person ?name
  WHERE {
    ?person foaf:name ?name
  }
""")

# Query specific named graph
{:ok, results} = TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  SELECT ?person ?name
  WHERE {
    GRAPH <http://example.org/social> {
      ?person foaf:name ?name
    }
  }
""")

# Process results
for row <- results do
  IO.puts("Person: #{inspect(row["person"])}, Name: #{inspect(row["name"])}")
end
```

### Closing the Store

Always close the store when done:

```elixir
:ok = TripleStore.close(store)
```

## Complete Example

Here's a complete example demonstrating quad store operations with named graphs:

```elixir
defmodule MyApp.Example do
  import RDF.Sigils

  def run do
    # 1. Open quad store
    {:ok, store} = TripleStore.open("./example_db", schema: :quad)

    # 2. Load data into different graphs
    load_data(store)

    # 3. Query across all graphs
    query_all_graphs(store)

    # 4. Query specific graph
    query_specific_graph(store)

    # 5. Add data to a named graph
    add_to_graph(store)

    # 6. Check statistics
    check_stats(store)

    # 7. Clean up
    :ok = TripleStore.close(store)
  end

  defp load_data(store) do
    # Load N-Quads file with graph context
    nquads = """
    <http://example.org/alice> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://example.org/people> .
    <http://example.org/alice> <http://xmlns.com/foaf/0.1/name> "Alice" <http://example.org/people> .
    <http://example.org/bob> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> <http://example.org/people> .
    <http://example.org/bob> <http://xmlns.com/foaf/0.1/name> "Bob" <http://example.org/people> .
    """

    {:ok, count} = TripleStore.load_string(store, nquads, :nquads)
    IO.puts("Loaded #{count} quads")
  end

  defp query_all_graphs(store) do
    {:ok, results} = TripleStore.query(store, """
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      SELECT ?g ?name
      WHERE {
        GRAPH ?g {
          ?s a foaf:Person ;
             foaf:name ?name .
        }
      }
    """)

    IO.puts("\nAll graphs:")
    for row <- results do
      IO.puts("  #{row["g"]}: #{row["name"]}")
    end
  end

  defp query_specific_graph(store) do
    {:ok, results} = TripleStore.query(store, """
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      SELECT ?s ?p ?o
      WHERE {
        GRAPH <http://example.org/people> {
          ?s ?p ?o
        }
      }
    """)

    IO.puts("\nPeople graph quads:")
    for row <- results do
      IO.puts("  #{row["s"]} #{row["p"]} #{row["o"]}")
    end
  end

  defp add_to_graph(store) do
    {:ok, _} = TripleStore.update(store, """
      PREFIX ex: <http://example.org/>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      INSERT DATA {
        GRAPH ex:people {
          ex:charlie a foaf:Person ;
                     foaf:name "Charlie" ;
                     foaf:age 35 .
        }
      }
    """)
  end

  defp check_stats(store) do
    {:ok, stats} = TripleStore.stats(store)
    IO.puts("\nTotal quads: #{stats.triple_count}")

    # List all graphs
    {:ok, graphs} = TripleStore.list_graphs(store)
    IO.puts("Graphs: #{inspect(graphs)}")
  end
end
```

## Supported RDF Formats

TripleStore supports these formats for loading and exporting:

| Format | Extension | Description | Graph Support |
|--------|-----------|-------------|---------------|
| **N-Quads** | `.nq` | Line-based, includes graph context | **Full quad support** |
| **TriG** | `.trig` | Turtle with named graphs | **Full quad support** |
| Turtle | `.ttl` | Human-readable, compact notation | Default graph only |
| N-Triples | `.nt` | Simple line-based format | Default graph only |
| RDF/XML | `.rdf` | XML-based (requires optional dependency) | Default graph only |
| JSON-LD | `.jsonld` | JSON-based (requires optional dependency) | Default graph only |

**Recommended formats for quad store:**
- **N-Quads (`.nq`)**: Best for streaming and large datasets
- **TriG (`.trig`)**: Best for human-readable data with named graphs

The format is auto-detected from the file extension, or you can specify it explicitly:

```elixir
# Auto-detection
{:ok, count} = TripleStore.load(store, "data.nq")

# Explicit format
{:ok, count} = TripleStore.load(store, "data.dat", format: :nquads)
```

### N-Quads Format

N-Quads extends N-Triples with a fourth position for the graph:

```
<subject> <predicate> <object> <graph> .
<http://example.org/alice> <http://xmlns.com/foaf/0.1/name> "Alice" <http://example.org/people> .
```

### TriG Format

TriG extends Turtle with named graphs:

```
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

<http://example.org/people> {
  <http://example.org/alice> a foaf:Person ;
                         foaf:name "Alice" .
}
```

## Using RDF.ex Terms

TripleStore uses [RDF.ex](https://hex.pm/packages/rdf) for RDF data structures. Here's a quick reference:

### IRIs

```elixir
import RDF.Sigils

# Using sigil
~I<http://example.org/resource>

# Using function
RDF.iri("http://example.org/resource")

# With namespace
alias RDF.NS.RDFS
RDFS.label  # => ~I<http://www.w3.org/2000/01/rdf-schema#label>
```

### Literals

```elixir
import RDF.Sigils

# Plain literal
~L"Hello, World!"

# Typed literal
RDF.literal(42)           # xsd:integer
RDF.literal(3.14)         # xsd:double
RDF.literal(true)         # xsd:boolean
RDF.literal(~D[2024-01-15])  # xsd:date

# Language-tagged literal
RDF.literal("Bonjour", language: "fr")
```

### Blank Nodes

```elixir
import RDF.Sigils

~B<b1>  # Named blank node
RDF.bnode()  # Anonymous blank node
```

## Error Handling

All functions return tagged tuples:

```elixir
case TripleStore.query(store, sparql) do
  {:ok, results} ->
    # Handle success
    process_results(results)

  {:error, {:parse_error, message}} ->
    # Invalid SPARQL syntax
    IO.puts("Parse error: #{message}")

  {:error, :timeout} ->
    # Query took too long
    IO.puts("Query timed out")

  {:error, reason} ->
    # Other errors
    IO.puts("Error: #{inspect(reason)}")
end
```

### Bang Variants

For scripts or when you want exceptions on error:

```elixir
# Raises TripleStore.Error on failure
store = TripleStore.open!("./my_database", schema: :quad)
count = TripleStore.load!(store, "data.nq")
results = TripleStore.query!(store, "SELECT * WHERE { ?s ?p ?o }")
```

## Tips

### Use Prefixes in SPARQL

Define prefixes to make queries more readable:

```elixir
TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  PREFIX ex: <http://example.org/>

  SELECT ?name
  WHERE {
    GRAPH ex:people {
      ?person a foaf:Person ;
              foaf:name ?name .
    }
  }
""")
```

### Batch Loading for Performance

When loading large datasets, use batch loading:

```elixir
# Load with larger batch size for better throughput
{:ok, count} = TripleStore.load(store, "large_file.nq", batch_size: 10_000)
```

### Check Health Before Critical Operations

```elixir
{:ok, health} = TripleStore.health(store)

if health.status == :healthy do
  # Safe to proceed
  perform_critical_operation(store)
else
  Logger.warn("Store health: #{health.status}")
end
```

## Choosing Between Triple and Quad Store

### Use Quad Store (Recommended) when:
- You need data isolation (multi-tenancy)
- You want to track data provenance
- You have multiple data sources to merge
- You need graph-scoped reasoning
- You're starting a new project

### Use Triple Store when:
- You have a simple, single-context dataset
- Maximum write performance is critical
- You don't need named graphs
- You're migrating from an existing triple store system

## Next Steps

- [Named Graphs](07-named-graphs.md) - Advanced named graph management
- [Data Management](02-data-management.md) - Loading, exporting, and backing up quads
- [SPARQL Queries](03-sparql-queries.md) - Query syntax with GRAPH clauses
- [SPARQL Updates](04-sparql-updates.md) - Modifying data with SPARQL
- [Reasoning](05-reasoning.md) - Graph-scoped OWL 2 RL inference
- [Configuration & Performance](06-configuration.md) - Tuning your quad store
