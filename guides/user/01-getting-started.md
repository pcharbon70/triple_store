# Getting Started

This guide will help you get up and running with TripleStore quickly.

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

The first compilation will build the Rust NIFs for RocksDB:

```bash
mix compile
```

> **Note**: You need Rust installed for NIF compilation. Install via [rustup](https://rustup.rs/).

## Quick Start

### Opening a Store

```elixir
# Open or create a store
{:ok, store} = TripleStore.open("./my_database")

# The store is now ready for use
```

The database directory will be created if it doesn't exist.

### Loading Data

Load RDF data from a file:

```elixir
# Load Turtle file
{:ok, count} = TripleStore.load(store, "data.ttl")
IO.puts("Loaded #{count} triples")

# Load N-Triples
{:ok, count} = TripleStore.load(store, "data.nt")
```

Or insert triples directly:

```elixir
# Using RDF.ex sigils
import RDF.Sigils

{:ok, 1} = TripleStore.insert(store, {
  ~I<http://example.org/alice>,
  ~I<http://example.org/knows>,
  ~I<http://example.org/bob>
})
```

### Querying with SPARQL

Execute SPARQL queries:

```elixir
# SELECT query
{:ok, results} = TripleStore.query(store, """
  SELECT ?person ?name
  WHERE {
    ?person <http://example.org/name> ?name
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

Here's a complete example demonstrating common operations:

```elixir
defmodule MyApp.Example do
  import RDF.Sigils

  def run do
    # 1. Open the store
    {:ok, store} = TripleStore.open("./example_db")

    # 2. Define some data
    data = """
    @prefix ex: <http://example.org/> .
    @prefix foaf: <http://xmlns.com/foaf/0.1/> .

    ex:alice a foaf:Person ;
             foaf:name "Alice" ;
             foaf:age 30 ;
             foaf:knows ex:bob .

    ex:bob a foaf:Person ;
           foaf:name "Bob" ;
           foaf:age 25 .
    """

    # 3. Load the data
    {:ok, count} = TripleStore.load_string(store, data, :turtle)
    IO.puts("Loaded #{count} triples")

    # 4. Query for people
    {:ok, results} = TripleStore.query(store, """
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      SELECT ?person ?name ?age
      WHERE {
        ?person a foaf:Person ;
                foaf:name ?name ;
                foaf:age ?age .
      }
      ORDER BY ?name
    """)

    IO.puts("\nPeople in the database:")
    for row <- results do
      IO.puts("  #{row["name"]} (age #{row["age"]})")
    end

    # 5. Add a new person
    {:ok, _} = TripleStore.update(store, """
      PREFIX ex: <http://example.org/>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      INSERT DATA {
        ex:charlie a foaf:Person ;
                   foaf:name "Charlie" ;
                   foaf:age 35 .
      }
    """)

    # 6. Check count
    {:ok, stats} = TripleStore.stats(store)
    IO.puts("\nTotal triples: #{stats.triple_count}")

    # 7. Clean up
    :ok = TripleStore.close(store)
  end
end
```

## Supported RDF Formats

TripleStore supports these formats for loading and exporting:

| Format | Extension | Description |
|--------|-----------|-------------|
| Turtle | `.ttl` | Human-readable, compact notation |
| N-Triples | `.nt` | Simple line-based format, good for streaming |
| N-Quads | `.nq` | N-Triples with named graphs |
| TriG | `.trig` | Turtle with named graphs |
| RDF/XML | `.rdf` | XML-based (requires optional dependency) |
| JSON-LD | `.jsonld` | JSON-based (requires optional dependency) |

The format is auto-detected from the file extension, or you can specify it explicitly:

```elixir
{:ok, count} = TripleStore.load(store, "data.xml", format: :rdfxml)
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
store = TripleStore.open!("./my_database")
count = TripleStore.load!(store, "data.ttl")
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
    ?person a foaf:Person ;
            foaf:name ?name .
  }
""")
```

### Batch Loading for Performance

When loading large datasets, use batch loading:

```elixir
# Load with larger batch size for better throughput
{:ok, count} = TripleStore.load(store, "large_file.nt", batch_size: 10_000)
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

## Next Steps

- [Data Management](02-data-management.md) - Loading, exporting, and backing up data
- [SPARQL Queries](03-sparql-queries.md) - Query syntax and examples
- [SPARQL Updates](04-sparql-updates.md) - Modifying data with SPARQL
- [Reasoning](05-reasoning.md) - Using OWL 2 RL inference
- [Configuration & Performance](06-configuration.md) - Tuning your store
