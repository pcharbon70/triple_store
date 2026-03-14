# Getting Started

This guide will help you get up and running with TripleStore quickly.

## Triple Store vs Quad Store

TripleStore supports two storage schemas:

| Feature | Triple Store (v1) | Quad Store (v2) |
|---------|-------------------|-----------------|
| **Data Model** | `{subject, predicate, object}` | `{graph, subject, predicate, object}` |
| **Named Graphs** | No (implicit default) | Yes (explicit graphs) |
| **Indices** | 3 (SPO, POS, OSP) | 4 (GSPO, GPOS, SPOG, POSG) |
| **Write Speed** | Faster (~33% faster writes) | Slower (more indices) |
| **Use Case** | Simple datasets | Multi-tenancy, provenance, data isolation |

## Which Should You Choose?

### Choose Triple Store when:
- You have a simple, single-context dataset
- Maximum write performance is critical
- You don't need named graphs
- You're migrating from an existing triple store system

### Choose Quad Store when:
- You need data isolation (multi-tenancy)
- You want to track data provenance
- You have multiple data sources to merge
- You need graph-scoped reasoning
- You're starting a new project

**Recommendation**: Use quad store (`schema: :quad`) for new projects. It supports everything triple store does plus named graphs for data isolation and provenance tracking.

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
>
> You also need a Rust toolchain because `mix compile` builds the SPARQL parser
> NIF from `native/sparql_parser_nif` into `priv/native/`. That binary is a
> local build artifact and should remain untracked. If it becomes stale after a
> branch or toolchain change, remove `priv/native/sparql_parser_nif.so` and rerun
> `mix compile --force`.

## Quick Start

### Triple Store Quick Start

```elixir
# Open a triple store
{:ok, store} = TripleStore.open("./my_database", schema: :triple)

# Insert some triples
TripleStore.update(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  INSERT DATA {
    <http://example.org/alice> a foaf:Person ;
                              foaf:name "Alice" .
  }
""")

# Query
{:ok, results} = TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  SELECT ?name
  WHERE {
    ?person foaf:name ?name
  }
""")

# Close when done
TripleStore.close(store)
```

### Quad Store Quick Start

```elixir
# Open a quad store
{:ok, store} = TripleStore.open("./my_database", schema: :quad)

# Insert into a named graph
TripleStore.update(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  INSERT DATA {
    GRAPH <http://example.org/people> {
      <http://example.org/alice> a foaf:Person ;
                                foaf:name "Alice" .
    }
  }
""")

# Query a specific graph
{:ok, results} = TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>

  SELECT ?name
  WHERE {
    GRAPH <http://example.org/people> {
      ?person foaf:name ?name
    }
  }
""")

# Close when done
TripleStore.close(store)
```

## Continue Your Journey

### Triple Store Path

If you chose **triple store**, continue to the triple-specific guides:

1. [Data Management](triples/02-data-management.md) - Loading, exporting, and backing up triples
2. [SPARQL Queries](triples/03-sparql-queries.md) - Querying your data
3. [SPARQL Updates](triples/04-sparql-updates.md) - Modifying data with SPARQL
4. [Reasoning](triples/05-reasoning.md) - OWL 2 RL inference
5. [Configuration & Performance](triples/06-configuration.md) - Tuning your triple store

### Quad Store Path

If you chose **quad store**, continue to the quad-specific guides:

1. [Data Management](quads/02-data-management.md) - Loading, exporting, and backing up quads with named graphs
2. [SPARQL Queries](quads/03-sparql-queries.md) - Querying with GRAPH clauses
3. [SPARQL Updates](quads/04-sparql-updates.md) - Modifying data in named graphs
4. [Reasoning](quads/05-reasoning.md) - Graph-scoped OWL 2 RL inference
5. [Configuration & Performance](quads/06-configuration.md) - Quad-specific tuning
6. [Named Graphs](quads/07-named-graphs.md) - Advanced named graph patterns

## Supported RDF Formats

TripleStore supports these formats for loading and exporting:

| Format | Extension | Triple Store | Quad Store |
|--------|-----------|--------------|------------|
| **N-Triples** | `.nt` | Yes | Default graph only |
| **Turtle** | `.ttl` | Yes | Default graph only |
| **N-Quads** | `.nq` | Loads to default | Full quad support |
| **TriG** | `.trig` | Loads to default | Full quad support |
| **RDF/XML** | `.rdf` | Yes | Default graph only |

## Using RDF.ex Terms

TripleStore uses [RDF.ex](https://hex.pm/packages/rdf) for RDF data structures:

```elixir
import RDF.Sigils

# IRIs
~I<http://example.org/resource>
RDF.iri("http://example.org/resource")

# Literals
~L"Hello, World!"
RDF.literal(42)           # xsd:integer
RDF.literal(3.14)         # xsd:double
RDF.literal("Bonjour", language: "fr")

# Blank Nodes
~B<b1>
RDF.bnode()
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

```elixir
TripleStore.query(store, """
  PREFIX foaf: <http://xmlns.com/foaf/0.1/>
  PREFIX ex: <http://example.org/>

  SELECT ?name
  WHERE {
    ?person foaf:name ?name
  }
""")
```

### Check Your Schema

```elixir
{:ok, schema} = TripleStore.schema(store)
IO.puts("Schema: #{schema}")  # => :triple or :quad
```

### Batch Loading for Performance

```elixir
# Larger batches = fewer commits, faster loading
{:ok, count} = TripleStore.load(store, "large_file.nt", batch_size: 10_000)
```
