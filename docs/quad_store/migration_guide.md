# Migration Guide: Triple Store to Quad Store

## Overview

This guide helps you migrate from the triple store to the quad store, understanding the differences in API, query patterns, and capabilities.

## Key Differences

### Data Model

| Aspect | Triple Store | Quad Store |
|--------|-------------|------------|
| Data Unit | Triple `{s, p, o}` | Quad `{s, p, o, g}` |
| Graph Context | Implicit default graph | Explicit named graphs |
| Indices | 3 (SPO, POS, OSP) | 4 (GSPO, GPOS, SPOG, POSG) |
| Loading | Turtle, N-Triples | N-Quads, TriG |
| Query Scope | Single graph | Multi-graph with GRAPH clause |

### API Changes

```elixir
# TRIPLE STORE - Insert
TripleStore.Operations.insert_triple(db, {s, p, o})

# QUAD STORE - Insert to default graph
TripleStore.QuadOperations.insert_quad(db, {s, p, o, 0})

# QUAD STORE - Insert to named graph
{:ok, g} = TripleStore.Dictionary.Manager.encode(manager, {:named_node, "http://example.org/graph"})
TripleStore.QuadOperations.insert_quad(db, {s, p, o, g})
```

## Migration Patterns

### Pattern 1: Keep Using Default Graph

If you don't need named graphs, the quad store works as a drop-in replacement:

```elixir
# Before (triple store)
{:ok, db} = TripleStore.Backend.RocksDB.NIF.open(path)
TripleStore.Operations.insert_triple(db, {1, 2, 3})

# After (quad store - using default graph)
{:ok, db} = TripleStore.Backend.RocksDB.NIF.open(path, schema: :quad)
TripleStore.QuadOperations.insert_quad(db, {1, 2, 3, 0})  # 0 = default graph
```

### Pattern 2: Move All Data to Single Named Graph

```elixir
# Before: Implicit default graph
query = "SELECT ?s WHERE { ?s a ex:Type }"

# After: Explicit named graph
query = """
  SELECT ?s WHERE {
    GRAPH <http://example.org/main> {
      ?s a ex:Type
    }
  }
"""
```

### Pattern 3: Partition Data by Type into Graphs

```elixir
# Before: All data in one place, filter with predicate
{:ok, db} = TripleStore.Backend.RocksDB.NIF.open(path)
# ... load all data ...

query = """
  SELECT ?s WHERE {
    ?s a ex:User .
    ?s ex:role ?role .
    FILTER(?role = ex:Admin)
  }
"""

# After: Partition by role into graphs
{:ok, db} = TripleStore.Backend.RocksDB.NIF.open(path, schema: :quad)
# Users in ex:users, admins in ex:admins ...

query = """
  SELECT ?s WHERE {
    GRAPH ex:admins {
      ?s a ex:User .
      ?s ex:role ex:Admin .
    }
  }
"""
```

### Pattern 4: Add Provenance with Named Graphs

```elixir
# Before: No provenance tracking
TripleStore.Operations.insert_triple(db, {s, p, o})

# After: Track data source
source_graph = "http://example.org/source/#{source_id}"
{:ok, g} = TripleStore.Dictionary.Manager.encode(manager, {:named_node, source_graph})
TripleStore.QuadOperations.insert_quad(db, {s, p, o, g})

# Query with provenance
query = """
  PREFIX prov: <http://www.w3.org/ns/prov#>

  SELECT ?s ?p ?o ?source WHERE {
    GRAPH ?source {
      ?s ?p ?o .
    }
    FILTER(STRENDS(STR(?source), "/source/import-2024"))
  }
"""
```

## Query Pattern Changes

### SELECT Queries

```elixir
# Before: Implicit default graph
query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"

# After: Explicit default or named graph
query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"  # Still works (queries default)
# OR
query = "SELECT ?s ?p ?o WHERE { GRAPH ex:main { ?s ?p ?o } }"
```

### INSERT DATA

```elixir
# Before
update = "INSERT DATA { <s> <p> \"o\" }"

# After - to default graph
update = "INSERT DATA { <s> <p> \"o\" }"  # Same syntax

# After - to named graph
update = "INSERT DATA { GRAPH ex:graph { <s> <p> \"o\" } }"
```

### DELETE DATA

```elixir
# Before
update = "DELETE DATA { <s> <p> \"o\" }"

# After - from default graph
update = "DELETE DATA { <s> <p> \"o\" }"  # Same syntax

# After - from named graph
update = "DELETE DATA { GRAPH ex:graph { <s> <p> \"o\" } }"
```

### Loading Files

```elixir
# Before - Load Turtle (triples)
{:ok, count} = TripleStore.Loader.load_file(db, manager, "data.ttl")

# After - Load N-Quads (quads)
{:ok, count} = TripleStore.Loader.load_file(db, manager, "data.nq")

# After - Load TriG (quads with named graphs)
{:ok, count} = TripleStore.Loader.load_file(db, manager, "data.trig", format: :trig)
```

## Data Migration Strategy

### Step 1: Export from Triple Store

```elixir
# Export all data as N-Triples
{:ok, data} = TripleStore.Exporter.export(db, manager, format: :ntriples)
File.write!("backup.nt", data)
```

### Step 2: Convert to N-Quads

```bash
# Add default graph context to each line
awk '{ print $0 " <http://www.w3.org/ns/graphs/default> ." }' backup.nt > backup.nq
```

Or programmatically:

```elixir
import TripleStore.Loader

# Read and convert
triples = File.read!("backup.nt")
quads = triples
  |> String.split("\n", trim: true)
  |> Enum.map(fn line ->
    if String.ends_with?(line, ".") do
      String.replace(line, ~s/.$/, "") <> " <http://www.w3.org/ns/graphs/default> ."
    else
      line
    end
  end)
  |> Enum.join("\n")

File.write!("backup.nq", quads)
```

### Step 3: Import to Quad Store

```elixir
# Open quad store database
{:ok, quad_db} = TripleStore.Backend.RocksDB.NIF.open(
  quad_path,
  schema: :quad
)
{:ok, quad_manager} = TripleStore.Dictionary.Manager.start_link(db: quad_db)

# Load converted data
{:ok, count} = TripleStore.Loader.load_file(
  quad_db,
  quad_manager,
  "backup.nq"
)
```

## Backwards Compatibility

The quad store maintains backwards compatibility for:

- **SPARQL Queries**: Queries without GRAPH clause work on default graph
- **INSERT DATA**: Without GRAPH goes to default graph
- **DELETE DATA**: Without GRAPH affects default graph
- **CONSTRUCT/DESCRIBE**: Work on default graph

## Known Limitations

1. **Property Paths**: Advanced property paths (`*`, `+`, `^`) not fully supported
2. **COUNT(*)**: Use `COUNT(?variable)` instead
3. **STR() on Graph Variables**: Not yet supported

## Testing Your Migration

Create a test to verify migration:

```elixir
defmodule MigrationTest do
  use ExUnit.Case

  test "triple store queries work on quad store" do
    # Setup triple store
    {:ok, triple_db} = NIF.open("/tmp/triple_test", schema: :triple)
    {:ok, triple_mgr} = Manager.start_link(db: triple_db)

    # Load test data
    TripleStore.Loader.load_trig_string(triple_db, triple_mgr, """
      @prefix ex: <http://example.org/> .
      ex:s1 ex:p "o1" .
    """)

    # Query triple store
    query = "SELECT ?s WHERE { ?s ex:p ?o }"
    {:ok, triple_results} = TripleStore.SPARQL.Query.query(
      %{db: triple_db, dict_manager: triple_mgr},
      query
    )

    # Setup quad store
    {:ok, quad_db} = NIF.open("/tmp/quad_test", schema: :quad)
    {:ok, quad_mgr} = Manager.start_link(db: quad_db)

    # Load same data
    TripleStore.Loader.load_trig_string(quad_db, quad_mgr, """
      @prefix ex: <http://example.org/> .
      ex:s1 ex:p "o1" .
    """)

    # Query quad store (same query)
    {:ok, quad_results} = TripleStore.SPARQL.Query.query(
      %{db: quad_db, dict_manager: quad_mgr},
      query
    )

    # Results should be identical
    assert triple_results == quad_results
  end
end
```

## Performance Comparison

| Operation | Triple Store | Quad Store | Notes |
|-----------|-------------|-----------|-------|
| Insert single triple/quad | ~0.1ms | ~0.1ms | Similar performance |
| Batch insert (1K) | ~50ms | ~55ms | Slightly slower (4 indices) |
| Pattern match | ~1ms | ~1ms | Similar with index |
| Graph-scoped query | N/A | ~1ms | New capability |
| Cross-graph query | N/A | ~5-50ms | New capability |

## Checklist

- [ ] Identify all data loading code
- [ ] Update database open calls to use `schema: :quad`
- [ ] Convert Turtle/N-Triples files to N-Quads/TriG
- [ ] Update INSERT DATA queries to use GRAPH
- [ ] Update query patterns to use GRAPH clause
- [ ] Add graph management to data access layer
- [ ] Update documentation
- [ ] Test all queries on quad store
- [ ] Performance test critical paths
- [ ] Plan rollback strategy
