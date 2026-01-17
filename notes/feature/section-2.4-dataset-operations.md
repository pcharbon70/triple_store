# Working Plan: Section 2.4 - Dataset Operations

## Branch: `feature/section-2.4-dataset-operations`

## Status: COMPLETED

## Overview

Section 2.4 implements dataset operations for managing named graphs in the quad store. This includes listing graphs, checking existence, deleting graphs, copying between graphs, and getting per-graph statistics.

---

## Part 1: Analysis (COMPLETED)

- [x] Review current QuadOperations module for existing graph operations
- [x] Review existing dictionary/manager for graph ID handling
- [x] Understand the quad index structure (GSPO, GPOS, etc.)
- [x] Determine the best module to place these operations (likely QuadOperations)

---

## Part 2: Implementation Tasks (ALL COMPLETED)

### 2.4.1 Graph Enumeration (COMPLETED)

**Module:** `lib/triple_store/quad_operations.ex`

**Tasks:**
- [x] 2.4.1.1 Implement `list_graphs/2` returning list of graph names
- [x] 2.4.1.2 Use GSPO index to find distinct graph IDs
- [x] 2.4.1.3 Exclude default graph from listing (configurable)
- [x] 2.4.1.4 Convert graph IDs to terms via dictionary
- [x] 2.4.1.5 Return as list of RDF.IRI or RDF.BlankNode terms

**API Design:**
```elixir
@spec list_graphs(db_ref(), keyword()) :: {:ok, [RDF.IRI.t() | RDF.BlankNode.t()]} | {:error, term()}
# Options: include_default: false
```

### 2.4.2 Graph Existence (COMPLETED)

**Module:** Same as above

**Tasks:**
- [x] 2.4.2.1 Implement `graph_exists?/3` for named graph check (requires manager)
- [x] 2.4.2.2 Implement `default_graph_exists?/1` for default graph
- [x] 2.4.2.3 Use GSPO prefix scan to check for any quad in graph
- [x] 2.4.2.4 Return boolean result

**API Design:**
```elixir
@spec graph_exists?(db_ref(), TripleStore.Dictionary.Manager.manager(), RDF.IRI.t() | RDF.BlankNode.t()) :: boolean()
@spec default_graph_exists?(db_ref()) :: boolean()
```

### 2.4.3 Graph Deletion (COMPLETED)

**Module:** Same as above

**Tasks:**
- [x] 2.4.3.1 Implement `delete_graph/3` removing all quads from graph
- [x] 2.4.3.2 Use GSPO prefix scan to find all quads in graph
- [x] 2.4.3.3 Delete from all four indices atomically
- [x] 2.4.3.4 Handle default graph deletion (clear all data)
- [x] 2.4.3.5 Return count of quads deleted

**API Design:**
```elixir
@spec delete_graph(db_ref(), TripleStore.Dictionary.Manager.manager(), RDF.IRI.t() | RDF.BlankNode.t() | :default) :: {:ok, non_neg_integer()} | {:error, term()}
# Accepts RDF.IRI, RDF.BlankNode, or :default
```

### 2.4.4 Graph Copying (COMPLETED)

**Module:** Same as above

**Tasks:**
- [x] 2.4.4.1 Implement `copy_graph/5` copying quads from source to target graph
- [x] 2.4.4.2 Use GSPO prefix scan on source graph
- [x] 2.4.4.3 Insert quads with new graph ID
- [x] 2.4.4.4 Handle graph already exists (merge/replace options)
- [x] 2.4.4.5 Return count of quads copied

**API Design:**
```elixir
@spec copy_graph(db_ref(), TripleStore.Dictionary.Manager.manager(), graph_term(), graph_term(), keyword()) ::
        {:ok, non_neg_integer()} | {:error, term()}
# Options: on_conflict: :merge | :replace | :error
```

### 2.4.5 Graph Statistics (COMPLETED)

**Module:** Same as above

**Tasks:**
- [x] 2.4.5.1 Implement `graph_quad_count/3` returning count of quads in graph
- [x] 2.4.5.2 Implement `graphs_summary/2` returning stats for all graphs
- [x] 2.4.5.3 Return map: `%{graph_term => quad_count}`
- [x] 2.4.5.4 Include default graph in summary (configurable)

**API Design:**
```elixir
@spec graph_quad_count(db_ref(), TripleStore.Dictionary.Manager.manager(), graph_term()) :: {:ok, non_neg_integer()} | {:error, term()}
@spec graphs_summary(db_ref(), keyword()) :: {:ok, %{(RDF.IRI.t() | RDF.BlankNode.t() | :default) => non_neg_integer()}} | {:error, term()}
# Options: include_default: true
```

---

## Part 3: Tests (ALL COMPLETED)

### Test File: `test/triple_store/dataset_operations_test.exs` (new file)

- [x] 2.4.6.1 Test list_graphs returns all named graphs
- [x] 2.4.6.2 Test list_graphs excludes default graph by default
- [x] 2.4.6.3 Test list_graphs includes default graph with option
- [x] 2.4.6.4 Test list_graphs returns empty list when no graphs
- [x] 2.4.6.5 Test graph_exists? returns true for existing graph
- [x] 2.4.6.6 Test graph_exists? returns false for non-existent graph
- [x] 2.4.6.7 Test default_graph_exists? returns true when data present
- [x] 2.4.6.8 Test default_graph_exists? returns false when empty
- [x] 2.4.6.9 Test delete_graph removes all quads from graph
- [x] 2.4.6.10 Test delete_default_graph clears all data
- [x] 2.4.6.11 Test delete_graph returns count of deleted quads
- [x] 2.4.6.12 Test delete_graph on non-existent graph returns ok with 0
- [x] 2.4.6.13 Test copy_graph duplicates quads correctly
- [x] 2.4.6.14 Test copy_graph to existing graph merges by default
- [x] 2.4.6.15 Test copy_graph with replace option clears target first
- [x] 2.4.6.16 Test copy_graph with error option fails if target exists
- [x] 2.4.6.17 Test graph_quad_count returns accurate count
- [x] 2.4.6.18 Test graph_quad_count for non-existent graph returns 0
- [x] 2.4.6.19 Test graphs_summary returns correct statistics
- [x] 2.4.6.20 Test graphs_summary includes/excludes default graph based on option

**Test Results: 31 tests, 0 failures**

---

## Dependencies

- `TripleStore.QuadOperations` - For quad operations
- `TripleStore.Dictionary.Manager` - For graph ID to term conversion
- Quad index structure (GSPO for graph prefix scans)
- RocksDB NIF for storage operations

---

## Success Criteria

1. list_graphs returns all named graphs
2. graph_exists? correctly reports graph existence
3. delete_graph removes all quads from specified graph
4. copy_graph copies quads between graphs
5. graph_quad_count returns accurate counts
6. graphs_summary returns per-graph statistics
7. All tests pass

---

## Notes

- Graph terms can be RDF.IRI, RDF.BlankNode, or :default atom
- Default graph has ID 0 in storage
- Need to convert between graph terms and storage IDs
- Operations should be atomic where possible
- Consider performance for large datasets (use prefix scans efficiently)
