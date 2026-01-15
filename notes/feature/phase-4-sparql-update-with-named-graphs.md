# Phase 4: SPARQL UPDATE with Named Graphs

**Status:** 🚧 Planning
**Branch:** `quad`
**Previous:** Phase 3 (SPARQL Query Execution) - Complete

## Overview

Phase 4 extends SPARQL UPDATE operations to support named graphs. SPARQL UPDATE includes operations like INSERT DATA, DELETE DATA, DELETE/INSERT, CLEAR, DROP, CREATE, COPY, MOVE, and ADD - all of which need to work with named graphs.

## Sections

### 4.1 UPDATE Statement Parsing with Graph Context
**Goal:** Ensure SPARQL UPDATE parser handles graph-targeted operations.

**Tasks:**
- [ ] Verify parser handles `GRAPH <iri> { INSERT DATA ... }`
- [ ] Verify parser handles `DELETE DATA { ... }` with default/named graph
- [ ] Verify parser handles `WITH <iri> DELETE/INSERT ...`
- [ ] Verify parser handles `USING NAMED` clauses
- [ ] Add AST nodes for graph-scoped updates

**Files:**
- `lib/triple_store/sparql/parser.ex` (check existing)
- `lib/triple_store/sparql/update.ex` (may need extension)

**Tests:**
- Parse INSERT DATA with graph target
- Parse DELETE DATA with graph target
- Parse DELETE/INSERT with USING clause
- Parse CLEAR/DROP/CREATE with graph IRI
- Parse COPY/MOVE/ADD between graphs

---

### 4.2 INSERT DATA with Named Graphs
**Goal:** Support inserting triples into specific named graphs.

**Tasks:**
- [ ] Add `insert_data_to_graph/3` function
- [ ] Handle `GRAPH <iri> { INSERT DATA ... }` syntax
- [ ] Convert triples to quads with graph context
- [ ] Validate graph IRI before insertion
- [ ] Check write authorization for target graph
- [ ] Emit telemetry for insert operations

**API:**
```elixir
def insert_data_to_graph(db, dict_manager, graph_iri, triples) do
  # Convert triples to quads
  # Validate graph IRI
  # Check write authorization
  # Insert using quad_operations
end
```

**Files:**
- `lib/triple_store/sparql/update_executor.ex` (NEW or extend)

**Tests:**
- Insert single triple to named graph
- Insert multiple triples to named graph
- Insert to default graph
- Insert to multiple graphs in one request
- Error: invalid graph IRI
- Error: unauthorized write

---

### 4.3 DELETE DATA with Named Graphs
**Goal:** Support deleting triples from specific named graphs.

**Tasks:**
- [ ] Add `delete_data_from_graph/3` function
- [ ] Handle `GRAPH <iri> { DELETE DATA ... }` syntax
- [ ] Match quads by pattern with graph context
- [ ] Support deletion from default graph
- [ ] Check write authorization for target graph
- [ ] Emit telemetry for delete operations

**API:**
```elixir
def delete_data_from_graph(db, dict_manager, graph_iri, triple_patterns) do
  # Convert patterns to quads
  # Find matching quads in specified graph
  # Delete using quad_operations
end
```

**Files:**
- `lib/triple_store/sparql/update_executor.ex`

**Tests:**
- Delete specific triple from named graph
- Delete with wildcard patterns
- Delete from default graph
- Delete from multiple graphs
- Error: graph doesn't exist
- Error: unauthorized delete

---

### 4.4 DELETE/INSERT with Graph Clauses
**Goal:** Support DELETE/INSERT operations with GRAPH clauses.

**Tasks:**
- [ ] Add `execute_delete_insert_with_graph/5` function
- [ ] Handle `WITH <iri>` for default graph context
- [ ] Handle `USING <iri>` and `USING NAMED <iri>` clauses
- [ ] Process DELETE templates with graph binding
- [ ] Process INSERT templates with graph binding
- [ ] Support GRAPH clause in WHERE pattern
- [ ] Transactional execution (all or nothing)

**API:**
```elixir
def execute_delete_insert_with_graph(
  db, dict_manager,
  delete_template,
  insert_template,
  where_clause,
  opts
) do
  # Evaluate WHERE with graph bindings
  # Instantiate DELETE template with bindings
  # Instantiate INSERT template with bindings
  # Execute delete then insert atomically
end
```

**Files:**
- `lib/triple_store/sparql/update_executor.ex`
- `lib/triple_store/quad_operations.ex` (may need batch delete)

**Tests:**
- DELETE/INSERT with GRAPH ?g
- DELETE/INSERT with named graph in WHERE
- DELETE/INSERT with USING clause
- Delete from one graph, insert to another
- Transaction rollback on error

---

### 4.5 CLEAR/DROP/CREATE with Named Graphs
**Goal:** Support graph-level management operations.

**Tasks:**
- [ ] Add `clear_graph/3` - remove all triples from graph
- [ ] Add `drop_graph/3` - remove graph and all triples
- [ ] Add `create_graph/3` - create empty named graph
- [ ] Support `CLEAR GRAPH <iri>`
- [ ] Support `DROP GRAPH <iri>`
- [ ] Support `CREATE GRAPH <iri>` (Silent operation if exists)
- [ ] Check authorization for each operation
- [ ] Emit telemetry events

**API:**
```elixir
def clear_graph(db, dict_manager, graph_iri) do
  # Remove all quads with graph context
end

def drop_graph(db, dict_manager, graph_iri) do
  # Remove all quads AND graph metadata
end

def create_graph(db, dict_manager, graph_iri) do
  # Create empty graph (no-op if exists per SPARQL spec)
end
```

**Files:**
- `lib/triple_store/quad_operations.ex` (extend existing)
- `lib/triple_store/sparql/update_executor.ex`

**Tests:**
- CLEAR GRAPH removes all triples
- DROP GRAPH removes triples and graph entry
- CREATE GRAPH creates empty graph
- CREATE is idempotent (no error if exists)
- Error: unauthorized CLEAR/DROP/CREATE

---

### 4.6 COPY/MOVE/ADD between Graphs
**Goal:** Support bulk operations between graphs.

**Tasks:**
- [ ] Add `copy_graph/5` - copy all triples from source to target
- [ ] Add `move_graph/5` - move triples from source to target
- [ ] Add `add_to_graph/5` - copy without clearing target
- [ ] Support conflict handling (replace/merge/error)
- [ ] Check authorization for both source and target
- [ ] Optimize for large graphs (batch operations)
- [ ] Emit telemetry events

**API:**
```elixir
def copy_graph(db, dict_manager, source_graph, target_graph, opts) do
  # Copy all quads from source to target
  # on_conflict: :replace | :merge | :error
end

def move_graph(db, dict_manager, source_graph, target_graph, opts) do
  # Copy source to target, then clear source
end

def add_to_graph(db, dict_manager, source_graph, target_graph, opts) do
  # Copy without clearing target
end
```

**Files:**
- `lib/triple_store/quad_operations.ex` (has copy_graph, may need updates)

**Tests:**
- COPY all triples to new graph
- COPY with conflict resolution
- MOVE empties source graph
- ADD merges with target
- Error: source graph doesn't exist
- Error: unauthorized operation

---

### 4.7 UPDATE Unit Tests
**Goal:** Comprehensive test coverage for all UPDATE operations.

**Tasks:**
- [ ] Test file: `test/triple_store/sparql/update_test.exs`
- [ ] 4.1 tests: Parsing with graph context (5+ tests)
- [ ] 4.2 tests: INSERT DATA with graphs (6+ tests)
- [ ] 4.3 tests: DELETE DATA with graphs (6+ tests)
- [ ] 4.4 tests: DELETE/INSERT with GRAPH (5+ tests)
- [ ] 4.5 tests: CLEAR/DROP/CREATE (8+ tests)
- [ ] 4.6 tests: COPY/MOVE/ADD (6+ tests)
- [ ] Integration tests: multi-graph updates (3+ tests)

**Total Target:** 40+ tests

**Files:**
- `test/triple_store/sparql/update_test.exs` (NEW)
- `test/triple_store/sparql/update_integration_test.exs` (NEW)

---

## Dependencies

### Required Before Starting:
- ✅ Phase 3 complete (quad patterns, GRAPH clause execution)
- ✅ QuadOperations module (insert_quad, delete_quad)
- ✅ Authorization module (can_write?, can_admin?)

### External Dependencies:
- SPARQL parser must support UPDATE with graph syntax
- RocksDB column families for quad indices

## Success Criteria

1. All SPARQL UPDATE operations work with named graphs
2. Authorization checks enforced for graph modifications
3. All operations emit telemetry events
4. 40+ tests, all passing
5. No regression in existing UPDATE functionality

## Estimated Effort

- 4.1: 1 day (parser verification)
- 4.2: 2-3 days (INSERT DATA)
- 4.3: 2-3 days (DELETE DATA)
- 4.4: 3-4 days (DELETE/INSERT with GRAPH)
- 4.5: 1-2 days (CLEAR/DROP/CREATE)
- 4.6: 2-3 days (COPY/MOVE/ADD)
- 4.7: 2-3 days (testing)

**Total:** 2-3 weeks

## Notes

- SPARQL UPDATE specification: https://www.w3.org/TR/sparql11-update/
- GRAPH clause in UPDATE: allows targeting specific graphs
- USING clause: provides default graph for DELETE/INSERT patterns
- Transaction semantics: DELETE/INSERT should be atomic within a graph
