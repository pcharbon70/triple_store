# Phase 4: SPARQL UPDATE with Named Graphs

## Overview

Phase 4 implements SPARQL UPDATE operations with named graph support. By the end of this phase, all SPARQL UPDATE operations (INSERT DATA, DELETE DATA, MODIFY, CREATE/DROP/CLEAR GRAPH) will work correctly with named graphs.

The current UPDATE executor ignores graph components. This phase extends it to fully support graph-scoped updates.

---

## 4.1 Graph Management Operations

### 4.1.1 CREATE GRAPH

Implement CREATE GRAPH operation.

- [ ] 4.1.1.1 Implement `execute_create_graph/3` for CREATE GRAPH
- [ ] 4.1.1.2 Handle `CREATE GRAPH <iri>` creating empty named graph
- [ ] 4.1.1.3 Handle `CREATE SILENT GRAPH` ignoring existing graph
- [ ] 4.1.1.4 Use graph ID resolution to check existence
- [ ] 4.1.1.5 Return error on duplicate (unless SILENT)

### 4.1.2 DROP GRAPH

Implement DROP GRAPH operation.

- [ ] 4.1.2.1 Implement `execute_drop_graph/3` for DROP GRAPH
- [ ] 4.1.2.2 Handle `DROP GRAPH <iri>` removing entire graph
- [ ] 4.1.2.3 Handle `DROP SILENT GRAPH` ignoring missing graph
- [ ] 4.1.2.4 Use `delete_graph/2` from dataset operations
- [ ] 4.1.2.5 Return count of quads deleted

### 4.1.3 CLEAR GRAPH

Implement CLEAR GRAPH operation.

- [ ] 4.1.3.1 Implement `execute_clear_graph/3` for CLEAR GRAPH
- [ ] 4.1.3.2 Handle `CLEAR GRAPH <iri>` clearing named graph
- [ ] 4.1.3.3 Handle `CLEAR GRAPH :default` clearing default graph
- [ ] 4.1.3.4 Handle `CLEAR GRAPH :all` clearing all graphs
- [ ] 4.1.3.5 Use `delete_graph/2` for implementation

### 4.1.4 Graph Existence Validation

Add validation for graph operations.

- [ ] 4.1.4.1 Implement `validate_graph_exists/2` check
- [ ] 4.1.4.2 Implement `validate_graph_not_exists/2` check
- [ ] 4.1.4.3 Return appropriate error codes
- [ ] 4.1.4.4 Handle SILENT modifier (no error on failure)

---

## 4.2 INSERT DATA with Graphs

### 4.2.1 Single Graph INSERT

Implement INSERT DATA for single graph.

- [ ] 4.2.1.1 Implement `execute_insert_data_graph/4` with graph parameter
- [ ] 4.2.1.2 Handle `INSERT DATA { GRAPH <g> { ... } }`
- [ ] 4.2.1.3 Handle `INSERT DATA { ... }` (default graph)
- [ ] 4.2.1.4 Use `insert_quad/3` for each quad
- [ ] 4.2.1.5 Return count of quads inserted

### 4.2.2 Multi-Graph INSERT

Implement INSERT DATA across multiple graphs.

- [ ] 4.2.2.1 Handle `INSERT DATA { GRAPH <g1> { ... } GRAPH <g2> { ... } }`
- [ ] 4.2.2.2 Parse multiple GRAPH blocks in single INSERT
- [ ] 4.2.2.3 Insert each block to its respective graph
- [ ] 4.2.2.3 Return summary per graph
- [ ] 4.2.2.4 Use WriteBatch for atomicity across graphs

### 4.2.3 INSERT Error Handling

Add error handling for INSERT operations.

- [ ] 4.2.3.1 Handle duplicate quad detection
- [ ] 4.2.3.2 Handle non-existent graph (create or error?)
- [ ] 4.2.3.3 Handle invalid graph terms
- [ ] 4.2.3.4 Return detailed error messages
- [ ] 4.2.3.5 Support INSERT SILENT (ignore errors)

---

## 4.3 DELETE DATA with Graphs

### 4.3.1 Single Graph DELETE

Implement DELETE DATA for single graph.

- [ ] 4.3.1.1 Implement `execute_delete_data_graph/4` with graph parameter
- [ ] 4.3.1.2 Handle `DELETE DATA { GRAPH <g> { ... } }`
- [ ] 4.3.1.3 Handle `DELETE DATA { ... }` (default graph)
- [ ] 4.3.1.4 Use `delete_quad/3` for each quad
- [ ] 4.3.1.5 Return count of quads deleted

### 4.3.2 Multi-Graph DELETE

Implement DELETE DATA across multiple graphs.

- [ ] 4.3.2.1 Handle `DELETE DATA { GRAPH <g1> { ... } GRAPH <g2> { ... } }`
- [ ] 4.3.2.2 Parse multiple GRAPH blocks in single DELETE
- [ ] 4.3.2.3 Delete from each graph independently
- [ ] 4.3.2.4 Return summary per graph
- [ ] 4.3.2.5 Handle missing quads gracefully

### 4.3.3 DELETE Error Handling

Add error handling for DELETE operations.

- [ ] 4.3.3.1 Handle non-existent quad (no-op, not error)
- [ ] 4.3.3.2 Handle non-existent graph (error or no-op?)
- [ ] 4.3.3.3 Handle invalid graph terms
- [ ] 4.3.3.4 Return detailed error messages
- [ ] 4.3.3.5 Support DELETE SILENT (ignore errors)

---

## 4.4 MODIFY with Graphs

### 4.4.1 DELETE/INSERT with Graph

Implement graph-scoped MODIFY operation.

- [ ] 4.4.1.1 Implement `execute_modify_graph/6` with graph parameter
- [ ] 4.4.1.2 Handle `WITH <g> DELETE { ... } INSERT { ... } WHERE { ... }`
- [ ] 4.4.1.3 Handle `DELETE { GRAPH <g> { ... } } INSERT { ... }`
- [ ] 4.4.1.4 Use graph-scoped execution for WHERE clause
- [ ] 4.4.1.5 Apply delete/insert to specified graph

### 4.4.2 WHERE Clause with Graph

Handle WHERE clause evaluation in MODIFY.

- [ ] 4.4.2.1 Execute WHERE in specified graph context
- [ ] 4.4.2.2 Bindings include graph variable when applicable
- [ ] 4.4.2.3 Use quad pattern execution for WHERE
- [ ] 4.4.2.4 Pass graph context through execution
- [ ] 4.4.2.5 Handle cross-graph WHERE (with GRAPH in WHERE)

### 4.4.3 Template Instantiation with Graph

Handle INSERT template graph context.

- [ ] 4.4.3.1 Implement `instantiate_quad_template/3`
- [ ] 4.4.3.2 Use specified graph for inserted quads
- [ ] 4.4.3.3 Handle graph variable in template
- [ ] 4.4.3.4 Validate graph terms in template
- [ ] 4.4.3.5 Return list of quads to insert

### 4.4.4 DELETE/INSERT Atomicity

Ensure atomicity of MODIFY operation.

- [ ] 4.4.4.1 Execute DELETE and INSERT in single transaction
- [ ] 4.4.4.2 Use snapshot for consistent WHERE read
- [ ] 4.4.4.3 Rollback on error during INSERT
- [ ] 4.4.4.4 Return combined counts (deleted, inserted)
- [ ] 4.4.4.5 Handle graph creation during INSERT

---

## 4.5 DELETE WHERE with Graphs

### 4.5.1 DELETE WHERE Implementation

Implement DELETE WHERE with graph support.

- [ ] 4.5.1.1 Implement `execute_delete_where_graph/5`
- [ ] 4.5.1.2 Handle `DELETE WHERE { GRAPH <g> { ... } }`
- [ ] 4.5.1.3 Handle `WITH <g> DELETE WHERE { ... }`
- [ ] 4.5.1.4 Execute WHERE in graph context
- [ ] 4.5.1.5 Delete matching quads from graph

### 4.5.2 Cross-Graph DELETE WHERE

Handle DELETE WHERE across graphs.

- [ ] 4.5.2.1 Handle `DELETE WHERE { ?s ?p ?o }` (all graphs)
- [ ] 4.5.2.2 Use quad pattern with graph variable
- [ ] 4.5.2.3 Delete from each graph independently
- [ ] 4.5.2.4 Return count per graph
- [ ] 4.5.2.5 Document behavior (all graphs vs default only)

### 4.5.3 Template DELETE WHERE

Handle DELETE with template WHERE.

- [ ] 4.5.3.1 Handle `DELETE { GRAPH <g> { ?s ?p ?o } } WHERE { ... }`
- [ ] 4.5.3.2 Bind WHERE variables to DELETE template
- [ ] 4.5.3.3 Use specified graph for DELETE
- [ ] 4.5.3.4 Validate template variable consistency
- [ ] 4.5.3.5 Delete instantiated quads

---

## 4.6 COPY and MOVE Operations

### 4.6.1 COPY GRAPH

Implement COPY GRAPH operation.

- [ ] 4.6.1.1 Implement `execute_copy_graph/4`
- [ ] 4.6.1.2 Handle `COPY <g1> TO <g2>`
- [ ] 4.6.1.3 Handle `COPY SILENT` (ignore errors)
- [ ] 4.6.1.4 Replace target graph entirely (not merge)
- [ ] 4.6.1.5 Return count of quads copied

### 4.6.2 MOVE GRAPH

Implement MOVE GRAPH operation.

- [ ] 4.6.2.1 Implement `execute_move_graph/4`
- [ ] 4.6.2.2 Handle `MOVE <g1> TO <g2>`
- [ ] 4.6.2.3 Handle `MOVE SILENT` (ignore errors)
- [ ] 4.6.2.4 Copy to target, then delete source
- [ ] 4.6.2.5 Return count of quads moved

### 4.6.3 ADD Operation

Implement ADD (merge) operation.

- [ ] 4.6.3.1 Implement `execute_add_graph/4`
- [ ] 4.6.3.2 Handle `ADD <g1> TO <g2>`
- [ ] 4.6.3.3 Handle `ADD SILENT` (ignore errors)
- [ ] 4.6.3.4 Merge source into target (keep existing)
- [ ] 4.6.3.5 Return count of quads added

---

## 4.7 Update Executor Refactoring

### 4.7.1 Module Updates

Update UpdateExecutor for quad support.

- [ ] 4.7.1.1 Change internal operations from triple to quad
- [ ] 4.7.1.2 Use `QuadIndex.insert_quad/3` instead of `Index.insert_triple/3`
- [ ] 4.7.1.3 Use `QuadIndex.delete_quad/3` instead of `Index.delete_triple/3`
- [ ] 4.7.1.4 Pass graph context through all operations
- [ ] 4.7.1.5 Update @moduledoc with graph examples

### 4.7.2 Transaction Coordination

Update transaction handling for quads.

- [ ] 4.7.2.1 Extend transaction coordinator for quads
- [ ] 4.7.2.2 Invalidate plan cache on graph modifications
- [ ] 4.7.2.3 Invalidate statistics on graph modifications
- [ ] 4.7.2.4 Add telemetry for graph operations
- [ ] 4.7.2.5 Handle graph list updates

### 4.7.3 Error Handling Updates

Add graph-specific error handling.

- [ ] 4.7.3.1 Add `{:error, :graph_not_found}` error
- [ ] 4.7.3.2 Add `{:error, :graph_already_exists}` error
- [ ] 4.7.3.3 Add `{:error, :invalid_graph_name}` error
- [ ] 4.7.3.4 Add `{:error, :default_graph_protected}` error
- [ ] 4.7.3.5 Document all error conditions

---

## 4.8 Unit Tests

### 4.8.1 Graph Management Tests

- [ ] 4.8.1.1 Test CREATE GRAPH creates empty graph
- [ ] 4.8.1.2 Test CREATE GRAPH on existing graph fails
- [ ] 4.8.1.3 Test CREATE SILENT GRAPH ignores existing
- [ ] 4.8.1.4 Test DROP GRAPH removes graph completely
- [ ] 4.8.1.5 Test DROP SILENT GRAPH ignores missing
- [ ] 4.8.1.6 Test CLEAR GRAPH empties graph
- [ ] 4.8.1.7 Test CLEAR DEFAULT clears default graph
- [ ] 4.8.1.8 Test CLEAR ALL clears all graphs

### 4.8.2 INSERT DATA Tests

- [ ] 4.8.2.1 Test INSERT DATA to default graph
- [ ] 4.8.2.2 Test INSERT DATA to named graph
- [ ] 4.8.2.3 Test INSERT DATA creates graph if needed
- [ ] 4.8.2.4 Test INSERT DATA with multiple GRAPH blocks
- [ ] 4.8.2.5 Test INSERT SILENT ignores errors
- [ ] 4.8.2.6 Test INSERT DATA returns correct count

### 4.8.3 DELETE DATA Tests

- [ ] 4.8.3.1 Test DELETE DATA from default graph
- [ ] 4.8.3.2 Test DELETE DATA from named graph
- [ ] 4.8.3.3 Test DELETE DATA with multiple GRAPH blocks
- [ ] 4.8.3.4 Test DELETE SILENT ignores errors
- [ ] 4.8.3.5 Test DELETE DATA returns correct count
- [ ] 4.8.3.6 Test DELETE non-existent data is no-op

### 4.8.4 MODIFY Tests

- [ ] 4.8.4.1 Test MODIFY with WHERE in same graph
- [ ] 4.8.4.2 Test MODIFY with WHERE across graphs
- [ ] 4.8.4.3 Test MODIFY WITH <g> sets graph context
- [ ] 4.8.4.4 Test MODIFY deletes and inserts atomically
- [ ] 4.8.4.5 Test MODIFY returns correct counts
- [ ] 4.8.4.6 Test MODIFY rollback on error

### 4.8.5 DELETE WHERE Tests

- [ ] 4.8.5.1 Test DELETE WHERE in named graph
- [ ] 4.8.5.2 Test DELETE WHERE across all graphs
- [ ] 4.8.5.3 Test DELETE WHERE with template
- [ ] 4.8.5.4 Test DELETE WHERE with complex WHERE
- [ ] 4.8.5.5 Test DELETE WHERE returns correct count

### 4.8.6 COPY/MOVE/ADD Tests

- [ ] 4.8.6.1 Test COPY copies graph completely
- [ ] 4.8.6.2 Test COPY replaces target graph
- [ ] 4.8.6.3 Test MOVE moves graph (source deleted)
- [ ] 4.8.6.4 Test MOVE to existing graph replaces
- [ ] 4.8.6.5 Test ADD merges graphs
- [ ] 4.8.6.6 Test SILENT modifier for all operations

### 4.8.7 Error Handling Tests

- [ ] 4.8.7.1 Test invalid graph IRI returns error
- [ ] 4.8.7.2 Test operation on non-existent graph fails
- [ ] 4.8.7.3 Test SILENT suppresses errors
- [ ] 4.8.7.4 Test protected default graph operations
- [ ] 4.8.7.5 Test detailed error messages

### 4.8.8 Transaction Tests

- [ ] 4.8.8.1 Test MODIFY is atomic
- [ ] 4.8.8.2 Test concurrent updates serialized
- [ ] 4.8.8.3 Test cache invalidation on update
- [ ] 4.8.8.4 Test statistics invalidation on update
- [ ] 4.8.8.5 Test telemetry events emitted

---

## Success Criteria

1. **Graph Management**: CREATE/DROP/CLEAR GRAPH work correctly
2. **INSERT/DELETE**: Single and multi-graph operations work
3. **MODIFY**: DELETE/INSERT WHERE with graph support
4. **COPY/MOVE/ADD**: All graph operations work
5. **Atomicity**: Operations are atomic where required
6. **Error Handling**: All error conditions handled gracefully

## Provides Foundation

This phase establishes the infrastructure for:
- **Phase 5**: Quad-aware statistics and optimization
- **Phase 6**: Integration testing of complete quad functionality
- **Phase 7**: Reasoning with named graphs

## Key Outputs

- Updated `TripleStore.SPARQL.UpdateExecutor` with graph support
- Complete SPARQL UPDATE with named graphs
- Graph management operations (CREATE/DROP/CLEAR)
- COPY/MOVE/ADD operations
