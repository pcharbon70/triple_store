# Task 3.3.3: Update API - Summary

## Overview

Implemented the public Update API module that provides convenient functions for modifying triple store data using SPARQL UPDATE strings or direct triple manipulation.

## Files Created

### Implementation
- `lib/triple_store/update.ex` (~280 lines)
  - Public API for SPARQL UPDATE operations
  - Supports both Transaction manager and direct context map usage
  - Functions: update/2, insert/2, delete/2, execute/2, clear/1

### Tests
- `test/triple_store/update_test.exs` (~400 lines)
  - 30 comprehensive tests

## Key Functions

### update/2

```elixir
@spec update(context(), String.t(), keyword()) :: update_result()
def update(context, sparql, opts \\ [])
```

Executes a SPARQL UPDATE operation. Works with either a Transaction manager or a direct context map.

### insert/2

```elixir
@spec insert(context(), [triple()], keyword()) :: update_result()
def insert(context, triples, opts \\ [])
```

Inserts triples directly using RDF.ex terms without parsing SPARQL.

### delete/2

```elixir
@spec delete(context(), [triple()], keyword()) :: update_result()
def delete(context, triples, opts \\ [])
```

Deletes triples directly. Idempotent - deleting non-existent triples returns `{:ok, 0}`.

### execute/2

```elixir
@spec execute(context(), term(), keyword()) :: update_result()
def execute(context, ast, opts \\ [])
```

Executes a pre-parsed SPARQL UPDATE AST.

### clear/1

```elixir
@spec clear(context(), keyword()) :: update_result()
def clear(context, opts \\ [])
```

Clears all triples from the store.

## Usage Patterns

### With Transaction Manager (Recommended)

```elixir
{:ok, txn} = Transaction.start_link(db: db, dict_manager: manager)
{:ok, 1} = Update.update(txn, "INSERT DATA { <s> <p> <o> }")
{:ok, 1} = Update.insert(txn, [{s, p, o}])
{:ok, 1} = Update.delete(txn, [{s, p, o}])
```

### Direct Context (Stateless)

```elixir
ctx = %{db: db, dict_manager: manager}
{:ok, 1} = Update.update(ctx, "INSERT DATA { <s> <p> <o> }")
{:ok, 1} = Update.insert(ctx, [{s, p, o}])
```

## SPARQL UPDATE Support

- `INSERT DATA { ... }` - Insert ground triples
- `DELETE DATA { ... }` - Delete ground triples
- `DELETE WHERE { ... }` - Delete triples matching pattern
- `INSERT { ... } WHERE { ... }` - Insert templated triples
- `DELETE { ... } INSERT { ... } WHERE { ... }` - Combined modify
- `CLEAR DEFAULT` / `CLEAR ALL` - Clear all triples

## Test Coverage

### Context Map Tests (17 tests)
- update/2: INSERT DATA, DELETE DATA, typed literals, language literals
- insert/2: Single, multiple, empty, typed, language, blank nodes
- delete/2: Existing, non-existent, empty
- execute/2: Pre-parsed AST
- clear/1: With data, empty database

### Transaction Manager Tests (7 tests)
- update/2: Insert and delete via transaction
- insert/2: Insert via transaction
- delete/2: Delete via transaction
- execute/2: Pre-parsed AST via transaction
- clear/1: Clear via transaction

### Data Integrity Tests (4 tests)
- Inserted data is queryable
- Deleted data is not queryable
- Cleared data is not queryable
- SPARQL UPDATE and direct API interoperability

### Named Manager Tests (1 test)
- Works with named transaction manager

### Error Handling (1 test)
- Parse error for invalid SPARQL

## Architecture Notes

The Update module provides a unified API that works with two types of contexts:

1. **Transaction Manager (pid or atom)**: Delegates to Transaction module which provides:
   - Write serialization
   - Snapshot-based read isolation
   - Plan cache invalidation

2. **Context Map**: Direct execution via UpdateExecutor for stateless operations

Both paths share the same term conversion logic and UpdateExecutor implementation.

## Planning Checklist

- [x] 3.3.3.1 Implement `TripleStore.update(db, sparql)` for SPARQL UPDATE
- [x] 3.3.3.2 Implement `TripleStore.insert(db, triples)` for direct insert
- [x] 3.3.3.3 Implement `TripleStore.delete(db, triples)` for direct delete
- [x] 3.3.3.4 Return affected triple count

## Next Steps

Task 3.3.4 (Unit Tests) will add:
- Test INSERT DATA adds triples
- Test DELETE DATA removes triples
- Test DELETE WHERE removes matching triples
- Test INSERT WHERE adds templated triples
- Test MODIFY combines delete and insert
- Test concurrent reads see consistent snapshot during update
- Test plan cache invalidated after update
- Test update failure leaves database unchanged
