# Section 6.8: Migration Tests

## Overview

Implement Section 6.8 of the quad store integration tests, focusing on migration from triple store (schema v1) to quad store (schema v2) and migration tool functionality validation.

## Feature Branch

`feature/section-6.8-migration-tests`

## Implementation Plan

### 6.8.1 Triple to Quad Migration

Test migration from triple to quad store.

- [ ] 6.8.1.1 Test export triple store as N-Triples
- [ ] 6.8.1.2 Test convert N-Triples to N-Quads (add default graph)
- [ ] 6.8.1.3 Test load N-Quads to new quad store
- [ ] 6.8.1.4 Test query migrated data returns same results
- [ ] 6.8.1.5 Test all data preserved in migration

### 6.8.2 Migration Tooling

Test migration tool functionality.

- [ ] 6.8.2.1 Test migration tool handles large datasets
- [ ] 6.8.2.2 Test migration tool reports progress
- [ ] 6.8.2.3 Test migration tool handles errors gracefully
- [ ] 6.8.2.4 Test migration tool validates output
- [ ] 6.8.2.5 Test migration tool can resume on failure

## File Structure

```
test/triple_store/integration/
└── migration_test.exs              # Migration tests

lib/triple_store/migration/          # (if needed)
├── migrator.ex                     # (if needed)
└── progress_reporter.ex           # (if needed)
```

## Technical Approach

### Schema Differences

**Triple Store (schema v1):**
- Column families: id2str, str2id, spo, pos, osp, derived, numeric_range
- 24-byte keys (3 IDs * 8 bytes)
- No graph context

**Quad Store (schema v2):**
- Column families: gspo, gpos, gosp, spog, id2str, str2id, derived
- 32-byte keys (4 IDs * 8 bytes) with graph as first component
- Named graphs support

### Migration Strategy

1. **Export from Triple Store:**
   - Use `Exporter.export_string(db, :ntriples)` to export as N-Triples
   - N-Triples format: `<subject> <predicate> "object" .`

2. **Convert to N-Quads:**
   - Add default graph context to each triple
   - N-Quads format: `<subject> <predicate> "object" <http://www.w3.org/1999/02/22-rdf-syntax-ns#default_graph> .`

3. **Import to Quad Store:**
   - Use `Loader.load_nquads_string/3` to load N-Quads

4. **Verification:**
   - Count quads in quad store
   - Query and compare results
   - Validate all data preserved

### Helper Functions Needed

- `create_triple_store/1` - Opens a triple store (schema: :triple)
- `create_quad_store/1` - Opens a quad store (schema: :quad)
- `populate_triple_store/2` - Adds test data to triple store
- `export_as_ntriples/1` - Exports triple store as N-Triples
- `ntriples_to_nquads/1` - Converts N-Triples to N-Quads with default graph
- `verify_migration/2` - Compares triple store and quad store contents

## Test Data

Use consistent test data for migration:

```turtle
@prefix ex: <http://example.org/> .

ex:subject1 ex:predicate1 "object1" .
ex:subject2 ex:predicate2 "object2" .
ex:subject3 ex:predicate1 "object3" .
```

## Migration Tooling Requirements

If migration tool doesn't exist, tests should:

1. **Large Dataset Test** - Use 1000+ triples to test performance
2. **Progress Reporting** - Mock or use telemetry events for progress
3. **Error Handling** - Test with invalid data, disk full scenarios
4. **Output Validation** - Verify quad count after migration
5. **Resume Capability** - Test migration from checkpoint

## Dependencies

- TripleStore.Backend.RocksDB.NIF (both schemas)
- TripleStore.Dictionary.Manager
- TripleStore.Loader
- TripleStore.Exporter
- TripleStore.QuadOperations
- TripleStore.SPARQL.Query

## Success Criteria

1. All data migrates correctly from triple to quad store
2. Query results match between original and migrated data
3. Migration handles edge cases (empty store, large datasets)
4. Progress reporting works for large migrations
5. Error handling prevents data corruption

## Status

**Complete** - All 10 tests passing successfully
