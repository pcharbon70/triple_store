# Phase 1: Quad Storage Foundation

## Overview

Phase 1 establishes the quad storage layer that extends the existing triple store to support named graphs. By the end of this phase, we will have a working quad store with four indices (GSPO, GPOS, SPOG, POSG) providing O(log n) access for all quad patterns.

The design maintains the same architectural patterns as the triple store: dictionary encoding, big-endian keys for lexicographic ordering, and atomic multi-index writes via WriteBatch.

**Key Difference from Triple Store:**
- Keys are 32 bytes (4 × 64-bit IDs) instead of 24 bytes
- Four indices instead of three (graph position added)
- Default graph represented as special ID (`0`)
- Named graphs stored alongside default graph in same indices

---

## 1.1 Quad Index Architecture

### 1.1.1 Index Design Decision

This section defines the four quad indices and their access patterns.

**Four Index Strategy:**

| Index | Key Ordering | Primary Use Case |
|-------|--------------|------------------|
| `gspo` | Graph-Subject-Predicate-Object | All triples in specific graph |
| `gpos` | Graph-Predicate-Object-Subject | All predicates in specific graph |
| `spog` | Subject-Predicate-Object-Graph | Subject-scoped queries across graphs |
| `posg` | Predicate-Object-Subject-Graph | Predicate-scoped queries across graphs |

**Rationale for Four Indices:**
- GSPO enables efficient `GRAPH <g> { ?s ?p ?o }` queries
- GPOS enables efficient `GRAPH <g> { ?p ?o ?s }` queries (predicate-first)
- SPOG enables efficient triple-scoped queries when graph is variable
- POSG enables efficient predicate-based queries across graphs

**Storage Tradeoff:**
- Key size: 32 bytes vs 24 bytes (~33% increase)
- Write amplification: 4x instead of 3x
- Skip `ospg` and `gosp` indices (less common patterns handled via filtering)

### 1.1.2 Column Family Definitions

Define the four quad index column families with appropriate tuning.

- [ ] 1.1.2.1 Define `gspo` column family with prefix extractor on graph (8 bytes)
- [ ] 1.1.2.2 Define `gpos` column family with prefix extractor on graph (8 bytes)
- [ ] 1.1.2.3 Define `spog` column family with prefix extractor on subject (8 bytes)
- [ ] 1.1.2.4 Define `posg` column family with prefix extractor on predicate (8 bytes)
- [ ] 1.1.2.5 Configure bloom filters (12 bits/key) for all indices
- [ ] 1.1.2.6 Set block size to 8KB for prefix scan optimization

**Configuration File:** `lib/triple_store/backend/rocksdb/column_family_config.ex`

### 1.1.3 Database Schema Versioning

Add schema version tracking to distinguish triple vs quad databases.

- [ ] 1.1.3.1 Add `schema_version` property to database metadata
- [ ] 1.1.3.2 Define version 1 = triple store (24-byte keys, 3 indices)
- [ ] 1.1.3.3 Define version 2 = quad store (32-byte keys, 4 indices)
- [ ] 1.1.3.4 Implement schema detection on database open
- [ ] 1.1.3.5 Return error on version mismatch (no backward compatibility)

---

## 1.2 Quad Key Encoding

### 1.2.1 Key Encoding Functions

Implement quad key encoding for all four indices.

- [ ] 1.2.1.1 Implement `gspo_key(g, s, p, o)` returning `<<g::64-big, s::64-big, p::64-big, o::64-big>>`
- [ ] 1.2.1.2 Implement `gpos_key(g, p, o, s)` returning `<<g::64-big, p::64-big, o::64-big, s::64-big>>`
- [ ] 1.2.1.3 Implement `spog_key(s, p, o, g)` returning `<<s::64-big, p::64-big, o::64-big, g::64-big>>`
- [ ] 1.2.1.4 Implement `posg_key(p, o, s, g)` returning `<<p::64-big, o::64-big, s::64-big, g::64-big>>`

**New Module:** `lib/triple_store/quad_index.ex` (replacement for `index.ex`)

### 1.2.2 Key Decoding Functions

Implement quad key decoding for all four indices.

- [ ] 1.2.2.1 Implement `decode_gspo_key(key)` extracting `{g, s, p, o}`
- [ ] 1.2.2.2 Implement `decode_gpos_key(key)` extracting `{g, p, o, s}`
- [ ] 1.2.2.3 Implement `decode_spog_key(key)` extracting `{s, p, o, g}`
- [ ] 1.2.2.4 Implement `decode_posg_key(key)` extracting `{p, o, s, g}`

### 1.2.3 Quad Prefix Functions

Implement prefix construction for efficient pattern matching.

- [ ] 1.2.3.1 Implement `gspo_prefix(g)` for graph-scoped scans
- [ ] 1.2.3.2 Implement `gspo_prefix(g, s)` for graph-subject scans
- [ ] 1.2.3.3 Implement `gspo_prefix(g, s, p)` for graph-subject-predicate scans
- [ ] 1.2.3.4 Implement `spog_prefix(s)` for subject scans across graphs
- [ ] 1.2.3.5 Implement `spog_prefix(s, p)` for subject-predicate scans
- [ ] 1.2.3.6 Implement `posg_prefix(p)` for predicate scans across graphs
- [ ] 1.2.3.7 Implement corresponding prefix functions for GPOS and POSG

### 1.2.4 Quad Key Utilities

Implement utility functions for quad key operations.

- [ ] 1.2.4.1 Implement `encode_quad_keys/4` returning all four index keys
- [ ] 1.2.4.2 Implement `key_to_quad/2` converting any index key to canonical `{s, p, o, g}`
- [ ] 1.2.4.3 Implement `quad_to_triple/1` extracting `{s, p, o}` from quad
- [ ] 1.2.4.4 Add guards for valid term IDs in all encoding functions

---

## 1.3 Graph ID Representation

### 1.3.1 Default Graph Identifier

Define the special identifier for the default graph.

- [ ] 1.3.1.1 Define `@default_graph_id` as `0` (reserved ID, never allocated by dictionary)
- [ ] 1.3.1.2 Document that ID 0 is reserved and never assigned to real terms
- [ ] 1.3.1.3 Add validation that dictionary never allocates ID 0
- [ ] 1.3.1.4 Implement `is_default_graph?(id)` guard function

### 1.3.2 Named Graph Encoding

Named graphs are encoded as regular RDF terms in the dictionary.

- [ ] 1.3.2.1 Graph URIs encoded as standard IRIs in dictionary
- [ ] 1.3.2.2 Blank node graphs supported (e.g., for scoping)
- [ ] 1.3.2.3 Graph terms use existing type tagging in dictionary (URI type)
- [ ] 1.3.2.4 No special graph ID space needed (reuse dictionary)

### 1.3.3 Graph ID Resolution

Implement functions for converting graph terms to IDs.

- [ ] 1.3.3.1 Implement `get_or_create_graph_id(db, graph_term)` for named graphs
- [ ] 1.3.3.2 Implement `resolve_graph_id(db, graph_or_var)` handling variables and default
- [ ] 1.3.3.3 Implement `id_to_graph_term(db, graph_id)` reverse lookup
- [ ] 1.3.3.4 Handle `:default` atom as special case returning `@default_graph_id`

---

## 1.4 Quad Pattern Matching

### 1.4.1 Pattern Representation

Extend pattern representation to include graph position.

- [ ] 1.4.1.1 Define quad pattern type: `{pattern_s, pattern_p, pattern_o, pattern_g}`
- [ ] 1.4.1.2 Each pattern position is `:bound` or `:var`
- [ ] 1.4.1.3 Example: `{:bound, :bound, :bound, :bound}` = fully bound quad
- [ ] 1.4.1.4 Example: `{:bound, :bound, :var, :bound}` = S-P-? in graph G

### 1.4.2 Index Selection for Quads

Implement optimal index selection for quad patterns.

- [ ] 1.4.2.1 Implement `select_index_for_quad/1` returning optimal index and prefix
- [ ] 1.4.2.2 Map all 16 quad patterns to optimal indices (4 positions × 2 states)
- [ ] 1.4.2.3 Patterns with bound graph prefer GSPO/GPOS indices
- [ ] 1.4.2.4 Patterns with unbound graph prefer SPOG/POSG indices

**Quad Pattern to Index Mapping:**

| Pattern | Index | Prefix |
|---------|-------|--------|
| `{:bound, :bound, :bound, :bound}` | GSPO | 24 bytes (g-s-p) |
| `{:bound, :bound, :bound, :var}` | SPOG | 24 bytes (s-p-o) |
| `{:bound, :bound, :var, :bound}` | GSPO | 16 bytes (g-s) |
| `{:bound, :var, :var, :bound}` | GSPO | 8 bytes (g) |
| `{:var, :bound, :bound, :bound}` | GPOS | 24 bytes (g-p-o) |
| `{:var, :bound, :var, :bound}` | GPOS | 16 bytes (g-p) |
| `{:var, :var, :bound, :bound}` | GSPO | 16 bytes (g-s) w/ filter |
| `{:bound, :bound, :var, :var}` | SPOG | 16 bytes (s-p) |
| `{:bound, :var, :var, :var}` | SPOG | 8 bytes (s) |
| `{:var, :bound, :var, :var}` | POSG | 8 bytes (p) |
| `{:var, :var, :bound, :var}` | SPOG | 16 bytes (s-o) w/ filter |
| `{:var, :var, :var, :bound}` | GSPO | 8 bytes (g) |

### 1.4.3 Prefix Construction for Quads

Implement prefix building for all quad patterns.

- [ ] 1.4.3.1 Implement `build_quad_prefix/2` for pattern × index
- [ ] 1.4.3.2 Handle bound positions at start of index key
- [ ] 1.4.3.3 Skip unbound positions in prefix construction
- [ ] 1.4.3.4 Return `{index, prefix, needs_filter, filter_positions}`

### 1.4.4 Post-Filtering for Quads

Implement filtering for patterns requiring additional constraints.

- [ ] 1.4.4.1 Implement `quad_matches_pattern?/2` for pattern validation
- [ ] 1.4.4.2 Handle non-contiguous bound positions (e.g., S-?O in graph)
- [ ] 1.4.4.3 Optimize filter application via pattern analysis
- [ ] 1.4.4.4 Document patterns requiring post-filtering

---

## 1.5 Quad Insert and Delete

### 1.5.1 Quad Insert Operations

Implement atomic quad insertion across all four indices.

- [ ] 1.5.1.1 Implement `insert_quad/3` inserting quad to all four indices
- [ ] 1.5.1.2 Implement `insert_quads/3` for batch quad insertion
- [ ] 1.5.1.3 Use WriteBatch for atomic multi-index write
- [ ] 1.5.1.4 Return `{:ok, :inserted}` or `{:error, :already_exists}`

### 1.5.2 Quad Delete Operations

Implement atomic quad deletion from all four indices.

- [ ] 1.5.2.1 Implement `delete_quad/3` deleting quad from all four indices
- [ ] 1.5.2.2 Implement `delete_quads/3` for batch quad deletion
- [ ] 1.5.2.3 Use WriteBatch for atomic multi-index delete
- [ ] 1.5.2.4 Return `{:ok, :deleted}` or `{:ok, :not_found}`

### 1.5.3 Quad Existence Check

Implement efficient quad existence checking.

- [ ] 1.5.3.1 Implement `quad_exists?/3` for exact quad lookup
- [ ] 1.5.3.2 Use optimal index (GSPO with full g-s-p prefix)
- [ ] 1.5.3.3 Return boolean without loading value
- [ ] 1.5.3.4 Handle default graph ID correctly

### 1.5.4 Quad Lookup

Implement pattern-based quad lookup.

- [ ] 1.5.4.1 Implement `lookup_quads/2` returning stream of matching quads
- [ ] 1.5.4.2 Use `select_index_for_quad/1` for optimal access
- [ ] 1.5.4.3 Apply prefix scan with post-filtering if needed
- [ ] 1.5.4.4 Decode keys and return canonical `{s, p, o, g}` tuples

---

## 1.6 Dictionary Compatibility

### 1.6.1 Dictionary Validation

Ensure dictionary module works with quad store.

- [x] 1.6.1.1 Verify ID 0 is never allocated by `get_or_create_id/2`
- [x] 1.6.1.2 Add `get_or_create_graph_id/2` as wrapper for graph terms
- [x] 1.6.1.3 Ensure graph IRIs use standard IRI encoding
- [x] 1.6.1.4 Test blank node graph encoding

### 1.6.2 Term ID Bounds Validation

Extend ID validation for quad compatibility.

- [x] 1.6.2.1 Add `valid_graph_id?/1` excluding ID 0 for named graphs
- [x] 1.6.2.2 Document that ID 0 reserved for default graph
- [x] 1.6.2.3 Verify sequence counter skips ID 0 by design (type tagging)
- [x] 1.6.2.4 Add tests for ID boundary conditions

---

## 1.7 Backend Adaptation

### 1.7.1 Column Family Configuration

Update backend configuration for quad indices.

- [ ] 1.7.1.1 Add `gspo`, `gpos`, `spog`, `posg` to CF definitions
- [ ] 1.7.1.2 Remove old `spo`, `pos`, `osp` CFs (quad store incompatible)
- [ ] 1.7.1.3 Update CF order in ErlangAdapter initialization
- [ ] 1.7.1.4 Configure per-CF options for quad indices

### 1.7.2 ErlangAdapter Updates

Update adapter for quad-specific operations.

- [ ] 1.7.2.1 Update `open/2` to require quad schema version
- [ ] 1.7.2.2 Update `create_new_database/2` for quad CF creation
- [ ] 1.7.2.3 Add `is_quad_store?/1` predicate for schema detection
- [ ] 1.7.2.4 Document migration path (export/import required)

### 1.7.3 Read Options for Quads

Update read options for quad access patterns.

- [ ] 1.7.3.1 Add `quad_prefix_scan/0` preset for GSPO/GPOS queries
- [ ] 1.7.3.2 Add `cross_graph_scan/0` preset for SPOG/POSG queries
- [ ] 1.7.3.3 Update `for_cf/1` to handle quad index CFs
- [ ] 1.7.3.4 Document read strategy per index type

---

## 1.8 Unit Tests

### 1.8.1 Key Encoding Tests

- [ ] 1.8.1.1 Test GSPO key encoding/decoding roundtrip
- [ ] 1.8.1.2 Test GPOS key encoding/decoding roundtrip
- [ ] 1.8.1.3 Test SPOG key encoding/decoding roundtrip
- [ ] 1.8.1.4 Test POSG key encoding/decoding roundtrip
- [ ] 1.8.1.5 Test all four indices encode same quad consistently
- [ ] 1.8.1.6 Test big-endian ordering is preserved

### 1.8.2 Prefix Tests

- [ ] 1.8.2.1 Test gspo_prefix(g) returns 8-byte prefix
- [ ] 1.8.2.2 Test gspo_prefix(g, s) returns 16-byte prefix
- [ ] 1.8.2.3 Test gspo_prefix(g, s, p) returns 24-byte prefix
- [ ] 1.8.2.4 Test prefix scans return correct results
- [ ] 1.8.2.5 Test prefix boundary conditions

### 1.8.3 Pattern Matching Tests

- [ ] 1.8.3.1 Test all 16 quad patterns map to correct indices
- [ ] 1.8.3.2 Test bound graph patterns select GSPO/GPOS
- [ ] 1.8.3.3 Test unbound graph patterns select SPOG/POSG
- [ ] 1.8.3.4 Test pattern with all bound returns exact lookup
- [ ] 1.8.3.5 Test pattern with all vars returns full scan

### 1.8.4 Insert/Delete Tests

- [ ] 1.8.4.1 Test single quad insert writes to all four indices
- [ ] 1.8.4.2 Test quad insert is idempotent
- [ ] 1.8.4.3 Test quad delete removes from all four indices
- [ ] 1.8.4.4 Test delete of non-existent quad is no-op
- [ ] 1.8.4.5 Test batch insert/delete atomicity

### 1.8.5 Graph ID Tests

- [ ] 1.8.5.1 Test default graph ID is 0
- [ ] 1.8.5.2 Test dictionary never allocates ID 0
- [ ] 1.8.5.3 Test named graph IDs are > 0
- [ ] 1.8.5.4 Test graph term encoding roundtrip
- [ ] 1.8.5.5 Test blank node graph encoding

### 1.8.6 Lookup Tests

- [ ] 1.8.6.1 Test exact quad lookup returns single result
- [ ] 1.8.6.2 Test graph-scoped query returns only quads from that graph
- [ ] 1.8.6.3 Test cross-graph query returns quads from all graphs
- [ ] 1.8.6.4 Test default graph query excludes named graphs
- [ ] 1.8.6.5 Test pattern with post-filter applies filter correctly

### 1.8.7 Backend Tests

- [ ] 1.8.7.1 Test database open fails on triple store schema
- [ ] 1.8.7.2 Test quad store schema version is persisted
- [ ] 1.8.7.3 Test all four CFs created on new database
- [ ] 1.8.7.4 Test CF handles are accessible via ErlangAdapter
- [ ] 1.8.7.5 Test read options optimize for quad access

---

## Success Criteria

1. **Schema Version**: Quad store databases identified as version 2
2. **Key Encoding**: All four quad indices encode/decode correctly
3. **Pattern Coverage**: All 16 quad patterns map to optimal index
4. **Insert/Delete**: Quads written atomically to all four indices
5. **Graph Support**: Default graph (ID 0) and named graphs both supported
6. **No Backward Compatibility**: Triple store databases rejected cleanly

## Provides Foundation

This phase establishes the infrastructure for:
- **Phase 2**: RDF.ex integration for quad formats (N-Quads, TriG)
- **Phase 3**: SPARQL query execution with GRAPH clause support
- **Phase 4**: SPARQL UPDATE with graph-scoped operations

## Key Outputs

- `TripleStore.QuadIndex` - Quad key encoding and pattern matching
- `TripleStore.Backend.RocksDB.ColumnFamilyConfig` - Quad index CF definitions
- Schema version 2 detection and validation
