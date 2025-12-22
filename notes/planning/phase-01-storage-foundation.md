# Phase 1: Storage Foundation

## Overview

Phase 1 establishes the persistent storage layer that forms the bedrock of the triple store. By the end of this phase, we will have a working RocksDB-backed storage system with dictionary encoding for RDF terms and three indices (SPO, POS, OSP) providing O(log n) access for all triple patterns.

The design prioritizes correctness and simplicity over optimization. We use Rustler NIFs for RocksDB operations with dirty CPU scheduler annotations to prevent BEAM scheduler blocking. Dictionary encoding maps all URIs, blank nodes, and literals to 64-bit integer IDs with type tagging, enabling compact storage and fast comparisons.

This phase integrates with the existing `rdf` hex package for RDF parsing and data structures, providing a clean adapter layer that converts `RDF.Triple` and `RDF.Graph` structures to our internal representation.

---

## 1.1 Project Scaffolding

- [x] **Section 1.1 Complete**

This section establishes the basic Elixir/Rust project structure with Rustler integration. We create the mix project, configure Rustler for NIF compilation, and set up the Rust crate structure for RocksDB integration.

### 1.1.1 Mix Project Setup

- [x] **Task 1.1.1 Complete**

Create the Elixir project with proper supervision tree structure and configure dependencies for RDF handling and NIF compilation.

- [x] 1.1.1.1 Create mix project with `mix new triple_store --sup`
- [x] 1.1.1.2 Add dependencies: `rdf`, `rustler`, `flow`, `telemetry`
- [x] 1.1.1.3 Configure application supervision tree in `lib/triple_store/application.ex`
- [x] 1.1.1.4 Create module namespace structure (`TripleStore.Backend`, `TripleStore.Dictionary`, `TripleStore.Index`)

### 1.1.2 Rustler NIF Configuration

- [x] **Task 1.1.2 Complete**

Configure Rustler to compile Rust NIFs with RocksDB integration. The Rust crate will handle all RocksDB operations through a clean NIF interface.

- [x] 1.1.2.1 Initialize Rustler with `mix rustler.new` creating `native/rocksdb_nif`
- [x] 1.1.2.2 Configure `Cargo.toml` with `rustler = "0.35"` and `rocksdb = "0.22"`
- [x] 1.1.2.3 Create `TripleStore.Backend.RocksDB` module with NIF stubs
- [x] 1.1.2.4 Verify NIF compilation with `mix compile`

### 1.1.3 Development Environment

- [ ] **Task 1.1.3 Complete**

Set up development tooling including code formatting, testing infrastructure, and documentation generation.

- [ ] 1.1.3.1 Configure `.formatter.exs` for consistent code style
- [ ] 1.1.3.2 Set up ExUnit with async test support
- [ ] 1.1.3.3 Configure `mix.exs` for documentation with ExDoc
- [ ] 1.1.3.4 Create `test/support/` helpers for test fixtures

### 1.1.4 Unit Tests

- [x] **Task 1.1.4 Complete**

- [x] Test mix project compiles without errors
- [x] Test Rustler NIF loads successfully
- [x] Test supervision tree starts correctly
- [x] Test module namespaces are properly defined

---

## 1.2 RocksDB NIF Wrapper

- [x] **Section 1.2 Complete**

This section implements the Rust NIF wrapper around RocksDB, providing the low-level storage operations. The wrapper uses column families to separate different data stores (dictionary, indices) and implements WriteBatch for atomic multi-index writes.

All NIF functions that may take >1ms use the `#[rustler::nif(schedule = "DirtyCpu")]` annotation to prevent BEAM scheduler blocking. Resource handles are wrapped in `ResourceArc` for safe garbage collection.

### 1.2.1 Database Lifecycle

- [x] **Task 1.2.1 Complete**

Implement database open/close operations with column family configuration. The database handle is wrapped in a ResourceArc for safe cross-NIF-boundary passing.

- [x] 1.2.1.1 Implement `open(path, opts)` NIF returning `ResourceArc<DbRef>`
- [x] 1.2.1.2 Configure column families: `id2str`, `str2id`, `spo`, `pos`, `osp`, `derived`
- [x] 1.2.1.3 Implement `close(db)` NIF with proper resource cleanup
- [x] 1.2.1.4 Add `#[rustler::nif(schedule = "DirtyCpu")]` to all I/O operations
- [x] 1.2.1.5 Implement error handling with Rustler's `Error` type

### 1.2.2 Basic Read/Write Operations

- [x] **Task 1.2.2 Complete**

Implement single key-value operations for each column family. These form the foundation for dictionary lookups and index queries.

- [x] 1.2.2.1 Implement `get(db, cf, key)` returning `{:ok, value}` or `:not_found`
- [x] 1.2.2.2 Implement `put(db, cf, key, value)` for single writes
- [x] 1.2.2.3 Implement `delete(db, cf, key)` for single deletes
- [x] 1.2.2.4 Implement `exists?(db, cf, key)` for existence checks

### 1.2.3 WriteBatch API

- [x] **Task 1.2.3 Complete**

Implement atomic batch writes for multi-index consistency. When inserting a triple, we must write to all three indices atomically.

- [x] 1.2.3.1 Implement `put_batch(db, operations)` where operations is `[{cf, key, value}]`
- [x] 1.2.3.2 Implement `delete_batch(db, operations)` where operations is `[{cf, key}]`
- [x] 1.2.3.3 Implement mixed batch with puts and deletes
- [x] 1.2.3.4 Ensure atomic commit semantics via RocksDB WriteBatch

### 1.2.4 Iterator API

- [x] **Task 1.2.4 Complete**

Implement prefix iterators for range queries over indices. The iterator returns a stream of key-value pairs matching a given prefix.

- [x] 1.2.4.1 Implement `prefix_iterator(db, cf, prefix)` returning iterator resource
- [x] 1.2.4.2 Implement `iterator_next(iter)` returning `{:ok, key, value}` or `:end`
- [x] 1.2.4.3 Implement `iterator_seek(iter, target)` for Leapfrog support (Phase 3)
- [x] 1.2.4.4 Implement `iterator_close(iter)` for resource cleanup
- [x] 1.2.4.5 Create Elixir `Stream` wrapper for ergonomic iteration

### 1.2.5 Snapshot Support

- [x] **Task 1.2.5 Complete**

Implement RocksDB snapshots for consistent reads during updates. This provides the foundation for transaction isolation in Phase 3.

- [x] 1.2.5.1 Implement `snapshot(db)` returning snapshot resource
- [x] 1.2.5.2 Implement `get_snapshot(snapshot, cf, key)` for point-in-time reads
- [x] 1.2.5.3 Implement `prefix_iterator_snapshot(snapshot, cf, prefix)`
- [x] 1.2.5.4 Implement `release_snapshot(snapshot)` for cleanup

### 1.2.6 Unit Tests

- [x] **Task 1.2.6 Complete**

- [x] Test database opens and closes without leaks
- [x] Test column families are created correctly
- [x] Test single get/put/delete operations
- [x] Test WriteBatch atomic commits
- [x] Test prefix iterator returns correct results
- [x] Test iterator seek positions correctly
- [x] Test snapshot provides consistent view
- [x] Test error handling for invalid paths/operations

---

## 1.3 Dictionary Encoding

- [x] **Section 1.3 Complete** (2025-12-21)

This section implements the dictionary encoding system that maps RDF terms to 64-bit integer IDs. The encoding uses type tags in the high bits to distinguish URIs, blank nodes, literals, and inline-encoded numeric types.

Inline encoding for numeric types (`xsd:integer`, `xsd:decimal`, `xsd:dateTime`) enables direct comparisons without dictionary lookups, which is critical for FILTER evaluation performance in Phase 2.

### 1.3.1 Term ID Encoding

- [x] **Task 1.3.1 Complete** (2025-12-21)

Define the 64-bit ID encoding scheme with type tags. The high 4 bits encode the term type, leaving 60 bits for the sequence number or inline value.

- [x] 1.3.1.1 Define type tag constants: `@type_uri (0b0001)`, `@type_bnode (0b0010)`, `@type_literal (0b0011)`, `@type_integer (0b0100)`, `@type_decimal (0b0101)`, `@type_datetime (0b0110)`
- [x] 1.3.1.2 Implement `encode_id(type, sequence)` combining type tag and sequence
- [x] 1.3.1.3 Implement `decode_id(id)` extracting `{type, sequence}`
- [x] 1.3.1.4 Implement `term_type(id)` for quick type checking

### 1.3.2 Sequence Counter

- [x] **Task 1.3.2 Complete** (2025-12-21)

Implement atomic sequence counter for generating unique IDs. We use `:atomics` for lock-free increment operations with persistence via RocksDB.

- [x] 1.3.2.1 Create `:atomics` counter initialized from persisted value on startup
- [x] 1.3.2.2 Implement `next_sequence()` with atomic increment
- [x] 1.3.2.3 Implement periodic persistence of counter to RocksDB
- [x] 1.3.2.4 Handle counter recovery on database open

### 1.3.3 String-to-ID Mapping

- [x] **Task 1.3.3 Complete** (2025-12-21)

Implement the forward mapping from RDF term strings to IDs using the `str2id` column family.

- [x] 1.3.3.1 Implement `encode_term(term)` serializing term to binary key
- [x] 1.3.3.2 Implement `lookup_id(db, term)` returning `{:ok, id}` or `:not_found`
- [x] 1.3.3.3 Implement `get_or_create_id(db, term)` with atomic create-if-missing (via Manager)
- [x] 1.3.3.4 Handle URI encoding with angle brackets stripped
- [x] 1.3.3.5 Handle literal encoding with datatype and language tag

### 1.3.4 ID-to-String Mapping

- [x] **Task 1.3.4 Complete** (2025-12-21)

Implement the reverse mapping from IDs to RDF term strings using the `id2str` column family.

- [x] 1.3.4.1 Implement `lookup_term(db, id)` returning `{:ok, term}` or `:not_found`
- [x] 1.3.4.2 Implement `decode_term(binary)` parsing binary back to term struct
- [x] 1.3.4.3 Implement batch lookup `lookup_terms(db, ids)` for result serialization
- [x] 1.3.4.4 Handle inline-encoded IDs (return computed value, no lookup needed)

### 1.3.5 Inline Numeric Encoding

- [x] **Task 1.3.5 Complete** (2025-12-21)

Implement inline encoding for numeric types that fit within 60 bits. These values are encoded directly in the ID without dictionary storage.

- [x] 1.3.5.1 Implement `encode_integer(n)` for xsd:integer in range [-2^59, 2^59)
- [x] 1.3.5.2 Implement `encode_decimal(d)` using custom floating-point representation
- [x] 1.3.5.3 Implement `encode_datetime(dt)` using Unix timestamp (milliseconds)
- [x] 1.3.5.4 Implement `decode_inline(id)` reconstructing original value
- [x] 1.3.5.5 Implement `inline_encodable?(term)` predicate for RDF terms

### 1.3.6 Unit Tests

- [x] **Task 1.3.6 Complete** (2025-12-21)

- [x] Test type tag encoding/decoding roundtrip (term_id_encoding_test.exs)
- [x] Test sequence counter increments atomically (sequence_counter_test.exs)
- [x] Test sequence counter persists across restarts (sequence_counter_test.exs)
- [x] Test string-to-ID mapping for URIs (string_to_id_test.exs)
- [x] Test string-to-ID mapping for blank nodes (string_to_id_test.exs)
- [x] Test string-to-ID mapping for typed literals (string_to_id_test.exs)
- [x] Test string-to-ID mapping for language-tagged literals (string_to_id_test.exs)
- [x] Test ID-to-string reverse lookup (id_to_string_test.exs)
- [x] Test inline integer encoding/decoding (inline_numeric_test.exs)
- [x] Test inline decimal encoding/decoding (inline_numeric_test.exs)
- [x] Test inline datetime encoding/decoding (inline_numeric_test.exs)
- [x] Test get_or_create_id idempotency (string_to_id_test.exs)

---

## 1.4 Triple Index Layer

- [x] **Section 1.4 Complete** (2025-12-21)

This section implements the three triple indices (SPO, POS, OSP) that provide efficient access patterns for all possible triple queries. Each index stores the same triple data in different orderings, using big-endian keys for natural lexicographic ordering.

The indices use empty values since the key itself encodes the complete triple. This minimizes storage overhead while maintaining fast prefix-based iteration for pattern matching.

### 1.4.1 Key Encoding

- [x] **Task 1.4.1 Complete** (2025-12-21)

Implement the key encoding functions for each index. Keys are 24 bytes (3 x 64-bit IDs) in big-endian format for correct lexicographic ordering.

- [x] 1.4.1.1 Implement `spo_key(s, p, o)` returning `<<s::64-big, p::64-big, o::64-big>>`
- [x] 1.4.1.2 Implement `pos_key(p, o, s)` returning `<<p::64-big, o::64-big, s::64-big>>`
- [x] 1.4.1.3 Implement `osp_key(o, s, p)` returning `<<o::64-big, s::64-big, p::64-big>>`
- [x] 1.4.1.4 Implement `decode_spo_key(binary)` extracting `{s, p, o}`
- [x] 1.4.1.5 Implement `decode_pos_key(binary)` extracting `{p, o, s}`
- [x] 1.4.1.6 Implement `decode_osp_key(binary)` extracting `{o, s, p}`

Additional functions implemented:
- `spo_prefix/1,2`, `pos_prefix/1,2`, `osp_prefix/1,2` for pattern matching
- `encode_triple_keys/3` for generating all index keys at once
- `key_to_triple/2` for converting any index key to canonical order

### 1.4.2 Triple Insert

- [x] **Task 1.4.2 Complete** (2025-12-21)

Implement triple insertion with atomic writes to all three indices. The dictionary is updated first to obtain IDs, then all index entries are written in a single batch.

- [x] 1.4.2.1 Implement `insert_triple(db, {s, p, o})` encoding terms and writing indices
- [x] 1.4.2.2 Implement `insert_triples(db, triples)` for batch insertion
- [x] 1.4.2.3 Ensure atomic commit across all three indices
- [x] 1.4.2.4 Handle duplicate triple insertion (idempotent)

Additional function implemented:
- `triple_exists?/2` for checking if a triple exists in the database

### 1.4.3 Triple Delete

- [x] **Task 1.4.3 Complete** (2025-12-21)

Implement triple deletion removing entries from all three indices atomically.

- [x] 1.4.3.1 Implement `delete_triple(db, {s, p, o})` looking up IDs and removing entries
- [x] 1.4.3.2 Implement `delete_triples(db, triples)` for batch deletion
- [x] 1.4.3.3 Ensure atomic removal across all three indices
- [x] 1.4.3.4 Handle deletion of non-existent triple (no-op)

### 1.4.4 Pattern Matching

- [x] **Task 1.4.4 Complete** (2025-12-21)

Implement pattern-to-index mapping for all 8 possible triple patterns. Each pattern selects the optimal index and constructs the appropriate prefix.

- [x] 1.4.4.1 Implement `pattern_to_index({:bound, :bound, :bound})` -> `:spo`
- [x] 1.4.4.2 Implement `pattern_to_index({:bound, :bound, :var})` -> `:spo`
- [x] 1.4.4.3 Implement `pattern_to_index({:bound, :var, :var})` -> `:spo`
- [x] 1.4.4.4 Implement `pattern_to_index({:var, :bound, :bound})` -> `:pos`
- [x] 1.4.4.5 Implement `pattern_to_index({:var, :bound, :var})` -> `:pos`
- [x] 1.4.4.6 Implement `pattern_to_index({:var, :var, :bound})` -> `:osp`
- [x] 1.4.4.7 Implement `pattern_to_index({:bound, :var, :bound})` -> `:osp` with filter
- [x] 1.4.4.8 Implement `pattern_to_index({:var, :var, :var})` -> `:spo` full scan

Additional functions implemented:
- `select_index/1` - Returns index selection with prefix and filter info
- `triple_matches_pattern?/2` - Post-filtering helper
- `pattern_shape/1` - Debug helper for pattern visualization

### 1.4.5 Index Lookup

- [x] **Task 1.4.5 Complete** (2025-12-21)

Implement the lookup function that returns a stream of matching triples for a given pattern.

- [x] 1.4.5.1 Implement `lookup(db, pattern)` returning `Stream.t()`
- [x] 1.4.5.2 Implement `build_prefix(pattern, index)` constructing prefix bytes (done via select_index/1)
- [x] 1.4.5.3 Implement result decoding from index keys back to `{s, p, o}` tuples
- [x] 1.4.5.4 Implement post-filtering for `{:bound, :var, :bound}` pattern
- [x] 1.4.5.5 Implement `triple_exists?(db, {s, p, o})` for existence check (done in 1.4.2)

Additional functions implemented:
- `lookup_all/2` - Collects all matching triples into a list
- `count/2` - Returns count of matching triples

### 1.4.6 Unit Tests

- [x] **Task 1.4.6 Complete** (2025-12-21)

All tests implemented across individual task test files:

- [x] Test SPO key encoding/decoding roundtrip (key_encoding_test.exs)
- [x] Test POS key encoding/decoding roundtrip (key_encoding_test.exs)
- [x] Test OSP key encoding/decoding roundtrip (key_encoding_test.exs)
- [x] Test single triple insert writes to all indices (triple_insert_test.exs)
- [x] Test batch triple insert atomicity (triple_insert_test.exs)
- [x] Test duplicate insert is idempotent (triple_insert_test.exs)
- [x] Test single triple delete removes from all indices (triple_delete_test.exs)
- [x] Test non-existent triple delete is no-op (triple_delete_test.exs)
- [x] Test pattern `{:bound, :bound, :bound}` exact lookup (index_lookup_test.exs)
- [x] Test pattern `{:bound, :bound, :var}` subject-predicate lookup (index_lookup_test.exs)
- [x] Test pattern `{:bound, :var, :var}` subject lookup (index_lookup_test.exs)
- [x] Test pattern `{:var, :bound, :bound}` predicate-object lookup (index_lookup_test.exs)
- [x] Test pattern `{:var, :bound, :var}` predicate lookup (index_lookup_test.exs)
- [x] Test pattern `{:var, :var, :bound}` object lookup (index_lookup_test.exs)
- [x] Test pattern `{:bound, :var, :bound}` subject-object with filter (index_lookup_test.exs)
- [x] Test pattern `{:var, :var, :var}` full scan (index_lookup_test.exs)

### 1.4.7 Review Improvements

- [x] **Task 1.4.7 Complete** (2025-12-21)

Following the comprehensive review of Section 1.4, the following improvements were implemented:

- [x] 1.4.7.1 Add term_id bounds validation with `valid_term_id?` guard (0 <= id <= 2^64-1)
- [x] 1.4.7.2 Add `@empty_value` compile-time constant for index values
- [x] 1.4.7.3 Document S?O pattern performance characteristics in `select_index/1`
- [x] 1.4.7.4 Add guards to all `select_index/1` pattern clauses
- [x] 1.4.7.5 Refactor `lookup/2` to use `with` for cleaner control flow
- [x] 1.4.7.6 Create `test/support/index_test_helper.ex` for shared test utilities

---

## 1.5 RDF.ex Integration

- [x] **Section 1.5 Complete** (2025-12-22)

This section provides the adapter layer between the `rdf` hex package and our internal storage representation. The adapter converts `RDF.Triple`, `RDF.Graph`, and `RDF.Dataset` structures to our dictionary-encoded format.

Bulk loading uses `Flow` for parallel processing with backpressure-aware batching. This enables efficient loading of large datasets while maintaining memory bounds.

### 1.5.1 Term Conversion

- [x] **Task 1.5.1 Complete** (2025-12-21)

Implement conversion between RDF.ex term types and our internal representation.

- [x] 1.5.1.1 Implement `from_rdf_iri(RDF.IRI.t())` -> internal URI term
- [x] 1.5.1.2 Implement `from_rdf_bnode(RDF.BlankNode.t())` -> internal blank node term
- [x] 1.5.1.3 Implement `from_rdf_literal(RDF.Literal.t())` -> internal literal term
- [x] 1.5.1.4 Implement `to_rdf_iri(term)` -> `RDF.IRI.t()`
- [x] 1.5.1.5 Implement `to_rdf_bnode(term)` -> `RDF.BlankNode.t()`
- [x] 1.5.1.6 Implement `to_rdf_literal(term)` -> `RDF.Literal.t()`

Additional functions implemented:
- `term_to_id/2` - Generic term to ID dispatch
- `terms_to_ids/2` - Batch term to ID conversion
- `id_to_term/2` - Generic ID to term dispatch
- `ids_to_terms/2` - Batch ID to term conversion
- `lookup_term_id/2` - Read-only term ID lookup

### 1.5.2 Triple/Graph Conversion

- [x] **Task 1.5.2 Complete** (2025-12-22)

Implement conversion for composite structures including triples and graphs.

- [x] 1.5.2.1 Implement `from_rdf_triple(RDF.Triple.t())` -> `{s, p, o}` internal tuple
- [x] 1.5.2.2 Implement `to_rdf_triple({s, p, o})` -> `RDF.Triple.t()`
- [x] 1.5.2.3 Implement `from_rdf_graph(RDF.Graph.t())` -> stream of internal triples
- [x] 1.5.2.4 Implement `to_rdf_graph(db, triples)` -> `RDF.Graph.t()`

Additional functions implemented:
- `from_rdf_triples/2` - Batch triple conversion (efficient term batching)
- `to_rdf_triples/2` - Batch reverse conversion
- `stream_from_rdf_graph/2` - Lazy stream conversion for large graphs

### 1.5.3 Bulk Loading Pipeline

- [x] **Task 1.5.3 Complete** (2025-12-22)

Implement efficient bulk loading using batched writes and Telemetry progress reporting.

- [x] 1.5.3.1 Implement `load_graph(db, graph)` streaming triples into storage
- [x] 1.5.3.2 Implement `load_file(db, path)` parsing and loading RDF files
- [x] 1.5.3.3 Support Turtle, N-Triples, N-Quads, TriG, RDF/XML, JSON-LD formats
- [x] 1.5.3.4 Implement batching (1000 triples per batch) for efficient writes
- [x] 1.5.3.5 Implement progress reporting via Telemetry events
- [ ] 1.5.3.6 Target: 1M triples in <30 seconds (deferred to Phase 5 optimization)

Additional functions implemented:
- `load_string/5` - Parse and load RDF content from string
- `load_stream/4` - Load from arbitrary enumerable of triples

### 1.5.4 Export Functions

- [x] **Task 1.5.4 Complete** (2025-12-22)

Implement export functions to serialize stored triples back to RDF formats.

- [x] 1.5.4.1 Implement `export_graph(db)` returning `RDF.Graph.t()` of all triples
- [x] 1.5.4.2 Implement `export_graph(db, pattern)` for filtered export
- [x] 1.5.4.3 Implement `export_file(db, path, format)` writing to file
- [x] 1.5.4.4 Support streaming export for large datasets

Additional functions implemented:
- `export_string/3` - Serialize triples to string
- `stream_triples/2` - Lazy stream of RDF triples
- `stream_internal_triples/2` - Lazy stream of internal triple IDs
- `count/2` - Count triples matching pattern
- `format_extension/1` - Get file extension for format

### 1.5.5 Unit Tests

- [x] **Task 1.5.5 Complete** (2025-12-22)

All tests implemented as part of tasks 1.5.1-1.5.4:

- [x] Test IRI conversion roundtrip (term_conversion_test.exs)
- [x] Test blank node conversion roundtrip (term_conversion_test.exs)
- [x] Test typed literal conversion roundtrip (term_conversion_test.exs)
- [x] Test language-tagged literal conversion roundtrip (term_conversion_test.exs)
- [x] Test triple conversion roundtrip (triple_graph_conversion_test.exs)
- [x] Test graph loading stores all triples (loader_test.exs)
- [x] Test file loading for Turtle format (loader_test.exs)
- [x] Test file loading for N-Triples format (loader_test.exs)
- [x] Test batch loading respects batch size (loader_test.exs)
- [x] Test export produces valid RDF graph (exporter_test.exs)
- [x] Test export with pattern filters correctly (exporter_test.exs)

Test coverage: 140 tests across RDF.ex integration layer

### 1.5.6 Review Improvements

- [x] **Task 1.5.6 Complete** (2025-12-22)

Following the comprehensive review of Section 1.5, the following improvements were implemented:

**Concerns Addressed:**
- [x] 1.5.6.1 C1: Document named graph limitation prominently in loader.ex moduledoc
- [x] 1.5.6.2 C2: Update documentation to reflect sequential processing (not Flow)
- [x] 1.5.6.3 C3: Add path validation to prevent path traversal attacks
- [x] 1.5.6.4 C4: Fix silent error swallowing in stream_triples/2 with Logger.warning
- [x] 1.5.6.5 C5: Document lookup_term_id/2 literal behavior clearly
- [x] 1.5.6.6 C6: Add @spec to all private functions in loader.ex
- [x] 1.5.6.7 C7: Add configurable file size limits (default 100MB)

**Suggestions Implemented:**
- [x] 1.5.6.8 S1: Extract with_telemetry/2 helper in loader.ex
- [x] 1.5.6.9 S2: Extract extract_default_graph/1 helper
- [x] 1.5.6.10 S3: Create test/support/rdf_integration_test_helper.ex
- [x] 1.5.6.11 S4: Enhanced error messages with {:unsupported_format, ext, [supported: list]}
- [x] 1.5.6.12 S5: Add export_by_subject/3, export_by_predicate/3, export_by_object/3
- [x] 1.5.6.13 S6: Add telemetry events to Exporter

**Suggestions Deferred to Phase 5:**
- [ ] S7: Property-based testing for roundtrip (requires stream_data dependency)
- [ ] S8: Concurrent access tests (requires additional test infrastructure)

Test coverage: 741 tests (up from 739)

---

## 1.6 Basic Statistics

- [ ] **Section 1.6 Complete**

This section implements basic statistics collection for the triple store. Statistics are used by the query optimizer in Phase 2 to estimate cardinalities and select efficient query plans.

### 1.6.1 Triple Counts

- [x] **Task 1.6.1 Complete** (2025-12-22)

Implement counters for basic triple statistics.

- [x] 1.6.1.1 Implement `triple_count(db)` returning total number of triples
- [x] 1.6.1.2 Implement `predicate_count(db, predicate)` for per-predicate counts
- [x] 1.6.1.3 Implement `distinct_subjects(db)` approximation
- [x] 1.6.1.4 Implement `distinct_predicates(db)` exact count
- [x] 1.6.1.5 Implement `distinct_objects(db)` approximation

Additional function implemented:
- `all/1` - Returns all statistics in a single call

Test coverage: 20 tests in statistics_test.exs

### 1.6.2 Statistics Cache

- [ ] **Task 1.6.2 Complete**

Implement caching for statistics to avoid repeated scans.

- [ ] 1.6.2.1 Create `TripleStore.Statistics` GenServer for cached stats
- [ ] 1.6.2.2 Implement periodic refresh of statistics
- [ ] 1.6.2.3 Implement invalidation on bulk updates
- [ ] 1.6.2.4 Store predicate frequency histogram

### 1.6.3 Unit Tests

- [ ] **Task 1.6.3 Complete**

- [ ] Test triple count accuracy
- [ ] Test predicate count accuracy
- [ ] Test statistics cache returns consistent values
- [ ] Test statistics invalidation on updates

---

## 1.7 Phase 1 Integration Tests

- [ ] **Section 1.7 Complete**

Integration tests validate the complete storage layer working together, including database lifecycle, dictionary encoding, triple indexing, and RDF.ex integration.

### 1.7.1 Database Lifecycle Testing

- [ ] **Task 1.7.1 Complete**

Test complete database lifecycle from open through heavy usage to close.

- [ ] 1.7.1.1 Test open database, insert triples, close, reopen, verify data persisted
- [ ] 1.7.1.2 Test concurrent read operations during writes
- [ ] 1.7.1.3 Test database recovery after simulated crash
- [ ] 1.7.1.4 Test multiple database instances in same process

### 1.7.2 Dictionary Consistency Testing

- [ ] **Task 1.7.2 Complete**

Test dictionary encoding maintains consistency across operations.

- [ ] 1.7.2.1 Test same term always gets same ID
- [ ] 1.7.2.2 Test ID-to-term and term-to-ID are inverse operations
- [ ] 1.7.2.3 Test inline-encoded values compare correctly
- [ ] 1.7.2.4 Test dictionary handles Unicode terms correctly

### 1.7.3 Index Consistency Testing

- [ ] **Task 1.7.3 Complete**

Test all three indices remain consistent through insert/delete cycles.

- [ ] 1.7.3.1 Test triple found via all applicable patterns after insert
- [ ] 1.7.3.2 Test triple not found via any pattern after delete
- [ ] 1.7.3.3 Test index consistency after interleaved inserts and deletes
- [ ] 1.7.3.4 Test batch operations maintain cross-index consistency

### 1.7.4 Bulk Loading Testing

- [ ] **Task 1.7.4 Complete**

Test bulk loading performance and correctness with large datasets.

- [ ] 1.7.4.1 Test loading 100K triples maintains index consistency
- [ ] 1.7.4.2 Test loading 1M triples completes in <30 seconds
- [ ] 1.7.4.3 Test memory usage stays bounded during bulk load
- [ ] 1.7.4.4 Test loading LUBM(1) dataset (~100K triples)

### 1.7.5 RDF.ex Roundtrip Testing

- [ ] **Task 1.7.5 Complete**

Test complete roundtrip from RDF.ex through storage and back.

- [ ] 1.7.5.1 Test load RDF.Graph, export, compare equality
- [ ] 1.7.5.2 Test load Turtle file, export to N-Triples, verify content
- [ ] 1.7.5.3 Test complex literals (language tags, datatypes) roundtrip
- [ ] 1.7.5.4 Test blank node identity preservation within graph

---

## Success Criteria

1. **Storage Operations**: RocksDB NIF compiles and all basic operations work correctly
2. **Dictionary Encoding**: Bidirectional term<->ID mapping with inline numeric support
3. **Index Coverage**: All 8 triple patterns answered correctly via appropriate index
4. **Bulk Performance**: 1M triples loaded in <30 seconds
5. **Persistence**: Data survives process restart with full integrity
6. **Integration**: Clean adapter layer for RDF.ex types

## Provides Foundation

This phase establishes the infrastructure for:
- **Phase 2**: Query engine uses indices for BGP evaluation and dictionary for result serialization
- **Phase 3**: Leapfrog Triejoin uses iterator seek operations; UPDATE uses WriteBatch
- **Phase 4**: Reasoner stores derived triples in `derived` column family
- **Phase 5**: Statistics form basis for query optimization and telemetry

## Key Outputs

- `TripleStore.Backend.RocksDB` - NIF module for RocksDB operations
- `TripleStore.Dictionary` - Term encoding/decoding with inline numeric support
- `TripleStore.Index` - Triple indices with pattern-based lookup
- `TripleStore.RDFAdapter` - Integration with RDF.ex types
- `TripleStore.Statistics` - Basic cardinality statistics
