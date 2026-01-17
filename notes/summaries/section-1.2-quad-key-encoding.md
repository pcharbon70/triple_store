# Section 1.2: Quad Key Encoding - Implementation Summary

## Branch
`feature/section-1.2-quad-key-encoding`

## Date Completed
2025-01-09

## Overview

Implemented quad key encoding for all four indices (GSPO, GPOS, SPOG, POSG) with 32-byte keys (4 × 64-bit IDs). This includes encoding, decoding, prefix construction, and utility functions.

## Tasks Completed

### 1.2.1 Key Encoding Functions
- [x] `gspo_key(g, s, p, o)` returning `<<g::64-big, s::64-big, p::64-big, o::64-big>>`
- [x] `gpos_key(g, p, o, s)` returning `<<g::64-big, p::64-big, o::64-big, s::64-big>>`
- [x] `spog_key(s, p, o, g)` returning `<<s::64-big, p::64-big, o::64-big, g::64-big>>`
- [x] `posg_key(p, o, s, g)` returning `<<p::64-big, o::64-big, s::64-big, g::64-big>>`

### 1.2.2 Key Decoding Functions
- [x] `decode_gspo_key(key)` extracting `{g, s, p, o}`
- [x] `decode_gpos_key(key)` extracting `{g, p, o, s}`
- [x] `decode_spog_key(key)` extracting `{s, p, o, g}`
- [x] `decode_posg_key(key)` extracting `{p, o, s, g}`

### 1.2.3 Quad Prefix Functions
- [x] `gspo_prefix(g)` for graph-scoped scans (8 bytes)
- [x] `gspo_prefix(g, s)` for graph-subject scans (16 bytes)
- [x] `gspo_prefix(g, s, p)` for graph-subject-predicate scans (24 bytes)
- [x] `spog_prefix(s)` for subject scans across graphs (8 bytes)
- [x] `spog_prefix(s, p)` for subject-predicate scans (16 bytes)
- [x] `posg_prefix(p)` for predicate scans across graphs (8 bytes)
- [x] `gpos_prefix(g)` and variants for graph-predicate scans

### 1.2.4 Quad Key Utilities
- [x] `encode_quad_keys/4` returning map of all four index keys
- [x] `key_to_quad/2` converting any index key to canonical `{s, p, o, g}`
- [x] `quad_to_triple/1` extracting `{s, p, o}` from quad
- [x] Guards for valid term IDs in all encoding functions
- [x] `index_for_key/1` to indicate index cannot be determined from key alone
- [x] `default_graph_id/0` returning 0
- [x] `is_default_graph?/1` checking if ID is default graph

## Files Created

1. **lib/triple_store/quad_index.ex** (new module, ~790 lines)
   - Four quad index encoding/decoding functions
   - Prefix construction for all pattern types
   - Utility functions for quad operations
   - Default graph constant and predicate

2. **test/triple_store/quad_index_test.exs** (new test file, ~380 lines)
   - 55 comprehensive tests covering all functions
   - Roundtrip encoding/decoding tests
   - Lexicographic ordering verification
   - Prefix boundary tests
   - Named graph tests
   - Edge case and boundary condition tests

## Key Design Decisions

1. **Canonical Form**: `{s, p, o, g}` (subject, predicate, object, graph)
   - Matches RDF quad convention (s, p, o, g)
   - Consistent with triple store canonical form `{s, p, o}`

2. **Default Graph ID**: `0` (reserved, never allocated by dictionary)
   - Simple special value check
   - Efficient comparison
   - Default graph sorts before all named graphs

3. **32-byte Keys**: 4 × 64-bit big-endian integers
   - +33% larger than triple store keys (24 bytes)
   - Enables lexicographic ordering matching numeric ID ordering
   - Supports efficient prefix-based range scans

4. **Index Detection**: Not possible from key alone
   - All four indices have same structure (4 × 64-bit integers)
   - Caller must track which index a key came from

## Test Results

All 55 tests passing:
- Key encoding/decoding tests
- Prefix construction tests
- Utility function tests
- Roundtrip tests
- Lexicographic ordering tests
- Prefix boundary tests
- Named graph tests
- Edge case tests

## Next Steps

Section 1.2 is complete. The next section would be implementing:
- Section 1.3: Graph ID Representation (default graph ID validation, dictionary integration)
- Section 1.4: Quad Pattern Matching (index selection for all 16 quad patterns)

This work is part of Phase 1: Quad Storage Foundation.
