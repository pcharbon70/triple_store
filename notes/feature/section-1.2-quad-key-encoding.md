# Working Plan: Section 1.2 - Quad Key Encoding

## Branch: `feature/section-1.2-quad-key-encoding`

## Status: COMPLETED

## Overview

Implement quad key encoding for all four indices (GSPO, GPOS, SPOG, POSG) with 32-byte keys (4 × 64-bit IDs). This includes encoding, decoding, prefix construction, and utility functions.

## Tasks

### 1.2.1 Key Encoding Functions
- [x] Implement `gspo_key(g, s, p, o)` returning `<<g::64-big, s::64-big, p::64-big, o::64-big>>`
- [x] Implement `gpos_key(g, p, o, s)` returning `<<g::64-big, p::64-big, o::64-big, s::64-big>>`
- [x] Implement `spog_key(s, p, o, g)` returning `<<s::64-big, p::64-big, o::64-big, g::64-big>>`
- [x] Implement `posg_key(p, o, s, g)` returning `<<p::64-big, o::64-big, s::64-big, g::64-big>>`
- Location: `lib/triple_store/quad_index.ex` (new module)

### 1.2.2 Key Decoding Functions
- [x] Implement `decode_gspo_key(key)` extracting `{g, s, p, o}`
- [x] Implement `decode_gpos_key(key)` extracting `{g, p, o, s}`
- [x] Implement `decode_spog_key(key)` extracting `{s, p, o, g}`
- [x] Implement `decode_posg_key(key)` extracting `{p, o, s, g}`

### 1.2.3 Quad Prefix Functions
- [x] Implement `gspo_prefix(g)` for graph-scoped scans (8 bytes)
- [x] Implement `gspo_prefix(g, s)` for graph-subject scans (16 bytes)
- [x] Implement `gspo_prefix(g, s, p)` for graph-subject-predicate scans (24 bytes)
- [x] Implement `spog_prefix(s)` for subject scans across graphs (8 bytes)
- [x] Implement `spog_prefix(s, p)` for subject-predicate scans (16 bytes)
- [x] Implement `posg_prefix(p)` for predicate scans across graphs (8 bytes)
- [x] Implement corresponding prefix functions for GPOS

### 1.2.4 Quad Key Utilities
- [x] Implement `encode_quad_keys/4` returning map of all four index keys
- [x] Implement `key_to_quad/2` converting any index key to canonical `{s, p, o, g}`
- [x] Implement `quad_to_triple/1` extracting `{s, p, o}` from quad (for compatibility)
- [x] Add guards for valid term IDs in all encoding functions
- [x] Implement `index_for_key/1` to indicate index cannot be determined from key

## Files Created

1. `lib/triple_store/quad_index.ex` - New module for quad key encoding/decoding (~790 lines)
2. `test/triple_store/quad_index_test.exs` - Tests for quad index functions (~380 lines, 55 tests)

## Success Criteria - ALL MET

1. [x] All four encoding/decoding functions work correctly
2. [x] Prefix functions generate correct byte sequences
3. [x] Utility functions provide convenient API
4. [x] All 55 tests passing

## Summary

All tasks for Section 1.2 have been completed. The quad key encoding layer is in place with:
- Four quad index encoding/decoding functions (GSPO, GPOS, SPOG, POSG)
- Prefix construction for all pattern types (8, 16, 24 byte prefixes)
- Utility functions (encode_quad_keys, key_to_quad, quad_to_triple, default_graph_id)
- Comprehensive test coverage (55 tests, all passing)

See `notes/summaries/section-1.2-quad-key-encoding.md` for detailed implementation summary.

## References

- See `notes/planning/quad/phase-01-quad-storage-foundation.md` section 1.2
