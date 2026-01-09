# Working Plan: Section 1.3 - Graph ID Representation

## Branch: `feature/section-1.3-graph-id-representation`

## Status: COMPLETED

## Overview

Implement graph ID representation, including default graph ID validation and named graph encoding. The default graph (ID 0) is reserved and never allocated by the dictionary.

## Tasks

### 1.3.1 Default Graph Identifier
- [x] Define `@default_graph_id` as `0` (already in QuadIndex)
- [x] Document that ID 0 is reserved in Dictionary module
- [x] Add validation that dictionary never allocates ID 0
- [x] Implement `is_default_graph?(id)` guard function (already in QuadIndex)

### 1.3.2 Named Graph Encoding
- [x] Verify graph URIs work with existing dictionary IRI encoding
- [x] Verify blank node graphs work with existing bnode encoding
- [x] Document that graph terms reuse existing type tagging

### 1.3.3 Graph ID Resolution
- [x] Implement `resolve_graph_id(db, graph_ref)` for named graphs
- [x] Implement `get_or_create_graph_id(manager, graph_term)` for named graphs
- [x] Implement `id_to_graph_term(db, graph_id)` reverse lookup
- [x] Add tests for graph ID resolution

## Files Modified

1. `lib/triple_store/dictionary.ex` - Added graph ID documentation and helper functions
2. `lib/triple_store/quad_index.ex` - Added graph resolution functions
3. `test/triple_store/quad_index_test.exs` - Added tests for graph ID representation

## Key Design Decisions

1. **Default Graph ID = 0**: Reserved, never allocated by dictionary
2. **Named Graphs**: Reuse existing dictionary encoding (URIs, blank nodes)
3. **No Special Graph ID Space**: Graph terms use same ID space as other terms
4. **:default Atom**: Special case in resolve_graph_id returning 0

## Success Criteria - ALL MET

1. [x] Dictionary never allocates ID 0 (documented in moduledoc)
2. [x] Graph terms (URIs, blank nodes) encoded correctly
3. [x] Graph ID resolution functions handle :default and named graphs
4. [x] All tests passing (62/62)

## Summary

All tasks for Section 1.3 have been completed. The graph ID representation layer is in place with:
- Default graph ID (0) properly reserved and documented
- Named graphs using existing dictionary encoding (URIs, blank nodes)
- Graph ID resolution functions for lookups and creation
- Comprehensive test coverage (7 new tests, 62 total)

See `notes/summaries/section-1.3-graph-id-representation.md` for detailed implementation summary.

## References

- See `notes/planning/quad/phase-01-quad-storage-foundation.md` section 1.3
