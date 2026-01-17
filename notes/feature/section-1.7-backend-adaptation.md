# Working Plan: Section 1.7 - Backend Adaptation

## Branch: `feature/section-1.7-backend-adaptation`

## Status: COMPLETED

## Overview

Update backend components for quad store support. Most quad infrastructure already exists;
this section completes the integration by updating read options and adding documentation.

## Tasks

### 1.7.1 Column Family Configuration
- [x] Quad CFs already defined in ColumnFamilyConfig (gspo, gpos, spog, posg)
- [x] Update ReadOptions.for_cf/1 to handle quad index CFs
- [x] Verify quad CF options are properly configured

### 1.7.2 ErlangAdapter Updates
- [x] open/2 with :schema option already exists
- [x] create_new_database/2 already handles quad CF creation
- [x] is_quad_store?/1 predicate already exists
- [x] Document migration path (export/import required)

### 1.7.3 Read Options for Quads
- [x] Add quad_prefix_scan/0 preset for GSPO/GPOS queries
- [x] Add cross_graph_scan/0 preset for SPOG/POSG queries
- [x] Update for_cf/1 to handle quad index CFs
- [x] Document read strategy per index type

## Files Modified

1. `lib/triple_store/backend/rocksdb/read_options.ex` - Added quad read presets and for_cf extensions
2. `lib/triple_store/backend/rocksdb/erlang_adapter.ex` - Added migration documentation
3. `test/triple_store/backend/rocksdb/read_options_quad_test.exs` - 13 unit tests

## Key Design Decisions

1. **Backward Compatibility**: Keep triple store CFs (spo, pos, osp) for v1 databases
2. **Read Strategy**: Use same options as prefix_scan() for both triple and quad indices
3. **Migration**: Export/import required for triple to quad conversion

## Success Criteria

1. [x] for_cf/1 handles all quad column families
2. [x] Quad-specific read presets documented
3. [x] Migration path documented in ErlangAdapter
4. [x] All tests passing (13 tests)

## References

- See `notes/planning/quad/phase-01-quad-storage-foundation.md` section 1.7
- See `notes/summaries/section-1.7-backend-adaptation.md` for implementation summary
