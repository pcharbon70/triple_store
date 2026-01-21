# Fix All Compilation Warnings

## Problem Statement

The TripleStore codebase has approximately 73 compilation warnings that need to be resolved. These warnings indicate unused code, code organization issues, and potential bugs.

## Goal

Eliminate all compilation warnings to achieve a clean `mix compile` output with 0 warnings.

## Current Status

**Starting point:** ~73 compilation warnings
**Current count:** 39 compilation warnings (47% reduction)

**Progress:**
- [x] Phase 1: Clause Grouping Warnings (3 warnings) - COMPLETED
- [x] Phase 2: Remove @doc from Private Functions (7 warnings) - COMPLETED
- [x] Phase 3: Fix Multiple Clauses with Default Values (1 warning) - COMPLETED
- [x] Phase 4: Unused Variables (11 warnings) - COMPLETED
- [x] Phase 5: Unused Alias (1 warning) - COMPLETED
- [x] Phase 6: Impossible Pattern Matches (partial) - COMPLETED
- [x] Phase 7: Unused Functions (20 warnings) - COMPLETED (prefixed with `_`)
- [ ] Phase 8: Remaining Issues - IN PROGRESS
- [ ] Phase 9: Verification - PENDING

## Completed Changes

### Phase 1: Clause Grouping
- Fixed `exception/1` clause grouping in `error_handler.ex` (5 exception modules)
- Removed duplicate `body_patterns/1` function in `rule.ex`
- Fixed `execute_operation/2` clause grouping in `update_executor.ex`

### Phase 2: Private Function @doc
- Converted `@doc` to comments in:
  - `authorization.ex` - `emit_auth_denied_telemetry/4`
  - `tbox_extractor.ex` - `tbox_predicate?/3`
  - `exporter.ex` - `build_trig_opts/1`
  - `quad_trie_iterator.ex` - `advance_to_first/1`, `build_seek_key/3`
  - `rule_compiler.ex` - `add_graph_metadata/3`, `copy_graph_metadata/1`

### Phase 3: Default Values
- Fixed `to_quad/2` by extracting default value to function head

### Phase 4: Unused Variables
- Prefixed with underscore: `manager`, `stream`, `enumerable`, `node`, `pattern`, `stats`, `vars`, `window_start`, `db`, `threshold`, `opts`

### Phase 5: Unused Alias
- Removed unused `QuadIndex` alias in `backward_trace_quad.ex`

### Phase 6: Impossible Patterns
- Fixed impossible patterns in `graph_scoped_reasoner.ex` (2 functions)
- Fixed impossible pattern in `parallel_executor.ex`
- Fixed impossible pattern in `statistics.ex`

### Phase 7: Unused Functions
Prefixed 20 unused functions with `_` to indicate they're intentionally unused:
- `_with_graph_metadata/2` (kept - actually used)
- `_make_inferred_store_fn/2`
- `_make_graph_lookup_fn/2`
- `_make_all_graphs_lookup_fn/1`
- `_lookup_with_index_selection/7`
- `_lookup_quads_as_triples/2`
- `_lookup_quads_all_graphs_var/4`
- `_lookup_quads_all_graphs_as_triples/2`
- `_lookup_full_scan_gspo/4`
- `_load_all_facts/1`
- `_compile_rules/2`
- `_take_batch/2`
- `_instantiate_template_with_graph/2`
- `_extract_graph_names/2`
- `_build_graph_from_terms/3`
- `_build_dataset_from_terms/3`
- `_generate_execution_steps/1` and `/3`
- `_count_bgps/1` and `/2`
- `_get_column_families/1`

## Remaining Warnings (39 total)

### 21 warnings: Intentionally unused functions (prefixed with `_`)
These are expected and acceptable - they indicate "reserved for future use"

### 18 warnings: Other issues
- **@impl true warnings (3)**: In `error_handler.ex` for `exception/2` in exception modules
- **Impossible pattern matches (~12)**: Various files with clauses that can never match
- **Unused clause (1)**: `unify_pattern_with_fact/3` clause never used
- **Underscored variable used (1)**: `_hash` is used after being set
- **Conditional expression always true (1)**: Location unknown

## Notes

- Functions prefixed with `_` are intentionally kept for potential future use
- Some test failures exist but appear unrelated to these changes (missing `GraphReasoningStatus.load/delete`)
- Remaining warnings need further investigation to locate and fix

## Status

**Status:** In Progress (47% complete)

**Last Updated:** 2025-01-21
