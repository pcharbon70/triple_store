# Section 7.2: Quad Pattern Matching for Rules - Summary

**Date:** 2025-01-18
**Status:** COMPLETE
**Feature Branch:** `feature/section-7.2-quad-pattern-matching`

## Overview

Section 7.2 extends the reasoning rule system to support quad patterns (graph, subject, predicate, object) in rule heads and bodies, enabling graph-aware reasoning. This builds on Section 7.1's infrastructure for graph-scoped reasoning.

## Implementation Summary

### Task 7.2.1: Rule Pattern Extension

**File:** `lib/triple_store/reasoner/rule.ex`

Added quad pattern support to the Rule module:

1. **head_quad_pattern/4** - Constructor for quad head patterns
   ```elixir
   Rule.head_quad_pattern(graph_term, subject, predicate, object)
   ```

2. **new_quad/5** - Constructor for quad-aware rules with metadata
   ```elixir
   Rule.new_quad(name, body, head, graph_id: 1, scope: :local)
   ```

3. **Helper functions**:
   - `quad_rule?/1` - Check if rule uses quad patterns
   - `graph_id/1` - Get graph_id from metadata
   - `scope/1` - Get scope from metadata
   - `applies_to_graph?/2` - Check if rule applies to specific graph
   - `put_graph_id/2`, `put_scope/2` - Update metadata
   - `validate_quad_body!/1` - Validate no mixed patterns

4. **Type updates**:
   - `@type t` head field now accepts `pattern() | quad_pattern()`
   - `@type metadata` includes `:graph_id`, `:scope`, `:tbox_rule`, `:quad_rule`

### Task 7.2.2: Quad Pattern Matching

**File:** `lib/triple_store/reasoner/delta_computation.ex`

Updated unification to use PatternMatcher for graph term handling:

1. **unify_pattern_with_fact/3** - Added quad pattern clause
   ```elixir
   defp unify_pattern_with_fact({:quad_pattern, [pg, ps, pp, po]}, {fg, fs, fp, fo}, binding) do
     with {:ok, b1} <- PatternMatcher.unify_graph_term(fg, pg, binding),
          # ... handle subject, predicate, object
   ```

The PatternMatcher module already had complete quad pattern support:
- `matches_quad?/2` - Match quads against patterns
- `matches_graph_term?/2` - Handle `:default`, `:all`, `{:bound, n}`, `{:var, name}`
- `unify_graph_term/3` - Unify graph terms with bindings

### Task 7.2.3: Rule Compilation for Quads

**File:** `lib/triple_store/reasoner/rule_compiler.ex`

Extended RuleCompiler with graph context support:

1. **compile/2 options**:
   - `:graph_id` - Which graph rules apply to (nil = global)
   - `:tbox_graph` - Graph containing TBox axioms (default: 0)

2. **compile_with_schema/2** - Updated with same options

3. **New functions**:
   - `add_graph_metadata/3` - Add graph_id/scope/tbox_rule to rules
   - `copy_graph_metadata/1` - Copy graph metadata for specialized rules
   - `derives_to_tbox_graph?/2` - Check if rule derives to TBox

4. **Specialization**:
   - `specialize_for_property/4` - Preserves graph metadata
   - `specialize_for_inverse/4` - Preserves graph metadata

## Files Modified

| File | Changes |
|------|---------|
| `lib/triple_store/reasoner/rule.ex` | Added quad pattern constructors, metadata helpers, validation |
| `lib/triple_store/reasoner/delta_computation.ex` | Updated unification for quad patterns |
| `lib/triple_store/reasoner/rule_compiler.ex` | Added graph_id/tbox_graph options, metadata handling |

## Files Created

| File | Purpose |
|------|---------|
| `test/triple_store/reasoner/section_7_2_quad_pattern_test.exs` | Unit tests (38 tests, all passing) |
| `notes/features/section-7.2-quad-pattern-matching.md` | Planning document |

## Test Results

```
Running ExUnit with seed: 597344
...
Finished in 0.1 seconds (0.1s async, 0.00s sync)
38 tests, 0 failures
```

### Test Coverage

- **Task 7.2.1**: 14 tests for rule pattern extension
- **Task 7.2.2**: 11 tests for quad pattern matching
- **Task 7.2.3**: 8 tests for rule compilation
- **Integration**: 5 tests for end-to-end scenarios

## Design Decisions

1. **No Mixed Patterns**: Rules cannot mix triple and quad patterns in the body. This is enforced by `validate_quad_body!/1`.

2. **Metadata over Struct Changes**: Graph context is stored in the metadata map rather than adding new struct fields, maintaining backward compatibility.

3. **Scope Semantics**:
   - `:local` - Rule applies only to specified graph_id
   - `:global` - Rule applies to all graphs
   - Default for compiled rules: `:local` if graph_id set, `:global` otherwise

4. **TBox Default**: TBox graph defaults to 0 (default graph), following user clarification.

## Dependencies

This section depends on:
- Section 7.1: Reasoning Scope Design (GraphReasoningConfig, GraphScopedReasoner)

This section enables:
- Section 7.3: Graph-Local Materialization
- Section 7.4: Global Materialization

## Next Steps

1. Section 7.3: Graph-Local Materialization (using quad-aware rules)
2. Section 7.4: Global Materialization
3. Rule optimization for quad patterns
4. Parallel rule evaluation across graphs

## Notes

- The PatternMatcher module already had complete quad support from Section 7.1
- Most work was extending Rule and RuleCompiler to use existing pattern matching
- Backward compatibility maintained - existing triple-only rules work unchanged
