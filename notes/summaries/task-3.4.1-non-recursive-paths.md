# Task 3.4.1: Non-Recursive Property Paths

## Summary

Implemented SPARQL property path support for non-recursive path types: sequence paths (p1/p2), alternative paths (p1|p2), inverse paths (^p), and negated property sets (!(p1|p2|...)).

## Files Changed

### New Files

- `lib/triple_store/sparql/property_path.ex` - Core property path evaluation module with support for:
  - Link paths (simple predicates)
  - Sequence paths (p1/p2)
  - Alternative paths (p1|p2)
  - Inverse paths (^p)
  - Negated property sets (!(p1|p2|...))

- `test/triple_store/sparql/property_path_test.exs` - Comprehensive test suite with 27 tests covering all path types and edge cases

### Modified Files

- `lib/triple_store/sparql/executor.ex`:
  - Added `PropertyPath` alias
  - Added `term_to_index_pattern/3` clause for blank nodes (used as intermediate variables by spargebra)
  - Added `maybe_bind/4` clause for blank nodes to track intermediate bindings
  - Added `extend_bindings/3` clause for `:path` patterns delegating to PropertyPath module

- `lib/triple_store/sparql/query.ex`:
  - Added `PropertyPath` alias
  - Added path pattern handling in `execute_pattern/2`

## Implementation Details

### Parser Integration

The spargebra parser already supports property paths. Key discoveries:
- **Sequence paths** are expanded by spargebra into BGP with blank nodes as intermediate variables
- **Inverse paths** are optimized by spargebra by swapping subject/object for simple inverse cases
- **Alternative paths** and **negated property sets** produce `{:path, subject, path_expr, object}` algebra nodes

### PropertyPath Module Design

The module evaluates paths against the triple index, converting results back to algebra terms for binding:

```elixir
# Entry point
evaluate(ctx, binding, subject, path_expr, object)

# Handles different path types
evaluate_link(...)           # {:predicate, uri}
evaluate_sequence(...)       # {:sequence, [path1, path2, ...]}
evaluate_alternative(...)    # {:alternative, [path1, path2]}
evaluate_reverse(...)        # {:reverse, inner_path}
evaluate_negated_property_set(...)  # {:negated_property_set, [uri1, uri2, ...]}
```

### Blank Node Handling

A key fix was ensuring blank nodes from spargebra work correctly:

1. **term_to_index_pattern**: Blank nodes `{:blank_node, name}` are treated as variables for pattern matching
2. **maybe_bind**: Blank nodes are tracked in bindings with key `{:blank_node, name}` storing the term ID directly

This enables sequence paths like `knows/knows` to work correctly when expanded by spargebra into:
```
{:triple, ?s, knows, _:blank1}
{:triple, _:blank1, knows, ?o}
```

### Stream-Based Evaluation

All path evaluations return `{:ok, [binding1, binding2, ...]}` which are converted to streams in the executor for lazy evaluation and memory efficiency.

## Tests

27 tests covering:
- Link paths with bound/variable subjects and objects
- Sequence paths (2-step and 3-step)
- Alternative paths
- Inverse paths
- Negated property sets (single and multiple predicates)
- Query integration (alternative, inverse, negated paths via SPARQL)
- Sequence paths via SPARQL (parser expansion with blank nodes)
- Edge cases: no matches, combined paths

## Branch

`feature/3.4.1-non-recursive-paths`

## Status

Complete - All 27 tests pass, full test suite (2194 tests) passes.
