# Task 3.4.3: Property Path Optimization

## Summary

Implemented optimizations for SPARQL property path evaluation:
1. **Fixed-length path optimization** - Detects sequences of simple predicates (p1/p2/p3) and converts to efficient chained lookups
2. **Bidirectional search** - Uses bidirectional BFS when both endpoints are bound for recursive paths (p*, p+)
3. **Materialized path indices** - Considered but not implemented (optional per plan)

## Files Changed

### Modified Files

- `lib/triple_store/sparql/property_path.ex` - Added path optimization section:
  - `evaluate_optimized/5` - Entry point that tries optimizations before standard evaluation
  - `fixed_length_path?/1` - Detects sequences of simple predicates
  - `simple_predicate?/1` - Checks if path element is a link, named_node, or reverse
  - `evaluate_fixed_length_path/5` - Optimizes fixed-length paths via predicate chain extraction
  - `extract_predicate_chain/1` - Extracts direction-tagged predicates from sequence paths
  - `evaluate_predicate_chain/5` - Evaluates predicate chains efficiently
  - `evaluate_bidirectional/7` - Uses bidirectional BFS for bounded recursive paths
  - `bidirectional_bfs/4` - Bidirectional BFS implementation with 1-hop and 2-hop fast paths
  - `do_bidirectional_bfs/8` - Recursive BFS expanding the smaller frontier
  - `get_one_step_forward/3` - Gets all nodes reachable in one step via a path

- `test/triple_store/sparql/property_path_test.exs` - Added 10 new optimization tests

## Implementation Details

### Fixed-Length Path Optimization

Sequences of simple predicates (links, named nodes, inverse predicates) are detected and converted to a chain of index lookups. This avoids the overhead of stream processing for intermediate results.

```elixir
defp fixed_length_path?({:sequence, left, right}) do
  simple_predicate?(left) and (simple_predicate?(right) or fixed_length_path?(right))
end

defp simple_predicate?({:link, _}), do: true
defp simple_predicate?({:named_node, _}), do: true
defp simple_predicate?({:reverse, inner}), do: simple_predicate?(inner)
defp simple_predicate?(_), do: false
```

### Bidirectional Search Optimization

When both endpoints are bound for a recursive path (p* or p+), we use bidirectional BFS which can be significantly faster than unidirectional search in sparse graphs:

- Search expands from both ends simultaneously
- Early termination when frontiers meet
- 1-hop and 2-hop fast paths for common cases
- Maximum depth limit of 50 to prevent runaway expansion

Key insight: For one-or-more paths (p+), the visited sets must NOT include the start/target initially - only nodes discovered through traversal. This ensures we don't incorrectly report a path when start == target but no actual cycle exists.

```elixir
defp bidirectional_bfs(ctx, inner_path, start_id, target_id) do
  # Take one step from each side first
  forward_step_1 = get_one_step_forward(ctx, inner_path, start_id)
  backward_step_1 = get_one_step_forward(ctx, reversed_path, target_id)

  # Check for 1-hop connection
  if MapSet.member?(forward_step_1, target_id) or MapSet.member?(backward_step_1, start_id) do
    true
  else
    # Check for 2-hop connection (frontiers meet in the middle)
    if MapSet.intersection(forward_step_1, backward_step_1) |> MapSet.size() > 0 do
      true
    else
      # Continue with bidirectional BFS
      # visited sets track only discovered nodes, NOT the start/target
      forward_visited = forward_step_1
      backward_visited = backward_step_1
      do_bidirectional_bfs(...)
    end
  end
end
```

### Materialized Path Indices

Considered but not implemented. The tradeoffs:
- **Pros**: O(1) lookup for transitive closures of common predicates
- **Cons**:
  - Significant storage overhead for large graphs
  - Complex invalidation logic on INSERT/DELETE operations
  - Only useful for frequently-traversed paths with stable data

For most use cases, the bidirectional BFS optimization provides sufficient performance without the maintenance overhead.

## Tests

61 total tests in property_path_test.exs including 10 new optimization tests:

**Fixed-length path tests (4):**
- `fixed-length path p1/p2 optimization` - 2-step sequence
- `fixed-length path p1/p2/p3 optimization` - 3-step sequence
- `fixed-length path with reverse predicate` - sequence with ^p
- `fixed-length path with unbound subject` - backward chaining

**Bidirectional search tests (6):**
- `bidirectional search finds path between bound endpoints` - 4-hop path
- `bidirectional search returns no path when none exists` - disconnected components
- `bidirectional search handles cycle correctly` - path via cycle
- `bidirectional search with zero-or-more same node` - identity path
- `bidirectional search 1-hop path` - direct connection
- `bidirectional search 2-hop path meeting in middle` - frontiers meet

## Branch

`feature/3.4.3-path-optimization`

## Status

Complete - All 2228 tests pass including 61 property path tests.
