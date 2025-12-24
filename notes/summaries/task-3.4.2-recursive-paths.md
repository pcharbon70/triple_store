# Task 3.4.2: Recursive Property Paths

## Summary

Implemented SPARQL recursive property path support: zero-or-more (p*), one-or-more (p+), and optional (p?) paths with cycle detection via BFS traversal.

## Files Changed

### Modified Files

- `lib/triple_store/sparql/property_path.ex` - Added recursive path evaluation:
  - `evaluate_zero_or_more/5` - Handles p* paths including identity (0 steps)
  - `evaluate_one_or_more/5` - Handles p+ paths (at least 1 step)
  - `evaluate_zero_or_one/5` - Handles p? paths (0 or 1 step)
  - `bfs_forward/5` - BFS traversal with cycle detection via visited set
  - `reverse_path/1` - Path reversal for backwards reachability queries
  - Maximum depth limit of 100 to prevent runaway evaluation

- `test/triple_store/sparql/property_path_test.exs` - Added 24 new tests for recursive paths

## Implementation Details

### Path Semantics

- **Zero-or-more (p*)**: Matches identity (s=o) OR one or more path steps
- **One-or-more (p+)**: Requires at least one step, excludes identity
- **Optional (p?)**: Matches identity OR exactly one step (not transitive)

### BFS Traversal with Cycle Detection

All recursive paths use breadth-first search with a visited set for cycle detection:

```elixir
defp bfs_forward(ctx, inner_path, frontier, visited, depth \\ 0)

# Stop at max depth (100) to prevent runaway evaluation
defp bfs_forward(_ctx, _inner_path, _frontier, visited, depth) when depth > @max_path_depth do
  visited
end

# Continue BFS until no new nodes found
defp bfs_forward(ctx, inner_path, frontier, visited, depth) do
  next_nodes = # evaluate one step from each frontier node
  new_frontier = MapSet.difference(next_nodes, visited)
  new_visited = MapSet.union(visited, new_frontier)
  # recurse if new nodes found
end
```

### Path Reversal for Backwards Queries

When the object is bound and subject is unbound (`?s path* target`), we need to find all nodes that can reach the target. This is computed using the reversed path:

```elixir
defp reverse_path({:reverse, inner}), do: inner
defp reverse_path({:named_node, iri}), do: {:reverse, {:named_node, iri}}
defp reverse_path({:sequence, left, right}), do: {:sequence, reverse_path(right), reverse_path(left)}
# etc.
```

### Variable Binding Patterns

Each recursive path handles four cases:
1. **Both bound**: Check if path exists between subject and object
2. **Subject bound, object unbound**: BFS forward from subject
3. **Subject unbound, object bound**: BFS forward from object using reversed path
4. **Both unbound**: Enumerate all reachable pairs (expensive)

## Tests

51 total tests in property_path_test.exs including:
- Zero-or-more: identity, transitive closure, cycles, reverse queries
- One-or-more: excludes identity, transitive, cycles
- Zero-or-one: identity + one step only (not transitive)
- SPARQL integration tests for p*, p+, p?
- Complex paths: inverse paths with zero-or-more, self-loops

## Branch

`feature/3.4.2-recursive-paths`

## Status

Complete - All 51 property path tests pass, full test suite (2218 tests) passes.
