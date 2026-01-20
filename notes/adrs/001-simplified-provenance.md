# ADR-001: Simplified Graph Provenance Model

**Status**: Accepted
**Date**: 2026-01-20
**Deciders**: Engineering Team
**Related**: Phase 7 (Reasoning with Named Graphs)

---

## Context and Problem Statement

When implementing graph-aware reasoning (Phase 7), we needed to track the provenance of derived quads to support:

1. **Explaining inferences** - Users need to understand why a derived quad exists
2. **Incremental maintenance** - When a premise quad is deleted, we must identify which derived quads are affected
3. **Cross-graph dependencies** - Global reasoning can derive quads from premises in multiple graphs

A full provenance tracking system would record:
- The specific rule used to derive each quad
- The exact premise quads that triggered the rule
- The derivation chain (for multi-step reasoning)

This creates significant storage and computation overhead:
- Each derived quad requires storing its derivation proof
- Storage grows with rule complexity (multi-premise rules)
- Re-derivation requires replaying proofs

## Decision

We adopted a **simplified provenance model** that tracks **graph-level provenance** instead of full derivation proofs:

### What We Track

```elixir
# For each derived quad, we track:
# 1. Which graphs contributed premises to its derivation
{derived_quad, source_graph_ids}

# Example:
# If quad (0, 1, 10, 100) was derived using:
# - Premises from graph 0
# - Premises from TBox graph 1
# We store: {(0, 1, 10, 100) => MapSet<[0, 1]>}
```

### What We Don't Track

- The specific rule that produced the derivation
- The individual premise quads
- The derivation chain depth

### Storage

Provenance is stored in a separate RocksDB column family (`:derivation_provenance`) separate from the derived quads themselves. This allows:
- Efficient lookups of "which quads depend on graph X?"
- O(1) invalidation when a graph is modified
- Compact storage (graph IDs vs full quads)

## Rationale

### Benefits

1. **Simplicity**: Graph-level provenance is much simpler to implement and reason about
2. **Storage efficiency**: Store ~8 bytes per graph reference vs storing full quad proofs
3. **Invalidation performance**: O(n) where n = derived quads, not O(n × premises)
4. **Sufficient for use cases**:
   - Deletion still works correctly (mark affected quads for re-derivation)
   - Users can still get coarse-grained explanations ("this comes from graphs 0 and 1")

### Trade-offs

1. **No detailed explanations**: Cannot show the exact derivation chain
2. **Coarser invalidation**: Deleting a single quad may invalidate more derived quads than strictly necessary
3. **Future work**: Full provenance tracking remains a potential enhancement

### Why This Works For Our Use Case

- **OWL 2 RL has low rule depth**: Most derivations are 1-2 steps, so derivation chains are short
- **Re-derivation is cheap**: Semi-naive evaluation efficiently re-derives affected quads
- **Graph granularity matches user mental model**: Users think in terms of "this graph affects that graph"

## Implementation

### GraphProvenance Module

```elixir
defmodule TripleStore.Reasoner.GraphProvenance do
  @moduledoc """
  Tracks graph-level provenance for derived quads.

  Stores mapping: {derived_quad} => MapSet<source_graph_id>
  """

  defstruct provenance: %{}  # %{derived_quad => MapSet<source_graph_id>}

  @doc "Records that a derived quad came from specific source graphs"
  def add_source(tracker, derived_quad, source_graph_ids) when is_list(source_graph_ids) do
    Map.update(tracker, derived_quad, MapSet.new(source_graph_ids), fn existing ->
      MapSet.union(existing, MapSet.new(source_graph_ids))
    end)
  end

  @doc "Returns all quads that may be affected by changes to the given graph"
  def quads_dependent_on_graph(tracker, graph_id) do
    for {quad, sources} <- tracker.provenance,
        MapSet.member?(sources, graph_id),
        do: quad
  end

  @doc "Removes a derived quad from tracking"
  def remove_quad(tracker, derived_quad) do
    %{tracker | provenance: Map.delete(tracker.provenance, derived_quad)}
  end
end
```

### Integration with Materialization

```elixir
# During materialization, track which graphs contributed to each derivation
defp track_derivation(provenance, derived_quad, lookup_context) do
  source_graphs = lookup_context.source_graph_ids  # graphs used in this derivation
  GraphProvenance.add_source(provenance, derived_quad, source_graphs)
end
```

### Deletion with Provenance

```elixir
# When deleting from a graph:
defp delete_with_provenance(db, deleted_quads, graph_id) do
  # 1. Find all derived quads that may depend on this graph
  affected = GraphProvenance.quads_dependent_on_graph(provenance, graph_id)

  # 2. Remove affected derived quads (they'll be re-derived if still valid)
  Enum.each(affected, &DerivedStore.delete_derived_quad(db, &1))

  # 3. Run forward rederivation
  ForwardRederiveQuad.rederive_quads(db, affected, rules)
end
```

## Alternatives Considered

### Alternative 1: Full Derivation Proofs

Store complete proof trees for each derived quad:

```elixir
%{
  derived_quad => %{
    rule: :sub_property_of,
    premises: [{g1, s1, p1, o1}, {g2, s2, p2, o2}],
    derivation_depth: 2
  }
}
```

**Rejected because**:
- Storage overhead is 10-100x higher
- Proof tree management adds significant complexity
- Re-derivation from proofs is slower than semi-naive re-evaluation

### Alternative 2: Rule-Level Provenance Only

Store only which rule produced each derived quad:

```elixir
%{
  derived_quad => :sub_property_of
}
```

**Rejected because**:
- Doesn't support cross-graph invalidation
- Can't determine which graphs are affected by a deletion
- Insufficient for multi-graph reasoning scenarios

### Alternative 3: No Provenance (Re-materialize Everything)

On deletion, clear all derived quads and re-materialize from scratch.

**Rejected because**:
- Prohibitive for large datasets
- Unnecessary work for localized changes
- Poor user experience (long waits after small edits)

## Consequences

### Positive

1. **Simple implementation**: ~300 lines vs ~2000 for full provenance
2. **Efficient invalidation**: O(n) where n = affected quads
3. **Modular design**: Provenance tracking is isolated in GraphProvenance module
4. **Testable**: Easy to write unit tests for graph dependency tracking

### Negative

1. **Limited explanations**: Users can't see exact derivation chains
2. **Potential over-invalidation**: Deleting one quad may invalidate more than necessary
3. **Coarse granularity**: All premises in a graph treated equally

### Neutral

1. **Column family overhead**: Additional CF in RocksDB schema
2. **Memory usage**: Provenance tracker must be kept in memory during materialization
3. **Future extension possible**: Can add full provenance tracking later if needed

## References

- Phase 7 planning document: `notes/features/phase-7-review-fixes.md`
- GraphProvenance module: `lib/triple_store/reasoner/graph_provenance.ex`
- DerivationProvenance module: `lib/triple_store/reasoner/derivation_provenance.ex`

## Revisions

| Date | Change | Author |
|------|--------|--------|
| 2026-01-20 | Initial ADR | Engineering Team |
