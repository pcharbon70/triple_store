# ADR-003: Scope Handling Design for Graph-Aware Reasoning

**Status**: Accepted
**Date**: 2026-01-20
**Deciders**: Engineering Team
**Related**: Phase 7 (Reasoning with Named Graphs), GraphScopedReasoner module

---

## Context and Problem Statement

Graph-aware reasoning needs to handle three distinct scopes:

1. **Local (`:local`)** - Reason within a single graph, using TBox as read-only
2. **Global (`:global`)** - Reason across all graphs as if they were one merged dataset
3. **Hybrid (`:hybrid`)** - Configure per-graph scope, mixing local and global

The challenge is determining what `:hybrid` means when graphs don't have explicit configuration.

### The Problem

```elixir
# User configures hybrid mode
config = ReasoningConfig.new(scope: :hybrid)

# System has graphs 0, 1, 2, 3
# Graphs 1 and 2 have explicit scope configuration
# Graphs 0 and 3 have no configuration

# Question: What scope should graphs 0 and 3 use?
```

### Options

1. **Default to `:local`** - Unconfigured graphs use local reasoning
2. **Default to `:global`** - Unconfigured graphs participate in global reasoning
3. **Require explicit config** - Raise error for unconfigured graphs
4. **Configurable default** - Add `hybrid_default_scope` option

## Decision

We use **option 1 with option 4**:

1. **Default behavior**: Unconfigured graphs in hybrid mode default to `:local`
2. **Warning logged**: Emit warning for each unconfigured graph
3. **Configurable default**: Allow `hybrid_default_scope` option for explicit control

### Implementation

```elixir
defmodule TripleStore.Reasoner.GraphHelpers do
  @moduledoc """
  Scope handling utilities for graph-aware reasoning.
  """

  @type scope :: :local | :global | :hybrid

  @doc """
  Normalizes scope for operations, handling hybrid delegation.

  ## Parameters
  - scope: The requested scope
  - default: The default to use when scope is :hybrid

  ## Returns
  - :local or :global (never returns :hybrid)

  ## Examples
      iex> normalize_scope(:local, :local)
      :local

      iex> normalize_scope(:hybrid, :local)
      :local
  """
  def normalize_scope(:hybrid, default), do: default
  def normalize_scope(scope, _default), do: scope

  @doc """
  Validates scope value.
  """
  def validate_scope(scope) when scope in [:local, :global, :hybrid], do: :ok
  def validate_scope(other), do: {:error, {:invalid_scope, other}}
end
```

### Hybrid Mode Behavior

```elixir
defp partition_graphs_by_scope(config, db) do
  all_graph_ids = get_all_graph_ids(db)

  case ReasoningConfig.scope(config) do
    :hybrid ->
      # For hybrid mode, require explicit per-graph configuration
      # Fallback to :local with warning
      partition_with_hybrid_defaults(all_graph_ids, config)

    :local ->
      # All graphs use local reasoning
      partition_all_local(all_graph_ids, config)

    :global ->
      # All graphs participate in global reasoning
      partition_all_global(all_graph_ids, config)
  end
end

defp partition_with_hybrid_defaults(all_graph_ids, config) do
  default_scope = ReasoningConfig.hybrid_default_scope(config) || :local

  Enum.reduce(all_graph_ids, {%{}, %{}}, fn graph_id, {local_acc, global_acc} ->
    case ReasoningConfig.graph_config(config, graph_id) do
      {:ok, %GraphReasoningConfig{scope: scope}} when scope in [:local, :global] ->
        partition_by_scope(scope, graph_id, config, local_acc, global_acc)

      {:ok, %GraphReasoningConfig{scope: :none}} ->
        # This graph opts out of reasoning
        {local_acc, global_acc}

      :error ->
        # No explicit config - use default with warning
        if ReasoningConfig.warn_on_unconfigured?(config) do
          Logger.warning(
            "Graph #{graph_id} has no explicit config in hybrid mode, using #{default_scope}"
          )
        end

        partition_by_scope(default_scope, graph_id, config, local_acc, global_acc)
    end
  end)
end
```

### Configuration Options

```elixir
@type hybrid_default_scope :: :local | :global

@doc """
Creates a new reasoning configuration.

## Options

- `:scope` - Overall reasoning scope (:local | :global | :hybrid)
- `:hybrid_default_scope` - Default for unconfigured graphs in hybrid mode (default: :local)
- `:warn_on_unconfigured` - Whether to warn about unconfigured graphs (default: true)

"""
def new(opts) do
  struct(__MODULE__, [
    scope: Keyword.get(opts, :scope, :local),
    hybrid_default_scope: Keyword.get(opts, :hybrid_default_scope, :local),
    warn_on_unconfigured: Keyword.get(opts, :warn_on_unconfigured, true)
  ])
end
```

## Rationale

### Why Default to :local

1. **Principle of least surprise**: Local reasoning is the expected default
2. **Isolation**: Unconfigured graphs remain isolated from others
3. **Safety**: Global reasoning can cause unexpected cross-graph inferences
4. **Migration path**: Users can opt-in to global reasoning per graph

### Why Not Default to :global

1. **Unexpected behavior**: Users may not expect unconfigured graphs to participate globally
2. **Performance**: Global reasoning is more expensive than local
3. **Data integrity**: Cross-graph inferences may be incorrect for multi-tenant scenarios
4. **Debugging difficulty**: Harder to trace where inferences came from

### Why Not Require Explicit Config

1. **Migration friction**: Existing datasets have no per-graph configuration
2. **Usability**: Requires configuration even for simple single-graph cases
3. **Backward compatibility**: Breaking change from pre-hybrid behavior
4. **Flexibility**: Default + warning is a good middle ground

### Why Hybrid Exists

1. **Complex datasets**: Some graphs should be local, some global
2. **TBox sharing**: Schema graphs should be global, data graphs local
3. **Multi-tenant**: Each tenant local, but sharing a common ontology
4. **Performance**: Only expensive global reasoning where needed

## Scope Semantics

### Local Scope

- **Reasoning**: Each graph materializes independently
- **TBox**: Shared across graphs (read-only reference)
- **Storage**: Derived quads stored in their source graph
- **Use cases**: Multi-tenant, isolated datasets, per-user graphs

```
Graph 0 (TBox):  [schema triples] ─┐
Graph 1:        [data triples]    ├──> Derived 1 (local only)
Graph 2:        [data triples]    ├──> Derived 2 (local only)
Graph 3:        [data triples]    └──> Derived 3 (local only)
```

### Global Scope

- **Reasoning**: All graphs treated as one merged dataset
- **TBox**: Part of the merged dataset
- **Storage**: Derived quads stored in designated inference graph
- **Use cases**: Unified datasets, cross-graph inference, knowledge graphs

```
All Graphs:     [merged triples] ───> Derived (global)
```

### Hybrid Scope

- **Reasoning**: Per-graph configuration of local/global
- **TBox**: Shared for local graphs, merged for global graphs
- **Storage**: Depends on each graph's configuration
- **Use cases**: Complex multi-scenario datasets

```
Graph 0 (TBox):  [schema] ─────────────────────┐
Graph 1 (local): [data] ───────────────────────┤
Graph 2 (local): [data] ───────────────────────┤
Graph 3 (global): [data] ──┐                   ├──> Derived (global)
Graph 4 (global): [data] ──┴──> Merged ─────────┘
```

## Implementation Details

### Scope Parameter Wiring

The `scope` parameter flows through the reasoning pipeline:

```elixir
# Entry point
def materialize(db, opts) do
  scope = Keyword.get(opts, :scope, :local)
  # ...
end

# Incremental reasoning
def add_quads(db, quads, opts) do
  scope = Keyword.get(opts, :scope, :local)
  semi_naive_opts = Keyword.put(opts, :scope, scope)
  # ...
end

# Semi-naive materialization
def materialize(lookup_fn, store_fn, rules, initial_facts, opts) do
  scope = Keyword.get(opts, :scope, :local)
  # Include scope in stats for telemetry
  # ...
end
```

### Scope in Telemetry

```elixir
@doc """
Emits telemetry event for reasoning operations.

Event metadata includes:
- scope: :local | :global | :hybrid
- graph_count: number of graphs processed
- iterations: number of fixpoint iterations
- derived: number of derived quads
"""
def emit_reasoning_complete(measurements, metadata) do
  :telemetry.execute(
    [:triple_store, :reasoner, :materialization_complete],
    measurements,
    metadata
  )
end
```

## Alternatives Considered

### Alternative 1: Always Require Explicit Config

```elixir
def partition_with_hybrid_defaults(all_graph_ids, config) do
  Enum.reduce(all_graph_ids, {%{}, %{}}, fn graph_id, {local_acc, global_acc} ->
    case ReasoningConfig.graph_config(config, graph_id) do
      {:ok, gc} -> partition_by_scope(gc.scope, graph_id, ...)
      :error -> raise "Graph #{graph_id} must have explicit config in hybrid mode"
    end
  end)
end
```

**Rejected because**:
- Breaking change for existing code
- Poor developer experience
- No good default for simple use cases

### Alternative 2: Default to Global

```elixir
@default_hybrid_scope :global
```

**Rejected because**:
- Can cause unexpected cross-graph inferences
- Performance degradation
- Security concerns in multi-tenant scenarios

### Alternative 3: No Hybrid Mode

Only support `:local` or `:global` at top level.

**Rejected because**:
- Doesn't address complex multi-scenario use cases
- Forces users to choose one scope for entire dataset
- Hybrid is explicitly mentioned in requirements

## Consequences

### Positive

1. **Explicit control**: Users can configure per-graph scope
2. **Sensible default**: Unconfigured graphs default to safe local reasoning
3. **Visibility**: Warnings alert users to unconfigured graphs
4. **Flexibility**: `hybrid_default_scope` allows overriding default

### Negative

1. **Complexity**: Three scope modes increase API surface area
2. **Configuration burden**: Users must understand hybrid to use it effectively
3. **Warning fatigue**: Many warnings may be emitted for large datasets
4. **Testing burden**: All three modes must be tested

### Mitigation Strategies

1. **Clear documentation**: Explain when to use each scope
2. **Warning suppression**: `warn_on_unconfigured: false` option
3. **Validation helpers**: Functions to check configuration completeness
4. **Telemetry**: Track scope usage to guide users

## Migration Guide

For users upgrading to hybrid-aware reasoning:

```elixir
# Before: Only local or global
config = ReasoningConfig.new(scope: :local)

# After: Can use hybrid with defaults
config = ReasoningConfig.new(
  scope: :hybrid,
  hybrid_default_scope: :local,  # Safe default
  warn_on_unconfigured: false    # Suppress warnings if desired
)

# Or configure per-graph
config = ReasoningConfig.new(
  scope: :hybrid,
  graph_configs: %{
    1 => GraphReasoningConfig.new(scope: :local),
    2 => GraphReasoningConfig.new(scope: :global),
    3 => GraphReasoningConfig.new(scope: :local)
  }
)
```

## References

- GraphScopedReasoner module: `lib/triple_store/reasoner/graph_scoped_reasoner.ex`
- GraphHelpers module: `lib/triple_store/reasoner/graph_helpers.ex`
- ReasoningConfig module: `lib/triple_store/reasoner/reasoning_config.ex`
- Phase 7 planning document: `notes/features/phase-7-review-fixes.md`

## Revisions

| Date | Change | Author |
|------|--------|--------|
| 2026-01-20 | Initial ADR | Engineering Team |
