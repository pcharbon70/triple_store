defmodule TripleStore.Reasoner.GraphProvenance do
  @moduledoc """
  Cross-graph dependency tracking for incremental reasoning.

  This module provides functionality to track which graphs a derived quad
  depends on. This is important for:
  - Correctly retracting derivations when a graph is modified
  - Understanding cross-graph dependencies in global reasoning
  - Optimizing re-derivation checks

  ## Provenance Model

  Each derived quad tracks the set of source graphs that contributed to its
  derivation. For example, if a quad in graph 1 was derived using premises
  from graphs 1 and 0 (TBox), its provenance would be MapSet([1, 0]).

  ## Limitations

  This is a simplified provenance model that tracks graph-level dependencies
  rather than full derivation chains. This means:
  - We may over-retract in some edge cases (safe but not optimal)
  - We don't track which specific premises from each graph were used
  - Cross-graph dependencies require manual re-derivation checks

  For most use cases, this simplified model provides a good balance between
  correctness and performance.

  ## Usage

      # Track provenance for a derived quad
      provenance = GraphProvenance.new()
      provenance = GraphProvenance.add_source(provenance, quad, [graph_id, tbox_graph_id])

      # Check if a derived quad depends on a specific graph
      depends_on? = GraphProvenance.depends_on?(provenance, quad, graph_id)

      # Get all source graphs for a derived quad
      sources = GraphProvenance.get_sources(provenance, quad)
  """

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "ID quad: {graph_id, subject_id, predicate_id, object_id}"
  @type id_quad :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "Set of graph IDs"
  @type graph_set :: MapSet.t(non_neg_integer())

  @typedoc "Provenance tracking map: quad => set of source graphs"
  @type provenance :: %{id_quad() => graph_set()}

  @typedoc "Provenance tracker with metadata"
  @type t :: %__MODULE__{
          tracking: provenance(),
          count: non_neg_integer()
        }

  defstruct [:tracking, :count]

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Creates a new provenance tracker.
  """
  @spec new() :: t()
  def new do
    %__MODULE__{
      tracking: %{},
      count: 0
    }
  end

  @doc """
  Adds source graph information for a derived quad.

  ## Parameters

  - `tracker` - The provenance tracker
  - `quad` - The derived quad to track
  - `source_graphs` - List of graph IDs that contributed to this derivation

  ## Returns

  Updated tracker with provenance information.
  """
  @spec add_source(t(), id_quad(), [non_neg_integer()]) :: t()
  def add_source(%__MODULE__{} = tracker, quad, source_graphs) when is_list(source_graphs) do
    graph_set = MapSet.new(source_graphs)

    updated_tracking =
      Map.update(tracker.tracking, quad, graph_set, fn existing ->
        MapSet.union(existing, graph_set)
      end)

    updated_count =
      if Map.has_key?(tracker.tracking, quad) do
        tracker.count
      else
        tracker.count + 1
      end

    %{tracker | tracking: updated_tracking, count: updated_count}
  end

  @doc """
  Checks if a derived quad depends on a specific graph.

  ## Parameters

  - `tracker` - The provenance tracker
  - `quad` - The derived quad to check
  - `graph_id` - The graph ID to check dependency on

  ## Returns

  `true` if the quad depends on the graph, `false` otherwise.
  """
  @spec depends_on?(t(), id_quad(), non_neg_integer()) :: boolean()
  def depends_on?(%__MODULE__{} = tracker, quad, graph_id) do
    case Map.get(tracker.tracking, quad) do
      nil -> false
      source_graphs -> MapSet.member?(source_graphs, graph_id)
    end
  end

  @doc """
  Gets all source graphs for a derived quad.

  ## Parameters

  - `tracker` - The provenance tracker
  - `quad` - The derived quad

  ## Returns

  Set of graph IDs that contributed to this quad's derivation.
  Returns `nil` if no provenance is tracked for this quad.
  """
  @spec get_sources(t(), id_quad()) :: graph_set() | nil
  def get_sources(%__MODULE__{} = tracker, quad) do
    Map.get(tracker.tracking, quad)
  end

  @doc """
  Removes provenance tracking for a quad.

  This is useful when a derived quad is deleted.

  ## Parameters

  - `tracker` - The provenance tracker
  - `quad` - The quad to remove

  ## Returns

  Updated tracker.
  """
  @spec remove_quad(t(), id_quad()) :: t()
  def remove_quad(%__MODULE__{} = tracker, quad) do
    if Map.has_key?(tracker.tracking, quad) do
      %{tracker | tracking: Map.delete(tracker.tracking, quad), count: tracker.count - 1}
    else
      tracker
    end
  end

  @doc """
  Finds all quads that depend on a specific graph.

  This is useful for determining which derived quads may be affected
  when a graph is modified.

  ## Parameters

  - `tracker` - The provenance tracker
  - `graph_id` - The graph ID to check

  ## Returns

  List of quads that depend on the specified graph.
  """
  @spec find_dependent_quads(t(), non_neg_integer()) :: [id_quad()]
  def find_dependent_quads(%__MODULE__{} = tracker, graph_id) do
    tracker.tracking
    |> Enum.filter(fn {_quad, sources} -> MapSet.member?(sources, graph_id) end)
    |> Enum.map(fn {quad, _sources} -> quad end)
  end

  @doc """
  Merges two provenance trackers.

  ## Parameters

  - `tracker1` - First tracker
  - `tracker2` - Second tracker

  ## Returns

  A new tracker combining both inputs. When both trackers have provenance
  for the same quad, the source graph sets are unioned.
  """
  @spec merge(t(), t()) :: t()
  def merge(%__MODULE__{} = tracker1, %__MODULE__{} = tracker2) do
    merged_tracking = Map.merge(tracker1.tracking, tracker2.tracking, fn _k, v1, v2 -> MapSet.union(v1, v2) end)

    %__MODULE__{
      tracking: merged_tracking,
      count: map_size(merged_tracking)
    }
  end

  @doc """
  Returns the number of quads being tracked.
  """
  @spec count(t()) :: non_neg_integer()
  def count(%__MODULE__{} = tracker), do: tracker.count

  @doc """
  Checks if the tracker is empty.
  """
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{} = tracker), do: tracker.count == 0

  @doc """
  Clears all provenance tracking.
  """
  @spec clear(t()) :: t()
  def clear(%__MODULE__{} = tracker) do
    %{tracker | tracking: %{}, count: 0}
  end

  # ============================================================================
  # Database Integration
  # ============================================================================

  @doc """
  Builds provenance information for derived quads in a graph.

  This function analyzes the derivation rules and facts to determine
  which graphs contributed to each derived quad.

  ## Parameters

  - `db` - Database reference
  - `graph_id` - Graph to analyze
  - `tbox_graph_id` - TBox graph (if shared)
  - `rules` - Reasoning rules used
  - `derived_quads` - List of derived quads to analyze

  ## Returns

  A provenance tracker populated with dependency information.
  """
  @spec build_provenance(
          term(),
          non_neg_integer(),
          non_neg_integer() | nil,
          [term()],
          [id_quad()]
        ) :: t()
  def build_provenance(db, graph_id, tbox_graph_id, _rules, derived_quads) do
    # For each derived quad, determine its source graphs
    # This is a simplified version that assumes:
    # - Derived quads in graph_id depend on graph_id
    # - If tbox_graph_id is set, they also depend on it

    tracker = new()

    source_graphs =
      if tbox_graph_id != nil do
        MapSet.new([graph_id, tbox_graph_id])
      else
        MapSet.new([graph_id])
      end

    # Add source graph info for each derived quad
    Enum.reduce(derived_quads, tracker, fn quad, acc ->
      add_source(acc, quad, MapSet.to_list(source_graphs))
    end)
  end

  @doc """
  Detects cross-graph dependencies in a set of derived quads.

  ## Parameters

  - `tracker` - Provenance tracker
  - `target_graph` - The graph to check

  ## Returns

  Map of quad to list of external source graphs.
  """
  @spec detect_cross_graph_deps(t(), non_neg_integer()) :: %{id_quad() => [non_neg_integer()]}
  def detect_cross_graph_deps(%__MODULE__{} = tracker, target_graph) do
    tracker.tracking
    |> Enum.filter(fn {_quad, sources} ->
      # Check if there are sources other than target_graph
      sources
      |> MapSet.delete(target_graph)
      |> MapSet.size() > 0
    end)
    |> Enum.map(fn {quad, sources} ->
      external_sources =
        sources
        |> MapSet.delete(target_graph)
        |> MapSet.to_list()
        |> Enum.sort()

      {quad, external_sources}
    end)
    |> Map.new()
  end
end
