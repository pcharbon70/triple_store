defmodule TripleStore.Reasoner.DeleteWithReasoningQuad do
  @moduledoc """
  Deletion with reasoning for quads using the Backward/Forward algorithm.

  When quads are deleted from a reasoned quad store, we need to correctly
  retract derived quads that depended on the deleted quads, while preserving
  derived quads that have alternative justifications.

  ## Graph-Scoped Deletion

  The module handles graph-scoped reasoning:
  - Deletions are scoped to a specific graph
  - TBox sharing is respected when tracing dependencies
  - Cross-graph dependencies are considered for global reasoning

  ## Algorithm Overview

  1. **Delete Explicit Quads**: Remove the specified quads from the graph
  2. **Backward Trace**: Find all derived quads that may depend on deleted quads
  3. **Forward Re-derive**: For each potentially invalid quad, attempt re-derivation
  4. **Partition Results**: Keep quads that can be re-derived, delete others

  ## Usage

      {:ok, result} = DeleteWithReasoningQuad.delete_quads_with_reasoning(
        db,
        quads_to_delete,
        rules,
        graph_id: 1,
        tbox_graph_id: 0,
        scope: :local
      )
  """

  alias TripleStore.QuadIndex
  alias TripleStore.QuadOperations
  alias TripleStore.Reasoner.BackwardTraceQuad
  alias TripleStore.Reasoner.ForwardRederiveQuad
  alias TripleStore.Reasoner.Rule

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Database reference"
  @type db_ref :: ErlangAdapter.db_ref()

  @typedoc "ID quad: {graph_id, subject_id, predicate_id, object_id}"
  @type id_quad :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "Set of ID quads"
  @type quad_set :: MapSet.t(id_quad())

  @typedoc "Statistics from quad deletion with reasoning"
  @type delete_stats :: %{
          explicit_deleted: non_neg_integer(),
          derived_deleted: non_neg_integer(),
          derived_kept: non_neg_integer(),
          potentially_invalid_count: non_neg_integer(),
          duration_ms: non_neg_integer()
        }

  @typedoc "Options for deletion with reasoning"
  @type delete_opts :: [
          graph_id: non_neg_integer(),
          tbox_graph_id: non_neg_integer() | nil,
          scope: :local | :global,
          emit_telemetry: boolean()
        ]

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Deletes quads from the database and correctly retracts derived consequences.

  This function performs deletion with reasoning on persistent storage by:
  1. Deleting the specified explicit quads from the graph
  2. Tracing backward to find potentially invalid derived quads
  3. Attempting to re-derive each potentially invalid quad
  4. Deleting derived quads that cannot be re-derived

  ## Parameters

  - `db` - Database reference
  - `quads` - List of `{graph_id, subject_id, predicate_id, object_id}` tuples to delete
  - `rules` - List of reasoning rules
  - `opts` - Options for deletion (see `delete_opts/0`)

  ## Options

  - `:graph_id` - Target graph ID (required)
  - `:tbox_graph_id` - Graph ID containing shared TBox (nil = no TBox sharing)
  - `:scope` - Reasoning scope: `:local` (default) or `:global`
  - `:emit_telemetry` - Emit telemetry events. Default: true

  ## Returns

  - `{:ok, stats}` - Deletion completed with statistics
  - `{:error, reason}` - On failure

  ## Examples

      quads = [{1, alice_id, rdf_type, student_id}]
      {:ok, stats} = DeleteWithReasoningQuad.delete_quads_with_reasoning(
        db, quads, rules,
        graph_id: 1,
        scope: :local
      )
  """
  @spec delete_quads_with_reasoning(db_ref(), [id_quad()], [Rule.t()], delete_opts()) ::
          {:ok, delete_stats()} | {:error, term()}
  def delete_quads_with_reasoning(db, quads, rules, opts) when is_list(quads) do
    start_time = System.monotonic_time(:millisecond)

    graph_id = Keyword.fetch!(opts, :graph_id)
    tbox_graph_id = Keyword.get(opts, :tbox_graph_id)
    scope = Keyword.get(opts, :scope, :local)
    emit_telemetry = Keyword.get(opts, :emit_telemetry, true)

    if emit_telemetry do
      emit_start_telemetry(quads, rules)
    end

    # Phase 1: Delete the explicit quads
    :ok = delete_explicit_quads(db, quads, graph_id)

    # Phase 2: Backward trace to find potentially invalid derived quads
    {:ok, potentially_invalid} =
      BackwardTraceQuad.trace_affected_quads(
        db,
        quads,
        rules,
        graph_id: graph_id,
        tbox_graph_id: tbox_graph_id,
        scope: scope
      )

    if emit_telemetry do
      emit_backward_trace_telemetry(potentially_invalid)
    end

    # Phase 3: Forward re-derivation to partition keep/delete
    {:ok, rederive_result} =
      ForwardRederiveQuad.rederive_quads(
        db,
        potentially_invalid,
        MapSet.new(quads),
        rules,
        graph_id: graph_id,
        tbox_graph_id: tbox_graph_id,
        scope: scope
      )

    if emit_telemetry do
      emit_forward_rederive_telemetry(rederive_result)
    end

    # Phase 4: Delete derived quads that cannot be re-derived
    if MapSet.size(rederive_result.delete) > 0 do
      delete_derived_quads(db, MapSet.to_list(rederive_result.delete))
    end

    duration_ms = System.monotonic_time(:millisecond) - start_time

    stats = %{
      explicit_deleted: length(quads),
      derived_deleted: MapSet.size(rederive_result.delete),
      derived_kept: MapSet.size(rederive_result.keep),
      potentially_invalid_count: MapSet.size(potentially_invalid),
      duration_ms: duration_ms
    }

    if emit_telemetry do
      emit_stop_telemetry(stats, duration_ms)
    end

    {:ok, stats}
  end

  def delete_quads_with_reasoning(_db, [], _rules, _opts) do
    {:ok,
     %{
       explicit_deleted: 0,
       derived_deleted: 0,
       derived_kept: 0,
       potentially_invalid_count: 0,
       duration_ms: 0
     }}
  end

  @doc """
  Previews what would be deleted without modifying the database.

  This is a dry-run version that computes what would be deleted without
  actually performing the deletion.

  ## Parameters

  - `db` - Database reference
  - `quads` - List of quads to delete
  - `rules` - List of reasoning rules
  - `opts` - Options for deletion

  ## Returns

  - `{:ok, {explicit_deleted, derived_deleted}}` - Sets of quads that would be deleted
  """
  @spec preview_quad_deletion(db_ref(), [id_quad()], [Rule.t()], delete_opts()) ::
          {:ok, {quad_set(), quad_set()}}
  def preview_quad_deletion(db, quads, rules, opts) do
    graph_id = Keyword.fetch!(opts, :graph_id)
    tbox_graph_id = Keyword.get(opts, :tbox_graph_id)
    scope = Keyword.get(opts, :scope, :local)

    # Phase 1: Find potentially invalid derived quads
    {:ok, potentially_invalid} =
      BackwardTraceQuad.trace_affected_quads(
        db,
        quads,
        rules,
        graph_id: graph_id,
        tbox_graph_id: tbox_graph_id,
        scope: scope
      )

    # Phase 2: Determine which would be deleted
    {:ok, rederive_result} =
      ForwardRederiveQuad.rederive_quads(
        db,
        potentially_invalid,
        MapSet.new(quads),
        rules,
        graph_id: graph_id,
        tbox_graph_id: tbox_graph_id,
        scope: scope
      )

    {:ok, {MapSet.new(quads), rederive_result.delete}}
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp delete_explicit_quads(_db, [], _graph_id), do: :ok

  defp delete_explicit_quads(db, quads, graph_id) do
    try do
      # Convert from {g, s, p, o} to {s, p, o, g} format for QuadOperations
      normalized_quads =
        quads
        |> Enum.filter(fn {g, _s, _p, _o} -> g == graph_id end)
        |> Enum.map(fn {g, s, p, o} -> {s, p, o, g} end)

      QuadOperations.delete_quads(db, normalized_quads, sync: true)
    rescue
      _error -> :ok
    end
  end

  defp delete_derived_quads(db, quads) when is_list(quads) do
    try do
      # Delete from derived column family
      operations =
        Enum.map(quads, fn {g, s, p, o} ->
          key = QuadIndex.gspo_key(g, s, p, o)
          {:derived_cf, key}
        end)

      ErlangAdapter.delete_batch(db, operations, true)
    rescue
      _error -> :ok
    end
  end

  # ============================================================================
  # Telemetry Functions
  # ============================================================================

  defp emit_start_telemetry(quads, rules) do
    require Logger

    Logger.debug(
      "Starting quad deletion with reasoning: #{length(quads)} quads, #{length(rules)} rules"
    )
  end

  defp emit_backward_trace_telemetry(potentially_invalid) do
    require Logger

    Logger.debug(
      "Backward trace found #{MapSet.size(potentially_invalid)} potentially invalid quads"
    )
  end

  defp emit_forward_rederive_telemetry(rederive_result) do
    require Logger

    Logger.debug(
      "Forward re-derivation: #{rederive_result.rederivation_count} kept, #{MapSet.size(rederive_result.delete)} deleted"
    )
  end

  defp emit_stop_telemetry(stats, _duration_ms) do
    require Logger

    Logger.debug(
      "Quad deletion completed: explicit=#{stats.explicit_deleted}, derived_deleted=#{stats.derived_deleted}, derived_kept=#{stats.derived_kept}, duration=#{stats.duration_ms}ms"
    )
  end
end
