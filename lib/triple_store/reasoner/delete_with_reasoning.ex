defmodule TripleStore.Reasoner.DeleteWithReasoning do
  @moduledoc """
  Deletion with reasoning using the Backward/Forward algorithm.

  When facts are deleted from a reasoned triple store, we need to correctly
  retract derived facts that depended on the deleted facts, while preserving
  derived facts that have alternative justifications.

  ## Algorithm Overview

  The Backward/Forward algorithm works in two phases:

  1. **Backward Phase**: Starting from deleted facts, trace all derived facts
     that may have depended on them (either directly or transitively).

  2. **Forward Phase**: For each potentially invalid fact, attempt to re-derive
     it using alternative justifications. Keep facts that can be re-derived,
     delete those that cannot.

  ## Why This Matters

  Consider deleting `alice rdf:type Student` when:
  - `Student rdfs:subClassOf Person` exists
  - `GradStudent rdfs:subClassOf Person` exists
  - `alice rdf:type GradStudent` exists

  The derived fact `alice rdf:type Person` was originally derived from the
  Student type. A naive deletion would remove it. But this fact can be
  re-derived via GradStudent, so it should be kept.

  ## In-Memory API

  This module provides an in-memory API for testing:

      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        deleted_facts,
        all_facts,
        derived_facts,
        rules
      )
      # result.explicit_deleted - explicit facts deleted
      # result.derived_deleted - derived facts deleted
      # result.derived_kept - derived facts kept via re-derivation
      # result.final_facts - remaining facts after deletion

  ## Database API

  For production use with persistent storage:

      {:ok, result} = DeleteWithReasoning.delete_with_reasoning(
        db,
        triples_to_delete,
        rules
      )
  """

  alias TripleStore.Reasoner.BackwardTrace
  alias TripleStore.Reasoner.ForwardRederive
  alias TripleStore.Reasoner.Rule
  alias TripleStore.Reasoner.Telemetry

  # Database-related imports for the database API
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Index
  alias TripleStore.Reasoner.DerivedStore

  require Logger

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A triple as RDF terms"
  @type term_triple :: {Rule.rule_term(), Rule.rule_term(), Rule.rule_term()}

  @typedoc "A set of triples"
  @type fact_set :: MapSet.t(term_triple())

  @typedoc "Database reference"
  @type db_ref :: NIF.db_ref()

  @typedoc "A triple as dictionary-encoded IDs"
  @type id_triple :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "Statistics from deletion with reasoning"
  @type delete_stats :: %{
          explicit_deleted: non_neg_integer(),
          derived_deleted: non_neg_integer(),
          derived_kept: non_neg_integer(),
          potentially_invalid_count: non_neg_integer(),
          duration_ms: non_neg_integer()
        }

  @typedoc "Result of in-memory deletion"
  @type delete_result :: %{
          explicit_deleted: fact_set(),
          derived_deleted: fact_set(),
          derived_kept: fact_set(),
          final_facts: fact_set(),
          stats: delete_stats()
        }

  @typedoc "Options for deletion with reasoning"
  @type delete_opts :: [
          max_trace_depth: non_neg_integer(),
          emit_telemetry: boolean()
        ]

  # ============================================================================
  # Configuration
  # ============================================================================

  @default_max_trace_depth 100

  # ============================================================================
  # In-Memory API
  # ============================================================================

  @doc """
  Deletes facts and correctly retracts derived consequences using reasoning.

  This function implements the full Backward/Forward deletion algorithm:
  1. Remove the specified facts from the fact set
  2. Trace backward to find all potentially invalid derived facts
  3. Attempt to re-derive each potentially invalid fact
  4. Keep facts that can be re-derived, delete those that cannot

  ## Parameters

  - `deleted_triples` - List of facts to delete
  - `all_facts` - Set of all facts (explicit + derived)
  - `derived_facts` - Set of all derived facts (subset of all_facts)
  - `rules` - List of reasoning rules
  - `opts` - Options (see below)

  ## Options

  - `:max_trace_depth` - Maximum depth for backward tracing. Default: 100

  ## Returns

  - `{:ok, result}` - Deletion completed
    - `result.explicit_deleted` - Explicit facts that were deleted
    - `result.derived_deleted` - Derived facts that were deleted
    - `result.derived_kept` - Derived facts kept via re-derivation
    - `result.final_facts` - All facts remaining after deletion
    - `result.stats` - Statistics about the deletion

  ## Examples

      deleted = [{iri("alice"), rdf_type(), iri("Student")}]
      all_facts = MapSet.new([...])
      derived = MapSet.new([{iri("alice"), rdf_type(), iri("Person")}])
      rules = [Rules.cax_sco()]

      {:ok, result} = DeleteWithReasoning.delete_in_memory(
        deleted, all_facts, derived, rules
      )
  """
  @spec delete_in_memory([term_triple()], fact_set(), fact_set(), [Rule.t()], delete_opts()) ::
          {:ok, delete_result()}
  def delete_in_memory(deleted_triples, all_facts, derived_facts, rules, opts \\ []) do
    start_time = System.monotonic_time()
    emit_telemetry = Keyword.get(opts, :emit_telemetry, true)
    max_depth = Keyword.get(opts, :max_trace_depth, @default_max_trace_depth)

    deleted_set = MapSet.new(deleted_triples)

    # Emit start telemetry
    if emit_telemetry do
      Telemetry.emit_start([:triple_store, :reasoner, :delete], %{
        triple_count: length(deleted_triples),
        rule_count: length(rules)
      })
    end

    # Partition deleted facts into explicit and derived
    explicit_deleted = MapSet.difference(deleted_set, derived_facts)
    derived_in_deleted = MapSet.intersection(deleted_set, derived_facts)

    # Phase 1: Remove the deleted facts from the fact set
    facts_after_explicit_delete = MapSet.difference(all_facts, deleted_set)
    derived_after_explicit_delete = MapSet.difference(derived_facts, deleted_set)

    # Phase 2: Backward trace to find potentially invalid derived facts
    {:ok, trace_result} = BackwardTrace.trace_in_memory(
      deleted_set,
      derived_after_explicit_delete,
      rules,
      max_depth: max_depth,
      include_deleted: false
    )

    # Emit backward trace telemetry
    if emit_telemetry do
      Telemetry.emit_backward_trace(%{
        trace_depth: trace_result.trace_depth,
        facts_examined: trace_result.facts_examined,
        potentially_invalid_count: MapSet.size(trace_result.potentially_invalid)
      })
    end

    potentially_invalid = trace_result.potentially_invalid

    # Phase 3: Forward re-derivation to partition keep/delete
    {:ok, rederive_result} = ForwardRederive.rederive_in_memory(
      potentially_invalid,
      facts_after_explicit_delete,
      deleted_set,
      rules
    )

    # Emit forward re-derivation telemetry
    if emit_telemetry do
      Telemetry.emit_forward_rederive(%{
        facts_checked: rederive_result.facts_checked,
        rederivation_count: rederive_result.rederivation_count,
        deleted_count: MapSet.size(rederive_result.delete)
      })
    end

    derived_kept = rederive_result.keep
    derived_deleted = MapSet.union(derived_in_deleted, rederive_result.delete)

    # Phase 4: Compute final facts
    final_facts = MapSet.difference(facts_after_explicit_delete, rederive_result.delete)

    duration = System.monotonic_time() - start_time
    duration_ms = System.convert_time_unit(duration, :native, :millisecond)

    stats = %{
      explicit_deleted: MapSet.size(explicit_deleted),
      derived_deleted: MapSet.size(derived_deleted),
      derived_kept: MapSet.size(derived_kept),
      potentially_invalid_count: MapSet.size(potentially_invalid),
      duration_ms: duration_ms
    }

    # Emit stop telemetry
    if emit_telemetry do
      Telemetry.emit_stop([:triple_store, :reasoner, :delete], duration, stats)
    end

    {:ok, %{
      explicit_deleted: explicit_deleted,
      derived_deleted: derived_deleted,
      derived_kept: derived_kept,
      final_facts: final_facts,
      stats: stats
    }}
  end

  @doc """
  Previews what would be deleted without modifying the fact set.

  This is a dry-run version of `delete_in_memory/5` that computes what
  would be deleted without actually performing the deletion.

  ## Parameters

  - `deleted_triples` - List of facts to delete
  - `all_facts` - Set of all facts
  - `derived_facts` - Set of derived facts
  - `rules` - List of reasoning rules

  ## Returns

  - `{:ok, {explicit_deleted, derived_deleted}}` - Sets of facts that would be deleted
  """
  @spec preview_delete_in_memory([term_triple()], fact_set(), fact_set(), [Rule.t()]) ::
          {:ok, {fact_set(), fact_set()}}
  def preview_delete_in_memory(deleted_triples, all_facts, derived_facts, rules) do
    {:ok, result} = delete_in_memory(deleted_triples, all_facts, derived_facts, rules)
    {:ok, {result.explicit_deleted, result.derived_deleted}}
  end

  # ============================================================================
  # Database API
  # ============================================================================

  @doc """
  Deletes facts from the database and correctly retracts derived consequences.

  This function performs deletion with reasoning on persistent storage by:
  1. Deleting the specified explicit facts from the database
  2. Tracing backward to find potentially invalid derived facts
  3. Attempting to re-derive each potentially invalid fact
  4. Deleting derived facts that cannot be re-derived

  ## Parameters

  - `db` - Database reference
  - `triples` - List of `{subject_id, predicate_id, object_id}` tuples to delete
  - `rules` - List of reasoning rules
  - `opts` - Options (see `delete_opts()`)

  ## Options

  - `:max_trace_depth` - Maximum depth for backward tracing. Default: 100
  - `:emit_telemetry` - Emit telemetry events. Default: true

  ## Returns

  - `{:ok, stats}` - Deletion completed with statistics
  - `{:error, reason}` - On failure

  ## Note

  This function requires the rules to work with dictionary-encoded ID triples.
  """
  @spec delete_with_reasoning(db_ref(), [id_triple()], [Rule.t()], delete_opts()) ::
          {:ok, delete_stats()} | {:error, term()}
  def delete_with_reasoning(db, triples, rules, opts \\ [])

  def delete_with_reasoning(_db, [], _rules, _opts) do
    {:ok, %{
      explicit_deleted: 0,
      derived_deleted: 0,
      derived_kept: 0,
      potentially_invalid_count: 0,
      duration_ms: 0
    }}
  end

  def delete_with_reasoning(db, triples, rules, opts) when is_list(triples) do
    start_time = System.monotonic_time(:millisecond)
    max_depth = Keyword.get(opts, :max_trace_depth, @default_max_trace_depth)

    # Partition triples into those that exist as explicit vs derived
    with {:ok, explicit_triples, derived_triples} <- partition_by_source(db, triples),
         # Delete explicit facts from the main index
         :ok <- delete_explicit_facts(db, explicit_triples),
         # Get all derived facts for backward tracing
         {:ok, all_derived} <- get_all_derived_facts(db),
         # Perform backward trace
         {:ok, trace_result} <- backward_trace_db(
           MapSet.new(triples),
           all_derived,
           rules,
           max_depth
         ),
         # Perform forward re-derivation
         {:ok, rederive_result} <- forward_rederive_db(
           db,
           trace_result.potentially_invalid,
           MapSet.new(triples),
           rules
         ),
         # Delete derived facts that cannot be re-derived
         :ok <- DerivedStore.delete_derived(db, MapSet.to_list(rederive_result.delete)),
         # Also delete any derived facts that were explicitly requested for deletion
         :ok <- DerivedStore.delete_derived(db, derived_triples) do

      duration_ms = System.monotonic_time(:millisecond) - start_time

      {:ok, %{
        explicit_deleted: length(explicit_triples),
        derived_deleted: MapSet.size(rederive_result.delete) + length(derived_triples),
        derived_kept: MapSet.size(rederive_result.keep),
        potentially_invalid_count: MapSet.size(trace_result.potentially_invalid),
        duration_ms: duration_ms
      }}
    end
  end

  @doc """
  Performs bulk deletion with reasoning, optimized for large deletions.

  This function batches the deletion operations to reduce memory usage
  and improve performance for large datasets.

  ## Parameters

  - `db` - Database reference
  - `triples` - List of triples to delete
  - `rules` - List of reasoning rules
  - `opts` - Options including `:batch_size`

  ## Options

  - `:batch_size` - Number of triples to process per batch. Default: 1000
  - `:max_trace_depth` - Maximum depth for backward tracing. Default: 100

  ## Returns

  - `{:ok, stats}` - Aggregated statistics from all batches
  - `{:error, reason}` - On failure
  """
  @bulk_batch_size 1000

  @spec bulk_delete_with_reasoning(db_ref(), [id_triple()], [Rule.t()], keyword()) ::
          {:ok, delete_stats()} | {:error, term()}
  def bulk_delete_with_reasoning(db, triples, rules, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, @bulk_batch_size)
    start_time = System.monotonic_time(:millisecond)

    result =
      triples
      |> Enum.chunk_every(batch_size)
      |> Enum.reduce_while(
        {:ok, %{explicit_deleted: 0, derived_deleted: 0, derived_kept: 0, potentially_invalid_count: 0}},
        fn batch, {:ok, acc} ->
          case delete_with_reasoning(db, batch, rules, opts) do
            {:ok, batch_stats} ->
              new_acc = %{
                explicit_deleted: acc.explicit_deleted + batch_stats.explicit_deleted,
                derived_deleted: acc.derived_deleted + batch_stats.derived_deleted,
                derived_kept: acc.derived_kept + batch_stats.derived_kept,
                potentially_invalid_count: acc.potentially_invalid_count + batch_stats.potentially_invalid_count
              }
              {:cont, {:ok, new_acc}}

            {:error, _} = error ->
              {:halt, error}
          end
        end
      )

    case result do
      {:ok, stats} ->
        duration_ms = System.monotonic_time(:millisecond) - start_time
        {:ok, Map.put(stats, :duration_ms, duration_ms)}

      error ->
        error
    end
  end

  # ============================================================================
  # Private Functions - Database Operations
  # ============================================================================

  defp partition_by_source(db, triples) do
    {explicit, derived} =
      Enum.reduce(triples, {[], []}, fn triple, {exp_acc, der_acc} ->
        case Index.triple_exists?(db, triple) do
          {:ok, true} ->
            {[triple | exp_acc], der_acc}

          {:ok, false} ->
            case DerivedStore.derived_exists?(db, triple) do
              {:ok, true} -> {exp_acc, [triple | der_acc]}
              {:ok, false} -> {exp_acc, der_acc}
              {:error, reason} ->
                Logger.warning(
                  "Error checking derived store for triple #{inspect(triple)}: #{inspect(reason)}"
                )
                {exp_acc, der_acc}
            end

          {:error, reason} ->
            Logger.warning(
              "Error checking index for triple #{inspect(triple)}: #{inspect(reason)}"
            )
            {exp_acc, der_acc}
        end
      end)

    {:ok, Enum.reverse(explicit), Enum.reverse(derived)}
  end

  defp delete_explicit_facts(_db, []), do: :ok
  defp delete_explicit_facts(db, triples), do: Index.delete_triples(db, triples)

  defp get_all_derived_facts(db) do
    case DerivedStore.lookup_derived(db, {:var, :var, :var}) do
      {:ok, stream} -> {:ok, stream |> Enum.to_list() |> MapSet.new()}
      error -> error
    end
  end

  defp backward_trace_db(deleted_set, all_derived, rules, max_depth) do
    BackwardTrace.trace_in_memory(
      deleted_set,
      all_derived,
      rules,
      max_depth: max_depth,
      include_deleted: false
    )
  end

  defp forward_rederive_db(db, potentially_invalid, deleted_set, rules) do
    # Get all facts (explicit + derived) for re-derivation checks
    with {:ok, explicit_stream} <- Index.lookup(db, {:var, :var, :var}),
         {:ok, derived_stream} <- DerivedStore.lookup_derived(db, {:var, :var, :var}) do
      explicit_facts = explicit_stream |> Enum.to_list() |> MapSet.new()
      derived_facts = derived_stream |> Enum.to_list() |> MapSet.new()
      all_facts = MapSet.union(explicit_facts, derived_facts)

      ForwardRederive.rederive_in_memory(
        potentially_invalid,
        all_facts,
        deleted_set,
        rules
      )
    end
  end
end
