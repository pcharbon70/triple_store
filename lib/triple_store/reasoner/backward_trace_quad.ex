defmodule TripleStore.Reasoner.BackwardTraceQuad do
  @moduledoc """
  Backward phase of incremental reasoning deletion for quads.

  When an explicit quad is deleted, this module determines which derived quads
  may have depended on it. It implements the backward tracing phase of the
  Backward/Forward algorithm.

  ## Graph-Scoped Backward Tracing

  The module handles graph-scoped reasoning:
  - Traces dependencies within the target graph
  - Optionally handles cross-graph dependencies when global reasoning is enabled
  - Tracks source graphs for multi-premise derivations

  ## Algorithm

  For each deleted quad:
  1. Find all rules where the quad matches a body pattern
  2. For each matching rule, find all derived quads that were derived using it
  3. Collect all potentially affected derived quads for the forward phase

  ## Usage

      {:ok, affected_quads} = BackwardTraceQuad.trace_affected_quads(
        db,
        deleted_quads,
        rules,
        graph_id: 1,
        scope: :local
      )
  """

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.QuadIndex
  alias TripleStore.Reasoner.DerivedStore
  alias TripleStore.Reasoner.Rule

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Database reference"
  @type db_ref :: NIF.db_ref()

  @typedoc "ID quad: {graph_id, subject_id, predicate_id, object_id}"
  @type id_quad :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "ID triple: {subject_id, predicate_id, object_id}"
  @type id_triple :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "Set of derived quads that may be affected by deletion"
  @type affected_quads :: MapSet.t(id_quad())

  @typedoc "Options for backward tracing"
  @type trace_opts :: [
          graph_id: non_neg_integer(),
          tbox_graph_id: non_neg_integer() | nil,
          scope: :local | :global
        ]

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Traces all derived quads that may be affected by deleting the given quads.

  This function performs the backward phase of the Backward/Forward algorithm.
  It returns the set of derived quads that:
  1. Were derived using rules that may have used the deleted quads as premises
  2. Exist in the derived column family for the target graph

  ## Parameters

  - `db` - Database reference
  - `deleted_quads` - List of `{graph, subject, predicate, object}` tuples that were deleted
  - `rules` - List of reasoning rules
  - `opts` - Options for tracing (see `trace_opts/0`)

  ## Returns

  - `{:ok, affected_quads}` - Set of derived quads that may need re-derivation
  - `{:error, reason}` - On failure

  ## Examples

      deleted = [{1, alice_id, rdf_type, student_id}]
      {:ok, affected} = BackwardTraceQuad.trace_affected_quads(
        db, deleted, rules,
        graph_id: 1,
        scope: :local
      )
  """
  @spec trace_affected_quads(db_ref(), [id_quad()], [Rule.t()], trace_opts()) ::
          {:ok, affected_quads()} | {:error, term()}
  def trace_affected_quads(db, deleted_quads, rules, opts) when is_list(deleted_quads) do
    graph_id = Keyword.fetch!(opts, :graph_id)
    tbox_graph_id = Keyword.get(opts, :tbox_graph_id)
    scope = Keyword.get(opts, :scope, :local)

    # Collect affected quads from all deleted quads
    affected_quads =
      Enum.reduce(deleted_quads, MapSet.new(), fn deleted_quad, acc ->
        case trace_single_deletion(db, deleted_quad, rules, graph_id, tbox_graph_id, scope) do
          {:ok, quads} -> MapSet.union(acc, quads)
          {:error, _} -> acc
        end
      end)

    {:ok, affected_quads}
  end

  @doc """
  Finds all rules that could have derived a given derived quad.

  This is the reverse lookup: given a derived quad, find all rules that
  could have produced it.

  ## Parameters

  - `derived_quad` - The derived quad to check
  - `rules` - List of reasoning rules

  ## Returns

  - List of rules that could derive this quad
  """
  @spec find_deriving_rules(id_quad(), [Rule.t()]) :: [Rule.t()]
  def find_deriving_rules({_g, s, p, o} = _derived_quad, rules) do
    Enum.filter(rules, fn rule ->
      Rule.could_derive?(rule, {s, p, o})
    end)
  end

  @doc """
  Checks if a deleted quad could satisfy a rule's body pattern.

  This determines whether the deleted quad could have been used as a premise
  to derive something using the given rule.

  ## Parameters

  - `deleted_quad` - The quad that was deleted
  - `rule` - The reasoning rule to check

  ## Returns

  - `true` if the quad could match any body pattern
  - `false` otherwise
  """
  @spec could_satisfy_rule?(id_quad(), Rule.t()) :: boolean()
  def could_satisfy_rule?({_g, s, p, o}, rule) do
    Rule.body_patterns(rule)
    |> Enum.any?(fn pattern ->
      matches_pattern?(pattern, {s, p, o})
    end)
  end

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  defp trace_single_deletion(db, {g, s, p, o}, rules, graph_id, _tbox_graph_id, :local) do
    # For graph-local reasoning, only consider rules and derivations within this graph
    deleted_triple = {s, p, o}

    # Find all rules that could have used this triple as a premise
    relevant_rules = Enum.filter(rules, &could_satisfy_rule?({g, s, p, o}, &1))

    # For each relevant rule, find derived quads in this graph
    affected_quads =
      Enum.reduce(relevant_rules, MapSet.new(), fn rule, acc ->
        case find_rule_derivations_in_graph(db, rule, graph_id, deleted_triple) do
          {:ok, quads} -> MapSet.union(acc, quads)
          {:error, _} -> acc
        end
      end)

    {:ok, affected_quads}
  end

  defp trace_single_deletion(db, {g, s, p, o}, rules, graph_id, tbox_graph_id, :global) do
    # For global reasoning, we need to consider cross-graph dependencies
    # This includes checking derived quads that may have premises from multiple graphs
    deleted_triple = {s, p, o}

    # Get all graphs that could be affected
    affected_graphs =
      if tbox_graph_id != nil && g == tbox_graph_id do
        # TBox deletion affects all graphs
        get_all_graphs_with_derivations(db)
      else
        # Regular graph deletion - may still affect other graphs if global reasoning
        MapSet.new([graph_id])
      end

    # Find all rules that could have used this triple
    relevant_rules = Enum.filter(rules, &could_satisfy_rule?({g, s, p, o}, &1))

    # Check derivations across all affected graphs
    affected_quads =
      Enum.reduce(relevant_rules, MapSet.new(), fn rule, acc ->
        Enum.reduce(affected_graphs, acc, fn affected_graph_id, inner_acc ->
          case find_rule_derivations_in_graph(db, rule, affected_graph_id, deleted_triple) do
            {:ok, quads} -> MapSet.union(inner_acc, quads)
            {:error, _} -> inner_acc
          end
        end)
      end)

    {:ok, affected_quads}
  end

  defp find_rule_derivations_in_graph(db, rule, graph_id, deleted_triple) do
    # Get all derived quads for this graph
    case DerivedStore.lookup_derived_quads_in_graph(db, graph_id) do
      {:ok, derived_quads} ->
        # Filter to those that could have come from this rule
        # and involve the deleted triple as a potential premise
        relevant =
          Enum.filter(derived_quads, fn {dg, ds, dp, dobj} ->
            dg == graph_id and could_rule_derive_with_premise?(rule, {ds, dp, dobj}, deleted_triple)
          end)

        {:ok, MapSet.new(relevant)}

      {:error, _reason} = error ->
        error
    end
  end

  defp could_rule_derive_with_premise?(rule, derived_triple, premise_triple) do
    # Check if:
    # 1. The rule could derive this triple
    # 2. The premise triple could be part of the rule's body

    Rule.could_derive?(rule, derived_triple) and
      Rule.body_patterns(rule)
      |> Enum.any?(fn pattern -> matches_pattern?(pattern, premise_triple) end)
  end

  defp matches_pattern?({:pattern, [s_pat, p_pat, o_pat]}, {s, p, o}) do
    matches_term_pattern?(s_pat, s) and
      matches_term_pattern?(p_pat, p) and matches_term_pattern?(o_pat, o)
  end

  defp matches_term_pattern?({:const, value}, term), do: value == term
  defp matches_term_pattern?({:var, _name}, _term), do: true
  defp matches_term_pattern?(:var, _term), do: true
  # Raw IRI and literal terms are treated as constants
  defp matches_term_pattern?({:iri, _} = iri, term), do: iri == term
  defp matches_term_pattern?({:literal, _, _} = literal, term), do: literal == term
  defp matches_term_pattern?(_other, _term), do: false

  defp get_all_graphs_with_derivations(db) do
    # Scan the derived column family for all graph IDs
    case NIF.fold_keys(db, :derived, <<>>, MapSet.new(), fn key, acc ->
      case DerivedStore.decode_derived_key(key) do
        {:ok, {g, _s, _p, _o}} -> MapSet.put(acc, g)
        _ -> acc
      end
    end) do
      {:ok, graphs} -> graphs
      _error -> MapSet.new()
    end
  end
end
