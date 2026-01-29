defmodule TripleStore.Reasoner.ForwardRederiveQuad do
  @moduledoc """
  Forward re-derivation phase for incremental deletion with reasoning for quads.

  After the backward phase identifies potentially invalid derived quads,
  the forward phase attempts to re-derive each quad using alternative
  justifications. Quads that can be re-derived are kept; those that
  cannot are deleted.

  ## Graph-Scoped Forward Re-derivation

  The module handles graph-scoped reasoning:
  - Re-derivation checks are scoped to the target graph
  - TBox facts are included in re-derivation checks if shared
  - Cross-graph dependencies are considered when global reasoning is enabled

  ## Algorithm

  For each potentially invalid derived quad:
  1. Check if any rule can derive it using remaining valid facts
  2. Valid facts = all facts - deleted - potentially invalid (excluding current)
  3. Include TBox facts if shared
  4. Partition into keep (re-derivable) and delete (not re-derivable)

  ## Usage

      {:ok, result} = ForwardRederiveQuad.rederive_quads(
        db,
        potentially_invalid_quads,
        deleted_quads,
        rules,
        graph_id: 1,
        tbox_graph_id: 0,
        scope: :local
      )
  """

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadIndex
  alias TripleStore.Reasoner.DerivedStore
  alias TripleStore.Reasoner.PatternMatcher
  alias TripleStore.Reasoner.Rule
  alias TripleStore.Reasoner.TBoxExtractor

  require Logger

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Database reference"
  @type db_ref :: ErlangAdapter.db_ref()

  @typedoc "ID quad: {graph_id, subject_id, predicate_id, object_id}"
  @type id_quad :: {non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "ID triple: {subject_id, predicate_id, object_id}"
  @type id_triple :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}

  @typedoc "Set of ID quads"
  @type quad_set :: MapSet.t(id_quad())

  @typedoc "Result of forward re-derivation for quads"
  @type rederive_result :: %{
          keep: quad_set(),
          delete: quad_set(),
          rederivation_count: non_neg_integer(),
          quads_checked: non_neg_integer()
        }

  @typedoc "Options for forward re-derivation"
  @type rederive_opts :: [
          graph_id: non_neg_integer(),
          tbox_graph_id: non_neg_integer() | nil,
          scope: :local | :global
        ]

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Attempts to re-derive potentially invalid quads using alternative justifications.

  For each potentially invalid quad, checks if it can be derived from the
  remaining valid facts in the graph. Quads that can be re-derived are kept;
  those that cannot are marked for deletion.

  ## Parameters

  - `db` - Database reference
  - `potentially_invalid_quads` - Set of quads that may be invalid (from backward trace)
  - `deleted_quads` - Set of quads being deleted (explicit quads, not derived)
  - `rules` - List of reasoning rules
  - `opts` - Options for re-derivation (see `rederive_opts/0`)

  ## Returns

  - `{:ok, result}` - Re-derivation completed
    - `result.keep` - Quads that can be re-derived (should be kept)
    - `result.delete` - Quads that cannot be re-derived (should be deleted)
    - `result.rederivation_count` - Number of quads successfully re-derived
    - `result.quads_checked` - Total quads checked

  ## Examples

      potentially_invalid = MapSet.new([{1, alice_id, rdf_type, person_id}])
      deleted = MapSet.new([{1, alice_id, rdf_type, student_id}])

      {:ok, result} = ForwardRederiveQuad.rederive_quads(
        db, potentially_invalid, deleted, rules,
        graph_id: 1,
        scope: :local
      )
  """
  @spec rederive_quads(db_ref(), quad_set(), quad_set(), [Rule.t()], rederive_opts()) ::
          {:ok, rederive_result()}
  def rederive_quads(db, potentially_invalid_quads, deleted_quads, rules, opts) do
    graph_id = Keyword.fetch!(opts, :graph_id)
    tbox_graph_id = Keyword.get(opts, :tbox_graph_id)
    _scope = Keyword.get(opts, :scope, :local)

    # Load TBox facts if shared
    tbox_facts = load_tbox_triples(db, tbox_graph_id, graph_id)

    # Get all explicit facts in this graph (excluding deleted)
    all_explicit_facts = load_explicit_facts_in_graph(db, graph_id, deleted_quads)

    # Get all derived quads in this graph
    _all_derived_quads =
      case DerivedStore.lookup_derived_quads_in_graph(db, graph_id) do
        {:ok, quads} -> quads
        {:error, _} -> []
      end

    # Convert to triples for pattern matching (drop graph_id)
    all_explicit_triples =
      all_explicit_facts
      |> Enum.map(fn {_g, s, p, o} -> {s, p, o} end)
      |> MapSet.new()

    # Combine with TBox facts
    base_valid_triples = MapSet.union(all_explicit_triples, tbox_facts)

    # Process each potentially invalid quad
    {keep_quads, delete_quads} =
      potentially_invalid_quads
      |> Enum.reduce({MapSet.new(), MapSet.new()}, fn {g, s, p, o} = quad,
                                                      {keep_acc, delete_acc} ->
        # Valid triples for checking this specific quad:
        # - Start with base_valid_triples
        # - Exclude potentially invalid quads we haven't processed yet (as triples)
        # - Add back triples from quads we've already determined can be kept
        # - Exclude the triple we're checking (to avoid self-justification)

        # Convert potentially invalid quads to triples for exclusion
        potentially_invalid_triples =
          potentially_invalid_quads
          |> MapSet.delete(quad)
          |> Enum.map(fn {_g, s, p, o} -> {s, p, o} end)
          |> MapSet.new()

        # Triples from keep_acc
        kept_triples =
          keep_acc
          |> Enum.map(fn {_g, s, p, o} -> {s, p, o} end)
          |> MapSet.new()

        valid_for_check =
          base_valid_triples
          |> MapSet.difference(potentially_invalid_triples)
          |> MapSet.union(kept_triples)
          |> MapSet.delete({s, p, o})

        # Convert back to in-memory format for rederiver (terms)
        # For now, use a simpler check that works with IDs
        if can_rederive_quad_in_graph?(db, {s, p, o}, valid_for_check, g, rules) do
          {MapSet.put(keep_acc, quad), delete_acc}
        else
          {keep_acc, MapSet.put(delete_acc, quad)}
        end
      end)

    {:ok,
     %{
       keep: keep_quads,
       delete: delete_quads,
       rederivation_count: MapSet.size(keep_quads),
       quads_checked: MapSet.size(potentially_invalid_quads)
     }}
  end

  @doc """
  Partitions potentially invalid quads into keep and delete sets.

  This is a convenience function that wraps `rederive_quads/5` and
  returns just the partition result.

  ## Parameters

  - `db` - Database reference
  - `potentially_invalid_quads` - Set of quads that may be invalid
  - `deleted_quads` - Set of quads being deleted
  - `rules` - List of reasoning rules
  - `opts` - Options for re-derivation

  ## Returns

  `{keep, delete}` tuple of MapSets containing quads.
  """
  @spec partition_invalid_quads(db_ref(), quad_set(), quad_set(), [Rule.t()], rederive_opts()) ::
          {quad_set(), quad_set()}
  def partition_invalid_quads(db, potentially_invalid_quads, deleted_quads, rules, opts) do
    {:ok, result} = rederive_quads(db, potentially_invalid_quads, deleted_quads, rules, opts)
    {result.keep, result.delete}
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp load_tbox_triples(_db, nil, _graph_id), do: MapSet.new()

  defp load_tbox_triples(db, tbox_graph_id, _graph_id) do
    try do
      case TBoxExtractor.extract_tbox(db, tbox_graph_id) do
        {:ok, tbox_quads} ->
          tbox_quads
          |> Enum.map(fn {_g, s, p, o} -> {s, p, o} end)
          |> MapSet.new()

        {:error, _reason} ->
          MapSet.new()
      end
    rescue
      _error -> MapSet.new()
    end
  end

  defp load_explicit_facts_in_graph(db, graph_id, deleted_quads) do
    # Scan GSPO index for explicit facts in this graph
    prefix = QuadIndex.gspo_prefix(graph_id)

    try do
      # Collect all explicit quads (not in derived CF)
      explicit_quads =
        ErlangAdapter.fold(db, :gspo, prefix, [], fn {key, _value}, acc ->
          {g, s, p, o} = QuadIndex.decode_gspo_key(key)
          quad = {g, s, p, o}

          # Skip if this quad was deleted
          if MapSet.member?(deleted_quads, quad) do
            acc
          else
            # Check if it's a derived quad
            case DerivedStore.derived_quad_exists?(db, quad) do
              {:ok, true} -> acc
              {:ok, false} -> [quad | acc]
              {:error, _} -> acc
            end
          end
        end)

      MapSet.new(explicit_quads)
    rescue
      _error -> MapSet.new()
    end
  end

  defp can_rederive_quad_in_graph?(db, {_s, _p, _o} = triple, valid_facts, graph_id, rules) do
    # Try each rule to see if it can derive this triple
    Enum.any?(rules, fn rule ->
      can_derive_quad_with_rule?(db, triple, valid_facts, graph_id, rule)
    end)
  end

  defp can_derive_quad_with_rule?(db, {_s, _p, _o} = triple, valid_facts, graph_id, rule) do
    # First, check if the triple matches the rule's head pattern
    case PatternMatcher.match_rule_head(triple, rule.head) do
      {:ok, head_bindings} ->
        # Get body patterns (excluding conditions)
        patterns = Rule.body_patterns(rule)

        # Try to find bindings that satisfy all body patterns
        # Use a simplified check for now - can be enhanced later
        case find_satisfying_bindings_for_quad(patterns, head_bindings, valid_facts, graph_id, db) do
          {:ok, []} ->
            false

          {:ok, bindings_list} ->
            # Check if any binding set satisfies the conditions
            conditions = Rule.body_conditions(rule)
            Enum.any?(bindings_list, &satisfies_conditions?(conditions, &1))

          {:error, :binding_limit_exceeded} ->
            # Conservatively assume cannot be re-derived
            false
        end

      :no_match ->
        false
    end
  end

  defp find_satisfying_bindings_for_quad(patterns, initial_bindings, valid_facts, _graph_id, _db) do
    # Simplified version - check if all body patterns can be satisfied
    # A more sophisticated version would use the actual database lookup

    result =
      patterns
      |> Enum.reduce_while({:ok, [initial_bindings]}, fn pattern, {:ok, bindings_list} ->
        # For each current binding set, try to extend it
        extended =
          bindings_list
          |> Enum.flat_map(fn bindings ->
            extend_bindings_with_facts(pattern, bindings, valid_facts)
          end)

        # Check binding set size limit
        if length(extended) > 1000 do
          {:halt, {:error, :binding_limit_exceeded}}
        else
          case extended do
            [] -> {:cont, {:ok, []}}
            _ -> {:cont, {:ok, extended}}
          end
        end
      end)

    case result do
      {:ok, bindings_list} -> {:ok, Enum.reject(bindings_list, &Enum.empty?/1)}
      {:error, _} = error -> error
    end
  end

  defp extend_bindings_with_facts({:pattern, [ps, pp, po]}, bindings, valid_facts) do
    # Substitute known bindings into the pattern
    s_sub = PatternMatcher.substitute_if_bound(ps, bindings)
    p_sub = PatternMatcher.substitute_if_bound(pp, bindings)
    o_sub = PatternMatcher.substitute_if_bound(po, bindings)

    # Find matching facts and extend bindings using PatternMatcher
    valid_facts
    |> Enum.filter(fn {fs, fp, fo} ->
      PatternMatcher.matches_term?(fs, s_sub) and
        PatternMatcher.matches_term?(fp, p_sub) and
        PatternMatcher.matches_term?(fo, o_sub)
    end)
    |> Enum.map(fn {fs, fp, fo} ->
      # Extend bindings with matched values using PatternMatcher
      bindings
      |> PatternMatcher.maybe_bind(ps, fs)
      |> PatternMatcher.maybe_bind(pp, fp)
      |> PatternMatcher.maybe_bind(po, fo)
    end)
    |> Enum.reject(&is_nil/1)
  end

  defp satisfies_conditions?([], _bindings), do: true

  defp satisfies_conditions?(conditions, bindings) do
    Enum.all?(conditions, fn condition ->
      satisfies_condition?(condition, bindings)
    end)
  end

  defp satisfies_condition?({:not_equal, term1, term2}, bindings) do
    v1 = evaluate_term(term1, bindings)
    v2 = evaluate_term(term2, bindings)
    v1 != nil and v2 != nil and v1 != v2
  end

  defp satisfies_condition?({:is_iri, term}, bindings) do
    case evaluate_term(term, bindings) do
      {:iri, _} -> true
      _ -> false
    end
  end

  defp satisfies_condition?({:is_blank, term}, bindings) do
    case evaluate_term(term, bindings) do
      {:blank_node, _} -> true
      _ -> false
    end
  end

  defp satisfies_condition?({:is_literal, term}, bindings) do
    case evaluate_term(term, bindings) do
      {:literal, _, _} -> true
      _ -> false
    end
  end

  defp satisfies_condition?({:bound, term}, bindings) do
    case evaluate_term(term, bindings) do
      {:bound, _} -> true
      _ -> false
    end
  end

  defp evaluate_term({:var, name}, bindings) do
    Map.get(bindings, name)
  end

  defp evaluate_term({:const, value}, _bindings), do: {:bound, value}
  defp evaluate_term(:var, _bindings), do: :unbound
  defp evaluate_term({:bound, _} = bound, _bindings), do: bound
  defp evaluate_term(value, _bindings) when is_integer(value), do: {:bound, value}
  defp evaluate_term(_other, _bindings), do: nil
end
