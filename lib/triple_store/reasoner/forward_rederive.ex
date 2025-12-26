defmodule TripleStore.Reasoner.ForwardRederive do
  @moduledoc """
  Forward re-derivation phase for incremental deletion with reasoning.

  After the backward phase identifies potentially invalid derived facts,
  the forward phase attempts to re-derive each fact using alternative
  justifications. Facts that can be re-derived are kept; those that
  cannot are deleted.

  ## Algorithm Overview

  The forward phase works by:
  1. Taking the set of potentially invalid facts from backward tracing
  2. For each fact, checking if any rule can derive it using remaining facts
  3. Partitioning facts into "can keep" (re-derivable) and "must delete" sets

  ## Re-derivation Check

  A fact `F` can be re-derived if there exists:
  - A rule `R` where `F` matches `R`'s head pattern
  - Bindings that satisfy all body patterns using only valid facts
  - (Valid facts = all facts minus deleted facts minus other invalid facts)

  ## Example

  Given:
  - `alice rdf:type Student` (explicit, being deleted)
  - `alice rdf:type GradStudent` (explicit, not deleted)
  - `Student rdfs:subClassOf Person` (explicit)
  - `GradStudent rdfs:subClassOf Person` (explicit)
  - `alice rdf:type Person` (derived, potentially invalid)

  The forward phase finds that `alice rdf:type Person` can be re-derived
  via `GradStudent rdfs:subClassOf Person`, so it should be kept.

  ## In-Memory API

  This module provides an in-memory API for testing:

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid,
        all_facts,
        deleted,
        rules
      )
      # result.keep - facts that can be re-derived
      # result.delete - facts that cannot be re-derived
  """

  alias TripleStore.Reasoner.Rule

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A triple as RDF terms"
  @type term_triple :: {Rule.rule_term(), Rule.rule_term(), Rule.rule_term()}

  @typedoc "A set of triples"
  @type fact_set :: MapSet.t(term_triple())

  @typedoc "Result of forward re-derivation"
  @type rederive_result :: %{
          keep: fact_set(),
          delete: fact_set(),
          rederivation_count: non_neg_integer(),
          facts_checked: non_neg_integer()
        }

  # ============================================================================
  # In-Memory API
  # ============================================================================

  @doc """
  Attempts to re-derive potentially invalid facts using alternative justifications.

  For each potentially invalid fact, checks if it can be derived from the
  remaining valid facts. Facts that can be re-derived are kept; those that
  cannot are marked for deletion.

  ## Parameters

  - `potentially_invalid` - Set of facts that may be invalid (from backward trace)
  - `all_facts` - Set of all facts (explicit + derived)
  - `deleted` - Set of facts that are being deleted
  - `rules` - List of reasoning rules

  ## Returns

  - `{:ok, result}` - Re-derivation completed
    - `result.keep` - Facts that can be re-derived (should be kept)
    - `result.delete` - Facts that cannot be re-derived (should be deleted)
    - `result.rederivation_count` - Number of facts successfully re-derived
    - `result.facts_checked` - Total facts checked

  ## Examples

      potentially_invalid = MapSet.new([{iri("alice"), rdf_type(), iri("Person")}])
      all_facts = MapSet.new([...])
      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])
      rules = [Rules.cax_sco()]

      {:ok, result} = ForwardRederive.rederive_in_memory(
        potentially_invalid, all_facts, deleted, rules
      )
  """
  @spec rederive_in_memory(fact_set(), fact_set(), fact_set(), [Rule.t()]) ::
          {:ok, rederive_result()}
  def rederive_in_memory(potentially_invalid, all_facts, deleted, rules) do
    # Valid facts = all facts - deleted facts - potentially invalid facts
    # We exclude potentially invalid facts to avoid circular reasoning
    base_valid = MapSet.difference(all_facts, deleted)

    # Process each potentially invalid fact
    {keep, delete} =
      potentially_invalid
      |> Enum.reduce({MapSet.new(), MapSet.new()}, fn fact, {keep_acc, delete_acc} ->
        # Valid facts for checking this specific fact
        # Include other potentially invalid facts that we've determined can be kept
        valid_for_check = MapSet.union(base_valid, keep_acc)
        # But exclude the fact we're checking (to avoid self-justification)
        valid_for_check = MapSet.delete(valid_for_check, fact)
        # Also exclude potentially invalid facts we haven't processed yet
        valid_for_check = MapSet.difference(valid_for_check, potentially_invalid)
        # But add back facts we've already determined are valid
        valid_for_check = MapSet.union(valid_for_check, keep_acc)

        if can_rederive?(fact, valid_for_check, rules) do
          {MapSet.put(keep_acc, fact), delete_acc}
        else
          {keep_acc, MapSet.put(delete_acc, fact)}
        end
      end)

    {:ok, %{
      keep: keep,
      delete: delete,
      rederivation_count: MapSet.size(keep),
      facts_checked: MapSet.size(potentially_invalid)
    }}
  end

  @doc """
  Checks if a fact can be re-derived from the given set of valid facts.

  A fact can be re-derived if there exists at least one rule where:
  1. The fact matches the rule's head pattern
  2. All body patterns can be satisfied by valid facts
  3. All conditions are satisfied

  ## Parameters

  - `fact` - The fact to check for re-derivation
  - `valid_facts` - Set of facts that can be used for re-derivation
  - `rules` - List of reasoning rules

  ## Returns

  `true` if the fact can be re-derived, `false` otherwise.

  ## Examples

      fact = {iri("alice"), rdf_type(), iri("Person")}
      valid_facts = MapSet.new([
        {iri("alice"), rdf_type(), iri("GradStudent")},
        {iri("GradStudent"), rdfs_subClassOf(), iri("Person")}
      ])
      rules = [Rules.cax_sco()]

      ForwardRederive.can_rederive?(fact, valid_facts, rules)
      # => true (can be derived via GradStudent subClassOf Person)
  """
  @spec can_rederive?(term_triple(), fact_set(), [Rule.t()]) :: boolean()
  def can_rederive?(fact, valid_facts, rules) do
    Enum.any?(rules, fn rule ->
      can_derive_with_rule?(fact, valid_facts, rule)
    end)
  end

  @doc """
  Partitions potentially invalid facts into keep and delete sets.

  This is a convenience function that wraps `rederive_in_memory/4` and
  returns just the partition result.

  ## Parameters

  - `potentially_invalid` - Set of facts that may be invalid
  - `all_facts` - Set of all facts
  - `deleted` - Set of facts being deleted
  - `rules` - List of reasoning rules

  ## Returns

  `{keep, delete}` tuple of MapSets.
  """
  @spec partition_invalid(fact_set(), fact_set(), fact_set(), [Rule.t()]) ::
          {fact_set(), fact_set()}
  def partition_invalid(potentially_invalid, all_facts, deleted, rules) do
    {:ok, result} = rederive_in_memory(potentially_invalid, all_facts, deleted, rules)
    {result.keep, result.delete}
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  # Check if a fact can be derived using a specific rule
  defp can_derive_with_rule?(fact, valid_facts, rule) do
    # First, check if the fact matches the rule's head pattern
    case match_head(fact, rule.head) do
      {:ok, head_bindings} ->
        # Get body patterns
        patterns = Rule.body_patterns(rule)
        conditions = Rule.body_conditions(rule)

        # Try to find bindings that satisfy all body patterns
        case find_satisfying_bindings(patterns, head_bindings, valid_facts) do
          [] ->
            false

          bindings_list ->
            # Check if any binding set satisfies the conditions
            Enum.any?(bindings_list, fn bindings ->
              satisfies_conditions?(conditions, bindings)
            end)
        end

      :no_match ->
        false
    end
  end

  # Match a fact against a rule head, returning bindings
  defp match_head({s, p, o}, {:pattern, [hs, hp, ho]}) do
    with {:ok, b1} <- unify_term(s, hs, %{}),
         {:ok, b2} <- unify_term(p, hp, b1),
         {:ok, b3} <- unify_term(o, ho, b2) do
      {:ok, b3}
    else
      :no_match -> :no_match
    end
  end

  # Unify a concrete term with a pattern term
  defp unify_term(concrete, {:var, name}, bindings) do
    case Map.get(bindings, name) do
      nil -> {:ok, Map.put(bindings, name, concrete)}
      ^concrete -> {:ok, bindings}
      _other -> :no_match
    end
  end

  defp unify_term(concrete, pattern, bindings) when concrete == pattern do
    {:ok, bindings}
  end

  defp unify_term(_concrete, _pattern, _bindings) do
    :no_match
  end

  # Find all binding sets that satisfy all body patterns
  defp find_satisfying_bindings(patterns, initial_bindings, valid_facts) do
    # Start with the initial bindings from the head match
    patterns
    |> Enum.reduce([[initial_bindings]], fn pattern, bindings_list ->
      # For each current binding set, try to extend it
      bindings_list
      |> Enum.flat_map(fn bindings_set ->
        bindings_set
        |> Enum.flat_map(fn bindings ->
          extend_bindings(pattern, bindings, valid_facts)
        end)
      end)
      |> case do
        [] -> [[]]  # No bindings found, will be filtered
        extended -> [extended]
      end
    end)
    |> List.flatten()
    |> Enum.reject(&Enum.empty?/1)
  end

  # Try to extend bindings by matching a pattern against valid facts
  defp extend_bindings({:pattern, [ps, pp, po]}, bindings, valid_facts) do
    # Substitute known bindings into the pattern
    s_sub = substitute_if_bound(ps, bindings)
    p_sub = substitute_if_bound(pp, bindings)
    o_sub = substitute_if_bound(po, bindings)

    # Find matching facts and extend bindings
    valid_facts
    |> Enum.filter(fn {fs, fp, fo} ->
      term_matches?(fs, s_sub) and term_matches?(fp, p_sub) and term_matches?(fo, o_sub)
    end)
    |> Enum.map(fn {fs, fp, fo} ->
      # Extend bindings with matched values
      bindings
      |> maybe_bind(ps, fs)
      |> maybe_bind(pp, fp)
      |> maybe_bind(po, fo)
    end)
    |> Enum.reject(&is_nil/1)
  end

  # Substitute a term if the binding exists
  defp substitute_if_bound({:var, name}, bindings) do
    case Map.get(bindings, name) do
      nil -> {:var, name}
      value -> value
    end
  end

  defp substitute_if_bound(term, _bindings), do: term

  # Check if a concrete term matches a pattern term
  defp term_matches?(_concrete, {:var, _}), do: true
  defp term_matches?(concrete, pattern), do: concrete == pattern

  # Maybe add a binding if the pattern is a variable
  defp maybe_bind(bindings, {:var, name}, value) do
    case Map.get(bindings, name) do
      nil -> Map.put(bindings, name, value)
      ^value -> bindings
      _other -> nil  # Inconsistent binding
    end
  end

  defp maybe_bind(bindings, pattern, value) do
    if pattern == value, do: bindings, else: nil
  end

  # Check if all conditions are satisfied
  defp satisfies_conditions?(conditions, bindings) do
    Enum.all?(conditions, fn condition ->
      Rule.evaluate_condition(condition, bindings)
    end)
  end
end
