defmodule TripleStore.Reasoner.BackwardTrace do
  @moduledoc """
  Backward tracing for incremental deletion with reasoning.

  When facts are deleted from a reasoned triple store, we need to identify
  all derived facts that may have depended on the deleted facts. This module
  implements the backward phase of the Backward/Forward deletion algorithm.

  ## Algorithm Overview

  The backward phase works by:
  1. Starting with the deleted fact(s)
  2. Finding all derived facts that could have used those facts in their derivation
  3. Recursively tracing through the dependency graph
  4. Returning all potentially invalid derived facts

  ## Dependency Detection

  A derived fact `D` depends on a fact `F` if there exists a rule `R` where:
  - `F` matches one of `R`'s body patterns
  - `D` matches `R`'s head pattern
  - The variable bindings are consistent

  For example, with the cax-sco rule:
  ```
  (?x rdf:type ?c2) :- (?x rdf:type ?c1), (?c1 rdfs:subClassOf ?c2)
  ```

  If we delete `(alice rdf:type Student)`, we must trace:
  - Any derived facts like `(alice rdf:type Person)` that came from this

  ## In-Memory API

  This module provides an in-memory API via `trace_in_memory/4` for testing
  with in-memory fact sets. Database integration is handled by the
  `DeleteWithReasoning` module which uses this module internally.

  ## Usage

      # Find all potentially invalid facts after deleting some facts
      deleted = MapSet.new([{alice, rdf_type, student}])
      {:ok, invalid} = BackwardTrace.trace_in_memory(deleted, all_facts, rules)
      # invalid contains all derived facts that may need to be re-evaluated
  """

  alias TripleStore.Reasoner.PatternMatcher
  alias TripleStore.Reasoner.Rule

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A triple as RDF terms"
  @type term_triple :: {Rule.rule_term(), Rule.rule_term(), Rule.rule_term()}

  @typedoc "A set of triples"
  @type fact_set :: MapSet.t(term_triple())

  @typedoc "Options for backward tracing"
  @type trace_opts :: [
          max_depth: non_neg_integer(),
          include_deleted: boolean()
        ]

  @typedoc "Result of backward tracing"
  @type trace_result :: %{
          potentially_invalid: fact_set(),
          trace_depth: non_neg_integer(),
          facts_examined: non_neg_integer()
        }

  # ============================================================================
  # Configuration
  # ============================================================================

  @default_max_depth 100

  # ============================================================================
  # In-Memory API
  # ============================================================================

  @doc """
  Traces backward from deleted facts to find all potentially invalid derived facts.

  This function finds all derived facts that may have depended on the deleted
  facts, either directly or transitively through other derived facts.

  ## Parameters

  - `deleted` - Set of facts that were deleted
  - `all_derived` - Set of all derived facts in the store
  - `rules` - List of reasoning rules that were used for derivation
  - `opts` - Options (see below)

  ## Options

  - `:max_depth` - Maximum recursion depth (default: #{@default_max_depth})
  - `:include_deleted` - Include deleted facts in result (default: false)

  ## Returns

  - `{:ok, result}` - Tracing completed successfully
    - `result.potentially_invalid` - Set of derived facts that may be invalid
    - `result.trace_depth` - Maximum depth reached during tracing
    - `result.facts_examined` - Total facts examined
  - `{:error, reason}` - On failure

  ## Examples

      deleted = MapSet.new([{iri("alice"), rdf_type(), iri("Student")}])
      derived = MapSet.new([{iri("alice"), rdf_type(), iri("Person")}, ...])
      rules = [Rules.cax_sco()]

      {:ok, result} = BackwardTrace.trace_in_memory(deleted, derived, rules)
      # result.potentially_invalid contains derived facts that may need re-evaluation
  """
  @spec trace_in_memory(fact_set(), fact_set(), [Rule.t()], trace_opts()) ::
          {:ok, trace_result()} | {:error, term()}
  def trace_in_memory(deleted, all_derived, rules, opts \\ []) do
    max_depth = Keyword.get(opts, :max_depth, @default_max_depth)
    include_deleted = Keyword.get(opts, :include_deleted, false)

    # Initial state
    state = %{
      potentially_invalid: MapSet.new(),
      visited: MapSet.new(),
      current_depth: 0,
      max_depth_reached: 0,
      facts_examined: 0
    }

    # Start tracing from deleted facts
    result = trace_recursive(deleted, all_derived, rules, state, max_depth)

    # Optionally include the original deleted facts
    potentially_invalid =
      if include_deleted do
        MapSet.union(result.potentially_invalid, deleted)
      else
        result.potentially_invalid
      end

    {:ok, %{
      potentially_invalid: potentially_invalid,
      trace_depth: result.max_depth_reached,
      facts_examined: result.facts_examined
    }}
  end

  @doc """
  Finds derived facts that directly depend on a given fact.

  This is a simpler version that only looks one level deep, finding
  derived facts that could have been produced by rules using the given fact.

  ## Parameters

  - `fact` - The fact to trace from
  - `all_derived` - Set of all derived facts
  - `rules` - List of reasoning rules

  ## Returns

  Set of derived facts that directly depend on the given fact.

  ## Examples

      fact = {iri("alice"), rdf_type(), iri("Student")}
      dependents = BackwardTrace.find_direct_dependents(fact, derived, rules)
  """
  @spec find_direct_dependents(term_triple(), fact_set(), [Rule.t()]) :: fact_set()
  def find_direct_dependents(fact, all_derived, rules) do
    rules
    |> Enum.flat_map(fn rule ->
      find_dependents_for_rule(fact, all_derived, rule)
    end)
    |> MapSet.new()
  end

  @doc """
  Checks if a derived fact could have been produced by a rule using a given input fact.

  ## Parameters

  - `derived` - The derived fact to check
  - `input` - The potential input fact
  - `rule` - The rule to check
  - `all_facts` - All facts (for binding verification)

  ## Returns

  `true` if the derived fact could have used the input fact via this rule.
  """
  @spec could_derive?(term_triple(), term_triple(), Rule.t(), fact_set()) :: boolean()
  def could_derive?(derived, input, rule, all_facts) do
    # Check if derived matches the rule head
    case PatternMatcher.match_rule_head(derived, rule.head) do
      {:ok, head_bindings} ->
        # Check if input matches any body pattern with consistent bindings
        patterns = Rule.body_patterns(rule)

        Enum.any?(patterns, fn pattern ->
          case PatternMatcher.match_rule_head(input, pattern) do
            {:ok, input_bindings} ->
              # Check binding consistency and other patterns
              consistent_bindings?(head_bindings, input_bindings) and
                other_patterns_satisfiable?(rule, input, head_bindings, all_facts)

            :no_match ->
              false
          end
        end)

      :no_match ->
        false
    end
  end

  # ============================================================================
  # Private Functions - Tracing
  # ============================================================================

  defp trace_recursive(facts_to_trace, all_derived, rules, state, max_depth) do
    if state.current_depth >= max_depth or MapSet.size(facts_to_trace) == 0 do
      state
    else
      # Find all facts that haven't been visited yet
      unvisited = MapSet.difference(facts_to_trace, state.visited)

      if MapSet.size(unvisited) == 0 do
        state
      else
        # Mark as visited
        new_visited = MapSet.union(state.visited, unvisited)

        # Find all derived facts that depend on the unvisited facts
        new_dependents =
          unvisited
          |> Enum.flat_map(fn fact ->
            find_direct_dependents(fact, all_derived, rules)
            |> MapSet.to_list()
          end)
          |> MapSet.new()

        # Filter to only derived facts (not explicit)
        new_invalid = MapSet.intersection(new_dependents, all_derived)

        # Update state
        new_state = %{
          potentially_invalid: MapSet.union(state.potentially_invalid, new_invalid),
          visited: new_visited,
          current_depth: state.current_depth + 1,
          max_depth_reached: max(state.max_depth_reached, state.current_depth + 1),
          facts_examined: state.facts_examined + MapSet.size(unvisited)
        }

        # Recursively trace from newly found invalid facts
        trace_recursive(new_invalid, all_derived, rules, new_state, max_depth)
      end
    end
  end

  # ============================================================================
  # Private Functions - Rule Matching
  # ============================================================================

  # Find derived facts that could have been produced by a rule using the given fact
  defp find_dependents_for_rule(fact, all_derived, rule) do
    patterns = Rule.body_patterns(rule)

    # For each body pattern, check if fact matches it
    patterns
    |> Enum.with_index()
    |> Enum.flat_map(fn {pattern, _idx} ->
      case PatternMatcher.match_rule_head(fact, pattern) do
        {:ok, bindings} ->
          # Find derived facts that match the head with these bindings
          find_matching_derivations(rule.head, bindings, all_derived)

        :no_match ->
          []
      end
    end)
  end

  # Find derived facts that match a head pattern with given bindings
  defp find_matching_derivations(head_pattern, bindings, all_derived) do
    # Substitute known bindings into the head pattern
    {:pattern, [h_s, h_p, h_o]} = head_pattern

    s_sub = PatternMatcher.substitute_if_bound(h_s, bindings)
    p_sub = PatternMatcher.substitute_if_bound(h_p, bindings)
    o_sub = PatternMatcher.substitute_if_bound(h_o, bindings)

    # Find derived facts that match the (partially) substituted head
    Enum.filter(all_derived, fn {d_s, d_p, d_o} ->
      PatternMatcher.matches_term?(d_s, s_sub) and
        PatternMatcher.matches_term?(d_p, p_sub) and
        PatternMatcher.matches_term?(d_o, o_sub)
    end)
  end

  # Check if two binding sets are consistent (no conflicting values)
  defp consistent_bindings?(bindings1, bindings2) do
    Enum.all?(bindings2, fn {key, value} ->
      case Map.get(bindings1, key) do
        nil -> true
        ^value -> true
        _other -> false
      end
    end)
  end

  # Check if other body patterns can be satisfied given bindings
  # This is a simplified check - in a full implementation, we'd verify
  # that the other patterns actually have matching facts
  defp other_patterns_satisfiable?(rule, input, bindings, all_facts) do
    patterns = Rule.body_patterns(rule)

    # For rules with multiple body patterns, check that other patterns
    # could be satisfied. This is conservative - we assume they could be
    # if the bindings are consistent.
    case patterns do
      [_single] ->
        # Only one pattern, input matches it, we're done
        true

      multiple ->
        # Multiple patterns - check if any pattern besides the one matching input
        # could have facts satisfying it
        Enum.any?(multiple, fn pattern ->
          case PatternMatcher.match_rule_head(input, pattern) do
            {:ok, _} ->
              # This is the pattern that input matches
              true

            :no_match ->
              # Check if this pattern could match any facts with current bindings
              pattern_satisfiable?(pattern, bindings, all_facts)
          end
        end)
    end
  end

  # Check if a pattern could be satisfied given current bindings
  defp pattern_satisfiable?({:pattern, [ps, pp, po]}, bindings, all_facts) do
    s_sub = PatternMatcher.substitute_if_bound(ps, bindings)
    p_sub = PatternMatcher.substitute_if_bound(pp, bindings)
    o_sub = PatternMatcher.substitute_if_bound(po, bindings)

    Enum.any?(all_facts, fn {fs, fp, fo} ->
      PatternMatcher.matches_term?(fs, s_sub) and
        PatternMatcher.matches_term?(fp, p_sub) and
        PatternMatcher.matches_term?(fo, o_sub)
    end)
  end
end
