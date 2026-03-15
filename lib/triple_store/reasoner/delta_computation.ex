defmodule TripleStore.Reasoner.DeltaComputation do
  # Suppress dialyzer warnings related to MapSet opaque type handling.
  @dialyzer {:nowarn_function, apply_rule_delta: 5}

  @moduledoc """
  Delta computation for semi-naive evaluation of reasoning rules.

  Semi-naive evaluation is an optimization over naive fixpoint iteration that
  avoids redundant computation by processing only newly derived facts (delta)
  in each iteration. This module provides the core delta computation primitives.

  ## Algorithm Overview

  In semi-naive evaluation:
  1. Initialize delta with all explicit facts (or new facts for incremental)
  2. For each iteration:
     - Apply rules using delta for at least one body pattern
     - Collect new derivations not already in the database
     - Set delta = new derivations
  3. Repeat until delta is empty (fixpoint)

  ## Delta Pattern Application

  For a rule with n body patterns, we generate n-1 delta variants. Each variant
  uses delta for a different body pattern position while using the full database
  for other positions. This ensures we find all new derivations without
  redundantly reprocessing old fact combinations.

  For example, a rule with 2 body patterns:
  ```
  head :- body1, body2
  ```

  Generates two delta variants:
  1. `head :- delta(body1), full(body2)` - Find matches with new body1 facts
  2. `head :- full(body1), delta(body2)` - Find matches with new body2 facts

  ## Quad Support

  This module supports both triple and quad patterns for graph-aware reasoning:

  - **Triple patterns**: `{:pattern, [subject, predicate, object]}`
  - **Quad patterns**: `{:quad_pattern, [graph, subject, predicate, object]}`

  When working with quads, the graph component is included in all operations
  including indexing, pattern matching, and unification.

  ## Usage

      # Apply a rule using delta facts
      {:ok, new_facts} = DeltaComputation.apply_rule_delta(
        lookup_fn,     # Function to look up facts matching a pattern
        rule,          # The reasoning rule
        delta_facts,   # Facts from previous iteration
        existing_facts # All known facts (for filtering duplicates)
      )

  ## Performance Considerations

  - Delta size typically decreases each iteration as we approach fixpoint
  - Indexing delta facts by predicate improves lookup performance
  - For rules with many body patterns, consider limiting delta positions
  """

  alias TripleStore.Reasoner.PatternMatcher
  alias TripleStore.Reasoner.Rule

  require Logger

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A ground triple as a tuple of three terms"
  @type triple :: {Rule.rule_term(), Rule.rule_term(), Rule.rule_term()}

  @typedoc "A ground quad as a tuple of four terms (graph, subject, predicate, object)"
  @type quad :: {Rule.rule_term(), Rule.rule_term(), Rule.rule_term(), Rule.rule_term()}

  @typedoc "A fact (triple or quad)"
  @type fact :: triple() | quad()

  @typedoc "A set of facts (triples or quads)"
  @type fact_set :: MapSet.t()

  @typedoc "Index mapping predicates to facts with that predicate"
  @type predicate_index :: %{Rule.rule_term() => [fact()]}

  @typedoc "Options for delta computation"
  @type delta_opts :: [
          max_derivations: non_neg_integer(),
          trace: boolean()
        ]

  @typedoc "Result of applying a rule with delta"
  @type apply_result :: {:ok, fact_set()} | {:error, term()}

  # ============================================================================
  # Configuration
  # ============================================================================

  @default_max_derivations 100_000

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Applies a rule using delta facts for at least one body pattern.

  This is the core function for semi-naive evaluation. It generates all possible
  instantiations of the rule head where at least one body pattern matches a
  delta fact, then filters out any derivations that already exist.

  ## Parameters

  - `lookup_fn` - Function to look up facts matching a pattern: `(pattern) -> [triple]`
  - `rule` - The reasoning rule to apply
  - `delta` - Set of new facts from previous iteration
  - `existing` - Set of all existing facts (for duplicate filtering)
  - `opts` - Options (see `delta_opts`)

  ## Returns

  - `{:ok, new_facts}` - Set of newly derived facts
  - `{:error, reason}` - On failure

  ## Examples

      lookup = fn pattern -> Index.lookup_all(db, pattern) end
      delta = MapSet.new([{{:iri, "a"}, {:iri, "p"}, {:iri, "b"}}])
      existing = MapSet.new([...])

      {:ok, new_facts} = DeltaComputation.apply_rule_delta(lookup, rule, delta, existing)
  """
  @spec apply_rule_delta(
          (Rule.pattern() -> {:ok, [triple()]} | {:error, term()}),
          Rule.t(),
          fact_set(),
          fact_set(),
          delta_opts()
        ) :: apply_result()
  def apply_rule_delta(lookup_fn, rule, delta, existing, opts \\ []) do
    max_derivations = Keyword.get(opts, :max_derivations, @default_max_derivations)

    # Get delta positions from rule metadata (or all pattern positions)
    delta_positions = Rule.delta_positions(rule)
    patterns = Rule.body_patterns(rule)

    if Enum.empty?(patterns) do
      # No patterns means rule cannot match anything
      {:ok, MapSet.new()}
    else
      # Index delta facts by predicate for efficient lookup
      delta_index = index_by_predicate(delta)

      # Apply rule for each delta position using Stream for lazy evaluation
      # This avoids building large intermediate lists before taking max_derivations
      new_facts =
        delta_positions
        |> Stream.flat_map(fn delta_pos ->
          apply_with_delta_at_position(
            lookup_fn,
            rule,
            patterns,
            delta,
            delta_index,
            delta_pos,
            existing
          )
        end)
        |> Stream.take(max_derivations)
        |> MapSet.new()

      # Filter out existing facts
      truly_new = MapSet.difference(new_facts, existing)

      {:ok, truly_new}
    end
  end

  @doc """
  Generates all bindings from matching a rule body against a database.

  This function finds all variable bindings that satisfy all body patterns,
  using delta facts for the specified position.

  ## Parameters

  - `lookup_fn` - Fact lookup function
  - `patterns` - List of body patterns
  - `delta` - Delta fact set
  - `delta_index` - Predicate-indexed delta facts
  - `delta_pos` - Which pattern position uses delta
  - `conditions` - Additional filter conditions

  ## Returns

  List of bindings (maps from variable names to terms).
  """
  @spec generate_bindings(
          (Rule.pattern() -> {:ok, [triple()]} | {:error, term()}),
          [Rule.pattern()],
          fact_set(),
          predicate_index(),
          non_neg_integer(),
          [Rule.condition()]
        ) :: [Rule.binding()]
  def generate_bindings(lookup_fn, patterns, delta, delta_index, delta_pos, conditions) do
    patterns
    |> Enum.with_index()
    |> Enum.reduce([%{}], fn {{:pattern, terms} = pattern, pos}, bindings ->
      use_delta = pos == delta_pos

      Enum.flat_map(bindings, fn binding ->
        expand_bindings_for_pattern(
          lookup_fn,
          pattern,
          terms,
          binding,
          delta,
          delta_index,
          use_delta
        )
      end)
    end)
    |> Enum.filter(&binding_satisfies_conditions?(&1, conditions))
  end

  @doc """
  Instantiates a rule head pattern with a binding to produce a ground fact.

  Supports both triple patterns `{:pattern, [s, p, o]}` and quad patterns
  `{:quad_pattern, [g, s, p, o]}`.

  ## Parameters

  - `head` - The rule head pattern
  - `binding` - Variable binding map

  ## Returns

  A ground triple or quad if all variables are bound, or nil if unbound variables remain.

  ## Examples

      head = {:pattern, [{:var, "x"}, {:iri, "type"}, {:var, "c"}]}
      binding = %{"x" => {:iri, "alice"}, "c" => {:iri, "Person"}}
      instantiate_head(head, binding)
      # => {{:iri, "alice"}, {:iri, "type"}, {:iri, "Person"}}

      head = {:quad_pattern, [{:var, "g"}, {:var, "x"}, {:iri, "type"}, {:var, "c"}]}
      binding = %{"g" => {:iri, "graph1"}, "x" => {:iri, "alice"}, "c" => {:iri, "Person"}}
      instantiate_head(head, binding)
      # => {{:iri, "graph1"}, {:iri, "alice"}, {:iri, "type"}, {:iri, "Person"}}
  """
  @spec instantiate_head(Rule.pattern() | Rule.quad_pattern(), Rule.binding()) :: fact() | nil
  def instantiate_head({:pattern, [s, p, o]}, binding) do
    s_sub = Rule.substitute(s, binding)
    p_sub = Rule.substitute(p, binding)
    o_sub = Rule.substitute(o, binding)

    # Check all terms are ground (no remaining variables)
    if ground_term?(s_sub) and ground_term?(p_sub) and ground_term?(o_sub) do
      {s_sub, p_sub, o_sub}
    else
      nil
    end
  end

  def instantiate_head({:quad_pattern, [g, s, p, o]}, binding) do
    g_sub = Rule.substitute(g, binding)
    s_sub = Rule.substitute(s, binding)
    p_sub = Rule.substitute(p, binding)
    o_sub = Rule.substitute(o, binding)

    # Check all terms are ground (no remaining variables)
    if ground_term?(g_sub) and ground_term?(s_sub) and ground_term?(p_sub) and ground_term?(o_sub) do
      {g_sub, s_sub, p_sub, o_sub}
    else
      nil
    end
  end

  @doc """
  Creates an index of facts organized by predicate for efficient lookup.

  Supports both triples and quads. For triples, extracts the predicate from
  position 1. For quads, extracts the predicate from position 2.

  ## Examples

      # Triple facts
      facts = MapSet.new([
        {{:iri, "a"}, {:iri, "type"}, {:iri, "Person"}},
        {{:iri, "b"}, {:iri, "type"}, {:iri, "Animal"}},
        {{:iri, "a"}, {:iri, "knows"}, {:iri, "b"}}
      ])

      index = index_by_predicate(facts)
      # => %{
      #   {:iri, "type"} => [fact1, fact2],
      #   {:iri, "knows"} => [fact3]
      # }

      # Quad facts
      quad_facts = MapSet.new([
        {{:iri, "g1"}, {:iri, "a"}, {:iri, "type"}, {:iri, "Person"}},
        {{:iri, "g1"}, {:iri, "b"}, {:iri, "type"}, {:iri, "Animal"}}
      ])

      index = index_by_predicate(quad_facts)
      # => %{
      #   {:iri, "type"} => [fact1, fact2]
      # }
  """
  @spec index_by_predicate(fact_set()) :: predicate_index()
  def index_by_predicate(facts) do
    Enum.group_by(facts, &predicate_of/1)
  end

  @doc """
  Extracts the predicate from a fact (triple or quad).

  For triples `{s, p, o}`, returns `p`.
  For quads `{g, s, p, o}`, returns `p`.
  """
  @spec predicate_of(fact()) :: Rule.rule_term()
  def predicate_of({_s, p, _o}), do: p
  def predicate_of({_g, _s, p, _o}), do: p

  @doc """
  Checks if a term is ground (contains no variables).
  """
  @spec ground_term?(Rule.rule_term()) :: boolean()
  def ground_term?({:var, _}), do: false
  def ground_term?(_), do: true

  @doc """
  Filters derived facts to only those not already in the existing set.

  ## Parameters

  - `derived` - Newly derived facts
  - `existing` - Already known facts

  ## Returns

  Set of truly new facts.
  """
  @spec filter_existing(fact_set(), fact_set()) :: fact_set()
  def filter_existing(derived, existing) do
    MapSet.difference(derived, existing)
  end

  @doc """
  Merges delta into existing facts for the next iteration.

  ## Parameters

  - `existing` - Current fact set
  - `delta` - New facts to add

  ## Returns

  Combined fact set.
  """
  @spec merge_delta(fact_set(), fact_set()) :: fact_set()
  def merge_delta(existing, delta) do
    MapSet.union(existing, delta)
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp apply_with_delta_at_position(
         lookup_fn,
         rule,
         patterns,
         delta,
         delta_index,
         delta_pos,
         existing
       ) do
    conditions = Rule.body_conditions(rule)

    # Generate all bindings using delta at specified position
    bindings =
      generate_bindings(
        lookup_fn,
        patterns,
        delta,
        delta_index,
        delta_pos,
        conditions
      )

    # Instantiate head for each binding
    bindings
    |> Enum.flat_map(fn binding ->
      case instantiate_head(rule.head, binding) do
        nil -> []
        triple -> [triple]
      end
    end)
    |> Enum.reject(fn triple -> MapSet.member?(existing, triple) end)
  end

  defp get_matching_facts(lookup_fn, pattern, delta, delta_index, use_delta) do
    case use_delta do
      true -> match_pattern_against_facts(pattern, delta, delta_index)
      false -> lookup_matching_facts(lookup_fn, pattern)
    end
  end

  defp expand_bindings_for_pattern(
         lookup_fn,
         pattern,
         terms,
         binding,
         delta,
         delta_index,
         use_delta
       ) do
    substituted_pattern =
      terms
      |> Enum.map(&Rule.substitute(&1, binding))
      |> then(&{:pattern, &1})

    lookup_fn
    |> get_matching_facts(substituted_pattern, delta, delta_index, use_delta)
    |> Enum.flat_map(&extend_binding_with_fact(pattern, &1, binding))
  end

  defp extend_binding_with_fact(pattern, fact, binding) do
    case unify_pattern_with_fact(pattern, fact, binding) do
      {:ok, extended_binding} -> [extended_binding]
      :no_match -> []
    end
  end

  defp binding_satisfies_conditions?(binding, conditions) do
    Enum.all?(conditions, &Rule.evaluate_condition(&1, binding))
  end

  defp lookup_matching_facts(lookup_fn, pattern) do
    case pattern_to_lookup(pattern) do
      {:ground, triple} ->
        [triple]

      {:lookup, lookup_pattern} ->
        fetch_lookup_facts(lookup_fn, lookup_pattern)
    end
  end

  defp fetch_lookup_facts(lookup_fn, lookup_pattern) do
    case lookup_fn.(lookup_pattern) do
      {:ok, facts} ->
        facts

      {:error, reason} ->
        Logger.warning("Lookup failed during delta computation: #{inspect(reason)}")
        []
    end
  end

  defp match_pattern_against_facts({:pattern, [s, p, o]}, delta, delta_index) do
    # If predicate is ground, use the index
    case p do
      {:var, _} ->
        # Variable predicate - scan all delta facts
        Enum.filter(delta, fn fact ->
          matches_pattern?(fact, {:pattern, [s, p, o]})
        end)

      ground_predicate ->
        # Use predicate index for efficiency
        delta_index
        |> Map.get(ground_predicate, [])
        |> Enum.filter(fn fact ->
          matches_pattern?(fact, {:pattern, [s, p, o]})
        end)
    end
  end

  defp match_pattern_against_facts({:quad_pattern, [g, s, p, o]}, delta, delta_index) do
    # If predicate is ground, use the index
    case p do
      {:var, _} ->
        # Variable predicate - scan all delta facts
        Enum.filter(delta, fn fact ->
          matches_pattern?(fact, {:quad_pattern, [g, s, p, o]})
        end)

      ground_predicate ->
        # Use predicate index for efficiency
        delta_index
        |> Map.get(ground_predicate, [])
        |> Enum.filter(fn fact ->
          matches_pattern?(fact, {:quad_pattern, [g, s, p, o]})
        end)
    end
  end

  defp matches_pattern?(fact, pattern) do
    case pattern do
      {:pattern, _} -> PatternMatcher.matches_triple?(fact, pattern)
      {:quad_pattern, _} -> PatternMatcher.matches_quad?(fact, pattern)
    end
  end

  defp pattern_to_lookup({:pattern, [s, p, o]}) do
    s_bound = pattern_element(s)
    p_bound = pattern_element(p)
    o_bound = pattern_element(o)

    if s_bound != :var and p_bound != :var and o_bound != :var do
      # All ground - exact triple check
      {:ground, {s, p, o}}
    else
      # Need to look up
      {:lookup, {:pattern, [s, p, o]}}
    end
  end

  defp pattern_to_lookup({:quad_pattern, [g, s, p, o]}) do
    g_bound = pattern_element(g)
    s_bound = pattern_element(s)
    p_bound = pattern_element(p)
    o_bound = pattern_element(o)

    if g_bound != :var and s_bound != :var and p_bound != :var and o_bound != :var do
      # All ground - exact quad check
      {:ground, {g, s, p, o}}
    else
      # Need to look up
      {:lookup, {:quad_pattern, [g, s, p, o]}}
    end
  end

  defp pattern_element({:var, _}), do: :var
  defp pattern_element(_), do: :bound

  defp unify_pattern_with_fact({:pattern, [ps, pp, po]}, {fs, fp, fo}, binding) do
    with {:ok, b1} <- unify_term(ps, fs, binding),
         {:ok, b2} <- unify_term(pp, fp, b1),
         {:ok, b3} <- unify_term(po, fo, b2) do
      {:ok, b3}
    else
      :no_match -> :no_match
    end
  end

  defp unify_term({:var, name}, fact_term, binding) do
    case Map.get(binding, name) do
      nil ->
        # Variable not yet bound - bind it
        {:ok, Map.put(binding, name, fact_term)}

      existing ->
        # Variable already bound - check consistency
        if existing == fact_term do
          {:ok, binding}
        else
          :no_match
        end
    end
  end

  defp unify_term(pattern_term, fact_term, binding) do
    if pattern_term == fact_term do
      {:ok, binding}
    else
      :no_match
    end
  end
end
