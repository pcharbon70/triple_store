defmodule TripleStore.Reasoner.SemiNaive do
  @moduledoc """
  Semi-naive evaluation for forward-chaining materialization.

  This module implements the fixpoint iteration loop for OWL 2 RL reasoning.
  Semi-naive evaluation is an optimization that processes only newly derived
  facts (delta) in each iteration, dramatically reducing redundant computation
  compared to naive fixpoint iteration.

  ## Algorithm

  The semi-naive algorithm proceeds as follows:

  1. **Initialization**: Set delta = all explicit facts in the database
  2. **Iteration**: While delta is non-empty:
     a. For each rule, apply it using delta facts
     b. Collect all new derivations (not already in database)
     c. Add new derivations to the database
     d. Set delta = new derivations
  3. **Termination**: When delta is empty, fixpoint is reached

  ## Usage

      # Materialize all inferences for a set of rules
      {:ok, stats} = SemiNaive.materialize(lookup_fn, store_fn, rules, initial_facts)

      # Stats includes iteration count, derivation count, timing

  ## Rule Stratification

  For rules involving negation (not present in OWL 2 RL core), stratification
  ensures correct evaluation order. Rules are grouped into strata where:
  - Rules in stratum 0 have no negated predicates
  - Rules in stratum n only negate predicates defined in strata < n

  For OWL 2 RL without negation, all rules are in stratum 0.

  ## Performance Characteristics

  - **Time complexity**: O(|derived| × |rules| × avg_rule_cost)
  - **Space complexity**: O(|facts|) for fact storage
  - **Convergence**: Guaranteed for monotonic rules (no negation)
  - **Typical iterations**: 3-10 for most ontologies

  ## Telemetry Events

  Emits telemetry events during materialization:
  - `[:triple_store, :reasoner, :materialize, :start]`
  - `[:triple_store, :reasoner, :materialize, :stop]`
  - `[:triple_store, :reasoner, :materialize, :iteration]`
  """

  alias TripleStore.Reasoner.DeltaComputation
  alias TripleStore.Reasoner.Rule
  alias TripleStore.Reasoner.Telemetry

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "A ground triple as a tuple of three terms"
  @type triple :: DeltaComputation.triple()

  @typedoc "A set of facts (triples)"
  @type fact_set :: DeltaComputation.fact_set()

  @typedoc "Function to look up facts matching a pattern"
  @type lookup_fn :: (Rule.pattern() -> {:ok, [triple()]} | {:error, term()})

  @typedoc "Function to store derived facts"
  @type store_fn :: (fact_set() -> :ok | {:error, term()})

  @typedoc "Materialization statistics"
  @type stats :: %{
          iterations: non_neg_integer(),
          total_derived: non_neg_integer(),
          derivations_per_iteration: [non_neg_integer()],
          duration_ms: non_neg_integer(),
          rules_applied: non_neg_integer()
        }

  @typedoc "Options for materialization"
  @type materialize_opts :: [
          max_iterations: non_neg_integer(),
          max_facts: non_neg_integer(),
          trace: boolean(),
          emit_telemetry: boolean()
        ]

  @typedoc "Stratum definition for rule ordering"
  @type stratum :: %{
          level: non_neg_integer(),
          rules: [Rule.t()]
        }

  # ============================================================================
  # Configuration
  # ============================================================================

  @default_max_iterations 1000
  @default_max_facts 10_000_000

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Materializes all inferences by iterating rules until fixpoint.

  This is the main entry point for forward-chaining materialization. It applies
  all provided rules repeatedly until no new facts can be derived.

  ## Parameters

  - `lookup_fn` - Function to look up facts: `(pattern) -> {:ok, [triple]} | {:error, reason}`
  - `store_fn` - Function to store derived facts: `(fact_set) -> :ok | {:error, reason}`
  - `rules` - List of reasoning rules to apply
  - `initial_facts` - Initial set of explicit facts (used as first delta)
  - `opts` - Options (see below)

  ## Options

  - `:max_iterations` - Maximum iterations before stopping (default: #{@default_max_iterations})
  - `:max_facts` - Maximum total facts before stopping (default: #{@default_max_facts})
  - `:trace` - Log detailed progress (default: false)
  - `:emit_telemetry` - Emit telemetry events (default: true)

  ## Returns

  - `{:ok, stats}` - Materialization completed with statistics
  - `{:error, :max_iterations_exceeded}` - Hit iteration limit
  - `{:error, :max_facts_exceeded}` - Hit fact limit
  - `{:error, reason}` - Other error

  ## Examples

      lookup = fn pattern -> Index.lookup_all(db, pattern) end
      store = fn facts -> Index.insert_triples(db, facts) end
      rules = Rules.owl2rl_rules()
      initial = load_explicit_facts(db)

      {:ok, stats} = SemiNaive.materialize(lookup, store, rules, initial)
      IO.puts("Derived \#{stats.total_derived} facts in \#{stats.iterations} iterations")
  """
  @spec materialize(lookup_fn(), store_fn(), [Rule.t()], fact_set(), materialize_opts()) ::
          {:ok, stats()} | {:error, term()}
  def materialize(lookup_fn, store_fn, rules, initial_facts, opts \\ []) do
    max_iterations = Keyword.get(opts, :max_iterations, @default_max_iterations)
    max_facts = Keyword.get(opts, :max_facts, @default_max_facts)
    emit_telemetry = Keyword.get(opts, :emit_telemetry, true)

    start_time = System.monotonic_time(:millisecond)

    # Stratify rules (for OWL 2 RL, all rules are in stratum 0)
    strata = stratify_rules(rules)

    # Initial state
    state = %{
      all_facts: initial_facts,
      delta: initial_facts,
      iterations: 0,
      total_derived: 0,
      derivations_per_iteration: [],
      rules_applied: 0
    }

    # Emit start telemetry
    if emit_telemetry do
      Telemetry.emit_start([:triple_store, :reasoner, :materialize], %{
        rule_count: length(rules),
        initial_fact_count: MapSet.size(initial_facts)
      })
    end

    # Run fixpoint loop
    result = run_fixpoint(
      lookup_fn,
      store_fn,
      strata,
      state,
      max_iterations,
      max_facts,
      emit_telemetry
    )

    # Calculate duration and emit stop telemetry
    duration_ms = System.monotonic_time(:millisecond) - start_time

    case result do
      {:ok, final_state} ->
        stats = %{
          iterations: final_state.iterations,
          total_derived: final_state.total_derived,
          derivations_per_iteration: Enum.reverse(final_state.derivations_per_iteration),
          duration_ms: duration_ms,
          rules_applied: final_state.rules_applied
        }

        if emit_telemetry do
          Telemetry.emit_stop([:triple_store, :reasoner, :materialize], duration_ms * 1_000_000, stats)
        end

        {:ok, stats}

      {:error, reason} = error ->
        if emit_telemetry do
          Telemetry.emit_stop([:triple_store, :reasoner, :materialize], duration_ms * 1_000_000, %{
            error: reason
          })
        end

        error
    end
  end

  @doc """
  Materializes inferences using an in-memory fact store.

  This is a convenience function that maintains facts in memory rather than
  requiring external storage. Useful for testing and small datasets.

  ## Parameters

  - `rules` - List of reasoning rules to apply
  - `initial_facts` - Initial set of explicit facts
  - `opts` - Options (same as `materialize/5`)

  ## Returns

  - `{:ok, all_facts, stats}` - All facts (explicit + derived) and statistics
  - `{:error, reason}` - On failure

  ## Examples

      rules = Rules.rdfs_rules()
      facts = MapSet.new([...])

      {:ok, all_facts, stats} = SemiNaive.materialize_in_memory(rules, facts)
  """
  @spec materialize_in_memory([Rule.t()], fact_set(), materialize_opts()) ::
          {:ok, fact_set(), stats()} | {:error, term()}
  def materialize_in_memory(rules, initial_facts, opts \\ []) do
    # Use an Agent to hold the current facts
    {:ok, agent} = Agent.start_link(fn -> initial_facts end)

    try do
      lookup_fn = fn pattern ->
        facts = Agent.get(agent, & &1)
        {:ok, match_pattern(pattern, facts)}
      end

      store_fn = fn new_facts ->
        Agent.update(agent, fn existing -> MapSet.union(existing, new_facts) end)
        :ok
      end

      case materialize(lookup_fn, store_fn, rules, initial_facts, opts) do
        {:ok, stats} ->
          all_facts = Agent.get(agent, & &1)
          {:ok, all_facts, stats}

        {:error, _} = error ->
          error
      end
    after
      Agent.stop(agent)
    end
  end

  @doc """
  Stratifies rules based on negation dependencies.

  For OWL 2 RL (which has no negation), all rules are placed in stratum 0.
  This function is included for future extensibility.

  ## Parameters

  - `rules` - List of rules to stratify

  ## Returns

  List of strata, each containing rules at that level.
  """
  @spec stratify_rules([Rule.t()]) :: [stratum()]
  def stratify_rules(rules) do
    # OWL 2 RL has no negation, so all rules are in stratum 0
    # Future: analyze rule dependencies for proper stratification
    [%{level: 0, rules: rules}]
  end

  @doc """
  Computes statistics about the materialization state.

  ## Parameters

  - `all_facts` - Set of all facts (explicit + derived)
  - `initial_count` - Number of initial explicit facts

  ## Returns

  Map with derived count and expansion ratio.
  """
  @spec compute_stats(fact_set(), non_neg_integer()) :: map()
  def compute_stats(all_facts, initial_count) do
    total = MapSet.size(all_facts)
    derived = total - initial_count

    %{
      total_facts: total,
      initial_facts: initial_count,
      derived_facts: derived,
      expansion_ratio: if(initial_count > 0, do: total / initial_count, else: 0.0)
    }
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp run_fixpoint(lookup_fn, store_fn, strata, state, max_iterations, max_facts, emit_telemetry) do
    if MapSet.size(state.delta) == 0 do
      # Fixpoint reached - no new facts
      {:ok, state}
    else
      if state.iterations >= max_iterations do
        {:error, :max_iterations_exceeded}
      else
        if MapSet.size(state.all_facts) >= max_facts do
          {:error, :max_facts_exceeded}
        else
          # Apply all strata in order
          case apply_strata(lookup_fn, strata, state) do
            {:ok, new_derivations, rules_applied} ->
              iteration_count = MapSet.size(new_derivations)

              # Emit iteration telemetry
              if emit_telemetry and iteration_count > 0 do
                :telemetry.execute(
                  [:triple_store, :reasoner, :materialize, :iteration],
                  %{derivations: iteration_count},
                  %{iteration: state.iterations + 1}
                )
              end

              # Store derived facts
              case store_fn.(new_derivations) do
                :ok ->
                  new_state = %{
                    all_facts: MapSet.union(state.all_facts, new_derivations),
                    delta: new_derivations,
                    iterations: state.iterations + 1,
                    total_derived: state.total_derived + iteration_count,
                    derivations_per_iteration: [iteration_count | state.derivations_per_iteration],
                    rules_applied: state.rules_applied + rules_applied
                  }

                  run_fixpoint(lookup_fn, store_fn, strata, new_state, max_iterations, max_facts, emit_telemetry)

                {:error, _} = error ->
                  error
              end

            {:error, _} = error ->
              error
          end
        end
      end
    end
  end

  defp apply_strata(lookup_fn, strata, state) do
    # Apply each stratum in order, collecting all derivations
    Enum.reduce_while(strata, {:ok, MapSet.new(), 0}, fn stratum, {:ok, acc_derivations, acc_rules} ->
      case apply_stratum(lookup_fn, stratum, state, acc_derivations) do
        {:ok, stratum_derivations, rules_applied} ->
          {:cont, {:ok, MapSet.union(acc_derivations, stratum_derivations), acc_rules + rules_applied}}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp apply_stratum(lookup_fn, %{rules: rules}, state, already_derived) do
    # Apply each rule in the stratum
    all_existing = MapSet.union(state.all_facts, already_derived)

    result =
      Enum.reduce_while(rules, {:ok, MapSet.new(), 0}, fn rule, {:ok, acc, rule_count} ->
        case DeltaComputation.apply_rule_delta(lookup_fn, rule, state.delta, all_existing) do
          {:ok, new_facts} ->
            # Filter out facts we've already derived in this iteration
            truly_new = MapSet.difference(new_facts, acc)
            {:cont, {:ok, MapSet.union(acc, truly_new), rule_count + 1}}

          {:error, _} = error ->
            {:halt, error}
        end
      end)

    case result do
      {:ok, derivations, rules_applied} ->
        # Filter out facts already in database
        filtered = MapSet.difference(derivations, state.all_facts)
        {:ok, filtered, rules_applied}

      error ->
        error
    end
  end

  defp match_pattern({:pattern, [s, p, o]}, facts) do
    Enum.filter(facts, fn {fs, fp, fo} ->
      matches_term?(fs, s) and matches_term?(fp, p) and matches_term?(fo, o)
    end)
  end

  defp matches_term?(_fact_term, {:var, _}), do: true
  defp matches_term?(fact_term, pattern_term), do: fact_term == pattern_term
end
