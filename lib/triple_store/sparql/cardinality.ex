defmodule TripleStore.SPARQL.Cardinality do
  @moduledoc """
  Cardinality estimation for SPARQL query optimization.

  This module provides functions to estimate the number of results (cardinality)
  for triple patterns and joins. These estimates are used by the cost-based
  optimizer to select efficient query execution plans.

  ## Estimation Approach

  Cardinality estimation uses a combination of:

  1. **Base statistics**: Total triples, distinct counts for S/P/O
  2. **Predicate histogram**: Per-predicate triple counts
  3. **Selectivity factors**: Adjustments for bound vs unbound positions
  4. **Join selectivity**: Estimates for joining intermediate results

  ## Selectivity Model

  The selectivity of a pattern is the fraction of the database it matches:

  - Unbound variable: selectivity = 1.0 (matches all values)
  - Bound constant: selectivity = 1/distinct_count for that position

  For example, with 1000 distinct subjects:
  - Pattern (?s, ?p, ?o) has subject selectivity 1.0
  - Pattern (alice, ?p, ?o) has subject selectivity 1/1000 = 0.001

  ## Join Cardinality

  Join cardinality is estimated using the independence assumption:

      card(A ⋈ B) = card(A) * card(B) * join_selectivity

  Where join_selectivity depends on the join variables and their domains.

  ## Usage

      # Estimate pattern cardinality
      stats = %{
        triple_count: 10000,
        distinct_subjects: 1000,
        distinct_predicates: 50,
        distinct_objects: 2000,
        predicate_histogram: %{42 => 500, 43 => 1500}
      }

      pattern = {:triple, {:variable, "s"}, 42, {:variable, "o"}}
      card = Cardinality.estimate_pattern(pattern, stats)
      # => 500.0 (from predicate histogram)

      # Estimate join cardinality
      join_card = Cardinality.estimate_join(500.0, 1500.0, ["s"], stats)
      # => ~750.0 (depends on domain sizes)

  ## References

  - Mannino, M. V., et al. (1988). Statistical Profile Estimation in Database Systems
  - Swami, A. (1989). Optimization of Large Join Queries
  """

  alias TripleStore.SPARQL.Leapfrog.PatternUtils

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Triple pattern from SPARQL algebra"
  @type triple_pattern :: {:triple, term(), term(), term()}

  @typedoc "Statistics map with cardinality information"
  @type stats :: %{
          optional(:triple_count) => non_neg_integer(),
          optional(:distinct_subjects) => non_neg_integer(),
          optional(:distinct_predicates) => non_neg_integer(),
          optional(:distinct_objects) => non_neg_integer(),
          optional(:predicate_histogram) => %{non_neg_integer() => non_neg_integer()}
        }

  @typedoc "Cardinality estimate (always positive)"
  @type cardinality :: float()

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Default estimates when statistics are unavailable
  @default_triple_count 10_000
  @default_distinct_subjects 1_000
  @default_distinct_predicates 100
  @default_distinct_objects 2_000

  # Minimum cardinality to avoid division by zero and unrealistic estimates
  @min_cardinality 1.0

  # ===========================================================================
  # Public API - Pattern Cardinality
  # ===========================================================================

  @doc """
  Estimates the cardinality of a triple pattern.

  The estimate is based on the pattern structure (which positions are bound)
  and available statistics about the data distribution.

  ## Arguments

  - `pattern` - A triple pattern `{:triple, subject, predicate, object}`
  - `stats` - Statistics map with counts and histogram

  ## Returns

  Estimated number of matching triples (float, always >= 1.0).

  ## Estimation Strategy

  1. If the predicate is a constant and in the histogram, use that count
  2. Otherwise, start with total triple count
  3. Apply selectivity factors for each bound position

  ## Examples

      # Pattern with known predicate
      pattern = {:triple, {:variable, "s"}, 42, {:variable, "o"}}
      stats = %{predicate_histogram: %{42 => 500}}
      Cardinality.estimate_pattern(pattern, stats)
      # => 500.0

      # Fully unbound pattern
      pattern = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      stats = %{triple_count: 10000}
      Cardinality.estimate_pattern(pattern, stats)
      # => 10000.0

      # Bound subject
      pattern = {:triple, 123, {:variable, "p"}, {:variable, "o"}}
      stats = %{triple_count: 10000, distinct_subjects: 1000}
      Cardinality.estimate_pattern(pattern, stats)
      # => 10.0 (10000 / 1000)

  """
  @spec estimate_pattern(triple_pattern(), stats()) :: cardinality()
  def estimate_pattern({:triple, subject, predicate, object}, stats) do
    # Get base cardinality from predicate if possible
    base_card = get_base_cardinality(predicate, stats)

    # Calculate selectivity for each position
    subject_sel = position_selectivity(subject, :subject, stats)
    predicate_sel = position_selectivity(predicate, :predicate, stats)
    object_sel = position_selectivity(object, :object, stats)

    # If predicate is bound and in histogram, don't apply predicate selectivity again
    predicate_sel =
      if is_constant?(predicate) and has_predicate_count?(predicate, stats) do
        1.0
      else
        predicate_sel
      end

    # Combine selectivities
    cardinality = base_card * subject_sel * predicate_sel * object_sel

    max(cardinality, @min_cardinality)
  end

  @doc """
  Estimates the cardinality of a pattern with additional bindings.

  When some variables are already bound from previous joins, their
  selectivity is factored in based on the binding domain size.

  ## Arguments

  - `pattern` - A triple pattern
  - `stats` - Statistics map
  - `bound_vars` - Map of variable name to domain size (number of distinct values)

  ## Returns

  Estimated cardinality considering bound variables.

  ## Examples

      # Pattern where ?s is already bound to ~100 distinct values
      pattern = {:triple, {:variable, "s"}, 42, {:variable, "o"}}
      stats = %{predicate_histogram: %{42 => 500}, distinct_subjects: 1000}
      bound = %{"s" => 100}
      Cardinality.estimate_pattern_with_bindings(pattern, stats, bound)
      # => 50.0 (500 * 100/1000)

  """
  @spec estimate_pattern_with_bindings(triple_pattern(), stats(), %{String.t() => pos_integer()}) ::
          cardinality()
  def estimate_pattern_with_bindings({:triple, subject, predicate, object}, stats, bound_vars) do
    # Start with base pattern cardinality
    base_card = estimate_pattern({:triple, subject, predicate, object}, stats)

    # Apply binding selectivity for each variable position
    s_adjustment = variable_binding_adjustment(subject, :subject, stats, bound_vars)
    p_adjustment = variable_binding_adjustment(predicate, :predicate, stats, bound_vars)
    o_adjustment = variable_binding_adjustment(object, :object, stats, bound_vars)

    cardinality = base_card * s_adjustment * p_adjustment * o_adjustment

    max(cardinality, @min_cardinality)
  end

  # ===========================================================================
  # Public API - Join Cardinality
  # ===========================================================================

  @doc """
  Estimates the cardinality of joining two result sets.

  Uses the independence assumption and domain-based selectivity.

  ## Arguments

  - `left_card` - Cardinality of left input
  - `right_card` - Cardinality of right input
  - `join_vars` - List of variable names being joined
  - `stats` - Statistics map

  ## Returns

  Estimated cardinality of the join result.

  ## Join Selectivity Model

  For each join variable, the selectivity is:

      sel(var) = 1 / max(domain_left(var), domain_right(var))

  The final join cardinality is:

      card(A ⋈ B) = card(A) * card(B) * product(selectivities)

  ## Examples

      # Simple join on one variable
      Cardinality.estimate_join(1000.0, 500.0, ["x"], stats)
      # If domain of x has ~1000 values: 1000 * 500 / 1000 = 500

      # Join on multiple variables (more selective)
      Cardinality.estimate_join(1000.0, 500.0, ["x", "y"], stats)
      # More selective due to two join conditions

  """
  @spec estimate_join(cardinality(), cardinality(), [String.t()], stats()) :: cardinality()
  def estimate_join(left_card, right_card, [], _stats) do
    # Cartesian product (no join variables)
    max(left_card * right_card, @min_cardinality)
  end

  def estimate_join(left_card, right_card, join_vars, stats) do
    # Calculate combined selectivity for all join variables
    join_selectivity =
      join_vars
      |> Enum.map(&estimate_join_variable_selectivity(&1, left_card, right_card, stats))
      |> Enum.reduce(1.0, &(&1 * &2))

    cardinality = left_card * right_card * join_selectivity

    max(cardinality, @min_cardinality)
  end

  @doc """
  Estimates the cardinality of joining multiple patterns.

  This is a convenience function for estimating a multi-way join.

  ## Arguments

  - `patterns` - List of triple patterns
  - `stats` - Statistics map

  ## Returns

  Estimated cardinality of joining all patterns.

  ## Algorithm

  Uses a greedy left-to-right join estimation, accumulating:
  - Running cardinality
  - Set of bound variables

  """
  @spec estimate_multi_pattern(list(triple_pattern()), stats()) :: cardinality()
  def estimate_multi_pattern([], _stats), do: @min_cardinality

  def estimate_multi_pattern([single], stats) do
    estimate_pattern(single, stats)
  end

  def estimate_multi_pattern([first | rest], stats) do
    # Start with first pattern
    initial_card = estimate_pattern(first, stats)
    initial_vars = pattern_variables(first)
    initial_domains = estimate_variable_domains(first, stats)

    # Accumulate joins
    {final_card, _vars, _domains} =
      Enum.reduce(rest, {initial_card, initial_vars, initial_domains}, fn pattern,
                                                                          {acc_card, acc_vars,
                                                                           acc_domains} ->
        pattern_card = estimate_pattern(pattern, stats)
        pattern_vars = pattern_variables(pattern)

        # Find join variables (intersection)
        join_vars = MapSet.intersection(acc_vars, pattern_vars) |> MapSet.to_list()

        # Estimate this join
        if join_vars == [] do
          # Cartesian product
          new_card = acc_card * pattern_card
          new_vars = MapSet.union(acc_vars, pattern_vars)
          new_domains = Map.merge(acc_domains, estimate_variable_domains(pattern, stats))
          {max(new_card, @min_cardinality), new_vars, new_domains}
        else
          # Join with selectivity
          join_card = estimate_join(acc_card, pattern_card, join_vars, stats)
          new_vars = MapSet.union(acc_vars, pattern_vars)
          new_domains = Map.merge(acc_domains, estimate_variable_domains(pattern, stats))
          {join_card, new_vars, new_domains}
        end
      end)

    final_card
  end

  # ===========================================================================
  # Public API - Selectivity
  # ===========================================================================

  @doc """
  Estimates the selectivity of a single triple pattern.

  Selectivity is the fraction of the database the pattern matches (0.0 to 1.0).

  ## Arguments

  - `pattern` - A triple pattern
  - `stats` - Statistics map

  ## Returns

  Selectivity factor (float between 0.0 and 1.0).

  ## Examples

      # Fully unbound pattern
      Cardinality.estimate_selectivity(pattern, stats)
      # => 1.0

      # Pattern with bound predicate
      Cardinality.estimate_selectivity(pattern, stats)
      # => 0.02 (if predicate has 2% of triples)

  """
  @spec estimate_selectivity(triple_pattern(), stats()) :: float()
  def estimate_selectivity(pattern, stats) do
    card = estimate_pattern(pattern, stats)
    total = get_stat(stats, :triple_count, @default_triple_count)

    if total > 0 do
      min(card / total, 1.0)
    else
      1.0
    end
  end

  # ===========================================================================
  # Private Helpers - Base Cardinality
  # ===========================================================================

  # Get base cardinality, preferring predicate histogram when available
  @spec get_base_cardinality(term(), stats()) :: cardinality()
  defp get_base_cardinality(predicate, stats) do
    case get_predicate_count(predicate, stats) do
      {:ok, count} -> count * 1.0
      :not_found -> get_stat(stats, :triple_count, @default_triple_count) * 1.0
    end
  end

  # Get count for a predicate from histogram
  @spec get_predicate_count(term(), stats()) :: {:ok, non_neg_integer()} | :not_found
  defp get_predicate_count(predicate, stats) do
    with true <- is_constant?(predicate),
         id when is_integer(id) <- get_constant_id(predicate),
         histogram when is_map(histogram) <- Map.get(stats, :predicate_histogram),
         count when is_integer(count) <- Map.get(histogram, id) do
      {:ok, count}
    else
      _ -> :not_found
    end
  end

  # Check if predicate has a count in histogram
  @spec has_predicate_count?(term(), stats()) :: boolean()
  defp has_predicate_count?(predicate, stats) do
    get_predicate_count(predicate, stats) != :not_found
  end

  # ===========================================================================
  # Private Helpers - Position Selectivity
  # ===========================================================================

  # Calculate selectivity for a position (subject, predicate, or object)
  @spec position_selectivity(term(), :subject | :predicate | :object, stats()) :: float()
  defp position_selectivity(term, position, stats) do
    if is_constant?(term) do
      # Bound constant: selectivity = 1/distinct_count
      distinct_count = distinct_count_for_position(position, stats)
      1.0 / max(distinct_count, 1)
    else
      # Unbound variable: selectivity = 1.0 (matches all)
      1.0
    end
  end

  # Get distinct count for a position
  @spec distinct_count_for_position(:subject | :predicate | :object, stats()) :: non_neg_integer()
  defp distinct_count_for_position(:subject, stats) do
    get_stat(stats, :distinct_subjects, @default_distinct_subjects)
  end

  defp distinct_count_for_position(:predicate, stats) do
    get_stat(stats, :distinct_predicates, @default_distinct_predicates)
  end

  defp distinct_count_for_position(:object, stats) do
    get_stat(stats, :distinct_objects, @default_distinct_objects)
  end

  # ===========================================================================
  # Private Helpers - Binding Adjustment
  # ===========================================================================

  # Calculate adjustment factor when a variable is already bound
  @spec variable_binding_adjustment(
          term(),
          :subject | :predicate | :object,
          stats(),
          %{String.t() => pos_integer()}
        ) :: float()
  defp variable_binding_adjustment(term, position, stats, bound_vars) do
    case PatternUtils.extract_var_name(term) do
      nil ->
        # Not a variable, no adjustment
        1.0

      var_name ->
        case Map.get(bound_vars, var_name) do
          nil ->
            # Variable not bound, no adjustment
            1.0

          bound_domain_size ->
            # Variable is bound to bound_domain_size distinct values
            # Adjustment = bound_domain / total_domain
            total_domain = distinct_count_for_position(position, stats)
            min(bound_domain_size / max(total_domain, 1), 1.0)
        end
    end
  end

  # ===========================================================================
  # Private Helpers - Join Selectivity
  # ===========================================================================

  # Estimate selectivity for a single join variable
  @spec estimate_join_variable_selectivity(
          String.t(),
          cardinality(),
          cardinality(),
          stats()
        ) :: float()
  defp estimate_join_variable_selectivity(_var_name, left_card, right_card, stats) do
    # Use the harmonic mean of estimated domain sizes
    # This is a conservative estimate that works well in practice
    total_triples = get_stat(stats, :triple_count, @default_triple_count)

    # Estimate domain size from cardinalities
    # Assumes uniform distribution
    left_domain = estimate_domain_from_card(left_card, total_triples)
    right_domain = estimate_domain_from_card(right_card, total_triples)

    # Selectivity is 1/max(domains)
    max_domain = max(left_domain, right_domain)
    1.0 / max(max_domain, 1.0)
  end

  # Estimate domain size from cardinality
  @spec estimate_domain_from_card(cardinality(), non_neg_integer()) :: float()
  defp estimate_domain_from_card(card, total_triples) do
    # Heuristic: domain size is roughly sqrt(cardinality) for uniform data
    # Bounded by total triple count
    min(:math.sqrt(card), total_triples * 1.0)
  end

  # ===========================================================================
  # Private Helpers - Pattern Analysis
  # ===========================================================================

  # Get all variables in a pattern
  @spec pattern_variables(triple_pattern()) :: MapSet.t(String.t())
  defp pattern_variables({:triple, s, p, o}) do
    [s, p, o]
    |> Enum.map(&PatternUtils.extract_var_name/1)
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  # Estimate domain sizes for variables in a pattern
  @spec estimate_variable_domains(triple_pattern(), stats()) :: %{String.t() => float()}
  defp estimate_variable_domains({:triple, s, p, o}, stats) do
    domains = %{}

    domains =
      case PatternUtils.extract_var_name(s) do
        nil -> domains
        name -> Map.put(domains, name, distinct_count_for_position(:subject, stats) * 1.0)
      end

    domains =
      case PatternUtils.extract_var_name(p) do
        nil -> domains
        name -> Map.put(domains, name, distinct_count_for_position(:predicate, stats) * 1.0)
      end

    case PatternUtils.extract_var_name(o) do
      nil -> domains
      name -> Map.put(domains, name, distinct_count_for_position(:object, stats) * 1.0)
    end
  end

  # ===========================================================================
  # Private Helpers - Term Analysis
  # ===========================================================================

  # Check if a term is a constant (not a variable)
  @spec is_constant?(term()) :: boolean()
  defp is_constant?({:variable, _}), do: false
  defp is_constant?(_), do: true

  # Get the ID from a constant term
  @spec get_constant_id(term()) :: non_neg_integer() | nil
  defp get_constant_id(id) when is_integer(id), do: id
  # Would need dictionary lookup
  defp get_constant_id({:named_node, _}), do: nil
  defp get_constant_id({:literal, _, _}), do: nil
  defp get_constant_id({:blank_node, _}), do: nil
  defp get_constant_id(_), do: nil

  # ===========================================================================
  # Private Helpers - Statistics Access
  # ===========================================================================

  # Get a statistic with default fallback
  @spec get_stat(stats(), atom(), non_neg_integer()) :: non_neg_integer()
  defp get_stat(stats, key, default) do
    Map.get(stats, key, default)
  end
end
