defmodule TripleStore.SPARQL.Leapfrog.VariableOrdering do
  @moduledoc """
  Determines optimal variable ordering for Leapfrog Triejoin execution.

  The Variable Elimination Order (VEO) is crucial for Leapfrog performance.
  A good ordering processes selective variables first, reducing the search
  space early and minimizing the number of intermediate values to consider.

  ## Ordering Strategy

  The algorithm uses a greedy approach:
  1. Collect all variables from the triple patterns
  2. Estimate the selectivity of each variable based on:
     - Number of patterns containing the variable (more = more constrained)
     - Position in patterns (predicates are typically more selective)
     - Bound terms in patterns (constants reduce cardinality)
     - Statistics (predicate counts, if available)
  3. Order variables from most selective to least selective

  ## Index Availability

  For each variable, we also consider which indices can efficiently provide
  values for that variable given the already-bound variables:
  - SPO: Subject first, then Predicate, then Object
  - POS: Predicate first, then Object, then Subject
  - OSP: Object first, then Subject, then Predicate

  ## Usage

      patterns = [
        {:triple, {:variable, "x"}, {:named_node, "knows"}, {:variable, "y"}},
        {:triple, {:variable, "y"}, {:named_node, "age"}, {:variable, "z"}}
      ]

      {:ok, order} = VariableOrdering.compute(patterns, stats)
      # => ["y", "x", "z"]  # y appears in both patterns, most constrained

  ## References

  - Veldhuizen, T. L. (2014). Leapfrog Triejoin: A simple, worst-case optimal join algorithm
  - Abo Khamis, M., et al. (2016). Joins via Geometric Resolutions
  """

  alias TripleStore.SPARQL.Leapfrog.PatternUtils

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "A triple pattern from the SPARQL algebra"
  @type triple_pattern :: {:triple, term(), term(), term()}

  @typedoc "Statistics map for selectivity estimation"
  @type stats :: map()

  @typedoc "Variable name"
  @type variable :: String.t()

  @typedoc "Variable ordering result"
  @type ordering :: [variable()]

  @typedoc """
  Information about a variable's selectivity and index support.

  - `:name` - Variable name
  - `:patterns` - Patterns containing this variable
  - `:positions` - Positions in patterns (:subject, :predicate, :object)
  - `:selectivity` - Estimated selectivity score (lower = more selective)
  - `:available_indices` - Indices that can provide this variable efficiently
  """
  @type variable_info :: %{
          name: variable(),
          patterns: [triple_pattern()],
          positions: [:subject | :predicate | :object],
          selectivity: float(),
          available_indices: [:spo | :pos | :osp]
        }

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Computes the optimal variable ordering for a set of triple patterns.

  ## Arguments

  - `patterns` - List of triple patterns
  - `stats` - Statistics map for selectivity estimation (optional)

  ## Returns

  - `{:ok, ordering}` - Ordered list of variable names
  - `{:error, reason}` - On failure

  ## Examples

      patterns = [
        {:triple, {:variable, "s"}, {:named_node, "type"}, {:variable, "t"}},
        {:triple, {:variable, "s"}, {:named_node, "name"}, {:variable, "n"}}
      ]

      {:ok, order} = VariableOrdering.compute(patterns)
      # => ["s", "t", "n"] or ["s", "n", "t"]
      # "s" is first because it appears in both patterns

  """
  @spec compute([triple_pattern()], stats()) :: {:ok, ordering()} | {:error, term()}
  def compute(patterns, stats \\ %{})

  def compute([], _stats) do
    {:ok, []}
  end

  def compute(patterns, stats) when is_list(patterns) do
    # 1. Extract all variables and their info
    var_infos = collect_variable_info(patterns, stats)

    # 2. Order variables by selectivity (greedy approach)
    ordering = greedy_order(var_infos)

    {:ok, ordering}
  end

  @doc """
  Computes variable ordering with detailed info for each variable.

  Returns the ordering along with selectivity scores and index recommendations.

  ## Arguments

  - `patterns` - List of triple patterns
  - `stats` - Statistics map for selectivity estimation (optional)

  ## Returns

  - `{:ok, ordering, var_infos}` - Ordering and detailed variable info
  - `{:error, reason}` - On failure

  """
  @spec compute_with_info([triple_pattern()], stats()) ::
          {:ok, ordering(), %{variable() => variable_info()}} | {:error, term()}
  def compute_with_info(patterns, stats \\ %{})

  def compute_with_info([], _stats) do
    {:ok, [], %{}}
  end

  def compute_with_info(patterns, stats) when is_list(patterns) do
    var_infos = collect_variable_info(patterns, stats)
    ordering = greedy_order(var_infos)

    {:ok, ordering, var_infos}
  end

  @doc """
  Returns the best index to use for a variable given already-bound variables.

  ## Arguments

  - `variable` - The variable to find values for
  - `pattern` - The triple pattern containing the variable
  - `bound_vars` - Set of already-bound variable names

  ## Returns

  - `{index, prefix_vars}` - The index to use and which bound variables form the prefix

  ## Examples

      # Pattern: (?s, knows, ?o) with ?s already bound
      {:spo, ["s"]} = best_index_for("o", pattern, MapSet.new(["s"]))

      # Pattern: (?s, ?p, obj) with neither s nor p bound
      {:osp, []} = best_index_for("s", pattern, MapSet.new())

  """
  @spec best_index_for(variable(), triple_pattern(), MapSet.t(variable())) ::
          {:spo | :pos | :osp, [variable()]}
  def best_index_for(variable, {:triple, subject, predicate, object}, bound_vars) do
    # Determine positions in the pattern
    s_var = PatternUtils.extract_var_name(subject)
    p_var = PatternUtils.extract_var_name(predicate)
    o_var = PatternUtils.extract_var_name(object)

    s_bound = PatternUtils.is_bound_or_const?(subject, bound_vars)
    p_bound = PatternUtils.is_bound_or_const?(predicate, bound_vars)
    o_bound = PatternUtils.is_bound_or_const?(object, bound_vars)

    # Find where our target variable is
    var_position =
      cond do
        s_var == variable -> :subject
        p_var == variable -> :predicate
        o_var == variable -> :object
        true -> :not_found
      end

    # Choose the best index based on what's bound
    case {var_position, s_bound, p_bound, o_bound} do
      # Variable is subject
      {:subject, _, true, true} -> {:pos, prefix_vars(p_var, o_var, bound_vars)}
      {:subject, _, true, false} -> {:pos, prefix_vars(p_var, nil, bound_vars)}
      {:subject, _, false, true} -> {:osp, prefix_vars(o_var, nil, bound_vars)}
      {:subject, _, false, false} -> {:spo, []}
      # Variable is predicate
      {:predicate, true, _, true} -> {:spo, prefix_vars(s_var, nil, bound_vars)}
      {:predicate, true, _, false} -> {:spo, prefix_vars(s_var, nil, bound_vars)}
      {:predicate, false, _, true} -> {:osp, prefix_vars(o_var, nil, bound_vars)}
      {:predicate, false, _, false} -> {:pos, []}
      # Variable is object
      {:object, true, true, _} -> {:spo, prefix_vars(s_var, p_var, bound_vars)}
      {:object, true, false, _} -> {:spo, prefix_vars(s_var, nil, bound_vars)}
      {:object, false, true, _} -> {:pos, prefix_vars(p_var, nil, bound_vars)}
      {:object, false, false, _} -> {:osp, []}
      # Variable not in pattern
      {:not_found, _, _, _} -> {:spo, []}
    end
  end

  @doc """
  Estimates the selectivity of a variable in a set of patterns.

  Lower selectivity score means more selective (fewer matching values).

  ## Arguments

  - `variable` - Variable name to estimate
  - `patterns` - Patterns containing this variable
  - `stats` - Statistics map

  ## Returns

  Selectivity score (float, lower = more selective)

  """
  @spec estimate_selectivity(variable(), [triple_pattern()], stats()) :: float()
  def estimate_selectivity(variable, patterns, stats \\ %{}) do
    patterns
    |> Enum.filter(&PatternUtils.pattern_contains_variable?(&1, variable))
    |> Enum.map(&pattern_selectivity_for_variable(&1, variable, stats))
    |> combine_selectivities()
  end

  # ===========================================================================
  # Private: Variable Collection
  # ===========================================================================

  # Collect information about all variables in the patterns
  defp collect_variable_info(patterns, stats) do
    # First, find all variables
    all_vars = extract_all_variables(patterns)

    # For each variable, compute its info
    all_vars
    |> Enum.map(fn var_name ->
      containing_patterns = patterns_containing_variable(patterns, var_name)
      positions = variable_positions(containing_patterns, var_name)
      selectivity = estimate_selectivity(var_name, containing_patterns, stats)
      indices = available_indices_for_positions(positions)

      {var_name,
       %{
         name: var_name,
         patterns: containing_patterns,
         positions: positions,
         selectivity: selectivity,
         available_indices: indices
       }}
    end)
    |> Map.new()
  end

  # Extract all unique variable names from patterns
  defp extract_all_variables(patterns) do
    patterns
    |> Enum.flat_map(&PatternUtils.pattern_variables/1)
    |> Enum.uniq()
  end

  # Find patterns containing a specific variable
  defp patterns_containing_variable(patterns, var_name) do
    Enum.filter(patterns, &PatternUtils.pattern_contains_variable?(&1, var_name))
  end

  # Get positions where a variable appears
  defp variable_positions(patterns, var_name) do
    patterns
    |> Enum.flat_map(fn {:triple, s, p, o} ->
      positions = []

      positions =
        if PatternUtils.extract_var_name(s) == var_name,
          do: [:subject | positions],
          else: positions

      positions =
        if PatternUtils.extract_var_name(p) == var_name,
          do: [:predicate | positions],
          else: positions

      positions =
        if PatternUtils.extract_var_name(o) == var_name,
          do: [:object | positions],
          else: positions

      positions
    end)
    |> Enum.uniq()
  end

  # Determine which indices can efficiently provide values for given positions
  defp available_indices_for_positions(positions) do
    # Map positions to indices that have them in useful order
    positions
    |> Enum.flat_map(fn
      # SPO has subject first, OSP has subject second
      :subject -> [:spo, :osp]
      # POS has predicate first, SPO has predicate second
      :predicate -> [:pos, :spo]
      # OSP has object first, POS has object second
      :object -> [:osp, :pos]
    end)
    |> Enum.uniq()
  end

  # ===========================================================================
  # Private: Selectivity Estimation
  # ===========================================================================

  # Estimate selectivity for a variable in a specific pattern
  defp pattern_selectivity_for_variable({:triple, s, p, o}, var_name, stats) do
    # Base selectivity from position
    position_sel =
      cond do
        # Subjects have high cardinality
        PatternUtils.extract_var_name(s) == var_name -> 100.0
        # Predicates usually low cardinality
        PatternUtils.extract_var_name(p) == var_name -> 20.0
        # Objects have high cardinality
        PatternUtils.extract_var_name(o) == var_name -> 100.0
        true -> 1000.0
      end

    # Adjust based on other bound terms in the pattern
    bound_adjustment =
      [s, p, o]
      |> Enum.count(&PatternUtils.is_constant?/1)
      |> case do
        # No constants - least selective
        0 -> 1.0
        # One constant - moderately selective
        1 -> 0.1
        # Two constants - very selective
        2 -> 0.01
        # All constants (rare) - maximally selective
        3 -> 0.001
      end

    # Adjust for predicate statistics if available
    predicate_adjustment =
      case p do
        {:named_node, uri} ->
          case Map.get(stats, {:predicate_count, uri}) do
            nil -> 1.0
            count when count < 10 -> 0.1
            count when count < 100 -> 0.3
            count when count < 1000 -> 0.5
            count when count < 10000 -> 0.8
            _ -> 1.0
          end

        _ ->
          1.0
      end

    position_sel * bound_adjustment * predicate_adjustment
  end

  # Combine selectivities from multiple patterns
  # More patterns = more constrained = lower selectivity score
  defp combine_selectivities([]), do: 1_000_000.0

  defp combine_selectivities(selectivities) do
    count = length(selectivities)

    # Use minimum selectivity (most selective pattern)
    min_sel = Enum.min(selectivities)

    # Apply bonus for appearing in multiple patterns
    # Each additional pattern reduces selectivity by factor of 0.3
    pattern_bonus = :math.pow(0.3, count - 1)

    min_sel * pattern_bonus
  end

  # ===========================================================================
  # Private: Greedy Ordering
  # ===========================================================================

  # Order variables greedily by selectivity
  defp greedy_order(var_infos) when map_size(var_infos) == 0, do: []

  defp greedy_order(var_infos) do
    # Sort by selectivity (ascending - most selective first)
    var_infos
    |> Enum.sort_by(fn {_name, info} -> info.selectivity end)
    |> Enum.map(fn {name, _info} -> name end)
  end

  # ===========================================================================
  # Private: Index Selection Helpers
  # ===========================================================================

  # Build prefix variable list for index
  defp prefix_vars(var1, var2, bound_vars) do
    [var1, var2]
    |> Enum.reject(&is_nil/1)
    |> Enum.filter(&MapSet.member?(bound_vars, &1))
  end
end
