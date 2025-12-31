defmodule TripleStore.SPARQL.JoinEnumeration do
  @moduledoc """
  Join enumeration for SPARQL query optimization.

  This module implements algorithms for enumerating different join orderings
  and selecting the optimal execution plan based on cost estimates. It supports
  both exhaustive enumeration for small queries and the DPccp (Dynamic Programming
  with connected subgraph complement pairs) algorithm for larger queries.

  ## Overview

  Given a set of triple patterns in a Basic Graph Pattern (BGP), this module:

  1. Builds a join graph where patterns are nodes and edges represent shared variables
  2. Enumerates valid join orderings that avoid unnecessary Cartesian products
  3. Costs each ordering using the CostModel
  4. Selects the best join strategy (nested loop, hash join, or Leapfrog)
  5. Returns an optimized execution plan

  ## Algorithms

  ### Exhaustive Enumeration (n <= 5)

  For small queries, we enumerate all permutations and select the cheapest.
  This guarantees finding the optimal plan but is O(n!) in complexity.

  ### DPccp Algorithm (n > 5)

  For larger queries, we use Dynamic Programming with connected complement pairs:

  1. Build connected subgraph pairs that can be joined
  2. Use memoization to avoid recomputing costs for subsets
  3. Prune orderings that would create Cartesian products

  This provides near-optimal plans in O(3^n) time.

  ## Join Graph

  Patterns are connected if they share at least one variable:

      Pattern 1: ?x :knows ?y
      Pattern 2: ?y :name ?n
      Pattern 3: ?x :age ?a

      Join Graph:
        P1 -- P2  (shared: ?y)
        P1 -- P3  (shared: ?x)
        P2 -- P3  (no shared variables - not connected)

  ## Strategy Selection

  The enumerator selects join strategies based on:

  - **Leapfrog**: For 4+ patterns sharing variables (multi-way join)
  - **Hash Join**: For pairs with large intermediate results
  - **Nested Loop**: For very small inputs or when memory is constrained

  ## Usage

      patterns = [
        {:triple, {:variable, "x"}, 1, {:variable, "y"}},
        {:triple, {:variable, "y"}, 2, {:variable, "z"}},
        {:triple, {:variable, "x"}, 3, {:variable, "w"}}
      ]

      stats = %{triple_count: 100_000, ...}

      {:ok, plan} = JoinEnumeration.enumerate(patterns, stats)
      # Returns optimized execution plan

  ## References

  - Moerkotte & Neumann (2006). Analysis of Two Existing and One New Dynamic
    Programming Algorithm for the Generation of Optimal Bushy Join Trees
  - Veldhuizen (2014). Leapfrog Triejoin: A Simple, Worst-Case Optimal Join
  """

  alias TripleStore.SPARQL.{Cardinality, CostModel}

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Triple pattern from SPARQL algebra"
  @type pattern :: {:triple, term(), term(), term()}

  @typedoc "Set of pattern indices"
  @type pattern_set :: MapSet.t(non_neg_integer())

  @typedoc "Join strategy"
  @type strategy :: :nested_loop | :hash_join | :leapfrog

  @typedoc "Execution plan node"
  @type plan_node ::
          {:scan, pattern()}
          | {:join, strategy(), plan_node(), plan_node(), [String.t()]}
          | {:leapfrog, [pattern()], [String.t()]}

  @typedoc "Complete execution plan with cost"
  @type plan :: %{
          tree: plan_node(),
          cost: CostModel.cost(),
          cardinality: float()
        }

  @typedoc "Statistics map"
  @type stats :: Cardinality.stats()

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Threshold for switching from exhaustive to DPccp
  @exhaustive_threshold 5

  # Minimum patterns for considering Leapfrog
  @leapfrog_min_patterns 4

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Enumerates join orderings and returns the optimal execution plan.

  ## Arguments

  - `patterns` - List of triple patterns to join
  - `stats` - Database statistics for cost estimation

  ## Returns

  - `{:ok, plan}` - The optimal execution plan
  - `{:error, reason}` - If enumeration fails

  ## Examples

      patterns = [
        {:triple, {:variable, "s"}, 1, {:variable, "o"}},
        {:triple, {:variable, "o"}, 2, {:variable, "z"}}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, stats)

  """
  @spec enumerate([pattern()], stats()) :: {:ok, plan()} | {:error, term()}
  def enumerate([], _stats) do
    {:error, :empty_patterns}
  end

  def enumerate([single], stats) do
    # Single pattern - just a scan
    cardinality = Cardinality.estimate_pattern(single, stats)
    cost = CostModel.pattern_cost(single, stats)

    {:ok,
     %{
       tree: {:scan, single},
       cost: cost,
       cardinality: cardinality
     }}
  end

  def enumerate(patterns, stats) when length(patterns) <= @exhaustive_threshold do
    exhaustive_enumerate(patterns, stats)
  end

  def enumerate(patterns, stats) do
    dpccp_enumerate(patterns, stats)
  end

  @doc """
  Builds the join graph for a set of patterns.

  Returns a map where keys are pattern indices and values are sets of
  connected pattern indices (patterns sharing at least one variable).

  ## Examples

      patterns = [
        {:triple, {:variable, "x"}, 1, {:variable, "y"}},
        {:triple, {:variable, "y"}, 2, {:variable, "z"}}
      ]

      graph = JoinEnumeration.build_join_graph(patterns)
      # => %{0 => MapSet.new([1]), 1 => MapSet.new([0])}

  """
  @spec build_join_graph([pattern()]) :: %{non_neg_integer() => pattern_set()}
  def build_join_graph(patterns) do
    indexed = Enum.with_index(patterns)
    n = length(patterns)

    # Initialize empty adjacency sets
    graph =
      for i <- 0..(n - 1), into: %{} do
        {i, MapSet.new()}
      end

    # Find connected pairs
    for {p1, i} <- indexed,
        {p2, j} <- indexed,
        i < j,
        shared_variables(p1, p2) != [],
        reduce: graph do
      acc ->
        acc
        |> Map.update!(i, &MapSet.put(&1, j))
        |> Map.update!(j, &MapSet.put(&1, i))
    end
  end

  @doc """
  Extracts variables from a triple pattern.

  ## Examples

      pattern = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      vars = JoinEnumeration.pattern_variables(pattern)
      # => ["x", "y"]

  """
  @spec pattern_variables(pattern()) :: [String.t()]
  def pattern_variables({:triple, s, p, o}) do
    [s, p, o]
    |> Enum.flat_map(&extract_variable/1)
    |> Enum.uniq()
  end

  @doc """
  Finds shared variables between two patterns.

  ## Examples

      p1 = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      p2 = {:triple, {:variable, "y"}, 2, {:variable, "z"}}
      shared = JoinEnumeration.shared_variables(p1, p2)
      # => ["y"]

  """
  @spec shared_variables(pattern(), pattern()) :: [String.t()]
  def shared_variables(p1, p2) do
    vars1 = MapSet.new(pattern_variables(p1))
    vars2 = MapSet.new(pattern_variables(p2))

    MapSet.intersection(vars1, vars2)
    |> MapSet.to_list()
  end

  @doc """
  Checks if two pattern sets are connected in the join graph.

  Two sets are connected if there exists at least one edge between them.

  """
  @spec sets_connected?(pattern_set(), pattern_set(), %{non_neg_integer() => pattern_set()}) ::
          boolean()
  def sets_connected?(set1, set2, join_graph) do
    Enum.any?(set1, fn i ->
      neighbors = Map.get(join_graph, i, MapSet.new())
      not MapSet.disjoint?(neighbors, set2)
    end)
  end

  @doc """
  Finds shared variables between two sets of patterns.

  """
  @spec shared_variables_between_sets([pattern()], pattern_set(), pattern_set()) :: [String.t()]
  def shared_variables_between_sets(patterns, set1, set2) do
    vars1 =
      set1
      |> Enum.flat_map(fn i -> pattern_variables(Enum.at(patterns, i)) end)
      |> MapSet.new()

    vars2 =
      set2
      |> Enum.flat_map(fn i -> pattern_variables(Enum.at(patterns, i)) end)
      |> MapSet.new()

    MapSet.intersection(vars1, vars2)
    |> MapSet.to_list()
  end

  # ===========================================================================
  # Exhaustive Enumeration
  # ===========================================================================

  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  defp exhaustive_enumerate(patterns, stats) do
    n = length(patterns)
    join_graph = build_join_graph(patterns)
    indexed_patterns = Enum.with_index(patterns) |> Enum.map(fn {p, i} -> {i, p} end) |> Map.new()

    # Check if we should use Leapfrog for the entire BGP
    if should_use_leapfrog_for_bgp?(patterns, stats) do
      build_leapfrog_plan(patterns, stats)
    else
      # Generate all permutations and find best
      indices = Enum.to_list(0..(n - 1))

      best_plan =
        permutations(indices)
        |> Stream.map(fn order ->
          build_left_deep_plan(order, indexed_patterns, join_graph, stats)
        end)
        |> Stream.filter(&valid_plan?/1)
        |> Enum.min_by(fn plan -> plan.cost.total end, fn -> nil end)

      if best_plan do
        {:ok, best_plan}
      else
        # No valid plan without Cartesian products - allow them
        best_plan_with_cartesian =
          permutations(indices)
          |> Stream.map(fn order ->
            build_left_deep_plan(order, indexed_patterns, join_graph, stats,
              allow_cartesian: true
            )
          end)
          |> Enum.min_by(fn plan -> plan.cost.total end, fn -> nil end)

        if best_plan_with_cartesian do
          {:ok, best_plan_with_cartesian}
        else
          {:error, :no_valid_plan}
        end
      end
    end
  end

  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  defp build_left_deep_plan(order, indexed_patterns, join_graph, stats, opts \\ []) do
    allow_cartesian = Keyword.get(opts, :allow_cartesian, false)

    case order do
      [] ->
        nil

      [first] ->
        pattern = Map.get(indexed_patterns, first)
        cardinality = Cardinality.estimate_pattern(pattern, stats)
        cost = CostModel.pattern_cost(pattern, stats)

        %{
          tree: {:scan, pattern},
          cost: cost,
          cardinality: cardinality,
          pattern_set: MapSet.new([first]),
          valid: true
        }

      [first | rest] ->
        # Start with first pattern
        initial = build_left_deep_plan([first], indexed_patterns, join_graph, stats, opts)

        # Join remaining patterns left-to-right
        Enum.reduce(rest, initial, fn idx, acc ->
          if acc == nil or not acc.valid do
            acc
          else
            pattern = Map.get(indexed_patterns, idx)
            right_set = MapSet.new([idx])

            # Check connectivity
            connected = sets_connected?(acc.pattern_set, right_set, join_graph)

            if not connected and not allow_cartesian do
              %{acc | valid: false}
            else
              join_vars =
                shared_variables_between_sets(
                  Map.values(indexed_patterns)
                  |> Enum.sort_by(fn p ->
                    Map.keys(indexed_patterns) |> Enum.find(&(Map.get(indexed_patterns, &1) == p))
                  end),
                  acc.pattern_set,
                  right_set
                )

              # Get right side cardinality and cost
              right_card = Cardinality.estimate_pattern(pattern, stats)
              right_cost = CostModel.pattern_cost(pattern, stats)

              # Select join strategy
              {strategy, join_cost} =
                CostModel.select_join_strategy(acc.cardinality, right_card, join_vars, stats)

              # Calculate output cardinality
              output_card =
                Cardinality.estimate_join(acc.cardinality, right_card, join_vars, stats)

              # Build join node
              right_node = {:scan, pattern}

              %{
                tree: {:join, strategy, acc.tree, right_node, join_vars},
                cost: CostModel.total_plan_cost([acc.cost, right_cost, join_cost]),
                cardinality: output_card,
                pattern_set: MapSet.union(acc.pattern_set, right_set),
                valid: true
              }
            end
          end
        end)
    end
  end

  defp valid_plan?(nil), do: false
  defp valid_plan?(%{valid: valid}), do: valid

  # ===========================================================================
  # DPccp Algorithm
  # ===========================================================================

  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  defp dpccp_enumerate(patterns, stats) do
    n = length(patterns)
    join_graph = build_join_graph(patterns)
    indexed_patterns = Enum.with_index(patterns) |> Enum.map(fn {p, i} -> {i, p} end) |> Map.new()

    # Check for Leapfrog first
    if should_use_leapfrog_for_bgp?(patterns, stats) do
      build_leapfrog_plan(patterns, stats)
    else
      # Initialize memoization table with single-pattern plans
      initial_memo =
        for i <- 0..(n - 1), into: %{} do
          pattern = Map.get(indexed_patterns, i)
          cardinality = Cardinality.estimate_pattern(pattern, stats)
          cost = CostModel.pattern_cost(pattern, stats)

          plan = %{
            tree: {:scan, pattern},
            cost: cost,
            cardinality: cardinality,
            pattern_set: MapSet.new([i])
          }

          {MapSet.new([i]), plan}
        end

      # Build plans for larger subsets using dynamic programming
      full_set = MapSet.new(0..(n - 1))

      memo =
        2..n
        |> Enum.reduce(initial_memo, fn size, memo ->
          enumerate_subsets_of_size(full_set, size)
          |> Enum.reduce(memo, fn subset, memo ->
            best_plan =
              find_best_plan_for_subset(subset, memo, join_graph, indexed_patterns, stats)

            if best_plan do
              Map.put(memo, subset, best_plan)
            else
              memo
            end
          end)
        end)

      case Map.get(memo, full_set) do
        nil -> {:error, :no_valid_plan}
        plan -> {:ok, Map.drop(plan, [:pattern_set])}
      end
    end
  end

  defp enumerate_subsets_of_size(full_set, size) do
    full_set
    |> MapSet.to_list()
    |> combinations(size)
    |> Enum.map(&MapSet.new/1)
  end

  defp find_best_plan_for_subset(subset, memo, join_graph, indexed_patterns, stats) do
    # Try all ways to partition subset into two connected parts
    subset
    |> generate_ccp(join_graph)
    |> Stream.map(fn {left, right} ->
      left_plan = Map.get(memo, left)
      right_plan = Map.get(memo, right)

      if left_plan && right_plan do
        build_join_plan(left_plan, right_plan, left, right, indexed_patterns, stats)
      else
        nil
      end
    end)
    |> Stream.filter(&(&1 != nil))
    |> Enum.min_by(fn plan -> plan.cost.total end, fn -> nil end)
  end

  # Generate connected complement pairs (ccp)
  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  defp generate_ccp(subset, join_graph) do
    subset_list = MapSet.to_list(subset)
    n = MapSet.size(subset)

    if n < 2 do
      []
    else
      # Generate all non-empty proper subsets
      1..(n - 1)
      |> Enum.flat_map(fn size ->
        combinations(subset_list, size)
        |> Enum.map(&MapSet.new/1)
      end)
      |> Enum.flat_map(fn left ->
        right = MapSet.difference(subset, left)

        # Check if left and right are connected
        if sets_connected?(left, right, join_graph) do
          # Return ordered pair to avoid duplicates
          if Enum.min(MapSet.to_list(left)) < Enum.min(MapSet.to_list(right)) do
            [{left, right}]
          else
            []
          end
        else
          []
        end
      end)
    end
  end

  defp build_join_plan(left_plan, right_plan, left_set, right_set, indexed_patterns, stats) do
    patterns = Map.values(indexed_patterns)

    join_vars = shared_variables_between_sets(patterns, left_set, right_set)

    # Select join strategy
    {strategy, join_cost} =
      CostModel.select_join_strategy(
        left_plan.cardinality,
        right_plan.cardinality,
        join_vars,
        stats
      )

    # Calculate output cardinality
    output_card =
      Cardinality.estimate_join(left_plan.cardinality, right_plan.cardinality, join_vars, stats)

    total_cost = CostModel.total_plan_cost([left_plan.cost, right_plan.cost, join_cost])

    %{
      tree: {:join, strategy, left_plan.tree, right_plan.tree, join_vars},
      cost: total_cost,
      cardinality: output_card,
      pattern_set: MapSet.union(left_set, right_set)
    }
  end

  # ===========================================================================
  # Leapfrog Detection and Planning
  # ===========================================================================

  defp should_use_leapfrog_for_bgp?(patterns, stats) do
    n = length(patterns)

    if n < @leapfrog_min_patterns do
      false
    else
      # Check if patterns share enough variables to benefit from Leapfrog
      all_vars =
        patterns
        |> Enum.flat_map(&pattern_variables/1)

      # Count variable occurrences
      var_counts =
        Enum.frequencies(all_vars)

      # Leapfrog is beneficial when multiple variables appear in 3+ patterns
      multi_occurrence_vars =
        var_counts
        |> Enum.filter(fn {_var, count} -> count >= 3 end)
        |> length()

      multi_occurrence_vars >= 1 and
        CostModel.should_use_leapfrog?(
          Enum.map(patterns, &Cardinality.estimate_pattern(&1, stats)),
          Map.keys(var_counts),
          stats
        )
    end
  end

  defp build_leapfrog_plan(patterns, stats) do
    all_vars =
      patterns
      |> Enum.flat_map(&pattern_variables/1)
      |> Enum.uniq()

    cardinalities = Enum.map(patterns, &Cardinality.estimate_pattern(&1, stats))
    cost = CostModel.leapfrog_cost(cardinalities, all_vars, stats)

    # Estimate output cardinality using multi-pattern estimation
    output_card = Cardinality.estimate_multi_pattern(patterns, stats)

    {:ok,
     %{
       tree: {:leapfrog, patterns, all_vars},
       cost: cost,
       cardinality: output_card
     }}
  end

  # ===========================================================================
  # Utility Functions
  # ===========================================================================

  defp extract_variable({:variable, name}), do: [name]
  defp extract_variable(_), do: []

  # Generate all permutations of a list
  defp permutations([]), do: [[]]

  defp permutations(list) do
    for elem <- list,
        rest <- permutations(list -- [elem]) do
      [elem | rest]
    end
  end

  # Generate combinations of size k from a list
  defp combinations(_, 0), do: [[]]
  defp combinations([], _), do: []

  defp combinations([head | tail], k) do
    with_head = for combo <- combinations(tail, k - 1), do: [head | combo]
    without_head = combinations(tail, k)
    with_head ++ without_head
  end
end
