defmodule TripleStore.SPARQL.Optimizer do
  @moduledoc """
  SPARQL query optimizer implementing rule-based transformations.

  This module provides optimization passes that transform SPARQL algebra trees
  to improve query execution performance. Optimizations include:

  - **Filter Push-Down**: Push filter expressions closer to data sources
  - **Constant Folding**: Evaluate constant expressions at compile time (future)
  - **BGP Reordering**: Reorder triple patterns based on selectivity (future)

  ## Filter Push-Down

  Filter push-down moves FILTER expressions as close to their data sources as
  possible, reducing the number of intermediate results that need to be processed.

  ### Rules

  1. A filter can be pushed past a JOIN if all its variables are defined by one side
  2. Filters are NOT pushed into OPTIONAL (left_join) right sides to preserve semantics
  3. Conjunctive filters (AND) can be split and pushed independently
  4. Filters are NOT pushed past UNION (would change semantics)

  ## Examples

      # Before optimization:
      Filter(?x > 5,
        Join(
          BGP([?x ?p ?o]),
          BGP([?y ?q ?z])
        )
      )

      # After optimization (filter pushed to left side):
      Join(
        Filter(?x > 5,
          BGP([?x ?p ?o])
        ),
        BGP([?y ?q ?z])
      )

  """

  alias TripleStore.SPARQL.Algebra
  alias TripleStore.SPARQL.Expression

  @type algebra :: Algebra.t()

  # ===========================================================================
  # Main Entry Point
  # ===========================================================================

  @doc """
  Applies all optimizations to an algebra tree.

  Currently applies filter push-down. Additional optimizations will be added
  in future tasks.

  ## Options
  - `:push_filters` - Enable filter push-down (default: true)

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?x WHERE { ?x ?p ?o . ?y ?q ?z FILTER(?x > 5) }")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> optimized = TripleStore.SPARQL.Optimizer.optimize(pattern)
      # Filter is pushed closer to the BGP containing ?x

  """
  @spec optimize(algebra(), keyword()) :: algebra()
  def optimize(algebra, opts \\ []) do
    push_filters? = Keyword.get(opts, :push_filters, true)

    algebra
    |> then(fn a -> if push_filters?, do: push_filters_down(a), else: a end)
  end

  # ===========================================================================
  # Filter Push-Down
  # ===========================================================================

  @doc """
  Pushes filter expressions as close to their data sources as possible.

  This optimization reduces intermediate result sizes by filtering earlier
  in query execution.

  ## Algorithm

  1. Traverse the algebra tree top-down looking for filter nodes
  2. For each filter, extract its required variables
  3. Try to push the filter past joins if variables allow
  4. Split conjunctive (AND) filters and push each part independently
  5. Stop at OPTIONAL boundaries (cannot change semantics)

  ## Examples

      iex> bgp = {:bgp, [{:triple, {:variable, "x"}, {:variable, "p"}, {:variable, "o"}}]}
      iex> algebra = {:filter, {:greater, {:variable, "x"}, {:literal, :typed, "5", "http://www.w3.org/2001/XMLSchema#integer"}}, bgp}
      iex> TripleStore.SPARQL.Optimizer.push_filters_down(algebra)
      # Returns the same since filter is already at the bottom

  """
  @spec push_filters_down(algebra()) :: algebra()
  def push_filters_down(algebra) do
    push_filters_recursive(algebra)
  end

  # ===========================================================================
  # Filter Push-Down Implementation
  # ===========================================================================

  # Main recursive traversal - processes nodes bottom-up then applies push-down
  defp push_filters_recursive({:filter, expr, pattern}) do
    # First, recursively optimize the inner pattern
    optimized_pattern = push_filters_recursive(pattern)

    # Split conjunctive filters and push each part
    split_and_push_filter(expr, optimized_pattern)
  end

  defp push_filters_recursive({:join, left, right}) do
    {:join, push_filters_recursive(left), push_filters_recursive(right)}
  end

  defp push_filters_recursive({:left_join, left, right, filter}) do
    # Optimize left side, but be careful with right side
    # Filters in OPTIONAL can be pushed into the right side if they only
    # reference right-side variables, but this is complex - for now just recurse
    optimized_left = push_filters_recursive(left)
    optimized_right = push_filters_recursive(right)
    {:left_join, optimized_left, optimized_right, filter}
  end

  defp push_filters_recursive({:minus, left, right}) do
    {:minus, push_filters_recursive(left), push_filters_recursive(right)}
  end

  defp push_filters_recursive({:union, left, right}) do
    # Don't push filters into UNION - it changes semantics
    {:union, push_filters_recursive(left), push_filters_recursive(right)}
  end

  defp push_filters_recursive({:extend, pattern, var, expr}) do
    {:extend, push_filters_recursive(pattern), var, expr}
  end

  defp push_filters_recursive({:group, pattern, vars, aggs}) do
    {:group, push_filters_recursive(pattern), vars, aggs}
  end

  defp push_filters_recursive({:project, pattern, vars}) do
    {:project, push_filters_recursive(pattern), vars}
  end

  defp push_filters_recursive({:distinct, pattern}) do
    {:distinct, push_filters_recursive(pattern)}
  end

  defp push_filters_recursive({:reduced, pattern}) do
    {:reduced, push_filters_recursive(pattern)}
  end

  defp push_filters_recursive({:order_by, pattern, conditions}) do
    {:order_by, push_filters_recursive(pattern), conditions}
  end

  defp push_filters_recursive({:slice, pattern, offset, limit}) do
    {:slice, push_filters_recursive(pattern), offset, limit}
  end

  defp push_filters_recursive({:graph, term, pattern}) do
    {:graph, term, push_filters_recursive(pattern)}
  end

  defp push_filters_recursive({:service, endpoint, pattern, silent}) do
    {:service, endpoint, push_filters_recursive(pattern), silent}
  end

  # Leaf nodes - no recursion needed
  defp push_filters_recursive({:bgp, _} = node), do: node
  defp push_filters_recursive({:values, _, _} = node), do: node
  defp push_filters_recursive({:path, _, _, _} = node), do: node

  # Catch-all for any other nodes
  defp push_filters_recursive(node), do: node

  # ===========================================================================
  # Conjunctive Filter Splitting
  # ===========================================================================

  # Split AND expressions and try to push each part independently
  defp split_and_push_filter(expr, pattern) do
    # Extract all conjuncts from nested ANDs
    conjuncts = extract_conjuncts(expr)

    # Try to push each conjunct down
    {pushed, remaining} = push_conjuncts(conjuncts, pattern)

    # Rebuild the pattern with pushed filters and wrap remaining filters
    result = pushed

    case remaining do
      [] -> result
      [single] -> {:filter, single, result}
      multiple -> {:filter, combine_conjuncts(multiple), result}
    end
  end

  # Extract all parts of a conjunctive expression
  defp extract_conjuncts({:and, left, right}) do
    extract_conjuncts(left) ++ extract_conjuncts(right)
  end

  defp extract_conjuncts(expr), do: [expr]

  # Combine conjuncts back into AND expressions
  defp combine_conjuncts([single]), do: single

  defp combine_conjuncts([first | rest]) do
    Enum.reduce(rest, first, fn expr, acc -> {:and, acc, expr} end)
  end

  # Try to push each conjunct as far down as possible
  defp push_conjuncts(conjuncts, pattern) do
    Enum.reduce(conjuncts, {pattern, []}, fn conjunct, {current_pattern, remaining} ->
      case try_push_filter(conjunct, current_pattern) do
        {:pushed, new_pattern} ->
          {new_pattern, remaining}

        :cannot_push ->
          {current_pattern, [conjunct | remaining]}
      end
    end)
    |> then(fn {pattern, remaining} -> {pattern, Enum.reverse(remaining)} end)
  end

  # ===========================================================================
  # Filter Pushing Logic
  # ===========================================================================

  # Try to push a single filter expression into a pattern
  # Dispatches to specialized handlers based on pattern type
  defp try_push_filter(filter_expr, {:join, left, right}) do
    filter_vars = Expression.expression_variables(filter_expr) |> MapSet.new()
    push_into_join(filter_expr, filter_vars, left, right)
  end

  defp try_push_filter(filter_expr, {:left_join, left, right, join_filter}) do
    filter_vars = Expression.expression_variables(filter_expr) |> MapSet.new()
    push_into_left_join(filter_expr, filter_vars, left, right, join_filter)
  end

  defp try_push_filter(filter_expr, {:bgp, _} = pattern) do
    push_into_bgp(filter_expr, pattern)
  end

  defp try_push_filter(filter_expr, {:filter, existing_expr, inner}) do
    push_into_filter(filter_expr, existing_expr, inner)
  end

  defp try_push_filter(filter_expr, {:extend, inner, var, expr}) do
    push_into_extend(filter_expr, inner, var, expr)
  end

  defp try_push_filter(_filter_expr, {:union, _, _}), do: :cannot_push
  defp try_push_filter(_filter_expr, {:minus, _, _}), do: :cannot_push
  defp try_push_filter(_filter_expr, {:group, _, _, _}), do: :cannot_push

  defp try_push_filter(filter_expr, {:graph, term, inner}) do
    push_through_wrapper(filter_expr, inner, fn new_inner -> {:graph, term, new_inner} end)
  end

  defp try_push_filter(filter_expr, {:project, inner, vars}) do
    push_through_wrapper(filter_expr, inner, fn new_inner -> {:project, new_inner, vars} end)
  end

  defp try_push_filter(filter_expr, {:distinct, inner}) do
    push_through_wrapper(filter_expr, inner, fn new_inner -> {:distinct, new_inner} end)
  end

  defp try_push_filter(filter_expr, {:reduced, inner}) do
    push_through_wrapper(filter_expr, inner, fn new_inner -> {:reduced, new_inner} end)
  end

  defp try_push_filter(filter_expr, {:order_by, inner, conditions}) do
    push_through_wrapper(filter_expr, inner, fn new_inner -> {:order_by, new_inner, conditions} end)
  end

  defp try_push_filter(filter_expr, {:slice, inner, offset, limit}) do
    push_through_wrapper(filter_expr, inner, fn new_inner -> {:slice, new_inner, offset, limit} end)
  end

  defp try_push_filter(_filter_expr, _pattern), do: :cannot_push

  # Push filter into a BGP - this is as far as we can go
  defp push_into_bgp(filter_expr, pattern) do
    filter_vars = Expression.expression_variables(filter_expr) |> MapSet.new()
    pattern_vars = pattern_variable_names(pattern)

    if MapSet.subset?(filter_vars, pattern_vars) do
      {:pushed, {:filter, filter_expr, pattern}}
    else
      :cannot_push
    end
  end

  # Push past or merge with existing filter
  defp push_into_filter(filter_expr, existing_expr, inner) do
    case try_push_filter(filter_expr, inner) do
      {:pushed, new_inner} -> {:pushed, {:filter, existing_expr, new_inner}}
      :cannot_push -> {:pushed, {:filter, {:and, filter_expr, existing_expr}, inner}}
    end
  end

  # Push past extend if filter doesn't use the extended variable
  defp push_into_extend(filter_expr, inner, var, expr) do
    filter_vars = Expression.expression_variables(filter_expr) |> MapSet.new()

    extended_var = case var do
      {:variable, name} -> name
      _ -> nil
    end

    if extended_var && extended_var in filter_vars do
      :cannot_push
    else
      push_through_wrapper(filter_expr, inner, fn new_inner -> {:extend, new_inner, var, expr} end)
    end
  end

  # Generic helper to push filter through a wrapper node
  defp push_through_wrapper(filter_expr, inner, rebuild_fn) do
    case try_push_filter(filter_expr, inner) do
      {:pushed, new_inner} -> {:pushed, rebuild_fn.(new_inner)}
      :cannot_push -> :cannot_push
    end
  end

  # Extract variable names from a pattern as a MapSet
  defp pattern_variable_names(pattern) do
    Algebra.variables(pattern)
    |> Enum.map(fn {:variable, n} -> n end)
    |> MapSet.new()
  end

  # Push filter into a join - try left side first, then right
  defp push_into_join(filter_expr, filter_vars, left, right) do
    left_vars = pattern_variable_names(left)
    right_vars = pattern_variable_names(right)

    cond do
      MapSet.subset?(filter_vars, left_vars) ->
        push_to_join_side(filter_expr, left, right, :left)

      MapSet.subset?(filter_vars, right_vars) ->
        push_to_join_side(filter_expr, left, right, :right)

      true ->
        :cannot_push
    end
  end

  # Helper to push filter to a specific side of join
  defp push_to_join_side(filter_expr, left, right, :left) do
    case try_push_filter(filter_expr, left) do
      {:pushed, new_left} -> {:pushed, {:join, new_left, right}}
      :cannot_push -> {:pushed, {:join, {:filter, filter_expr, left}, right}}
    end
  end

  defp push_to_join_side(filter_expr, left, right, :right) do
    case try_push_filter(filter_expr, right) do
      {:pushed, new_right} -> {:pushed, {:join, left, new_right}}
      :cannot_push -> {:pushed, {:join, left, {:filter, filter_expr, right}}}
    end
  end

  # Push filter into a left join (OPTIONAL)
  # CRITICAL: We can only push into the LEFT side, not the right side
  defp push_into_left_join(filter_expr, filter_vars, left, right, join_filter) do
    left_vars = pattern_variable_names(left)
    right_vars = pattern_variable_names(right)

    cond do
      MapSet.subset?(filter_vars, left_vars) ->
        push_to_left_join_left(filter_expr, left, right, join_filter)

      # Filter uses right-side variables - CANNOT push into OPTIONAL
      not MapSet.disjoint?(filter_vars, right_vars) ->
        :cannot_push

      true ->
        :cannot_push
    end
  end

  defp push_to_left_join_left(filter_expr, left, right, join_filter) do
    case try_push_filter(filter_expr, left) do
      {:pushed, new_left} ->
        {:pushed, {:left_join, new_left, right, join_filter}}

      :cannot_push ->
        {:pushed, {:left_join, {:filter, filter_expr, left}, right, join_filter}}
    end
  end

  # ===========================================================================
  # Analysis Utilities
  # ===========================================================================

  @doc """
  Returns statistics about filter push-down opportunities in an algebra tree.

  Useful for debugging and understanding optimization behavior.
  """
  @spec analyze_filters(algebra()) :: %{
          total_filters: non_neg_integer(),
          pushable_filters: non_neg_integer(),
          blocked_by_optional: non_neg_integer(),
          blocked_by_union: non_neg_integer()
        }
  def analyze_filters(algebra) do
    analyze_filters_recursive(algebra, %{
      total_filters: 0,
      pushable_filters: 0,
      blocked_by_optional: 0,
      blocked_by_union: 0
    })
  end

  defp analyze_filters_recursive({:filter, _expr, pattern}, stats) do
    stats = %{stats | total_filters: stats.total_filters + 1}
    analyze_filters_recursive(pattern, stats)
  end

  defp analyze_filters_recursive({:join, left, right}, stats) do
    stats
    |> then(&analyze_filters_recursive(left, &1))
    |> then(&analyze_filters_recursive(right, &1))
  end

  defp analyze_filters_recursive({:left_join, left, right, _}, stats) do
    stats
    |> then(&analyze_filters_recursive(left, &1))
    |> then(&analyze_filters_recursive(right, &1))
  end

  defp analyze_filters_recursive({:union, left, right}, stats) do
    stats
    |> then(&analyze_filters_recursive(left, &1))
    |> then(&analyze_filters_recursive(right, &1))
  end

  defp analyze_filters_recursive({tag, pattern}, stats)
       when tag in [:distinct, :reduced] do
    analyze_filters_recursive(pattern, stats)
  end

  defp analyze_filters_recursive({tag, pattern, _}, stats)
       when tag in [:project, :graph] do
    analyze_filters_recursive(pattern, stats)
  end

  defp analyze_filters_recursive({:extend, pattern, _, _}, stats) do
    analyze_filters_recursive(pattern, stats)
  end

  defp analyze_filters_recursive({:group, pattern, _, _}, stats) do
    analyze_filters_recursive(pattern, stats)
  end

  defp analyze_filters_recursive({:order_by, pattern, _}, stats) do
    analyze_filters_recursive(pattern, stats)
  end

  defp analyze_filters_recursive({:slice, pattern, _, _}, stats) do
    analyze_filters_recursive(pattern, stats)
  end

  defp analyze_filters_recursive(_, stats), do: stats
end
