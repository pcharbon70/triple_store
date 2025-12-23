defmodule TripleStore.SPARQL.Optimizer do
  @moduledoc """
  SPARQL query optimizer implementing rule-based transformations.

  This module provides optimization passes that transform SPARQL algebra trees
  to improve query execution performance. Optimizations include:

  - **Filter Push-Down**: Push filter expressions closer to data sources
  - **Constant Folding**: Evaluate constant expressions at compile time
  - **BGP Reordering**: Reorder triple patterns based on selectivity estimates

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

  Currently applies constant folding, filter push-down, and BGP reordering.

  ## Options
  - `:push_filters` - Enable filter push-down (default: true)
  - `:fold_constants` - Enable constant folding (default: true)
  - `:reorder_bgp` - Enable BGP pattern reordering (default: true)
  - `:stats` - Statistics map for selectivity estimation (optional)

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?x WHERE { ?x ?p ?o . ?y ?q ?z FILTER(?x > 5) }")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> optimized = TripleStore.SPARQL.Optimizer.optimize(pattern)
      # Filter is pushed closer to the BGP containing ?x

  """
  @spec optimize(algebra(), keyword()) :: algebra()
  def optimize(algebra, opts \\ []) do
    push_filters? = Keyword.get(opts, :push_filters, true)
    fold_constants? = Keyword.get(opts, :fold_constants, true)
    reorder_bgp? = Keyword.get(opts, :reorder_bgp, true)
    stats = Keyword.get(opts, :stats, %{})

    algebra
    |> then(fn a -> if fold_constants?, do: fold_constants(a), else: a end)
    |> then(fn a -> if reorder_bgp?, do: reorder_bgp_patterns(a, stats), else: a end)
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
  # Constant Folding
  # ===========================================================================

  @doc """
  Evaluates constant expressions at compile time.

  This optimization reduces runtime computation by pre-evaluating expressions
  that contain only literal values (no variables). It also simplifies filters
  that are always true or always false.

  ## Optimizations Applied

  1. Evaluate arithmetic on constant operands (1 + 2 → 3)
  2. Evaluate comparisons on constant operands (5 > 3 → true)
  3. Evaluate logical expressions on constants (true && false → false)
  4. Simplify always-true filters (remove the filter)
  5. Simplify always-false filters (replace pattern with empty result)
  6. Evaluate constant function calls where possible

  ## Examples

      # Before:
      Filter(5 > 3, BGP([...]))

      # After (filter removed since always true):
      BGP([...])

      # Before:
      Filter(1 > 10, BGP([...]))

      # After (pattern replaced with empty since always false):
      {:bgp, []}

  """
  @spec fold_constants(algebra()) :: algebra()
  def fold_constants(algebra) do
    fold_constants_recursive(algebra)
  end

  # Recursive constant folding over algebra tree
  defp fold_constants_recursive({:filter, expr, pattern}) do
    folded_pattern = fold_constants_recursive(pattern)
    folded_expr = fold_expression(expr)

    case evaluate_constant_filter(folded_expr) do
      :always_true ->
        # Filter is always true - remove it
        folded_pattern

      :always_false ->
        # Filter is always false - result is empty
        {:bgp, []}

      :not_constant ->
        # Filter contains variables - keep it with folded expression
        {:filter, folded_expr, folded_pattern}
    end
  end

  defp fold_constants_recursive({:join, left, right}) do
    folded_left = fold_constants_recursive(left)
    folded_right = fold_constants_recursive(right)

    # If either side is empty, the join is empty
    case {folded_left, folded_right} do
      {{:bgp, []}, _} -> {:bgp, []}
      {_, {:bgp, []}} -> {:bgp, []}
      _ -> {:join, folded_left, folded_right}
    end
  end

  defp fold_constants_recursive({:left_join, left, right, filter}) do
    folded_left = fold_constants_recursive(left)
    folded_right = fold_constants_recursive(right)
    folded_filter = if filter, do: fold_expression(filter), else: nil

    # If left is empty, the result is empty
    case folded_left do
      {:bgp, []} -> {:bgp, []}
      _ -> {:left_join, folded_left, folded_right, folded_filter}
    end
  end

  defp fold_constants_recursive({:minus, left, right}) do
    {:minus, fold_constants_recursive(left), fold_constants_recursive(right)}
  end

  defp fold_constants_recursive({:union, left, right}) do
    folded_left = fold_constants_recursive(left)
    folded_right = fold_constants_recursive(right)

    # If one side is empty, return the other
    case {folded_left, folded_right} do
      {{:bgp, []}, r} -> r
      {l, {:bgp, []}} -> l
      _ -> {:union, folded_left, folded_right}
    end
  end

  defp fold_constants_recursive({:extend, pattern, var, expr}) do
    folded_pattern = fold_constants_recursive(pattern)
    folded_expr = fold_expression(expr)
    {:extend, folded_pattern, var, folded_expr}
  end

  defp fold_constants_recursive({:group, pattern, vars, aggs}) do
    {:group, fold_constants_recursive(pattern), vars, aggs}
  end

  defp fold_constants_recursive({:project, pattern, vars}) do
    {:project, fold_constants_recursive(pattern), vars}
  end

  defp fold_constants_recursive({:distinct, pattern}) do
    {:distinct, fold_constants_recursive(pattern)}
  end

  defp fold_constants_recursive({:reduced, pattern}) do
    {:reduced, fold_constants_recursive(pattern)}
  end

  defp fold_constants_recursive({:order_by, pattern, conditions}) do
    folded_conds = Enum.map(conditions, fn {dir, expr} -> {dir, fold_expression(expr)} end)
    {:order_by, fold_constants_recursive(pattern), folded_conds}
  end

  defp fold_constants_recursive({:slice, pattern, offset, limit}) do
    {:slice, fold_constants_recursive(pattern), offset, limit}
  end

  defp fold_constants_recursive({:graph, term, pattern}) do
    {:graph, term, fold_constants_recursive(pattern)}
  end

  defp fold_constants_recursive({:service, endpoint, pattern, silent}) do
    {:service, endpoint, fold_constants_recursive(pattern), silent}
  end

  # Leaf nodes
  defp fold_constants_recursive({:bgp, _} = node), do: node
  defp fold_constants_recursive({:values, _, _} = node), do: node
  defp fold_constants_recursive({:path, _, _, _} = node), do: node
  defp fold_constants_recursive(node), do: node

  # ===========================================================================
  # Expression Folding
  # ===========================================================================

  # Fold constants in an expression, returning a simplified expression
  defp fold_expression(expr) do
    case expr do
      # Literals and variables are already in simplest form
      {:variable, _} -> expr
      {:named_node, _} -> expr
      {:blank_node, _} -> expr
      {:literal, _, _} -> expr
      {:literal, _, _, _} -> expr

      # Arithmetic operations
      {:add, left, right} -> fold_binary_arithmetic(:add, left, right)
      {:subtract, left, right} -> fold_binary_arithmetic(:subtract, left, right)
      {:multiply, left, right} -> fold_binary_arithmetic(:multiply, left, right)
      {:divide, left, right} -> fold_binary_arithmetic(:divide, left, right)
      {:unary_minus, arg} -> fold_unary_minus(arg)

      # Comparison operations
      {:equal, left, right} -> fold_comparison(:equal, left, right)
      {:greater, left, right} -> fold_comparison(:greater, left, right)
      {:less, left, right} -> fold_comparison(:less, left, right)
      {:greater_or_equal, left, right} -> fold_comparison(:greater_or_equal, left, right)
      {:less_or_equal, left, right} -> fold_comparison(:less_or_equal, left, right)

      # Logical operations
      {:and, left, right} -> fold_logical_and(left, right)
      {:or, left, right} -> fold_logical_or(left, right)
      {:not, arg} -> fold_logical_not(arg)

      # Conditional expressions
      {:if_expr, cond_expr, then_expr, else_expr} ->
        fold_if_expr(cond_expr, then_expr, else_expr)

      {:coalesce, args} when is_list(args) ->
        fold_coalesce(args)

      # Function calls - try to evaluate if all args are constant
      {:function_call, name, args} ->
        fold_function_call(name, args)

      # Other expressions - just fold children
      {:bound, arg} -> {:bound, fold_expression(arg)}
      {:exists, pattern} -> {:exists, pattern}
      {:in_expr, needle, haystack} ->
        {:in_expr, fold_expression(needle), Enum.map(haystack, &fold_expression/1)}

      # Unknown expressions - return as-is
      _ -> expr
    end
  end

  # Fold binary arithmetic operations
  defp fold_binary_arithmetic(op, left, right) do
    folded_left = fold_expression(left)
    folded_right = fold_expression(right)

    if Expression.is_constant?(folded_left) and Expression.is_constant?(folded_right) do
      case Expression.evaluate({op, folded_left, folded_right}, %{}) do
        {:ok, result} -> result
        :error -> {op, folded_left, folded_right}
      end
    else
      {op, folded_left, folded_right}
    end
  end

  # Fold unary minus
  defp fold_unary_minus(arg) do
    folded = fold_expression(arg)

    if Expression.is_constant?(folded) do
      case Expression.evaluate({:unary_minus, folded}, %{}) do
        {:ok, result} -> result
        :error -> {:unary_minus, folded}
      end
    else
      {:unary_minus, folded}
    end
  end

  # Fold comparison operations
  defp fold_comparison(op, left, right) do
    folded_left = fold_expression(left)
    folded_right = fold_expression(right)

    if Expression.is_constant?(folded_left) and Expression.is_constant?(folded_right) do
      case Expression.evaluate({op, folded_left, folded_right}, %{}) do
        {:ok, result} -> result
        :error -> {op, folded_left, folded_right}
      end
    else
      {op, folded_left, folded_right}
    end
  end

  # Fold logical AND with short-circuit optimization
  defp fold_logical_and(left, right) do
    folded_left = fold_expression(left)
    folded_right = fold_expression(right)

    cond do
      # false && anything = false
      is_false_literal?(folded_left) -> folded_left
      # true && x = x
      is_true_literal?(folded_left) -> folded_right
      # x && false = false
      is_false_literal?(folded_right) -> folded_right
      # x && true = x
      is_true_literal?(folded_right) -> folded_left
      # Both constant - evaluate
      Expression.is_constant?(folded_left) and Expression.is_constant?(folded_right) ->
        case Expression.evaluate({:and, folded_left, folded_right}, %{}) do
          {:ok, result} -> result
          :error -> {:and, folded_left, folded_right}
        end
      true ->
        {:and, folded_left, folded_right}
    end
  end

  # Fold logical OR with short-circuit optimization
  defp fold_logical_or(left, right) do
    folded_left = fold_expression(left)
    folded_right = fold_expression(right)

    cond do
      # true || anything = true
      is_true_literal?(folded_left) -> folded_left
      # false || x = x
      is_false_literal?(folded_left) -> folded_right
      # x || true = true
      is_true_literal?(folded_right) -> folded_right
      # x || false = x
      is_false_literal?(folded_right) -> folded_left
      # Both constant - evaluate
      Expression.is_constant?(folded_left) and Expression.is_constant?(folded_right) ->
        case Expression.evaluate({:or, folded_left, folded_right}, %{}) do
          {:ok, result} -> result
          :error -> {:or, folded_left, folded_right}
        end
      true ->
        {:or, folded_left, folded_right}
    end
  end

  # Fold logical NOT
  defp fold_logical_not(arg) do
    folded = fold_expression(arg)

    cond do
      is_true_literal?(folded) -> make_boolean(false)
      is_false_literal?(folded) -> make_boolean(true)
      # Double negation: NOT(NOT(x)) = x
      match?({:not, _}, folded) ->
        {:not, inner} = folded
        inner
      Expression.is_constant?(folded) ->
        case Expression.evaluate({:not, folded}, %{}) do
          {:ok, result} -> result
          :error -> {:not, folded}
        end
      true ->
        {:not, folded}
    end
  end

  # Fold IF expression
  defp fold_if_expr(cond_expr, then_expr, else_expr) do
    folded_cond = fold_expression(cond_expr)
    folded_then = fold_expression(then_expr)
    folded_else = fold_expression(else_expr)

    cond do
      is_true_literal?(folded_cond) -> folded_then
      is_false_literal?(folded_cond) -> folded_else
      true -> {:if_expr, folded_cond, folded_then, folded_else}
    end
  end

  # Fold COALESCE - return first non-error constant at the start, or simplified list
  defp fold_coalesce(args) do
    folded_args = Enum.map(args, &fold_expression/1)

    # Check if first argument is a constant that evaluates successfully
    case folded_args do
      [first | _] when is_tuple(first) ->
        if Expression.is_constant?(first) do
          case Expression.evaluate(first, %{}) do
            {:ok, _} ->
              # First arg is a valid constant - return it
              first

            :error ->
              # First arg is an error constant - skip it and recurse
              case folded_args do
                [_] -> {:coalesce, folded_args}
                [_ | rest] -> fold_coalesce_remaining(rest)
              end
          end
        else
          # First arg is a variable - can't fold further, but filter out error constants
          simplify_coalesce_args(folded_args)
        end

      _ ->
        {:coalesce, folded_args}
    end
  end

  # Helper to continue folding remaining COALESCE args
  defp fold_coalesce_remaining(args) do
    case args do
      [] -> {:coalesce, []}
      [single] -> single
      _ -> fold_coalesce(args)
    end
  end

  # Simplify COALESCE by removing error constants but preserving order
  defp simplify_coalesce_args(args) do
    # Filter out known-error constants but keep variables and valid constants
    remaining =
      Enum.filter(args, fn arg ->
        not Expression.is_constant?(arg) or
          match?({:ok, _}, Expression.evaluate(arg, %{}))
      end)

    case remaining do
      [] -> {:coalesce, args}
      [single] -> single
      _ -> {:coalesce, remaining}
    end
  end

  # Fold function calls with constant arguments
  defp fold_function_call(name, args) do
    folded_args = Enum.map(args, &fold_expression/1)

    if Enum.all?(folded_args, &Expression.is_constant?/1) do
      case Expression.evaluate({:function_call, name, folded_args}, %{}) do
        {:ok, result} -> result
        _ -> {:function_call, name, folded_args}
      end
    else
      {:function_call, name, folded_args}
    end
  end

  # ===========================================================================
  # Filter Evaluation Helpers
  # ===========================================================================

  # Determine if a filter expression is always true, always false, or variable
  defp evaluate_constant_filter(expr) do
    if Expression.is_constant?(expr) do
      case Expression.evaluate(expr, %{}) do
        {:ok, result} ->
          cond do
            is_true_literal?(result) -> :always_true
            is_false_literal?(result) -> :always_false
            true -> :not_constant
          end

        :error ->
          :not_constant
      end
    else
      :not_constant
    end
  end

  # Check if a term is the boolean true literal
  defp is_true_literal?({:literal, :typed, "true", "http://www.w3.org/2001/XMLSchema#boolean"}),
    do: true

  defp is_true_literal?({:literal, :typed, "1", "http://www.w3.org/2001/XMLSchema#boolean"}),
    do: true

  defp is_true_literal?(_), do: false

  # Check if a term is the boolean false literal
  defp is_false_literal?({:literal, :typed, "false", "http://www.w3.org/2001/XMLSchema#boolean"}),
    do: true

  defp is_false_literal?({:literal, :typed, "0", "http://www.w3.org/2001/XMLSchema#boolean"}),
    do: true

  defp is_false_literal?(_), do: false

  # Create a boolean literal
  defp make_boolean(true),
    do: {:literal, :typed, "true", "http://www.w3.org/2001/XMLSchema#boolean"}

  defp make_boolean(false),
    do: {:literal, :typed, "false", "http://www.w3.org/2001/XMLSchema#boolean"}

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

  # ===========================================================================
  # BGP Pattern Reordering
  # ===========================================================================

  @doc """
  Reorders triple patterns within BGPs based on selectivity estimates.

  This optimization places the most selective patterns first, reducing
  intermediate result sizes during query execution.

  ## Algorithm

  1. Traverse the algebra tree looking for BGP nodes
  2. For each BGP, estimate selectivity of each triple pattern
  3. Sort patterns by selectivity (most selective first)
  4. Consider variable binding propagation between patterns

  ## Selectivity Estimation

  Selectivity is estimated based on:
  - **Bound positions**: Bound subjects/predicates/objects are more selective
  - **Predicate cardinalities**: Known predicates have known frequencies
  - **Variable binding**: Patterns using already-bound variables are more selective

  ## Examples

      # Before:
      BGP([?x ?p ?o], [<Bob> <knows> ?y], [?a ?b ?c])

      # After (bound pattern first):
      BGP([<Bob> <knows> ?y], [?x ?p ?o], [?a ?b ?c])

  """
  @spec reorder_bgp_patterns(algebra(), map()) :: algebra()
  def reorder_bgp_patterns(algebra, stats \\ %{}) do
    reorder_bgp_recursive(algebra, stats)
  end

  # Recursive traversal for BGP reordering
  defp reorder_bgp_recursive({:bgp, patterns}, stats) do
    reordered = reorder_patterns(patterns, stats)
    {:bgp, reordered}
  end

  defp reorder_bgp_recursive({:filter, expr, pattern}, stats) do
    {:filter, expr, reorder_bgp_recursive(pattern, stats)}
  end

  defp reorder_bgp_recursive({:join, left, right}, stats) do
    {:join, reorder_bgp_recursive(left, stats), reorder_bgp_recursive(right, stats)}
  end

  defp reorder_bgp_recursive({:left_join, left, right, filter}, stats) do
    {:left_join,
     reorder_bgp_recursive(left, stats),
     reorder_bgp_recursive(right, stats),
     filter}
  end

  defp reorder_bgp_recursive({:minus, left, right}, stats) do
    {:minus, reorder_bgp_recursive(left, stats), reorder_bgp_recursive(right, stats)}
  end

  defp reorder_bgp_recursive({:union, left, right}, stats) do
    {:union, reorder_bgp_recursive(left, stats), reorder_bgp_recursive(right, stats)}
  end

  defp reorder_bgp_recursive({:extend, pattern, var, expr}, stats) do
    {:extend, reorder_bgp_recursive(pattern, stats), var, expr}
  end

  defp reorder_bgp_recursive({:group, pattern, vars, aggs}, stats) do
    {:group, reorder_bgp_recursive(pattern, stats), vars, aggs}
  end

  defp reorder_bgp_recursive({:project, pattern, vars}, stats) do
    {:project, reorder_bgp_recursive(pattern, stats), vars}
  end

  defp reorder_bgp_recursive({:distinct, pattern}, stats) do
    {:distinct, reorder_bgp_recursive(pattern, stats)}
  end

  defp reorder_bgp_recursive({:reduced, pattern}, stats) do
    {:reduced, reorder_bgp_recursive(pattern, stats)}
  end

  defp reorder_bgp_recursive({:order_by, pattern, conditions}, stats) do
    {:order_by, reorder_bgp_recursive(pattern, stats), conditions}
  end

  defp reorder_bgp_recursive({:slice, pattern, offset, limit}, stats) do
    {:slice, reorder_bgp_recursive(pattern, stats), offset, limit}
  end

  defp reorder_bgp_recursive({:graph, term, pattern}, stats) do
    {:graph, term, reorder_bgp_recursive(pattern, stats)}
  end

  defp reorder_bgp_recursive({:service, endpoint, pattern, silent}, stats) do
    {:service, endpoint, reorder_bgp_recursive(pattern, stats), silent}
  end

  # Leaf nodes and catch-all
  defp reorder_bgp_recursive({:values, _, _} = node, _stats), do: node
  defp reorder_bgp_recursive({:path, _, _, _} = node, _stats), do: node
  defp reorder_bgp_recursive(node, _stats), do: node

  # ===========================================================================
  # Pattern Reordering Logic
  # ===========================================================================

  # Reorder patterns considering selectivity and variable binding propagation
  @spec reorder_patterns(list(), map()) :: list()
  defp reorder_patterns([], _stats), do: []
  defp reorder_patterns([single], _stats), do: [single]

  defp reorder_patterns(patterns, stats) do
    # Use greedy algorithm: repeatedly select the most selective pattern
    # given the variables already bound by previous patterns
    greedy_reorder(patterns, MapSet.new(), stats, [])
  end

  # Greedy reordering: pick most selective pattern at each step
  defp greedy_reorder([], _bound_vars, _stats, acc) do
    Enum.reverse(acc)
  end

  defp greedy_reorder(remaining, bound_vars, stats, acc) do
    # Score each remaining pattern considering bound variables
    scored =
      remaining
      |> Enum.map(fn pattern ->
        score = estimate_selectivity(pattern, bound_vars, stats)
        {pattern, score}
      end)
      |> Enum.sort_by(fn {_pattern, score} -> score end)

    # Pick the most selective (lowest score)
    [{best_pattern, _score} | _rest] = scored
    remaining_patterns = List.delete(remaining, best_pattern)

    # Add variables from selected pattern to bound set
    new_bound = pattern_variables(best_pattern) |> MapSet.union(bound_vars)

    greedy_reorder(remaining_patterns, new_bound, stats, [best_pattern | acc])
  end

  # ===========================================================================
  # Selectivity Estimation
  # ===========================================================================

  @doc """
  Estimates the selectivity of a triple pattern.

  Lower scores indicate more selective patterns (fewer expected results).
  The estimation considers:
  1. Number of bound positions (subject, predicate, object)
  2. Predicate cardinality from statistics (if available)
  3. Whether variables are already bound from previous patterns

  ## Scoring System

  Base scores for position binding:
  - Bound subject: 1.0 (subjects are usually unique identifiers)
  - Bound predicate: 10.0 (predicates have varying cardinalities)
  - Bound object: 5.0 (objects can be unique or repeated)
  - Variable: 1000.0 (completely unbound)

  The final score is: product of position scores / binding bonus

  ## Arguments

  - `pattern` - A triple pattern tuple
  - `bound_vars` - Set of already-bound variable names
  - `stats` - Statistics map with predicate cardinalities

  ## Returns

  A numeric selectivity score (lower = more selective)
  """
  @spec estimate_selectivity(tuple(), MapSet.t(), map()) :: float()
  def estimate_selectivity(pattern, bound_vars \\ MapSet.new(), stats \\ %{})

  def estimate_selectivity({:triple, subject, predicate, object}, bound_vars, stats) do
    # Score each position
    s_score = position_score(subject, bound_vars, :subject, stats)
    p_score = position_score(predicate, bound_vars, :predicate, stats)
    o_score = position_score(object, bound_vars, :object, stats)

    # Combine scores - multiplicative model
    # Lower score = more selective
    s_score * p_score * o_score
  end

  # Fallback for non-triple patterns
  def estimate_selectivity(_pattern, _bound_vars, _stats), do: 1_000_000.0

  # Score a single position in a triple pattern
  defp position_score(term, bound_vars, position, stats) do
    case term do
      # Bound term (literal or IRI) - very selective
      {:named_node, uri} ->
        case position do
          :predicate -> predicate_selectivity(uri, stats)
          :subject -> 1.0
          :object -> 5.0
        end

      {:literal, _, _} ->
        # Literals are usually selective, especially in object position
        2.0

      {:literal, _, _, _} ->
        # Typed literals
        2.0

      {:blank_node, _} ->
        # Blank nodes are specific within a graph
        3.0

      # Variable - check if already bound
      {:variable, name} ->
        if MapSet.member?(bound_vars, name) do
          # Variable is bound from previous pattern - very selective
          1.0
        else
          # Unbound variable - not selective
          case position do
            :subject -> 100.0
            :predicate -> 50.0
            :object -> 100.0
          end
        end

      # Unknown term type
      _ ->
        1000.0
    end
  end

  # Get selectivity score for a predicate based on statistics
  defp predicate_selectivity(predicate_uri, stats) do
    # Check if we have cardinality info for this predicate
    case Map.get(stats, {:predicate_count, predicate_uri}) do
      nil ->
        # No stats available - use default predicate score
        10.0

      count when count < 10 ->
        # Very rare predicate - very selective
        0.5

      count when count < 100 ->
        # Uncommon predicate
        2.0

      count when count < 1000 ->
        # Moderately common predicate
        10.0

      count when count < 10000 ->
        # Common predicate
        50.0

      _ ->
        # Very common predicate (like rdf:type)
        100.0
    end
  end

  # Extract variable names from a triple pattern
  defp pattern_variables({:triple, subject, predicate, object}) do
    [subject, predicate, object]
    |> Enum.flat_map(&term_variables/1)
    |> MapSet.new()
  end

  defp pattern_variables(_), do: MapSet.new()

  defp term_variables({:variable, name}), do: [name]
  defp term_variables(_), do: []
end
