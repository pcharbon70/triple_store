defmodule TripleStore.SPARQL.Optimizer do
  @moduledoc """
  SPARQL query optimizer implementing rule-based transformations.

  @dialyzer directives are used to suppress opaque type warnings that occur
  when Dialyzer traces through MapSet construction in private functions.
  This is a known issue with Dialyzer and opaque types.

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

  require Logger

  @type algebra :: Algebra.t()

  # Maximum recursion depth to prevent stack overflow from deeply nested queries
  @max_depth 100

  @typedoc "Optimization options"
  @type opt ::
          {:push_filters, boolean()}
          | {:fold_constants, boolean()}
          | {:reorder_bgp, boolean()}
          | {:stats, map()}
          | {:log, boolean()}
          | {:explain, boolean()}
          | {:range_indexed_predicates, MapSet.t()}

  # ===========================================================================
  # Main Entry Point
  # ===========================================================================

  @doc """
  Applies all optimizations to an algebra tree.

  Runs the optimization pipeline in this order:
  1. Constant folding - Evaluate constant expressions at compile time
  2. BGP reordering - Reorder triple patterns by selectivity
  3. Filter push-down - Push filters closer to data sources

  ## Options
  - `:push_filters` - Enable filter push-down (default: true)
  - `:fold_constants` - Enable constant folding (default: true)
  - `:reorder_bgp` - Enable BGP pattern reordering (default: true)
  - `:stats` - Statistics map for selectivity estimation (optional)
  - `:log` - Enable debug logging of optimization steps (default: false)
  - `:explain` - Return explanation instead of optimizing (default: false)

  ## Examples

      iex> {:ok, ast} = TripleStore.SPARQL.Parser.parse("SELECT ?x WHERE { ?x ?p ?o . ?y ?q ?z FILTER(?x > 5) }")
      iex> {:ok, pattern} = TripleStore.SPARQL.Algebra.extract_pattern(ast)
      iex> optimized = TripleStore.SPARQL.Optimizer.optimize(pattern)
      # Filter is pushed closer to the BGP containing ?x

      # With logging enabled:
      iex> optimized = TripleStore.SPARQL.Optimizer.optimize(pattern, log: true)
      # Logs each optimization pass and changes made

      # Get explanation without optimizing:
      iex> {:explain, info} = TripleStore.SPARQL.Optimizer.optimize(pattern, explain: true)
      # Returns optimization analysis without modifying the algebra

  """
  @spec optimize(algebra(), [opt()]) :: algebra() | {:explain, map()}
  def optimize(algebra, opts \\ []) do
    explain? = Keyword.get(opts, :explain, false)

    if explain? do
      explain(algebra, opts)
    else
      run_pipeline(algebra, opts)
    end
  end

  # Run the full optimization pipeline
  defp run_pipeline(algebra, opts) do
    push_filters? = Keyword.get(opts, :push_filters, true)
    fold_constants? = Keyword.get(opts, :fold_constants, true)
    reorder_bgp? = Keyword.get(opts, :reorder_bgp, true)
    stats = Keyword.get(opts, :stats, %{})
    log? = Keyword.get(opts, :log, false)
    range_indexed = Keyword.get(opts, :range_indexed_predicates, MapSet.new())

    if log?, do: log_start(algebra)

    # Build optimization context with filter information
    filter_context = extract_range_filters(algebra)

    enriched_stats =
      Map.merge(stats, %{filter_context: filter_context, range_indexed: range_indexed})

    start_time = System.monotonic_time()

    result =
      algebra
      |> run_pass(:constant_folding, fold_constants?, fn a -> fold_constants(a) end, log?)
      |> run_pass(
        :bgp_reordering,
        reorder_bgp?,
        fn a -> reorder_bgp_patterns(a, enriched_stats) end,
        log?
      )
      |> run_pass(:filter_push_down, push_filters?, fn a -> push_filters_down(a) end, log?)
      |> tap(fn r -> if log?, do: log_complete(r) end)

    # Emit telemetry for optimization (C1 from review)
    duration = System.monotonic_time() - start_time

    :telemetry.execute(
      [:triple_store, :sparql, :optimizer, :complete],
      %{
        duration: duration,
        passes: count_enabled_passes(fold_constants?, reorder_bgp?, push_filters?)
      },
      %{
        changed: algebra != result,
        filter_push_down: push_filters?,
        constant_folding: fold_constants?,
        bgp_reordering: reorder_bgp?
      }
    )

    result
  end

  defp count_enabled_passes(fold?, reorder?, push?) do
    Enum.count([fold?, reorder?, push?], & &1)
  end

  # Run a single optimization pass with optional logging
  defp run_pass(algebra, _name, false, _fun, _log?), do: algebra

  defp run_pass(algebra, name, true, fun, log?) do
    result = fun.(algebra)

    if log? do
      changed = algebra != result
      log_pass(name, changed)
    end

    result
  end

  # ===========================================================================
  # Explain Mode
  # ===========================================================================

  @doc """
  Analyzes an algebra tree and returns optimization opportunities without applying them.

  This is useful for understanding what optimizations would be applied and why,
  without actually modifying the query.

  ## Returns

  A map containing:
  - `:original` - The original algebra tree
  - `:optimizations` - List of optimizations that would be applied
  - `:statistics` - Analysis statistics (filters, patterns, etc.)
  - `:estimated_improvement` - Rough estimate of improvement potential

  ## Examples

      iex> {:explain, info} = Optimizer.optimize(algebra, explain: true)
      iex> info.optimizations
      [:constant_folding, :bgp_reordering, :filter_push_down]

  """
  @spec explain(algebra(), keyword()) :: {:explain, map()}
  def explain(algebra, opts \\ []) do
    stats = Keyword.get(opts, :stats, %{})

    # Analyze the algebra tree
    filter_stats = analyze_filters(algebra)
    bgp_stats = analyze_bgp_patterns(algebra)

    # Determine which optimizations would apply
    optimizations = determine_applicable_optimizations(algebra, filter_stats, bgp_stats, opts)

    # Calculate potential improvement
    improvement = estimate_improvement(filter_stats, bgp_stats)

    {:explain,
     %{
       original: algebra,
       optimizations: optimizations,
       statistics: %{
         filters: filter_stats,
         bgp_patterns: bgp_stats,
         predicate_stats_available: map_size(stats) > 0
       },
       estimated_improvement: improvement
     }}
  end

  # Determine which optimizations would actually change the algebra
  defp determine_applicable_optimizations(algebra, filter_stats, bgp_stats, opts) do
    push_filters? = Keyword.get(opts, :push_filters, true)
    fold_constants? = Keyword.get(opts, :fold_constants, true)
    reorder_bgp? = Keyword.get(opts, :reorder_bgp, true)

    optimizations = []

    # Check constant folding
    optimizations =
      if fold_constants? and has_foldable_constants?(algebra) do
        [:constant_folding | optimizations]
      else
        optimizations
      end

    # Check BGP reordering
    optimizations =
      if reorder_bgp? and bgp_stats.multi_pattern_bgps > 0 do
        [:bgp_reordering | optimizations]
      else
        optimizations
      end

    # Check filter push-down
    optimizations =
      if push_filters? and filter_stats.total_filters > 0 do
        [:filter_push_down | optimizations]
      else
        optimizations
      end

    Enum.reverse(optimizations)
  end

  # Estimate potential improvement from optimizations
  defp estimate_improvement(filter_stats, bgp_stats) do
    filter_improvement = if filter_stats.total_filters > 0, do: :moderate, else: :none
    bgp_improvement = if bgp_stats.multi_pattern_bgps > 0, do: :moderate, else: :none

    cond do
      filter_improvement == :moderate and bgp_improvement == :moderate -> :high
      filter_improvement == :moderate or bgp_improvement == :moderate -> :moderate
      true -> :low
    end
  end

  # Check if algebra contains constants that could be folded
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp has_foldable_constants?(algebra) do
    case algebra do
      {:filter, expr, pattern} ->
        has_constant_expression?(expr) or has_foldable_constants?(pattern)

      {:extend, pattern, _var, expr} ->
        has_constant_expression?(expr) or has_foldable_constants?(pattern)

      {:join, left, right} ->
        has_foldable_constants?(left) or has_foldable_constants?(right)

      {:left_join, left, right, filter} ->
        has_foldable_constants?(left) or has_foldable_constants?(right) or
          (filter != nil and has_constant_expression?(filter))

      {:union, left, right} ->
        has_foldable_constants?(left) or has_foldable_constants?(right)

      {:project, pattern, _} ->
        has_foldable_constants?(pattern)

      {:distinct, pattern} ->
        has_foldable_constants?(pattern)

      {:order_by, pattern, conditions} ->
        has_foldable_constants?(pattern) or
          Enum.any?(conditions, fn {_dir, expr} -> has_constant_expression?(expr) end)

      _ ->
        false
    end
  end

  # Check if expression contains constant sub-expressions
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp has_constant_expression?(expr) do
    case expr do
      {:add, left, right} ->
        (Expression.constant?(left) and Expression.constant?(right)) or
          has_constant_expression?(left) or has_constant_expression?(right)

      {:subtract, left, right} ->
        (Expression.constant?(left) and Expression.constant?(right)) or
          has_constant_expression?(left) or has_constant_expression?(right)

      {:multiply, left, right} ->
        (Expression.constant?(left) and Expression.constant?(right)) or
          has_constant_expression?(left) or has_constant_expression?(right)

      {:divide, left, right} ->
        (Expression.constant?(left) and Expression.constant?(right)) or
          has_constant_expression?(left) or has_constant_expression?(right)

      {:and, left, right} ->
        (Expression.constant?(left) and Expression.constant?(right)) or
          has_constant_expression?(left) or has_constant_expression?(right)

      {:or, left, right} ->
        (Expression.constant?(left) and Expression.constant?(right)) or
          has_constant_expression?(left) or has_constant_expression?(right)

      {:not, arg} ->
        Expression.constant?(arg) or has_constant_expression?(arg)

      {:equal, left, right} ->
        Expression.constant?(left) and Expression.constant?(right)

      {:greater, left, right} ->
        Expression.constant?(left) and Expression.constant?(right)

      {:less, left, right} ->
        Expression.constant?(left) and Expression.constant?(right)

      _ ->
        false
    end
  end

  # Analyze BGP patterns in the algebra tree
  defp analyze_bgp_patterns(algebra) do
    analyze_bgp_recursive(algebra, %{
      total_bgps: 0,
      total_patterns: 0,
      multi_pattern_bgps: 0,
      max_patterns_in_bgp: 0
    })
  end

  defp analyze_bgp_recursive({:bgp, patterns}, stats) do
    count = length(patterns)

    %{
      stats
      | total_bgps: stats.total_bgps + 1,
        total_patterns: stats.total_patterns + count,
        multi_pattern_bgps: stats.multi_pattern_bgps + if(count > 1, do: 1, else: 0),
        max_patterns_in_bgp: max(stats.max_patterns_in_bgp, count)
    }
  end

  defp analyze_bgp_recursive({:join, left, right}, stats) do
    stats
    |> then(&analyze_bgp_recursive(left, &1))
    |> then(&analyze_bgp_recursive(right, &1))
  end

  defp analyze_bgp_recursive({:left_join, left, right, _}, stats) do
    stats
    |> then(&analyze_bgp_recursive(left, &1))
    |> then(&analyze_bgp_recursive(right, &1))
  end

  defp analyze_bgp_recursive({:union, left, right}, stats) do
    stats
    |> then(&analyze_bgp_recursive(left, &1))
    |> then(&analyze_bgp_recursive(right, &1))
  end

  defp analyze_bgp_recursive({:filter, _, pattern}, stats) do
    analyze_bgp_recursive(pattern, stats)
  end

  defp analyze_bgp_recursive({:project, pattern, _}, stats) do
    analyze_bgp_recursive(pattern, stats)
  end

  defp analyze_bgp_recursive({:distinct, pattern}, stats) do
    analyze_bgp_recursive(pattern, stats)
  end

  defp analyze_bgp_recursive({:order_by, pattern, _}, stats) do
    analyze_bgp_recursive(pattern, stats)
  end

  defp analyze_bgp_recursive({:extend, pattern, _, _}, stats) do
    analyze_bgp_recursive(pattern, stats)
  end

  defp analyze_bgp_recursive(_, stats), do: stats

  # ===========================================================================
  # Logging Helpers
  # ===========================================================================

  defp log_start(algebra) do
    node_count = count_nodes(algebra)
    root_type = elem(algebra, 0)

    Logger.debug(
      "[Optimizer] Starting optimization pipeline (nodes: #{node_count}, root_type: #{inspect(root_type)})"
    )
  end

  defp log_pass(name, changed) do
    if changed do
      Logger.debug("[Optimizer] Pass #{name} made changes")
    else
      Logger.debug("[Optimizer] Pass #{name} - no changes")
    end
  end

  defp log_complete(result) do
    node_count = count_nodes(result)
    root_type = elem(result, 0)

    Logger.debug(
      "[Optimizer] Optimization complete (nodes: #{node_count}, root_type: #{inspect(root_type)})"
    )
  end

  # Count nodes in algebra tree
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp count_nodes(algebra) do
    case algebra do
      {:bgp, patterns} -> 1 + length(patterns)
      {:filter, _, pattern} -> 1 + count_nodes(pattern)
      {:join, left, right} -> 1 + count_nodes(left) + count_nodes(right)
      {:left_join, left, right, _} -> 1 + count_nodes(left) + count_nodes(right)
      {:union, left, right} -> 1 + count_nodes(left) + count_nodes(right)
      {:project, pattern, _} -> 1 + count_nodes(pattern)
      {:distinct, pattern} -> 1 + count_nodes(pattern)
      {:order_by, pattern, _} -> 1 + count_nodes(pattern)
      {:extend, pattern, _, _} -> 1 + count_nodes(pattern)
      {:group, pattern, _, _} -> 1 + count_nodes(pattern)
      {:graph, _, pattern} -> 1 + count_nodes(pattern)
      {:slice, pattern, _, _} -> 1 + count_nodes(pattern)
      _ -> 1
    end
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
    push_filters_recursive(algebra, 0)
  end

  # ===========================================================================
  # Filter Push-Down Implementation
  # ===========================================================================

  # Main recursive traversal - processes nodes bottom-up then applies push-down
  # Includes depth limiting to prevent stack overflow from deeply nested queries
  defp push_filters_recursive(_algebra, depth) when depth > @max_depth do
    raise ArgumentError,
          "Query too deeply nested (max depth: #{@max_depth}). " <>
            "This may indicate a malformed query or an attack."
  end

  defp push_filters_recursive({:filter, expr, pattern}, depth) do
    # First, recursively optimize the inner pattern
    optimized_pattern = push_filters_recursive(pattern, depth + 1)

    # Split conjunctive filters and push each part
    split_and_push_filter(expr, optimized_pattern)
  end

  defp push_filters_recursive({:join, left, right}, depth) do
    {:join, push_filters_recursive(left, depth + 1), push_filters_recursive(right, depth + 1)}
  end

  defp push_filters_recursive({:left_join, left, right, filter}, depth) do
    # Optimize left side, but be careful with right side
    # Filters in OPTIONAL can be pushed into the right side if they only
    # reference right-side variables, but this is complex - for now just recurse
    optimized_left = push_filters_recursive(left, depth + 1)
    optimized_right = push_filters_recursive(right, depth + 1)
    {:left_join, optimized_left, optimized_right, filter}
  end

  defp push_filters_recursive({:minus, left, right}, depth) do
    {:minus, push_filters_recursive(left, depth + 1), push_filters_recursive(right, depth + 1)}
  end

  defp push_filters_recursive({:union, left, right}, depth) do
    # Don't push filters into UNION - it changes semantics
    {:union, push_filters_recursive(left, depth + 1), push_filters_recursive(right, depth + 1)}
  end

  defp push_filters_recursive({:extend, pattern, var, expr}, depth) do
    {:extend, push_filters_recursive(pattern, depth + 1), var, expr}
  end

  defp push_filters_recursive({:group, pattern, vars, aggs}, depth) do
    {:group, push_filters_recursive(pattern, depth + 1), vars, aggs}
  end

  defp push_filters_recursive({:project, pattern, vars}, depth) do
    {:project, push_filters_recursive(pattern, depth + 1), vars}
  end

  defp push_filters_recursive({:distinct, pattern}, depth) do
    {:distinct, push_filters_recursive(pattern, depth + 1)}
  end

  defp push_filters_recursive({:reduced, pattern}, depth) do
    {:reduced, push_filters_recursive(pattern, depth + 1)}
  end

  defp push_filters_recursive({:order_by, pattern, conditions}, depth) do
    {:order_by, push_filters_recursive(pattern, depth + 1), conditions}
  end

  defp push_filters_recursive({:slice, pattern, offset, limit}, depth) do
    {:slice, push_filters_recursive(pattern, depth + 1), offset, limit}
  end

  defp push_filters_recursive({:graph, term, pattern}, depth) do
    {:graph, term, push_filters_recursive(pattern, depth + 1)}
  end

  defp push_filters_recursive({:service, endpoint, pattern, silent}, depth) do
    {:service, endpoint, push_filters_recursive(pattern, depth + 1), silent}
  end

  # Leaf nodes - no recursion needed
  defp push_filters_recursive({:bgp, _} = node, _depth), do: node
  defp push_filters_recursive({:values, _, _} = node, _depth), do: node
  defp push_filters_recursive({:path, _, _, _} = node, _depth), do: node

  # Catch-all for any other nodes
  defp push_filters_recursive(node, _depth), do: node

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
    push_through_wrapper(filter_expr, inner, fn new_inner ->
      {:order_by, new_inner, conditions}
    end)
  end

  defp try_push_filter(filter_expr, {:slice, inner, offset, limit}) do
    push_through_wrapper(filter_expr, inner, fn new_inner ->
      {:slice, new_inner, offset, limit}
    end)
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

    extended_var =
      case var do
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
    fold_constants_recursive(algebra, 0)
  end

  # Recursive constant folding over algebra tree
  # Includes depth limiting to prevent stack overflow from deeply nested queries
  defp fold_constants_recursive(_algebra, depth) when depth > @max_depth do
    raise ArgumentError,
          "Query too deeply nested (max depth: #{@max_depth}). " <>
            "This may indicate a malformed query or an attack."
  end

  defp fold_constants_recursive({:filter, expr, pattern}, depth) do
    folded_pattern = fold_constants_recursive(pattern, depth + 1)
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

  defp fold_constants_recursive({:join, left, right}, depth) do
    folded_left = fold_constants_recursive(left, depth + 1)
    folded_right = fold_constants_recursive(right, depth + 1)

    # If either side is empty, the join is empty
    case {folded_left, folded_right} do
      {{:bgp, []}, _} -> {:bgp, []}
      {_, {:bgp, []}} -> {:bgp, []}
      _ -> {:join, folded_left, folded_right}
    end
  end

  defp fold_constants_recursive({:left_join, left, right, filter}, depth) do
    folded_left = fold_constants_recursive(left, depth + 1)
    folded_right = fold_constants_recursive(right, depth + 1)
    folded_filter = if filter, do: fold_expression(filter), else: nil

    # If left is empty, the result is empty
    case folded_left do
      {:bgp, []} -> {:bgp, []}
      _ -> {:left_join, folded_left, folded_right, folded_filter}
    end
  end

  defp fold_constants_recursive({:minus, left, right}, depth) do
    {:minus, fold_constants_recursive(left, depth + 1),
     fold_constants_recursive(right, depth + 1)}
  end

  defp fold_constants_recursive({:union, left, right}, depth) do
    folded_left = fold_constants_recursive(left, depth + 1)
    folded_right = fold_constants_recursive(right, depth + 1)

    # If one side is empty, return the other
    case {folded_left, folded_right} do
      {{:bgp, []}, r} -> r
      {l, {:bgp, []}} -> l
      _ -> {:union, folded_left, folded_right}
    end
  end

  defp fold_constants_recursive({:extend, pattern, var, expr}, depth) do
    folded_pattern = fold_constants_recursive(pattern, depth + 1)
    folded_expr = fold_expression(expr)
    {:extend, folded_pattern, var, folded_expr}
  end

  defp fold_constants_recursive({:group, pattern, vars, aggs}, depth) do
    {:group, fold_constants_recursive(pattern, depth + 1), vars, aggs}
  end

  defp fold_constants_recursive({:project, pattern, vars}, depth) do
    {:project, fold_constants_recursive(pattern, depth + 1), vars}
  end

  defp fold_constants_recursive({:distinct, pattern}, depth) do
    {:distinct, fold_constants_recursive(pattern, depth + 1)}
  end

  defp fold_constants_recursive({:reduced, pattern}, depth) do
    {:reduced, fold_constants_recursive(pattern, depth + 1)}
  end

  defp fold_constants_recursive({:order_by, pattern, conditions}, depth) do
    folded_conds = Enum.map(conditions, fn {dir, expr} -> {dir, fold_expression(expr)} end)
    {:order_by, fold_constants_recursive(pattern, depth + 1), folded_conds}
  end

  defp fold_constants_recursive({:slice, pattern, offset, limit}, depth) do
    {:slice, fold_constants_recursive(pattern, depth + 1), offset, limit}
  end

  defp fold_constants_recursive({:graph, term, pattern}, depth) do
    {:graph, term, fold_constants_recursive(pattern, depth + 1)}
  end

  defp fold_constants_recursive({:service, endpoint, pattern, silent}, depth) do
    {:service, endpoint, fold_constants_recursive(pattern, depth + 1), silent}
  end

  # Leaf nodes
  defp fold_constants_recursive({:bgp, _} = node, _depth), do: node
  defp fold_constants_recursive({:values, _, _} = node, _depth), do: node
  defp fold_constants_recursive({:path, _, _, _} = node, _depth), do: node
  defp fold_constants_recursive(node, _depth), do: node

  # ===========================================================================
  # Expression Folding
  # ===========================================================================

  # Fold constants in an expression, returning a simplified expression
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp fold_expression(expr) do
    case expr do
      # Literals and variables are already in simplest form
      {:variable, _} ->
        expr

      {:named_node, _} ->
        expr

      {:blank_node, _} ->
        expr

      {:literal, _, _} ->
        expr

      {:literal, _, _, _} ->
        expr

      # Arithmetic operations
      {:add, left, right} ->
        fold_binary_arithmetic(:add, left, right)

      {:subtract, left, right} ->
        fold_binary_arithmetic(:subtract, left, right)

      {:multiply, left, right} ->
        fold_binary_arithmetic(:multiply, left, right)

      {:divide, left, right} ->
        fold_binary_arithmetic(:divide, left, right)

      {:unary_minus, arg} ->
        fold_unary_minus(arg)

      # Comparison operations
      {:equal, left, right} ->
        fold_comparison(:equal, left, right)

      {:greater, left, right} ->
        fold_comparison(:greater, left, right)

      {:less, left, right} ->
        fold_comparison(:less, left, right)

      {:greater_or_equal, left, right} ->
        fold_comparison(:greater_or_equal, left, right)

      {:less_or_equal, left, right} ->
        fold_comparison(:less_or_equal, left, right)

      # Logical operations
      {:and, left, right} ->
        fold_logical_and(left, right)

      {:or, left, right} ->
        fold_logical_or(left, right)

      {:not, arg} ->
        fold_logical_not(arg)

      # Conditional expressions
      {:if_expr, cond_expr, then_expr, else_expr} ->
        fold_if_expr(cond_expr, then_expr, else_expr)

      {:coalesce, args} when is_list(args) ->
        fold_coalesce(args)

      # Function calls - try to evaluate if all args are constant
      {:function_call, name, args} ->
        fold_function_call(name, args)

      # Other expressions - just fold children
      {:bound, arg} ->
        {:bound, fold_expression(arg)}

      {:exists, pattern} ->
        {:exists, pattern}

      {:in_expr, needle, haystack} ->
        {:in_expr, fold_expression(needle), Enum.map(haystack, &fold_expression/1)}

      # Unknown expressions - return as-is
      _ ->
        expr
    end
  end

  # Fold binary arithmetic operations
  defp fold_binary_arithmetic(op, left, right) do
    folded_left = fold_expression(left)
    folded_right = fold_expression(right)

    if Expression.constant?(folded_left) and Expression.constant?(folded_right) do
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

    if Expression.constant?(folded) do
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

    if Expression.constant?(folded_left) and Expression.constant?(folded_right) do
      case Expression.evaluate({op, folded_left, folded_right}, %{}) do
        {:ok, result} -> result
        :error -> {op, folded_left, folded_right}
      end
    else
      {op, folded_left, folded_right}
    end
  end

  # Fold logical AND with short-circuit optimization
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp fold_logical_and(left, right) do
    folded_left = fold_expression(left)
    folded_right = fold_expression(right)

    cond do
      # false && anything = false
      false_literal?(folded_left) ->
        folded_left

      # true && x = x
      true_literal?(folded_left) ->
        folded_right

      # x && false = false
      false_literal?(folded_right) ->
        folded_right

      # x && true = x
      true_literal?(folded_right) ->
        folded_left

      # Both constant - evaluate
      Expression.constant?(folded_left) and Expression.constant?(folded_right) ->
        case Expression.evaluate({:and, folded_left, folded_right}, %{}) do
          {:ok, result} -> result
          :error -> {:and, folded_left, folded_right}
        end

      true ->
        {:and, folded_left, folded_right}
    end
  end

  # Fold logical OR with short-circuit optimization
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  defp fold_logical_or(left, right) do
    folded_left = fold_expression(left)
    folded_right = fold_expression(right)

    cond do
      # true || anything = true
      true_literal?(folded_left) ->
        folded_left

      # false || x = x
      false_literal?(folded_left) ->
        folded_right

      # x || true = true
      true_literal?(folded_right) ->
        folded_right

      # x || false = x
      false_literal?(folded_right) ->
        folded_left

      # Both constant - evaluate
      Expression.constant?(folded_left) and Expression.constant?(folded_right) ->
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
      true_literal?(folded) ->
        make_boolean(false)

      false_literal?(folded) ->
        make_boolean(true)

      # Double negation: NOT(NOT(x)) = x
      match?({:not, _}, folded) ->
        {:not, inner} = folded
        inner

      Expression.constant?(folded) ->
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
      true_literal?(folded_cond) -> folded_then
      false_literal?(folded_cond) -> folded_else
      true -> {:if_expr, folded_cond, folded_then, folded_else}
    end
  end

  # Fold COALESCE - return first non-error constant at the start, or simplified list
  defp fold_coalesce(args) do
    folded_args = Enum.map(args, &fold_expression/1)

    # Check if first argument is a constant that evaluates successfully
    case folded_args do
      [first | _] when is_tuple(first) ->
        if Expression.constant?(first) do
          case Expression.evaluate(first, %{}) do
            {:ok, _} ->
              # First arg is a valid constant - return it
              first

            :error ->
              # First arg is an error constant - skip it and recurse
              # credo:disable-for-next-line Credo.Check.Refactor.Nesting
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
        not Expression.constant?(arg) or
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

    if Enum.all?(folded_args, &Expression.constant?/1) do
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
    if Expression.constant?(expr) do
      case Expression.evaluate(expr, %{}) do
        {:ok, result} ->
          # credo:disable-for-next-line Credo.Check.Refactor.Nesting
          cond do
            true_literal?(result) -> :always_true
            false_literal?(result) -> :always_false
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
  defp true_literal?({:literal, :typed, "true", "http://www.w3.org/2001/XMLSchema#boolean"}),
    do: true

  defp true_literal?({:literal, :typed, "1", "http://www.w3.org/2001/XMLSchema#boolean"}),
    do: true

  defp true_literal?(_), do: false

  # Check if a term is the boolean false literal
  defp false_literal?({:literal, :typed, "false", "http://www.w3.org/2001/XMLSchema#boolean"}),
    do: true

  defp false_literal?({:literal, :typed, "0", "http://www.w3.org/2001/XMLSchema#boolean"}),
    do: true

  defp false_literal?(_), do: false

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
    reorder_bgp_recursive(algebra, stats, 0)
  end

  # Recursive traversal for BGP reordering
  # Includes depth limiting to prevent stack overflow from deeply nested queries
  defp reorder_bgp_recursive(_algebra, _stats, depth) when depth > @max_depth do
    raise ArgumentError,
          "Query too deeply nested (max depth: #{@max_depth}). " <>
            "This may indicate a malformed query or an attack."
  end

  defp reorder_bgp_recursive({:bgp, patterns}, stats, _depth) do
    reordered = reorder_patterns(patterns, stats)
    {:bgp, reordered}
  end

  defp reorder_bgp_recursive({:filter, expr, pattern}, stats, depth) do
    {:filter, expr, reorder_bgp_recursive(pattern, stats, depth + 1)}
  end

  defp reorder_bgp_recursive({:join, left, right}, stats, depth) do
    {:join, reorder_bgp_recursive(left, stats, depth + 1),
     reorder_bgp_recursive(right, stats, depth + 1)}
  end

  defp reorder_bgp_recursive({:left_join, left, right, filter}, stats, depth) do
    {:left_join, reorder_bgp_recursive(left, stats, depth + 1),
     reorder_bgp_recursive(right, stats, depth + 1), filter}
  end

  defp reorder_bgp_recursive({:minus, left, right}, stats, depth) do
    {:minus, reorder_bgp_recursive(left, stats, depth + 1),
     reorder_bgp_recursive(right, stats, depth + 1)}
  end

  defp reorder_bgp_recursive({:union, left, right}, stats, depth) do
    {:union, reorder_bgp_recursive(left, stats, depth + 1),
     reorder_bgp_recursive(right, stats, depth + 1)}
  end

  defp reorder_bgp_recursive({:extend, pattern, var, expr}, stats, depth) do
    {:extend, reorder_bgp_recursive(pattern, stats, depth + 1), var, expr}
  end

  defp reorder_bgp_recursive({:group, pattern, vars, aggs}, stats, depth) do
    {:group, reorder_bgp_recursive(pattern, stats, depth + 1), vars, aggs}
  end

  defp reorder_bgp_recursive({:project, pattern, vars}, stats, depth) do
    {:project, reorder_bgp_recursive(pattern, stats, depth + 1), vars}
  end

  defp reorder_bgp_recursive({:distinct, pattern}, stats, depth) do
    {:distinct, reorder_bgp_recursive(pattern, stats, depth + 1)}
  end

  defp reorder_bgp_recursive({:reduced, pattern}, stats, depth) do
    {:reduced, reorder_bgp_recursive(pattern, stats, depth + 1)}
  end

  defp reorder_bgp_recursive({:order_by, pattern, conditions}, stats, depth) do
    {:order_by, reorder_bgp_recursive(pattern, stats, depth + 1), conditions}
  end

  defp reorder_bgp_recursive({:slice, pattern, offset, limit}, stats, depth) do
    {:slice, reorder_bgp_recursive(pattern, stats, depth + 1), offset, limit}
  end

  defp reorder_bgp_recursive({:graph, term, pattern}, stats, depth) do
    {:graph, term, reorder_bgp_recursive(pattern, stats, depth + 1)}
  end

  defp reorder_bgp_recursive({:service, endpoint, pattern, silent}, stats, depth) do
    {:service, endpoint, reorder_bgp_recursive(pattern, stats, depth + 1), silent}
  end

  # Leaf nodes and catch-all
  defp reorder_bgp_recursive({:values, _, _} = node, _stats, _depth), do: node
  defp reorder_bgp_recursive({:path, _, _, _} = node, _stats, _depth), do: node
  defp reorder_bgp_recursive(node, _stats, _depth), do: node

  # ===========================================================================
  # Pattern Reordering Logic
  # ===========================================================================

  # Suppress MapSet opaque type warnings in pattern reordering functions
  @dialyzer {:nowarn_function, reorder_patterns: 2}
  @dialyzer {:nowarn_function, greedy_reorder: 4}
  @dialyzer {:nowarn_function, estimate_selectivity: 1}
  @dialyzer {:nowarn_function, estimate_selectivity: 2}
  @dialyzer {:nowarn_function, estimate_selectivity: 3}
  @dialyzer {:nowarn_function, position_score: 4}
  @dialyzer {:nowarn_function, pattern_variables: 1}

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
  @spec greedy_reorder(list(), MapSet.t(), map(), list()) :: list()
  defp greedy_reorder([], _bound_vars, _stats, acc) do
    Enum.reverse(acc)
  end

  defp greedy_reorder(remaining, bound_vars, stats, acc) do
    # Find the most selective pattern (lowest score) using Enum.min_by
    # This is more efficient than sorting when we only need the minimum
    best_pattern =
      Enum.min_by(remaining, fn pattern ->
        estimate_selectivity(pattern, bound_vars, stats)
      end)

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

  def estimate_selectivity({:triple, subject, predicate, object} = triple, bound_vars, stats) do
    # Score each position
    s_score = position_score(subject, bound_vars, :subject, stats)
    p_score = position_score(predicate, bound_vars, :predicate, stats)
    o_score = position_score(object, bound_vars, :object, stats)

    # Base score - multiplicative model
    base_score = s_score * p_score * o_score

    # Check if this pattern can benefit from a range filter
    filter_context = Map.get(stats, :filter_context, %{})
    range_indexed = Map.get(stats, :range_indexed, MapSet.new())

    # Apply range filter boost if applicable
    apply_range_filter_boost(triple, base_score, filter_context, range_indexed, predicate)
  end

  # Fallback for non-triple patterns
  def estimate_selectivity(_pattern, _bound_vars, _stats), do: 1_000_000.0

  # Apply selectivity boost for patterns that bind range-filtered variables
  # with predicates that have range indices
  defp apply_range_filter_boost(
         {:triple, _subj, predicate, {:variable, var}},
         base_score,
         filter_context,
         range_indexed,
         _pred_term
       ) do
    range_vars = Map.get(filter_context, :range_filtered_vars, MapSet.new())

    # Check if the object variable has a range filter
    if MapSet.member?(range_vars, var) do
      # Check if the predicate has a range index
      predicate_uri = extract_predicate_uri(predicate)

      if predicate_uri && predicate_has_range_index?(predicate_uri, range_indexed) do
        # Strong boost - range index can be used
        # Divide by 100 to make this pattern very selective
        base_score / 100.0
      else
        # Variable has range filter but predicate doesn't have range index
        # Still give moderate boost - the filter will eliminate results
        base_score / 10.0
      end
    else
      base_score
    end
  end

  defp apply_range_filter_boost(_, base_score, _, _, _), do: base_score

  # Extract the URI from a predicate term
  defp extract_predicate_uri({:named_node, uri}), do: uri
  defp extract_predicate_uri(_), do: nil

  # Check if a predicate has a range index
  defp predicate_has_range_index?(predicate_uri, range_indexed) do
    MapSet.member?(range_indexed, predicate_uri)
  end

  # Score a single position in a triple pattern
  @spec position_score(term(), MapSet.t(), atom(), map()) :: float()
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
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
          # credo:disable-for-next-line Credo.Check.Refactor.Nesting
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

      count when count < 10_000 ->
        # Common predicate
        50.0

      _ ->
        # Very common predicate (like rdf:type)
        100.0
    end
  end

  # Extract variable names from a triple pattern
  @spec pattern_variables(tuple()) :: MapSet.t(String.t())
  defp pattern_variables({:triple, subject, predicate, object}) do
    [subject, predicate, object]
    |> Enum.flat_map(&term_variables/1)
    |> MapSet.new()
  end

  defp pattern_variables(_), do: MapSet.new()

  @spec term_variables(term()) :: [String.t()]
  defp term_variables({:variable, name}), do: [name]
  defp term_variables(_), do: []

  # ===========================================================================
  # Range Filter Extraction
  # ===========================================================================

  @doc """
  Extracts range filter information from an algebra tree.

  Scans the tree for FILTER expressions containing range comparisons
  (>=, <=, >, <) and returns a map of variable names to their filter info.

  This is used by BGP reordering to boost selectivity of patterns that
  bind filtered variables, especially when the pattern's predicate has
  a numeric range index.

  ## Returns

  A map with:
  - `:range_filtered_vars` - MapSet of variable names that have range filters
  - `:variable_ranges` - Map of variable name to `{min, max}` bounds

  ## Example

      # For FILTER (?price >= 50 && ?price <= 500)
      %{
        range_filtered_vars: MapSet<["price"]>,
        variable_ranges: %{"price" => {50.0, 500.0}}
      }

  """
  @spec extract_range_filters(algebra()) :: map()
  def extract_range_filters(algebra) do
    filters = collect_filter_expressions(algebra, [])

    # Extract range comparisons from all filters
    range_info =
      Enum.flat_map(filters, fn filter_expr ->
        extract_range_comparisons(filter_expr)
      end)

    # Build the result map
    range_vars = Enum.map(range_info, fn {var, _, _} -> var end) |> MapSet.new()

    # Merge ranges for the same variable
    variable_ranges =
      Enum.reduce(range_info, %{}, fn {var, bound_type, value}, acc ->
        current = Map.get(acc, var, {nil, nil})
        updated = merge_bound(current, bound_type, value)
        Map.put(acc, var, updated)
      end)

    %{
      range_filtered_vars: range_vars,
      variable_ranges: variable_ranges
    }
  end

  # Collect all filter expressions from the algebra tree
  defp collect_filter_expressions({:filter, expr, pattern}, acc) do
    collect_filter_expressions(pattern, [expr | acc])
  end

  defp collect_filter_expressions({:join, left, right}, acc) do
    acc
    |> then(&collect_filter_expressions(left, &1))
    |> then(&collect_filter_expressions(right, &1))
  end

  defp collect_filter_expressions({:left_join, left, right, filter}, acc) do
    acc = if filter, do: [filter | acc], else: acc

    acc
    |> then(&collect_filter_expressions(left, &1))
    |> then(&collect_filter_expressions(right, &1))
  end

  defp collect_filter_expressions({:union, left, right}, acc) do
    acc
    |> then(&collect_filter_expressions(left, &1))
    |> then(&collect_filter_expressions(right, &1))
  end

  defp collect_filter_expressions({tag, pattern}, acc) when tag in [:distinct, :reduced] do
    collect_filter_expressions(pattern, acc)
  end

  defp collect_filter_expressions({tag, pattern, _}, acc)
       when tag in [:project, :graph] do
    collect_filter_expressions(pattern, acc)
  end

  defp collect_filter_expressions({:extend, pattern, _, _}, acc) do
    collect_filter_expressions(pattern, acc)
  end

  defp collect_filter_expressions({:group, pattern, _, _}, acc) do
    collect_filter_expressions(pattern, acc)
  end

  defp collect_filter_expressions({:order_by, pattern, _}, acc) do
    collect_filter_expressions(pattern, acc)
  end

  defp collect_filter_expressions({:slice, pattern, _, _}, acc) do
    collect_filter_expressions(pattern, acc)
  end

  defp collect_filter_expressions(_, acc), do: acc

  # Extract range comparisons from a filter expression
  # Returns list of {variable_name, bound_type, numeric_value}
  # where bound_type is :min or :max
  defp extract_range_comparisons({:and, left, right}) do
    extract_range_comparisons(left) ++ extract_range_comparisons(right)
  end

  defp extract_range_comparisons({:or, left, right}) do
    # Don't extract from OR - the range is not guaranteed
    # Only return if both sides filter the same variable with compatible ranges
    left_ranges = extract_range_comparisons(left)
    right_ranges = extract_range_comparisons(right)

    # For simplicity, only use ranges that appear in both branches
    left_vars = Enum.map(left_ranges, fn {v, _, _} -> v end) |> MapSet.new()
    right_vars = Enum.map(right_ranges, fn {v, _, _} -> v end) |> MapSet.new()
    common = MapSet.intersection(left_vars, right_vars)

    if MapSet.size(common) > 0 do
      # Return the most permissive range (union of ranges)
      # This is conservative - we widen the range
      Enum.filter(left_ranges ++ right_ranges, fn {v, _, _} -> v in common end)
    else
      []
    end
  end

  # ?var >= value (var has minimum bound)
  defp extract_range_comparisons({:greater_or_equal, {:variable, var}, value}) do
    case extract_numeric_value(value) do
      {:ok, num} -> [{var, :min, num}]
      :error -> []
    end
  end

  # value <= ?var (var has minimum bound)
  defp extract_range_comparisons({:less_or_equal, value, {:variable, var}}) do
    case extract_numeric_value(value) do
      {:ok, num} -> [{var, :min, num}]
      :error -> []
    end
  end

  # ?var <= value (var has maximum bound)
  defp extract_range_comparisons({:less_or_equal, {:variable, var}, value}) do
    case extract_numeric_value(value) do
      {:ok, num} -> [{var, :max, num}]
      :error -> []
    end
  end

  # value >= ?var (var has maximum bound)
  defp extract_range_comparisons({:greater_or_equal, value, {:variable, var}}) do
    case extract_numeric_value(value) do
      {:ok, num} -> [{var, :max, num}]
      :error -> []
    end
  end

  # ?var > value (var has minimum bound, exclusive)
  defp extract_range_comparisons({:greater, {:variable, var}, value}) do
    case extract_numeric_value(value) do
      {:ok, num} -> [{var, :min, num}]
      :error -> []
    end
  end

  # value < ?var (var has minimum bound, exclusive)
  defp extract_range_comparisons({:less, value, {:variable, var}}) do
    case extract_numeric_value(value) do
      {:ok, num} -> [{var, :min, num}]
      :error -> []
    end
  end

  # ?var < value (var has maximum bound, exclusive)
  defp extract_range_comparisons({:less, {:variable, var}, value}) do
    case extract_numeric_value(value) do
      {:ok, num} -> [{var, :max, num}]
      :error -> []
    end
  end

  # value > ?var (var has maximum bound, exclusive)
  defp extract_range_comparisons({:greater, value, {:variable, var}}) do
    case extract_numeric_value(value) do
      {:ok, num} -> [{var, :max, num}]
      :error -> []
    end
  end

  defp extract_range_comparisons(_), do: []

  # Extract numeric value from a literal
  defp extract_numeric_value({:literal, :typed, value, datatype})
       when datatype in [
              "http://www.w3.org/2001/XMLSchema#integer",
              "http://www.w3.org/2001/XMLSchema#decimal",
              "http://www.w3.org/2001/XMLSchema#float",
              "http://www.w3.org/2001/XMLSchema#double"
            ] do
    case Float.parse(value) do
      {num, _} -> {:ok, num}
      :error -> :error
    end
  end

  defp extract_numeric_value({:literal, :typed, value, _}) do
    # Try parsing as number anyway
    case Float.parse(value) do
      {num, _} -> {:ok, num}
      :error -> :error
    end
  end

  defp extract_numeric_value(_), do: :error

  # Merge a new bound with existing bounds
  defp merge_bound({current_min, current_max}, :min, value) do
    new_min =
      case current_min do
        nil -> value
        existing -> max(existing, value)
      end

    {new_min, current_max}
  end

  defp merge_bound({current_min, current_max}, :max, value) do
    new_max =
      case current_max do
        nil -> value
        existing -> min(existing, value)
      end

    {current_min, new_max}
  end

  @doc """
  Checks if a pattern binds a variable that has a range filter.

  Used by selectivity estimation to boost patterns that can benefit
  from range index lookups.
  """
  @spec binds_range_filtered_variable?(tuple(), map()) :: boolean()
  def binds_range_filtered_variable?({:triple, _subj, _pred, {:variable, var}}, filter_context) do
    range_vars = Map.get(filter_context, :range_filtered_vars, MapSet.new())
    MapSet.member?(range_vars, var)
  end

  def binds_range_filtered_variable?(_, _), do: false

  @doc """
  Gets the range bounds for a variable if it has range filters.

  Returns `{min, max}` where min and max can be nil (unbounded).
  """
  @spec get_variable_range(String.t(), map()) :: {float() | nil, float() | nil}
  def get_variable_range(var_name, filter_context) do
    variable_ranges = Map.get(filter_context, :variable_ranges, %{})
    Map.get(variable_ranges, var_name, {nil, nil})
  end
end
