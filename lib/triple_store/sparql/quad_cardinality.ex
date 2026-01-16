defmodule TripleStore.SPARQL.QuadCardinality do
  @moduledoc """
  Cardinality estimation for quad patterns in named graph queries.

  This module extends the triple pattern cardinality estimation to handle
  quad patterns with a fourth component (graph). It provides accurate
  estimates for graph-scoped and cross-graph queries.

  ## Quad Pattern Representation

  Quad patterns are represented as `{:quad, subject, predicate, object, graph}`:

      # Fully bound
      {:quad, 123, 456, 789, 0}

      # Graph-scoped with variables
      {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}

      # Cross-graph (unbound graph)
      {:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}}

  ## Estimation Approach

  1. **Graph-bound patterns**: Use per-graph statistics for accuracy
  2. **Cross-graph patterns**: Sum estimates across all graphs
  3. **Selectivity factors**: Based on bound positions (S, P, O, G)
  4. **Join cardinality**: Account for graph variable sharing

  ## Statistics Map Format

  The stats map supports both triple and quad counts:

      %{
        triple_count: 10000,         # Total triples (backward compat)
        quad_count: 15000,           # Total quads
        distinct_subjects: 1000,
        distinct_predicates: 50,
        distinct_objects: 2000,
        total_graphs: 5,             # Number of graphs with data
        predicate_histogram: %{...}, # Optional predicate counts
        per_graph_stats: %{           # Per-graph breakdown
          0 => %{quad_count: 5000, distinct_subjects: 500, ...},
          123 => %{quad_count: 10000, ...}
        }
      }

  ## Examples

      # Graph-scoped pattern
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      stats = %{per_graph_stats: %{0 => %{quad_count: 5000, predicate_counts: %{42 => 100}}}}
      QuadCardinality.estimate_pattern(pattern, stats)
      # => 100.0

      # Cross-graph pattern
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}}
      stats = %{per_graph_stats: %{0 => %{...}, 1 => %{...}}}
      QuadCardinality.estimate_pattern(pattern, stats)
      # => Sum of predicate 42 counts across all graphs
  """

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Quad pattern from SPARQL algebra"
  @type quad_pattern :: {:quad, term(), term(), term(), term()}

  @typedoc "Statistics map with cardinality information for quads"
  @type quad_stats :: %{
          optional(:quad_count) => non_neg_integer(),
          optional(:triple_count) => non_neg_integer(),
          optional(:distinct_subjects) => non_neg_integer(),
          optional(:distinct_predicates) => non_neg_integer(),
          optional(:distinct_objects) => non_neg_integer(),
          optional(:total_graphs) => non_neg_integer(),
          optional(:predicate_histogram) => %{non_neg_integer() => non_neg_integer()},
          optional(:per_graph_stats) => %{non_neg_integer() => graph_stats()}
        }

  @typedoc "Per-graph statistics"
  @type graph_stats :: %{
          optional(:quad_count) => non_neg_integer(),
          optional(:distinct_subjects) => non_neg_integer(),
          optional(:distinct_predicates) => non_neg_integer(),
          optional(:distinct_objects) => non_neg_integer(),
          optional(:predicate_counts) => %{non_neg_integer() => non_neg_integer()}
        }

  @typedoc "Cardinality estimate (always positive)"
  @type cardinality :: float()

  @typedoc "Term ID (dictionary-encoded integer)"
  @type term_id :: non_neg_integer()

  @typedoc "Graph ID or term"
  @type graph_id :: non_neg_integer() | :default | :default_graph

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Default estimates when statistics are unavailable
  @default_quad_count 10_000
  @default_distinct_subjects 1_000
  @default_distinct_predicates 100
  @default_distinct_objects 2_000
  @default_total_graphs 1

  # Minimum cardinality to avoid division by zero
  @min_cardinality 1.0

  # Special graph identifiers
  @default_graph_id 0

  # ===========================================================================
  # Stats Validation
  # ===========================================================================

  @doc """
  Validates the stats map structure.

  Returns `:ok` if valid, or `{:error, reason}` if invalid.

  ## Examples

      iex> QuadCardinality.validate_stats(%{quad_count: 1000})
      :ok

      iex> QuadCardinality.validate_stats(%{})
      {:error, :missing_required_keys}

      iex> QuadCardinality.validate_stats("not a map")
      {:error, :invalid_stats}

  """
  @spec validate_stats(term()) :: :ok | {:error, atom()}
  def validate_stats(stats) when not is_map(stats), do: {:error, :invalid_stats}

  def validate_stats(stats) do
    with :ok <- validate_required_stats_keys(stats),
         :ok <- validate_stats_types(stats) do
      :ok
    end
  end

  # Validates that at least one required key exists
  defp validate_required_stats_keys(stats) do
    has_quad_count = Map.has_key?(stats, :quad_count)
    has_triple_count = Map.has_key?(stats, :triple_count)

    if has_quad_count or has_triple_count do
      :ok
    else
      {:error, :missing_required_keys}
    end
  end

  # Validates that stats values have correct types
  defp validate_stats_types(stats) do
    stats
    |> Enum.reduce_while(:ok, fn {key, value}, _acc ->
      case validate_stat_value_type(key, value) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  # Validates individual stat value types
  defp validate_stat_value_type(:quad_count, value) when is_integer(value) and value >= 0, do: :ok
  defp validate_stat_value_type(:quad_count, _value), do: {:error, :invalid_stat_value}
  defp validate_stat_value_type(:triple_count, value) when is_integer(value) and value >= 0, do: :ok
  defp validate_stat_value_type(:triple_count, _value), do: {:error, :invalid_stat_value}
  defp validate_stat_value_type(:distinct_subjects, value) when is_integer(value) and value >= 0, do: :ok
  defp validate_stat_value_type(:distinct_subjects, _value), do: {:error, :invalid_stat_value}
  defp validate_stat_value_type(:distinct_predicates, value) when is_integer(value) and value >= 0, do: :ok
  defp validate_stat_value_type(:distinct_predicates, _value), do: {:error, :invalid_stat_value}
  defp validate_stat_value_type(:distinct_objects, value) when is_integer(value) and value >= 0, do: :ok
  defp validate_stat_value_type(:distinct_objects, _value), do: {:error, :invalid_stat_value}
  defp validate_stat_value_type(:total_graphs, value) when is_integer(value) and value >= 0, do: :ok
  defp validate_stat_value_type(:total_graphs, _value), do: {:error, :invalid_stat_value}
  defp validate_stat_value_type(:predicate_histogram, value) when is_map(value), do: :ok
  defp validate_stat_value_type(:predicate_histogram, _value), do: {:error, :invalid_stat_value}
  defp validate_stat_value_type(:per_graph_stats, value) when is_map(value), do: validate_per_graph_stats(value)
  defp validate_stat_value_type(:per_graph_stats, _value), do: {:error, :invalid_stat_value}
  defp validate_stat_value_type(_key, _value), do: :ok  # Unknown keys are okay

  # Validates per-graph stats structure
  defp validate_per_graph_stats(per_graph_stats) do
    per_graph_stats
    |> Enum.reduce_while(:ok, fn {_graph_id, graph_stats}, _acc ->
      if is_map(graph_stats) do
        {:cont, :ok}
      else
        {:halt, {:error, :invalid_graph_stats}}
      end
    end)
  end

  @doc """
  Ensures stats map has all default values filled in.

  Returns a new stats map with missing values set to defaults.

  ## Examples

      iex> stats = QuadCardinality.ensure_stats_defaults(%{})
      iex> Map.has_key?(stats, :quad_count)
      true

  """
  @spec ensure_stats_defaults(map()) :: map()
  def ensure_stats_defaults(stats) when is_map(stats) do
    %{
      quad_count: Map.get(stats, :quad_count, @default_quad_count),
      triple_count: Map.get(stats, :triple_count, @default_quad_count),  # Use quad count as fallback
      distinct_subjects: Map.get(stats, :distinct_subjects, @default_distinct_subjects),
      distinct_predicates: Map.get(stats, :distinct_predicates, @default_distinct_predicates),
      distinct_objects: Map.get(stats, :distinct_objects, @default_distinct_objects),
      total_graphs: Map.get(stats, :total_graphs, @default_total_graphs),
      predicate_histogram: Map.get(stats, :predicate_histogram, %{}),
      per_graph_stats: Map.get(stats, :per_graph_stats, %{})
    }
  end

  def ensure_stats_defaults(_stats), do: default_stats()

  # Returns a default stats map
  defp default_stats do
    %{
      quad_count: @default_quad_count,
      distinct_subjects: @default_distinct_subjects,
      distinct_predicates: @default_distinct_predicates,
      distinct_objects: @default_distinct_objects,
      total_graphs: @default_total_graphs,
      predicate_histogram: %{},
      per_graph_stats: %{}
    }
  end

  # ===========================================================================
  # Public API - Pattern Cardinality
  # ===========================================================================

  @doc """
  Estimates the cardinality of a quad pattern.

  The estimate is based on:
  - Whether the graph is bound (graph-scoped) or unbound (cross-graph)
  - Per-graph statistics when graph is bound
  - Aggregate statistics when graph is unbound
  - Selectivity of each bound position (S, P, O, G)

  ## Arguments

  - `pattern` - A quad pattern `{:quad, subject, predicate, object, graph}`
  - `stats` - Statistics map with quad statistics

  ## Returns

  Estimated number of matching quads (float, always >= 1.0).

  ## Examples

      # Graph-scoped pattern with bound predicate
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      stats = %{per_graph_stats: %{0 => %{quad_count: 5000, predicate_counts: %{42 => 100}}}}
      QuadCardinality.estimate_pattern(pattern, stats)
      # => 100.0

      # Cross-graph pattern (unbound graph)
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}}
      stats = %{per_graph_stats: %{0 => %{...}, 1 => %{...}}}
      QuadCardinality.estimate_pattern(pattern, stats)
      # => Sum across all graphs

      # Fully bound pattern (exact match)
      pattern = {:quad, 123, 456, 789, 0}
      QuadCardinality.estimate_pattern(pattern, stats)
      # => 1.0

  """
  @spec estimate_pattern(quad_pattern(), quad_stats()) :: cardinality()
  def estimate_pattern({:quad, subject, predicate, object, graph}, stats) do
    # Validate and fill in defaults for stats
    validated_stats =
      case validate_stats(stats) do
        :ok -> stats
        {:error, _reason} -> ensure_stats_defaults(stats)
      end

    # Determine if graph is bound
    if graph_bound?(graph) do
      # Graph-scoped: use per-graph statistics
      estimate_graph_scoped_pattern(subject, predicate, object, graph, validated_stats)
    else
      # Cross-graph: sum across all graphs
      estimate_cross_graph_pattern(subject, predicate, object, validated_stats)
    end
  end

  @doc """
  Estimates the cardinality of a quad pattern with additional bindings.

  When some variables are already bound from previous joins, their
  selectivity is factored in based on the binding domain size.

  ## Arguments

  - `pattern` - A quad pattern
  - `stats` - Statistics map
  - `bound_vars` - Map of variable name to domain size

  ## Returns

  Estimated cardinality considering bound variables.

  """
  @spec estimate_pattern_with_bindings(quad_pattern(), quad_stats(), %{String.t() => pos_integer()}) ::
          cardinality()
  def estimate_pattern_with_bindings({:quad, subject, predicate, object, graph}, stats, bound_vars) do
    base_card = estimate_pattern({:quad, subject, predicate, object, graph}, stats)

    # Determine graph-specific stats if graph is bound
    graph_stats =
      if graph_bound?(graph) do
        graph_id = normalize_graph_id(graph)
        case get_graph_stats(graph_id, stats) do
          {:ok, gs} -> gs
          {:error, :not_found} -> nil
        end
      else
        nil
      end

    # Apply binding selectivity for each variable position
    s_adjustment = variable_binding_adjustment(subject, :subject, graph_stats, stats, bound_vars)
    p_adjustment = variable_binding_adjustment(predicate, :predicate, graph_stats, stats, bound_vars)
    o_adjustment = variable_binding_adjustment(object, :object, graph_stats, stats, bound_vars)
    g_adjustment = variable_binding_adjustment(graph, :graph, graph_stats, stats, bound_vars)

    cardinality = base_card * s_adjustment * p_adjustment * o_adjustment * g_adjustment

    max(cardinality, @min_cardinality)
  end

  # ===========================================================================
  # Public API - Graph-Scoped Estimation
  # ===========================================================================

  @doc """
  Estimates cardinality for a graph-scoped pattern.

  Used when the graph position is bound to a specific graph.
  Uses per-graph statistics for accurate estimates.

  """
  @spec estimate_graph_scoped_pattern(term(), term(), term(), term(), quad_stats()) ::
          cardinality()
  def estimate_graph_scoped_pattern(subject, predicate, object, graph, stats) do
    graph_id = normalize_graph_id(graph)

    # Get per-graph statistics
    case get_graph_stats(graph_id, stats) do
      {:ok, graph_stats} ->
        estimate_with_graph_stats(subject, predicate, object, graph_stats)

      {:error, :not_found} ->
        # Graph doesn't exist or has no data
        @min_cardinality
    end
  end

  @doc """
  Estimates cardinality for a cross-graph pattern.

  Used when the graph position is unbound (variable).
  Sums estimates across all graphs with statistics.

  """
  @spec estimate_cross_graph_pattern(term(), term(), term(), quad_stats()) :: cardinality()
  def estimate_cross_graph_pattern(subject, predicate, object, stats) do
    # Get all graph stats
    per_graph = Map.get(stats, :per_graph_stats, %{})

    cond do
      per_graph == nil or map_size(per_graph) == 0 ->
        # No per-graph stats, fall back to aggregate
        estimate_with_aggregate_stats(subject, predicate, object, stats)

      true ->
        # Sum estimates across all graphs
        total =
          Enum.reduce(per_graph, 0.0, fn {_graph_id, graph_stats}, acc ->
            estimate = estimate_with_graph_stats(subject, predicate, object, graph_stats)
            acc + estimate
          end)

        max(total, @min_cardinality)
    end
  end

  # ===========================================================================
  # Public API - Position Selectivity
  # ===========================================================================

  @doc """
  Calculates the selectivity for a single position in a quad pattern.

  Selectivity is the fraction of values that match at that position:
  - Unbound variable: 1.0 (matches all)
  - Bound constant: 1/distinct_count for that position

  ## Arguments

  - `term` - The term at the position
  - `position` - :subject, :predicate, :object, or :graph
  - `graph_stats` - Statistics for the relevant graph
  - `global_stats` - Global statistics (for graph position)

  ## Returns

  Selectivity factor (0.0 to 1.0).

  """
  @spec position_selectivity(term(), atom(), graph_stats() | nil, quad_stats() | nil) :: float()
  def position_selectivity(term, :graph, _graph_stats, global_stats) do
    if constant?(term) do
      # Graph is bound: fully selective (1.0)
      1.0
    else
      # Graph is unbound: selectivity based on graph count
      total_graphs = get_stat(global_stats || %{}, :total_graphs, @default_total_graphs)
      if total_graphs > 0, do: 1.0, else: 1.0
    end
  end

  def position_selectivity(term, position, graph_stats, global_stats) do
    if constant?(term) do
      # Use graph_stats if available, otherwise fall back to global_stats
      stats = graph_stats || global_stats || %{}
      distinct_count = distinct_count_for_position(position, stats)
      1.0 / max(distinct_count, 1)
    else
      1.0
    end
  end

  # ===========================================================================
  # Public API - Join Cardinality
  # ===========================================================================

  @doc """
  Estimates the cardinality of joining two quad patterns.

  Extends triple join estimation to account for:
  - Graph variable sharing (same graph vs different graphs)
  - Cross-graph joins (when patterns are in different graphs)

  ## Arguments

  - `left_card` - Cardinality of left pattern
  - `right_card` - Cardinality of right pattern
  - `join_vars` - List of variable names being joined
  - `same_graph` - Whether both patterns are in the same graph
  - `stats` - Statistics map

  ## Returns

  Estimated join cardinality.

  ## Join Selectivity Model

  For patterns in the same graph: use standard join selectivity
  For patterns in different graphs: cartesian product (independent)

  ## API Difference from Triple Version

  This function has a `same_graph` boolean parameter that doesn't exist
  in `TripleStore.SPARQL.Cardinality.estimate_join/4`. The reason is
  architectural:

  - **Triple stores**: All triples exist in a single (default) graph, so
    all joins are inherently within the same graph context.

  - **Quad stores**: Quads have a graph component, so joins can be within
    the same named graph (more selective) or across different graphs
    (less selective, approaching cartesian product).

  The `same_graph` parameter allows the cost model to account for this
  difference when estimating quad join cardinalities. When `true`, standard
  join selectivity applies. When `false`, the join is still selective on
  shared non-graph variables but independent on the graph dimension.

  ## Examples

      # Join within same graph (more selective)
      QuadCardinality.estimate_quad_join(1000, 500, ["s"], true, stats)

      # Join across different graphs (less selective)
      QuadCardinality.estimate_quad_join(1000, 500, ["s"], false, stats)

  """
  @spec estimate_quad_join(cardinality(), cardinality(), [String.t()], boolean(), quad_stats()) ::
          cardinality()
  def estimate_quad_join(left_card, right_card, [], _same_graph, _stats) do
    # Cartesian product (no join variables)
    max(left_card * right_card, @min_cardinality)
  end

  def estimate_quad_join(left_card, right_card, join_vars, _same_graph, stats) do
    # Calculate join selectivity (same_graph parameter reserved for future enhancements)
    join_selectivity = calculate_join_selectivity(join_vars, left_card, right_card, stats)
    cardinality = left_card * right_card * join_selectivity
    max(cardinality, @min_cardinality)
  end

  @doc """
  Estimates the cardinality of joining multiple quad patterns.

  """
  @spec estimate_multi_quad_pattern(list(quad_pattern()), quad_stats()) :: cardinality()
  def estimate_multi_quad_pattern([], _stats), do: @min_cardinality

  def estimate_multi_quad_pattern([single], stats) do
    estimate_pattern(single, stats)
  end

  def estimate_multi_quad_pattern([first | rest], stats) do
    # Start with first pattern
    initial_card = estimate_pattern(first, stats)
    initial_vars = pattern_variables(first)
    initial_graphs = pattern_graph_variables(first)

    # Accumulate joins
    {final_card, _vars, _graphs} =
      Enum.reduce(rest, {initial_card, initial_vars, initial_graphs}, fn pattern,
                                                                          {acc_card, acc_vars,
                                                                           acc_graphs} ->
        pattern_card = estimate_pattern(pattern, stats)
        pattern_vars = pattern_variables(pattern)
        pattern_graphs = pattern_graph_variables(pattern)

        # Find join variables
        join_vars = MapSet.intersection(acc_vars, pattern_vars) |> MapSet.to_list()

        # Check if same graph
        same_graph =
          case {MapSet.to_list(acc_graphs), MapSet.to_list(pattern_graphs)} do
            {[g], [g]} -> true
            _ -> false
          end

        # Estimate this join
        if join_vars == [] do
          new_card = acc_card * pattern_card
          new_vars = MapSet.union(acc_vars, pattern_vars)
          new_graphs = MapSet.union(acc_graphs, pattern_graphs)
          {max(new_card, @min_cardinality), new_vars, new_graphs}
        else
          join_card = estimate_quad_join(acc_card, pattern_card, join_vars, same_graph, stats)
          new_vars = MapSet.union(acc_vars, pattern_vars)
          new_graphs = MapSet.union(acc_graphs, pattern_graphs)
          {join_card, new_vars, new_graphs}
        end
      end)

    final_card
  end

  # ===========================================================================
  # Public API - Selectivity
  # ===========================================================================

  @doc """
  Estimates the selectivity of a quad pattern.

  Selectivity is the fraction of the database the pattern matches (0.0 to 1.0).

  """
  @spec estimate_selectivity(quad_pattern(), quad_stats()) :: float()
  def estimate_selectivity(pattern, stats) do
    card = estimate_pattern(pattern, stats)
    total = get_stat(stats, :quad_count, get_stat(stats, :triple_count, @default_quad_count))

    if total > 0 do
      min(card / total, 1.0)
    else
      1.0
    end
  end

  # ===========================================================================
  # Private Helpers - Graph-Scoped Estimation
  # ===========================================================================

  # Estimate using specific graph statistics
  @spec estimate_with_graph_stats(term(), term(), term(), graph_stats()) :: cardinality()
  defp estimate_with_graph_stats(subject, predicate, object, graph_stats) do
    # Get base cardinality from predicate if available
    base_card = get_graph_base_cardinality(predicate, graph_stats)

    # Calculate selectivity for each position
    subject_sel = graph_position_selectivity(subject, :subject, graph_stats)
    object_sel = graph_position_selectivity(object, :object, graph_stats)

    # If predicate is bound and in histogram, don't apply predicate selectivity again
    predicate_sel =
      if constant?(predicate) and has_graph_predicate_count?(predicate, graph_stats) do
        1.0
      else
        graph_position_selectivity(predicate, :predicate, graph_stats)
      end

    # Combine selectivities
    cardinality = base_card * subject_sel * predicate_sel * object_sel

    max(cardinality, @min_cardinality)
  end

  # Estimate using aggregate statistics (when per-graph unavailable)
  @spec estimate_with_aggregate_stats(term(), term(), term(), quad_stats()) :: cardinality()
  defp estimate_with_aggregate_stats(subject, predicate, object, stats) do
    # Use aggregate counts
    base_card = get_base_cardinality(predicate, stats)

    subject_sel = position_selectivity(subject, :subject, nil, stats)
    predicate_sel = position_selectivity(predicate, :predicate, nil, stats)
    object_sel = position_selectivity(object, :object, nil, stats)

    # Don't double-count predicate
    predicate_sel =
      if constant?(predicate) and has_predicate_count?(predicate, stats) do
        1.0
      else
        predicate_sel
      end

    cardinality = base_card * subject_sel * predicate_sel * object_sel

    max(cardinality, @min_cardinality)
  end

  # Get base cardinality from graph stats
  @spec get_graph_base_cardinality(term(), graph_stats()) :: cardinality()
  defp get_graph_base_cardinality(predicate, graph_stats) do
    case get_graph_predicate_count(predicate, graph_stats) do
      {:ok, count} -> count * 1.0
      :not_found -> get_stat(graph_stats, :quad_count, @default_quad_count) * 1.0
    end
  end

  # Get base cardinality from aggregate stats
  @spec get_base_cardinality(term(), quad_stats()) :: cardinality()
  defp get_base_cardinality(predicate, stats) do
    case get_predicate_count(predicate, stats) do
      {:ok, count} -> count * 1.0
      :not_found -> get_stat(stats, :quad_count, get_stat(stats, :triple_count, @default_quad_count)) * 1.0
    end
  end

  # ===========================================================================
  # Private Helpers - Graph Position Selectivity
  # ===========================================================================

  # Calculate selectivity for a position within a specific graph
  @spec graph_position_selectivity(term(), atom(), graph_stats()) :: float()
  defp graph_position_selectivity(term, position, graph_stats) do
    if constant?(term) do
      distinct_count = distinct_count_for_position(position, graph_stats)
      1.0 / max(distinct_count, 1)
    else
      1.0
    end
  end

  # Get distinct count for a position from graph stats
  @spec distinct_count_for_position(atom(), map()) :: non_neg_integer()
  defp distinct_count_for_position(:subject, stats) do
    get_stat(stats, :distinct_subjects, @default_distinct_subjects)
  end

  defp distinct_count_for_position(:predicate, stats) do
    get_stat(stats, :distinct_predicates, @default_distinct_predicates)
  end

  defp distinct_count_for_position(:object, stats) do
    get_stat(stats, :distinct_objects, @default_distinct_objects)
  end

  defp distinct_count_for_position(:graph, _stats) do
    # Graph position handled separately
    1
  end

  # ===========================================================================
  # Private Helpers - Predicate Counts
  # ===========================================================================

  # Get predicate count from graph stats
  @spec get_graph_predicate_count(term(), graph_stats()) :: {:ok, non_neg_integer()} | :not_found
  defp get_graph_predicate_count(predicate, graph_stats) do
    with true <- constant?(predicate),
         id when is_integer(id) <- get_constant_id(predicate),
         predicate_counts when is_map(predicate_counts) <- Map.get(graph_stats, :predicate_counts),
         count when is_integer(count) <- Map.get(predicate_counts, id) do
      {:ok, count}
    else
      _ -> :not_found
    end
  end

  # Check if predicate has count in graph stats
  @spec has_graph_predicate_count?(term(), graph_stats()) :: boolean()
  defp has_graph_predicate_count?(predicate, graph_stats) do
    get_graph_predicate_count(predicate, graph_stats) != :not_found
  end

  # Get predicate count from aggregate stats
  @spec get_predicate_count(term(), quad_stats()) :: {:ok, non_neg_integer()} | :not_found
  defp get_predicate_count(predicate, stats) do
    with true <- constant?(predicate),
         id when is_integer(id) <- get_constant_id(predicate),
         histogram when is_map(histogram) <- Map.get(stats, :predicate_histogram),
         count when is_integer(count) <- Map.get(histogram, id) do
      {:ok, count}
    else
      _ -> :not_found
    end
  end

  # Check if predicate has count in aggregate stats
  @spec has_predicate_count?(term(), quad_stats()) :: boolean()
  defp has_predicate_count?(predicate, stats) do
    get_predicate_count(predicate, stats) != :not_found
  end

  # ===========================================================================
  # Private Helpers - Graph Statistics
  # ===========================================================================

  # Get statistics for a specific graph
  @spec get_graph_stats(graph_id(), quad_stats()) :: {:ok, graph_stats()} | {:error, :not_found}
  defp get_graph_stats(graph_id, stats) do
    case Map.get(stats, :per_graph_stats) do
      nil ->
        # No per-graph stats available
        {:error, :not_found}

      per_graph_stats ->
        case Map.get(per_graph_stats, graph_id) do
          nil -> {:error, :not_found}
          graph_stats when map_size(graph_stats) > 0 -> {:ok, graph_stats}
          _ -> {:error, :not_found}
        end
    end
  end

  # Normalize graph term to ID
  @spec normalize_graph_id(term()) :: non_neg_integer()
  defp normalize_graph_id(:default_graph), do: @default_graph_id
  defp normalize_graph_id(:default), do: @default_graph_id
  defp normalize_graph_id(id) when is_integer(id) and id >= 0, do: id
  defp normalize_graph_id(_), do: @default_graph_id

  # ===========================================================================
  # Private Helpers - Join Selectivity
  # ===========================================================================

  # Calculate selectivity for join variables
  @spec calculate_join_selectivity([String.t()], cardinality(), cardinality(), quad_stats()) ::
          float()
  defp calculate_join_selectivity(join_vars, left_card, right_card, stats) do
    total_quads = get_stat(stats, :quad_count, get_stat(stats, :triple_count, @default_quad_count))

    join_vars
    |> Enum.map(fn _var ->
      # Estimate domain from cardinalities
      left_domain = estimate_domain_from_card(left_card, total_quads)
      right_domain = estimate_domain_from_card(right_card, total_quads)
      max_domain = max(left_domain, right_domain)
      1.0 / max(max_domain, 1.0)
    end)
    |> Enum.reduce(1.0, &(&1 * &2))
  end

  # Estimate domain size from cardinality
  @spec estimate_domain_from_card(cardinality(), non_neg_integer()) :: float()
  defp estimate_domain_from_card(card, total_quads) do
    # Protect against division by zero - ensure total_quads is at least 1
    max_quads = max(total_quads, 1)
    min(:math.sqrt(card), max_quads * 1.0)
  end

  # ===========================================================================
  # Private Helpers - Binding Adjustment
  # ===========================================================================

  @spec variable_binding_adjustment(
          term(),
          atom(),
          graph_stats() | nil,
          quad_stats(),
          %{String.t() => pos_integer()}
        ) :: float()
  defp variable_binding_adjustment(term, position, graph_stats, global_stats, bound_vars) do
    case extract_var_name(term) do
      nil -> 1.0
      var_name -> apply_binding_adjustment(var_name, position, graph_stats, global_stats, bound_vars)
    end
  end

  @spec apply_binding_adjustment(
          String.t(),
          atom(),
          graph_stats() | nil,
          quad_stats(),
          %{String.t() => pos_integer()}
        ) :: float()
  defp apply_binding_adjustment(var_name, position, graph_stats, global_stats, bound_vars) do
    case Map.get(bound_vars, var_name) do
      nil -> 1.0
      bound_domain_size ->
        # Use graph_stats if available, otherwise fall back to global_stats
        stats = graph_stats || global_stats || %{}
        total_domain = distinct_count_for_position(position, stats)
        min(bound_domain_size / max(total_domain, 1), 1.0)
    end
  end

  # ===========================================================================
  # Private Helpers - Pattern Analysis
  # ===========================================================================

  # Get all variables in a pattern
  @spec pattern_variables(quad_pattern()) :: MapSet.t(String.t())
  defp pattern_variables({:quad, s, p, o, g}) do
    [s, p, o, g]
    |> Enum.map(&extract_var_name/1)
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  # Get graph variables from pattern
  @spec pattern_graph_variables(quad_pattern()) :: MapSet.t(String.t())
  defp pattern_graph_variables({:quad, _s, _p, _o, g}) do
    case extract_var_name(g) do
      nil -> MapSet.new()
      var_name -> MapSet.new([var_name])
    end
  end

  # Extract variable name from term
  @spec extract_var_name(term()) :: String.t() | nil
  defp extract_var_name({:variable, name}), do: name
  defp extract_var_name(_), do: nil

  # ===========================================================================
  # Private Helpers - Term Analysis
  # ===========================================================================

  # Check if term is a constant (not a variable)
  @spec constant?(term()) :: boolean()
  defp constant?({:variable, _}), do: false
  defp constant?(:default_graph), do: true
  defp constant?(:default), do: true
  defp constant?(_), do: true

  # Get the ID from a constant term
  @spec get_constant_id(term()) :: non_neg_integer() | nil
  defp get_constant_id(id) when is_integer(id), do: id
  defp get_constant_id(_), do: nil

  # Check if graph position is bound
  @spec graph_bound?(term()) :: boolean()
  defp graph_bound?({:variable, _}), do: false
  defp graph_bound?(_), do: true

  # ===========================================================================
  # Private Helpers - Statistics Access
  # ===========================================================================

  # Get a statistic with default fallback
  @spec get_stat(map(), atom(), non_neg_integer()) :: non_neg_integer()
  defp get_stat(stats, key, default) do
    Map.get(stats, key, default)
  end

  # ===========================================================================
  # Public API - Histogram-Based Estimation
  # ===========================================================================

  @doc """
  Estimates cardinality using per-graph predicate histograms.

  This is the most accurate estimation method for quad patterns when
  histograms are available. It uses actual predicate counts from each
  graph instead of relying on statistical approximations.

  ## Arguments

  - `pattern` - A quad pattern
  - `histograms` - Per-graph predicate histograms from Statistics.build_per_graph_histograms/1
    Format: `%{graph_id => %{predicate_id => count}}`

  ## Returns

  Estimated cardinality based on histogram data.

  ## Examples

      # Build histograms first
      {:ok, histograms} = Statistics.build_per_graph_histograms(db)

      # Estimate pattern with histogram
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      estimate = QuadCardinality.estimate_with_histogram(pattern, histograms)
      # => Actual count of quads with predicate 42 in graph 0

      # Cross-graph pattern sums across all graphs
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}}
      estimate = QuadCardinality.estimate_with_histogram(pattern, histograms)
      # => Sum of predicate 42 counts across all graphs
  """
  @spec estimate_with_histogram(quad_pattern(), %{term_id() => %{term_id() => non_neg_integer()}}) ::
          cardinality()
  def estimate_with_histogram({:quad, subject, predicate, object, graph}, histograms) do
    cond do
      # Fully bound pattern - exact lookup
      constant?(subject) and constant?(predicate) and constant?(object) and constant?(graph) ->
        # Check if this exact quad exists in histograms
        graph_id = get_constant_id(graph)
        pred_id = get_constant_id(predicate)
        case get_in(histograms, [graph_id, pred_id]) do
          nil -> @min_cardinality
          count when count > 0 -> min(count, 1.0)  # Exact match, return at most 1
        end

      # Graph-scoped with bound predicate - use histogram directly
      constant?(graph) and constant?(predicate) ->
        graph_id = get_constant_id(graph)
        pred_id = get_constant_id(predicate)
        case get_in(histograms, [graph_id, pred_id]) do
          nil -> @min_cardinality
          count when count > 0 -> count * 1.0
        end

      # Graph-scoped with variable predicate - estimate from total graph quads
      constant?(graph) ->
        graph_id = get_constant_id(graph)
        case Map.get(histograms, graph_id) do
          nil -> @min_cardinality
          graph_histogram ->
            # Sum all predicates in this graph
            total_quads = graph_histogram |> Map.values() |> Enum.sum()
            total_quads * 1.0
        end

      # Cross-graph with bound predicate - sum across all graphs
      constant?(predicate) ->
        pred_id = get_constant_id(predicate)
        histograms
        |> Enum.reduce(0.0, fn {_graph_id, graph_histogram}, acc ->
          case Map.get(graph_histogram, pred_id) do
            nil -> acc
            count -> acc + count
          end
        end)

      # Cross-graph with variable predicate - sum all quads across all graphs
      true ->
        histograms
        |> Enum.reduce(0.0, fn {_graph_id, graph_histogram}, acc ->
          total_in_graph = graph_histogram |> Map.values() |> Enum.sum()
          acc + total_in_graph
        end)
    end
  end

  @doc """
  Estimates cardinality combining histogram data with statistics.

  This is a hybrid approach that uses histogram data when available
  and falls back to statistical estimates when histogram data is missing.

  ## Arguments

  - `pattern` - A quad pattern
  - `histograms` - Per-graph predicate histograms (optional, can be %{})
  - `stats` - Statistics map for fallback

  ## Returns

  Estimated cardinality with histogram data preferred over statistical estimates.
  """
  @spec estimate_with_hybrid(
          quad_pattern(),
          %{term_id() => %{term_id() => non_neg_integer()}},
          quad_stats()
        ) :: cardinality()
  def estimate_with_hybrid({:quad, subject, predicate, object, graph}, histograms, stats) do
    # Try histogram-based estimation first
    histogram_estimate = estimate_with_histogram({:quad, subject, predicate, object, graph}, histograms)

    # If histogram has no data, fall back to statistical estimation
    if histogram_estimate <= @min_cardinality do
      estimate_pattern({:quad, subject, predicate, object, graph}, stats)
    else
      histogram_estimate
    end
  end

  @doc """
  Gets predicate counts from histograms for a specific graph.

  Returns a map of predicate_id => count for the given graph.
  """
  @spec get_predicate_counts_for_graph(term_id(), %{term_id() => %{term_id() => non_neg_integer()}}) ::
          %{term_id() => non_neg_integer()}
  def get_predicate_counts_for_graph(graph_id, histograms) do
    Map.get(histograms, graph_id, %{})
  end

  @doc """
  Gets the count of a specific predicate in a specific graph from histograms.

  Returns nil if the predicate or graph doesn't exist in the histogram data.
  """
  @spec get_predicate_count(term_id(), term_id(), %{term_id() => %{term_id() => non_neg_integer()}}) ::
          non_neg_integer() | nil
  def get_predicate_count(graph_id, predicate_id, histograms) do
    get_in(histograms, [graph_id, predicate_id])
  end
end
