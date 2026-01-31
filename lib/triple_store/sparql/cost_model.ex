defmodule TripleStore.SPARQL.CostModel do
  @moduledoc """
  Cost model for SPARQL query optimization.

  This module estimates execution costs for different join strategies and
  operations, enabling the cost-based optimizer to select efficient execution
  plans. Costs are expressed in abstract "cost units" that approximate relative
  execution time.

  ## Cost Model Philosophy

  The cost model balances several factors:

  1. **CPU cost**: Processing time for comparisons, hashing, etc.
  2. **I/O cost**: Disk access for index scans and data retrieval
  3. **Memory cost**: Space required for hash tables and materialization

  Costs are relative, not absolute - the goal is to rank plans correctly,
  not predict exact execution time.

  ## Join Strategies

  The model supports three join strategies:

  1. **Nested Loop Join**: O(n * m) CPU, O(1) memory
     - Best for small inputs or when outer is very selective
     - Simple but expensive for large inputs

  2. **Hash Join**: O(n + m) CPU, O(n) memory
     - Best for medium to large inputs with good hash distribution
     - Requires materializing the build side

  3. **Leapfrog Triejoin**: Based on AGM bound
     - Best for multi-way joins with shared variables
     - Worst-case optimal for cyclic queries

  ## Cost Components

  Each cost estimate includes:
  - `cpu`: Estimated CPU operations
  - `io`: Estimated I/O operations (index seeks, scans)
  - `memory`: Estimated memory usage in tuples
  - `total`: Weighted combination for comparison

  ## Usage

      stats = %{triple_count: 100_000, distinct_subjects: 10_000, ...}

      # Cost a nested loop join
      cost = CostModel.nested_loop_cost(1000, 500)
      # => %{cpu: 500_000, io: 0, memory: 500, total: 500_500}

      # Cost a hash join
      cost = CostModel.hash_join_cost(1000, 500)
      # => %{cpu: 1500, io: 0, memory: 1000, total: 2500}

      # Compare strategies
      best = CostModel.select_join_strategy(1000, 500, ["x"], stats)
      # => {:hash_join, %{...}}

  ## References

  - Selinger et al. (1979). Access Path Selection in a Relational Database
  - Veldhuizen (2014). Leapfrog Triejoin: A Simple, Worst-Case Optimal Join
  """

  alias TripleStore.SPARQL.Cardinality
  alias TripleStore.SPARQL.QuadCardinality

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Cost estimate with component breakdown"
  @type cost :: %{
          cpu: float(),
          io: float(),
          memory: float(),
          total: float()
        }

  @typedoc "Join strategy identifier"
  @type join_strategy :: :nested_loop | :hash_join | :leapfrog

  @typedoc "Statistics map for cost estimation"
  @type stats :: Cardinality.stats()

  @typedoc "Index scan type"
  @type scan_type :: :point_lookup | :prefix_scan | :full_scan

  @typedoc "Quad pattern for quad stores"
  @type quad_pattern :: {:quad, pattern_term(), pattern_term(), pattern_term(), pattern_term()}

  @typedoc "Term in a pattern (bound value or variable)"
  @type pattern_term ::
          {:variable, String.t()}
          | {:named_node, String.t()}
          | {:literal, :plain, String.t(), String.t() | nil}
          | {:literal, :typed, String.t(), String.t()}
          | {:literal, :language, String.t(), String.t()}
          | {:blank_node, String.t()}
          | :default_graph
          | integer()

  # ===========================================================================
  # Constants - Cost Weights (3.3.1)
  # ===========================================================================

  # Default cost weights - can be overridden via config/2
  @default_weights %{
    # CPU cost weights
    comparison_cost: 1.0,
    hash_cost: 2.0,
    hash_probe_cost: 1.5,

    # I/O cost weights (relative to CPU)
    index_seek_cost: 10.0,
    sequential_read_cost: 0.1,

    # Memory cost weight (penalty for memory usage)
    memory_weight: 1.0,

    # Leapfrog-specific costs
    leapfrog_seek_cost: 5.0,
    leapfrog_comparison_cost: 1.5,

    # Join thresholds
    hash_join_threshold: 100,

    # Weight factors for total cost calculation
    cpu_weight: 1.0,
    io_weight: 1.0,
    memory_weight_factor: 0.1
  }

  # Module attribute for default values (used when not using config)
  @comparison_cost @default_weights.comparison_cost
  @hash_cost @default_weights.hash_cost
  @hash_probe_cost @default_weights.hash_probe_cost
  @index_seek_cost @default_weights.index_seek_cost
  @sequential_read_cost @default_weights.sequential_read_cost
  @memory_weight @default_weights.memory_weight
  @leapfrog_seek_cost @default_weights.leapfrog_seek_cost
  @leapfrog_comparison_cost @default_weights.leapfrog_comparison_cost
  @hash_join_threshold @default_weights.hash_join_threshold

  # Bounds checking for cost calculations
  # Prevents integer overflow and incorrect cost estimates
  # 1 trillion
  @max_safe_cardinality 1_000_000_000_000
  # 10^15 (quadrillion)
  @max_cost 1_000_000_000_000_000

  # ===========================================================================
  # Public API - Configuration (3.3.1)
  # ===========================================================================

  @doc """
  Returns the currently configured cost weights.

  Checks application environment configuration first, then falls back
  to defaults.

  ## Configuration

  Weights can be configured via application environment:

      config :triple_store, :sparql,
        cost_model_weights: %{
          comparison_cost: 1.5,
          hash_cost: 3.0,
          index_seek_cost: 15.0
        }

  ## Returns

  Map of weight names to their values.

  """
  @spec get_weights() :: map()
  def get_weights do
    app_weights =
      Application.get_env(:triple_store, :sparql, [])
      |> Keyword.get(:cost_model_weights, %{})

    Map.merge(@default_weights, app_weights)
  end

  @doc """
  Returns the default cost weights.

  These weights can be customized using `with_weights/2` for cost calculations.

  ## Returns

  Map of weight names to their default values.
  """
  @spec default_weights() :: map()
  def default_weights, do: @default_weights

  @doc """
  Validates a weights map for correctness.

  Ensures all weights have valid types and reasonable values.

  ## Returns

  - `:ok` if weights are valid
  - `{:error, reason}` if validation fails

  ## Examples

      iex> CostModel.validate_weights(%{hash_cost: 3.0})
      :ok

      iex> CostModel.validate_weights(%{hash_cost: -1})
      {:error, {:invalid_weight, :hash_cost, :must_be_positive}}

  """
  @spec validate_weights(map()) :: :ok | {:error, term()}
  def validate_weights(weights) when is_map(weights) do
    # Define expected types and validators for each weight
    validators = %{
      # Float weights - must be positive
      comparison_cost: &validate_positive_float/1,
      hash_cost: &validate_positive_float/1,
      hash_probe_cost: &validate_positive_float/1,
      index_seek_cost: &validate_positive_float/1,
      sequential_read_cost: &validate_positive_float/1,
      memory_weight: &validate_positive_float/1,
      leapfrog_seek_cost: &validate_positive_float/1,
      leapfrog_comparison_cost: &validate_positive_float/1,
      cpu_weight: &validate_positive_float/1,
      io_weight: &validate_positive_float/1,
      memory_weight_factor: &validate_positive_float/1,

      # Integer thresholds - must be positive integers
      hash_join_threshold: &validate_positive_integer/1
    }

    # Validate each weight in the map
    Enum.reduce_while(weights, :ok, fn {key, value}, _acc ->
      case Map.get(validators, key) do
        nil ->
          # Unknown key - ignore (allows for future extensibility)
          {:cont, :ok}

        validator ->
          case validator.(value) do
            :ok -> {:cont, :ok}
            error -> {:halt, error}
          end
      end
    end)
  end

  # Validates that a value is a positive float
  defp validate_positive_float(value) when is_number(value) and value > 0, do: :ok
  defp validate_positive_float(_value), do: {:error, {:invalid_weight, :must_be_positive_float}}

  # Validates that a value is a positive integer
  defp validate_positive_integer(value) when is_integer(value) and value > 0, do: :ok

  defp validate_positive_integer(_value),
    do: {:error, {:invalid_weight, :must_be_positive_integer}}

  @doc """
  Sets the cost model weights at runtime.

  Updates the application environment configuration for the cost model.

  ## Arguments

  - `weights` - Map of weight overrides

  ## Returns

  - `:ok` if weights were set successfully
  - `{:error, reason}` if validation fails

  ## Examples

      iex> CostModel.set_weights(%{hash_cost: 3.0})
      :ok

  """
  @spec set_weights(map()) :: :ok | {:error, term()}
  def set_weights(weights) do
    case validate_weights(weights) do
      :ok ->
        current_config = Application.get_env(:triple_store, :sparql, [])
        current_weights = Keyword.get(current_config, :cost_model_weights, %{})
        merged_weights = Map.merge(current_weights, weights)
        updated_config = Keyword.put(current_config, :cost_model_weights, merged_weights)
        Application.put_env(:triple_store, :sparql, updated_config)
        :ok

      error ->
        error
    end
  end

  @doc """
  Estimates cost with custom weight configuration.

  ## Arguments

  - `cost_fn` - Function to compute cost (takes weights as argument)
  - `custom_weights` - Map of weight overrides

  ## Returns

  Cost calculated with merged weights.

  ## Examples

      custom = %{hash_cost: 3.0, io_weight: 2.0}
      cost = CostModel.with_weights(custom, fn weights ->
        hash_join_cost_with_weights(1000, 500, weights)
      end)

  """
  @spec with_weights(map(), (map() -> cost())) :: cost()
  def with_weights(custom_weights, cost_fn) do
    weights = Map.merge(@default_weights, custom_weights)
    cost_fn.(weights)
  end

  @doc """
  Returns a cost breakdown with explanations for debugging/explain output.

  ## Arguments

  - `cost` - Cost estimate
  - `operation` - Description of the operation

  ## Returns

  Map with cost components and explanation.

  ## Examples

      cost = CostModel.hash_join_cost(1000, 500)
      explained = CostModel.explain_cost(cost, "Hash join P1 ⋈ P2")
      # => %{
      #      operation: "Hash join P1 ⋈ P2",
      #      cpu: 2750.0,
      #      io: 0.0,
      #      memory: 1000.0,
      #      total: 3750.0,
      #      breakdown: "CPU: 2750, I/O: 0, Memory: 1000"
      #    }

  """
  @spec explain_cost(cost(), String.t()) :: map()
  def explain_cost(cost, operation) do
    Map.merge(cost, %{
      operation: operation,
      breakdown:
        "CPU: #{Float.round(cost.cpu, 1)}, I/O: #{Float.round(cost.io, 1)}, Memory: #{Float.round(cost.memory, 1)}"
    })
  end

  @doc """
  Estimates the cost of a filter operation.

  ## Arguments

  - `input_card` - Cardinality of input
  - `selectivity` - Filter selectivity (0.0 to 1.0)

  ## Returns

  Cost estimate for the filter.
  """
  @spec filter_cost(number(), float()) :: cost()
  def filter_cost(input_card, selectivity) do
    # CPU: evaluate filter for each input tuple
    cpu = input_card * @comparison_cost

    # No I/O or memory for simple filters
    io = 0.0
    memory = 0.0

    cost = build_cost(cpu, io, memory)

    # Adjust for output cardinality
    %{cost | memory: input_card * selectivity * @memory_weight}
  end

  @doc """
  Estimates the cost of a filter with range predicate using histogram data.

  ## Arguments

  - `input_card` - Input cardinality
  - `predicate_id` - The predicate being filtered
  - `min_value` - Minimum value
  - `max_value` - Maximum value
  - `stats` - Statistics with histograms

  ## Returns

  Cost with accurate selectivity from histogram.
  """
  @spec range_filter_cost(number(), term(), number(), number(), stats()) :: cost()
  def range_filter_cost(input_card, predicate_id, min_value, max_value, stats) do
    selectivity =
      Cardinality.estimate_range_selectivity(predicate_id, min_value, max_value, stats)

    filter_cost(input_card, selectivity)
  end

  # ===========================================================================
  # Public API - Join Costs
  # ===========================================================================

  @doc """
  Estimates the cost of a nested loop join.

  Nested loop join iterates through all combinations of left and right inputs,
  producing O(left * right) comparisons in the worst case.

  ## Arguments

  - `left_card` - Cardinality of the left (outer) input
  - `right_card` - Cardinality of the right (inner) input

  ## Returns

  Cost estimate with component breakdown.

  ## Complexity

  - CPU: O(left_card * right_card) comparisons
  - Memory: O(right_card) to materialize inner relation
  - I/O: 0 (assumes inputs already materialized)

  ## Examples

      iex> CostModel.nested_loop_cost(100, 50)
      %{cpu: 5000.0, io: 0.0, memory: 50.0, total: 5050.0}

  """
  @spec nested_loop_cost(number(), number()) :: cost()
  def nested_loop_cost(left_card, right_card) do
    # Cap cardinalities to prevent overflow
    capped_left = min(left_card, @max_safe_cardinality)
    capped_right = min(right_card, @max_safe_cardinality)

    # CPU: Compare each left tuple with each right tuple
    cpu = capped_left * capped_right * @comparison_cost
    cpu = min(cpu, @max_cost)

    # Memory: Need to materialize the right side for repeated iteration
    memory = capped_right * @memory_weight
    memory = min(memory, @max_cost)

    # I/O: None for pure join (inputs assumed materialized)
    io = 0.0

    build_cost(cpu, io, memory)
  end

  @doc """
  Estimates the cost of a hash join.

  Hash join builds a hash table on one input (build side) and probes with
  the other (probe side). This provides O(n + m) complexity.

  ## Arguments

  - `left_card` - Cardinality of the left (build) input
  - `right_card` - Cardinality of the right (probe) input

  ## Returns

  Cost estimate with component breakdown.

  ## Complexity

  - CPU: O(left_card) for building + O(right_card) for probing
  - Memory: O(left_card) for hash table
  - I/O: 0 (assumes inputs already materialized)

  ## Examples

      iex> CostModel.hash_join_cost(1000, 500)
      %{cpu: 2750.0, io: 0.0, memory: 1000.0, total: 3750.0}

  """
  @spec hash_join_cost(number(), number()) :: cost()
  def hash_join_cost(left_card, right_card) do
    # Cap cardinalities to prevent overflow
    capped_left = min(left_card, @max_safe_cardinality)
    capped_right = min(right_card, @max_safe_cardinality)

    # CPU: Hash each left tuple, then probe with each right tuple
    build_cost_cpu = capped_left * @hash_cost
    probe_cost_cpu = capped_right * @hash_probe_cost
    cpu = min(build_cost_cpu + probe_cost_cpu, @max_cost)

    # Memory: Hash table for left side
    memory = min(capped_left * @memory_weight, @max_cost)

    # I/O: None for pure join
    io = 0.0

    build_cost(cpu, io, memory)
  end

  @doc """
  Estimates the cost of a Leapfrog Triejoin.

  Leapfrog Triejoin is a worst-case optimal algorithm for multi-way joins.
  The cost is based on the AGM (Atserias-Grohe-Marx) bound, which provides
  a tight upper bound on the output size for cyclic queries.

  ## Arguments

  - `pattern_cards` - List of cardinalities for each pattern
  - `join_vars` - List of shared variable names
  - `stats` - Statistics for the database

  ## Returns

  Cost estimate with component breakdown.

  ## Complexity

  For k patterns with cardinalities c1, c2, ..., ck and output size OUT:
  - CPU: O(k * OUT * log(max(ci))) for leapfrog iterations
  - Memory: O(k) for iterator state
  - I/O: O(k * log(N)) for initial index positioning

  The AGM bound ensures output-sensitive behavior.

  ## Examples

      pattern_cards = [1000, 500, 800]
      cost = CostModel.leapfrog_cost(pattern_cards, ["x", "y"], stats)

  """
  @spec leapfrog_cost([number()], [String.t()], stats()) :: cost()
  # credo:disable-for-next-line Credo.Check.Refactor.Nesting
  def leapfrog_cost(pattern_cards, join_vars, stats) when is_list(pattern_cards) do
    k = length(pattern_cards)

    if k < 2 do
      # Leapfrog doesn't make sense for < 2 patterns
      build_cost(Float.max_finite(), 0.0, 0.0)
    else
      # Estimate output cardinality using AGM bound approximation
      # For simplicity, use geometric mean of input cardinalities adjusted by join selectivity
      output_estimate = estimate_leapfrog_output(pattern_cards, join_vars, stats)

      # CPU: For each output tuple, we do k seeks and comparisons
      # Each seek is O(log N) where N is the index size
      #
      # ## Logarithmic Scaling Rationale
      #
      # We use log2(triple_count) because RocksDB uses B-trees for indexing,
      # and B-tree operations have O(log₂ N) complexity. This is the
      # theoretically correct cost factor for index seeks.
      #
      # The max(triple_count, 2) guard prevents log(0) and log(1) which are
      # undefined or zero, ensuring a minimum cost factor.
      #
      # Example values:
      # - 1,000 triples   → log₂(1000) ≈ 10 (3 levels in B-tree)
      # - 10,000 triples  → log₂(10000) ≈ 13 (4 levels)
      # - 1,000,000 triples → log₂(10⁶) ≈ 20 (5-6 levels)
      #
      triple_count = Map.get(stats, :triple_count, 10_000)
      log_factor = :math.log2(max(triple_count, 2))

      seek_cost_per_tuple = k * @leapfrog_seek_cost * log_factor
      comparison_cost_per_tuple = k * @leapfrog_comparison_cost

      cpu = output_estimate * (seek_cost_per_tuple + comparison_cost_per_tuple)

      # Memory: Just iterator state, O(k)
      memory = k * @memory_weight

      # I/O: Initial positioning + seeks during iteration
      # Each iterator does O(OUT / ci) seeks on average
      avg_seeks_per_iterator =
        Enum.reduce(pattern_cards, 0.0, fn card, acc ->
          if card > 0, do: acc + output_estimate / card, else: acc
        end)

      io = avg_seeks_per_iterator * @index_seek_cost

      build_cost(cpu, io, memory)
    end
  end

  # ===========================================================================
  # Public API - Index Scan Costs
  # ===========================================================================

  @doc """
  Estimates the cost of an index scan for a triple pattern.

  The cost depends on the scan type:
  - Point lookup: Single key access
  - Prefix scan: Range scan within prefix
  - Full scan: Entire index traversal

  ## Arguments

  - `scan_type` - Type of scan (:point_lookup, :prefix_scan, :full_scan)
  - `estimated_results` - Expected number of results
  - `stats` - Database statistics

  ## Returns

  Cost estimate with component breakdown.

  ## Examples

      # Point lookup for fully bound pattern
      cost = CostModel.index_scan_cost(:point_lookup, 1, stats)

      # Prefix scan for partially bound pattern
      cost = CostModel.index_scan_cost(:prefix_scan, 500, stats)

      # Full scan for unbound pattern
      cost = CostModel.index_scan_cost(:full_scan, 100_000, stats)

      # For quad patterns, specify pattern type
      cost = CostModel.index_scan_cost(:full_scan, 100_000, stats, :quad)

  """
  @spec index_scan_cost(scan_type(), number(), stats()) :: cost()
  def index_scan_cost(scan_type, estimated_results, stats) do
    index_scan_cost(scan_type, estimated_results, stats, :triple)
  end

  @spec index_scan_cost(scan_type(), number(), stats(), :triple | :quad) :: cost()
  def index_scan_cost(:point_lookup, _estimated_results, _stats, _pattern_type) do
    # Single key lookup: one seek, one read
    cpu = @comparison_cost
    io = @index_seek_cost
    memory = @memory_weight

    build_cost(cpu, io, memory)
  end

  def index_scan_cost(:prefix_scan, estimated_results, _stats, _pattern_type) do
    # Prefix scan: one seek, then sequential reads
    cpu = estimated_results * @comparison_cost
    io = @index_seek_cost + estimated_results * @sequential_read_cost
    memory = @memory_weight

    build_cost(cpu, io, memory)
  end

  def index_scan_cost(:full_scan, estimated_results, stats, pattern_type) do
    # Full scan: no seek advantage, read everything
    # Use pattern-appropriate count key
    _count_key =
      case pattern_type do
        :quad -> :quad_count
        :triple -> :triple_count
      end

    # For quad, fall back to triple_count if quad_count not available
    total_count =
      case pattern_type do
        :quad -> Map.get(stats, :quad_count) || Map.get(stats, :triple_count, estimated_results)
        :triple -> Map.get(stats, :triple_count, estimated_results)
      end

    cpu = total_count * @comparison_cost
    io = total_count * @sequential_read_cost
    memory = @memory_weight

    build_cost(cpu, io, memory)
  end

  @doc """
  Determines the scan type for a triple pattern based on bound positions.

  ## Arguments

  - `pattern` - Triple pattern {:triple, s, p, o}

  ## Returns

  The appropriate scan type.

  ## Examples

      # Fully bound pattern
      scan_type = CostModel.pattern_scan_type({:triple, 1, 2, 3})
      # => :point_lookup

      # Partially bound pattern
      scan_type = CostModel.pattern_scan_type({:triple, 1, {:variable, "p"}, {:variable, "o"}})
      # => :prefix_scan

      # Unbound pattern
      scan_type = CostModel.pattern_scan_type({:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}})
      # => :full_scan

  """
  @spec pattern_scan_type(Cardinality.triple_pattern()) :: scan_type()
  def pattern_scan_type({:triple, s, p, o}) do
    bound_count = count_bound_positions(s, p, o)

    case bound_count do
      3 -> :point_lookup
      0 -> :full_scan
      _ -> :prefix_scan
    end
  end

  @doc """
  Estimates the cost of a pattern scan including index access and result processing.

  ## Arguments

  - `pattern` - Triple pattern {:triple, s, p, o}
  - `stats` - Database statistics

  ## Returns

  Cost estimate with component breakdown.

  """
  @spec pattern_cost(Cardinality.triple_pattern(), stats()) :: cost()
  def pattern_cost(pattern, stats) do
    scan_type = pattern_scan_type(pattern)
    estimated_results = Cardinality.estimate_pattern(pattern, stats)

    # Base scan cost
    scan_cost = index_scan_cost(scan_type, estimated_results, stats)

    # Add post-filter cost for S?O patterns (requires filtering)
    post_filter_cost =
      if needs_post_filter?(pattern) do
        # Must read more tuples and filter
        filter_ratio = 2.0
        filter_cpu = estimated_results * filter_ratio * @comparison_cost
        build_cost(filter_cpu, 0.0, 0.0)
      else
        build_cost(0.0, 0.0, 0.0)
      end

    add_costs(scan_cost, post_filter_cost)
  end

  # ===========================================================================
  # Public API - Strategy Selection
  # ===========================================================================

  @doc """
  Selects the best join strategy for two inputs.

  Compares nested loop and hash join costs and returns the cheaper option.
  Does not consider Leapfrog here as that's for multi-way joins.

  ## Arguments

  - `left_card` - Cardinality of left input
  - `right_card` - Cardinality of right input
  - `join_vars` - Shared variables (for selectivity)
  - `stats` - Database statistics

  ## Returns

  Tuple of selected strategy and its cost.

  ## Examples

      {strategy, cost} = CostModel.select_join_strategy(1000, 500, ["x"], stats)
      # => {:hash_join, %{...}}

  """
  @spec select_join_strategy(number(), number(), [String.t()], stats()) ::
          {join_strategy(), cost()}
  def select_join_strategy(left_card, right_card, _join_vars, _stats) do
    nl_cost = nested_loop_cost(left_card, right_card)
    hj_cost = hash_join_cost(left_card, right_card)

    # Also consider swapping build/probe sides for hash join
    hj_cost_swapped = hash_join_cost(right_card, left_card)
    best_hj_cost = if hj_cost_swapped.total < hj_cost.total, do: hj_cost_swapped, else: hj_cost

    cond do
      # For very small inputs, nested loop is fine
      left_card < @hash_join_threshold and right_card < @hash_join_threshold ->
        {:nested_loop, nl_cost}

      # Otherwise, compare total costs
      nl_cost.total <= best_hj_cost.total ->
        {:nested_loop, nl_cost}

      true ->
        {:hash_join, best_hj_cost}
    end
  end

  @doc """
  Determines if Leapfrog Triejoin should be used for a multi-pattern join.

  Leapfrog is preferred when:
  1. There are 3+ patterns
  2. Multiple shared variables create a cyclic query structure
  3. The AGM bound suggests sub-quadratic output size

  ## Arguments

  - `pattern_cards` - List of pattern cardinalities
  - `join_vars` - List of all shared variables
  - `stats` - Database statistics

  ## Returns

  `true` if Leapfrog should be used, `false` otherwise.

  """
  @spec should_use_leapfrog?([number()], [String.t()], stats()) :: boolean()
  def should_use_leapfrog?(pattern_cards, join_vars, stats) do
    k = length(pattern_cards)

    # Need at least 3 patterns for Leapfrog to be beneficial
    # With 2 patterns, hash join is typically better
    if k < 3 do
      false
    else
      # Compare Leapfrog cost to pairwise hash join cascade
      lf_cost = leapfrog_cost(pattern_cards, join_vars, stats)
      cascade_cost = estimate_pairwise_cascade_cost(pattern_cards, join_vars, stats)

      lf_cost.total < cascade_cost.total
    end
  end

  @doc """
  Computes the total cost of a query plan.

  ## Arguments

  - `plan` - List of operations with their costs

  ## Returns

  Combined cost estimate.

  """
  @spec total_plan_cost([cost()]) :: cost()
  def total_plan_cost(costs) when is_list(costs) do
    Enum.reduce(costs, build_cost(0.0, 0.0, 0.0), &add_costs/2)
  end

  # ===========================================================================
  # Public API - Graph-Aware Costs for Quad Patterns
  # ===========================================================================

  @doc """
  Estimates the cost of switching between graphs during query execution.

  Graph switching occurs when a query transitions from processing patterns
  in one graph to processing patterns in another graph. This has a cost
  due to:
  - Changing the column family prefix in RocksDB
  - Potential cache misses when accessing a different graph's data
  - Context switching in the query executor

  ## Returns

  Cost estimate for a single graph switch operation.

  ## Examples

      iex> CostModel.graph_switch_cost()
      %{cpu: 5.0, io: 20.0, memory: 0.0, total: 25.0}

  """
  @spec graph_switch_cost() :: cost()
  def graph_switch_cost do
    # CPU: Minimal overhead for changing graph context
    cpu = 5.0

    # I/O: Column family prefix change has small I/O cost
    io = 20.0

    # Memory: No additional memory required
    memory = 0.0

    build_cost(cpu, io, memory)
  end

  @doc """
  Estimates the cost of a cross-graph join between two quad patterns.

  Cross-graph joins are more expensive than single-graph joins because:
  - May require iterating over multiple graphs
  - Cannot use graph-scoped index optimizations
  - Higher cardinality due to independent graph domains

  ## Arguments

  - `left_card` - Cardinality of the left (build) input
  - `right_card` - Cardinality of the right (probe) input
  - `num_graphs` - Number of graphs to iterate over (if cross-graph)

  ## Returns

  Cost estimate with component breakdown.

  ## Examples

      # Same-graph join (num_graphs = 1)
      iex> CostModel.cross_graph_join_cost(1000, 500, 1)
      # Similar to regular hash join

      # Cross-graph join (num_graphs = 5)
      iex> CostModel.cross_graph_join_cost(1000, 500, 5)
      # Higher cost due to graph iteration

  """
  @spec cross_graph_join_cost(number(), number(), pos_integer()) :: cost()
  def cross_graph_join_cost(left_card, right_card, num_graphs \\ 1) do
    # Base hash join cost
    base_cost = hash_join_cost(left_card, right_card)

    if num_graphs <= 1 do
      # Single graph - same as regular hash join
      base_cost
    else
      # Cross-graph: multiply by graph iteration factor
      #
      # ## Logarithmic Scaling for Cross-Graph Joins
      #
      # We use ln(num_graphs + 1) rather than linear scaling because:
      #
      # 1. **Avoids over-penalizing many small graphs**: A query across 100 small
      #    graphs (10 quads each) shouldn't be 100x more expensive than a query
      #    across one graph with 1000 quads. Log scaling captures the reality that
      #    graph iteration has diminishing marginal cost.
      #
      # 2. **Reflects implementation reality**: The executor processes graphs
      #    iteratively, but the overhead per graph decreases with more graphs due
      #    to amortized setup costs and caching effects.
      #
      # 3. **Practical cost model**: Linear scaling would make cross-graph queries
      #    appear prohibitively expensive, causing the optimizer to incorrectly
      #    favor suboptimal plans.
      #
      # The +1 offset ensures ln(1) = 0 for single-graph queries (no penalty).
      #
      # Example multipliers:
      # - 2 graphs  → ln(3) ≈ 1.1x cost
      # - 10 graphs → ln(11) ≈ 2.4x cost
      # - 100 graphs → ln(101) ≈ 4.6x cost
      # - 1000 graphs → ln(1001) ≈ 6.9x cost
      #
      # Note: We use natural log (ln) rather than log2 here because the graph
      # iteration factor doesn't have a direct relationship to binary tree
      # height—it's more of an amortization factor.
      #
      graph_multiplier = :math.log(num_graphs + 1)

      %{
        cpu: base_cost.cpu * graph_multiplier,
        io: base_cost.io * graph_multiplier,
        memory: base_cost.memory,
        total: 0.0
      }
      |> update_total()
    end
  end

  @doc """
  Estimates the cost of an index scan for a quad pattern.

  Similar to triple index scan but accounts for the graph position.
  Quad patterns have four possible scan types:
  - Point lookup: All four positions bound (S, P, O, G)
  - Prefix scan: Some prefix positions bound
  - Full scan: No bound positions

  ## Arguments

  - `scan_type` - Type of scan (:point_lookup, :prefix_scan, :full_scan)
  - `estimated_results` - Expected number of results
  - `stats` - Database statistics (optional)

  ## Returns

  Cost estimate with component breakdown.

  ## Examples

      # Point lookup for fully bound quad pattern
      iex> CostModel.quad_index_scan_cost(:point_lookup, 1, stats)

      # Graph-scoped prefix scan (most common)
      iex> CostModel.quad_index_scan_cost(:prefix_scan, 500, stats)

      # Cross-graph full scan (most expensive)
      iex> CostModel.quad_index_scan_cost(:full_scan, 100_000, stats)

  """
  @spec quad_index_scan_cost(scan_type(), number(), stats()) :: cost()
  def quad_index_scan_cost(scan_type, estimated_results, stats) do
    index_scan_cost(scan_type, estimated_results, stats, :quad)
  end

  @doc """
  Determines the scan type for a quad pattern based on bound positions.

  ## Arguments

  - `pattern` - Quad pattern {:quad, s, p, o, g}

  ## Returns

  The appropriate scan type.

  ## Examples

      # Fully bound quad pattern
      iex> CostModel.quad_pattern_scan_type({:quad, 1, 2, 3, 0})
      :point_lookup

      # Graph-scoped pattern (graph bound, some SPO bound)
      iex> CostModel.quad_pattern_scan_type({:quad, 1, {:variable, "p"}, {:variable, "o"}, 0})
      :prefix_scan

      # Cross-graph pattern (graph unbound)
      iex> CostModel.quad_pattern_scan_type({:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}})
      :full_scan

  """
  @spec quad_pattern_scan_type(quad_pattern()) :: scan_type()
  def quad_pattern_scan_type({:quad, s, p, o, g}) do
    bound_count = count_bound_quad_positions(s, p, o, g)

    cond do
      bound_count == 4 -> :point_lookup
      bound_count == 0 -> :full_scan
      true -> :prefix_scan
    end
  end

  @doc """
  Estimates the cost of a quad pattern scan including index access and result processing.

  This is the quad-specific equivalent of `pattern_cost/2` for triple patterns.

  ## Arguments

  - `pattern` - Quad pattern {:quad, s, p, o, g}
  - `stats` - Database statistics

  ## Returns

  Cost estimate with component breakdown.

  """
  @dialyzer {:nowarn_function, quad_pattern_cost: 2}
  @spec quad_pattern_cost(quad_pattern(), stats()) :: cost()
  def quad_pattern_cost(pattern, stats) do
    scan_type = quad_pattern_scan_type(pattern)

    # Use QuadCardinality for accurate estimation when available
    # QuadCardinality.estimate_pattern/2 returns a raw float (cardinality),
    # consistent with Cardinality.estimate_pattern/2 for triple patterns
    estimated_results =
      if Code.ensure_loaded?(QuadCardinality) do
        try do
          QuadCardinality.estimate_pattern(pattern, stats)
        rescue
          _ -> fallback_quad_estimate(pattern, stats)
        end
      else
        fallback_quad_estimate(pattern, stats)
      end

    # Base scan cost
    scan_cost = quad_index_scan_cost(scan_type, estimated_results, stats)

    # Add post-filter cost for patterns that need additional filtering
    post_filter_cost = quad_needs_post_filter?(pattern, scan_cost, estimated_results)

    add_costs(scan_cost, post_filter_cost)
  end

  # ===========================================================================
  # Private Helpers - Quad Pattern Support
  # ===========================================================================

  # Count bound positions in a quad pattern
  defp count_bound_quad_positions(s, p, o, g) do
    [s, p, o, g]
    |> Enum.count(&bound?/1)
  end

  # Fallback estimation when QuadCardinality is not available
  defp fallback_quad_estimate({:quad, s, p, o, g}, stats) do
    base_count = Map.get(stats, :quad_count, 10_000)

    # Estimate based on bound positions
    s_mult = if bound?(s), do: 0.01, else: 1.0
    p_mult = if bound?(p), do: 0.1, else: 1.0
    o_mult = if bound?(o), do: 0.1, else: 1.0
    g_mult = if bound?(g), do: 0.01, else: 1.0

    max(1.0, base_count * s_mult * p_mult * o_mult * g_mult)
  end

  # Check if quad pattern needs post-filtering
  defp quad_needs_post_filter?({:quad, s, p, o, g}, _scan_cost, estimated_results) do
    # Similar to triple patterns, some quad patterns require additional filtering
    # when the bound positions don't align perfectly with the index structure
    needs_filter =
      bound?(s) and not bound?(p) and bound?(o) and not bound?(g)

    if needs_filter do
      # Must read more tuples and filter
      filter_ratio = 2.0
      filter_cpu = estimated_results * filter_ratio * @comparison_cost
      build_cost(filter_cpu, 0.0, 0.0)
    else
      build_cost(0.0, 0.0, 0.0)
    end
  end

  # Update the total cost field after modifications
  defp update_total(cost) do
    %{cost | total: cost.cpu + cost.io + cost.memory}
  end

  @doc """
  Compares two costs and returns which is cheaper.

  ## Arguments

  - `cost1` - First cost estimate
  - `cost2` - Second cost estimate

  ## Returns

  `:lt` if cost1 < cost2, `:gt` if cost1 > cost2, `:eq` if equal.

  """
  @spec compare_costs(cost(), cost()) :: :lt | :gt | :eq
  def compare_costs(cost1, cost2) do
    cond do
      cost1.total < cost2.total -> :lt
      cost1.total > cost2.total -> :gt
      true -> :eq
    end
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp build_cost(cpu, io, memory) do
    %{
      cpu: cpu * 1.0,
      io: io * 1.0,
      memory: memory * 1.0,
      total: cpu + io + memory
    }
  end

  defp add_costs(cost1, cost2) do
    %{
      cpu: cost1.cpu + cost2.cpu,
      io: cost1.io + cost2.io,
      memory: cost1.memory + cost2.memory,
      total: cost1.total + cost2.total
    }
  end

  defp count_bound_positions(s, p, o) do
    [s, p, o]
    |> Enum.count(&bound?/1)
  end

  defp bound?({:variable, _}), do: false
  defp bound?({:blank_node, _}), do: false
  defp bound?(_), do: true

  defp needs_post_filter?({:triple, s, p, o}) do
    # S?O pattern uses OSP index but needs to filter by subject
    # which requires reading O-S pairs and checking S matches
    bound?(s) and not bound?(p) and bound?(o)
  end

  defp estimate_leapfrog_output(pattern_cards, join_vars, stats) do
    # Simplified AGM bound estimation
    # For a query with k patterns and shared variables,
    # the output is bounded by the geometric mean of cardinalities
    # raised to the power of 1/rho where rho is the fractional edge cover number

    if Enum.empty?(join_vars) do
      # No shared variables = Cartesian product
      Enum.reduce(pattern_cards, 1.0, &(&1 * &2))
    else
      # Approximate: geometric mean adjusted by join selectivity
      k = length(pattern_cards)
      geometric_mean = :math.pow(Enum.reduce(pattern_cards, 1.0, &(&1 * &2)), 1 / k)

      # Selectivity factor based on number of join variables
      # More join variables = more selective
      num_join_vars = length(join_vars)
      triple_count = Map.get(stats, :triple_count, 10_000)

      distinct_avg =
        (Map.get(stats, :distinct_subjects, 1000) +
           Map.get(stats, :distinct_predicates, 100) +
           Map.get(stats, :distinct_objects, 2000)) / 3

      selectivity = :math.pow(distinct_avg / triple_count, num_join_vars / 2)

      max(1.0, geometric_mean * selectivity)
    end
  end

  defp estimate_pairwise_cascade_cost(pattern_cards, join_vars, stats) do
    # Estimate cost of joining patterns pairwise left-to-right
    case pattern_cards do
      [] ->
        build_cost(0.0, 0.0, 0.0)

      [single] ->
        index_scan_cost(:prefix_scan, single, stats)

      [first | rest] ->
        # Start with first pattern
        {total_cost, _current_card} =
          Enum.reduce(rest, {build_cost(0.0, 0.0, 0.0), first}, fn right_card,
                                                                   {acc_cost, left_card} ->
            # Use hash join for each pair
            join_cost = hash_join_cost(left_card, right_card)

            # Estimate output cardinality
            output_card = Cardinality.estimate_join(left_card, right_card, join_vars, stats)

            {add_costs(acc_cost, join_cost), output_card}
          end)

        total_cost
    end
  end
end
