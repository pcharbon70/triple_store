defmodule TripleStore.Reasoner.ReasoningMode do
  @moduledoc """
  Defines reasoning modes for OWL 2 RL inference.

  A reasoning mode determines *when* and *how* inferences are computed.
  Different modes provide different trade-offs between query latency,
  update cost, and memory usage.

  ## Built-in Modes

  ### `:materialized` - Full Materialization
  All inferences are pre-computed and stored when data is loaded or modified.
  - **Query time**: O(1) lookup - fastest queries
  - **Update time**: O(rules × delta) - slower updates
  - **Memory**: O(derived) - stores all derived triples
  - **Best for**: Read-heavy workloads, complex queries, static data

  ### `:query_time` - On-Demand Reasoning
  No pre-computation; inferences are computed during query execution.
  - **Query time**: O(rules × data) - slower queries
  - **Update time**: O(1) - instant updates
  - **Memory**: O(1) - no derived storage
  - **Best for**: Write-heavy workloads, simple queries, memory-constrained

  ### `:hybrid` - Selective Materialization
  Common inferences are pre-computed; rare inferences computed on-demand.
  - **Query time**: Between materialized and query_time
  - **Update time**: Between materialized and query_time
  - **Memory**: Partial derived storage
  - **Best for**: Mixed workloads, balancing performance

  ## Mode Configuration

  Modes can be configured with additional options:

  ### Materialized Mode Options
  - `:parallel` - Enable parallel rule evaluation (default: false)
  - `:max_iterations` - Maximum fixpoint iterations (default: 1000)

  ### Hybrid Mode Options
  - `:materialized_rules` - Rules to materialize (default: RDFS rules)
  - `:query_time_rules` - Rules to evaluate at query time

  ### Query-Time Mode Options
  - `:max_depth` - Maximum backward chaining depth (default: 10)
  - `:cache_results` - Cache query-time inferences (default: false)

  ## Usage

      # Get mode information
      info = ReasoningMode.info(:materialized)

      # Validate mode configuration
      {:ok, config} = ReasoningMode.validate_config(:hybrid,
        materialized_rules: [:scm_sco, :cax_sco]
      )

      # Get default configuration for a mode
      config = ReasoningMode.default_config(:materialized)
  """

  alias TripleStore.Reasoner.ReasoningProfile

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Built-in reasoning mode names"
  @type mode_name :: :materialized | :query_time | :hybrid | :none

  @typedoc "Mode configuration options"
  @type mode_config :: %{
          mode: mode_name(),
          parallel: boolean(),
          max_iterations: pos_integer(),
          max_depth: pos_integer(),
          cache_results: boolean(),
          materialized_rules: [atom()] | nil,
          query_time_rules: [atom()] | nil
        }

  @typedoc "Mode information"
  @type mode_info :: %{
          name: mode_name(),
          description: String.t(),
          query_complexity: String.t(),
          update_complexity: String.t(),
          memory_usage: String.t(),
          best_for: String.t()
        }

  # ============================================================================
  # Default Configurations
  # ============================================================================

  @default_max_iterations 1000
  @default_max_depth 10

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Returns the list of valid mode names.
  """
  @spec mode_names() :: [mode_name()]
  def mode_names, do: [:materialized, :query_time, :hybrid, :none]

  @doc """
  Validates a mode name.

  ## Examples

      iex> ReasoningMode.valid_mode?(:materialized)
      true

      iex> ReasoningMode.valid_mode?(:unknown)
      false
  """
  @spec valid_mode?(atom()) :: boolean()
  def valid_mode?(name), do: name in mode_names()

  @doc """
  Returns information about a reasoning mode.

  ## Examples

      info = ReasoningMode.info(:materialized)
      info.query_complexity  # => "O(1)"
  """
  @spec info(mode_name()) :: mode_info()
  def info(:none) do
    %{
      name: :none,
      description: "No reasoning - returns only explicit triples",
      query_complexity: "O(1)",
      update_complexity: "O(1)",
      memory_usage: "None",
      best_for: "When reasoning is not needed"
    }
  end

  def info(:materialized) do
    %{
      name: :materialized,
      description: "All inferences pre-computed and stored",
      query_complexity: "O(1) lookup",
      update_complexity: "O(rules × delta)",
      memory_usage: "O(derived triples)",
      best_for: "Read-heavy workloads, complex queries, static data"
    }
  end

  def info(:query_time) do
    %{
      name: :query_time,
      description: "Inferences computed on-demand during queries",
      query_complexity: "O(rules × data)",
      update_complexity: "O(1)",
      memory_usage: "O(1) - no derived storage",
      best_for: "Write-heavy workloads, simple queries, memory-constrained"
    }
  end

  def info(:hybrid) do
    %{
      name: :hybrid,
      description: "Common inferences materialized, rare ones computed on-demand",
      query_complexity: "Between O(1) and O(rules × data)",
      update_complexity: "Between O(1) and O(rules × delta)",
      memory_usage: "Partial derived storage",
      best_for: "Mixed workloads, balancing query and update performance"
    }
  end

  @doc """
  Returns the default configuration for a mode.

  ## Examples

      config = ReasoningMode.default_config(:materialized)
      # => %{mode: :materialized, parallel: false, max_iterations: 1000, ...}
  """
  @spec default_config(mode_name()) :: mode_config()
  def default_config(:none) do
    %{
      mode: :none,
      parallel: false,
      max_iterations: 0,
      max_depth: 0,
      cache_results: false,
      materialized_rules: nil,
      query_time_rules: nil
    }
  end

  def default_config(:materialized) do
    %{
      mode: :materialized,
      parallel: false,
      max_iterations: @default_max_iterations,
      max_depth: 0,
      cache_results: false,
      materialized_rules: nil,
      query_time_rules: nil
    }
  end

  def default_config(:query_time) do
    %{
      mode: :query_time,
      parallel: false,
      max_iterations: 0,
      max_depth: @default_max_depth,
      cache_results: false,
      materialized_rules: nil,
      query_time_rules: nil
    }
  end

  def default_config(:hybrid) do
    # By default, materialize RDFS rules and compute OWL rules at query time
    rdfs_rules = [:scm_sco, :scm_spo, :cax_sco, :prp_spo1, :prp_dom, :prp_rng]

    owl_rules = [
      :prp_trp, :prp_symp, :prp_inv1, :prp_inv2, :prp_fp, :prp_ifp,
      :eq_ref, :eq_sym, :eq_trans, :eq_rep_s, :eq_rep_p, :eq_rep_o,
      :cls_hv1, :cls_hv2, :cls_svf1, :cls_svf2, :cls_avf
    ]

    %{
      mode: :hybrid,
      parallel: false,
      max_iterations: @default_max_iterations,
      max_depth: @default_max_depth,
      cache_results: true,
      materialized_rules: rdfs_rules,
      query_time_rules: owl_rules
    }
  end

  @doc """
  Validates and normalizes mode configuration.

  Takes a mode name and optional configuration options, validates them,
  and returns a complete configuration map.

  ## Options

  ### Common Options
  - `:parallel` - Enable parallel rule evaluation (boolean)

  ### Materialized Mode
  - `:max_iterations` - Maximum fixpoint iterations (pos_integer)

  ### Query-Time Mode
  - `:max_depth` - Maximum backward chaining depth (pos_integer)
  - `:cache_results` - Cache query-time results (boolean)

  ### Hybrid Mode
  - `:materialized_rules` - Rules to materialize (list of atoms)
  - `:query_time_rules` - Rules for query-time evaluation (list of atoms)

  ## Examples

      {:ok, config} = ReasoningMode.validate_config(:materialized, parallel: true)

      {:ok, config} = ReasoningMode.validate_config(:hybrid,
        materialized_rules: [:scm_sco, :cax_sco]
      )
  """
  @spec validate_config(mode_name(), keyword()) :: {:ok, mode_config()} | {:error, term()}
  def validate_config(mode, opts \\ [])

  def validate_config(mode, opts) when mode in [:none, :materialized, :query_time, :hybrid] do
    base_config = default_config(mode)

    with {:ok, config} <- apply_options(base_config, opts),
         :ok <- validate_rules(config) do
      {:ok, config}
    end
  end

  def validate_config(mode, _opts) do
    {:error, {:invalid_mode, mode, "expected one of: #{inspect(mode_names())}"}}
  end

  @doc """
  Validates configuration, raising on error.
  """
  @spec validate_config!(mode_name(), keyword()) :: mode_config()
  def validate_config!(mode, opts \\ []) do
    case validate_config(mode, opts) do
      {:ok, config} -> config
      {:error, reason} -> raise ArgumentError, "Invalid mode configuration: #{inspect(reason)}"
    end
  end

  @doc """
  Suggests an appropriate mode based on workload characteristics.

  ## Parameters

  - `:read_heavy` - True if read operations dominate
  - `:write_heavy` - True if write operations dominate
  - `:memory_constrained` - True if memory is limited
  - `:complex_queries` - True if queries involve multiple inference steps

  ## Examples

      mode = ReasoningMode.suggest_mode(read_heavy: true, complex_queries: true)
      # => :materialized

      mode = ReasoningMode.suggest_mode(write_heavy: true, memory_constrained: true)
      # => :query_time
  """
  @spec suggest_mode(keyword()) :: mode_name()
  def suggest_mode(opts \\ []) do
    read_heavy = Keyword.get(opts, :read_heavy, false)
    write_heavy = Keyword.get(opts, :write_heavy, false)
    memory_constrained = Keyword.get(opts, :memory_constrained, false)
    complex_queries = Keyword.get(opts, :complex_queries, false)

    cond do
      memory_constrained and write_heavy -> :query_time
      memory_constrained -> :hybrid
      write_heavy and not complex_queries -> :query_time
      read_heavy or complex_queries -> :materialized
      true -> :hybrid
    end
  end

  @doc """
  Returns the profile to use for materialization in a given mode.

  For `:materialized` mode, returns the full profile.
  For `:hybrid` mode, returns a custom profile with only materialized rules.
  For `:query_time` and `:none` modes, returns `:none`.
  """
  @spec materialization_profile(mode_config(), atom()) :: atom() | {:custom, [atom()]}
  def materialization_profile(%{mode: :none}, _profile), do: :none

  def materialization_profile(%{mode: :materialized}, profile), do: profile

  def materialization_profile(%{mode: :query_time}, _profile), do: :none

  def materialization_profile(%{mode: :hybrid, materialized_rules: rules}, _profile)
      when is_list(rules) and length(rules) > 0 do
    {:custom, rules}
  end

  def materialization_profile(%{mode: :hybrid}, _profile), do: :rdfs

  @doc """
  Returns whether the mode requires materialization on data load.
  """
  @spec requires_materialization?(mode_config()) :: boolean()
  def requires_materialization?(%{mode: :materialized}), do: true
  def requires_materialization?(%{mode: :hybrid}), do: true
  def requires_materialization?(%{mode: :query_time}), do: false
  def requires_materialization?(%{mode: :none}), do: false

  @doc """
  Returns whether the mode supports incremental updates.
  """
  @spec supports_incremental?(mode_config()) :: boolean()
  def supports_incremental?(%{mode: :materialized}), do: true
  def supports_incremental?(%{mode: :hybrid}), do: true
  def supports_incremental?(%{mode: :query_time}), do: false
  def supports_incremental?(%{mode: :none}), do: false

  @doc """
  Returns whether the mode requires backward chaining for queries.
  """
  @spec requires_backward_chaining?(mode_config()) :: boolean()
  def requires_backward_chaining?(%{mode: :materialized}), do: false
  def requires_backward_chaining?(%{mode: :hybrid}), do: true
  def requires_backward_chaining?(%{mode: :query_time}), do: true
  def requires_backward_chaining?(%{mode: :none}), do: false

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp apply_options(config, opts) do
    try do
      config =
        Enum.reduce(opts, config, fn {key, value}, acc ->
          case key do
            :parallel when is_boolean(value) ->
              %{acc | parallel: value}

            :max_iterations when is_integer(value) and value > 0 ->
              %{acc | max_iterations: value}

            :max_depth when is_integer(value) and value >= 0 ->
              %{acc | max_depth: value}

            :cache_results when is_boolean(value) ->
              %{acc | cache_results: value}

            :materialized_rules when is_list(value) ->
              %{acc | materialized_rules: value}

            :query_time_rules when is_list(value) ->
              %{acc | query_time_rules: value}

            _ ->
              throw({:invalid_option, key, value})
          end
        end)

      {:ok, config}
    catch
      {:invalid_option, key, value} ->
        {:error, {:invalid_option, key, "invalid value: #{inspect(value)}"}}
    end
  end

  defp validate_rules(%{materialized_rules: rules}) when is_list(rules) do
    available = ReasoningProfile.available_rules()
    unknown = Enum.reject(rules, &(&1 in available))

    if Enum.empty?(unknown) do
      :ok
    else
      {:error, {:unknown_rules, unknown}}
    end
  end

  defp validate_rules(_config), do: :ok
end
