defmodule TripleStore.Reasoner.ReasoningConfig do
  @moduledoc """
  Unified reasoning configuration combining profile and mode settings.

  This module provides a single point of configuration for the reasoning
  subsystem, combining:
  - **Profile**: Which rules to use (RDFS, OWL 2 RL, custom)
  - **Mode**: When to compute inferences (materialized, query-time, hybrid)

  ## Configuration Structure

  A reasoning configuration contains:
  - `:profile` - The reasoning profile (`:rdfs`, `:owl2rl`, `:custom`, `:none`)
  - `:mode` - The reasoning mode (`:materialized`, `:query_time`, `:hybrid`, `:none`)
  - `:mode_config` - Mode-specific configuration options
  - `:profile_opts` - Profile-specific options (rules, exclude)

  ## Usage

      # Create a simple configuration
      {:ok, config} = ReasoningConfig.new(profile: :owl2rl, mode: :materialized)

      # Create with mode options
      {:ok, config} = ReasoningConfig.new(
        profile: :owl2rl,
        mode: :materialized,
        parallel: true,
        max_iterations: 500
      )

      # Create hybrid configuration
      {:ok, config} = ReasoningConfig.new(
        profile: :owl2rl,
        mode: :hybrid,
        materialized_rules: [:scm_sco, :cax_sco],
        cache_results: true
      )

      # Get effective rules for materialization
      rules = ReasoningConfig.materialization_rules(config)

  ## Presets

  Common configurations are available as presets:

      # Fast queries, pre-compute everything
      config = ReasoningConfig.preset(:full_materialization)

      # Minimal memory, compute on demand
      config = ReasoningConfig.preset(:minimal_memory)

      # Balanced approach
      config = ReasoningConfig.preset(:balanced)
  """

  alias TripleStore.Reasoner.{GraphReasoningConfig, ReasoningMode, ReasoningProfile, Rules}

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Reasoning scope for quad store"
  @type reasoning_scope :: :local | :global | :hybrid

  @typedoc "Complete reasoning configuration"
  @type t :: %__MODULE__{
          profile: ReasoningProfile.profile_name(),
          mode: ReasoningMode.mode_name(),
          mode_config: ReasoningMode.mode_config(),
          profile_opts: keyword(),
          scope: reasoning_scope(),
          graph_configs: %{non_neg_integer() => GraphReasoningConfig.t()} | nil,
          tbox_graph: non_neg_integer() | nil,
          inferred_graph: non_neg_integer() | :separate | nil,
          created_at: DateTime.t()
        }

  defstruct [
    :profile,
    :mode,
    :mode_config,
    :profile_opts,
    :scope,
    :graph_configs,
    :tbox_graph,
    :inferred_graph,
    :created_at
  ]

  # ============================================================================
  # Public API
  # ============================================================================

  @doc """
  Creates a new reasoning configuration.

  ## Options

  ### Profile Options
  - `:profile` - Reasoning profile (default: `:owl2rl`)
  - `:rules` - Custom rules for `:custom` profile
  - `:exclude` - Rules to exclude from profile

  ### Mode Options
  - `:mode` - Reasoning mode (default: `:materialized`)
  - `:parallel` - Enable parallel evaluation
  - `:max_iterations` - Maximum fixpoint iterations
  - `:max_depth` - Maximum backward chaining depth
  - `:cache_results` - Cache query-time results
  - `:materialized_rules` - Rules to materialize in hybrid mode
  - `:query_time_rules` - Rules for query-time in hybrid mode

  ### Graph Scope Options (for quad store)
  - `:scope` - Reasoning scope: `:local` (default), `:global`, or `:hybrid`
  - `:graph_configs` - Map of graph_id to GraphReasoningConfig for per-graph configuration
  - `:tbox_graph` - Graph ID containing shared TBox (nil = each graph has own TBox)
  - `:inferred_graph` - Graph ID for global inferences (nil = same as premises, `:separate` = dedicated graph)

  ## Examples

      {:ok, config} = ReasoningConfig.new(profile: :rdfs, mode: :materialized)

      {:ok, config} = ReasoningConfig.new(
        profile: :owl2rl,
        mode: :hybrid,
        materialized_rules: [:scm_sco, :cax_sco]
      )

      # Graph-local reasoning (default)
      {:ok, config} = ReasoningConfig.new(
        profile: :owl2rl,
        scope: :local
      )

      # Global reasoning across all graphs
      {:ok, config} = ReasoningConfig.new(
        profile: :owl2rl,
        scope: :global,
        tbox_graph: 0
      )

      # Hybrid with per-graph configuration
      {:ok, config} = ReasoningConfig.new(
        profile: :owl2rl,
        scope: :hybrid,
        tbox_graph: 0,
        inferred_graph: :separate
      )
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts \\ []) do
    profile = Keyword.get(opts, :profile, :owl2rl)
    mode = Keyword.get(opts, :mode, :materialized)
    scope = Keyword.get(opts, :scope, :local)

    # Separate profile and mode options
    profile_opts = Keyword.take(opts, [:rules, :exclude])

    mode_opts =
      Keyword.take(opts, [
        :parallel,
        :max_iterations,
        :max_depth,
        :cache_results,
        :materialized_rules,
        :query_time_rules
      ])

    graph_scope_opts =
      Keyword.take(opts, [
        :graph_configs,
        :tbox_graph,
        :inferred_graph
      ])

    with :ok <- validate_profile(profile, profile_opts),
         :ok <- validate_scope(scope),
         {:ok, mode_config} <- ReasoningMode.validate_config(mode, mode_opts) do
      config = %__MODULE__{
        profile: profile,
        mode: mode,
        mode_config: mode_config,
        profile_opts: profile_opts,
        scope: scope,
        graph_configs: Keyword.get(graph_scope_opts, :graph_configs),
        tbox_graph: Keyword.get(graph_scope_opts, :tbox_graph),
        inferred_graph: Keyword.get(graph_scope_opts, :inferred_graph),
        created_at: DateTime.utc_now()
      }

      {:ok, config}
    end
  end

  @doc """
  Creates a new configuration, raising on error.
  """
  @spec new!(keyword()) :: t()
  def new!(opts \\ []) do
    case new(opts) do
      {:ok, config} -> config
      {:error, reason} -> raise ArgumentError, "Invalid configuration: #{inspect(reason)}"
    end
  end

  @doc """
  Returns a preset configuration.

  ## Available Presets

  - `:full_materialization` - OWL 2 RL with full materialization
  - `:rdfs_only` - RDFS profile with materialization
  - `:minimal_memory` - Query-time mode, no storage overhead
  - `:balanced` - Hybrid mode with RDFS materialized
  - `:none` - No reasoning

  ## Examples

      config = ReasoningConfig.preset(:full_materialization)
  """
  @spec preset(atom()) :: t()
  def preset(:full_materialization) do
    new!(profile: :owl2rl, mode: :materialized)
  end

  def preset(:rdfs_only) do
    new!(profile: :rdfs, mode: :materialized)
  end

  def preset(:minimal_memory) do
    new!(profile: :owl2rl, mode: :query_time)
  end

  def preset(:balanced) do
    new!(profile: :owl2rl, mode: :hybrid)
  end

  def preset(:none) do
    new!(profile: :none, mode: :none)
  end

  @doc """
  Returns the list of available preset names.
  """
  @spec preset_names() :: [atom()]
  def preset_names do
    [:full_materialization, :rdfs_only, :minimal_memory, :balanced, :none]
  end

  @doc """
  Returns the rules to use for materialization.

  For `:materialized` mode, returns the full profile rules.
  For `:hybrid` mode, returns the materialized subset.
  For `:query_time` and `:none` modes, returns empty list.
  """
  @spec materialization_rules(t()) :: [atom()]
  def materialization_rules(%__MODULE__{mode: :none}), do: []

  def materialization_rules(%__MODULE__{mode: :query_time}), do: []

  def materialization_rules(%__MODULE__{
        mode: :materialized,
        profile: profile,
        profile_opts: opts
      }) do
    case ReasoningProfile.rules_for(profile, opts) do
      {:ok, rules} -> Enum.map(rules, & &1.name)
      {:error, _} -> []
    end
  end

  def materialization_rules(%__MODULE__{
        mode: :hybrid,
        mode_config: %{materialized_rules: rules}
      })
      when is_list(rules) do
    rules
  end

  def materialization_rules(%__MODULE__{mode: :hybrid}) do
    # Default to RDFS rules for hybrid mode
    Rules.rdfs_rule_names()
  end

  @doc """
  Returns the rules to evaluate at query time.

  For `:query_time` mode, returns the full profile rules.
  For `:hybrid` mode, returns the query-time subset.
  For `:materialized` and `:none` modes, returns empty list.
  """
  @spec query_time_rules(t()) :: [atom()]
  def query_time_rules(%__MODULE__{mode: :none}), do: []

  def query_time_rules(%__MODULE__{mode: :materialized}), do: []

  def query_time_rules(%__MODULE__{
        mode: :query_time,
        profile: profile,
        profile_opts: opts
      }) do
    case ReasoningProfile.rules_for(profile, opts) do
      {:ok, rules} ->
        Enum.map(rules, & &1.name)

      {:error, reason} ->
        require Logger
        Logger.warning("Failed to get rules for profile #{inspect(profile)}: #{inspect(reason)}")
        []
    end
  end

  def query_time_rules(%__MODULE__{
        mode: :hybrid,
        mode_config: %{query_time_rules: rules}
      })
      when is_list(rules) do
    rules
  end

  def query_time_rules(%__MODULE__{mode: :hybrid, profile: profile, profile_opts: opts}) do
    # Return all rules minus the materialized ones
    # Default to RDFS rules for hybrid mode when no explicit materialized_rules are set
    materialized = Rules.rdfs_rule_names()

    case ReasoningProfile.rules_for(profile, opts) do
      {:ok, rules} ->
        rules
        |> Enum.map(& &1.name)
        |> Enum.reject(&(&1 in materialized))

      {:error, reason} ->
        require Logger
        Logger.warning("Failed to get rules for profile #{inspect(profile)}: #{inspect(reason)}")
        []
    end
  end

  @doc """
  Returns whether the configuration requires materialization on data load.
  """
  @spec requires_materialization?(t()) :: boolean()
  def requires_materialization?(%__MODULE__{mode_config: config}) do
    ReasoningMode.requires_materialization?(config)
  end

  @doc """
  Returns whether the configuration supports incremental updates.
  """
  @spec supports_incremental?(t()) :: boolean()
  def supports_incremental?(%__MODULE__{mode_config: config}) do
    ReasoningMode.supports_incremental?(config)
  end

  @doc """
  Returns whether the configuration requires backward chaining for queries.
  """
  @spec requires_backward_chaining?(t()) :: boolean()
  def requires_backward_chaining?(%__MODULE__{mode_config: config}) do
    ReasoningMode.requires_backward_chaining?(config)
  end

  @doc """
  Returns a summary of the configuration.
  """
  @spec summary(t()) :: map()
  def summary(%__MODULE__{} = config) do
    %{
      profile: config.profile,
      mode: config.mode,
      scope: config.scope,
      materialization_rules: materialization_rules(config),
      query_time_rules: query_time_rules(config),
      requires_materialization: requires_materialization?(config),
      requires_backward_chaining: requires_backward_chaining?(config),
      parallel: config.mode_config.parallel,
      tbox_graph: config.tbox_graph,
      inferred_graph: config.inferred_graph,
      graph_config_count: graph_config_count(config)
    }
  end

  # ============================================================================
  # Graph Scope Query Functions
  # ============================================================================

  @doc """
  Returns the reasoning scope.
  """
  @spec scope(t()) :: reasoning_scope()
  def scope(%__MODULE__{scope: scope}), do: scope

  @doc """
  Returns true if reasoning is graph-local (default).
  """
  @spec local?(t()) :: boolean()
  def local?(%__MODULE__{scope: :local}), do: true
  def local?(%__MODULE__{}), do: false

  @doc """
  Returns true if reasoning is global across all graphs.
  """
  @spec global?(t()) :: boolean()
  def global?(%__MODULE__{scope: :global}), do: true
  def global?(%__MODULE__{}), do: false

  @doc """
  Returns true if reasoning uses hybrid (per-graph) configuration.
  """
  @spec hybrid?(t()) :: boolean()
  def hybrid?(%__MODULE__{scope: :hybrid}), do: true
  def hybrid?(%__MODULE__{}), do: false

  @doc """
  Returns the graph configuration for the given graph ID.
  """
  @spec graph_config(t(), non_neg_integer()) :: {:ok, GraphReasoningConfig.t()} | :error
  def graph_config(%__MODULE__{graph_configs: nil}, _graph_id), do: :error

  def graph_config(%__MODULE__{graph_configs: configs}, graph_id) when is_map(configs) do
    case Map.get(configs, graph_id) do
      nil -> :error
      config -> {:ok, config}
    end
  end

  @doc """
  Returns all graph configurations.
  """
  @spec graph_configs(t()) :: %{non_neg_integer() => GraphReasoningConfig.t()} | nil
  def graph_configs(%__MODULE__{graph_configs: configs}), do: configs

  @doc """
  Returns the number of configured graphs.
  """
  @spec graph_config_count(t()) :: non_neg_integer()
  def graph_config_count(%__MODULE__{graph_configs: nil}), do: 0
  def graph_config_count(%__MODULE__{graph_configs: configs}) when is_map(configs), do: map_size(configs)

  @doc """
  Returns the TBox graph ID if configured.
  """
  @spec tbox_graph(t()) :: non_neg_integer() | nil
  def tbox_graph(%__MODULE__{tbox_graph: graph_id}), do: graph_id

  @doc """
  Returns whether TBox is shared across graphs.
  """
  @spec shared_tbox?(t()) :: boolean()
  def shared_tbox?(%__MODULE__{tbox_graph: nil}), do: false
  def shared_tbox?(%__MODULE__{tbox_graph: _graph_id}), do: true

  @doc """
  Returns the inferred graph ID or :separate if configured.
  """
  @spec inferred_graph(t()) :: non_neg_integer() | :separate | nil
  def inferred_graph(%__MODULE__{inferred_graph: graph}), do: graph

  @doc """
  Returns whether derived quads are stored in a separate graph.
  """
  @spec separate_inferred_graph?(t()) :: boolean()
  def separate_inferred_graph?(%__MODULE__{inferred_graph: :separate}), do: true
  def separate_inferred_graph?(%__MODULE__{inferred_graph: graph_id}) when is_integer(graph_id), do: true
  def separate_inferred_graph?(%__MODULE__{}), do: false

  @doc """
  Adds a graph configuration to the reasoning config.
  """
  @spec put_graph_config(t(), GraphReasoningConfig.t()) :: t()
  def put_graph_config(%__MODULE__{} = config, %GraphReasoningConfig{graph_id: graph_id}) do
    updated_configs =
      config.graph_configs
      |> Map.get(%{})
      |> Map.put(graph_id, %GraphReasoningConfig{graph_id: graph_id})

    %{config | graph_configs: updated_configs}
  end

  @doc """
  Removes a graph configuration from the reasoning config.
  """
  @spec remove_graph_config(t(), non_neg_integer()) :: t()
  def remove_graph_config(%__MODULE__{graph_configs: nil} = config, _graph_id), do: config

  def remove_graph_config(%__MODULE__{graph_configs: configs} = config, graph_id)
      when is_map(configs) do
    %{config | graph_configs: Map.delete(configs, graph_id)}
  end

  @doc """
  Sets the reasoning scope.
  """
  @spec put_scope(t(), reasoning_scope()) :: t()
  def put_scope(%__MODULE__{} = config, scope) when scope in [:local, :global, :hybrid] do
    %{config | scope: scope}
  end

  @doc """
  Sets the TBox graph ID.
  """
  @spec put_tbox_graph(t(), non_neg_integer() | nil) :: t()
  def put_tbox_graph(%__MODULE__{} = config, tbox_graph), do: %{config | tbox_graph: tbox_graph}

  @doc """
  Sets the inferred graph location.
  """
  @spec put_inferred_graph(t(), non_neg_integer() | :separate | nil) :: t()
  def put_inferred_graph(%__MODULE__{} = config, inferred_graph),
    do: %{config | inferred_graph: inferred_graph}

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp validate_profile(profile, opts) do
    case ReasoningProfile.rules_for(profile, opts) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_scope(scope) when scope in [:local, :global, :hybrid], do: :ok
  defp validate_scope(_scope), do: {:error, :invalid_scope}
end
