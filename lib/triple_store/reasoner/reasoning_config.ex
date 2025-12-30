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

  alias TripleStore.Reasoner.{ReasoningMode, ReasoningProfile, Rules}

  # ============================================================================
  # Types
  # ============================================================================

  @typedoc "Complete reasoning configuration"
  @type t :: %__MODULE__{
          profile: ReasoningProfile.profile_name(),
          mode: ReasoningMode.mode_name(),
          mode_config: ReasoningMode.mode_config(),
          profile_opts: keyword(),
          created_at: DateTime.t()
        }

  defstruct [
    :profile,
    :mode,
    :mode_config,
    :profile_opts,
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

  ## Examples

      {:ok, config} = ReasoningConfig.new(profile: :rdfs, mode: :materialized)

      {:ok, config} = ReasoningConfig.new(
        profile: :owl2rl,
        mode: :hybrid,
        materialized_rules: [:scm_sco, :cax_sco]
      )
  """
  @spec new(keyword()) :: {:ok, t()} | {:error, term()}
  def new(opts \\ []) do
    profile = Keyword.get(opts, :profile, :owl2rl)
    mode = Keyword.get(opts, :mode, :materialized)

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

    with :ok <- validate_profile(profile, profile_opts),
         {:ok, mode_config} <- ReasoningMode.validate_config(mode, mode_opts) do
      config = %__MODULE__{
        profile: profile,
        mode: mode,
        mode_config: mode_config,
        profile_opts: profile_opts,
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
    materialized =
      materialization_rules(%__MODULE__{
        mode: :hybrid,
        profile: profile,
        profile_opts: opts,
        mode_config: %{materialized_rules: nil},
        created_at: DateTime.utc_now()
      })

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
      materialization_rules: materialization_rules(config),
      query_time_rules: query_time_rules(config),
      requires_materialization: requires_materialization?(config),
      requires_backward_chaining: requires_backward_chaining?(config),
      parallel: config.mode_config.parallel
    }
  end

  # ============================================================================
  # Private Functions
  # ============================================================================

  defp validate_profile(profile, opts) do
    case ReasoningProfile.rules_for(profile, opts) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
