defmodule TripleStore.Config do
  @moduledoc """
  Configuration management for the TripleStore.

  This module provides centralized configuration with support for:

  - **Application environment**: Configuration via `config/config.exs`
  - **Runtime overrides**: Configuration via function parameters
  - **Presets**: Pre-defined configurations for common use cases
  - **Validation**: Type checking and constraint validation

  ## Configuration Hierarchy

  Configuration is resolved in this order (later overrides earlier):

  1. Default values defined in this module
  2. Application environment (`Application.get_env(:triple_store, ...)`)
  3. Runtime options passed to functions

  ## Configuration Categories

  ### Query Configuration

  - `:query_timeout` - Maximum query execution time in ms (default: 30_000)
  - `:max_bind_join_results` - Maximum results for bind joins (default: 100_000)
  - `:max_distinct_size` - Maximum DISTINCT result set size (default: 1_000_000)
  - `:max_order_by_size` - Maximum ORDER BY result set size (default: 1_000_000)

  ### Loader Configuration

  - `:loader_batch_size` - Batch size for bulk loading (default: 10_000)
  - `:loader_max_file_size` - Maximum file size for loading (default: 100MB)

  ### Reasoner Configuration

  - `:max_iterations` - Maximum reasoning iterations (default: 1000)
  - `:max_depth` - Maximum reasoning depth (default: 100)
  - `:reasoning_profile` - Default reasoning profile (default: :owl2rl)

  ### Cache Configuration

  - `:plan_cache_max_size` - Maximum entries in plan cache (default: 1000)
  - `:stats_cache_refresh_interval` - Statistics refresh interval in ms (default: 60_000)

  ## Usage

      # Get a configuration value
      timeout = TripleStore.Config.get(:query_timeout)

      # Get with runtime override
      timeout = TripleStore.Config.get(:query_timeout, opts)

      # Get all configuration for a category
      query_config = TripleStore.Config.query_config()

  ## Presets

      # Apply a preset
      TripleStore.Config.preset(:production_large_memory)

  """

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Configuration key"
  @type config_key ::
          :query_timeout
          | :max_bind_join_results
          | :max_distinct_size
          | :max_order_by_size
          | :loader_batch_size
          | :loader_max_file_size
          | :max_iterations
          | :max_depth
          | :reasoning_profile
          | :plan_cache_max_size
          | :stats_cache_refresh_interval

  @typedoc "Configuration preset name"
  @type preset :: :development | :production_large_memory | :production_low_memory | :query_heavy

  # ===========================================================================
  # Default Values
  # ===========================================================================

  @defaults %{
    # Query configuration
    query_timeout: 30_000,
    max_bind_join_results: 100_000,
    max_distinct_size: 1_000_000,
    max_order_by_size: 1_000_000,

    # Loader configuration
    loader_batch_size: 10_000,
    # 100 MB
    loader_max_file_size: 100 * 1024 * 1024,

    # Reasoner configuration
    max_iterations: 1000,
    max_depth: 100,
    reasoning_profile: :owl2rl,

    # Cache configuration
    plan_cache_max_size: 1000,
    # 1 minute
    stats_cache_refresh_interval: 60_000
  }

  # ===========================================================================
  # Presets
  # ===========================================================================

  @presets %{
    development: %{
      query_timeout: 60_000,
      max_bind_join_results: 10_000,
      max_distinct_size: 100_000,
      max_order_by_size: 100_000,
      plan_cache_max_size: 100
    },
    production_large_memory: %{
      query_timeout: 30_000,
      max_bind_join_results: 500_000,
      max_distinct_size: 10_000_000,
      max_order_by_size: 10_000_000,
      plan_cache_max_size: 10_000,
      stats_cache_refresh_interval: 30_000
    },
    production_low_memory: %{
      query_timeout: 30_000,
      max_bind_join_results: 50_000,
      max_distinct_size: 100_000,
      max_order_by_size: 100_000,
      plan_cache_max_size: 500,
      stats_cache_refresh_interval: 120_000
    },
    query_heavy: %{
      query_timeout: 60_000,
      max_bind_join_results: 1_000_000,
      max_distinct_size: 10_000_000,
      max_order_by_size: 10_000_000,
      plan_cache_max_size: 50_000,
      stats_cache_refresh_interval: 10_000
    }
  }

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Returns the default value for a configuration key.

  ## Examples

      iex> TripleStore.Config.default(:query_timeout)
      30_000

  """
  @spec default(config_key()) :: term()
  def default(key) when is_map_key(@defaults, key) do
    Map.fetch!(@defaults, key)
  end

  @doc """
  Gets a configuration value.

  Resolution order:
  1. Runtime options (if provided)
  2. Application environment
  3. Default value

  ## Arguments

  - `key` - Configuration key
  - `opts` - Optional runtime overrides

  ## Examples

      # Get with defaults
      timeout = TripleStore.Config.get(:query_timeout)

      # Get with runtime override
      timeout = TripleStore.Config.get(:query_timeout, query_timeout: 60_000)

  """
  @spec get(config_key(), keyword()) :: term()
  def get(key, opts \\ []) when is_map_key(@defaults, key) do
    # Check runtime options first
    case Keyword.fetch(opts, key) do
      {:ok, value} ->
        value

      :error ->
        # Check application environment
        case Application.get_env(:triple_store, key) do
          nil -> Map.fetch!(@defaults, key)
          value -> value
        end
    end
  end

  @doc """
  Gets all configuration values with optional overrides.

  ## Examples

      config = TripleStore.Config.all()
      config = TripleStore.Config.all(query_timeout: 60_000)

  """
  @spec all(keyword()) :: map()
  def all(opts \\ []) do
    Map.new(@defaults, fn {key, default} ->
      value = get(key, opts)
      {key, if(value == nil, do: default, else: value)}
    end)
  end

  @doc """
  Gets query-related configuration.

  ## Examples

      %{
        timeout: 30_000,
        max_bind_join_results: 100_000,
        max_distinct_size: 1_000_000,
        max_order_by_size: 1_000_000
      } = TripleStore.Config.query_config()

  """
  @spec query_config(keyword()) :: map()
  def query_config(opts \\ []) do
    %{
      timeout: get(:query_timeout, opts),
      max_bind_join_results: get(:max_bind_join_results, opts),
      max_distinct_size: get(:max_distinct_size, opts),
      max_order_by_size: get(:max_order_by_size, opts)
    }
  end

  @doc """
  Gets loader-related configuration.

  ## Examples

      %{
        batch_size: 10_000,
        max_file_size: 104857600
      } = TripleStore.Config.loader_config()

  """
  @spec loader_config(keyword()) :: map()
  def loader_config(opts \\ []) do
    %{
      batch_size: get(:loader_batch_size, opts),
      max_file_size: get(:loader_max_file_size, opts)
    }
  end

  @doc """
  Gets reasoner-related configuration.

  ## Examples

      %{
        max_iterations: 1000,
        max_depth: 100,
        profile: :owl2rl
      } = TripleStore.Config.reasoner_config()

  """
  @spec reasoner_config(keyword()) :: map()
  def reasoner_config(opts \\ []) do
    %{
      max_iterations: get(:max_iterations, opts),
      max_depth: get(:max_depth, opts),
      profile: get(:reasoning_profile, opts)
    }
  end

  @doc """
  Gets cache-related configuration.

  ## Examples

      %{
        plan_cache_max_size: 1000,
        stats_cache_refresh_interval: 60_000
      } = TripleStore.Config.cache_config()

  """
  @spec cache_config(keyword()) :: map()
  def cache_config(opts \\ []) do
    %{
      plan_cache_max_size: get(:plan_cache_max_size, opts),
      stats_cache_refresh_interval: get(:stats_cache_refresh_interval, opts)
    }
  end

  @doc """
  Returns the configuration values for a preset.

  ## Presets

  - `:development` - Relaxed timeouts, smaller caches
  - `:production_large_memory` - Large caches, aggressive refresh
  - `:production_low_memory` - Conservative memory usage
  - `:query_heavy` - Optimized for frequent complex queries

  ## Examples

      config = TripleStore.Config.preset(:production_large_memory)
      # => %{plan_cache_max_size: 10_000, ...}

  """
  @spec preset(preset()) :: map()
  def preset(name) when is_map_key(@presets, name) do
    Map.fetch!(@presets, name)
  end

  @doc """
  Lists all available preset names.

  ## Examples

      [:development, :production_large_memory, ...] = TripleStore.Config.preset_names()

  """
  @spec preset_names() :: [preset()]
  def preset_names do
    Map.keys(@presets)
  end

  @doc """
  Lists all configuration keys.

  ## Examples

      [:query_timeout, :max_bind_join_results, ...] = TripleStore.Config.keys()

  """
  @spec keys() :: [config_key()]
  def keys do
    Map.keys(@defaults)
  end

  @doc """
  Validates a configuration value.

  ## Examples

      :ok = TripleStore.Config.validate(:query_timeout, 30_000)
      {:error, reason} = TripleStore.Config.validate(:query_timeout, -1)

  """
  @spec validate(config_key(), term()) :: :ok | {:error, String.t()}
  def validate(key, value) do
    do_validate(key, value)
  end

  # ===========================================================================
  # Validation
  # ===========================================================================

  defp do_validate(:query_timeout, value) when is_integer(value) and value > 0, do: :ok
  defp do_validate(:query_timeout, _), do: {:error, "query_timeout must be a positive integer"}

  defp do_validate(:max_bind_join_results, value) when is_integer(value) and value > 0, do: :ok

  defp do_validate(:max_bind_join_results, _),
    do: {:error, "max_bind_join_results must be a positive integer"}

  defp do_validate(:max_distinct_size, value) when is_integer(value) and value > 0, do: :ok

  defp do_validate(:max_distinct_size, _),
    do: {:error, "max_distinct_size must be a positive integer"}

  defp do_validate(:max_order_by_size, value) when is_integer(value) and value > 0, do: :ok

  defp do_validate(:max_order_by_size, _),
    do: {:error, "max_order_by_size must be a positive integer"}

  defp do_validate(:loader_batch_size, value) when is_integer(value) and value > 0, do: :ok

  defp do_validate(:loader_batch_size, _),
    do: {:error, "loader_batch_size must be a positive integer"}

  defp do_validate(:loader_max_file_size, value) when is_integer(value) and value > 0, do: :ok

  defp do_validate(:loader_max_file_size, _),
    do: {:error, "loader_max_file_size must be a positive integer"}

  defp do_validate(:max_iterations, value) when is_integer(value) and value > 0, do: :ok
  defp do_validate(:max_iterations, _), do: {:error, "max_iterations must be a positive integer"}

  defp do_validate(:max_depth, value) when is_integer(value) and value > 0, do: :ok
  defp do_validate(:max_depth, _), do: {:error, "max_depth must be a positive integer"}

  defp do_validate(:reasoning_profile, value) when value in [:rdfs, :owl2rl, :all], do: :ok

  defp do_validate(:reasoning_profile, _),
    do: {:error, "reasoning_profile must be :rdfs, :owl2rl, or :all"}

  defp do_validate(:plan_cache_max_size, value) when is_integer(value) and value > 0, do: :ok

  defp do_validate(:plan_cache_max_size, _),
    do: {:error, "plan_cache_max_size must be a positive integer"}

  defp do_validate(:stats_cache_refresh_interval, value) when is_integer(value) and value > 0,
    do: :ok

  defp do_validate(:stats_cache_refresh_interval, _),
    do: {:error, "stats_cache_refresh_interval must be a positive integer"}

  defp do_validate(key, _value) do
    {:error, "unknown configuration key: #{inspect(key)}"}
  end
end
