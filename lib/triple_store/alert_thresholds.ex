defmodule TripleStore.AlertThresholds do
  @moduledoc """
  Default alert thresholds for quad store monitoring.

  These thresholds can be configured via application environment:

      config :triple_store, :alert_thresholds,
        graph_size_warning: 1_000_000,
        graph_size_critical: 10_000_000,
        slow_query_ms: 1000,
        ...

  """

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Default alert threshold configuration.

  ## Graph Size Thresholds

  - `:graph_size_warning` - Quad count before warning (default: 1,000,000)
  - `:graph_size_critical` - Quad count before critical (default: 10,000,000)

  ## Query Performance Thresholds

  - `:slow_query_ms` - Query duration before slow alert (default: 1000ms)
  - `:slow_cross_graph_ms` - Cross-graph query duration before alert (default: 5000ms)
  - `:max_cross_graphs` - Maximum graphs before large query alert (default: 10)

  ## Graph Enumeration Thresholds

  - `:slow_graph_enumeration_ms` - Enumeration duration before alert (default: 1000ms)
  - `:max_graphs_to_list` - Maximum graphs before warning (default: 1000)

  ## Growth Rate Thresholds

  - `:rapid_growth_rate` - Quads/hour before rapid growth alert (default: 10,000)
  - `:stale_hours` - Hours without access before stale alert (default: 24)

  """
  @spec defaults() :: keyword()
  def defaults do
    [
      # Graph size thresholds
      graph_size_warning: 1_000_000,
      graph_size_critical: 10_000_000,

      # Query performance
      slow_query_ms: 1000,
      slow_cross_graph_ms: 5000,
      max_cross_graphs: 10,

      # Graph enumeration
      slow_graph_enumeration_ms: 1000,
      max_graphs_to_list: 1000,

      # Growth rate
      rapid_growth_rate: 10_000,
      stale_hours: 24
    ]
  end

  @doc """
  Gets a specific alert threshold value.

  Returns the configured value or the default if not configured.

  ## Examples

      iex> TripleStore.AlertThresholds.get(:slow_query_ms)
      1000

  """
  @spec get(atom()) :: term()
  def get(key) do
    case Application.fetch_env(:triple_store, :alert_thresholds) do
      {:ok, config} when is_list(config) ->
        Keyword.get(config, key, Keyword.get(defaults(), key))

      :error ->
        Keyword.get(defaults(), key)
    end
  end

  @doc """
  Gets all alert thresholds, merging application config with defaults.

  Application config takes precedence over defaults.

  ## Examples

      # In config/config.exs
      config :triple_store, :alert_thresholds,
        slow_query_ms: 2000  # Override default

      # At runtime
      TripleStore.AlertThresholds.all()
      # => [slow_query_ms: 2000, graph_size_warning: 1_000_000, ...]

  """
  @spec all() :: keyword()
  def all do
    case Application.fetch_env(:triple_store, :alert_thresholds) do
      {:ok, config} when is_list(config) ->
        Keyword.merge(defaults(), config)

      :error ->
        defaults()
    end
  end

  @doc """
  Sets an alert threshold at runtime.

  ## Examples

      :ok = TripleStore.AlertThresholds.set(:slow_query_ms, 2000)

  """
  @spec set(atom(), term()) :: :ok
  def set(key, value) do
    current = all()
    updated = Keyword.put(current, key, value)
    Application.put_env(:triple_store, :alert_thresholds, updated)
    :ok
  end

  # ===========================================================================
  # Threshold Categories
  # ===========================================================================

  @doc """
  Returns all graph size thresholds.
  """
  @spec graph_size_thresholds() :: keyword()
  def graph_size_thresholds do
    all()
    |> Keyword.take([:graph_size_warning, :graph_size_critical])
  end

  @doc """
  Returns all query performance thresholds.
  """
  @spec query_performance_thresholds() :: keyword()
  def query_performance_thresholds do
    all()
    |> Keyword.take([:slow_query_ms, :slow_cross_graph_ms, :max_cross_graphs])
  end

  @doc """
  Returns all growth rate thresholds.
  """
  @spec growth_rate_thresholds() :: keyword()
  def growth_rate_thresholds do
    all()
    |> Keyword.take([:rapid_growth_rate, :stale_hours])
  end

  # ===========================================================================
  # Validation
  # ===========================================================================

  @doc """
  Validates that all configured thresholds are within acceptable ranges.

  Returns `:ok` if all thresholds are valid, or `{:error, warnings}` if any
  thresholds are outside recommended ranges.

  """
  @spec validate() :: :ok | {:error, [String.t()]}
  def validate do
    all()
    |> Enum.reduce([], fn {key, value}, acc ->
      case validate_threshold(key, value) do
        :ok -> acc
        {:warning, msg} -> [msg | acc]
      end
    end)
    |> case do
      [] -> :ok
      warnings -> {:error, Enum.reverse(warnings)}
    end
  end

  defp validate_threshold(:graph_size_warning, value) when value < 1000 do
    {:warning, "graph_size_warning (#{value}) is very low; may cause frequent alerts"}
  end

  defp validate_threshold(:graph_size_critical, value) do
    graph_size_warning = get(:graph_size_warning)
    if value < graph_size_warning do
      {:warning, "graph_size_critical (#{value}) is less than graph_size_warning (#{graph_size_warning})"}
    else
      :ok
    end
  end

  defp validate_threshold(:slow_query_ms, value) when value < 10 do
    {:warning, "slow_query_ms (#{value}) is very low; may cause noise"}
  end

  defp validate_threshold(:slow_cross_graph_ms, value) do
    slow_query_ms = get(:slow_query_ms)
    if value < slow_query_ms do
      {:warning, "slow_cross_graph_ms (#{value}) is less than slow_query_ms (#{slow_query_ms})"}
    else
      :ok
    end
  end

  defp validate_threshold(_key, _value), do: :ok
end
