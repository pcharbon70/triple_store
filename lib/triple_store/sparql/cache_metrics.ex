defmodule TripleStore.SPARQL.CacheMetrics do
  @moduledoc """
  Centralized cache metrics collection and monitoring (S24).

  This module provides a unified interface for collecting and reporting
  cache metrics across the SPARQL query engine, including:
  - Plan cache (query plan caching)
  - Statistics cache (per-graph statistics)
  - Cardinality cache (pattern cardinality estimates)

  ## Cache Metrics Structure

  Each cache reports the following metrics:

      %{
        hits: non_neg_integer(),          # Number of cache hits
        misses: non_neg_integer(),        # Number of cache misses
        evictions: non_neg_integer(),     # Number of entries evicted
        size: non_neg_integer(),          # Current cache size
        max_size: non_neg_integer(),      # Maximum cache size
        hit_rate: float(),                # hits / (hits + misses)
        memory_mb: float()                # Estimated memory usage in MB
      }

  ## Usage

      # Get metrics for all caches
      metrics = CacheMetrics.all_metrics()

      # Get metrics for a specific cache
      plan_metrics = CacheMetrics.metrics(:plan_cache)
      stats_metrics = CacheMetrics.metrics(:stats_cache)

      # Reset metrics (for testing or after configuration changes)
      CacheMetrics.reset_metrics()

      # Attach a telemetry handler for continuous monitoring
      CacheMetrics.attach_telemetry_handler()

  ## Telemetry Events

  The following telemetry events are emitted:

  - `[:triple_store, :cache, :hit]` - Cache hit event
  - `[:triple_store, :cache, :miss]` - Cache miss event
  - `[:triple_store, :cache, :eviction]` - Cache eviction event
  - `[:triple_store, :cache, :snapshot]` - Periodic cache metrics snapshot

  """

  use GenServer

  require Logger

  @telemetry_prefix [:triple_store, :cache]

  # Cache types
  @cache_types [
    :plan_cache,
    :stats_cache,
    :cardinality_cache
  ]

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the cache metrics server.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Gets metrics for all caches.
  """
  def all_metrics do
    GenServer.call(__MODULE__, :all_metrics)
  end

  @doc """
  Gets metrics for a specific cache type.

  ## Parameters

  - `cache_type` - One of `:plan_cache`, `:stats_cache`, `:cardinality_cache`

  ## Returns

  - `{:ok, metrics}` - Map with cache metrics for valid cache type
  - `{:error, :unknown_cache}` - For invalid cache type
  """
  def metrics(cache_type) when cache_type in @cache_types do
    {:ok, GenServer.call(__MODULE__, {:metrics, cache_type})}
  end

  def metrics(_cache_type), do: {:error, :unknown_cache}

  @doc """
  Records a cache hit.

  ## Parameters

  - `cache_type` - Cache that had the hit
  - `metadata` - Optional metadata map
  """
  def record_hit(cache_type, metadata \\ %{}) do
    GenServer.cast(__MODULE__, {:hit, cache_type, metadata})

    :telemetry.execute(
      @telemetry_prefix ++ [:hit],
      %{cache: cache_type},
      metadata
    )

    :ok
  end

  @doc """
  Records a cache miss.

  ## Parameters

  - `cache_type` - Cache that had the miss
  - `metadata` - Optional metadata map
  """
  def record_miss(cache_type, metadata \\ %{}) do
    GenServer.cast(__MODULE__, {:miss, cache_type, metadata})

    :telemetry.execute(
      @telemetry_prefix ++ [:miss],
      %{cache: cache_type},
      metadata
    )

    :ok
  end

  @doc """
  Records a cache eviction.

  ## Parameters

  - `cache_type` - Cache that performed the eviction
  - `metadata` - Optional metadata map
  """
  def record_eviction(cache_type, metadata \\ %{}) do
    GenServer.cast(__MODULE__, {:eviction, cache_type, metadata})

    :telemetry.execute(
      @telemetry_prefix ++ [:eviction],
      %{cache: cache_type},
      metadata
    )

    :ok
  end

  @doc """
  Records cache size change.

  ## Parameters

  - `cache_type` - Cache that changed size
  - `size` - New size of the cache

  Negative sizes are ignored and treated as no-ops.
  """
  def record_size(cache_type, size) when is_integer(size) and size >= 0 do
    GenServer.cast(__MODULE__, {:size, cache_type, size})
    :ok
  end

  def record_size(_cache_type, _size) do
    # Negative or invalid sizes are no-ops
    :ok
  end

  @doc """
  Resets all metrics to zero.
  """
  def reset_metrics do
    GenServer.call(__MODULE__, :reset)
  end

  @doc """
  Resets metrics for a specific cache type.
  """
  def reset_metrics(cache_type) when cache_type in @cache_types do
    GenServer.call(__MODULE__, {:reset, cache_type})
  end

  @doc """
  Gets a snapshot of current metrics as a map.
  Useful for logging or external monitoring systems.
  """
  def snapshot do
    GenServer.call(__MODULE__, :snapshot)
  end

  @doc """
  Attaches a telemetry handler for cache metrics events.

  This attaches handlers that will log cache events and can be
  extended to send to external monitoring systems.
  """
  def attach_telemetry_handler do
    :telemetry.attach(
      "cache-metrics-logger",
      @telemetry_prefix ++ [:hit],
      &handle_hit_event/4,
      nil
    )

    :telemetry.attach(
      "cache-metrics-miss-logger",
      @telemetry_prefix ++ [:miss],
      &handle_miss_event/4,
      nil
    )

    :telemetry.attach(
      "cache-metrics-eviction-logger",
      @telemetry_prefix ++ [:eviction],
      &handle_eviction_event/4,
      nil
    )

    :ok
  end

  def detach_telemetry_handler do
    :telemetry.detach("cache-metrics-logger")
    :telemetry.detach("cache-metrics-miss-logger")
    :telemetry.detach("cache-metrics-eviction-logger")
    :ok
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(_opts) do
    # Initialize metrics for all cache types
    initial_state =
      @cache_types
      |> Enum.map(fn cache_type -> {cache_type, fresh_metrics()} end)
      |> Map.new()

    # Start periodic snapshot timer (every 60 seconds)
    schedule_snapshot()

    {:ok, initial_state}
  end

  @impl true
  def handle_call(:all_metrics, _from, state) do
    # Calculate derived metrics for each cache
    metrics =
      Enum.map(state, fn {cache_type, base_metrics} ->
        {cache_type, calculate_metrics(base_metrics)}
      end)
      |> Map.new()

    {:reply, metrics, state}
  end

  def handle_call({:metrics, cache_type}, _from, state) do
    case Map.get(state, cache_type) do
      nil ->
        {:reply, {:error, :unknown_cache}, state}

      base_metrics ->
        metrics = calculate_metrics(base_metrics)
        {:reply, metrics, state}
    end
  end

  def handle_call(:reset, _from, _state) do
    new_state =
      @cache_types
      |> Enum.map(fn cache_type -> {cache_type, fresh_metrics()} end)
      |> Map.new()

    {:reply, :ok, new_state}
  end

  def handle_call({:reset, cache_type}, _from, state) do
    new_state = Map.put(state, cache_type, fresh_metrics())
    {:reply, :ok, new_state}
  end

  def handle_call(:snapshot, _from, state) do
    snapshot =
      Enum.map(state, fn {cache_type, base_metrics} ->
        metrics = calculate_metrics(base_metrics)
        {cache_type, metrics}
      end)
      |> Map.new()

    # Emit telemetry event with full snapshot
    :telemetry.execute(
      @telemetry_prefix ++ [:snapshot],
      %{cache_count: map_size(state)},
      %{snapshot: snapshot}
    )

    {:reply, snapshot, state}
  end

  @impl true
  def handle_cast({:hit, cache_type, _metadata}, state) do
    new_state =
      Map.update!(state, cache_type, fn metrics ->
        %{metrics | hits: metrics.hits + 1}
      end)

    {:noreply, new_state}
  end

  def handle_cast({:miss, cache_type, _metadata}, state) do
    new_state =
      Map.update!(state, cache_type, fn metrics ->
        %{metrics | misses: metrics.misses + 1}
      end)

    {:noreply, new_state}
  end

  def handle_cast({:eviction, cache_type, _metadata}, state) do
    new_state =
      Map.update!(state, cache_type, fn metrics ->
        %{metrics | evictions: metrics.evictions + 1}
      end)

    {:noreply, new_state}
  end

  def handle_cast({:size, cache_type, size}, state) do
    new_state =
      Map.update!(state, cache_type, fn metrics ->
        %{metrics | size: size}
      end)

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:snapshot_timer, state) do
    # Emit periodic snapshot
    {:reply, _snapshot, _state} = handle_call(:snapshot, nil, state)

    # Reschedule
    schedule_snapshot()

    {:noreply, state}
  end

  # ===========================================================================
  # Telemetry Event Handlers
  # ===========================================================================

  defp handle_hit_event(_event, measurements, metadata, _config) do
    Logger.debug("[CacheMetrics] Hit in cache: #{measurements.cache}")
  end

  defp handle_miss_event(_event, measurements, metadata, _config) do
    Logger.debug("[CacheMetrics] Miss in cache: #{measurements.cache}")
  end

  defp handle_eviction_event(_event, measurements, metadata, _config) do
    Logger.debug("[CacheMetrics] Eviction in cache: #{measurements.cache}")
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp fresh_metrics do
    %{
      hits: 0,
      misses: 0,
      evictions: 0,
      size: 0,
      max_size: nil
    }
  end

  defp calculate_metrics(base_metrics) do
    total_requests = base_metrics.hits + base_metrics.misses

    hit_rate =
      if total_requests > 0 do
        base_metrics.hits / total_requests
      else
        0.0
      end

    # Estimate memory (rough estimate: 100 bytes per entry)
    memory_mb = base_metrics.size * 100 / 1_048_576

    base_metrics
    |> Map.put(:hit_rate, Float.round(hit_rate, 4))
    |> Map.put(:memory_mb, Float.round(memory_mb, 2))
  end

  defp schedule_snapshot do
    # Snapshot every 60 seconds
    Process.send_after(self(), :snapshot_timer, 60_000)
  end

  # ===========================================================================
  # Metrics Formatting
  # ===========================================================================

  @doc """
  Formats cache metrics as a human-readable string.

  For a single cache metrics map, formats those metrics.
  For a map of all cache metrics, formats each cache type.
  """
  def format_metrics(%{hits: hits, misses: misses, evictions: evictions, size: size, max_size: max_size, hit_rate: hit_rate, memory_mb: memory_mb}) do
    """
    Cache Metrics:
    - Hits: #{hits}
    - Misses: #{misses}
    - Evictions: #{evictions}
    - Size: #{size} / #{max_size || "∞"}
    - Hit Rate: #{(hit_rate * 100) |> Float.round(2)}%
    - Memory: #{memory_mb} MB
    """
    |> String.trim()
  end

  def format_metrics(all_metrics) when is_map(all_metrics) do
    Enum.map(all_metrics, fn {cache_type, metrics} ->
      "#{cache_type}:\n  #{format_metrics(metrics) |> String.replace("\n", "\n  ")}"
    end)
    |> Enum.join("\n\n")
  end
end
