defmodule TripleStore.Metrics do
  @moduledoc """
  Metrics collection and aggregation for the TripleStore.

  This module provides a GenServer-based metrics collector that attaches to
  telemetry events and maintains aggregated statistics. The metrics are useful
  for monitoring, alerting, and performance analysis.

  ## Collected Metrics

  ### Query Duration Histogram

  Tracks query execution times with percentile breakdown:
  - p50, p90, p95, p99 latencies
  - Total query count
  - Mean and max duration

  ### Insert/Delete Throughput

  Tracks data modification rates:
  - Triples inserted per second
  - Triples deleted per second
  - Rolling window averages

  ### Cache Hit Rate

  Tracks cache effectiveness:
  - Hit count and miss count
  - Hit rate percentage
  - Per-cache-type breakdown (plan, query, stats)

  ### Reasoning Metrics

  Tracks materialization performance:
  - Total iterations across all materializations
  - Derived facts per iteration
  - Materialization durations

  ## Usage

      # Start the metrics collector (typically via supervision tree)
      {:ok, _pid} = TripleStore.Metrics.start_link()

      # Get all metrics
      metrics = TripleStore.Metrics.get_all()

      # Get specific metric categories
      query_metrics = TripleStore.Metrics.query_metrics()
      cache_metrics = TripleStore.Metrics.cache_metrics()

      # Reset metrics
      TripleStore.Metrics.reset()

  ## Configuration

  - `:name` - Process name (default: `TripleStore.Metrics`)
  - `:histogram_buckets` - Duration buckets in ms (default: [1, 5, 10, 25, 50, 100, 250, 500, 1000])

  """

  use GenServer

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Query duration metrics"
  @type query_metrics :: %{
          count: non_neg_integer(),
          total_duration_ms: float(),
          min_duration_ms: float() | nil,
          max_duration_ms: float() | nil,
          mean_duration_ms: float(),
          histogram: %{atom() => non_neg_integer()},
          percentiles: %{atom() => float()}
        }

  @typedoc "Throughput metrics"
  @type throughput_metrics :: %{
          insert_count: non_neg_integer(),
          delete_count: non_neg_integer(),
          insert_triple_count: non_neg_integer(),
          delete_triple_count: non_neg_integer(),
          window_start: integer()
        }

  @typedoc "Cache metrics"
  @type cache_metrics :: %{
          hits: non_neg_integer(),
          misses: non_neg_integer(),
          hit_rate: float(),
          by_type: %{atom() => %{hits: non_neg_integer(), misses: non_neg_integer()}}
        }

  @typedoc "Reasoning metrics"
  @type reasoning_metrics :: %{
          materialization_count: non_neg_integer(),
          total_iterations: non_neg_integer(),
          total_derived: non_neg_integer(),
          total_duration_ms: float()
        }

  @typedoc "All metrics"
  @type all_metrics :: %{
          query: query_metrics(),
          throughput: throughput_metrics(),
          cache: cache_metrics(),
          reasoning: reasoning_metrics(),
          collected_at: DateTime.t()
        }

  # ===========================================================================
  # Constants
  # ===========================================================================

  @default_name __MODULE__

  # Default histogram buckets in milliseconds
  @default_buckets [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000]

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the metrics collector.

  ## Options

  - `:name` - Process name (default: `TripleStore.Metrics`)
  - `:histogram_buckets` - Duration buckets in ms

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns all collected metrics.
  """
  @spec get_all(keyword()) :: all_metrics()
  def get_all(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :get_all)
  end

  @doc """
  Returns query duration metrics.
  """
  @spec query_metrics(keyword()) :: query_metrics()
  def query_metrics(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :query_metrics)
  end

  @doc """
  Returns throughput metrics.
  """
  @spec throughput_metrics(keyword()) :: throughput_metrics()
  def throughput_metrics(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :throughput_metrics)
  end

  @doc """
  Returns cache hit/miss metrics.
  """
  @spec cache_metrics(keyword()) :: cache_metrics()
  def cache_metrics(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :cache_metrics)
  end

  @doc """
  Returns reasoning metrics.
  """
  @spec reasoning_metrics(keyword()) :: reasoning_metrics()
  def reasoning_metrics(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :reasoning_metrics)
  end

  @doc """
  Resets all metrics to initial values.
  """
  @spec reset(keyword()) :: :ok
  def reset(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :reset)
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(opts) do
    buckets = Keyword.get(opts, :histogram_buckets, @default_buckets)

    state = initial_state(buckets)

    # Attach telemetry handlers using shared utility
    handlers =
      TripleStore.Telemetry.attach_metrics_handlers(
        self(),
        "triple_store_metrics_#{inspect(self())}"
      )

    {:ok, Map.put(state, :handlers, handlers)}
  end

  @impl true
  def terminate(_reason, state) do
    TripleStore.Telemetry.detach_metrics_handlers(Map.get(state, :handlers, []))
    :ok
  end

  @impl true
  def handle_call(:get_all, _from, state) do
    metrics = %{
      query: compute_query_metrics(state),
      throughput: compute_throughput_metrics(state),
      cache: compute_cache_metrics(state),
      reasoning: compute_reasoning_metrics(state),
      collected_at: DateTime.utc_now()
    }

    {:reply, metrics, state}
  end

  @impl true
  def handle_call(:query_metrics, _from, state) do
    {:reply, compute_query_metrics(state), state}
  end

  @impl true
  def handle_call(:throughput_metrics, _from, state) do
    {:reply, compute_throughput_metrics(state), state}
  end

  @impl true
  def handle_call(:cache_metrics, _from, state) do
    {:reply, compute_cache_metrics(state), state}
  end

  @impl true
  def handle_call(:reasoning_metrics, _from, state) do
    {:reply, compute_reasoning_metrics(state), state}
  end

  @impl true
  def handle_call(:reset, _from, state) do
    {:reply, :ok, initial_state(state.buckets)}
  end

  @impl true
  def handle_info({:telemetry_event, event, measurements, metadata}, state) do
    state = handle_telemetry_event(event, measurements, metadata, state)
    {:noreply, state}
  end

  # ===========================================================================
  # Event Handling
  # ===========================================================================

  defp handle_telemetry_event(
         [:triple_store, :query, :execute, :stop],
         measurements,
         _metadata,
         state
       ) do
    duration_ms = get_duration_ms(measurements)
    update_query_metrics(state, duration_ms)
  end

  defp handle_telemetry_event([:triple_store, :insert, :stop], measurements, metadata, state) do
    count = Map.get(metadata, :count, 1)
    duration_ms = get_duration_ms(measurements)
    update_insert_metrics(state, count, duration_ms)
  end

  defp handle_telemetry_event([:triple_store, :delete, :stop], measurements, metadata, state) do
    count = Map.get(metadata, :count, 1)
    duration_ms = get_duration_ms(measurements)
    update_delete_metrics(state, count, duration_ms)
  end

  defp handle_telemetry_event([:triple_store, :load, :stop], measurements, metadata, state) do
    count = Map.get(metadata, :total_count, Map.get(metadata, :count, 0))
    duration_ms = get_duration_ms(measurements)
    update_insert_metrics(state, count, duration_ms)
  end

  defp handle_telemetry_event(
         [:triple_store, :cache, cache_type, :hit],
         _measurements,
         _metadata,
         state
       ) do
    update_cache_hit(state, cache_type)
  end

  defp handle_telemetry_event(
         [:triple_store, :cache, cache_type, :miss],
         _measurements,
         _metadata,
         state
       ) do
    update_cache_miss(state, cache_type)
  end

  defp handle_telemetry_event(
         [:triple_store, :reasoner, :materialize, :stop],
         measurements,
         metadata,
         state
       ) do
    duration_ms = get_duration_ms(measurements)
    iterations = Map.get(metadata, :iterations, 0)
    derived = Map.get(metadata, :total_derived, 0)
    update_materialization_metrics(state, duration_ms, iterations, derived)
  end

  defp handle_telemetry_event(
         [:triple_store, :reasoner, :materialize, :iteration],
         measurements,
         _metadata,
         state
       ) do
    derivations = Map.get(measurements, :derivations, 0)
    update_iteration_metrics(state, derivations)
  end

  defp handle_telemetry_event(_event, _measurements, _metadata, state) do
    state
  end

  # Use shared duration extraction utility from Telemetry module
  defp get_duration_ms(measurements) do
    TripleStore.Telemetry.duration_ms(measurements)
  end

  # ===========================================================================
  # State Updates
  # ===========================================================================

  defp initial_state(buckets) do
    %{
      buckets: buckets,
      # Query metrics
      query_count: 0,
      query_durations: [],
      query_total_ms: 0.0,
      query_min_ms: nil,
      query_max_ms: nil,
      query_histogram: initialize_histogram(buckets),
      # Throughput metrics
      insert_count: 0,
      insert_triple_count: 0,
      delete_count: 0,
      delete_triple_count: 0,
      window_start: System.monotonic_time(:millisecond),
      # Cache metrics
      cache_hits: %{},
      cache_misses: %{},
      # Reasoning metrics
      materialization_count: 0,
      total_iterations: 0,
      total_derived: 0,
      reasoning_total_ms: 0.0
    }
  end

  defp initialize_histogram(buckets) do
    buckets
    |> Enum.map(fn b -> {bucket_key(b), 0} end)
    |> Map.new()
    |> Map.put(:inf, 0)
  end

  defp bucket_key(ms), do: String.to_atom("le_#{ms}ms")

  defp update_query_metrics(state, duration_ms) do
    # Update histogram bucket
    bucket = find_bucket(duration_ms, state.buckets)
    histogram = Map.update!(state.query_histogram, bucket, &(&1 + 1))

    # Keep last 1000 durations for percentile calculation
    durations =
      [duration_ms | state.query_durations]
      |> Enum.take(1000)

    %{
      state
      | query_count: state.query_count + 1,
        query_durations: durations,
        query_total_ms: state.query_total_ms + duration_ms,
        query_min_ms: min_or_first(state.query_min_ms, duration_ms),
        query_max_ms: max_or_first(state.query_max_ms, duration_ms),
        query_histogram: histogram
    }
  end

  defp find_bucket(duration_ms, buckets) do
    Enum.find(buckets, fn b -> duration_ms <= b end)
    |> case do
      nil -> :inf
      b -> bucket_key(b)
    end
  end

  defp min_or_first(nil, val), do: val
  defp min_or_first(existing, val), do: min(existing, val)

  defp max_or_first(nil, val), do: val
  defp max_or_first(existing, val), do: max(existing, val)

  defp update_insert_metrics(state, count, _duration_ms) do
    %{
      state
      | insert_count: state.insert_count + 1,
        insert_triple_count: state.insert_triple_count + count
    }
  end

  defp update_delete_metrics(state, count, _duration_ms) do
    %{
      state
      | delete_count: state.delete_count + 1,
        delete_triple_count: state.delete_triple_count + count
    }
  end

  defp update_cache_hit(state, cache_type) do
    hits = Map.update(state.cache_hits, cache_type, 1, &(&1 + 1))
    %{state | cache_hits: hits}
  end

  defp update_cache_miss(state, cache_type) do
    misses = Map.update(state.cache_misses, cache_type, 1, &(&1 + 1))
    %{state | cache_misses: misses}
  end

  defp update_materialization_metrics(state, duration_ms, iterations, derived) do
    %{
      state
      | materialization_count: state.materialization_count + 1,
        total_iterations: state.total_iterations + iterations,
        total_derived: state.total_derived + derived,
        reasoning_total_ms: state.reasoning_total_ms + duration_ms
    }
  end

  defp update_iteration_metrics(state, derivations) do
    %{
      state
      | total_derived: state.total_derived + derivations
    }
  end

  # ===========================================================================
  # Metrics Computation
  # ===========================================================================

  defp compute_query_metrics(state) do
    mean =
      if state.query_count > 0 do
        state.query_total_ms / state.query_count
      else
        0.0
      end

    percentiles = compute_percentiles(state.query_durations)

    %{
      count: state.query_count,
      total_duration_ms: state.query_total_ms,
      min_duration_ms: state.query_min_ms,
      max_duration_ms: state.query_max_ms,
      mean_duration_ms: mean,
      histogram: state.query_histogram,
      percentiles: percentiles
    }
  end

  defp compute_percentiles([]), do: %{p50: 0.0, p90: 0.0, p95: 0.0, p99: 0.0}

  defp compute_percentiles(durations) do
    sorted = Enum.sort(durations)
    len = length(sorted)

    %{
      p50: percentile(sorted, len, 50),
      p90: percentile(sorted, len, 90),
      p95: percentile(sorted, len, 95),
      p99: percentile(sorted, len, 99)
    }
  end

  defp percentile(sorted, len, p) do
    idx = min(round(len * p / 100), len - 1)
    Enum.at(sorted, idx, 0.0)
  end

  defp compute_throughput_metrics(state) do
    now = System.monotonic_time(:millisecond)
    window_duration_ms = now - state.window_start

    # Calculate rates (per second)
    {insert_rate, delete_rate} =
      if window_duration_ms > 0 do
        seconds = window_duration_ms / 1000.0

        {
          state.insert_triple_count / seconds,
          state.delete_triple_count / seconds
        }
      else
        {0.0, 0.0}
      end

    %{
      insert_count: state.insert_count,
      delete_count: state.delete_count,
      insert_triple_count: state.insert_triple_count,
      delete_triple_count: state.delete_triple_count,
      insert_rate_per_sec: insert_rate,
      delete_rate_per_sec: delete_rate,
      window_duration_ms: window_duration_ms,
      window_start: state.window_start
    }
  end

  defp compute_cache_metrics(state) do
    total_hits = state.cache_hits |> Map.values() |> Enum.sum()
    total_misses = state.cache_misses |> Map.values() |> Enum.sum()
    total = total_hits + total_misses

    hit_rate =
      if total > 0 do
        total_hits / total
      else
        0.0
      end

    # Build per-type metrics
    all_types =
      MapSet.new(Map.keys(state.cache_hits) ++ Map.keys(state.cache_misses))

    by_type =
      Enum.map(all_types, fn type ->
        hits = Map.get(state.cache_hits, type, 0)
        misses = Map.get(state.cache_misses, type, 0)
        total_type = hits + misses

        rate =
          if total_type > 0 do
            hits / total_type
          else
            0.0
          end

        {type, %{hits: hits, misses: misses, hit_rate: rate}}
      end)
      |> Map.new()

    %{
      hits: total_hits,
      misses: total_misses,
      hit_rate: hit_rate,
      by_type: by_type
    }
  end

  defp compute_reasoning_metrics(state) do
    %{
      materialization_count: state.materialization_count,
      total_iterations: state.total_iterations,
      total_derived: state.total_derived,
      total_duration_ms: state.reasoning_total_ms
    }
  end
end
