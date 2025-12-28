defmodule TripleStore.Prometheus do
  @moduledoc """
  Prometheus metrics integration for the TripleStore.

  This module provides Prometheus-compatible metrics export by attaching
  to telemetry events and maintaining metric values. It supports the standard
  Prometheus text exposition format.

  ## Metric Types

  - **Counters**: Monotonically increasing values (queries, inserts, cache hits)
  - **Histograms**: Distribution of values (query duration, batch sizes)
  - **Gauges**: Point-in-time values (triple count, memory usage)

  ## Usage

  ### Starting the Metrics Server

      # Add to your supervision tree
      children = [
        {TripleStore.Prometheus, []}
      ]

  ### Exposing Metrics (Plug Example)

      get "/metrics" do
        metrics = TripleStore.Prometheus.format()
        conn
        |> put_resp_content_type("text/plain")
        |> send_resp(200, metrics)
      end

  ## Metric Naming Convention

  All metrics follow the Prometheus naming convention:
  - Prefix: `triple_store_`
  - Snake_case names
  - Unit suffix where applicable (e.g., `_seconds`, `_bytes`, `_total`)

  ## Available Metrics

  ### Query Metrics
  - `triple_store_query_duration_seconds` - Query execution time histogram
  - `triple_store_query_total` - Total queries executed (counter)

  ### Data Modification Metrics
  - `triple_store_insert_total` - Total insert operations (counter)
  - `triple_store_insert_triples_total` - Total triples inserted (counter)
  - `triple_store_delete_total` - Total delete operations (counter)
  - `triple_store_delete_triples_total` - Total triples deleted (counter)

  ### Cache Metrics
  - `triple_store_cache_hits_total` - Cache hit count by type (counter)
  - `triple_store_cache_misses_total` - Cache miss count by type (counter)

  ### Reasoning Metrics
  - `triple_store_reasoning_iterations_total` - Total reasoning iterations (counter)
  - `triple_store_reasoning_derived_total` - Total derived facts (counter)
  - `triple_store_reasoning_duration_seconds` - Reasoning duration histogram

  ### Store Metrics (Gauges)
  - `triple_store_triples` - Current triple count (gauge)
  - `triple_store_memory_bytes` - Estimated memory usage (gauge)

  """

  use GenServer

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Metric type"
  @type metric_type :: :counter | :gauge | :histogram

  @typedoc "Metric definition"
  @type metric_def :: %{
          name: String.t(),
          type: metric_type(),
          help: String.t(),
          labels: [atom()]
        }

  @typedoc "Histogram bucket boundaries (in seconds)"
  @type buckets :: [float()]

  # ===========================================================================
  # Constants
  # ===========================================================================

  @default_name __MODULE__

  # Default histogram buckets for query duration (in seconds)
  @default_buckets [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]

  # ===========================================================================
  # Metric Definitions
  # ===========================================================================

  @doc """
  Returns all metric definitions.

  Each metric includes its name, type, help text, and labels.
  """
  @spec metric_definitions() :: [metric_def()]
  def metric_definitions do
    [
      # Query metrics
      %{
        name: "triple_store_query_duration_seconds",
        type: :histogram,
        help: "Query execution time in seconds",
        labels: []
      },
      %{
        name: "triple_store_query_total",
        type: :counter,
        help: "Total number of queries executed",
        labels: []
      },
      %{
        name: "triple_store_query_errors_total",
        type: :counter,
        help: "Total number of query errors",
        labels: []
      },

      # Insert metrics
      %{
        name: "triple_store_insert_total",
        type: :counter,
        help: "Total number of insert operations",
        labels: []
      },
      %{
        name: "triple_store_insert_triples_total",
        type: :counter,
        help: "Total number of triples inserted",
        labels: []
      },

      # Delete metrics
      %{
        name: "triple_store_delete_total",
        type: :counter,
        help: "Total number of delete operations",
        labels: []
      },
      %{
        name: "triple_store_delete_triples_total",
        type: :counter,
        help: "Total number of triples deleted",
        labels: []
      },

      # Load metrics
      %{
        name: "triple_store_load_total",
        type: :counter,
        help: "Total number of load operations",
        labels: []
      },
      %{
        name: "triple_store_load_triples_total",
        type: :counter,
        help: "Total number of triples loaded",
        labels: []
      },

      # Cache metrics
      %{
        name: "triple_store_cache_hits_total",
        type: :counter,
        help: "Total number of cache hits",
        labels: [:cache_type]
      },
      %{
        name: "triple_store_cache_misses_total",
        type: :counter,
        help: "Total number of cache misses",
        labels: [:cache_type]
      },

      # Reasoning metrics
      %{
        name: "triple_store_reasoning_total",
        type: :counter,
        help: "Total number of materialization operations",
        labels: []
      },
      %{
        name: "triple_store_reasoning_iterations_total",
        type: :counter,
        help: "Total number of reasoning iterations",
        labels: []
      },
      %{
        name: "triple_store_reasoning_derived_total",
        type: :counter,
        help: "Total number of derived facts",
        labels: []
      },
      %{
        name: "triple_store_reasoning_duration_seconds",
        type: :histogram,
        help: "Reasoning duration in seconds",
        labels: []
      },

      # Store gauges
      %{
        name: "triple_store_triples",
        type: :gauge,
        help: "Current number of triples in the store",
        labels: []
      },
      %{
        name: "triple_store_memory_bytes",
        type: :gauge,
        help: "Estimated memory usage in bytes",
        labels: []
      },
      %{
        name: "triple_store_index_entries",
        type: :gauge,
        help: "Number of entries in each index",
        labels: [:index]
      }
    ]
  end

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the Prometheus metrics collector.

  ## Options

  - `:name` - Process name (default: `TripleStore.Prometheus`)
  - `:buckets` - Histogram bucket boundaries in seconds

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns metrics in Prometheus text exposition format.

  ## Examples

      text = TripleStore.Prometheus.format()
      # => "# HELP triple_store_query_total Total number of queries executed\\n..."

  """
  @spec format(keyword()) :: String.t()
  def format(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :format)
  end

  @doc """
  Returns raw metric values as a map.

  Useful for testing and programmatic access.
  """
  @spec get_metrics(keyword()) :: map()
  def get_metrics(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :get_metrics)
  end

  @doc """
  Updates gauge metrics from a store.

  Call this periodically to update triple count, memory usage, etc.

  ## Arguments

  - `store` - Store handle from `TripleStore.open/2`

  """
  @spec update_gauges(TripleStore.store(), keyword()) :: :ok
  def update_gauges(store, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.cast(name, {:update_gauges, store})
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
    buckets = Keyword.get(opts, :buckets, @default_buckets)

    state = %{
      buckets: buckets,
      counters: initial_counters(),
      histograms: initial_histograms(buckets),
      gauges: initial_gauges()
    }

    attach_handlers()

    {:ok, state}
  end

  @impl true
  def terminate(_reason, _state) do
    detach_handlers()
    :ok
  end

  @impl true
  def handle_call(:format, _from, state) do
    text = format_prometheus(state)
    {:reply, text, state}
  end

  @impl true
  def handle_call(:get_metrics, _from, state) do
    metrics = %{
      counters: state.counters,
      histograms: state.histograms,
      gauges: state.gauges
    }

    {:reply, metrics, state}
  end

  @impl true
  def handle_call(:reset, _from, state) do
    new_state = %{
      buckets: state.buckets,
      counters: initial_counters(),
      histograms: initial_histograms(state.buckets),
      gauges: initial_gauges()
    }

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_cast({:update_gauges, store}, state) do
    gauges = update_gauge_values(store, state.gauges)
    {:noreply, %{state | gauges: gauges}}
  end

  @impl true
  def handle_info({:telemetry_event, event, measurements, metadata}, state) do
    state = handle_telemetry_event(event, measurements, metadata, state)
    {:noreply, state}
  end

  # ===========================================================================
  # Telemetry Handlers
  # ===========================================================================

  defp attach_handlers do
    pid = self()

    # Query events
    :telemetry.attach(
      handler_id(:query_stop),
      [:triple_store, :query, :execute, :stop],
      fn event, measurements, metadata, _config ->
        send(pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    :telemetry.attach(
      handler_id(:query_exception),
      [:triple_store, :query, :execute, :exception],
      fn event, measurements, metadata, _config ->
        send(pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    # Insert events
    :telemetry.attach(
      handler_id(:insert_stop),
      [:triple_store, :insert, :stop],
      fn event, measurements, metadata, _config ->
        send(pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    # Delete events
    :telemetry.attach(
      handler_id(:delete_stop),
      [:triple_store, :delete, :stop],
      fn event, measurements, metadata, _config ->
        send(pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    # Load events
    :telemetry.attach(
      handler_id(:load_stop),
      [:triple_store, :load, :stop],
      fn event, measurements, metadata, _config ->
        send(pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    # Cache hit events
    :telemetry.attach_many(
      handler_id(:cache_hit),
      [
        [:triple_store, :cache, :plan, :hit],
        [:triple_store, :cache, :query, :hit],
        [:triple_store, :cache, :stats, :hit]
      ],
      fn event, measurements, metadata, _config ->
        send(pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    # Cache miss events
    :telemetry.attach_many(
      handler_id(:cache_miss),
      [
        [:triple_store, :cache, :plan, :miss],
        [:triple_store, :cache, :query, :miss],
        [:triple_store, :cache, :stats, :miss]
      ],
      fn event, measurements, metadata, _config ->
        send(pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )

    # Reasoning events
    :telemetry.attach(
      handler_id(:materialize_stop),
      [:triple_store, :reasoner, :materialize, :stop],
      fn event, measurements, metadata, _config ->
        send(pid, {:telemetry_event, event, measurements, metadata})
      end,
      nil
    )
  end

  defp detach_handlers do
    :telemetry.detach(handler_id(:query_stop))
    :telemetry.detach(handler_id(:query_exception))
    :telemetry.detach(handler_id(:insert_stop))
    :telemetry.detach(handler_id(:delete_stop))
    :telemetry.detach(handler_id(:load_stop))
    :telemetry.detach(handler_id(:cache_hit))
    :telemetry.detach(handler_id(:cache_miss))
    :telemetry.detach(handler_id(:materialize_stop))
  end

  defp handler_id(suffix) do
    "triple_store_prometheus_#{inspect(self())}_#{suffix}"
  end

  # ===========================================================================
  # Event Handling
  # ===========================================================================

  defp handle_telemetry_event([:triple_store, :query, :execute, :stop], measurements, _metadata, state) do
    duration_s = get_duration_seconds(measurements)

    state
    |> increment_counter(:query_total)
    |> observe_histogram(:query_duration, duration_s)
  end

  defp handle_telemetry_event([:triple_store, :query, :execute, :exception], _measurements, _metadata, state) do
    state
    |> increment_counter(:query_errors_total)
  end

  defp handle_telemetry_event([:triple_store, :insert, :stop], _measurements, metadata, state) do
    count = Map.get(metadata, :count, 1)

    state
    |> increment_counter(:insert_total)
    |> increment_counter(:insert_triples_total, count)
  end

  defp handle_telemetry_event([:triple_store, :delete, :stop], _measurements, metadata, state) do
    count = Map.get(metadata, :count, 1)

    state
    |> increment_counter(:delete_total)
    |> increment_counter(:delete_triples_total, count)
  end

  defp handle_telemetry_event([:triple_store, :load, :stop], _measurements, metadata, state) do
    count = Map.get(metadata, :total_count, Map.get(metadata, :count, 0))

    state
    |> increment_counter(:load_total)
    |> increment_counter(:load_triples_total, count)
  end

  defp handle_telemetry_event([:triple_store, :cache, cache_type, :hit], _measurements, _metadata, state) do
    increment_counter_with_label(state, :cache_hits_total, :cache_type, cache_type)
  end

  defp handle_telemetry_event([:triple_store, :cache, cache_type, :miss], _measurements, _metadata, state) do
    increment_counter_with_label(state, :cache_misses_total, :cache_type, cache_type)
  end

  defp handle_telemetry_event([:triple_store, :reasoner, :materialize, :stop], measurements, metadata, state) do
    duration_s = get_duration_seconds(measurements)
    iterations = Map.get(metadata, :iterations, 0)
    derived = Map.get(metadata, :total_derived, 0)

    state
    |> increment_counter(:reasoning_total)
    |> increment_counter(:reasoning_iterations_total, iterations)
    |> increment_counter(:reasoning_derived_total, derived)
    |> observe_histogram(:reasoning_duration, duration_s)
  end

  defp handle_telemetry_event(_event, _measurements, _metadata, state) do
    state
  end

  defp get_duration_seconds(measurements) do
    cond do
      Map.has_key?(measurements, :duration_ms) ->
        measurements.duration_ms / 1000.0

      Map.has_key?(measurements, :duration) ->
        System.convert_time_unit(measurements.duration, :native, :microsecond) / 1_000_000.0

      true ->
        0.0
    end
  end

  # ===========================================================================
  # State Initialization
  # ===========================================================================

  defp initial_counters do
    %{
      query_total: 0,
      query_errors_total: 0,
      insert_total: 0,
      insert_triples_total: 0,
      delete_total: 0,
      delete_triples_total: 0,
      load_total: 0,
      load_triples_total: 0,
      cache_hits_total: %{},
      cache_misses_total: %{},
      reasoning_total: 0,
      reasoning_iterations_total: 0,
      reasoning_derived_total: 0
    }
  end

  defp initial_histograms(buckets) do
    histogram_template = %{
      buckets: Enum.map(buckets, fn b -> {b, 0} end) |> Map.new(),
      sum: 0.0,
      count: 0
    }

    %{
      query_duration: histogram_template,
      reasoning_duration: histogram_template
    }
  end

  defp initial_gauges do
    %{
      triples: 0,
      memory_bytes: 0,
      index_entries: %{}
    }
  end

  # ===========================================================================
  # State Updates
  # ===========================================================================

  defp increment_counter(state, key, amount \\ 1) do
    counters = Map.update!(state.counters, key, &(&1 + amount))
    %{state | counters: counters}
  end

  defp increment_counter_with_label(state, key, _label_name, label_value) do
    counters =
      Map.update!(state.counters, key, fn labels_map ->
        Map.update(labels_map, label_value, 1, &(&1 + 1))
      end)

    %{state | counters: counters}
  end

  defp observe_histogram(state, key, value) do
    histograms =
      Map.update!(state.histograms, key, fn histogram ->
        buckets =
          Enum.map(histogram.buckets, fn {bound, count} ->
            if value <= bound do
              {bound, count + 1}
            else
              {bound, count}
            end
          end)
          |> Map.new()

        %{
          histogram
          | buckets: buckets,
            sum: histogram.sum + value,
            count: histogram.count + 1
        }
      end)

    %{state | histograms: histograms}
  end

  defp update_gauge_values(store, gauges) do
    # Get triple count
    triple_count =
      case TripleStore.Statistics.triple_count(store.db) do
        {:ok, count} -> count
        _ -> gauges.triples
      end

    # Get memory estimate
    memory = :erlang.memory(:total)

    # Get index sizes if Health module is available
    index_entries =
      try do
        TripleStore.Health.get_index_sizes(store.db)
      rescue
        _ -> gauges.index_entries
      end

    %{
      gauges
      | triples: triple_count,
        memory_bytes: memory,
        index_entries: index_entries
    }
  end

  # ===========================================================================
  # Prometheus Formatting
  # ===========================================================================

  defp format_prometheus(state) do
    lines = []

    # Format counters
    lines = lines ++ format_counter("triple_store_query_total", "Total number of queries executed", state.counters.query_total)
    lines = lines ++ format_counter("triple_store_query_errors_total", "Total number of query errors", state.counters.query_errors_total)
    lines = lines ++ format_counter("triple_store_insert_total", "Total number of insert operations", state.counters.insert_total)
    lines = lines ++ format_counter("triple_store_insert_triples_total", "Total number of triples inserted", state.counters.insert_triples_total)
    lines = lines ++ format_counter("triple_store_delete_total", "Total number of delete operations", state.counters.delete_total)
    lines = lines ++ format_counter("triple_store_delete_triples_total", "Total number of triples deleted", state.counters.delete_triples_total)
    lines = lines ++ format_counter("triple_store_load_total", "Total number of load operations", state.counters.load_total)
    lines = lines ++ format_counter("triple_store_load_triples_total", "Total number of triples loaded", state.counters.load_triples_total)
    lines = lines ++ format_counter("triple_store_reasoning_total", "Total number of materialization operations", state.counters.reasoning_total)
    lines = lines ++ format_counter("triple_store_reasoning_iterations_total", "Total number of reasoning iterations", state.counters.reasoning_iterations_total)
    lines = lines ++ format_counter("triple_store_reasoning_derived_total", "Total number of derived facts", state.counters.reasoning_derived_total)

    # Format labeled counters
    lines = lines ++ format_labeled_counter("triple_store_cache_hits_total", "Total number of cache hits", :cache_type, state.counters.cache_hits_total)
    lines = lines ++ format_labeled_counter("triple_store_cache_misses_total", "Total number of cache misses", :cache_type, state.counters.cache_misses_total)

    # Format histograms
    lines = lines ++ format_histogram("triple_store_query_duration_seconds", "Query execution time in seconds", state.histograms.query_duration, state.buckets)
    lines = lines ++ format_histogram("triple_store_reasoning_duration_seconds", "Reasoning duration in seconds", state.histograms.reasoning_duration, state.buckets)

    # Format gauges
    lines = lines ++ format_gauge("triple_store_triples", "Current number of triples in the store", state.gauges.triples)
    lines = lines ++ format_gauge("triple_store_memory_bytes", "Estimated memory usage in bytes", state.gauges.memory_bytes)
    lines = lines ++ format_labeled_gauge("triple_store_index_entries", "Number of entries in each index", :index, state.gauges.index_entries)

    Enum.join(lines, "\n") <> "\n"
  end

  defp format_counter(name, help, value) do
    [
      "# HELP #{name} #{help}",
      "# TYPE #{name} counter",
      "#{name} #{value}"
    ]
  end

  defp format_labeled_counter(name, help, _label_name, labels_map) when map_size(labels_map) == 0 do
    [
      "# HELP #{name} #{help}",
      "# TYPE #{name} counter"
    ]
  end

  defp format_labeled_counter(name, help, label_name, labels_map) do
    header = [
      "# HELP #{name} #{help}",
      "# TYPE #{name} counter"
    ]

    values =
      Enum.map(labels_map, fn {label_value, count} ->
        "#{name}{#{label_name}=\"#{label_value}\"} #{count}"
      end)

    header ++ values
  end

  defp format_histogram(name, help, histogram, buckets) do
    header = [
      "# HELP #{name} #{help}",
      "# TYPE #{name} histogram"
    ]

    bucket_lines =
      Enum.map(buckets, fn bound ->
        count = Map.get(histogram.buckets, bound, 0)
        "#{name}_bucket{le=\"#{format_float(bound)}\"} #{count}"
      end)

    # Add +Inf bucket (total count)
    inf_line = "#{name}_bucket{le=\"+Inf\"} #{histogram.count}"

    sum_line = "#{name}_sum #{format_float(histogram.sum)}"
    count_line = "#{name}_count #{histogram.count}"

    header ++ bucket_lines ++ [inf_line, sum_line, count_line]
  end

  defp format_gauge(name, help, value) do
    [
      "# HELP #{name} #{help}",
      "# TYPE #{name} gauge",
      "#{name} #{value}"
    ]
  end

  defp format_labeled_gauge(name, help, _label_name, labels_map) when map_size(labels_map) == 0 do
    [
      "# HELP #{name} #{help}",
      "# TYPE #{name} gauge"
    ]
  end

  defp format_labeled_gauge(name, help, label_name, labels_map) do
    header = [
      "# HELP #{name} #{help}",
      "# TYPE #{name} gauge"
    ]

    values =
      Enum.map(labels_map, fn {label_value, value} ->
        "#{name}{#{label_name}=\"#{label_value}\"} #{value}"
      end)

    header ++ values
  end

  defp format_float(f) when is_float(f) do
    :erlang.float_to_binary(f, [:compact, decimals: 6])
  end

  defp format_float(i) when is_integer(i) do
    "#{i}.0"
  end
end
