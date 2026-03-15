defmodule TripleStore.Statistics.AccuracyTracker do
  @moduledoc """
  Tracks accuracy of cardinality estimates vs actual results.

  This module monitors how well the cost model's estimates match actual query
  execution results, enabling continuous improvement of query optimization.

  ## Features

  - **Estimate tracking**: Records estimated cardinality for each query pattern
  - **Actual tracking**: Records actual result count after query execution
  - **Error metrics**: Calculates absolute and relative error
  - **Telemetry**: Emits accuracy metrics for monitoring
  - **Aggregation**: Provides aggregated accuracy statistics

  ## Usage

      # Start tracking a query estimate
      :ok = AccuracyTracker.track_estimate(ctx, pattern, estimated_count)

      # Record actual result after execution
      :ok = AccuracyTracker.track_actual(ctx, pattern, actual_count)

      # Get accuracy statistics
      {:ok, stats} = AccuracyTracker.get_accuracy_stats(ctx)

  ## Telemetry Events

  - `[:triple_store, :statistics, :estimate_accuracy]` - Emitted when actual results are recorded
    - Measurement: `relative_error` - Relative error as a percentage (0-100)
    - Metadata: `pattern_type`, `estimated`, `actual`, `absolute_error`
  """

  use GenServer
  require Logger

  @table_name :triple_store_accuracy_tracking
  @telemetry_event [:triple_store, :statistics, :estimate_accuracy]

  # Client API

  @doc """
  Starts the accuracy tracker GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Tracks an estimated cardinality for a query pattern.
  """
  def track_estimate(_ctx, pattern, estimated) when is_number(estimated) and estimated >= 0 do
    pattern_key = pattern_to_key(pattern)

    :ets.insert(@table_name, {{pattern_key, :estimate}, estimated})
    :ets.insert(@table_name, {{pattern_key, :timestamp}, System.monotonic_time(:millisecond)})

    # Initialize samples list if not exists
    unless :ets.match_object(@table_name, {{pattern_key, :samples}, :_}) |> length() > 0 do
      :ets.insert(@table_name, {{pattern_key, :samples}, []})
    end

    :ok
  end

  def track_estimate(_ctx, _pattern, _estimated) do
    {:error, :invalid_estimate}
  end

  @doc """
  Tracks the actual result count for a query pattern and emits telemetry.
  """
  def track_actual(_ctx, pattern, actual) when is_number(actual) and actual >= 0 do
    pattern_key = pattern_to_key(pattern)

    case :ets.lookup(@table_name, {pattern_key, :estimate}) do
      [{{^pattern_key, :estimate}, estimated}] ->
        # Calculate error metrics
        absolute_error = abs(estimated - actual)
        relative_error = calculate_relative_error(estimated, actual)

        # Get existing samples or initialize empty list
        samples =
          case :ets.lookup(@table_name, {pattern_key, :samples}) do
            [{{^pattern_key, :samples}, existing_samples}] when is_list(existing_samples) ->
              existing_samples

            _ ->
              []
          end

        new_sample = %{estimated: estimated, actual: actual, relative_error: relative_error}
        :ets.insert(@table_name, {{pattern_key, :samples}, [new_sample | samples]})

        # Store latest actual result
        :ets.insert(@table_name, {{pattern_key, :actual}, actual})

        # Emit telemetry event
        :telemetry.execute(
          @telemetry_event,
          %{relative_error: relative_error},
          %{
            pattern_type: pattern_type(pattern),
            estimated: estimated,
            actual: actual,
            absolute_error: absolute_error
          }
        )

        # Update aggregated stats
        update_aggregated_stats(relative_error)

        :ok

      [] ->
        # No estimate was tracked, just store actual
        :ets.insert(@table_name, {{pattern_key, :actual}, actual})
        {:error, :no_estimate}
    end
  end

  def track_actual(_ctx, _pattern, _actual) do
    {:error, :invalid_actual}
  end

  @doc """
  Gets accuracy statistics for all tracked patterns.
  """
  def get_accuracy_stats(_ctx) do
    # Get all pattern keys that have samples
    pattern_keys =
      @table_name
      |> :ets.tab2list()
      |> Enum.filter(fn
        {{_pattern_key, :samples}, _samples} -> true
        _ -> false
      end)
      |> Enum.map(fn {{pattern_key, :samples}, _samples} -> pattern_key end)

    # Build stats for each pattern
    stats =
      Enum.reduce(pattern_keys, %{}, fn pattern_key, acc ->
        case :ets.lookup(@table_name, {pattern_key, :samples}) do
          [{{^pattern_key, :samples}, samples}] when is_list(samples) ->
            maybe_put_pattern_stats(acc, pattern_key, samples)

          _ ->
            acc
        end
      end)

    {:ok, stats}
  end

  @doc """
  Gets aggregated accuracy statistics across all patterns.
  """
  def get_aggregated_stats(_ctx) do
    case :ets.lookup(@table_name, :aggregated) do
      [{:aggregated, stats}] -> {:ok, stats}
      [] -> {:ok, %{total_samples: 0, avg_relative_error: 0.0, max_relative_error: 0.0}}
    end
  end

  @doc """
  Resets all tracking data.
  """
  def reset(_ctx) do
    :ets.delete_all_objects(@table_name)
    initialize_aggregated_stats()
    :ok
  end

  # GenServer callbacks

  @impl true
  def init(_opts) do
    # Create ETS table for tracking
    :ets.new(@table_name, [:named_table, :public, :set])

    # Initialize aggregated stats
    initialize_aggregated_stats()

    {:ok, %{}}
  end

  @impl true
  def handle_call(_req, _from, state) do
    {:reply, {:error, :unknown_request}, state}
  end

  @impl true
  def handle_cast(_msg, state) do
    {:noreply, state}
  end

  # Private functions

  defp pattern_to_key({:quad, s, p, o, g}) do
    # Create a normalized key representation
    s_key = component_to_key(s)
    p_key = component_to_key(p)
    o_key = component_to_key(o)
    g_key = component_to_key(g)
    {:quad, s_key, p_key, o_key, g_key}
  end

  defp pattern_to_key(_pattern), do: :unknown

  defp maybe_put_pattern_stats(acc, _pattern_key, []), do: acc

  defp maybe_put_pattern_stats(acc, pattern_key, samples) do
    total_error =
      Enum.reduce(samples, 0.0, fn sample, sum ->
        sum + sample.relative_error
      end)

    Map.put(acc, pattern_key, %{
      avg_relative_error: total_error / length(samples),
      samples: length(samples)
    })
  end

  defp component_to_key({:variable, _name}), do: :var
  defp component_to_key(value) when is_integer(value), do: {:bound, value}
  defp component_to_key(_other), do: :other

  defp pattern_type({:quad, _, _, _, _}), do: :quad
  defp pattern_type(_), do: :unknown

  defp calculate_relative_error(estimated, actual) do
    if estimated == 0 do
      if actual == 0, do: 0.0, else: 100.0
    else
      abs((estimated - actual) / estimated) * 100.0
    end
  end

  defp update_aggregated_stats(relative_error) do
    case :ets.lookup(@table_name, :aggregated) do
      [{:aggregated, stats}] ->
        new_stats = %{
          total_samples: stats.total_samples + 1,
          avg_relative_error:
            (stats.avg_relative_error * stats.total_samples + relative_error) /
              (stats.total_samples + 1),
          max_relative_error: max(stats.max_relative_error, relative_error)
        }

        :ets.insert(@table_name, {:aggregated, new_stats})

      [] ->
        initialize_aggregated_stats()
    end
  end

  defp initialize_aggregated_stats do
    :ets.insert(
      @table_name,
      {:aggregated,
       %{
         total_samples: 0,
         avg_relative_error: 0.0,
         max_relative_error: 0.0
       }}
    )
  end
end
