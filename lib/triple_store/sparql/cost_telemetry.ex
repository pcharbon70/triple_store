defmodule TripleStore.SPARQL.CostTelemetry do
  @moduledoc """
  Telemetry for cost model validation (S5).

  This module tracks:
  - Estimated costs vs actual execution costs
  - Query execution times
  - Cardinality estimate accuracy
  - Join algorithm performance

  Telemetry events emitted:
  - `[:triple_store, :sparql, :query, :start]` - Query execution started
  - `[:triple_store, :sparql, :query, :stop]` - Query execution completed
  - `[:triple_store, :sparql, :cost, :estimate]` - Cost estimate recorded
  - `[:triple_store, :sparql, :cost, :actual]` - Actual cost recorded
  - `[:triple_store, :sparql, :cardinality, :estimate]` - Cardinality estimate
  - `[:triple_store, :sparql, :cardinality, :actual]` - Actual cardinality
  - `[:triple_store, :sparql, :join, :start]` - Join operation started
  - `[:triple_store, :sparql, :join, :stop]` - Join operation completed
  """

  require Logger

  @doc """
  Attach a handler to collect telemetry events.
  """
  def attach_handler(handler_id \\ :cost_telemetry_handler) do
    :telemetry.attach(
      handler_id,
      [:triple_store, :sparql, :query, :stop],
      &handle_query_stop/4,
      nil
    )

    :telemetry.attach(
      "#{handler_id}_cost",
      [:triple_store, :sparql, :cost, :actual],
      &handle_cost_actual/4,
      nil
    )

    :telemetry.attach(
      "#{handler_id}_cardinality",
      [:triple_store, :sparql, :cardinality, :actual],
      &handle_cardinality_actual/4,
      nil
    )

    :ok
  end

  @doc """
  Detach a telemetry handler.
  """
  def detach_handler(handler_id \\ :cost_telemetry_handler) do
    :telemetry.detach(handler_id)
    :telemetry.detach("#{handler_id}_cost")
    :telemetry.detach("#{handler_id}_cardinality")
    :ok
  end

  @doc """
  Record a query start event.

  Returns the start_time for use with query_stop.
  """
  def query_start(query, algebra, opts \\ []) do
    start_time = System.monotonic_time()

    metadata = %{
      query: truncate(query),
      algebra_type: algebra_type(algebra),
      has_filter: has_filter?(algebra),
      pattern_count: pattern_count(algebra),
      estimated_cost: Keyword.get(opts, :estimated_cost),
      strategy: Keyword.get(opts, :strategy),
      start_time: start_time
    }

    :telemetry.execute(
      [:triple_store, :sparql, :query, :start],
      %{system_time: System.system_time()},
      metadata
    )

    start_time
  end

  @doc """
  Record a query stop event.
  """
  def query_stop(start_time, result_count, opts \\ []) do
    duration = System.monotonic_time() - start_time

    metadata = Map.new(opts)

    measurements = %{
      duration: duration,
      result_count: result_count
    }

    :telemetry.execute(
      [:triple_store, :sparql, :query, :stop],
      measurements,
      metadata
    )
  end

  @doc """
  Record a cost estimate.
  """
  def cost_estimate(algebra, estimated_cost, opts \\ []) do
    metadata = %{
      algebra_type: algebra_type(algebra),
      pattern_count: pattern_count(algebra),
      has_filter: has_filter?(algebra),
      strategy: Keyword.get(opts, :strategy)
    }

    :telemetry.execute(
      [:triple_store, :sparql, :cost, :estimate],
      %{estimated_cost: estimated_cost},
      metadata
    )
  end

  @doc """
  Record actual execution cost.
  """
  def cost_actual(algebra, actual_cost, estimated_cost, opts \\ []) do
    error_ratio = if estimated_cost > 0 do
      abs(actual_cost - estimated_cost) / estimated_cost
    else
      0.0
    end

    metadata = %{
      algebra_type: algebra_type(algebra),
      pattern_count: pattern_count(algebra),
      has_filter: has_filter?(algebra),
      estimated_cost: estimated_cost,
      error_ratio: error_ratio,
      strategy: Keyword.get(opts, :strategy)
    }

    :telemetry.execute(
      [:triple_store, :sparql, :cost, :actual],
      %{actual_cost: actual_cost},
      metadata
    )
  end

  @doc """
  Record a cardinality estimate.
  """
  def cardinality_estimate(pattern, estimated, opts \\ []) do
    metadata = %{
      pattern_type: pattern_type(pattern),
      bound_terms: bound_term_count(pattern),
      graph: Keyword.get(opts, :graph)
    }

    :telemetry.execute(
      [:triple_store, :sparql, :cardinality, :estimate],
      %{estimated: estimated},
      metadata
    )
  end

  @doc """
  Record actual cardinality.
  """
  def cardinality_actual(pattern, actual, estimated, opts \\ []) do
    error_ratio = if estimated > 0 do
      abs(actual - estimated) / estimated
    else
      0.0
    end

    metadata = %{
      pattern_type: pattern_type(pattern),
      bound_terms: bound_term_count(pattern),
      graph: Keyword.get(opts, :graph),
      estimated: estimated,
      error_ratio: error_ratio
    }

    :telemetry.execute(
      [:triple_store, :sparql, :cardinality, :actual],
      %{actual: actual},
      metadata
    )
  end

  @doc """
  Record a join operation start.
  """
  def join_start(left_size, right_size, algorithm) do
    measurements = %{
      left_size: left_size,
      right_size: right_size
    }

    metadata = %{
      algorithm: algorithm,
      start_time: System.monotonic_time()
    }

    :telemetry.execute(
      [:triple_store, :sparql, :join, :start],
      measurements,
      metadata
    )

    metadata.start_time
  end

  @doc """
  Record a join operation stop.
  """
  def join_stop(start_time, result_size, algorithm) do
    duration = System.monotonic_time() - start_time

    measurements = %{
      duration: duration,
      result_size: result_size
    }

    metadata = %{
      algorithm: algorithm
    }

    :telemetry.execute(
      [:triple_store, :sparql, :join, :stop],
      measurements,
      metadata
    )
  end

  @doc """
  Get cost model accuracy summary.
  """
  def get_accuracy_summary do
    # This would typically read from a telemetry event store
    # For now, return a placeholder
    %{
      total_queries: 0,
      avg_error_ratio: 0.0,
      median_error_ratio: 0.0,
      max_error_ratio: 0.0,
      estimates_within_2x: 0,
      estimates_within_10x: 0
    }
  end

  # Private: Handle query stop events
  defp handle_query_stop(_event, measurements, metadata, _config) do
    duration = measurements.duration
    result_count = measurements.result_count

    if Keyword.get(metadata, :estimated_cost) do
      estimated = metadata.estimated_cost
      actual = duration * result_count
      error_ratio = if estimated > 0, do: abs(actual - estimated) / estimated, else: 0.0

      Logger.debug("Query cost: estimated=#{estimated}, actual=#{actual}, error_ratio=#{:erlang.float_to_binary(error_ratio, decimals: 2)}")
    end
  end

  # Private: Handle cost actual events
  defp handle_cost_actual(_event, measurements, metadata, _config) do
    actual = measurements.actual_cost
    estimated = metadata.estimated_cost
    error_ratio = metadata.error_ratio

    if error_ratio > 10.0 do
      Logger.warning("Cost estimate error: estimated=#{estimated}, actual=#{actual}, error_ratio=#{:erlang.float_to_binary(error_ratio, decimals: 2)}")
    end
  end

  # Private: Handle cardinality actual events
  defp handle_cardinality_actual(_event, measurements, metadata, _config) do
    actual = measurements.actual
    estimated = metadata.estimated
    error_ratio = metadata.error_ratio

    if error_ratio > 10.0 do
      Logger.warning("Cardinality estimate error: estimated=#{estimated}, actual=#{actual}, error_ratio=#{:erlang.float_to_binary(error_ratio, decimals: 2)}")
    end
  end

  # Private: Determine algebra type
  defp algebra_type({:bgp, _}), do: :bgp
  defp algebra_type({:join, _, _}), do: :join
  defp algebra_type({:left_join, _, _, _}), do: :left_join
  defp algebra_type({:filter, _, _}), do: :filter
  defp algebra_type({:union, _, _}), do: :union
  defp algebra_type({:project, _, _}), do: :project
  defp algebra_type({:order, _, _}), do: :order
  defp algebra_type({:distinct, _}), do: :distinct
  defp algebra_type({:reduced, _}), do: :reduced
  defp algebra_type(_), do: :unknown

  # Private: Check if algebra has a filter
  defp has_filter?({:filter, _, _}), do: true
  defp has_filter?({:join, left, right}), do: has_filter?(left) or has_filter?(right)
  defp has_filter?({:left_join, left, right, _}), do: has_filter?(left) or has_filter?(right)
  defp has_filter?({:bgp, _}), do: false
  defp has_filter?(_), do: false

  # Private: Count patterns in algebra
  defp pattern_count({:bgp, patterns}), do: length(patterns)
  defp pattern_count({:join, left, right}), do: pattern_count(left) + pattern_count(right)
  defp pattern_count({:left_join, left, right, _}), do: pattern_count(left) + pattern_count(right)
  defp pattern_count({:filter, _, child}), do: pattern_count(child)
  defp pattern_count(_), do: 0

  # Private: Get pattern type
  defp pattern_type({:triple, _, _, _}), do: :triple
  defp pattern_type({:quad, _, _, _, _}), do: :quad
  defp pattern_type(_), do: :unknown

  # Private: Count bound terms in pattern
  defp bound_term_count({:triple, s, p, o}) do
    count_bound([s, p, o])
  end

  defp bound_term_count({:quad, s, p, o, g}) do
    count_bound([s, p, o, g])
  end

  defp bound_term_count(_), do: 0

  defp count_bound(terms) do
    Enum.count(terms, fn
      {:variable, _} -> false
      _ -> true
    end)
  end

  # Private: Truncate a string for logging
  defp truncate(str, max_length \\ 100)

  defp truncate(str, max_length) when is_binary(str) do
    if String.length(str) > max_length do
      String.slice(str, 0, max_length) <> "..."
    else
      str
    end
  end

  defp truncate(_, _max_length), do: ""
end
