defmodule TripleStore.Reasoner.Telemetry do
  @moduledoc """
  Telemetry events for the OWL 2 RL reasoner subsystem.

  This module provides instrumentation for monitoring compilation, optimization,
  and evaluation performance.

  ## Events

  ### Rule Compilation

  - `[:triple_store, :reasoner, :compile, :start]` - Compilation started
  - `[:triple_store, :reasoner, :compile, :stop]` - Compilation completed
  - `[:triple_store, :reasoner, :compile, :exception]` - Compilation failed

  Measurements:
  - `:duration` - Time in native units (stop only)

  Metadata:
  - `:profile` - Reasoning profile (:rdfs, :owl2rl, :all)
  - `:rule_count` - Number of applicable rules (stop only)
  - `:specialized_count` - Number of specialized rules (stop only)

  ### Rule Optimization

  - `[:triple_store, :reasoner, :optimize, :start]` - Optimization started
  - `[:triple_store, :reasoner, :optimize, :stop]` - Optimization completed

  Measurements:
  - `:duration` - Time in native units (stop only)

  Metadata:
  - `:rule_count` - Number of rules optimized
  - `:batch_count` - Number of batches created (stop only)
  - `:dead_rule_count` - Number of dead rules detected (stop only)

  ### Schema Extraction

  - `[:triple_store, :reasoner, :extract_schema, :start]` - Schema extraction started
  - `[:triple_store, :reasoner, :extract_schema, :stop]` - Schema extraction completed

  Measurements:
  - `:duration` - Time in native units (stop only)

  Metadata:
  - `:property_count` - Total specialized properties found (stop only)

  ### Materialization (Semi-Naive Evaluation)

  - `[:triple_store, :reasoner, :materialize, :start]` - Materialization started
  - `[:triple_store, :reasoner, :materialize, :stop]` - Materialization completed
  - `[:triple_store, :reasoner, :materialize, :iteration]` - Iteration completed

  Measurements (start):
  - `:system_time` - System time at start

  Measurements (stop):
  - `:duration` - Time in native units

  Measurements (iteration):
  - `:derivations` - Number of facts derived in this iteration

  Metadata (start):
  - `:rule_count` - Number of rules being applied
  - `:initial_fact_count` - Number of initial facts
  - `:parallel` - Whether parallel evaluation is enabled

  Metadata (stop):
  - `:iterations` - Total number of iterations
  - `:total_derived` - Total number of derived facts
  - `:duration_ms` - Duration in milliseconds
  - `:rules_applied` - Total rule applications
  - `:error` - Error reason (on failure only)

  Metadata (iteration):
  - `:iteration` - Current iteration number

  ## Usage with Telemetry Handlers

      :telemetry.attach_many(
        "reasoner-metrics",
        [
          [:triple_store, :reasoner, :compile, :stop],
          [:triple_store, :reasoner, :optimize, :stop]
        ],
        &handle_event/4,
        nil
      )

      defp handle_event(event, measurements, metadata, _config) do
        duration_ms = System.convert_time_unit(measurements.duration, :native, :millisecond)
        Logger.info("Reasoner event: \#{inspect(event)}, duration: \#{duration_ms}ms")
      end

  ## Span API

  The `span/3` function provides a convenient way to instrument code blocks:

      Telemetry.span(:compile, %{profile: :owl2rl}, fn ->
        # compilation code
        {:ok, result}
      end)
  """

  # ============================================================================
  # Event Prefixes
  # ============================================================================

  @prefix [:triple_store, :reasoner]

  # ============================================================================
  # Span API
  # ============================================================================

  @doc """
  Executes a function within a telemetry span.

  Emits start/stop/exception events automatically.

  ## Parameters

  - `event_suffix` - Event name suffix (e.g., :compile, :optimize)
  - `metadata` - Metadata to include with events
  - `fun` - Function to execute (should return result to include in stop metadata)

  ## Examples

      Telemetry.span(:compile, %{profile: :owl2rl}, fn ->
        # ... compilation code ...
        %{rule_count: 10, specialized_count: 5}
      end)
  """
  @spec span(atom(), map(), (-> map())) :: term()
  def span(event_suffix, metadata, fun) when is_atom(event_suffix) and is_map(metadata) do
    event = @prefix ++ [event_suffix]
    start_time = System.monotonic_time()

    emit_start(event, metadata)

    try do
      result = fun.()
      duration = System.monotonic_time() - start_time
      emit_stop(event, duration, Map.merge(metadata, result))
      result
    rescue
      e ->
        duration = System.monotonic_time() - start_time
        emit_exception(event, duration, metadata, e, __STACKTRACE__)
        reraise e, __STACKTRACE__
    end
  end

  @doc """
  Emits a start event.
  """
  @spec emit_start([atom()], map()) :: :ok
  def emit_start(event, metadata) do
    :telemetry.execute(
      event ++ [:start],
      %{system_time: System.system_time()},
      metadata
    )
  end

  @doc """
  Emits a stop event with duration.
  """
  @spec emit_stop([atom()], integer(), map()) :: :ok
  def emit_stop(event, duration, metadata) do
    :telemetry.execute(
      event ++ [:stop],
      %{duration: duration},
      metadata
    )
  end

  @doc """
  Emits an exception event.
  """
  @spec emit_exception([atom()], integer(), map(), Exception.t(), Exception.stacktrace()) :: :ok
  def emit_exception(event, duration, metadata, exception, stacktrace) do
    :telemetry.execute(
      event ++ [:exception],
      %{duration: duration},
      Map.merge(metadata, %{
        kind: :error,
        reason: exception,
        stacktrace: stacktrace
      })
    )
  end

  # ============================================================================
  # Convenience Functions
  # ============================================================================

  @doc """
  Emits a compilation event.
  """
  @spec emit_compilation(map()) :: :ok
  def emit_compilation(metadata) do
    :telemetry.execute(@prefix ++ [:compile, :complete], %{}, metadata)
  end

  @doc """
  Emits an optimization event.
  """
  @spec emit_optimization(map()) :: :ok
  def emit_optimization(metadata) do
    :telemetry.execute(@prefix ++ [:optimize, :complete], %{}, metadata)
  end

  @doc """
  Emits a schema extraction event.
  """
  @spec emit_schema_extraction(map()) :: :ok
  def emit_schema_extraction(metadata) do
    :telemetry.execute(@prefix ++ [:extract_schema, :complete], %{}, metadata)
  end

  @doc """
  Returns the event prefix for the reasoner.
  """
  @spec prefix() :: [atom()]
  def prefix, do: @prefix

  @doc """
  Returns all event names emitted by the reasoner.
  """
  @spec event_names() :: [[atom()]]
  def event_names do
    [
      @prefix ++ [:compile, :start],
      @prefix ++ [:compile, :stop],
      @prefix ++ [:compile, :exception],
      @prefix ++ [:compile, :complete],
      @prefix ++ [:optimize, :start],
      @prefix ++ [:optimize, :stop],
      @prefix ++ [:optimize, :complete],
      @prefix ++ [:extract_schema, :start],
      @prefix ++ [:extract_schema, :stop],
      @prefix ++ [:extract_schema, :complete],
      @prefix ++ [:materialize, :start],
      @prefix ++ [:materialize, :stop],
      @prefix ++ [:materialize, :iteration]
    ]
  end

  @doc """
  Emits a materialization iteration event.

  ## Parameters

  - `derivations` - Number of facts derived in this iteration
  - `iteration` - Current iteration number
  """
  @spec emit_iteration(non_neg_integer(), non_neg_integer()) :: :ok
  def emit_iteration(derivations, iteration) do
    :telemetry.execute(
      @prefix ++ [:materialize, :iteration],
      %{derivations: derivations},
      %{iteration: iteration}
    )
  end
end
