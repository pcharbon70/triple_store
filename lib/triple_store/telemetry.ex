defmodule TripleStore.Telemetry do
  @moduledoc """
  Unified telemetry instrumentation for the TripleStore.

  This module provides a centralized entry point for all telemetry events
  emitted by the TripleStore. It consolidates event definitions, span
  instrumentation, and handler attachment.

  ## Event Naming Convention

  All events follow the pattern: `[:triple_store, :subsystem, :operation, :phase]`

  Where:
  - `:subsystem` - Component emitting the event (e.g., `:query`, `:reasoner`, `:cache`)
  - `:operation` - Specific operation (e.g., `:execute`, `:compile`, `:lookup`)
  - `:phase` - Event phase (`:start`, `:stop`, `:exception`)

  ## Event Categories

  ### Query Events

  - `[:triple_store, :query, :parse, :start | :stop | :exception]`
  - `[:triple_store, :query, :execute, :start | :stop | :exception]`

  ### Insert/Delete Events

  - `[:triple_store, :insert, :start | :stop | :exception]`
  - `[:triple_store, :delete, :start | :stop | :exception]`

  ### Cache Events

  - `[:triple_store, :cache, :plan, :hit | :miss]`
  - `[:triple_store, :cache, :stats, :hit | :miss]`
  - `[:triple_store, :cache, :query, :hit | :miss | :expired]`
  - `[:triple_store, :cache, :query, :persist | :warm]`

  ### Reasoner Events

  See `TripleStore.Reasoner.Telemetry` for reasoning-specific events.

  ## Usage

      # Instrument a code block
      TripleStore.Telemetry.span(:query, :execute, %{sparql: query}, fn ->
        # query execution code
        {:ok, results}
      end)

      # Attach a handler
      TripleStore.Telemetry.attach_handler("my-handler", fn event, measurements, metadata, config ->
        Logger.info("Event: \#{inspect(event)}")
      end)

      # Get all event names for attaching handlers
      events = TripleStore.Telemetry.all_events()

  ## Span Pattern

  The span pattern provides automatic start/stop/exception event emission:

      def execute_query(query) do
        Telemetry.span(:query, :execute, %{query: query}, fn ->
          result = do_execute(query)
          # Return additional metadata for stop event
          {result, %{result_count: length(result)}}
        end)
      end

  """

  require Logger

  # ===========================================================================
  # Event Prefixes
  # ===========================================================================

  @prefix [:triple_store]

  # ===========================================================================
  # Span API
  # ===========================================================================

  @doc """
  Executes a function within a telemetry span.

  Automatically emits start/stop/exception events with proper timing.

  ## Parameters

  - `subsystem` - Subsystem name (e.g., `:query`, `:reasoner`)
  - `operation` - Operation name (e.g., `:execute`, `:compile`)
  - `metadata` - Metadata to include with all events
  - `fun` - Function to execute

  The function can return:
  - A simple result (used as-is)
  - `{result, extra_metadata}` - Extra metadata merged into stop event

  ## Examples

      # Simple return
      Telemetry.span(:query, :execute, %{sparql: query}, fn ->
        execute(query)
      end)

      # With extra stop metadata
      Telemetry.span(:query, :execute, %{sparql: query}, fn ->
        results = execute(query)
        {results, %{result_count: length(results)}}
      end)

  """
  @spec span(atom(), atom(), map(), (-> term() | {term(), map()})) :: term()
  def span(subsystem, operation, metadata, fun)
      when is_atom(subsystem) and is_atom(operation) and is_map(metadata) do
    event = @prefix ++ [subsystem, operation]
    start_time = System.monotonic_time()

    emit_start(event, metadata)

    try do
      case fun.() do
        {result, extra_metadata} when is_map(extra_metadata) ->
          duration = System.monotonic_time() - start_time
          emit_stop(event, duration, Map.merge(metadata, extra_metadata))
          result

        result ->
          duration = System.monotonic_time() - start_time
          emit_stop(event, duration, metadata)
          result
      end
    rescue
      e ->
        duration = System.monotonic_time() - start_time
        emit_exception(event, duration, metadata, e, __STACKTRACE__)
        reraise e, __STACKTRACE__
    end
  end

  @doc """
  Emits a start event.

  ## Parameters

  - `event` - Full event path (list of atoms)
  - `metadata` - Event metadata

  """
  @spec emit_start([atom()], map()) :: :ok
  def emit_start(event, metadata) when is_list(event) and is_map(metadata) do
    :telemetry.execute(
      event ++ [:start],
      %{system_time: System.system_time(), monotonic_time: System.monotonic_time()},
      metadata
    )
  end

  @doc """
  Emits a stop event with duration.

  ## Parameters

  - `event` - Full event path (list of atoms)
  - `duration` - Duration in native time units
  - `metadata` - Event metadata

  """
  @spec emit_stop([atom()], integer(), map()) :: :ok
  def emit_stop(event, duration, metadata) when is_list(event) and is_map(metadata) do
    :telemetry.execute(
      event ++ [:stop],
      %{duration: duration},
      Map.put(metadata, :duration_ms, System.convert_time_unit(duration, :native, :millisecond))
    )
  end

  @doc """
  Emits an exception event.

  ## Parameters

  - `event` - Full event path (list of atoms)
  - `duration` - Duration in native time units
  - `metadata` - Event metadata
  - `exception` - The exception that was raised
  - `stacktrace` - Exception stacktrace

  """
  @spec emit_exception([atom()], integer(), map(), Exception.t(), Exception.stacktrace()) :: :ok
  def emit_exception(event, duration, metadata, exception, stacktrace)
      when is_list(event) and is_map(metadata) do
    :telemetry.execute(
      event ++ [:exception],
      %{duration: duration},
      Map.merge(metadata, %{
        kind: :error,
        reason: exception,
        stacktrace: stacktrace,
        duration_ms: System.convert_time_unit(duration, :native, :millisecond)
      })
    )
  end

  @doc """
  Emits a cache hit event.

  ## Parameters

  - `cache_type` - Type of cache (`:plan` or `:stats`)
  - `metadata` - Event metadata (e.g., `%{key: cache_key}`)

  """
  @spec emit_cache_hit(atom(), map()) :: :ok
  def emit_cache_hit(cache_type, metadata \\ %{}) when is_atom(cache_type) and is_map(metadata) do
    :telemetry.execute(
      @prefix ++ [:cache, cache_type, :hit],
      %{count: 1},
      metadata
    )
  end

  @doc """
  Emits a cache miss event.

  ## Parameters

  - `cache_type` - Type of cache (`:plan` or `:stats`)
  - `metadata` - Event metadata

  """
  @spec emit_cache_miss(atom(), map()) :: :ok
  def emit_cache_miss(cache_type, metadata \\ %{}) when is_atom(cache_type) and is_map(metadata) do
    :telemetry.execute(
      @prefix ++ [:cache, cache_type, :miss],
      %{count: 1},
      metadata
    )
  end

  # ===========================================================================
  # Event Registry
  # ===========================================================================

  @doc """
  Returns all event names emitted by the TripleStore.

  This includes events from all subsystems. Use this when attaching
  telemetry handlers to ensure you capture all events.

  ## Examples

      events = TripleStore.Telemetry.all_events()
      :telemetry.attach_many("my-handler", events, &handle_event/4, nil)

  """
  @spec all_events() :: [[atom()]]
  def all_events do
    query_events() ++
      insert_events() ++
      delete_events() ++
      cache_events() ++
      load_events() ++
      reasoner_events()
  end

  @doc """
  Returns query-related event names.
  """
  @spec query_events() :: [[atom()]]
  def query_events do
    [
      @prefix ++ [:query, :parse, :start],
      @prefix ++ [:query, :parse, :stop],
      @prefix ++ [:query, :parse, :exception],
      @prefix ++ [:query, :execute, :start],
      @prefix ++ [:query, :execute, :stop],
      @prefix ++ [:query, :execute, :exception]
    ]
  end

  @doc """
  Returns insert-related event names.
  """
  @spec insert_events() :: [[atom()]]
  def insert_events do
    [
      @prefix ++ [:insert, :start],
      @prefix ++ [:insert, :stop],
      @prefix ++ [:insert, :exception]
    ]
  end

  @doc """
  Returns delete-related event names.
  """
  @spec delete_events() :: [[atom()]]
  def delete_events do
    [
      @prefix ++ [:delete, :start],
      @prefix ++ [:delete, :stop],
      @prefix ++ [:delete, :exception]
    ]
  end

  @doc """
  Returns cache-related event names.
  """
  @spec cache_events() :: [[atom()]]
  def cache_events do
    [
      # Plan cache events
      @prefix ++ [:cache, :plan, :hit],
      @prefix ++ [:cache, :plan, :miss],
      # Stats cache events
      @prefix ++ [:cache, :stats, :hit],
      @prefix ++ [:cache, :stats, :miss],
      # Query result cache events
      @prefix ++ [:cache, :query, :hit],
      @prefix ++ [:cache, :query, :miss],
      @prefix ++ [:cache, :query, :expired],
      @prefix ++ [:cache, :query, :persist],
      @prefix ++ [:cache, :query, :warm]
    ]
  end

  @doc """
  Returns load-related event names.
  """
  @spec load_events() :: [[atom()]]
  def load_events do
    [
      @prefix ++ [:load, :start],
      @prefix ++ [:load, :stop],
      @prefix ++ [:load, :exception],
      @prefix ++ [:load, :batch, :complete]
    ]
  end

  @doc """
  Returns reasoner event names.

  Delegates to `TripleStore.Reasoner.Telemetry.event_names/0`.
  """
  @spec reasoner_events() :: [[atom()]]
  def reasoner_events do
    TripleStore.Reasoner.Telemetry.event_names()
  end

  # ===========================================================================
  # Handler Attachment
  # ===========================================================================

  @doc """
  Attaches a handler to all TripleStore events.

  This is a convenience function that attaches a handler to all events
  returned by `all_events/0`.

  ## Parameters

  - `handler_id` - Unique identifier for the handler
  - `handler_fn` - Handler function `(event, measurements, metadata, config) -> any()`
  - `config` - Optional configuration passed to handler

  ## Examples

      TripleStore.Telemetry.attach_handler("my-logger", fn event, measurements, metadata, _config ->
        Logger.info("Event: \#{inspect(event)}, duration: \#{measurements[:duration]}")
      end)

  """
  @spec attach_handler(String.t(), function(), term()) :: :ok | {:error, :already_exists}
  def attach_handler(handler_id, handler_fn, config \\ nil) do
    :telemetry.attach_many(handler_id, all_events(), handler_fn, config)
  end

  @doc """
  Detaches a handler by its ID.

  ## Examples

      :ok = TripleStore.Telemetry.detach_handler("my-logger")

  """
  @spec detach_handler(String.t()) :: :ok | {:error, :not_found}
  def detach_handler(handler_id) do
    :telemetry.detach(handler_id)
  end

  # ===========================================================================
  # Utility Functions
  # ===========================================================================

  @doc """
  Returns the event prefix for the TripleStore.
  """
  @spec prefix() :: [atom()]
  def prefix, do: @prefix

  @doc """
  Converts duration from native time units to milliseconds.

  ## Examples

      duration_ms = TripleStore.Telemetry.to_milliseconds(duration)

  """
  @spec to_milliseconds(integer()) :: float()
  def to_milliseconds(duration) do
    System.convert_time_unit(duration, :native, :millisecond)
  end

  @doc """
  Builds a full event path from subsystem and operation.

  ## Examples

      [:triple_store, :query, :execute] = TripleStore.Telemetry.event_path(:query, :execute)

  """
  @spec event_path(atom(), atom()) :: [atom()]
  def event_path(subsystem, operation) do
    @prefix ++ [subsystem, operation]
  end
end
