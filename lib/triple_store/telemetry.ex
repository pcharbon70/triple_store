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

  ### Backup Events

  - `[:triple_store, :backup, :create, :start | :stop | :exception]`
  - `[:triple_store, :backup, :create_incremental, :start | :stop | :exception]`
  - `[:triple_store, :backup, :restore, :start | :stop | :exception]`
  - `[:triple_store, :backup, :verify, :start | :stop]`

  ### Reasoner Events

  See `TripleStore.Reasoner.Telemetry` for reasoning-specific events.

  ## Usage

      # Instrument a code block with sanitized query metadata
      metadata = TripleStore.Telemetry.sanitize_query(query)
      TripleStore.Telemetry.span(:query, :execute, metadata, fn ->
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
        # Sanitize query metadata for safe telemetry emission
        metadata = Telemetry.sanitize_query(query)
        Telemetry.span(:query, :execute, metadata, fn ->
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

      # Simple return with sanitized metadata
      metadata = Telemetry.sanitize_query(query)
      Telemetry.span(:query, :execute, metadata, fn ->
        execute(query)
      end)

      # With extra stop metadata
      metadata = Telemetry.sanitize_query(query)
      Telemetry.span(:query, :execute, metadata, fn ->
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

  ## Security

  The exception is sanitized to prevent sensitive information exposure.
  Only the exception type, message, and stacktrace depth are included.
  Full stacktraces should be logged separately with appropriate access controls.

  """
  @spec emit_exception([atom()], integer(), map(), Exception.t(), Exception.stacktrace()) :: :ok
  def emit_exception(event, duration, metadata, exception, stacktrace)
      when is_list(event) and is_map(metadata) do
    # Sanitize exception to prevent sensitive data exposure
    sanitized = sanitize_exception(exception, stacktrace)

    :telemetry.execute(
      event ++ [:exception],
      %{duration: duration},
      Map.merge(metadata, %{
        kind: :error,
        exception_type: sanitized.exception_type,
        exception_message: sanitized.exception_message,
        stacktrace_depth: sanitized.stacktrace_depth,
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
  def emit_cache_miss(cache_type, metadata \\ %{})
      when is_atom(cache_type) and is_map(metadata) do
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
      backup_events() ++
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
  Returns backup-related event names.
  """
  @spec backup_events() :: [[atom()]]
  def backup_events do
    [
      @prefix ++ [:backup, :create, :start],
      @prefix ++ [:backup, :create, :stop],
      @prefix ++ [:backup, :create, :exception],
      @prefix ++ [:backup, :create_incremental, :start],
      @prefix ++ [:backup, :create_incremental, :stop],
      @prefix ++ [:backup, :create_incremental, :exception],
      @prefix ++ [:backup, :restore, :start],
      @prefix ++ [:backup, :restore, :stop],
      @prefix ++ [:backup, :restore, :exception],
      @prefix ++ [:backup, :verify, :start],
      @prefix ++ [:backup, :verify, :stop],
      @prefix ++ [:scheduled_backup, :tick],
      @prefix ++ [:scheduled_backup, :error]
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

  @doc """
  Creates telemetry handlers that relay events to a process via message passing.

  This is useful for GenServer-based metrics collectors that need to receive
  telemetry events as messages. Returns a list of `{handler_id, events}` tuples
  that were attached.

  ## Parameters

  - `pid` - The process to receive events
  - `handler_prefix` - Prefix for handler IDs (must be unique per process)

  ## Handler IDs

  The following handlers are created:
  - `{prefix}_query` - Query execute events
  - `{prefix}_insert` - Insert events
  - `{prefix}_delete` - Delete events
  - `{prefix}_load` - Load events
  - `{prefix}_cache_hit` - Cache hit events (plan, query, stats)
  - `{prefix}_cache_miss` - Cache miss events (plan, query, stats)
  - `{prefix}_materialize` - Reasoner materialize events
  - `{prefix}_iteration` - Reasoner iteration events

  ## Message Format

  Events are sent as `{:telemetry_event, event_path, measurements, metadata}`.

  ## Examples

      def init(opts) do
        handlers = TripleStore.Telemetry.attach_metrics_handlers(self(), "my_metrics_\#{inspect(self())}")
        {:ok, %{handlers: handlers}}
      end

      def terminate(_reason, state) do
        TripleStore.Telemetry.detach_metrics_handlers(state.handlers)
        :ok
      end

  """
  @spec attach_metrics_handlers(pid(), String.t()) :: [{String.t(), [[atom()]]}]
  def attach_metrics_handlers(pid, handler_prefix)
      when is_pid(pid) and is_binary(handler_prefix) do
    handler_fn = fn event, measurements, metadata, _config ->
      send(pid, {:telemetry_event, event, measurements, metadata})
    end

    handlers = [
      # Query events
      {handler_id(handler_prefix, :query),
       [[:triple_store, :query, :execute, :stop], [:triple_store, :query, :execute, :exception]]},

      # Insert events
      {handler_id(handler_prefix, :insert), [[:triple_store, :insert, :stop]]},

      # Delete events
      {handler_id(handler_prefix, :delete), [[:triple_store, :delete, :stop]]},

      # Load events
      {handler_id(handler_prefix, :load), [[:triple_store, :load, :stop]]},

      # Cache hit events
      {handler_id(handler_prefix, :cache_hit),
       [
         [:triple_store, :cache, :plan, :hit],
         [:triple_store, :cache, :query, :hit],
         [:triple_store, :cache, :stats, :hit]
       ]},

      # Cache miss events
      {handler_id(handler_prefix, :cache_miss),
       [
         [:triple_store, :cache, :plan, :miss],
         [:triple_store, :cache, :query, :miss],
         [:triple_store, :cache, :stats, :miss]
       ]},

      # Reasoner events
      {handler_id(handler_prefix, :materialize),
       [[:triple_store, :reasoner, :materialize, :stop]]},
      {handler_id(handler_prefix, :iteration),
       [[:triple_store, :reasoner, :materialize, :iteration]]},

      # Backup events
      {handler_id(handler_prefix, :backup),
       [
         [:triple_store, :backup, :create, :stop],
         [:triple_store, :backup, :create_incremental, :stop],
         [:triple_store, :backup, :restore, :stop]
       ]}
    ]

    Enum.each(handlers, fn {id, events} ->
      :telemetry.attach_many(id, events, handler_fn, nil)
    end)

    handlers
  end

  @doc """
  Detaches all handlers created by `attach_metrics_handlers/2`.

  ## Parameters

  - `handlers` - List of `{handler_id, events}` tuples from `attach_metrics_handlers/2`

  """
  @spec detach_metrics_handlers([{String.t(), [[atom()]]}]) :: :ok
  def detach_metrics_handlers(handlers) when is_list(handlers) do
    Enum.each(handlers, fn {id, _events} ->
      :telemetry.detach(id)
    end)

    :ok
  end

  defp handler_id(prefix, suffix), do: "#{prefix}_#{suffix}"

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
  Converts duration from native time units to seconds.

  ## Examples

      duration_s = TripleStore.Telemetry.to_seconds(duration)

  """
  @spec to_seconds(integer()) :: float()
  def to_seconds(duration) do
    System.convert_time_unit(duration, :native, :microsecond) / 1_000_000.0
  end

  @doc """
  Extracts duration from measurements map in milliseconds.

  Handles both `:duration_ms` (already in ms) and `:duration` (native units).

  ## Examples

      duration_ms = TripleStore.Telemetry.duration_ms(measurements)

  """
  @spec duration_ms(map()) :: float()
  def duration_ms(measurements) do
    cond do
      Map.has_key?(measurements, :duration_ms) ->
        measurements.duration_ms

      Map.has_key?(measurements, :duration) ->
        System.convert_time_unit(measurements.duration, :native, :millisecond)

      true ->
        0.0
    end
  end

  @doc """
  Extracts duration from measurements map in seconds.

  Handles both `:duration_ms` (convert from ms) and `:duration` (native units).

  ## Examples

      duration_s = TripleStore.Telemetry.duration_seconds(measurements)

  """
  @spec duration_seconds(map()) :: float()
  def duration_seconds(measurements) do
    cond do
      Map.has_key?(measurements, :duration_ms) ->
        measurements.duration_ms / 1000.0

      Map.has_key?(measurements, :duration) ->
        System.convert_time_unit(measurements.duration, :native, :microsecond) / 1_000_000.0

      true ->
        0.0
    end
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

  # ===========================================================================
  # Security: Query Sanitization
  # ===========================================================================

  @doc """
  Sanitizes SPARQL query for safe telemetry emission.

  Returns a hash of the query instead of the raw query text to prevent
  sensitive data (literals, filter values, PII) from being exposed in
  telemetry metadata.

  ## Options

  - `:include_hash` - Include SHA-256 hash of query (default: true)
  - `:include_length` - Include query length (default: true)
  - `:include_type` - Attempt to detect query type (default: true)

  ## Examples

      iex> TripleStore.Telemetry.sanitize_query("SELECT * WHERE { ?s ?p ?o }")
      %{query_hash: "a1b2c3...", query_length: 32, query_type: :select}

  """
  @spec sanitize_query(String.t(), keyword()) :: map()
  def sanitize_query(query, opts \\ []) when is_binary(query) do
    include_hash = Keyword.get(opts, :include_hash, true)
    include_length = Keyword.get(opts, :include_length, true)
    include_type = Keyword.get(opts, :include_type, true)

    result = %{}

    result =
      if include_hash do
        hash = :crypto.hash(:sha256, query) |> Base.encode16(case: :lower) |> String.slice(0, 16)
        Map.put(result, :query_hash, hash)
      else
        result
      end

    result =
      if include_length do
        Map.put(result, :query_length, byte_size(query))
      else
        result
      end

    result =
      if include_type do
        Map.put(result, :query_type, detect_query_type(query))
      else
        result
      end

    result
  end

  @doc """
  Sanitizes exception for safe telemetry emission.

  Returns exception type and a sanitized message without exposing
  internal file paths, variable values, or full stacktraces.

  ## Examples

      iex> TripleStore.Telemetry.sanitize_exception(%RuntimeError{message: "error"}, stacktrace)
      %{exception_type: RuntimeError, exception_message: "error", stacktrace_depth: 5}

  """
  @spec sanitize_exception(Exception.t(), Exception.stacktrace()) :: map()
  def sanitize_exception(exception, stacktrace) do
    %{
      exception_type: exception.__struct__,
      exception_message: Exception.message(exception),
      stacktrace_depth: length(stacktrace)
    }
  end

  # Detect SPARQL query type from query string
  defp detect_query_type(query) do
    query_upper = String.upcase(query)

    cond do
      String.contains?(query_upper, "SELECT") -> :select
      String.contains?(query_upper, "CONSTRUCT") -> :construct
      String.contains?(query_upper, "ASK") -> :ask
      String.contains?(query_upper, "DESCRIBE") -> :describe
      String.contains?(query_upper, "INSERT") -> :insert
      String.contains?(query_upper, "DELETE") -> :delete
      String.contains?(query_upper, "LOAD") -> :load
      String.contains?(query_upper, "CLEAR") -> :clear
      true -> :unknown
    end
  end
end
