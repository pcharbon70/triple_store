defmodule TripleStore.SPARQL.Update.RateLimiter do
  @moduledoc """
  Rate limiter for SPARQL UPDATE operations.

  This module provides rate limiting to prevent DoS attacks and ensure
  fair resource allocation among users. It uses a token bucket algorithm
  with ETS for fast, lock-free rate limit checks.

  ## Configuration

  Rate limits are configured per operation type and can be customized:

  ```elixir
  # Default limits
  @default_limits %{
    insert_data: {100, 60},  # 100 operations per 60 seconds
    delete_data: {100, 60},
    modify: {50, 60},
    create_graph: {10, 60},
    drop_graph: {10, 60},
    clear: {10, 60},
    copy: {20, 60},
    move: {20, 60},
    add: {20, 60}
  }
  ```

  ## Usage

  ```elixir
  # Check if operation is allowed
  case RateLimiter.allow?(user_id, :insert_data) do
    :ok -> # Proceed with operation
    {:error, :rate_limited} -> # Reject with rate limit error
  end

  # Record an operation (if not using allow?)
  RateLimiter.record(user_id, :insert_data)

  # Get current usage stats
  {:ok, stats} = RateLimiter.stats(user_id, :insert_data)
  ```

  ## Architecture

  - ETS table `:sparql_update_rate_limits` stores per-user operation counters
  - Each entry: `{user_id, operation_type}` => `{count, window_start}`
  - Sliding window algorithm with automatic cleanup of stale entries
  - Telemetry events for rate limit hits

  """

  use GenServer
  require Logger

  # ===========================================================================
  @table_name :sparql_update_rate_limits

  # Default rate limits: {max_operations, window_seconds}
  @default_limits %{
    insert_data: {100, 60},
    delete_data: {100, 60},
    modify: {50, 60},
    create_graph: {10, 60},
    drop_graph: {10, 60},
    clear: {10, 60},
    copy: {20, 60},
    move: {20, 60},
    add: {20, 60}
  }

  # Cleanup interval for stale entries
  # 5 minutes
  @cleanup_interval 300_000

  # ===========================================================================
  # Types
  # ===========================================================================

  @type user_id :: term()
  @type operation :: atom()
  # {max_ops, window_seconds}
  @type limit :: {non_neg_integer(), pos_integer()}

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the RateLimiter GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Checks if an operation is allowed for the given user.

  Returns `:ok` if the operation is allowed, or `{:error, :rate_limited}` if the rate limit has been exceeded.

  ## Examples

      iex> RateLimiter.allow?("user123", :insert_data)
      :ok

      iex> RateLimiter.allow?("user456", :insert_data)
      {:error, :rate_limited}

  """
  @spec allow?(user_id(), operation()) :: :ok | {:error, :rate_limited}
  def allow?(user_id, operation) do
    ensure_rate_limit_table()

    case get_limit(operation) do
      {:ok, {max_ops, window_seconds}} ->
        now = System.system_time(:second)
        key = {user_id, operation}
        apply_rate_limit(key, now, user_id, operation, max_ops, window_seconds)

      {:error, :unknown_operation} ->
        # Unknown operation - allow by default
        :ok
    end
  end

  defp apply_rate_limit(key, now, user_id, operation, max_ops, window_seconds) do
    case get_rate_limit_state(key) do
      {count, window_start} when now - window_start < window_seconds ->
        apply_rate_limit_window(key, window_start, user_id, operation, count, max_ops)

      {_count, _window_start} ->
        set_rate_limit_state(key, {1, now})
        :ok

      :not_found ->
        set_rate_limit_state(key, {1, now})
        :ok
    end
  end

  defp apply_rate_limit_window(key, window_start, user_id, operation, count, max_ops) do
    if count < max_ops do
      set_rate_limit_state(key, {count + 1, window_start})
      :ok
    else
      emit_rate_limit_telemetry(user_id, operation, count, max_ops)
      {:error, :rate_limited}
    end
  end

  @doc """
  Records an operation for the given user.

  This is typically used instead of `allow?/2` when you want to record
  the operation separately from the rate limit check.
  """
  @spec record(user_id(), operation()) :: :ok
  def record(user_id, operation) do
    ensure_rate_limit_table()

    key = {user_id, operation}
    now = System.system_time(:second)

    case get_rate_limit_state(key) do
      {count, window_start} ->
        set_rate_limit_state(key, {count + 1, window_start})

      :not_found ->
        set_rate_limit_state(key, {1, now})
    end

    :ok
  end

  @doc """
  Gets the current rate limit statistics for a user and operation.

  Returns `{:ok, %{count: count, remaining: remaining, reset_at: timestamp}}`
  or `{:error, :unknown_operation}` if the operation type is unknown.
  """
  @spec stats(user_id(), operation()) :: {:ok, map()} | {:error, :unknown_operation}
  def stats(user_id, operation) do
    ensure_rate_limit_table()

    case get_limit(operation) do
      {:ok, {max_ops, window_seconds}} ->
        now = System.system_time(:second)
        key = {user_id, operation}

        case get_rate_limit_state(key) do
          {count, window_start} when now - window_start < window_seconds ->
            remaining = max(0, max_ops - count)
            reset_at = window_start + window_seconds
            {:ok, %{count: count, remaining: remaining, reset_at: reset_at}}

          {_count, _window_start} ->
            # Window expired
            {:ok, %{count: 0, remaining: max_ops, reset_at: now}}

          :not_found ->
            {:ok, %{count: 0, remaining: max_ops, reset_at: now}}
        end

      {:error, :unknown_operation} ->
        {:error, :unknown_operation}
    end
  end

  @doc """
  Resets the rate limit counter for a user and operation.

  This is primarily used for testing purposes.
  """
  @spec reset(user_id(), operation()) :: :ok
  def reset(user_id, operation) do
    ensure_rate_limit_table()

    key = {user_id, operation}
    :ets.delete(@table_name, key)

    :ok
  end

  @doc """
  Resets all rate limit counters for a user.
  """
  @spec reset_user(user_id()) :: :ok
  def reset_user(user_id) do
    ensure_rate_limit_table()

    # Delete all entries for this user
    pattern = {{user_id, :_}, :_}
    :ets.select_delete(@table_name, [{pattern, [], [true]}])

    :ok
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(_opts) do
    # Create ETS table for rate limit state
    table =
      :ets.new(@table_name, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    # Schedule periodic cleanup
    schedule_cleanup()

    {:ok, %{table: table}}
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_stale_entries()
    schedule_cleanup()
    {:noreply, state}
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  # Gets the rate limit for an operation type
  defp get_limit(operation) do
    case Map.get(@default_limits, operation) do
      nil -> {:error, :unknown_operation}
      limit -> {:ok, limit}
    end
  end

  # Gets the current rate limit state from ETS
  defp get_rate_limit_state(key) do
    ensure_rate_limit_table()

    case :ets.lookup(@table_name, key) do
      [{^key, state}] -> state
      [] -> :not_found
    end
  end

  # Sets the rate limit state in ETS
  defp set_rate_limit_state(key, state) do
    ensure_rate_limit_table()
    :ets.insert(@table_name, {key, state})
  end

  # Cleans up stale entries from the ETS table
  defp cleanup_stale_entries do
    now = System.system_time(:second)
    max_window = get_max_window_seconds()
    cutoff = now - max_window * 2

    # Delete entries where window_start is too old
    :ets.foldl(
      fn {key, {_count, window_start}}, acc ->
        if window_start < cutoff do
          :ets.delete(@table_name, key)
        end

        acc
      end,
      nil,
      @table_name
    )
  end

  # Gets the maximum window size for cleanup purposes
  defp get_max_window_seconds do
    @default_limits
    |> Map.values()
    |> Enum.map(fn {_max, window} -> window end)
    |> Enum.max(fn -> 60 end)
  end

  # Schedules the next cleanup
  defp schedule_cleanup do
    Process.send_after(self(), :cleanup, @cleanup_interval)
  end

  # Emits telemetry when rate limit is hit
  defp emit_rate_limit_telemetry(user_id, operation, count, limit) do
    :telemetry.execute(
      [:triple_store, :sparql, :update, :rate_limited],
      %{count: 1},
      %{
        user_id: inspect(user_id),
        operation: operation,
        count: count,
        limit: limit
      }
    )
  end

  defp ensure_rate_limit_table do
    case :ets.whereis(@table_name) do
      :undefined ->
        ensure_rate_limiter_server()

        case :ets.whereis(@table_name) do
          :undefined -> create_rate_limit_table()
          _table -> :ok
        end

      _table ->
        :ok
    end
  end

  defp ensure_rate_limiter_server do
    case Process.whereis(__MODULE__) do
      nil ->
        case start_link() do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, _reason} -> :ok
        end

      _pid ->
        :ok
    end
  end

  defp create_rate_limit_table do
    :ets.new(@table_name, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    :ok
  rescue
    ArgumentError -> :ok
  end
end
