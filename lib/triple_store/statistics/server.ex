defmodule TripleStore.Statistics.Server do
  @moduledoc """
  GenServer for caching and auto-refreshing statistics.

  Provides in-memory caching of statistics with configurable auto-refresh.
  Statistics are refreshed either after a threshold of writes or on a
  periodic schedule.

  > **Note:** This module replaces `TripleStore.Statistics.Cache` which is deprecated.
  > Use `Statistics.Server` for new code.

  ## Features

  - **In-memory caching**: Fast access to statistics without disk reads
  - **Write threshold refresh**: Refresh after N triple modifications
  - **Periodic refresh**: Background refresh on a schedule
  - **Stale detection**: Track whether cached statistics are outdated
  - **Telemetry**: Cache hit/miss and refresh timing events

  ## Usage

      # Start the server (typically in application supervision tree)
      {:ok, pid} = Statistics.Server.start_link(db: db)

      # Get cached statistics
      {:ok, stats} = Statistics.Server.get_stats()

      # Force refresh
      {:ok, stats} = Statistics.Server.refresh()

      # Notify of triple modification
      Statistics.Server.notify_modification()

  ## Configuration

      Statistics.Server.start_link(
        db: db,
        auto_refresh: true,
        refresh_threshold: 10_000,      # Refresh after 10K modifications
        refresh_interval: :timer.hours(1)  # Or refresh every hour
      )

  ## Telemetry Events

  - `[:triple_store, :cache, :stats, :hit]` - Cache hit (returns cached stats)
  - `[:triple_store, :cache, :stats, :miss]` - Cache miss (needs collection)
  - `[:triple_store, :cache, :stats, :refresh]` - Statistics refresh completed
  """

  use GenServer

  alias TripleStore.Statistics

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Server state"
  @type state :: %{
          db: reference(),
          stats: Statistics.stats() | nil,
          modification_count: non_neg_integer(),
          refresh_threshold: non_neg_integer(),
          refresh_interval: pos_integer() | nil,
          auto_refresh: boolean(),
          last_refresh: DateTime.t() | nil,
          refresh_in_progress: boolean(),
          timeout: timeout()
        }

  # ===========================================================================
  # Constants
  # ===========================================================================

  @default_refresh_threshold 10_000
  @default_refresh_interval :timer.hours(1)
  @default_timeout 60_000
  @server_name __MODULE__

  # Valid options for Keyword.validate!/2
  @valid_options [
    :db,
    :name,
    :auto_refresh,
    :refresh_threshold,
    :refresh_interval,
    :timeout
  ]

  # ===========================================================================
  # Child Spec (S14)
  # ===========================================================================

  @doc """
  Returns a child specification for starting the server under a supervisor.

  ## Options

  All options from `start_link/1` are supported.

  ## Example

      children = [
        {Statistics.Server, db: db, name: MyStats}
      ]

      Supervisor.start_link(children, strategy: :one_for_one)
  """
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 5000
    }
  end

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the statistics server.

  ## Options

  - `:db` - Database reference (required)
  - `:auto_refresh` - Enable auto-refresh (default: true)
  - `:refresh_threshold` - Number of modifications before refresh (default: 10,000)
  - `:refresh_interval` - Milliseconds between periodic refreshes (default: 1 hour)
  - `:timeout` - GenServer call timeout in milliseconds (default: 60,000)
  - `:name` - Process name (default: #{inspect(@server_name)})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    # Validate options (S5)
    opts = Keyword.validate!(opts, @valid_options)
    name = Keyword.get(opts, :name, @server_name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Gets the cached statistics.

  Returns cached statistics if available. If no statistics are cached,
  collects fresh statistics and caches them.

  ## Options

  - `:server` - Server name or pid (default: #{inspect(@server_name)})
  - `:timeout` - Call timeout (default: uses server's configured timeout)

  ## Returns

  - `{:ok, stats}` - Statistics map
  - `{:error, reason}` - On failure
  """
  @spec get_stats(keyword()) :: {:ok, Statistics.stats()} | {:error, term()}
  def get_stats(opts \\ []) do
    server = Keyword.get(opts, :server, @server_name)
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    GenServer.call(server, :get_stats, timeout)
  end

  @doc """
  Forces a statistics refresh.

  Collects fresh statistics and updates the cache.

  ## Options

  - `:server` - Server name or pid (default: #{inspect(@server_name)})
  - `:timeout` - Call timeout (default: uses server's configured timeout)

  ## Returns

  - `{:ok, stats}` - Fresh statistics
  - `{:error, reason}` - On failure
  """
  @spec refresh(keyword()) :: {:ok, Statistics.stats()} | {:error, term()}
  def refresh(opts \\ []) do
    server = Keyword.get(opts, :server, @server_name)
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    GenServer.call(server, :refresh, timeout)
  end

  @doc """
  Notifies the server of a triple modification.

  Increments the modification counter. When the counter exceeds
  the refresh threshold, triggers a background refresh.

  ## Options

  - `:server` - Server name or pid (default: #{inspect(@server_name)})
  - `:count` - Number of modifications (default: 1)
  """
  @spec notify_modification(keyword()) :: :ok
  def notify_modification(opts \\ []) do
    server = Keyword.get(opts, :server, @server_name)
    count = Keyword.get(opts, :count, 1)
    GenServer.cast(server, {:modification, count})
  end

  @doc """
  Gets server statistics for monitoring.

  ## Options

  - `:server` - Server name or pid (default: #{inspect(@server_name)})

  ## Returns

  Map with:
  - `:modification_count` - Modifications since last refresh
  - `:last_refresh` - Timestamp of last refresh
  - `:cached` - Whether statistics are cached
  - `:stale` - Whether cached statistics are outdated
  """
  @spec server_stats(keyword()) :: map()
  def server_stats(opts \\ []) do
    server = Keyword.get(opts, :server, @server_name)
    GenServer.call(server, :server_stats)
  end

  @doc """
  Checks if the server is running.
  """
  @spec running?(keyword()) :: boolean()
  def running?(opts \\ []) do
    server = Keyword.get(opts, :server, @server_name)

    case Process.whereis(server) do
      nil -> false
      pid -> Process.alive?(pid)
    end
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    auto_refresh = Keyword.get(opts, :auto_refresh, true)
    refresh_threshold = Keyword.get(opts, :refresh_threshold, @default_refresh_threshold)
    refresh_interval = Keyword.get(opts, :refresh_interval, @default_refresh_interval)
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    state = %{
      db: db,
      stats: nil,
      modification_count: 0,
      refresh_threshold: refresh_threshold,
      refresh_interval: refresh_interval,
      auto_refresh: auto_refresh,
      last_refresh: nil,
      refresh_in_progress: false,
      timeout: timeout
    }

    # Schedule initial refresh
    send(self(), :initial_load)

    # Schedule periodic refresh if enabled
    if auto_refresh and refresh_interval do
      schedule_periodic_refresh(refresh_interval)
    end

    {:ok, state}
  end

  @impl true
  def handle_call(:get_stats, _from, %{stats: nil} = state) do
    # Cache miss - emit telemetry (S3)
    :telemetry.execute(
      [:triple_store, :cache, :stats, :miss],
      %{},
      %{}
    )

    # No cached stats, collect fresh
    case do_refresh(state) do
      {:ok, stats, new_state} ->
        {:reply, {:ok, stats}, new_state}

      {:error, reason, new_state} ->
        {:reply, {:error, reason}, new_state}
    end
  end

  def handle_call(:get_stats, _from, %{stats: stats} = state) do
    # Cache hit - emit telemetry (S3)
    :telemetry.execute(
      [:triple_store, :cache, :stats, :hit],
      %{},
      %{stale: state.modification_count > 0}
    )

    {:reply, {:ok, stats}, state}
  end

  def handle_call(:refresh, _from, state) do
    case do_refresh(state) do
      {:ok, stats, new_state} ->
        {:reply, {:ok, stats}, new_state}

      {:error, reason, new_state} ->
        {:reply, {:error, reason}, new_state}
    end
  end

  def handle_call(:server_stats, _from, state) do
    info = %{
      modification_count: state.modification_count,
      last_refresh: state.last_refresh,
      cached: state.stats != nil,
      stale: state.modification_count > 0,
      refresh_in_progress: state.refresh_in_progress
    }

    {:reply, info, state}
  end

  @impl true
  def handle_cast({:modification, count}, state) do
    new_count = state.modification_count + count
    new_state = %{state | modification_count: new_count}

    # Trigger background refresh if threshold exceeded
    if new_count >= state.refresh_threshold and state.auto_refresh and
         not state.refresh_in_progress do
      send(self(), :background_refresh)
    end

    {:noreply, new_state}
  end

  @impl true
  def handle_info(:initial_load, state) do
    # Try to load persisted statistics
    case Statistics.load(state.db) do
      {:ok, nil} ->
        # No persisted stats, collect fresh
        case do_refresh(state) do
          {:ok, _stats, new_state} -> {:noreply, new_state}
          {:error, _reason, new_state} -> {:noreply, new_state}
        end

      {:ok, stats} ->
        # Migrate stats if needed
        stats = Statistics.migrate_stats_if_needed(stats)
        {:noreply, %{state | stats: stats, last_refresh: stats[:collected_at]}}

      {:error, _reason} ->
        # Failed to load, try fresh collection
        case do_refresh(state) do
          {:ok, _stats, new_state} -> {:noreply, new_state}
          {:error, _reason, new_state} -> {:noreply, new_state}
        end
    end
  end

  def handle_info(:background_refresh, %{refresh_in_progress: true} = state) do
    # Already refreshing, ignore
    {:noreply, state}
  end

  def handle_info(:background_refresh, state) do
    # Set refresh_in_progress flag BEFORE starting (C3 fix)
    state = %{state | refresh_in_progress: true}

    # Do the refresh directly instead of via a GenServer call
    case do_refresh(state) do
      {:ok, _stats, new_state} -> {:noreply, new_state}
      {:error, _reason, new_state} -> {:noreply, new_state}
    end
  end

  def handle_info(:periodic_refresh, state) do
    # Schedule next refresh
    if state.auto_refresh and state.refresh_interval do
      schedule_periodic_refresh(state.refresh_interval)
    end

    # Only refresh if there have been modifications
    if state.modification_count > 0 and not state.refresh_in_progress do
      # Set refresh_in_progress flag BEFORE starting (C3 fix)
      state = %{state | refresh_in_progress: true}

      case do_refresh(state) do
        {:ok, _stats, new_state} -> {:noreply, new_state}
        {:error, _reason, new_state} -> {:noreply, new_state}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Terminate callback for cleanup (S7)
  @impl true
  def terminate(_reason, _state) do
    :ok
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  @spec do_refresh(state()) :: {:ok, Statistics.stats(), state()} | {:error, term(), state()}
  defp do_refresh(state) do
    start_time = System.monotonic_time()

    case Statistics.refresh(state.db) do
      {:ok, stats} ->
        duration = System.monotonic_time() - start_time

        # Fixed telemetry event naming to use :cache namespace (C11)
        :telemetry.execute(
          [:triple_store, :cache, :stats, :refresh],
          %{duration: duration},
          %{modification_count: state.modification_count}
        )

        new_state = %{
          state
          | stats: stats,
            modification_count: 0,
            last_refresh: DateTime.utc_now(),
            refresh_in_progress: false
        }

        {:ok, stats, new_state}

      {:error, reason} ->
        Logger.warning("Statistics refresh failed: #{inspect(reason)}")
        {:error, reason, %{state | refresh_in_progress: false}}
    end
  end

  defp schedule_periodic_refresh(interval) do
    Process.send_after(self(), :periodic_refresh, interval)
  end
end
