defmodule TripleStore.Statistics.Cache do
  @moduledoc """
  GenServer for caching triple store statistics.

  Provides cached access to statistics with automatic periodic refresh
  and manual invalidation for bulk updates. This avoids repeated full
  scans of indices for frequently accessed statistics.

  ## Features

  - **Cached access**: Fast retrieval of pre-computed statistics
  - **Periodic refresh**: Automatic background refresh at configurable intervals
  - **Manual invalidation**: Explicit invalidation after bulk updates
  - **Predicate histogram**: Frequency distribution of predicates

  ## Usage

      # Start the cache (typically in your supervision tree)
      {:ok, cache} = Statistics.Cache.start_link(db: db)

      # Get cached statistics
      {:ok, stats} = Statistics.Cache.get(cache)

      # Invalidate after bulk updates
      :ok = Statistics.Cache.invalidate(cache)

      # Get predicate histogram
      {:ok, histogram} = Statistics.Cache.predicate_histogram(cache)

  ## Configuration

  - `:refresh_interval` - Milliseconds between automatic refreshes (default: 60_000)
  - `:db` - Database reference (required)

  ## Supervision

  Add to your application supervision tree:

      children = [
        {TripleStore.Statistics.Cache, db: db, name: MyApp.StatsCache}
      ]
  """

  use GenServer

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Statistics

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Cache server reference"
  @type cache :: GenServer.server()

  @typedoc "Cached statistics map"
  @type cached_stats :: %{
          triple_count: non_neg_integer(),
          distinct_subjects: non_neg_integer(),
          distinct_predicates: non_neg_integer(),
          distinct_objects: non_neg_integer(),
          computed_at: DateTime.t()
        }

  @typedoc "Predicate frequency histogram"
  @type predicate_histogram :: %{non_neg_integer() => non_neg_integer()}

  # ===========================================================================
  # Constants
  # ===========================================================================

  # 1 minute
  @default_refresh_interval 60_000

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the statistics cache GenServer.

  ## Options

  - `:db` - Database reference (required)
  - `:refresh_interval` - Milliseconds between refreshes (default: #{@default_refresh_interval})
  - `:name` - GenServer name for registration

  ## Examples

      {:ok, cache} = Statistics.Cache.start_link(db: db)
      {:ok, cache} = Statistics.Cache.start_link(db: db, refresh_interval: 30_000)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns the cached statistics.

  If the cache is empty or stale, triggers a refresh before returning.

  ## Arguments

  - `cache` - Cache server reference

  ## Returns

  - `{:ok, stats}` - Cached statistics map
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, stats} = Statistics.Cache.get(cache)
      stats.triple_count
      # => 1000
  """
  @spec get(cache()) :: {:ok, cached_stats()} | {:error, term()}
  def get(cache) do
    GenServer.call(cache, :get)
  end

  @doc """
  Returns the predicate frequency histogram.

  The histogram maps predicate IDs to their occurrence counts.

  ## Arguments

  - `cache` - Cache server reference

  ## Returns

  - `{:ok, histogram}` - Map of predicate_id => count
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, histogram} = Statistics.Cache.predicate_histogram(cache)
      # => %{100 => 500, 101 => 250, 102 => 750}
  """
  @spec predicate_histogram(cache()) :: {:ok, predicate_histogram()} | {:error, term()}
  def predicate_histogram(cache) do
    GenServer.call(cache, :predicate_histogram)
  end

  @doc """
  Invalidates the cache, forcing a refresh on next access.

  Call this after bulk updates (load_graph, load_file, etc.) to ensure
  statistics reflect the new data.

  ## Arguments

  - `cache` - Cache server reference

  ## Returns

  - `:ok` - Cache invalidated successfully

  ## Examples

      Loader.load_graph(db, manager, large_graph)
      Statistics.Cache.invalidate(cache)
  """
  @spec invalidate(cache()) :: :ok
  def invalidate(cache) do
    GenServer.cast(cache, :invalidate)
  end

  @doc """
  Forces an immediate refresh of the cache.

  This is a synchronous operation that blocks until refresh completes.

  ## Arguments

  - `cache` - Cache server reference

  ## Returns

  - `{:ok, stats}` - Newly computed statistics
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, stats} = Statistics.Cache.refresh(cache)
  """
  @spec refresh(cache()) :: {:ok, cached_stats()} | {:error, term()}
  def refresh(cache) do
    GenServer.call(cache, :refresh, :infinity)
  end

  @doc """
  Stops the cache GenServer.

  ## Arguments

  - `cache` - Cache server reference

  ## Returns

  - `:ok`
  """
  @spec stop(cache()) :: :ok
  def stop(cache) do
    GenServer.stop(cache, :normal)
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(opts) do
    db = Keyword.fetch!(opts, :db)
    refresh_interval = Keyword.get(opts, :refresh_interval, @default_refresh_interval)

    state = %{
      db: db,
      refresh_interval: refresh_interval,
      stats: nil,
      histogram: nil,
      timer_ref: nil
    }

    # Compute initial statistics asynchronously
    send(self(), :compute_stats)

    {:ok, state}
  end

  @impl true
  def handle_call(:get, _from, state) do
    case state.stats do
      nil ->
        # Stats not computed yet, compute synchronously
        case compute_stats(state.db) do
          {:ok, stats} ->
            new_state = schedule_refresh(%{state | stats: stats})
            {:reply, {:ok, stats}, new_state}

          {:error, _} = error ->
            {:reply, error, state}
        end

      stats ->
        {:reply, {:ok, stats}, state}
    end
  end

  @impl true
  def handle_call(:predicate_histogram, _from, state) do
    case state.histogram do
      nil ->
        # Histogram not computed yet, compute synchronously
        case compute_histogram(state.db) do
          {:ok, histogram} ->
            {:reply, {:ok, histogram}, %{state | histogram: histogram}}

          {:error, _} = error ->
            {:reply, error, state}
        end

      histogram ->
        {:reply, {:ok, histogram}, state}
    end
  end

  @impl true
  def handle_call(:refresh, _from, state) do
    case compute_all(state.db) do
      {:ok, stats, histogram} ->
        new_state =
          state
          |> Map.put(:stats, stats)
          |> Map.put(:histogram, histogram)
          |> schedule_refresh()

        {:reply, {:ok, stats}, new_state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_cast(:invalidate, state) do
    # Cancel any pending refresh timer
    if state.timer_ref do
      Process.cancel_timer(state.timer_ref)
    end

    # Clear cached data
    new_state = %{state | stats: nil, histogram: nil, timer_ref: nil}
    {:noreply, new_state}
  end

  @impl true
  def handle_info(:compute_stats, state) do
    case compute_all(state.db) do
      {:ok, stats, histogram} ->
        new_state =
          state
          |> Map.put(:stats, stats)
          |> Map.put(:histogram, histogram)
          |> schedule_refresh()

        {:noreply, new_state}

      {:error, reason} ->
        Logger.warning("Failed to compute statistics: #{inspect(reason)}")
        # Retry after interval
        new_state = schedule_refresh(state)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info(:refresh, state) do
    case compute_all(state.db) do
      {:ok, stats, histogram} ->
        new_state =
          state
          |> Map.put(:stats, stats)
          |> Map.put(:histogram, histogram)
          |> schedule_refresh()

        {:noreply, new_state}

      {:error, reason} ->
        Logger.warning("Failed to refresh statistics: #{inspect(reason)}")
        # Keep old stats, schedule next refresh
        new_state = schedule_refresh(state)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  @spec compute_stats(NIF.db_ref()) :: {:ok, cached_stats()} | {:error, term()}
  defp compute_stats(db) do
    with {:ok, base_stats} <- Statistics.all(db) do
      stats = Map.put(base_stats, :computed_at, DateTime.utc_now())
      {:ok, stats}
    end
  end

  @spec compute_histogram(NIF.db_ref()) :: {:ok, predicate_histogram()} | {:error, term()}
  defp compute_histogram(db) do
    # Get all predicates by scanning the POS index
    case NIF.prefix_stream(db, :pos, <<>>) do
      {:ok, stream} ->
        histogram =
          stream
          |> Stream.map(fn {key, _value} -> extract_first_id(key) end)
          |> Enum.reduce(%{}, fn predicate_id, acc ->
            Map.update(acc, predicate_id, 1, &(&1 + 1))
          end)

        {:ok, histogram}

      {:error, _} = error ->
        error
    end
  end

  @spec compute_all(NIF.db_ref()) ::
          {:ok, cached_stats(), predicate_histogram()} | {:error, term()}
  defp compute_all(db) do
    with {:ok, stats} <- compute_stats(db),
         {:ok, histogram} <- compute_histogram(db) do
      {:ok, stats, histogram}
    end
  end

  @spec schedule_refresh(map()) :: map()
  defp schedule_refresh(state) do
    # Cancel any existing timer
    if state.timer_ref do
      Process.cancel_timer(state.timer_ref)
    end

    # Schedule next refresh
    timer_ref = Process.send_after(self(), :refresh, state.refresh_interval)
    %{state | timer_ref: timer_ref}
  end

  # Extract the first 8-byte ID from a 24-byte index key
  @spec extract_first_id(binary()) :: non_neg_integer()
  defp extract_first_id(<<first_id::64-big, _rest::binary>>) do
    first_id
  end
end
