defmodule TripleStore.Snapshot do
  @moduledoc """
  Snapshot management with TTL-based lifecycle and automatic cleanup.

  This module provides safe snapshot management to prevent resource leaks:

  - **TTL Support**: Snapshots expire after a configurable time (default 5 minutes)
  - **Owner Tracking**: Snapshots are tied to owner processes and auto-released on termination
  - **Safe Wrapper**: `with_snapshot/2` ensures cleanup on success or exception

  ## Usage

  ### Safe Wrapper (Recommended)

      TripleStore.Snapshot.with_snapshot(db_ref, fn snapshot ->
        # Use snapshot for consistent reads
        NIF.snapshot_get(snapshot, :spo, key)
      end)

  ### Manual Management

      # Create with default 5-minute TTL
      {:ok, snapshot} = TripleStore.Snapshot.create(db_ref)

      # Create with custom TTL
      {:ok, snapshot} = TripleStore.Snapshot.create(db_ref, ttl: :timer.minutes(10))

      # Release when done
      :ok = TripleStore.Snapshot.release(snapshot)

  ## Telemetry Events

  - `[:triple_store, :snapshot, :created]` - Snapshot created
  - `[:triple_store, :snapshot, :released]` - Snapshot released (with reason)
  - `[:triple_store, :snapshot, :expired_warning]` - Snapshot approaching TTL

  """

  use GenServer

  require Logger

  alias TripleStore.Backend.RocksDB.NIF

  # Default TTL: 5 minutes
  @default_ttl :timer.minutes(5)

  # Soft TTL warning threshold: 80% of TTL
  @soft_ttl_ratio 0.8

  # Periodic cleanup interval: 60 seconds
  @cleanup_interval :timer.seconds(60)

  @type snapshot_ref :: reference()
  @type db_ref :: reference()
  @type option :: {:ttl, pos_integer()}

  @doc """
  Starts the snapshot registry.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Creates a snapshot with automatic TTL management.

  The snapshot is registered with the calling process as owner. If the owner
  process terminates, the snapshot is automatically released.

  ## Options

  - `:ttl` - Time-to-live in milliseconds (default: 5 minutes)

  ## Examples

      {:ok, snapshot} = TripleStore.Snapshot.create(db_ref)
      {:ok, snapshot} = TripleStore.Snapshot.create(db_ref, ttl: :timer.minutes(10))

  """
  @spec create(db_ref(), [option()]) :: {:ok, snapshot_ref()} | {:error, term()}
  def create(db_ref, opts \\ []) do
    ttl = Keyword.get(opts, :ttl, @default_ttl)
    owner = self()

    case NIF.snapshot(db_ref) do
      {:ok, snapshot_ref} ->
        case GenServer.call(__MODULE__, {:register, snapshot_ref, owner, ttl}) do
          :ok ->
            emit_created(snapshot_ref, ttl)
            {:ok, snapshot_ref}

          {:error, _} = error ->
            # Registration failed, release the snapshot
            NIF.release_snapshot(snapshot_ref)
            error
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Releases a snapshot and removes it from the registry.

  ## Examples

      :ok = TripleStore.Snapshot.release(snapshot)

  """
  @spec release(snapshot_ref()) :: :ok | {:error, term()}
  def release(snapshot_ref) do
    case GenServer.call(__MODULE__, {:unregister, snapshot_ref}) do
      :ok ->
        result = NIF.release_snapshot(snapshot_ref)
        emit_released(snapshot_ref, :manual)
        result

      {:error, :not_found} ->
        # Not in registry, try direct release anyway
        NIF.release_snapshot(snapshot_ref)
    end
  end

  @doc """
  Executes a function with a snapshot, ensuring cleanup on success or exception.

  This is the recommended way to use snapshots as it guarantees the snapshot
  is released regardless of how the function exits.

  ## Options

  - `:ttl` - Time-to-live in milliseconds (default: 5 minutes)

  ## Examples

      result = TripleStore.Snapshot.with_snapshot(db_ref, fn snapshot ->
        NIF.snapshot_get(snapshot, :spo, "key1")
      end)

      # With custom TTL
      result = TripleStore.Snapshot.with_snapshot(db_ref, [ttl: :timer.minutes(1)], fn snapshot ->
        # Quick operation
      end)

  """
  @spec with_snapshot(db_ref(), [option()], (snapshot_ref() -> result)) :: result
        when result: term()
  def with_snapshot(db_ref, opts \\ [], fun) when is_function(fun, 1) do
    case create(db_ref, opts) do
      {:ok, snapshot} ->
        try do
          fun.(snapshot)
        after
          release(snapshot)
        end

      {:error, reason} ->
        raise "Failed to create snapshot: #{inspect(reason)}"
    end
  end

  @doc """
  Returns information about active snapshots.

  ## Examples

      info = TripleStore.Snapshot.info()
      # %{count: 2, snapshots: [...]}

  """
  @spec info() :: %{count: non_neg_integer(), snapshots: [map()]}
  def info do
    GenServer.call(__MODULE__, :info)
  end

  @doc """
  Returns the number of active snapshots.
  """
  @spec count() :: non_neg_integer()
  def count do
    GenServer.call(__MODULE__, :count)
  end

  # GenServer Callbacks

  @impl true
  def init(_opts) do
    # Schedule periodic cleanup
    schedule_cleanup()

    state = %{
      # snapshot_ref => %{owner: pid, monitor: ref, created_at: time, ttl: ms}
      snapshots: %{},
      # monitor_ref => snapshot_ref
      monitors: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:register, snapshot_ref, owner, ttl}, _from, state) do
    # Monitor the owner process
    monitor_ref = Process.monitor(owner)
    now = System.monotonic_time(:millisecond)

    entry = %{
      owner: owner,
      monitor: monitor_ref,
      created_at: now,
      ttl: ttl,
      warned: false
    }

    new_state = %{
      state
      | snapshots: Map.put(state.snapshots, snapshot_ref, entry),
        monitors: Map.put(state.monitors, monitor_ref, snapshot_ref)
    }

    {:reply, :ok, new_state}
  end

  def handle_call({:unregister, snapshot_ref}, _from, state) do
    case Map.fetch(state.snapshots, snapshot_ref) do
      {:ok, entry} ->
        # Stop monitoring the owner
        Process.demonitor(entry.monitor, [:flush])

        new_state = %{
          state
          | snapshots: Map.delete(state.snapshots, snapshot_ref),
            monitors: Map.delete(state.monitors, entry.monitor)
        }

        {:reply, :ok, new_state}

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:info, _from, state) do
    now = System.monotonic_time(:millisecond)

    snapshots =
      Enum.map(state.snapshots, fn {ref, entry} ->
        age_ms = now - entry.created_at
        remaining_ms = max(0, entry.ttl - age_ms)

        %{
          ref: ref,
          owner: entry.owner,
          age_ms: age_ms,
          remaining_ms: remaining_ms,
          ttl: entry.ttl
        }
      end)

    {:reply, %{count: map_size(state.snapshots), snapshots: snapshots}, state}
  end

  def handle_call(:count, _from, state) do
    {:reply, map_size(state.snapshots), state}
  end

  @impl true
  def handle_info({:DOWN, monitor_ref, :process, _pid, _reason}, state) do
    case Map.fetch(state.monitors, monitor_ref) do
      {:ok, snapshot_ref} ->
        # Owner process died, release the snapshot
        release_snapshot_internal(snapshot_ref, :owner_down)

        new_state = %{
          state
          | snapshots: Map.delete(state.snapshots, snapshot_ref),
            monitors: Map.delete(state.monitors, monitor_ref)
        }

        {:noreply, new_state}

      :error ->
        {:noreply, state}
    end
  end

  def handle_info(:cleanup, state) do
    new_state = cleanup_expired(state)
    schedule_cleanup()
    {:noreply, new_state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Private Functions

  defp schedule_cleanup do
    Process.send_after(self(), :cleanup, @cleanup_interval)
  end

  defp cleanup_expired(state) do
    now = System.monotonic_time(:millisecond)

    {expired, _warned, remaining} =
      Enum.reduce(state.snapshots, {[], [], %{}}, fn {ref, entry}, {exp, warn, rem} ->
        age_ms = now - entry.created_at

        cond do
          # Expired
          age_ms >= entry.ttl ->
            {[{ref, entry} | exp], warn, rem}

          # Approaching expiry (soft TTL warning)
          age_ms >= entry.ttl * @soft_ttl_ratio and not entry.warned ->
            emit_expired_warning(ref, entry.ttl - age_ms)
            {exp, [{ref, entry} | warn], Map.put(rem, ref, %{entry | warned: true})}

          true ->
            {exp, warn, Map.put(rem, ref, entry)}
        end
      end)

    # Release expired snapshots
    Enum.each(expired, fn {ref, entry} ->
      Process.demonitor(entry.monitor, [:flush])
      release_snapshot_internal(ref, :expired)
    end)

    # Update monitors map
    expired_monitors = Enum.map(expired, fn {_ref, entry} -> entry.monitor end)

    new_monitors =
      Enum.reduce(expired_monitors, state.monitors, fn mon, acc ->
        Map.delete(acc, mon)
      end)

    %{state | snapshots: remaining, monitors: new_monitors}
  end

  defp release_snapshot_internal(snapshot_ref, reason) do
    case NIF.release_snapshot(snapshot_ref) do
      :ok ->
        emit_released(snapshot_ref, reason)
        :ok

      {:error, :snapshot_released} ->
        # Already released, that's fine
        :ok

      error ->
        Logger.warning("Failed to release snapshot #{inspect(snapshot_ref)}: #{inspect(error)}")
        error
    end
  end

  # Telemetry

  defp emit_created(snapshot_ref, ttl) do
    :telemetry.execute(
      [:triple_store, :snapshot, :created],
      %{count: 1},
      %{snapshot_ref: snapshot_ref, ttl: ttl, owner: self()}
    )
  end

  defp emit_released(snapshot_ref, reason) do
    :telemetry.execute(
      [:triple_store, :snapshot, :released],
      %{count: 1},
      %{snapshot_ref: snapshot_ref, reason: reason}
    )
  end

  defp emit_expired_warning(snapshot_ref, remaining_ms) do
    :telemetry.execute(
      [:triple_store, :snapshot, :expired_warning],
      %{remaining_ms: remaining_ms},
      %{snapshot_ref: snapshot_ref}
    )
  end
end
