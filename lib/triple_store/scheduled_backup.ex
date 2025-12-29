defmodule TripleStore.ScheduledBackup do
  @moduledoc """
  GenServer for scheduled periodic backups.

  Provides automatic backup scheduling with configurable intervals and
  rotation policies. Uses `TripleStore.Backup.rotate/3` for each backup
  to automatically clean up old backups.

  ## Usage

      # Start scheduled backups every hour, keeping 24 backups
      {:ok, pid} = TripleStore.ScheduledBackup.start_link(
        store: store,
        backup_dir: "/backups/mydb",
        interval: :timer.hours(1),
        max_backups: 24
      )

      # Check status
      {:ok, status} = TripleStore.ScheduledBackup.status(pid)

      # Stop scheduled backups
      :ok = TripleStore.ScheduledBackup.stop(pid)

  ## Options

  - `:store` - (required) Store handle from `TripleStore.open/2`
  - `:backup_dir` - (required) Directory to store backups
  - `:interval` - Backup interval in milliseconds (default: 1 hour)
  - `:max_backups` - Maximum backups to keep (default: 5)
  - `:prefix` - Backup name prefix (default: "scheduled")
  - `:run_immediately` - Run first backup immediately (default: false)

  ## Telemetry Events

  Emits standard backup telemetry events via `TripleStore.Backup.rotate/3`.
  Additionally emits:

  - `[:triple_store, :scheduled_backup, :tick]` - On each scheduled backup attempt
    - Measurements: `%{count: integer}` - Number of successful backups so far
    - Metadata: `%{backup_dir: String.t, interval_ms: integer}`

  - `[:triple_store, :scheduled_backup, :error]` - On backup failure
    - Measurements: `%{}`
    - Metadata: `%{reason: term, backup_dir: String.t}`

  """

  use GenServer

  alias TripleStore.Backup

  require Logger

  # Default backup interval: 1 hour
  @default_interval :timer.hours(1)

  # Default max backups to keep
  @default_max_backups 5

  # Default backup prefix
  @default_prefix "scheduled"

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Scheduler options"
  @type opts :: [
          store: TripleStore.store(),
          backup_dir: Path.t(),
          interval: pos_integer(),
          max_backups: pos_integer(),
          prefix: String.t(),
          run_immediately: boolean()
        ]

  @typedoc "Scheduler status"
  @type status :: %{
          running: boolean(),
          backup_dir: Path.t(),
          interval_ms: pos_integer(),
          max_backups: pos_integer(),
          backup_count: non_neg_integer(),
          last_backup: DateTime.t() | nil,
          last_error: term() | nil,
          next_backup: DateTime.t() | nil
        }

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts a scheduled backup process.

  ## Options

  - `:store` - (required) Store handle from `TripleStore.open/2`
  - `:backup_dir` - (required) Directory to store backups
  - `:interval` - Backup interval in milliseconds (default: 1 hour)
  - `:max_backups` - Maximum backups to keep (default: 5)
  - `:prefix` - Backup name prefix (default: "scheduled")
  - `:run_immediately` - Run first backup immediately (default: false)

  ## Returns

  - `{:ok, pid}` - Scheduler started successfully
  - `{:error, reason}` - Failed to start scheduler

  ## Examples

      {:ok, pid} = TripleStore.ScheduledBackup.start_link(
        store: store,
        backup_dir: "/backups",
        interval: :timer.minutes(30)
      )

  """
  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Gets the status of the scheduled backup process.

  ## Returns

  - `{:ok, status}` - Current status
  - `{:error, :not_running}` - Process is not running

  """
  @spec status(GenServer.server()) :: {:ok, status()} | {:error, :not_running}
  def status(server) do
    GenServer.call(server, :status)
  catch
    :exit, _ -> {:error, :not_running}
  end

  @doc """
  Triggers an immediate backup, resetting the interval timer.

  ## Returns

  - `{:ok, metadata}` - Backup completed successfully
  - `{:error, reason}` - Backup failed

  """
  @spec trigger_backup(GenServer.server()) ::
          {:ok, Backup.backup_metadata()} | {:error, term()}
  def trigger_backup(server) do
    GenServer.call(server, :trigger_backup, :timer.minutes(5))
  end

  @doc """
  Stops the scheduled backup process gracefully.

  ## Returns

  - `:ok` - Process stopped

  """
  @spec stop(GenServer.server()) :: :ok
  def stop(server) do
    GenServer.stop(server, :normal)
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(opts) do
    with {:ok, store} <- Keyword.fetch(opts, :store),
         {:ok, backup_dir} <- Keyword.fetch(opts, :backup_dir) do
      interval = Keyword.get(opts, :interval, @default_interval)
      max_backups = Keyword.get(opts, :max_backups, @default_max_backups)
      prefix = Keyword.get(opts, :prefix, @default_prefix)
      run_immediately = Keyword.get(opts, :run_immediately, false)

      state = %{
        store: store,
        backup_dir: backup_dir,
        interval: interval,
        max_backups: max_backups,
        prefix: prefix,
        backup_count: 0,
        last_backup: nil,
        last_error: nil,
        timer_ref: nil
      }

      # Schedule first backup
      state =
        if run_immediately do
          # Run immediately by sending a message to ourselves
          send(self(), :backup)
          state
        else
          schedule_next_backup(state)
        end

      {:ok, state}
    else
      :error ->
        {:stop, :missing_required_option}
    end
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      running: true,
      backup_dir: state.backup_dir,
      interval_ms: state.interval,
      max_backups: state.max_backups,
      backup_count: state.backup_count,
      last_backup: state.last_backup,
      last_error: state.last_error,
      next_backup: calculate_next_backup(state)
    }

    {:reply, {:ok, status}, state}
  end

  @impl true
  def handle_call(:trigger_backup, _from, state) do
    # Cancel existing timer
    state = cancel_timer(state)

    # Run backup
    {result, state} = do_backup(state)

    # Schedule next backup
    state = schedule_next_backup(state)

    {:reply, result, state}
  end

  @impl true
  def handle_info(:backup, state) do
    # Run backup
    {_result, state} = do_backup(state)

    # Schedule next backup
    state = schedule_next_backup(state)

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    # Store process died - stop the scheduler
    Logger.warning("Store process died, stopping scheduled backups")
    {:stop, :normal, state}
  end

  @impl true
  def terminate(_reason, state) do
    cancel_timer(state)
    :ok
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp do_backup(state) do
    %{
      store: store,
      backup_dir: backup_dir,
      max_backups: max_backups,
      prefix: prefix,
      backup_count: count
    } = state

    # Emit telemetry for scheduled backup tick
    :telemetry.execute(
      [:triple_store, :scheduled_backup, :tick],
      %{count: count},
      %{backup_dir: backup_dir, interval_ms: state.interval}
    )

    case Backup.rotate(store, backup_dir, max_backups: max_backups, prefix: prefix) do
      {:ok, metadata} ->
        Logger.info("Scheduled backup completed: #{metadata.path}")

        state = %{
          state
          | backup_count: count + 1,
            last_backup: DateTime.utc_now(),
            last_error: nil
        }

        {{:ok, metadata}, state}

      {:error, reason} = error ->
        Logger.error("Scheduled backup failed: #{inspect(reason)}")

        # Emit error telemetry
        :telemetry.execute(
          [:triple_store, :scheduled_backup, :error],
          %{},
          %{reason: reason, backup_dir: backup_dir}
        )

        state = %{state | last_error: reason}
        {error, state}
    end
  end

  defp schedule_next_backup(state) do
    timer_ref = Process.send_after(self(), :backup, state.interval)
    %{state | timer_ref: timer_ref}
  end

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(%{timer_ref: ref} = state) do
    Process.cancel_timer(ref)
    %{state | timer_ref: nil}
  end

  defp calculate_next_backup(%{timer_ref: nil}), do: nil

  defp calculate_next_backup(%{timer_ref: ref, interval: _interval}) do
    case Process.read_timer(ref) do
      false ->
        nil

      remaining_ms ->
        DateTime.utc_now()
        |> DateTime.add(remaining_ms, :millisecond)
    end
  end
end
