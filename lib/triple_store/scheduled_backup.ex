defmodule TripleStore.ScheduledBackup do
  @moduledoc """
  GenServer for scheduled periodic backups.

  Provides automatic backup scheduling with configurable intervals and
  rotation policies. Uses `TripleStore.Backup.rotate/3` for each backup
  to automatically clean up old backups.

  The scheduler monitors the store's dictionary manager. That process is
  owned by every store opened through `TripleStore.open/2`, including stores
  that use an external transaction coordinator, and it is stopped before the
  database is closed. Its termination is therefore the scheduler's signal to
  cancel timers, stop any in-progress backup task, and terminate without
  scheduling another backup against a closed store.

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
  - `:backup_runner` - Optional three-argument function used to execute a
    rotation. It defaults to `TripleStore.Backup.rotate/3` and is primarily
    useful for controlled embedding and lifecycle tests.

  ## Telemetry Events

  Emits standard backup telemetry events via `TripleStore.Backup.rotate/3`.
  Additionally emits:

  - `[:triple_store, :scheduled_backup, :tick]` - On each scheduled backup attempt
    - Measurements: `%{count: integer}` - Number of successful backups so far
    - Metadata: `%{backup_dir: String.t, interval_ms: integer}`

  - `[:triple_store, :scheduled_backup, :error]` - On backup failure
    - Measurements: `%{}`
    - Metadata: `%{reason: term, backup_dir: String.t}`

  - `[:triple_store, :scheduled_backup, :stop]` - When the scheduler stops
    - Measurements: `%{backup_count: integer}`
    - Metadata: `%{reason: atom, backup_dir: String.t}`

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
          run_immediately: boolean(),
          backup_runner: (TripleStore.store(), Path.t(), keyword() ->
                            {:ok, Backup.backup_metadata()} | {:error, term()})
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
          next_backup: DateTime.t() | nil,
          backup_in_progress: boolean()
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
  - `:backup_runner` - Optional three-argument backup function; defaults to
    `TripleStore.Backup.rotate/3`

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
      backup_runner = Keyword.get(opts, :backup_runner, &Backup.rotate/3)

      with {:ok, lifecycle_pid} <- store_lifecycle_pid(store),
           true <- is_function(backup_runner, 3) do
        lifecycle_ref = Process.monitor(lifecycle_pid)

        state = %{
          store: store,
          backup_dir: backup_dir,
          interval: interval,
          max_backups: max_backups,
          prefix: prefix,
          backup_runner: backup_runner,
          backup_count: 0,
          last_backup: nil,
          last_error: nil,
          timer_ref: nil,
          lifecycle_pid: lifecycle_pid,
          lifecycle_ref: lifecycle_ref,
          backup_task: nil,
          backup_waiter: nil,
          stop_reason: :operator_stop
        }

        if run_immediately do
          send(self(), :backup)
          {:ok, state}
        else
          {:ok, schedule_next_backup(state)}
        end
      else
        false -> {:stop, :invalid_backup_runner}
        {:error, reason} -> {:stop, reason}
      end
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
      next_backup: calculate_next_backup(state),
      backup_in_progress: not is_nil(state.backup_task)
    }

    {:reply, {:ok, status}, state}
  end

  @impl true
  def handle_call(:trigger_backup, from, %{backup_task: nil} = state) do
    state = state |> cancel_timer() |> start_backup(from)
    {:noreply, state}
  end

  def handle_call(:trigger_backup, _from, state) do
    {:reply, {:error, :backup_in_progress}, state}
  end

  @impl true
  def handle_info(:backup, %{backup_task: nil} = state) do
    {:noreply, start_backup(state, nil)}
  end

  @impl true
  def handle_info(:backup, state), do: {:noreply, state}

  def handle_info(
        {ref, result},
        %{backup_task: %Task{ref: ref}} = state
      ) do
    Process.demonitor(ref, [:flush])
    finish_backup(result, %{state | backup_task: nil})
  end

  def handle_info(
        {:DOWN, lifecycle_ref, :process, lifecycle_pid, reason},
        %{lifecycle_ref: lifecycle_ref, lifecycle_pid: lifecycle_pid} = state
      ) do
    Logger.warning("Store lifecycle ended; stopping scheduled backups")

    state = %{
      state
      | lifecycle_ref: nil,
        stop_reason: :store_lifecycle_down,
        last_error: {:store_lifecycle_down, reason}
    }

    reply_to_waiter(state.backup_waiter, {:error, {:store_unavailable, reason}})
    {:stop, :normal, %{state | backup_waiter: nil}}
  end

  def handle_info(
        {:DOWN, task_ref, :process, task_pid, reason},
        %{backup_task: %Task{ref: task_ref, pid: task_pid}} = state
      ) do
    finish_backup({:error, {:backup_task_exit, reason}}, %{state | backup_task: nil})
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    state
    |> cancel_timer()
    |> stop_backup_task()
    |> demonitor_store()

    :telemetry.execute(
      [:triple_store, :scheduled_backup, :stop],
      %{backup_count: state.backup_count},
      %{reason: state.stop_reason, backup_dir: Path.basename(state.backup_dir)}
    )

    :ok
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp start_backup(state, waiter) do
    emit_backup_tick(state)

    task =
      Task.async(fn ->
        run_backup(state)
      end)

    %{state | backup_task: task, backup_waiter: waiter, timer_ref: nil}
  end

  defp run_backup(state) do
    %{
      store: store,
      backup_dir: backup_dir,
      max_backups: max_backups,
      prefix: prefix,
      backup_runner: backup_runner
    } = state

    backup_runner.(store, backup_dir, max_backups: max_backups, prefix: prefix)
  rescue
    exception -> {:error, {:backup_exception, exception.__struct__}}
  catch
    kind, reason -> {:error, {:backup_throw, kind, reason}}
  end

  defp emit_backup_tick(state) do
    :telemetry.execute(
      [:triple_store, :scheduled_backup, :tick],
      %{count: state.backup_count},
      %{backup_dir: Path.basename(state.backup_dir), interval_ms: state.interval}
    )
  end

  defp finish_backup(result, state) do
    waiter = state.backup_waiter
    state = %{state | backup_waiter: nil}

    case result do
      {:ok, metadata} ->
        Logger.info("Scheduled backup completed: #{metadata.path}")

        state = %{
          state
          | backup_count: state.backup_count + 1,
            last_backup: DateTime.utc_now(),
            last_error: nil
        }

        reply_to_waiter(waiter, {:ok, metadata})
        {:noreply, schedule_next_backup(state)}

      {:error, reason} = error ->
        Logger.error("Scheduled backup failed: #{inspect(reason)}")
        emit_backup_error(state, reason)
        reply_to_waiter(waiter, error)
        state = %{state | last_error: reason}

        if terminal_backup_error?(reason) do
          {:stop, :normal, %{state | stop_reason: :terminal_backup_error}}
        else
          {:noreply, schedule_next_backup(state)}
        end

      invalid_result ->
        finish_backup({:error, {:invalid_backup_result, invalid_result}}, state)
    end
  end

  defp emit_backup_error(state, reason) do
    :telemetry.execute(
      [:triple_store, :scheduled_backup, :error],
      %{},
      %{reason: reason, backup_dir: Path.basename(state.backup_dir)}
    )
  end

  defp terminal_backup_error?(reason) do
    reason in [:database_closed, :db_closed, :invalid_db] or
      match?({:corrupt_acl, _}, reason) or
      match?({:corrupt_provenance, _}, reason) or
      match?({:cannot_open, _}, reason)
  end

  defp reply_to_waiter(nil, _result), do: :ok
  defp reply_to_waiter(waiter, result), do: GenServer.reply(waiter, result)

  defp schedule_next_backup(state) do
    timer_ref = Process.send_after(self(), :backup, state.interval)
    %{state | timer_ref: timer_ref}
  end

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(%{timer_ref: ref} = state) do
    Process.cancel_timer(ref)
    %{state | timer_ref: nil}
  end

  defp stop_backup_task(%{backup_task: nil} = state), do: state

  defp stop_backup_task(%{backup_task: task, backup_waiter: waiter} = state) do
    Task.shutdown(task, :brutal_kill)
    reply_to_waiter(waiter, {:error, :scheduler_stopped})
    %{state | backup_task: nil, backup_waiter: nil}
  end

  defp demonitor_store(%{lifecycle_ref: nil} = state), do: state

  defp demonitor_store(%{lifecycle_ref: ref} = state) do
    Process.demonitor(ref, [:flush])
    %{state | lifecycle_ref: nil}
  end

  defp store_lifecycle_pid(%{dict_manager: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: {:ok, pid}, else: {:error, :store_unavailable}
  end

  defp store_lifecycle_pid(_store), do: {:error, :invalid_store_lifecycle}

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
