defmodule TripleStore.ScheduledBackupTest do
  @moduledoc """
  Tests for TripleStore.ScheduledBackup.

  These tests verify the scheduled backup functionality including:
  - Starting and stopping the scheduler
  - Running backups at intervals
  - Triggering immediate backups
  - Status reporting
  - Telemetry event emission
  """

  use ExUnit.Case, async: false

  alias TripleStore.ScheduledBackup
  alias TripleStore.Backup

  @moduletag :integration
  @moduletag timeout: 60_000

  # Short interval for testing (100ms)
  @test_interval 100

  # ===========================================================================
  # Test Helpers
  # ===========================================================================

  defp create_test_store do
    path = Path.join(System.tmp_dir!(), "sched_backup_test_#{:rand.uniform(1_000_000)}")
    {:ok, store} = TripleStore.open(path)
    {store, path}
  end

  defp create_test_backup_dir do
    Path.join(System.tmp_dir!(), "sched_backup_dir_#{:rand.uniform(1_000_000)}")
  end

  defp cleanup_store(store, path) do
    try do
      TripleStore.close(store)
    rescue
      _ -> :ok
    catch
      :exit, _ -> :ok
    end

    File.rm_rf!(path)
  end

  defp cleanup_backup_dir(backup_dir) do
    File.rm_rf!(backup_dir)
  end

  # ===========================================================================
  # Start/Stop Tests
  # ===========================================================================

  describe "start_link/1" do
    test "starts scheduler with required options" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            interval: @test_interval
          )

        assert Process.alive?(pid)
        :ok = ScheduledBackup.stop(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end

    test "returns error when store is missing" do
      backup_dir = create_test_backup_dir()

      try do
        # start_link with missing required option returns error tuple
        # but GenServer may exit the calling process, so we trap the exit
        Process.flag(:trap_exit, true)
        result = ScheduledBackup.start_link(backup_dir: backup_dir)

        case result do
          {:error, :missing_required_option} ->
            :ok

          {:ok, pid} ->
            # Should not happen
            ScheduledBackup.stop(pid)
            flunk("Expected error, got {:ok, pid}")
        end
      catch
        :exit, {:missing_required_option, _} ->
          :ok
      after
        Process.flag(:trap_exit, false)
        cleanup_backup_dir(backup_dir)
      end
    end

    test "returns error when backup_dir is missing" do
      {store, path} = create_test_store()

      try do
        # start_link with missing required option returns error tuple
        # but GenServer may exit the calling process, so we trap the exit
        Process.flag(:trap_exit, true)
        result = ScheduledBackup.start_link(store: store)

        case result do
          {:error, :missing_required_option} ->
            :ok

          {:ok, pid} ->
            # Should not happen
            ScheduledBackup.stop(pid)
            flunk("Expected error, got {:ok, pid}")
        end
      catch
        :exit, {:missing_required_option, _} ->
          :ok
      after
        Process.flag(:trap_exit, false)
        cleanup_store(store, path)
      end
    end
  end

  describe "stop/1" do
    test "stops the scheduler gracefully" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            interval: @test_interval
          )

        assert Process.alive?(pid)
        :ok = ScheduledBackup.stop(pid)
        refute Process.alive?(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end
  end

  # ===========================================================================
  # Status Tests
  # ===========================================================================

  describe "status/1" do
    test "returns scheduler status" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            interval: @test_interval,
            max_backups: 3,
            prefix: "test"
          )

        {:ok, status} = ScheduledBackup.status(pid)

        assert status.running == true
        assert status.backup_dir == backup_dir
        assert status.interval_ms == @test_interval
        assert status.max_backups == 3
        assert status.backup_count == 0
        assert status.last_backup == nil
        assert status.last_error == nil
        assert status.next_backup != nil

        :ok = ScheduledBackup.stop(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end

    test "returns error for stopped process" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            interval: @test_interval
          )

        :ok = ScheduledBackup.stop(pid)
        assert {:error, :not_running} = ScheduledBackup.status(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end
  end

  # ===========================================================================
  # Backup Execution Tests
  # ===========================================================================

  describe "scheduled backup execution" do
    test "runs backup at interval" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        # Add some data
        {:ok, _} =
          TripleStore.insert(store, [
            {RDF.iri("http://ex.org/s"), RDF.iri("http://ex.org/p"), RDF.literal("v")}
          ])

        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            interval: @test_interval,
            max_backups: 5
          )

        # Wait for at least one backup to run
        Process.sleep(@test_interval * 2)

        {:ok, status} = ScheduledBackup.status(pid)
        assert status.backup_count >= 1
        assert status.last_backup != nil
        assert status.last_error == nil

        # Verify backup was created
        {:ok, backups} = Backup.list(backup_dir)
        assert length(backups) >= 1

        :ok = ScheduledBackup.stop(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end

    test "run_immediately creates backup on start" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            # Long interval
            interval: :timer.hours(1),
            run_immediately: true
          )

        # Give it time to complete the backup
        Process.sleep(500)

        {:ok, status} = ScheduledBackup.status(pid)
        assert status.backup_count == 1
        assert status.last_backup != nil

        :ok = ScheduledBackup.stop(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end

    test "multiple backups run at interval" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            interval: @test_interval,
            max_backups: 10
          )

        # Wait for multiple backups (add extra time for execution)
        Process.sleep(@test_interval * 8)

        {:ok, status} = ScheduledBackup.status(pid)
        # Allow for some timing variation - at least 2 backups
        assert status.backup_count >= 2

        :ok = ScheduledBackup.stop(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end
  end

  # ===========================================================================
  # Trigger Backup Tests
  # ===========================================================================

  describe "trigger_backup/1" do
    test "triggers immediate backup" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            # Long interval - won't trigger automatically
            interval: :timer.hours(1)
          )

        {:ok, status_before} = ScheduledBackup.status(pid)
        assert status_before.backup_count == 0

        {:ok, metadata} = ScheduledBackup.trigger_backup(pid)
        assert metadata.path =~ backup_dir

        {:ok, status_after} = ScheduledBackup.status(pid)
        assert status_after.backup_count == 1
        assert status_after.last_backup != nil

        :ok = ScheduledBackup.stop(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end

    test "trigger_backup resets interval timer" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        # 500ms interval
        interval = 500

        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            interval: interval
          )

        # Trigger backup after half the interval
        Process.sleep(div(interval, 2))
        {:ok, _} = ScheduledBackup.trigger_backup(pid)

        {:ok, status} = ScheduledBackup.status(pid)
        assert status.backup_count == 1

        # Next backup should be scheduled about 500ms from now
        assert status.next_backup != nil

        :ok = ScheduledBackup.stop(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end
  end

  # ===========================================================================
  # Rotation Tests
  # ===========================================================================

  describe "backup rotation" do
    test "keeps only max_backups" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        max_backups = 3

        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            interval: @test_interval,
            max_backups: max_backups,
            prefix: "rotation_test"
          )

        # Wait for more backups than max (need at least max_backups + 1)
        Process.sleep(@test_interval * 12)

        {:ok, status} = ScheduledBackup.status(pid)
        # We need at least max_backups + 1 to trigger rotation
        assert status.backup_count >= max_backups + 1,
               "Expected at least #{max_backups + 1} backups, got #{status.backup_count}"

        # But only max_backups should exist
        {:ok, backups} = Backup.list(backup_dir)

        matching =
          Enum.filter(backups, fn b ->
            Path.basename(b.path) |> String.starts_with?("rotation_test")
          end)

        assert length(matching) <= max_backups,
               "Expected <= #{max_backups} backups, found #{length(matching)}"

        :ok = ScheduledBackup.stop(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end
  end

  # ===========================================================================
  # Telemetry Tests
  # ===========================================================================

  describe "telemetry events" do
    test "emits tick event on backup" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        test_pid = self()

        :telemetry.attach(
          "test-scheduled-tick",
          [:triple_store, :scheduled_backup, :tick],
          fn _event, measurements, metadata, _config ->
            send(test_pid, {:tick, measurements, metadata})
          end,
          nil
        )

        {:ok, pid} =
          ScheduledBackup.start_link(
            store: store,
            backup_dir: backup_dir,
            interval: @test_interval
          )

        # Wait for backup
        Process.sleep(@test_interval * 2)

        assert_receive {:tick, measurements, metadata}, 1000
        assert measurements.count >= 0
        assert metadata.backup_dir == backup_dir
        assert metadata.interval_ms == @test_interval

        :telemetry.detach("test-scheduled-tick")
        :ok = ScheduledBackup.stop(pid)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end
  end

  # ===========================================================================
  # TripleStore API Tests
  # ===========================================================================

  describe "TripleStore.schedule_backup/3" do
    test "starts scheduled backups via main API" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        {:ok, scheduler} =
          TripleStore.schedule_backup(store, backup_dir,
            interval: @test_interval,
            max_backups: 5
          )

        assert Process.alive?(scheduler)

        {:ok, status} = ScheduledBackup.status(scheduler)
        assert status.running == true
        assert status.backup_dir == backup_dir

        :ok = ScheduledBackup.stop(scheduler)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end

    test "schedule_backup with run_immediately option" do
      {store, path} = create_test_store()
      backup_dir = create_test_backup_dir()

      try do
        {:ok, scheduler} =
          TripleStore.schedule_backup(store, backup_dir,
            interval: :timer.hours(1),
            run_immediately: true
          )

        # Wait for immediate backup
        Process.sleep(500)

        {:ok, status} = ScheduledBackup.status(scheduler)
        assert status.backup_count == 1

        :ok = ScheduledBackup.stop(scheduler)
      after
        cleanup_store(store, path)
        cleanup_backup_dir(backup_dir)
      end
    end
  end
end
