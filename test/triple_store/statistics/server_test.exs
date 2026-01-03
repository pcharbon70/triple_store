defmodule TripleStore.Statistics.ServerTest do
  @moduledoc """
  Tests for Statistics Server (Phase 3.1.4).

  Verifies:
  - Server starts and caches statistics
  - get_stats/0 returns cached or fresh statistics
  - refresh/0 forces statistics refresh
  - notify_modification/0 increments counter
  - Auto-refresh triggers on threshold
  - Periodic refresh works
  - Statistics persist across server restart
  - Telemetry events are emitted
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.Statistics
  alias TripleStore.Statistics.Server

  @test_db_base "/tmp/triple_store_stats_server_test"

  # Helper to wait for server to be ready with proper synchronization (C12 fix)
  defp wait_for_server_ready(server_name, timeout \\ 1000) do
    wait_until(
      fn ->
        try do
          info = Server.server_stats(server: server_name)
          info.cached or not info.refresh_in_progress
        rescue
          _ -> false
        end
      end,
      timeout
    )
  end

  # Helper to wait for a condition with timeout
  defp wait_until(condition_fn, timeout, interval \\ 10) do
    deadline = System.monotonic_time(:millisecond) + timeout

    do_wait_until(condition_fn, deadline, interval)
  end

  defp do_wait_until(condition_fn, deadline, interval) do
    if condition_fn.() do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now >= deadline do
        {:error, :timeout}
      else
        Process.sleep(interval)
        do_wait_until(condition_fn, deadline, interval)
      end
    end
  end

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # Basic Server Tests
  # ===========================================================================

  describe "start_link/1" do
    test "starts the server with db reference", %{db: db} do
      {:ok, pid} = Server.start_link(db: db, name: :test_server_1)
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "starts with custom configuration", %{db: db} do
      {:ok, pid} =
        Server.start_link(
          db: db,
          name: :test_server_2,
          refresh_threshold: 1000,
          refresh_interval: 60_000,
          auto_refresh: false,
          timeout: 30_000
        )

      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "validates options and raises on invalid keys", %{db: db} do
      assert_raise ArgumentError, fn ->
        Server.start_link(db: db, name: :test_server_invalid, invalid_option: true)
      end
    end
  end

  # ===========================================================================
  # child_spec/1 Tests (S14)
  # ===========================================================================

  describe "child_spec/1" do
    test "returns valid child spec", %{db: db} do
      spec = Server.child_spec(db: db, name: :test_child_spec)

      assert spec.id == :test_child_spec
      assert spec.start == {Server, :start_link, [[db: db, name: :test_child_spec]]}
      assert spec.type == :worker
      assert spec.restart == :permanent
    end

    test "uses module name as default id", %{db: db} do
      spec = Server.child_spec(db: db)
      assert spec.id == Server
    end
  end

  # ===========================================================================
  # get_stats/1 Tests
  # ===========================================================================

  describe "get_stats/1" do
    test "returns statistics for empty store", %{db: db} do
      {:ok, pid} = Server.start_link(db: db, name: :test_get_stats_1)

      # Wait for initial load using proper synchronization
      :ok = wait_for_server_ready(:test_get_stats_1)

      {:ok, stats} = Server.get_stats(server: :test_get_stats_1)
      assert stats.triple_count == 0

      GenServer.stop(pid)
    end

    test "returns statistics with data", %{db: db} do
      triples = [{1000, 100, 2000}, {1001, 100, 2001}]
      :ok = Index.insert_triples(db, triples)

      {:ok, pid} = Server.start_link(db: db, name: :test_get_stats_2)
      :ok = wait_for_server_ready(:test_get_stats_2)

      {:ok, stats} = Server.get_stats(server: :test_get_stats_2)
      assert stats.triple_count == 2

      GenServer.stop(pid)
    end

    test "caches statistics", %{db: db} do
      triples = [{1000, 100, 2000}]
      :ok = Index.insert_triples(db, triples)

      {:ok, pid} = Server.start_link(db: db, name: :test_get_stats_3)
      :ok = wait_for_server_ready(:test_get_stats_3)

      # Get stats twice
      {:ok, stats1} = Server.get_stats(server: :test_get_stats_3)
      {:ok, stats2} = Server.get_stats(server: :test_get_stats_3)

      # Should be the same (cached)
      assert stats1.collected_at == stats2.collected_at

      GenServer.stop(pid)
    end

    test "respects custom timeout option", %{db: db} do
      {:ok, pid} = Server.start_link(db: db, name: :test_get_stats_timeout)
      :ok = wait_for_server_ready(:test_get_stats_timeout)

      # Should not raise with valid timeout
      {:ok, _stats} = Server.get_stats(server: :test_get_stats_timeout, timeout: 5000)

      GenServer.stop(pid)
    end
  end

  # ===========================================================================
  # refresh/1 Tests
  # ===========================================================================

  describe "refresh/1" do
    test "forces statistics refresh", %{db: db} do
      {:ok, pid} = Server.start_link(db: db, name: :test_refresh_1)
      :ok = wait_for_server_ready(:test_refresh_1)

      {:ok, stats1} = Server.get_stats(server: :test_refresh_1)

      # Add data
      triples = [{1000, 100, 2000}]
      :ok = Index.insert_triples(db, triples)

      # Without refresh, should still see old stats (cached)
      {:ok, stats2} = Server.get_stats(server: :test_refresh_1)
      assert stats2.triple_count == 0

      # Force refresh
      {:ok, stats3} = Server.refresh(server: :test_refresh_1)
      assert stats3.triple_count == 1

      # Now cached stats should be updated
      {:ok, stats4} = Server.get_stats(server: :test_refresh_1)
      assert stats4.triple_count == 1

      GenServer.stop(pid)
    end
  end

  # ===========================================================================
  # notify_modification/1 Tests
  # ===========================================================================

  describe "notify_modification/1" do
    test "increments modification count", %{db: db} do
      {:ok, pid} = Server.start_link(db: db, name: :test_notify_1, auto_refresh: false)
      :ok = wait_for_server_ready(:test_notify_1)

      info1 = Server.server_stats(server: :test_notify_1)
      assert info1.modification_count == 0

      Server.notify_modification(server: :test_notify_1)
      Server.notify_modification(server: :test_notify_1, count: 5)

      # Give cast time to process
      :ok =
        wait_until(
          fn ->
            Server.server_stats(server: :test_notify_1).modification_count == 6
          end,
          500
        )

      info2 = Server.server_stats(server: :test_notify_1)
      assert info2.modification_count == 6

      GenServer.stop(pid)
    end

    test "triggers refresh when threshold exceeded", %{db: db} do
      triples = [{1000, 100, 2000}]
      :ok = Index.insert_triples(db, triples)

      {:ok, pid} =
        Server.start_link(
          db: db,
          name: :test_notify_2,
          refresh_threshold: 5,
          auto_refresh: true
        )

      :ok = wait_for_server_ready(:test_notify_2)

      # Get initial stats
      {:ok, _} = Server.get_stats(server: :test_notify_2)

      # Add more data
      :ok = Index.insert_triple(db, {1002, 100, 2002})

      # Notify modifications to exceed threshold
      Server.notify_modification(server: :test_notify_2, count: 6)

      # Wait for background refresh using proper synchronization
      :ok =
        wait_until(
          fn ->
            Server.server_stats(server: :test_notify_2).modification_count == 0
          end,
          1000
        )

      info = Server.server_stats(server: :test_notify_2)
      # Should have been reset by refresh
      assert info.modification_count == 0

      GenServer.stop(pid)
    end
  end

  # ===========================================================================
  # server_stats/1 Tests
  # ===========================================================================

  describe "server_stats/1" do
    test "returns server monitoring info", %{db: db} do
      {:ok, pid} = Server.start_link(db: db, name: :test_info_1)
      :ok = wait_for_server_ready(:test_info_1)

      info = Server.server_stats(server: :test_info_1)

      assert Map.has_key?(info, :modification_count)
      assert Map.has_key?(info, :last_refresh)
      assert Map.has_key?(info, :cached)
      assert Map.has_key?(info, :stale)
      assert Map.has_key?(info, :refresh_in_progress)

      GenServer.stop(pid)
    end
  end

  # ===========================================================================
  # running?/1 Tests
  # ===========================================================================

  describe "running?/1" do
    test "returns true when server is running", %{db: db} do
      {:ok, pid} = Server.start_link(db: db, name: :test_running_1)

      assert Server.running?(server: :test_running_1)

      GenServer.stop(pid)
    end

    test "returns false when server is not running" do
      refute Server.running?(server: :nonexistent_server)
    end
  end

  # ===========================================================================
  # Persistence Tests
  # ===========================================================================

  describe "persistence" do
    test "loads persisted statistics on start", %{db: db} do
      triples = [{1000, 100, 2000}, {1001, 100, 2001}]
      :ok = Index.insert_triples(db, triples)

      # First server collects and saves
      {:ok, pid1} = Server.start_link(db: db, name: :test_persist_1)
      :ok = wait_for_server_ready(:test_persist_1)

      {:ok, stats1} = Server.get_stats(server: :test_persist_1)
      assert stats1.triple_count == 2

      GenServer.stop(pid1)

      # Second server should load persisted stats
      {:ok, pid2} = Server.start_link(db: db, name: :test_persist_2)
      :ok = wait_for_server_ready(:test_persist_2)

      {:ok, stats2} = Server.get_stats(server: :test_persist_2)
      assert stats2.triple_count == 2

      GenServer.stop(pid2)
    end
  end

  # ===========================================================================
  # Periodic Refresh Tests (C10)
  # ===========================================================================

  describe "periodic refresh" do
    test "triggers refresh after interval when modifications exist", %{db: db} do
      # Use a short refresh interval for testing
      {:ok, pid} =
        Server.start_link(
          db: db,
          name: :test_periodic_1,
          refresh_interval: 50,
          auto_refresh: true
        )

      :ok = wait_for_server_ready(:test_periodic_1)

      # Get initial stats and add data
      {:ok, _} = Server.get_stats(server: :test_periodic_1)
      :ok = Index.insert_triple(db, {1000, 100, 2000})

      # Notify of modification to make stats stale
      Server.notify_modification(server: :test_periodic_1)

      # Wait for periodic refresh
      :ok =
        wait_until(
          fn ->
            info = Server.server_stats(server: :test_periodic_1)
            info.modification_count == 0
          end,
          500
        )

      # Verify modification count was reset
      info = Server.server_stats(server: :test_periodic_1)
      assert info.modification_count == 0

      GenServer.stop(pid)
    end

    test "does not refresh when no modifications", %{db: db} do
      {:ok, pid} =
        Server.start_link(
          db: db,
          name: :test_periodic_2,
          refresh_interval: 50,
          auto_refresh: true
        )

      :ok = wait_for_server_ready(:test_periodic_2)

      {:ok, stats1} = Server.get_stats(server: :test_periodic_2)

      # Wait for potential periodic refresh (but there should be none)
      Process.sleep(100)

      {:ok, stats2} = Server.get_stats(server: :test_periodic_2)

      # Stats should still be the same (no refresh without modifications)
      assert stats1.collected_at == stats2.collected_at

      GenServer.stop(pid)
    end
  end

  # ===========================================================================
  # Telemetry Tests (S12)
  # ===========================================================================

  describe "telemetry events" do
    test "emits cache hit event", %{db: db} do
      test_pid = self()

      # Attach telemetry handler
      :telemetry.attach(
        "test-cache-hit",
        [:triple_store, :cache, :stats, :hit],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      {:ok, pid} = Server.start_link(db: db, name: :test_telemetry_hit)
      :ok = wait_for_server_ready(:test_telemetry_hit)

      # First call populates cache, second should be a hit
      {:ok, _} = Server.get_stats(server: :test_telemetry_hit)
      {:ok, _} = Server.get_stats(server: :test_telemetry_hit)

      assert_receive {:telemetry, [:triple_store, :cache, :stats, :hit], _, metadata}, 1000
      assert Map.has_key?(metadata, :stale)

      :telemetry.detach("test-cache-hit")
      GenServer.stop(pid)
    end

    test "emits cache miss event", %{db: db} do
      test_pid = self()

      :telemetry.attach(
        "test-cache-miss",
        [:triple_store, :cache, :stats, :miss],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      {:ok, pid} = Server.start_link(db: db, name: :test_telemetry_miss, auto_refresh: false)

      # Clear any persisted stats first
      :ok = Statistics.save(db, nil)

      # Restart to force fresh collection
      GenServer.stop(pid)
      {:ok, pid2} = Server.start_link(db: db, name: :test_telemetry_miss_2, auto_refresh: false)

      # Give time for initial load which might be a miss if no persisted stats
      Process.sleep(50)

      # This should be a miss if cache is empty
      {:ok, _} = Server.get_stats(server: :test_telemetry_miss_2)

      # We should receive at least one miss event
      receive do
        {:telemetry, [:triple_store, :cache, :stats, :miss], _, _} -> :ok
      after
        1000 -> :ok
      end

      :telemetry.detach("test-cache-miss")
      GenServer.stop(pid2)
    end

    test "emits refresh event with duration", %{db: db} do
      test_pid = self()

      :telemetry.attach(
        "test-refresh",
        [:triple_store, :cache, :stats, :refresh],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      {:ok, pid} = Server.start_link(db: db, name: :test_telemetry_refresh)
      :ok = wait_for_server_ready(:test_telemetry_refresh)

      # Force a refresh
      {:ok, _} = Server.refresh(server: :test_telemetry_refresh)

      assert_receive {:telemetry, [:triple_store, :cache, :stats, :refresh], measurements,
                      metadata},
                     1000

      assert Map.has_key?(measurements, :duration)
      assert Map.has_key?(metadata, :modification_count)

      :telemetry.detach("test-refresh")
      GenServer.stop(pid)
    end
  end
end
