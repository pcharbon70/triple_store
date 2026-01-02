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
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.Statistics
  alias TripleStore.Statistics.Server

  @test_db_base "/tmp/triple_store_stats_server_test"

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
          auto_refresh: false
        )

      assert Process.alive?(pid)
      GenServer.stop(pid)
    end
  end

  # ===========================================================================
  # get_stats/1 Tests
  # ===========================================================================

  describe "get_stats/1" do
    test "returns statistics for empty store", %{db: db} do
      {:ok, pid} = Server.start_link(db: db, name: :test_get_stats_1)

      # Wait for initial load
      Process.sleep(100)

      {:ok, stats} = Server.get_stats(server: :test_get_stats_1)
      assert stats.triple_count == 0

      GenServer.stop(pid)
    end

    test "returns statistics with data", %{db: db} do
      triples = [{1000, 100, 2000}, {1001, 100, 2001}]
      :ok = Index.insert_triples(db, triples)

      {:ok, pid} = Server.start_link(db: db, name: :test_get_stats_2)
      Process.sleep(100)

      {:ok, stats} = Server.get_stats(server: :test_get_stats_2)
      assert stats.triple_count == 2

      GenServer.stop(pid)
    end

    test "caches statistics", %{db: db} do
      triples = [{1000, 100, 2000}]
      :ok = Index.insert_triples(db, triples)

      {:ok, pid} = Server.start_link(db: db, name: :test_get_stats_3)
      Process.sleep(100)

      # Get stats twice
      {:ok, stats1} = Server.get_stats(server: :test_get_stats_3)
      {:ok, stats2} = Server.get_stats(server: :test_get_stats_3)

      # Should be the same (cached)
      assert stats1.collected_at == stats2.collected_at

      GenServer.stop(pid)
    end
  end

  # ===========================================================================
  # refresh/1 Tests
  # ===========================================================================

  describe "refresh/1" do
    test "forces statistics refresh", %{db: db} do
      {:ok, pid} = Server.start_link(db: db, name: :test_refresh_1)
      Process.sleep(100)

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
      Process.sleep(100)

      info1 = Server.server_stats(server: :test_notify_1)
      assert info1.modification_count == 0

      Server.notify_modification(server: :test_notify_1)
      Server.notify_modification(server: :test_notify_1, count: 5)

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

      Process.sleep(100)

      # Get initial stats
      {:ok, _} = Server.get_stats(server: :test_notify_2)

      # Add more data
      :ok = Index.insert_triple(db, {1002, 100, 2002})

      # Notify modifications to exceed threshold
      Server.notify_modification(server: :test_notify_2, count: 6)

      # Wait for background refresh
      Process.sleep(200)

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
      Process.sleep(100)

      info = Server.server_stats(server: :test_info_1)

      assert Map.has_key?(info, :modification_count)
      assert Map.has_key?(info, :last_refresh)
      assert Map.has_key?(info, :cached)
      assert Map.has_key?(info, :stale)

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
      Process.sleep(100)

      {:ok, stats1} = Server.get_stats(server: :test_persist_1)
      assert stats1.triple_count == 2

      GenServer.stop(pid1)

      # Second server should load persisted stats
      {:ok, pid2} = Server.start_link(db: db, name: :test_persist_2)
      Process.sleep(100)

      {:ok, stats2} = Server.get_stats(server: :test_persist_2)
      assert stats2.triple_count == 2

      GenServer.stop(pid2)
    end
  end
end
