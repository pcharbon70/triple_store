defmodule TripleStore.HealthTest do
  @moduledoc """
  Tests for the Health check module.

  Verifies liveness, readiness, and comprehensive health checks
  including index sizes, memory estimates, and component status.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Health
  alias TripleStore.Backend.RocksDB.NIF

  # Helper to create a mock store
  defp create_mock_store do
    # Create a temporary directory for the test database
    path = Path.join(System.tmp_dir!(), "health_test_#{:erlang.unique_integer([:positive])}")
    File.rm_rf!(path)

    case NIF.open(path) do
      {:ok, db} ->
        # Start a mock dict_manager
        {:ok, agent} = Agent.start_link(fn -> %{} end)

        store = %{
          db: db,
          dict_manager: agent,
          transaction: nil,
          path: path
        }

        {:ok, store, path}

      {:error, _} = error ->
        error
    end
  end

  defp cleanup_store(%{db: db, dict_manager: dict_manager, path: path}) do
    if is_pid(dict_manager) and Process.alive?(dict_manager) do
      Agent.stop(dict_manager)
    end

    NIF.close(db)
    File.rm_rf!(path)
  end

  describe "liveness/1" do
    test "returns :ok when database is open" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert :ok = Health.liveness(store)
          cleanup_store(store)

        {:error, _} ->
          # Skip if NIF not available
          :ok
      end
    end

    test "returns error when database is closed" do
      case create_mock_store() do
        {:ok, store, path} ->
          NIF.close(store.db)
          assert {:error, :database_closed} = Health.liveness(store)
          File.rm_rf!(path)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "readiness/1" do
    test "returns ready when all components are running" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, :ready} = Health.readiness(store)
          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "returns error when database is closed" do
      case create_mock_store() do
        {:ok, store, path} ->
          NIF.close(store.db)
          assert {:error, :database_closed} = Health.readiness(store)
          File.rm_rf!(path)

        {:error, _} ->
          :ok
      end
    end

    test "returns not_ready when dict_manager is not running" do
      case create_mock_store() do
        {:ok, store, path} ->
          Agent.stop(store.dict_manager)
          store = %{store | dict_manager: nil}
          assert {:ok, :not_ready} = Health.readiness(store)
          NIF.close(store.db)
          File.rm_rf!(path)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "health/2" do
    test "returns comprehensive health status" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, health} = Health.health(store)

          assert is_atom(health.status)
          assert is_boolean(health.database_open)
          assert is_boolean(health.dict_manager_alive)
          assert is_boolean(health.plan_cache_alive)
          assert is_boolean(health.query_cache_alive)
          assert is_boolean(health.metrics_alive)
          assert %DateTime{} = health.checked_at

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "includes triple count by default" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, health} = Health.health(store)
          assert Map.has_key?(health, :triple_count)
          assert is_integer(health.triple_count)

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "excludes triple count when include_stats: false" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, health} = Health.health(store, include_stats: false)
          refute Map.has_key?(health, :triple_count)

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "includes index sizes when include_indices: true" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, health} = Health.health(store, include_indices: true)
          assert Map.has_key?(health, :index_sizes)
          assert is_map(health.index_sizes)
          assert Map.has_key?(health.index_sizes, :spo)
          assert Map.has_key?(health.index_sizes, :pos)
          assert Map.has_key?(health.index_sizes, :osp)
          assert Map.has_key?(health.index_sizes, :derived)
          assert Map.has_key?(health.index_sizes, :dictionary)

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "includes memory estimates when include_memory: true" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, health} = Health.health(store, include_memory: true)
          assert Map.has_key?(health, :memory)
          assert is_map(health.memory)
          assert Map.has_key?(health.memory, :beam_mb)
          assert Map.has_key?(health.memory, :estimated_data_mb)
          assert Map.has_key?(health.memory, :estimated_total_mb)

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "includes compaction status when include_compaction: true" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, health} = Health.health(store, include_compaction: true)
          assert Map.has_key?(health, :compaction)
          assert is_map(health.compaction)
          assert Map.has_key?(health.compaction, :running)
          assert Map.has_key?(health.compaction, :pending_bytes)
          assert Map.has_key?(health.compaction, :pending_compactions)

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "includes all optional metrics when include_all: true" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, health} = Health.health(store, include_all: true)

          assert Map.has_key?(health, :triple_count)
          assert Map.has_key?(health, :index_sizes)
          assert Map.has_key?(health, :memory)
          assert Map.has_key?(health, :compaction)

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "returns degraded status when optional components are not running" do
      case create_mock_store() do
        {:ok, store, _path} ->
          # Plan cache, query cache, and metrics are not started
          # So status should be degraded
          assert {:ok, health} = Health.health(store)
          assert health.status == :degraded

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "get_index_sizes/1" do
    test "returns index entry counts" do
      case create_mock_store() do
        {:ok, store, _path} ->
          sizes = Health.get_index_sizes(store.db)

          assert is_map(sizes)
          assert is_integer(sizes.spo)
          assert is_integer(sizes.pos)
          assert is_integer(sizes.osp)
          assert is_integer(sizes.derived)
          assert is_integer(sizes.dictionary)

          # Empty database should have 0 entries
          assert sizes.spo == 0
          assert sizes.pos == 0

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "estimate_memory/1" do
    test "returns memory estimates" do
      memory = Health.estimate_memory(10_000)

      assert is_map(memory)
      assert is_float(memory.beam_mb)
      assert is_float(memory.estimated_data_mb)
      assert is_float(memory.estimated_total_mb)

      # BEAM memory should be positive
      assert memory.beam_mb > 0

      # 10k triples * 100 bytes = ~1 MB
      assert memory.estimated_data_mb > 0
    end

    test "handles zero triples" do
      memory = Health.estimate_memory(0)

      assert memory.estimated_data_mb == 0.0
      assert memory.beam_mb > 0
    end
  end

  describe "get_compaction_status/0" do
    test "returns compaction status" do
      status = Health.get_compaction_status()

      assert is_map(status)
      assert is_boolean(status.running)
      assert is_integer(status.pending_bytes)
      assert is_integer(status.pending_compactions)
    end
  end

  describe "component_status/2" do
    test "returns database status" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, :running} = Health.component_status(:database, store)
          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "returns dict_manager status" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, :running} = Health.component_status(:dict_manager, store)
          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "returns plan_cache status" do
      case create_mock_store() do
        {:ok, store, _path} ->
          # Plan cache may or may not be running depending on application state
          assert {:ok, status} = Health.component_status(:plan_cache, store)
          assert status in [:running, :stopped]
          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "returns query_cache status" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, :stopped} = Health.component_status(:query_cache, store)
          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "returns metrics status" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, :stopped} = Health.component_status(:metrics, store)
          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "returns error for unknown component" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:error, {:unknown_component, :unknown}} =
                   Health.component_status(:unknown, store)

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "plan_cache_running?/0" do
    test "returns boolean indicating plan cache status" do
      # Plan cache may or may not be running depending on application state
      assert is_boolean(Health.plan_cache_running?())
    end
  end

  describe "query_cache_running?/0" do
    test "returns false when query cache is not started" do
      refute Health.query_cache_running?()
    end
  end

  describe "metrics_running?/0" do
    test "returns false when metrics is not started" do
      refute Health.metrics_running?()
    end

    test "returns true when metrics is started" do
      name = :"metrics_health_test_#{:erlang.unique_integer([:positive])}"
      {:ok, pid} = TripleStore.Metrics.start_link(name: name)

      # Our check uses the default name, so we need to register it
      Process.unregister(name)
      Process.register(pid, TripleStore.Metrics)

      assert Health.metrics_running?()

      Process.unregister(TripleStore.Metrics)
      GenServer.stop(pid)
    end
  end

  describe "get_metrics/0" do
    test "returns error when metrics collector is not running" do
      assert {:error, :not_running} = Health.get_metrics()
    end

    test "returns metrics when collector is running" do
      name = :"metrics_health_test2_#{:erlang.unique_integer([:positive])}"
      {:ok, pid} = TripleStore.Metrics.start_link(name: name)

      Process.unregister(name)
      Process.register(pid, TripleStore.Metrics)

      assert {:ok, metrics} = Health.get_metrics()
      assert is_map(metrics)
      assert Map.has_key?(metrics, :query)
      assert Map.has_key?(metrics, :cache)
      assert Map.has_key?(metrics, :throughput)
      assert Map.has_key?(metrics, :reasoning)

      Process.unregister(TripleStore.Metrics)
      GenServer.stop(pid)
    end
  end

  describe "summary/2" do
    test "returns JSON-serializable summary" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, summary} = Health.summary(store)

          assert is_binary(summary.status)
          assert is_boolean(summary.database_open)
          assert is_map(summary.components)
          assert is_binary(summary.checked_at)

          # Should be JSON serializable
          assert {:ok, _json} = Jason.encode(summary)

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end

    test "includes optional metrics when requested" do
      case create_mock_store() do
        {:ok, store, _path} ->
          assert {:ok, summary} = Health.summary(store, include_all: true)

          assert Map.has_key?(summary, :triple_count)
          assert Map.has_key?(summary, :index_sizes)
          assert Map.has_key?(summary, :memory)
          assert Map.has_key?(summary, :compaction)

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end
  end

  describe "estimate_data_size/1" do
    test "returns estimated data size in bytes" do
      case create_mock_store() do
        {:ok, store, _path} ->
          size = Health.estimate_data_size(store.db)

          assert is_integer(size)
          # Empty database should have minimal size
          assert size >= 0

          cleanup_store(store)

        {:error, _} ->
          :ok
      end
    end
  end
end
