defmodule TripleStore.Reasoner.ReasoningStatusTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.{ReasoningStatus, ReasoningConfig}

  # ============================================================================
  # Test Helpers
  # ============================================================================

  defp sample_config do
    ReasoningConfig.preset(:full_materialization)
  end

  defp sample_stats do
    %{
      derived_count: 1500,
      iterations: 5,
      duration_ms: 250,
      rules_applied: 23
    }
  end

  # ============================================================================
  # Tests: Creation
  # ============================================================================

  describe "new/1" do
    test "creates status with nil config" do
      {:ok, status} = ReasoningStatus.new()
      assert status.config == nil
      assert status.derived_count == 0
      assert status.state == :initialized
    end

    test "creates status with config" do
      config = sample_config()
      {:ok, status} = ReasoningStatus.new(config)
      assert status.config == config
    end

    test "sets timestamps" do
      {:ok, status} = ReasoningStatus.new()
      assert %DateTime{} = status.created_at
      assert %DateTime{} = status.updated_at
    end

    test "initializes counts to zero" do
      {:ok, status} = ReasoningStatus.new()
      assert status.derived_count == 0
      assert status.explicit_count == 0
      assert status.materialization_count == 0
    end
  end

  describe "new!/1" do
    test "returns status" do
      status = ReasoningStatus.new!()
      assert %ReasoningStatus{} = status
    end
  end

  # ============================================================================
  # Tests: Recording Materialization
  # ============================================================================

  describe "record_materialization/2" do
    test "updates derived count" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_materialization(status, sample_stats())

      assert status.derived_count == 1500
    end

    test "sets last materialization time" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_materialization(status, sample_stats())

      assert %DateTime{} = status.last_materialization
    end

    test "increments materialization count" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_materialization(status, sample_stats())
      status = ReasoningStatus.record_materialization(status, sample_stats())

      assert status.materialization_count == 2
    end

    test "stores stats" do
      {:ok, status} = ReasoningStatus.new()
      stats = sample_stats()
      status = ReasoningStatus.record_materialization(status, stats)

      assert status.last_materialization_stats == stats
    end

    test "sets state to materialized" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_materialization(status, sample_stats())

      assert status.state == :materialized
    end

    test "clears previous error" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_error(status, :some_error)
      status = ReasoningStatus.record_materialization(status, sample_stats())

      assert status.error == nil
      assert status.state == :materialized
    end
  end

  # ============================================================================
  # Tests: Count Updates
  # ============================================================================

  describe "update_explicit_count/2" do
    test "updates explicit count" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.update_explicit_count(status, 5000)

      assert status.explicit_count == 5000
    end

    test "updates timestamp" do
      {:ok, status} = ReasoningStatus.new()
      original = status.updated_at
      Process.sleep(1)
      status = ReasoningStatus.update_explicit_count(status, 5000)

      assert DateTime.compare(status.updated_at, original) in [:gt, :eq]
    end
  end

  describe "update_derived_count/2" do
    test "updates derived count" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.update_derived_count(status, 1500)

      assert status.derived_count == 1500
    end
  end

  # ============================================================================
  # Tests: State Changes
  # ============================================================================

  describe "mark_stale/1" do
    test "sets state to stale" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_materialization(status, sample_stats())
      status = ReasoningStatus.mark_stale(status)

      assert status.state == :stale
    end
  end

  describe "record_error/2" do
    test "sets state to error" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_error(status, {:max_iterations, 1000})

      assert status.state == :error
    end

    test "stores error" do
      {:ok, status} = ReasoningStatus.new()
      error = {:max_iterations, 1000}
      status = ReasoningStatus.record_error(status, error)

      assert status.error == error
    end
  end

  describe "update_config/2" do
    test "updates config" do
      {:ok, status} = ReasoningStatus.new()
      config = sample_config()
      status = ReasoningStatus.update_config(status, config)

      assert status.config == config
    end
  end

  # ============================================================================
  # Tests: Status Queries
  # ============================================================================

  describe "summary/1" do
    test "returns complete summary" do
      config = sample_config()
      {:ok, status} = ReasoningStatus.new(config)
      status = ReasoningStatus.update_explicit_count(status, 5000)
      status = ReasoningStatus.record_materialization(status, sample_stats())

      summary = ReasoningStatus.summary(status)

      assert summary.state == :materialized
      assert summary.profile == :owl2rl
      assert summary.mode == :materialized
      assert summary.derived_count == 1500
      assert summary.explicit_count == 5000
      assert summary.total_count == 6500
      assert summary.materialization_count == 1
    end

    test "handles nil config" do
      {:ok, status} = ReasoningStatus.new()
      summary = ReasoningStatus.summary(status)

      assert summary.profile == nil
      assert summary.mode == nil
    end
  end

  describe "profile/1" do
    test "returns profile from config" do
      config = sample_config()
      {:ok, status} = ReasoningStatus.new(config)

      assert ReasoningStatus.profile(status) == :owl2rl
    end

    test "returns nil without config" do
      {:ok, status} = ReasoningStatus.new()

      assert ReasoningStatus.profile(status) == nil
    end
  end

  describe "mode/1" do
    test "returns mode from config" do
      config = sample_config()
      {:ok, status} = ReasoningStatus.new(config)

      assert ReasoningStatus.mode(status) == :materialized
    end

    test "returns nil without config" do
      {:ok, status} = ReasoningStatus.new()

      assert ReasoningStatus.mode(status) == nil
    end
  end

  describe "derived_count/1" do
    test "returns derived count" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.update_derived_count(status, 1500)

      assert ReasoningStatus.derived_count(status) == 1500
    end
  end

  describe "explicit_count/1" do
    test "returns explicit count" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.update_explicit_count(status, 5000)

      assert ReasoningStatus.explicit_count(status) == 5000
    end
  end

  describe "total_count/1" do
    test "returns sum of explicit and derived" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.update_explicit_count(status, 5000)
      status = ReasoningStatus.update_derived_count(status, 1500)

      assert ReasoningStatus.total_count(status) == 6500
    end
  end

  describe "last_materialization/1" do
    test "returns nil before materialization" do
      {:ok, status} = ReasoningStatus.new()

      assert ReasoningStatus.last_materialization(status) == nil
    end

    test "returns time after materialization" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_materialization(status, sample_stats())

      assert %DateTime{} = ReasoningStatus.last_materialization(status)
    end
  end

  describe "last_materialization_stats/1" do
    test "returns nil before materialization" do
      {:ok, status} = ReasoningStatus.new()

      assert ReasoningStatus.last_materialization_stats(status) == nil
    end

    test "returns stats after materialization" do
      {:ok, status} = ReasoningStatus.new()
      stats = sample_stats()
      status = ReasoningStatus.record_materialization(status, stats)

      assert ReasoningStatus.last_materialization_stats(status) == stats
    end
  end

  describe "state/1" do
    test "returns current state" do
      {:ok, status} = ReasoningStatus.new()

      assert ReasoningStatus.state(status) == :initialized
    end
  end

  describe "needs_rematerialization?/1" do
    test "true when stale" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.mark_stale(status)

      assert ReasoningStatus.needs_rematerialization?(status)
    end

    test "true when initialized with materializing mode" do
      config = ReasoningConfig.preset(:full_materialization)
      {:ok, status} = ReasoningStatus.new(config)

      assert ReasoningStatus.needs_rematerialization?(status)
    end

    test "false when initialized with query_time mode" do
      config = ReasoningConfig.preset(:minimal_memory)
      {:ok, status} = ReasoningStatus.new(config)

      refute ReasoningStatus.needs_rematerialization?(status)
    end

    test "false when materialized" do
      config = sample_config()
      {:ok, status} = ReasoningStatus.new(config)
      status = ReasoningStatus.record_materialization(status, sample_stats())

      refute ReasoningStatus.needs_rematerialization?(status)
    end
  end

  describe "error?/1" do
    test "false when no error" do
      {:ok, status} = ReasoningStatus.new()

      refute ReasoningStatus.error?(status)
    end

    test "true when error recorded" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_error(status, :some_error)

      assert ReasoningStatus.error?(status)
    end
  end

  describe "error/1" do
    test "returns nil when no error" do
      {:ok, status} = ReasoningStatus.new()

      assert ReasoningStatus.error(status) == nil
    end

    test "returns error when present" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_error(status, {:some_error, "details"})

      assert ReasoningStatus.error(status) == {:some_error, "details"}
    end
  end

  describe "time_since_materialization/1" do
    test "returns nil before materialization" do
      {:ok, status} = ReasoningStatus.new()

      assert ReasoningStatus.time_since_materialization(status) == nil
    end

    test "returns seconds since materialization" do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_materialization(status, sample_stats())

      seconds = ReasoningStatus.time_since_materialization(status)
      assert is_integer(seconds)
      assert seconds >= 0
    end
  end

  # ============================================================================
  # Tests: Persistent Term Storage
  # ============================================================================

  describe "store/2 and load/1" do
    setup do
      key = :"test_status_#{System.unique_integer([:positive])}"
      on_exit(fn -> ReasoningStatus.remove(key) end)
      {:ok, key: key}
    end

    test "stores and loads status", %{key: key} do
      {:ok, status} = ReasoningStatus.new()
      status = ReasoningStatus.record_materialization(status, sample_stats())

      :ok = ReasoningStatus.store(status, key)
      {:ok, loaded} = ReasoningStatus.load(key)

      assert loaded.derived_count == status.derived_count
      assert loaded.state == status.state
    end

    test "load returns error for unknown key" do
      assert {:error, :not_found} = ReasoningStatus.load(:unknown_key)
    end
  end

  describe "exists?/1" do
    setup do
      key = :"test_exists_#{System.unique_integer([:positive])}"
      on_exit(fn -> ReasoningStatus.remove(key) end)
      {:ok, key: key}
    end

    test "returns false for unknown key", %{key: key} do
      refute ReasoningStatus.exists?(key)
    end

    test "returns true after storing", %{key: key} do
      {:ok, status} = ReasoningStatus.new()
      ReasoningStatus.store(status, key)

      assert ReasoningStatus.exists?(key)
    end
  end

  describe "remove/1" do
    test "removes stored status" do
      key = :"test_remove_#{System.unique_integer([:positive])}"
      {:ok, status} = ReasoningStatus.new()
      ReasoningStatus.store(status, key)

      assert ReasoningStatus.exists?(key)
      ReasoningStatus.remove(key)
      refute ReasoningStatus.exists?(key)
    end
  end

  describe "list_stored/0" do
    test "returns stored keys" do
      # Clear first
      ReasoningStatus.clear_all()

      key1 = :"test_list_1_#{System.unique_integer([:positive])}"
      key2 = :"test_list_2_#{System.unique_integer([:positive])}"

      {:ok, status} = ReasoningStatus.new()
      ReasoningStatus.store(status, key1)
      ReasoningStatus.store(status, key2)

      stored = ReasoningStatus.list_stored()
      assert key1 in stored
      assert key2 in stored

      # Cleanup
      ReasoningStatus.clear_all()
    end
  end

  describe "clear_all/0" do
    test "removes all stored statuses" do
      key1 = :"test_clear_1_#{System.unique_integer([:positive])}"
      key2 = :"test_clear_2_#{System.unique_integer([:positive])}"

      {:ok, status} = ReasoningStatus.new()
      ReasoningStatus.store(status, key1)
      ReasoningStatus.store(status, key2)

      ReasoningStatus.clear_all()

      refute ReasoningStatus.exists?(key1)
      refute ReasoningStatus.exists?(key2)
    end
  end
end
