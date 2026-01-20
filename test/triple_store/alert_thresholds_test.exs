defmodule TripleStore.AlertThresholdsTest do
  @moduledoc """
  Tests for the AlertThresholds module.

  Verifies configurable alert thresholds for quad store monitoring,
  including defaults, getters, setters, and validation.
  """

  use ExUnit.Case, async: false

  alias TripleStore.AlertThresholds

  # Save and restore application config between tests
  setup do
    # Save original config
    original_config = Application.get_env(:triple_store, :alert_thresholds)

    # Clear config before each test for clean state
    Application.delete_env(:triple_store, :alert_thresholds)

    on_exit(fn ->
      # Restore original config (only if it was a list)
      if is_list(original_config) do
        Application.put_env(:triple_store, :alert_thresholds, original_config)
      else
        Application.delete_env(:triple_store, :alert_thresholds)
      end
    end)

    :ok
  end

  describe "defaults/0" do
    test "returns all default threshold values" do
      defaults = AlertThresholds.defaults()

      assert is_list(defaults)
      assert Keyword.keyword?(defaults)

      # Check graph size thresholds
      assert Keyword.has_key?(defaults, :graph_size_warning)
      assert Keyword.has_key?(defaults, :graph_size_critical)

      # Check query performance thresholds
      assert Keyword.has_key?(defaults, :slow_query_ms)
      assert Keyword.has_key?(defaults, :slow_cross_graph_ms)
      assert Keyword.has_key?(defaults, :max_cross_graphs)

      # Check graph enumeration thresholds
      assert Keyword.has_key?(defaults, :slow_graph_enumeration_ms)
      assert Keyword.has_key?(defaults, :max_graphs_to_list)

      # Check growth rate thresholds
      assert Keyword.has_key?(defaults, :rapid_growth_rate)
      assert Keyword.has_key?(defaults, :stale_hours)
    end

    test "default values are positive integers" do
      defaults = AlertThresholds.defaults()

      for {_key, value} <- defaults do
        assert is_integer(value) and value > 0,
               "Default threshold value #{value} should be a positive integer"
      end
    end

    test "critical threshold is higher than warning threshold" do
      defaults = AlertThresholds.defaults()

      assert defaults[:graph_size_critical] > defaults[:graph_size_warning]
    end
  end

  describe "get/1" do
    test "returns default value when no config is set" do
      # Setup already cleared config
      assert AlertThresholds.get(:slow_query_ms) == 1000
      assert AlertThresholds.get(:graph_size_warning) == 1_000_000
    end

    test "returns configured value when config is set" do
      Application.put_env(:triple_store, :alert_thresholds, slow_query_ms: 2000)

      assert AlertThresholds.get(:slow_query_ms) == 2000
    end

    test "returns default for unconfigured keys in partial config" do
      Application.put_env(:triple_store, :alert_thresholds, slow_query_ms: 500)

      assert AlertThresholds.get(:slow_query_ms) == 500
      assert AlertThresholds.get(:graph_size_warning) == 1_000_000
    end

    test "returns nil for unknown keys" do
      Application.delete_env(:triple_store, :alert_thresholds)

      assert is_nil(AlertThresholds.get(:unknown_key))
    end
  end

  describe "all/0" do
    test "returns defaults when no config is set" do
      Application.delete_env(:triple_store, :alert_thresholds)

      assert AlertThresholds.all() == AlertThresholds.defaults()
    end

    test "merges config with defaults (config takes precedence)" do
      Application.put_env(:triple_store, :alert_thresholds, slow_query_ms: 2000)

      all = AlertThresholds.all()

      assert all[:slow_query_ms] == 2000
      assert all[:graph_size_warning] == 1_000_000  # default
    end

    test "includes all configured and default values" do
      Application.put_env(:triple_store, :alert_thresholds, slow_query_ms: 500)

      all = AlertThresholds.all()

      # Should have more keys than just the configured one
      assert length(all) > 1

      # Configured value should be present
      assert all[:slow_query_ms] == 500
    end
  end

  describe "set/2" do
    test "sets a single threshold value" do
      Application.delete_env(:triple_store, :alert_thresholds)

      assert :ok = AlertThresholds.set(:slow_query_ms, 2000)
      assert AlertThresholds.get(:slow_query_ms) == 2000
    end

    test "preserves existing values when setting new value" do
      AlertThresholds.set(:slow_query_ms, 500)
      AlertThresholds.set(:graph_size_warning, 500_000)

      assert AlertThresholds.get(:slow_query_ms) == 500
      assert AlertThresholds.get(:graph_size_warning) == 500_000
      assert AlertThresholds.get(:graph_size_critical) == 10_000_000  # default
    end

    test "can override default value" do
      AlertThresholds.set(:stale_hours, 48)

      assert AlertThresholds.get(:stale_hours) == 48
    end
  end

  describe "graph_size_thresholds/0" do
    test "returns only graph size related thresholds" do
      thresholds = AlertThresholds.graph_size_thresholds()

      assert Keyword.keyword?(thresholds)
      assert Keyword.has_key?(thresholds, :graph_size_warning)
      assert Keyword.has_key?(thresholds, :graph_size_critical)

      # Should not have other threshold types
      refute Keyword.has_key?(thresholds, :slow_query_ms)
      refute Keyword.has_key?(thresholds, :stale_hours)
    end
  end

  describe "query_performance_thresholds/0" do
    test "returns only query performance related thresholds" do
      thresholds = AlertThresholds.query_performance_thresholds()

      assert Keyword.keyword?(thresholds)
      assert Keyword.has_key?(thresholds, :slow_query_ms)
      assert Keyword.has_key?(thresholds, :slow_cross_graph_ms)
      assert Keyword.has_key?(thresholds, :max_cross_graphs)

      # Should not have other threshold types
      refute Keyword.has_key?(thresholds, :graph_size_warning)
      refute Keyword.has_key?(thresholds, :stale_hours)
    end
  end

  describe "growth_rate_thresholds/0" do
    test "returns only growth rate related thresholds" do
      thresholds = AlertThresholds.growth_rate_thresholds()

      assert Keyword.keyword?(thresholds)
      assert Keyword.has_key?(thresholds, :rapid_growth_rate)
      assert Keyword.has_key?(thresholds, :stale_hours)

      # Should not have other threshold types
      refute Keyword.has_key?(thresholds, :graph_size_warning)
      refute Keyword.has_key?(thresholds, :slow_query_ms)
    end
  end

  describe "validate/0" do
    test "returns :ok for valid default thresholds" do
      Application.delete_env(:triple_store, :alert_thresholds)

      assert :ok = AlertThresholds.validate()
    end

    test "returns warnings for very low graph_size_warning" do
      Application.put_env(:triple_store, :alert_thresholds, graph_size_warning: 100)

      assert {:error, warnings} = AlertThresholds.validate()
      assert is_list(warnings)
      assert Enum.any?(warnings, fn msg ->
               String.contains?(msg, "graph_size_warning")
             end)
    end

    test "returns warning when critical is less than warning" do
      Application.put_env(:triple_store, :alert_thresholds,
        graph_size_warning: 1_000_000,
        graph_size_critical: 100_000
      )

      assert {:error, warnings} = AlertThresholds.validate()
      assert Enum.any?(warnings, fn msg ->
               String.contains?(msg, "graph_size_critical")
             end)
    end

    test "returns warning for very low slow_query_ms" do
      Application.put_env(:triple_store, :alert_thresholds, slow_query_ms: 5)

      assert {:error, warnings} = AlertThresholds.validate()
      assert Enum.any?(warnings, fn msg ->
               String.contains?(msg, "slow_query_ms")
             end)
    end

    test "returns :ok when thresholds are within valid ranges" do
      Application.put_env(:triple_store, :alert_thresholds,
        graph_size_warning: 100_000,
        graph_size_critical: 1_000_000,
        slow_query_ms: 100
      )

      assert :ok = AlertThresholds.validate()
    end

    test "returns multiple warnings for multiple invalid thresholds" do
      Application.put_env(:triple_store, :alert_thresholds,
        graph_size_warning: 500,  # too low
        slow_query_ms: 5          # too low
      )

      assert {:error, warnings} = AlertThresholds.validate()
      assert length(warnings) >= 2
    end
  end
end
