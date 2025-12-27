defmodule TripleStore.Reasoner.ReasoningModeTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.ReasoningMode

  # ============================================================================
  # Tests: Mode Names
  # ============================================================================

  describe "mode_names/0" do
    test "returns all valid mode names" do
      names = ReasoningMode.mode_names()
      assert :materialized in names
      assert :query_time in names
      assert :hybrid in names
      assert :none in names
    end
  end

  describe "valid_mode?/1" do
    test "returns true for valid modes" do
      assert ReasoningMode.valid_mode?(:materialized)
      assert ReasoningMode.valid_mode?(:query_time)
      assert ReasoningMode.valid_mode?(:hybrid)
      assert ReasoningMode.valid_mode?(:none)
    end

    test "returns false for invalid modes" do
      refute ReasoningMode.valid_mode?(:unknown)
      refute ReasoningMode.valid_mode?(:full)
      refute ReasoningMode.valid_mode?("materialized")
    end
  end

  # ============================================================================
  # Tests: Mode Info
  # ============================================================================

  describe "info/1" do
    test "returns info for none mode" do
      info = ReasoningMode.info(:none)
      assert info.name == :none
      assert info.query_complexity == "O(1)"
    end

    test "returns info for materialized mode" do
      info = ReasoningMode.info(:materialized)
      assert info.name == :materialized
      assert info.query_complexity == "O(1) lookup"
      assert String.contains?(info.best_for, "Read-heavy")
    end

    test "returns info for query_time mode" do
      info = ReasoningMode.info(:query_time)
      assert info.name == :query_time
      assert info.update_complexity == "O(1)"
      assert String.contains?(info.best_for, "Write-heavy")
    end

    test "returns info for hybrid mode" do
      info = ReasoningMode.info(:hybrid)
      assert info.name == :hybrid
      assert String.contains?(info.description, "Common inferences materialized")
    end
  end

  # ============================================================================
  # Tests: Default Config
  # ============================================================================

  describe "default_config/1" do
    test "returns default config for none mode" do
      config = ReasoningMode.default_config(:none)
      assert config.mode == :none
      assert config.parallel == false
      assert config.max_iterations == 0
    end

    test "returns default config for materialized mode" do
      config = ReasoningMode.default_config(:materialized)
      assert config.mode == :materialized
      assert config.parallel == false
      assert config.max_iterations == 1000
    end

    test "returns default config for query_time mode" do
      config = ReasoningMode.default_config(:query_time)
      assert config.mode == :query_time
      assert config.max_depth == 10
      assert config.cache_results == false
    end

    test "returns default config for hybrid mode" do
      config = ReasoningMode.default_config(:hybrid)
      assert config.mode == :hybrid
      assert is_list(config.materialized_rules)
      assert is_list(config.query_time_rules)
      assert :scm_sco in config.materialized_rules
    end
  end

  # ============================================================================
  # Tests: Validate Config
  # ============================================================================

  describe "validate_config/2" do
    test "validates materialized mode with defaults" do
      {:ok, config} = ReasoningMode.validate_config(:materialized)
      assert config.mode == :materialized
    end

    test "validates materialized mode with parallel option" do
      {:ok, config} = ReasoningMode.validate_config(:materialized, parallel: true)
      assert config.parallel == true
    end

    test "validates materialized mode with max_iterations" do
      {:ok, config} = ReasoningMode.validate_config(:materialized, max_iterations: 500)
      assert config.max_iterations == 500
    end

    test "validates query_time mode with max_depth" do
      {:ok, config} = ReasoningMode.validate_config(:query_time, max_depth: 20)
      assert config.max_depth == 20
    end

    test "validates query_time mode with cache_results" do
      {:ok, config} = ReasoningMode.validate_config(:query_time, cache_results: true)
      assert config.cache_results == true
    end

    test "validates hybrid mode with materialized_rules" do
      {:ok, config} = ReasoningMode.validate_config(:hybrid,
        materialized_rules: [:scm_sco, :prp_trp]
      )
      assert config.materialized_rules == [:scm_sco, :prp_trp]
    end

    test "returns error for max_iterations exceeding limit" do
      {:error, reason} = ReasoningMode.validate_config(:materialized, max_iterations: 200_000)
      assert {:invalid_option, :max_iterations, message} = reason
      assert message =~ "exceeds maximum"
    end

    test "returns error for max_depth exceeding limit" do
      {:error, reason} = ReasoningMode.validate_config(:query_time, max_depth: 2000)
      assert {:invalid_option, :max_depth, message} = reason
      assert message =~ "exceeds maximum"
    end

    test "returns error for invalid mode" do
      {:error, reason} = ReasoningMode.validate_config(:invalid)
      assert {:invalid_mode, :invalid, _} = reason
    end

    test "returns error for unknown rules in hybrid mode" do
      {:error, reason} = ReasoningMode.validate_config(:hybrid,
        materialized_rules: [:unknown_rule]
      )
      assert {:unknown_rules, :materialized_rules, [:unknown_rule]} = reason
    end

    test "returns error for unknown query_time_rules in hybrid mode" do
      {:error, reason} = ReasoningMode.validate_config(:hybrid,
        query_time_rules: [:unknown_rule]
      )
      assert {:unknown_rules, :query_time_rules, [:unknown_rule]} = reason
    end
  end

  describe "validate_config!/2" do
    test "returns config for valid mode" do
      config = ReasoningMode.validate_config!(:materialized)
      assert config.mode == :materialized
    end

    test "raises for invalid mode" do
      assert_raise ArgumentError, fn ->
        ReasoningMode.validate_config!(:invalid)
      end
    end
  end

  # ============================================================================
  # Tests: Suggest Mode
  # ============================================================================

  describe "suggest_mode/1" do
    test "suggests materialized for read-heavy workloads" do
      assert ReasoningMode.suggest_mode(read_heavy: true) == :materialized
    end

    test "suggests materialized for complex queries" do
      assert ReasoningMode.suggest_mode(complex_queries: true) == :materialized
    end

    test "suggests query_time for write-heavy memory-constrained" do
      assert ReasoningMode.suggest_mode(write_heavy: true, memory_constrained: true) == :query_time
    end

    test "suggests query_time for write-heavy simple queries" do
      assert ReasoningMode.suggest_mode(write_heavy: true, complex_queries: false) == :query_time
    end

    test "suggests hybrid for memory-constrained without write-heavy" do
      assert ReasoningMode.suggest_mode(memory_constrained: true) == :hybrid
    end

    test "suggests hybrid by default" do
      assert ReasoningMode.suggest_mode([]) == :hybrid
    end
  end

  # ============================================================================
  # Tests: Materialization Profile
  # ============================================================================

  describe "materialization_profile/2" do
    test "returns :none for none mode" do
      config = ReasoningMode.default_config(:none)
      assert ReasoningMode.materialization_profile(config, :owl2rl) == :none
    end

    test "returns profile for materialized mode" do
      config = ReasoningMode.default_config(:materialized)
      assert ReasoningMode.materialization_profile(config, :owl2rl) == :owl2rl
    end

    test "returns :none for query_time mode" do
      config = ReasoningMode.default_config(:query_time)
      assert ReasoningMode.materialization_profile(config, :owl2rl) == :none
    end

    test "returns custom rules for hybrid mode" do
      config = ReasoningMode.default_config(:hybrid)
      result = ReasoningMode.materialization_profile(config, :owl2rl)
      assert {:custom, rules} = result
      assert is_list(rules)
    end
  end

  # ============================================================================
  # Tests: Mode Capabilities
  # ============================================================================

  describe "requires_materialization?/1" do
    test "true for materialized mode" do
      config = ReasoningMode.default_config(:materialized)
      assert ReasoningMode.requires_materialization?(config)
    end

    test "true for hybrid mode" do
      config = ReasoningMode.default_config(:hybrid)
      assert ReasoningMode.requires_materialization?(config)
    end

    test "false for query_time mode" do
      config = ReasoningMode.default_config(:query_time)
      refute ReasoningMode.requires_materialization?(config)
    end

    test "false for none mode" do
      config = ReasoningMode.default_config(:none)
      refute ReasoningMode.requires_materialization?(config)
    end
  end

  describe "supports_incremental?/1" do
    test "true for materialized mode" do
      config = ReasoningMode.default_config(:materialized)
      assert ReasoningMode.supports_incremental?(config)
    end

    test "true for hybrid mode" do
      config = ReasoningMode.default_config(:hybrid)
      assert ReasoningMode.supports_incremental?(config)
    end

    test "false for query_time mode" do
      config = ReasoningMode.default_config(:query_time)
      refute ReasoningMode.supports_incremental?(config)
    end
  end

  describe "requires_backward_chaining?/1" do
    test "false for materialized mode" do
      config = ReasoningMode.default_config(:materialized)
      refute ReasoningMode.requires_backward_chaining?(config)
    end

    test "true for hybrid mode" do
      config = ReasoningMode.default_config(:hybrid)
      assert ReasoningMode.requires_backward_chaining?(config)
    end

    test "true for query_time mode" do
      config = ReasoningMode.default_config(:query_time)
      assert ReasoningMode.requires_backward_chaining?(config)
    end

    test "false for none mode" do
      config = ReasoningMode.default_config(:none)
      refute ReasoningMode.requires_backward_chaining?(config)
    end
  end
end
