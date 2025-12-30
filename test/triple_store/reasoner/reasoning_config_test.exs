defmodule TripleStore.Reasoner.ReasoningConfigTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.ReasoningConfig

  # ============================================================================
  # Tests: New Configuration
  # ============================================================================

  describe "new/1" do
    test "creates config with defaults" do
      {:ok, config} = ReasoningConfig.new()
      assert config.profile == :owl2rl
      assert config.mode == :materialized
    end

    test "creates config with specified profile" do
      {:ok, config} = ReasoningConfig.new(profile: :rdfs)
      assert config.profile == :rdfs
    end

    test "creates config with specified mode" do
      {:ok, config} = ReasoningConfig.new(mode: :query_time)
      assert config.mode == :query_time
    end

    test "creates config with mode options" do
      {:ok, config} =
        ReasoningConfig.new(
          mode: :materialized,
          parallel: true,
          max_iterations: 500
        )

      assert config.mode_config.parallel == true
      assert config.mode_config.max_iterations == 500
    end

    test "creates config with profile options" do
      {:ok, config} =
        ReasoningConfig.new(
          profile: :owl2rl,
          exclude: [:eq_ref]
        )

      assert config.profile_opts[:exclude] == [:eq_ref]
    end

    test "creates hybrid config with custom materialized rules" do
      {:ok, config} =
        ReasoningConfig.new(
          mode: :hybrid,
          materialized_rules: [:scm_sco, :cax_sco]
        )

      assert config.mode_config.materialized_rules == [:scm_sco, :cax_sco]
    end

    test "returns error for invalid profile" do
      {:error, reason} = ReasoningConfig.new(profile: :invalid)
      assert {:invalid_profile, :invalid, _} = reason
    end

    test "returns error for invalid mode" do
      {:error, reason} = ReasoningConfig.new(mode: :invalid)
      assert {:invalid_mode, :invalid, _} = reason
    end

    test "sets created_at timestamp" do
      {:ok, config} = ReasoningConfig.new()
      assert %DateTime{} = config.created_at
    end
  end

  describe "new!/1" do
    test "returns config for valid options" do
      config = ReasoningConfig.new!(profile: :rdfs)
      assert config.profile == :rdfs
    end

    test "raises for invalid options" do
      assert_raise ArgumentError, fn ->
        ReasoningConfig.new!(profile: :invalid)
      end
    end
  end

  # ============================================================================
  # Tests: Presets
  # ============================================================================

  describe "preset/1" do
    test "returns full_materialization preset" do
      config = ReasoningConfig.preset(:full_materialization)
      assert config.profile == :owl2rl
      assert config.mode == :materialized
    end

    test "returns rdfs_only preset" do
      config = ReasoningConfig.preset(:rdfs_only)
      assert config.profile == :rdfs
      assert config.mode == :materialized
    end

    test "returns minimal_memory preset" do
      config = ReasoningConfig.preset(:minimal_memory)
      assert config.profile == :owl2rl
      assert config.mode == :query_time
    end

    test "returns balanced preset" do
      config = ReasoningConfig.preset(:balanced)
      assert config.profile == :owl2rl
      assert config.mode == :hybrid
    end

    test "returns none preset" do
      config = ReasoningConfig.preset(:none)
      assert config.profile == :none
      assert config.mode == :none
    end
  end

  describe "preset_names/0" do
    test "returns all preset names" do
      names = ReasoningConfig.preset_names()
      assert :full_materialization in names
      assert :rdfs_only in names
      assert :minimal_memory in names
      assert :balanced in names
      assert :none in names
    end
  end

  # ============================================================================
  # Tests: Materialization Rules
  # ============================================================================

  describe "materialization_rules/1" do
    test "returns empty list for none mode" do
      config = ReasoningConfig.preset(:none)
      assert ReasoningConfig.materialization_rules(config) == []
    end

    test "returns empty list for query_time mode" do
      config = ReasoningConfig.preset(:minimal_memory)
      assert ReasoningConfig.materialization_rules(config) == []
    end

    test "returns profile rules for materialized mode" do
      config = ReasoningConfig.preset(:full_materialization)
      rules = ReasoningConfig.materialization_rules(config)
      assert is_list(rules)
      assert :scm_sco in rules
      assert :prp_trp in rules
    end

    test "returns RDFS rules for rdfs_only preset" do
      config = ReasoningConfig.preset(:rdfs_only)
      rules = ReasoningConfig.materialization_rules(config)
      assert :scm_sco in rules
      assert :prp_dom in rules
      refute :prp_trp in rules
    end

    test "returns materialized rules for hybrid mode" do
      {:ok, config} =
        ReasoningConfig.new(
          mode: :hybrid,
          materialized_rules: [:scm_sco, :cax_sco]
        )

      rules = ReasoningConfig.materialization_rules(config)
      assert rules == [:scm_sco, :cax_sco]
    end

    test "returns default RDFS rules for hybrid mode without custom rules" do
      {:ok, config} = ReasoningConfig.new(mode: :hybrid)
      # Override mode_config to have nil materialized_rules
      config = %{config | mode_config: %{config.mode_config | materialized_rules: nil}}
      rules = ReasoningConfig.materialization_rules(config)
      assert :scm_sco in rules
    end
  end

  # ============================================================================
  # Tests: Query-Time Rules
  # ============================================================================

  describe "query_time_rules/1" do
    test "returns empty list for none mode" do
      config = ReasoningConfig.preset(:none)
      assert ReasoningConfig.query_time_rules(config) == []
    end

    test "returns empty list for materialized mode" do
      config = ReasoningConfig.preset(:full_materialization)
      assert ReasoningConfig.query_time_rules(config) == []
    end

    test "returns profile rules for query_time mode" do
      config = ReasoningConfig.preset(:minimal_memory)
      rules = ReasoningConfig.query_time_rules(config)
      assert is_list(rules)
      assert :scm_sco in rules
    end

    test "returns query_time_rules for hybrid mode" do
      {:ok, config} =
        ReasoningConfig.new(
          mode: :hybrid,
          query_time_rules: [:prp_trp, :eq_sym]
        )

      rules = ReasoningConfig.query_time_rules(config)
      assert rules == [:prp_trp, :eq_sym]
    end
  end

  # ============================================================================
  # Tests: Capability Queries
  # ============================================================================

  describe "requires_materialization?/1" do
    test "true for materialized mode" do
      config = ReasoningConfig.preset(:full_materialization)
      assert ReasoningConfig.requires_materialization?(config)
    end

    test "true for hybrid mode" do
      config = ReasoningConfig.preset(:balanced)
      assert ReasoningConfig.requires_materialization?(config)
    end

    test "false for query_time mode" do
      config = ReasoningConfig.preset(:minimal_memory)
      refute ReasoningConfig.requires_materialization?(config)
    end
  end

  describe "supports_incremental?/1" do
    test "true for materialized mode" do
      config = ReasoningConfig.preset(:full_materialization)
      assert ReasoningConfig.supports_incremental?(config)
    end

    test "false for query_time mode" do
      config = ReasoningConfig.preset(:minimal_memory)
      refute ReasoningConfig.supports_incremental?(config)
    end
  end

  describe "requires_backward_chaining?/1" do
    test "false for materialized mode" do
      config = ReasoningConfig.preset(:full_materialization)
      refute ReasoningConfig.requires_backward_chaining?(config)
    end

    test "true for query_time mode" do
      config = ReasoningConfig.preset(:minimal_memory)
      assert ReasoningConfig.requires_backward_chaining?(config)
    end

    test "true for hybrid mode" do
      config = ReasoningConfig.preset(:balanced)
      assert ReasoningConfig.requires_backward_chaining?(config)
    end
  end

  # ============================================================================
  # Tests: Summary
  # ============================================================================

  describe "summary/1" do
    test "returns summary for config" do
      config = ReasoningConfig.preset(:full_materialization)
      summary = ReasoningConfig.summary(config)

      assert summary.profile == :owl2rl
      assert summary.mode == :materialized
      assert is_list(summary.materialization_rules)
      assert summary.requires_materialization == true
      assert summary.requires_backward_chaining == false
    end

    test "summary includes parallel setting" do
      {:ok, config} = ReasoningConfig.new(parallel: true)
      summary = ReasoningConfig.summary(config)

      assert summary.parallel == true
    end
  end

  # ============================================================================
  # Tests: Complex Configurations
  # ============================================================================

  describe "complex configurations" do
    test "hybrid with custom rules and exclusions" do
      {:ok, config} =
        ReasoningConfig.new(
          profile: :owl2rl,
          mode: :hybrid,
          materialized_rules: [:scm_sco, :cax_sco, :prp_dom],
          query_time_rules: [:prp_trp, :prp_symp],
          cache_results: true
        )

      assert config.mode_config.cache_results == true
      assert ReasoningConfig.materialization_rules(config) == [:scm_sco, :cax_sco, :prp_dom]
      assert ReasoningConfig.query_time_rules(config) == [:prp_trp, :prp_symp]
    end

    test "materialized with parallel and custom iterations" do
      {:ok, config} =
        ReasoningConfig.new(
          profile: :owl2rl,
          mode: :materialized,
          parallel: true,
          max_iterations: 2000
        )

      assert config.mode_config.parallel == true
      assert config.mode_config.max_iterations == 2000
    end

    test "query_time with custom depth and caching" do
      {:ok, config} =
        ReasoningConfig.new(
          profile: :rdfs,
          mode: :query_time,
          max_depth: 20,
          cache_results: true
        )

      assert config.mode_config.max_depth == 20
      assert config.mode_config.cache_results == true
    end
  end
end
