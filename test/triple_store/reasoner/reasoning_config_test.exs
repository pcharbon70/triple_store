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

  # ============================================================================
  # Tests: Graph-Aware Reasoning Scope Options (Section 7.1)
  # ============================================================================

  describe "graph-aware reasoning scope" do
    test "creates config with local scope (default)" do
      {:ok, config} = ReasoningConfig.new()
      assert config.scope == :local
    end

    test "creates config with global scope" do
      {:ok, config} = ReasoningConfig.new(scope: :global)
      assert config.scope == :global
    end

    test "creates config with hybrid scope" do
      {:ok, config} = ReasoningConfig.new(scope: :hybrid)
      assert config.scope == :hybrid
    end

    test "returns error for invalid scope" do
      {:error, reason} = ReasoningConfig.new(scope: :invalid)
      assert reason == :invalid_scope
    end

    test "creates config with tbox_graph option" do
      {:ok, config} = ReasoningConfig.new(scope: :global, tbox_graph: 0)
      assert config.tbox_graph == 0
    end

    test "creates config with inferred_graph option" do
      {:ok, config} = ReasoningConfig.new(scope: :global, inferred_graph: 99)
      assert config.inferred_graph == 99
    end

    test "creates config with graph_configs map" do
      {:ok, config} =
        ReasoningConfig.new(
          scope: :hybrid,
          graph_configs: %{
            1 => [scope: :local],
            2 => [scope: :global]
          }
        )

      assert config.graph_configs[1][:scope] == :local
      assert config.graph_configs[2][:scope] == :global
    end

    test "accepts empty graph_configs" do
      {:ok, config} = ReasoningConfig.new(scope: :hybrid, graph_configs: %{})
      assert config.graph_configs == %{}
    end
  end

  # ============================================================================
  # Tests: Persistent Term Storage (Section 7.7.1.5)
  # ============================================================================

  describe "persistent_term storage" do
    # These tests cannot be async because they share persistent_term storage
    # and need sequential execution for correct cleanup

    setup do
      # Clean up all entries from previous test runs
      ReasoningConfig.clear_all()

      on_exit(fn ->
        ReasoningConfig.clear_all()
      end)

      :ok
    end

    test "stores configuration in persistent_term" do
      key = :test_store_key
      config = ReasoningConfig.preset(:full_materialization)
      assert :ok = ReasoningConfig.store(config, key)
      assert ReasoningConfig.stored?(key)
    end

    test "loads stored configuration from persistent_term" do
      key = :test_load_key
      original = ReasoningConfig.preset(:rdfs_only)
      :ok = ReasoningConfig.store(original, key)

      assert {:ok, loaded} = ReasoningConfig.load(key)
      assert loaded.profile == original.profile
      assert loaded.mode == original.mode
    end

    test "returns error for non-existent configuration" do
      assert {:error, :not_found} = ReasoningConfig.load(:non_existent_key)
    end

    test "load! returns default for non-existent configuration" do
      loaded = ReasoningConfig.load!(:non_existent_key)
      assert loaded.profile == :owl2rl
      assert loaded.mode == :materialized
    end

    test "load! returns stored config if it exists" do
      key = :test_load_bang_key
      original = ReasoningConfig.preset(:minimal_memory)
      :ok = ReasoningConfig.store(original, key)

      loaded = ReasoningConfig.load!(key)
      # minimal_memory preset has :owl2rl profile
      assert loaded.profile == :owl2rl
      assert loaded.mode == :query_time
    end

    test "stored? checks if configuration exists" do
      key = :test_stored_key
      refute ReasoningConfig.stored?(key)

      config = ReasoningConfig.preset(:balanced)
      :ok = ReasoningConfig.store(config, key)

      assert ReasoningConfig.stored?(key)
    end

    test "delete removes configuration from persistent_term" do
      key = :test_delete_key
      config = ReasoningConfig.preset(:full_materialization)
      :ok = ReasoningConfig.store(config, key)
      assert ReasoningConfig.stored?(key)

      :ok = ReasoningConfig.delete(key)
      refute ReasoningConfig.stored?(key)
    end

    test "list_stored returns all stored keys" do
      config1 = ReasoningConfig.preset(:full_materialization)
      config2 = ReasoningConfig.preset(:rdfs_only)

      :ok = ReasoningConfig.store(config1, :list_test1)
      :ok = ReasoningConfig.store(config2, :list_test2)

      keys = ReasoningConfig.list_stored()
      assert :list_test1 in keys
      assert :list_test2 in keys
    end

    test "clear_all removes all stored configurations" do
      # Store multiple configs
      ReasoningConfig.store(ReasoningConfig.preset(:full_materialization), :clear_test1)
      ReasoningConfig.store(ReasoningConfig.preset(:rdfs_only), :clear_test2)

      assert ReasoningConfig.stored?(:clear_test1)
      assert ReasoningConfig.stored?(:clear_test2)

      :ok = ReasoningConfig.clear_all()

      refute ReasoningConfig.stored?(:clear_test1)
      refute ReasoningConfig.stored?(:clear_test2)
    end

    test "overwrites existing configuration with same key" do
      key = :test_overwrite_key
      config1 = ReasoningConfig.preset(:full_materialization)
      config2 = ReasoningConfig.preset(:rdfs_only)

      :ok = ReasoningConfig.store(config1, key)
      :ok = ReasoningConfig.store(config2, key)

      assert {:ok, loaded} = ReasoningConfig.load(key)
      # rdfs_only preset has :rdfs profile
      assert loaded.profile == :rdfs
      assert loaded.mode == :materialized
    end
  end

  # ============================================================================
  # Tests: Graph Configuration Management
  # ============================================================================

  describe "graph configuration management" do
    alias TripleStore.Reasoner.GraphReasoningConfig

    test "adds graph configuration to reasoning config" do
      {:ok, config} = ReasoningConfig.new(scope: :hybrid)
      graph_config = GraphReasoningConfig.new!(graph_id: 1, scope: :local)

      updated = ReasoningConfig.put_graph_config(config, graph_config)

      assert {:ok, retrieved} = ReasoningConfig.graph_config(updated, 1)
      assert retrieved.graph_id == 1
      assert retrieved.scope == :local
    end

    test "removes graph configuration from reasoning config" do
      graph_config = GraphReasoningConfig.new!(graph_id: 1, scope: :local)

      {:ok, config} =
        ReasoningConfig.new(
          scope: :hybrid,
          graph_configs: %{1 => graph_config}
        )

      assert ReasoningConfig.graph_config_count(config) == 1

      updated = ReasoningConfig.remove_graph_config(config, 1)
      assert ReasoningConfig.graph_config_count(updated) == 0
    end

    test "returns graph_configs map" do
      graph_config1 = GraphReasoningConfig.new!(graph_id: 1, scope: :local)
      graph_config2 = GraphReasoningConfig.new!(graph_id: 2, scope: :global)

      {:ok, config} =
        ReasoningConfig.new(
          scope: :hybrid,
          graph_configs: %{1 => graph_config1, 2 => graph_config2}
        )

      configs = ReasoningConfig.graph_configs(config)
      assert map_size(configs) == 2
      assert configs[1].scope == :local
      assert configs[2].scope == :global
    end

    test "sets reasoning scope" do
      {:ok, config} = ReasoningConfig.new(scope: :local)
      updated = ReasoningConfig.put_scope(config, :global)
      assert ReasoningConfig.scope(updated) == :global
    end

    test "sets TBox graph" do
      {:ok, config} = ReasoningConfig.new()
      updated = ReasoningConfig.put_tbox_graph(config, 0)
      assert ReasoningConfig.tbox_graph(updated) == 0
    end

    test "sets inferred graph" do
      {:ok, config} = ReasoningConfig.new()
      updated = ReasoningConfig.put_inferred_graph(config, 99)
      assert ReasoningConfig.inferred_graph(updated) == 99
    end

    test "sets storage strategy" do
      {:ok, config} = ReasoningConfig.new()
      updated = ReasoningConfig.put_storage_strategy(config, :separate_graph)
      assert ReasoningConfig.storage_strategy(updated) == :separate_graph
    end
  end

  # ============================================================================
  # Tests: Storage Strategy Validation
  # ============================================================================

  describe "storage strategy validation" do
    test "accepts same_as_premises strategy" do
      {:ok, config} = ReasoningConfig.new(storage_strategy: :same_as_premises)
      assert config.storage_strategy == :same_as_premises
    end

    test "accepts separate_graph strategy" do
      {:ok, config} = ReasoningConfig.new(storage_strategy: :separate_graph)
      assert config.storage_strategy == :separate_graph
    end

    test "accepts per_graph_cf strategy" do
      {:ok, config} = ReasoningConfig.new(storage_strategy: :per_graph_cf)
      assert config.storage_strategy == :per_graph_cf
    end

    test "returns error for invalid storage strategy" do
      {:error, reason} = ReasoningConfig.new(storage_strategy: :invalid)
      assert {:invalid_storage_strategy, :invalid} = reason
    end
  end
end
