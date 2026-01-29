defmodule TripleStore.Reasoner.GraphReasoningConfigTest do
  use ExUnit.Case, async: true

  alias TripleStore.Reasoner.GraphReasoningConfig

  # ============================================================================
  # Tests: New Configuration
  # ============================================================================

  describe "new/1" do
    test "creates config with required graph_id" do
      {:ok, config} = GraphReasoningConfig.new(graph_id: 1)
      assert config.graph_id == 1
      assert config.scope == :local
      assert config.enabled == true
    end

    test "creates config with local scope" do
      {:ok, config} = GraphReasoningConfig.new(graph_id: 1, scope: :local)
      assert config.scope == :local
    end

    test "creates config with global scope" do
      {:ok, config} = GraphReasoningConfig.new(graph_id: 1, scope: :global)
      assert config.scope == :global
    end

    test "creates config with none scope" do
      {:ok, config} = GraphReasoningConfig.new(graph_id: 1, scope: :none)
      assert config.scope == :none
    end

    test "creates config with enabled: false" do
      {:ok, config} = GraphReasoningConfig.new(graph_id: 1, enabled: false)
      assert config.enabled == false
    end

    test "creates config with tbox_source option" do
      {:ok, config} = GraphReasoningConfig.new(graph_id: 1, tbox_source: :shared)
      assert config.tbox_source == :shared
    end

    test "creates config with tbox_source as graph_id" do
      {:ok, config} = GraphReasoningConfig.new(graph_id: 1, tbox_source: 0)
      assert config.tbox_source == 0
    end

    test "creates config with store_inferred option" do
      {:ok, config} = GraphReasoningConfig.new(graph_id: 1, store_inferred: :separate)
      assert config.store_inferred == :separate
    end

    test "returns error for invalid scope" do
      {:error, reason} = GraphReasoningConfig.new(graph_id: 1, scope: :invalid)
      assert reason == :invalid_scope
    end

    test "returns error for missing graph_id" do
      {:error, reason} = GraphReasoningConfig.new([])
      assert reason == :graph_id_required
    end

    test "returns error for invalid graph_id" do
      {:error, reason} = GraphReasoningConfig.new(graph_id: -1)
      assert reason == :invalid_graph_id

      {:error, reason} = GraphReasoningConfig.new(graph_id: "not_an_integer")
      assert reason == :invalid_graph_id
    end
  end

  describe "new!/1" do
    test "returns config for valid options" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :global)
      assert config.graph_id == 1
      assert config.scope == :global
    end

    test "raises for invalid options" do
      assert_raise ArgumentError, ~r/Invalid graph reasoning config/, fn ->
        GraphReasoningConfig.new!(graph_id: -1)
      end
    end
  end

  describe "default/1" do
    test "creates default config for graph" do
      config = GraphReasoningConfig.default(1)
      assert config.graph_id == 1
      assert config.scope == :local
      assert config.enabled == true
      assert config.tbox_source == :self
    end
  end

  # ============================================================================
  # Tests: Query Functions
  # ============================================================================

  describe "participates?/1" do
    test "returns true for local scope" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :local)
      assert GraphReasoningConfig.participates?(config) == true
    end

    test "returns true for global scope" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :global)
      assert GraphReasoningConfig.participates?(config) == true
    end

    test "returns false for none scope" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :none)
      assert GraphReasoningConfig.participates?(config) == false
    end

    test "returns false when enabled is false" do
      config = GraphReasoningConfig.new!(graph_id: 1, enabled: false)
      assert GraphReasoningConfig.participates?(config) == false
    end

    test "returns false for none scope even when enabled is true" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :none, enabled: true)
      assert GraphReasoningConfig.participates?(config) == false
    end
  end

  describe "local?/1" do
    test "returns true for local scope" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :local)
      assert GraphReasoningConfig.local?(config) == true
    end

    test "returns false for global scope" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :global)
      assert GraphReasoningConfig.local?(config) == false
    end

    test "returns false for none scope" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :none)
      assert GraphReasoningConfig.local?(config) == false
    end
  end

  describe "global?/1" do
    test "returns true for global scope" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :global)
      assert GraphReasoningConfig.global?(config) == true
    end

    test "returns false for local scope" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :local)
      assert GraphReasoningConfig.global?(config) == false
    end

    test "returns false for none scope" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :none)
      assert GraphReasoningConfig.global?(config) == false
    end
  end

  # ============================================================================
  # Tests: Update Functions
  # ============================================================================

  describe "put_scope/2" do
    test "updates scope to local" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :global)
      updated = GraphReasoningConfig.put_scope(config, :local)
      assert updated.scope == :local
    end

    test "updates scope to global" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :local)
      updated = GraphReasoningConfig.put_scope(config, :global)
      assert updated.scope == :global
    end

    test "updates scope to none" do
      config = GraphReasoningConfig.new!(graph_id: 1, scope: :local)
      updated = GraphReasoningConfig.put_scope(config, :none)
      assert updated.scope == :none
    end
  end

  describe "put_tbox_source/2" do
    test "sets tbox_source to shared" do
      config = GraphReasoningConfig.new!(graph_id: 1)
      updated = GraphReasoningConfig.put_tbox_source(config, :shared)
      assert updated.tbox_source == :shared
    end

    test "sets tbox_source to graph_id" do
      config = GraphReasoningConfig.new!(graph_id: 1)
      updated = GraphReasoningConfig.put_tbox_source(config, 0)
      assert updated.tbox_source == 0
    end
  end

  describe "put_store_inferred/2" do
    test "sets store_inferred to self" do
      config = GraphReasoningConfig.new!(graph_id: 1, store_inferred: :separate)
      updated = GraphReasoningConfig.put_store_inferred(config, :self)
      assert updated.store_inferred == :self
    end

    test "sets store_inferred to separate" do
      config = GraphReasoningConfig.new!(graph_id: 1)
      updated = GraphReasoningConfig.put_store_inferred(config, :separate)
      assert updated.store_inferred == :separate
    end
  end

  # ============================================================================
  # Tests: Summary
  # ============================================================================

  describe "summary/1" do
    test "returns summary map" do
      config =
        GraphReasoningConfig.new!(
          graph_id: 1,
          scope: :local,
          enabled: true,
          tbox_source: :shared
        )

      summary = GraphReasoningConfig.summary(config)

      assert summary.graph_id == 1
      assert summary.scope == :local
      assert summary.enabled == true
      assert summary.participates == true
      assert summary.tbox_source == :shared
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
      GraphReasoningConfig.clear_store(:test_store_key)
      GraphReasoningConfig.clear_store(:store1)
      GraphReasoningConfig.clear_store(:store2)

      on_exit(fn ->
        GraphReasoningConfig.clear_store(:test_store_key)
        GraphReasoningConfig.clear_store(:store1)
        GraphReasoningConfig.clear_store(:store2)
      end)

      :ok
    end

    test "stores configuration in persistent_term" do
      config = GraphReasoningConfig.new!(graph_id: 99, scope: :local)
      assert :ok = GraphReasoningConfig.store(config, :test_store_key)
      assert GraphReasoningConfig.stored?(:test_store_key, 99)
    end

    test "loads stored configuration from persistent_term" do
      original = GraphReasoningConfig.new!(graph_id: 98, scope: :global)
      :ok = GraphReasoningConfig.store(original, :test_store_key)

      assert {:ok, loaded} = GraphReasoningConfig.load(:test_store_key, 98)
      assert loaded.graph_id == original.graph_id
      assert loaded.scope == original.scope
    end

    test "returns error for non-existent configuration" do
      assert {:error, :not_found} = GraphReasoningConfig.load(:test_store_key, 999)
    end

    test "load! returns default for non-existent configuration" do
      loaded = GraphReasoningConfig.load!(:test_store_key, 999)
      assert loaded.graph_id == 999
      assert loaded.scope == :local
      assert loaded.enabled == true
    end

    test "load! returns stored config if it exists" do
      original = GraphReasoningConfig.new!(graph_id: 97, scope: :none)
      :ok = GraphReasoningConfig.store(original, :test_store_key)

      loaded = GraphReasoningConfig.load!(:test_store_key, 97)
      assert loaded.graph_id == 97
      assert loaded.scope == :none
    end

    test "stored? checks if configuration exists" do
      refute GraphReasoningConfig.stored?(:test_store_key, 96)

      config = GraphReasoningConfig.new!(graph_id: 96, scope: :local)
      :ok = GraphReasoningConfig.store(config, :test_store_key)

      assert GraphReasoningConfig.stored?(:test_store_key, 96)
    end

    test "delete removes configuration from persistent_term" do
      config = GraphReasoningConfig.new!(graph_id: 95, scope: :local)
      :ok = GraphReasoningConfig.store(config, :test_store_key)
      assert GraphReasoningConfig.stored?(:test_store_key, 95)

      :ok = GraphReasoningConfig.delete(:test_store_key, 95)
      refute GraphReasoningConfig.stored?(:test_store_key, 95)
    end

    test "list_stored_for_store returns all graph IDs for a store" do
      # Store multiple graph configs
      config1 = GraphReasoningConfig.new!(graph_id: 1, scope: :local)
      config2 = GraphReasoningConfig.new!(graph_id: 2, scope: :global)
      config3 = GraphReasoningConfig.new!(graph_id: 3, scope: :local)

      :ok = GraphReasoningConfig.store(config1, :test_store_key)
      :ok = GraphReasoningConfig.store(config2, :test_store_key)
      :ok = GraphReasoningConfig.store(config3, :test_store_key)

      graph_ids = GraphReasoningConfig.list_stored_for_store(:test_store_key)
      assert 1 in graph_ids
      assert 2 in graph_ids
      assert 3 in graph_ids
    end

    test "clear_store removes all configurations for a store" do
      # Store multiple graph configs
      config1 = GraphReasoningConfig.new!(graph_id: 10, scope: :local)
      config2 = GraphReasoningConfig.new!(graph_id: 20, scope: :global)

      :ok = GraphReasoningConfig.store(config1, :test_clear_store_key)
      :ok = GraphReasoningConfig.store(config2, :test_clear_store_key)

      assert GraphReasoningConfig.stored?(:test_clear_store_key, 10)
      assert GraphReasoningConfig.stored?(:test_clear_store_key, 20)

      :ok = GraphReasoningConfig.clear_store(:test_clear_store_key)

      refute GraphReasoningConfig.stored?(:test_clear_store_key, 10)
      refute GraphReasoningConfig.stored?(:test_clear_store_key, 20)
    end

    test "overwrites existing configuration with same store_key and graph_id" do
      config1 = GraphReasoningConfig.new!(graph_id: 94, scope: :local)
      config2 = GraphReasoningConfig.new!(graph_id: 94, scope: :none)

      :ok = GraphReasoningConfig.store(config1, :test_store_key)
      :ok = GraphReasoningConfig.store(config2, :test_store_key)

      assert {:ok, loaded} = GraphReasoningConfig.load(:test_store_key, 94)
      assert loaded.scope == :none
    end

    test "separates configurations by store_key" do
      config1 = GraphReasoningConfig.new!(graph_id: 1, scope: :local)
      config2 = GraphReasoningConfig.new!(graph_id: 1, scope: :global)

      :ok = GraphReasoningConfig.store(config1, :store1)
      :ok = GraphReasoningConfig.store(config2, :store2)

      assert {:ok, loaded1} = GraphReasoningConfig.load(:store1, 1)
      assert {:ok, loaded2} = GraphReasoningConfig.load(:store2, 1)

      assert loaded1.scope == :local
      assert loaded2.scope == :global
    end
  end

  # ============================================================================
  # Tests: TBox and Storage Configuration
  # ============================================================================

  describe "tbox_source configuration" do
    test "accepts :self tbox_source" do
      config = GraphReasoningConfig.new!(graph_id: 1, tbox_source: :self)
      assert config.tbox_source == :self
      assert GraphReasoningConfig.tbox_graph_id(config) == :self
    end

    test "accepts :shared tbox_source" do
      config = GraphReasoningConfig.new!(graph_id: 1, tbox_source: :shared)
      assert config.tbox_source == :shared
      assert GraphReasoningConfig.tbox_graph_id(config) == 0
    end

    test "accepts graph_id as tbox_source" do
      config = GraphReasoningConfig.new!(graph_id: 1, tbox_source: 5)
      assert config.tbox_source == 5
      assert GraphReasoningConfig.tbox_graph_id(config) == 5
    end

    test "returns error for invalid tbox_source" do
      {:error, reason} = GraphReasoningConfig.new(graph_id: 1, tbox_source: :invalid)
      assert reason == :invalid_tbox_source
    end

    test "returns error for tbox_source equal to graph_id" do
      {:error, reason} = GraphReasoningConfig.new(graph_id: 1, tbox_source: 1)
      assert reason == :invalid_tbox_source
    end

    test "shared_tbox? returns true for shared source" do
      config = GraphReasoningConfig.new!(graph_id: 1, tbox_source: :shared)
      assert GraphReasoningConfig.shared_tbox?(config)
    end

    test "shared_tbox? returns true for graph_id source" do
      config = GraphReasoningConfig.new!(graph_id: 1, tbox_source: 5)
      assert GraphReasoningConfig.shared_tbox?(config)
    end

    test "shared_tbox? returns false for self source" do
      config = GraphReasoningConfig.new!(graph_id: 1, tbox_source: :self)
      refute GraphReasoningConfig.shared_tbox?(config)
    end
  end

  describe "store_inferred configuration" do
    test "accepts :self store_inferred" do
      config = GraphReasoningConfig.new!(graph_id: 1, store_inferred: :self)
      assert config.store_inferred == :self
      refute GraphReasoningConfig.separate_inferred_graph?(config)
    end

    test "accepts :separate store_inferred" do
      config = GraphReasoningConfig.new!(graph_id: 1, store_inferred: :separate)
      assert config.store_inferred == :separate
      assert GraphReasoningConfig.separate_inferred_graph?(config)
    end

    test "returns error for invalid store_inferred" do
      {:error, reason} = GraphReasoningConfig.new(graph_id: 1, store_inferred: :invalid)
      assert reason == :invalid_store_inferred
    end
  end
end
