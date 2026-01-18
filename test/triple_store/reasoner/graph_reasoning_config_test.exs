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
end
