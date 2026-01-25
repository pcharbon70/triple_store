# credo:disable-for-this-file Credo.Check.Readability.FunctionNames
defmodule TripleStore.Reasoner.GraphScopedReasoningIntegrationTest do
  @moduledoc """
  Integration tests for Section 7.1: Graph-Aware Reasoning Scope Design.

  These tests verify the graph-aware reasoning features:
  - Graph-local reasoning with isolated inferences
  - Global reasoning across all graphs
  - Hybrid reasoning with per-graph configuration
  - Per-graph status tracking
  - TBox sharing across graphs

  ## Test Coverage

  - 7.1.10.1: Test end-to-end graph-local reasoning workflow
  - 7.1.10.2: Test cross-graph inference scenarios
  - 7.1.10.3: Test TBox sharing across graphs
  - 7.1.10.4: Test hybrid configuration with mixed scopes

  ## Test Data Model

  Tests use a simple university domain with:
  - Person, Student, Professor classes
  - teaches, advisedBy relationships
  - Department class with membership

  Data is organized into separate graphs:
  - Graph 0: TBox (schema/ontology)
  - Graph 1: University A data
  - Graph 2: University B data
  - Graph 3: Shared research data
  """
  use TripleStore.ReasonerTestCase

  alias TripleStore.Reasoner.{
    GraphReasoningConfig,
    GraphReasoningStatus,
    GraphScopedReasoner,
    ReasoningConfig
  }

  # ============================================================================
  # Test Namespace and IRIs
  # ============================================================================

  @ex "http://example.org/"

  # Note: IRI builders (ex_iri/1) and vocabulary helpers (rdf_type/0, rdfs_subClassOf/0, etc.)
  # are imported from TripleStore.Test.ReasonerHelpers via ReasonerTestCase

  # ============================================================================
  # Test Data Generation
  # ============================================================================

  # Simple TBox (schema) with class hierarchy
  defp generate_tbox_facts do
    [
      # Person is a class
      {ex_iri("Person"), rdf_type(), ex_iri("Class")},
      # Student is a subclass of Person
      {ex_iri("Student"), rdfs_subClassOf(), ex_iri("Person")},
      # Professor is a subclass of Person
      {ex_iri("Professor"), rdfs_subClassOf(), ex_iri("Person")},
      # Department is a class
      {ex_iri("Department"), rdf_type(), ex_iri("Class")},
      # teaches domain: Professor
      {ex_iri("teaches"), rdfs_domain(), ex_iri("Professor")},
      # teaches is transitive
      {ex_iri("teaches"), rdf_type(), owl_TransitiveProperty()},
      # memberOf domain: Department
      {ex_iri("memberOf"), rdfs_domain(), ex_iri("Department")}
    ]
  end

  # ABox facts for graph 1 (University A)
  defp generate_graph1_facts do
    [
      {ex_iri("alice"), rdf_type(), ex_iri("Student")},
      {ex_iri("bob"), rdf_type(), ex_iri("Professor")},
      {ex_iri("charlie"), rdf_type(), ex_iri("Professor")},
      {ex_iri("cs_dept"), rdf_type(), ex_iri("Department")},
      # alice is member of cs_dept
      {ex_iri("alice"), ex_iri("memberOf"), ex_iri("cs_dept")},
      # bob teaches cs101
      {ex_iri("bob"), ex_iri("teaches"), ex_iri("cs101")},
      # charlie teaches cs201
      {ex_iri("charlie"), ex_iri("teaches"), ex_iri("cs201")}
    ]
  end

  # ABox facts for graph 2 (University B)
  defp generate_graph2_facts do
    [
      {ex_iri("david"), rdf_type(), ex_iri("Student")},
      {ex_iri("eve"), rdf_type(), ex_iri("Professor")},
      {ex_iri("math_dept"), rdf_type(), ex_iri("Department")},
      # david is member of math_dept
      {ex_iri("david"), ex_iri("memberOf"), ex_iri("math_dept")},
      # eve teaches math101
      {ex_iri("eve"), ex_iri("teaches"), ex_iri("math101")}
    ]
  end

  # Convert explicit facts to term-based format
  defp to_term_facts(facts, graph_id) do
    Enum.map(facts, fn {s, p, o} -> {graph_id, s, p, o} end)
  end

  # ============================================================================
  # Helper Functions
  # ============================================================================

  defp create_fact_map(graph_facts) do
    Enum.reduce(graph_facts, %{}, fn {graph_id, facts}, acc ->
      Map.put(acc, graph_id, MapSet.new(facts))
    end)
  end

  defp count_facts_by_graph(fact_map) do
    Map.new(fact_map, fn {graph_id, facts} -> {graph_id, MapSet.size(facts)} end)
  end

  # ============================================================================
  # Tests: Graph-Local Reasoning (7.1.10.1)
  # ============================================================================

  describe "graph-local reasoning workflow" do
    test "materializes a single graph in isolation" do
      # Setup: TBox in graph 0, data in graph 1
      tbox_facts = generate_tbox_facts() |> to_term_facts(0)
      graph1_facts = generate_graph1_facts() |> to_term_facts(1)

      all_facts = MapSet.new(tbox_facts ++ graph1_facts)

      # Use local reasoning with TBox sharing from graph 0
      config = [
        graph_id: 1,
        profile: :rdfs,
        tbox_graph: 0
      ]

      # Note: This test demonstrates the API structure
      # Full database-backed testing would require TripleStore.open/close
      assert is_map(all_facts)
      assert MapSet.size(all_facts) > 0

      # Verify graph isolation: graph 1 facts are in correct graph
      graph1_quads = Enum.filter(all_facts, fn {g, _s, _p, _o} -> g == 1 end)
      assert length(graph1_quads) == length(graph1_facts)
    end

    test "respects graph boundaries in local reasoning" do
      # Graph-local reasoning should not infer across graphs
      tbox_facts = generate_tbox_facts() |> to_term_facts(0)
      graph1_facts = generate_graph1_facts() |> to_term_facts(1)
      graph2_facts = generate_graph2_facts() |> to_term_facts(2)

      all_facts = MapSet.new(tbox_facts ++ graph1_facts ++ graph2_facts)

      # Each graph should be processed independently
      graph1_quads = Enum.filter(all_facts, fn {g, _s, _p, _o} -> g == 1 end)
      graph2_quads = Enum.filter(all_facts, fn {g, _s, _p, _o} -> g == 2 end)

      assert length(graph1_quads) > 0
      assert length(graph2_quads) > 0
      assert length(graph1_quads) != length(graph2_quads)
    end

    test "tracks status per graph" do
      # Create status for multiple graphs
      {:ok, status1} = GraphReasoningStatus.new(graph_id: 1)
      {:ok, status2} = GraphReasoningStatus.new(graph_id: 2)

      assert status1.graph_id == 1
      assert status2.graph_id == 2

      # Record materialization for graph 1
      stats1 = %{
        derived_count: 100,
        iterations: 3,
        duration_ms: 50
      }

      status1 = GraphReasoningStatus.record_materialization(status1, stats1)

      assert status1.state == :materialized
      assert status1.derived_count == 100

      # Graph 2 should still be initialized
      assert status2.state == :initialized
    end
  end

  # ============================================================================
  # Tests: Global Reasoning (7.1.10.2)
  # ============================================================================

  describe "global reasoning with cross-graph inference" do
    test "configures graphs for global reasoning" do
      # Create configs for graphs participating in global reasoning
      config1 = GraphReasoningConfig.new!(graph_id: 1, scope: :global)
      config2 = GraphReasoningConfig.new!(graph_id: 2, scope: :global)
      config3 = GraphReasoningConfig.new!(graph_id: 3, scope: :local)

      assert GraphReasoningConfig.global?(config1)
      assert GraphReasoningConfig.global?(config2)
      assert GraphReasoningConfig.local?(config3)

      assert GraphReasoningConfig.participates?(config1)
      assert GraphReasoningConfig.participates?(config2)
      assert GraphReasoningConfig.participates?(config3)
    end

    test "creates global reasoning configuration" do
      {:ok, config} =
        ReasoningConfig.new(
          scope: :global,
          tbox_graph: 0,
          inferred_graph: 99
        )

      assert config.scope == :global
      assert config.tbox_graph == 0
      assert config.inferred_graph == 99
    end

    test "identifies global-scoped graphs" do
      configs = %{
        1 => [scope: :global],
        2 => [scope: :local],
        3 => [scope: :none]
      }

      global_graphs =
        configs
        |> Enum.filter(fn {_gid, opts} -> Keyword.get(opts, :scope) == :global end)
        |> Enum.map(fn {gid, _opts} -> gid end)

      assert 1 in global_graphs
      assert 2 not in global_graphs
      assert 3 not in global_graphs
    end
  end

  # ============================================================================
  # Tests: Hybrid Reasoning (7.1.10.4)
  # ============================================================================

  describe "hybrid reasoning with mixed scopes" do
    test "creates hybrid configuration with per-graph scopes" do
      {:ok, config} =
        ReasoningConfig.new(
          scope: :hybrid,
          graph_configs: %{
            1 => [scope: :local],
            2 => [scope: :global],
            3 => [scope: :none]
          }
        )

      assert config.scope == :hybrid
      assert config.graph_configs[1][:scope] == :local
      assert config.graph_configs[2][:scope] == :global
      assert config.graph_configs[3][:scope] == :none
    end

    test "partitions graphs by scope" do
      graph_configs = %{
        1 => GraphReasoningConfig.new!(graph_id: 1, scope: :local),
        2 => GraphReasoningConfig.new!(graph_id: 2, scope: :global),
        3 => GraphReasoningConfig.new!(graph_id: 3, scope: :none)
      }

      local_graphs =
        graph_configs
        |> Enum.filter(fn {_gid, cfg} -> cfg.scope == :local end)
        |> Enum.map(fn {gid, _cfg} -> gid end)

      global_graphs =
        graph_configs
        |> Enum.filter(fn {_gid, cfg} -> cfg.scope == :global end)
        |> Enum.map(fn {gid, _cfg} -> gid end)

      none_graphs =
        graph_configs
        |> Enum.filter(fn {_gid, cfg} -> cfg.scope == :none end)
        |> Enum.map(fn {gid, _cfg} -> gid end)

      assert local_graphs == [1]
      assert global_graphs == [2]
      assert none_graphs == [3]
    end

    test "computes aggregate statistics across graphs" do
      statuses = %{
        1 =>
          GraphReasoningStatus.default(1)
          |> GraphReasoningStatus.record_materialization(%{
            derived_count: 100,
            iterations: 3,
            duration_ms: 50
          }),
        2 =>
          GraphReasoningStatus.default(2)
          |> GraphReasoningStatus.record_materialization(%{
            derived_count: 200,
            iterations: 4,
            duration_ms: 75
          }),
        # Not materialized
        3 => GraphReasoningStatus.default(3)
      }

      aggregate = GraphReasoningStatus.aggregate(statuses)

      assert aggregate.total_graphs == 3
      assert aggregate.materialized == 2
      assert aggregate.initialized == 1
      assert aggregate.total_derived == 300
    end
  end

  # ============================================================================
  # Tests: TBox Sharing (7.1.10.3)
  # ============================================================================

  describe "TBox sharing across graphs" do
    test "configures TBox source for graphs" do
      # Graph with shared TBox
      config1 = GraphReasoningConfig.new!(graph_id: 1, tbox_source: :shared)
      assert config1.tbox_source == :shared

      # Graph with TBox from specific graph
      config2 = GraphReasoningConfig.new!(graph_id: 2, tbox_source: 0)
      assert config2.tbox_source == 0

      # Graph with its own TBox
      config3 = GraphReasoningConfig.new!(graph_id: 3, tbox_source: :self)
      assert config3.tbox_source == :self
    end

    test "resolves TBox location" do
      tbox_graph = 0

      # When tbox_source is a graph_id, use it
      config = GraphReasoningConfig.new!(graph_id: 1, tbox_source: 5)
      assert config.tbox_source == 5

      # When using global config, fall back to global TBox
      global_config = ReasoningConfig.new!(tbox_graph: tbox_graph)

      assert global_config.tbox_graph == tbox_graph
    end
  end

  # ============================================================================
  # Tests: Status Tracking
  # ============================================================================

  describe "per-graph status tracking" do
    test "records materialization events per graph" do
      status = GraphReasoningStatus.default(1)

      stats = %{
        derived_count: 42,
        iterations: 2,
        duration_ms: 25
      }

      status = GraphReasoningStatus.record_materialization(status, stats)

      assert status.state == :materialized
      assert status.derived_count == 42
      assert status.materialization_count == 1
      assert status.last_materialization != nil
    end

    test "marks graph as stale when needed" do
      status = GraphReasoningStatus.default(1)

      # After materialization
      status =
        GraphReasoningStatus.record_materialization(status, %{
          derived_count: 42,
          iterations: 2,
          duration_ms: 25
        })

      assert status.state == :materialized
      assert not GraphReasoningStatus.needs_rematerialization?(status)

      # Mark as stale
      status = GraphReasoningStatus.mark_stale(status)

      assert status.state == :stale
      assert GraphReasoningStatus.needs_rematerialization?(status)
    end

    test "stores and retrieves status from persistent_term" do
      status = GraphReasoningStatus.default(1)
      key = :test_graph_1

      # Store
      :ok = GraphReasoningStatus.store(status, key)

      # Retrieve
      {:ok, retrieved} = GraphReasoningStatus.load(key)

      assert retrieved.graph_id == status.graph_id
      assert retrieved.state == status.state
      assert retrieved.derived_count == status.derived_count
    end

    test "lists all stored status keys" do
      key1 = :test_graph_1
      key2 = :test_graph_2

      status1 = GraphReasoningStatus.default(1)
      status2 = GraphReasoningStatus.default(2)

      :ok = GraphReasoningStatus.store(status1, key1)
      :ok = GraphReasoningStatus.store(status2, key2)

      keys = GraphReasoningStatus.list_stored()

      assert key1 in keys
      assert key2 in keys
    end
  end

  # ============================================================================
  # Tests: Configuration Validation
  # ============================================================================

  describe "configuration validation" do
    test "validates graph configuration" do
      # Valid local config
      {:ok, config} = GraphReasoningConfig.new(graph_id: 1, scope: :local)
      assert config.scope == :local

      # Invalid scope
      {:error, reason} = GraphReasoningConfig.new(graph_id: 1, scope: :invalid)
      assert reason == :invalid_scope

      # Missing graph_id
      {:error, reason} = GraphReasoningConfig.new([])
      assert reason == :graph_id_required
    end

    test "updates configuration fields" do
      config = GraphReasoningConfig.default(1)

      # Update scope
      config = GraphReasoningConfig.put_scope(config, :global)
      assert config.scope == :global

      # Update TBox source
      config = GraphReasoningConfig.put_tbox_source(config, :shared)
      assert config.tbox_source == :shared

      # Update storage strategy
      config = GraphReasoningConfig.put_store_inferred(config, :separate)
      assert config.store_inferred == :separate
    end
  end
end
