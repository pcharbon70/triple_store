defmodule TripleStore.SPARQL.QuadPatternSPARQLIntegrationTest do
  @moduledoc """
  Integration tests for quad pattern SPARQL queries.

  These tests verify end-to-end integration of quad pattern recognition,
  optimization modules, and the SPARQL executor's decision logic.

  Covers Section 3.5 requirements from Phase 3 planning.
  """
  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.QuadPatternRecognition
  alias TripleStore.SPARQL.GraphClauseOptimization

  @moduletag :quad_pattern_sparql_integration

  # ===========================================================================
  # Integration Test 3.5.1: Pattern Recognition Integration
  # ===========================================================================

  describe "Quad pattern recognition integration" do
    test "full quad pattern workflow: analyze -> decide -> translate" do
      # Fully-unbound pattern
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      # Step 1: Analyze pattern
      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)
      assert analysis.decision == :use_multi_iterator
      assert analysis.variable_count == 4

      # Step 2: Check decision helper
      assert QuadPatternRecognition.should_use_multi_iterator?(pattern)

      # Step 3: Translate to leapfrog pattern
      translated = QuadPatternRecognition.translate_to_leapfrog_pattern(pattern)
      assert translated == pattern

      # Step 4: Extract variables
      variables = QuadPatternRecognition.extract_variables(pattern)
      assert variables == ["s", "p", "o", "g"]
    end

    test "partially-bound pattern workflow" do
      # Pattern with 2 bound, 2 variable components
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:bound, 42}, {:bound, 0}}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)
      assert analysis.decision == :use_multi_iterator
      assert analysis.variable_count == 2

      # Should recommend multi-iterator for 2+ variables
      assert QuadPatternRecognition.should_use_multi_iterator?(pattern)

      # Extract variables should return variable names and nils
      variables = QuadPatternRecognition.extract_variables(pattern)
      assert variables == ["s", "p", nil, nil]
    end

    test "single-variable pattern uses single iterator" do
      pattern = {:quad, {:variable, "s"}, {:bound, 42}, {:bound, 35}, {:bound, 0}}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)
      assert analysis.decision == :use_single_iterator
      assert analysis.recommended_approach == :prefix_scan

      # Should NOT recommend multi-iterator
      refute QuadPatternRecognition.should_use_multi_iterator?(pattern)
    end

    test "fully-bound pattern uses direct lookup" do
      pattern = {:quad, {:bound, 1}, {:bound, 2}, {:bound, 3}, {:bound, 4}}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)
      assert analysis.decision == :use_single_iterator
      assert analysis.recommended_approach == :direct_lookup
      assert analysis.estimated_cardinality == 1.0

      refute QuadPatternRecognition.should_use_multi_iterator?(pattern)
    end
  end

  # ===========================================================================
  # Integration Test 3.5.2: GRAPH Clause Optimization Integration
  # ===========================================================================

  describe "GRAPH clause optimization integration" do
    test "static graph with single variable: graph-prefixed iterator" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:bound, 42}, {:bound, 35}}]}
      graph_term = {:named_node, "http://example.org/graph"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :static
      assert analysis.recommended_strategy == :graph_prefixed_single_iterator
      assert Keyword.get(analysis.optimization_hints, :bind_graph) == true
      assert Keyword.get(analysis.optimization_hints, :reduce_iterator_count) == true
    end

    test "static graph with 2+ variables: multi-iterator with bound graph" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:bound, 35}}]}
      graph_term = {:named_node, "http://example.org/graph"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :static
      assert analysis.recommended_strategy == :multi_iterator_with_bound_graph
      assert Keyword.get(analysis.optimization_hints, :use_multi_iterator) == true
      assert Keyword.get(analysis.optimization_hints, :bind_graph) == true

      # Should use multi-iterator
      assert GraphClauseOptimization.should_use_multi_iterator_for_graph?(graph_term, pattern)
    end

    test "variable graph with 3 other variables: 4-iterator enumeration" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}]}
      graph_term = {:variable, "g"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :variable
      assert analysis.recommended_strategy == :four_iterator_enumeration
      assert Keyword.get(analysis.optimization_hints, :use_multi_iterator) == true
      assert Keyword.get(analysis.optimization_hints, :enumerate_all_graphs) == true

      # Should use multi-iterator
      assert GraphClauseOptimization.should_use_multi_iterator_for_graph?(graph_term, pattern)
    end

    test "variable graph with fewer variables: sequential iteration" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:bound, 42}, {:bound, 35}}]}
      graph_term = {:variable, "g"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :variable
      assert analysis.recommended_strategy == :sequential_graph_iteration
      assert Keyword.get(analysis.optimization_hints, :use_sequential_iteration) == true

      # Should NOT use multi-iterator
      refute GraphClauseOptimization.should_use_multi_iterator_for_graph?(graph_term, pattern)
    end

    test "optimization hints application" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:bound, 42}, {:bound, 35}}]}
      hints = [bind_graph: true, graph_binding: %{g: 123}]

      {optimized_pattern, graph_binding} =
        GraphClauseOptimization.apply_optimization_hints(pattern, hints)

      assert optimized_pattern == pattern
      assert graph_binding == %{g: 123}
    end
  end

  # ===========================================================================
  # Integration Test 3.5.3: Quad Pattern with Statistics
  # ===========================================================================

  describe "Pattern recognition with statistics" do
    test "uses statistics for cardinality estimation" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:bound, 35}, {:bound, 0}}
      stats = %{total_quads: 1_000_000}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern, stats)

      # With high stats, multi-iterator is recommended
      assert analysis.decision == :use_multi_iterator
      # Cardinality should be influenced by stats
      assert analysis.estimated_cardinality > 0
    end

    test "low stats still recommends multi-iterator for 2+ variables" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:bound, 35}, {:bound, 0}}
      low_stats = %{total_quads: 100}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern, low_stats)

      # Multi-iterator for worst-case optimal guarantees
      assert analysis.decision == :use_multi_iterator
    end
  end

  # ===========================================================================
  # Integration Test 3.5.4: Edge Cases
  # ===========================================================================

  describe "Edge case handling" do
    test "pattern with all variable positions" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)

      assert analysis.variable_count == 4
      assert analysis.decision == :use_multi_iterator
    end

    test "pattern with no variable positions" do
      pattern = {:quad, {:bound, 1}, {:bound, 2}, {:bound, 3}, {:bound, 4}}

      analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)

      assert analysis.variable_count == 0
      assert analysis.decision == :use_single_iterator
      assert analysis.recommended_approach == :direct_lookup
    end

    test "pattern with mixed atom and integer values translates correctly" do
      pattern = {:quad, :default_graph, 42, 35, 0}

      translated = QuadPatternRecognition.translate_to_leapfrog_pattern(pattern)

      assert translated ==
               {:quad, {:bound, :default_graph}, {:bound, 42}, {:bound, 35}, {:bound, 0}}
    end

    test "empty BGP pattern uses direct lookup" do
      pattern = {:bgp, []}
      graph_term = {:variable, "g"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      # Empty pattern (0 variables) uses direct lookup
      assert analysis.recommended_strategy == :direct_lookup
    end
  end

  # ===========================================================================
  # Integration Test 3.5.5: Strategy Selection Consistency
  # ===========================================================================

  describe "Strategy selection consistency" do
    test "strategies are consistent across modules" do
      # A 4-variable quad pattern
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      # QuadPatternRecognition should recommend multi-iterator
      quad_analysis = QuadPatternRecognition.analyze_quad_pattern(pattern)
      assert quad_analysis.decision == :use_multi_iterator

      # should_use_multi_iterator? should return true
      assert QuadPatternRecognition.should_use_multi_iterator?(pattern)

      # For equivalent GRAPH clause with 3 variables + 1 graph variable
      graph_pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}]}
      graph_term = {:variable, "g"}

      graph_analysis = GraphClauseOptimization.analyze_graph_clause(graph_pattern, graph_term)
      assert graph_analysis.recommended_strategy == :four_iterator_enumeration
      assert GraphClauseOptimization.should_use_multi_iterator_for_graph?(graph_term, graph_pattern)
    end

    test "single-variable patterns consistently use single iterator" do
      # Quad pattern with single variable
      quad_pattern = {:quad, {:variable, "s"}, {:bound, 42}, {:bound, 35}, {:bound, 0}}

      quad_analysis = QuadPatternRecognition.analyze_quad_pattern(quad_pattern)
      assert quad_analysis.decision == :use_single_iterator
      refute QuadPatternRecognition.should_use_multi_iterator?(quad_pattern)

      # Equivalent GRAPH clause
      graph_pattern = {:bgp, [{:triple, {:variable, "s"}, {:bound, 42}, {:bound, 35}}]}
      graph_term = {:named_node, "http://example.org/graph"}

      graph_analysis = GraphClauseOptimization.analyze_graph_clause(graph_pattern, graph_term)
      assert graph_analysis.recommended_strategy == :graph_prefixed_single_iterator
      refute GraphClauseOptimization.should_use_multi_iterator_for_graph?(graph_term, graph_pattern)
    end
  end

  # ===========================================================================
  # Integration Test 3.5.6: Performance Validation
  # ===========================================================================

  describe "Performance characteristics" do
    test "analysis completes quickly for complex patterns" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      stats = %{total_quads: 10_000_000}

      {time, _analysis} = :timer.tc(fn -> QuadPatternRecognition.analyze_quad_pattern(pattern, stats) end)

      # Analysis should be sub-millisecond
      assert time < 1000
    end

    test "GRAPH clause analysis completes quickly" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}]}
      graph_term = {:variable, "g"}

      {time, _analysis} =
        :timer.tc(fn -> GraphClauseOptimization.analyze_graph_clause(pattern, graph_term) end)

      # Analysis should be sub-millisecond
      assert time < 1000
    end

    test "variable counting is efficient" do
      # Large BGP with many triple patterns
      triple_patterns =
        Enum.map(1..100, fn i ->
          {:triple, {:variable, "s#{i}"}, {:variable, "p#{i}"}, {:bound, i}}
        end)

      pattern = {:bgp, triple_patterns}

      {time, _analysis} =
        :timer.tc(fn -> GraphClauseOptimization.analyze_graph_clause(pattern, {:variable, "g"}) end)

      # Should handle 100 triple patterns efficiently
      assert time < 10_000
    end
  end
end
