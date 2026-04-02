defmodule TripleStore.SPARQL.GraphClauseOptimizationTest do
  use ExUnit.Case, async: true
  alias TripleStore.SPARQL.GraphClauseOptimization

  @moduletag :graph_clause_optimization

  describe "analyze_graph_clause/3" do
    test "recommends direct lookup for fully-bound pattern" do
      pattern = {:bgp, [{:triple, {:bound, 1}, {:bound, 2}, {:bound, 3}}]}
      graph_term = {:named_node, "http://example.org/graph"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :static
      assert analysis.recommended_strategy == :direct_lookup
      assert Keyword.get(analysis.optimization_hints, :use_direct_lookup) == true
    end

    test "recommends graph-prefixed iterator for static graph with single variable" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:bound, 42}, {:bound, 35}}]}
      graph_term = {:named_node, "http://example.org/graph"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :static
      assert analysis.recommended_strategy == :graph_prefixed_single_iterator
      assert Keyword.get(analysis.optimization_hints, :bind_graph) == true
      assert Keyword.get(analysis.optimization_hints, :reduce_iterator_count) == true
    end

    test "recommends multi-iterator for static graph with 2+ variables" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:bound, 35}}]}
      graph_term = {:named_node, "http://example.org/graph"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :static
      assert analysis.recommended_strategy == :multi_iterator_with_bound_graph
      assert Keyword.get(analysis.optimization_hints, :use_multi_iterator) == true
    end

    test "recommends 4-iterator for variable graph with 3 variables" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}]}
      graph_term = {:variable, "g"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :variable
      assert analysis.recommended_strategy == :four_iterator_enumeration
      assert Keyword.get(analysis.optimization_hints, :use_multi_iterator) == true
    end

    test "recommends sequential iteration for variable graph with fewer variables" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:bound, 42}, {:bound, 35}}]}
      graph_term = {:variable, "g"}

      analysis = GraphClauseOptimization.analyze_graph_clause(pattern, graph_term)

      assert analysis.graph_type == :variable
      assert analysis.recommended_strategy == :sequential_graph_iteration
    end
  end

  describe "should_use_multi_iterator_for_graph?/2" do
    test "returns false for static graph with single variable" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:bound, 42}, {:bound, 35}}]}
      graph_term = {:named_node, "http://example.org/graph"}

      refute GraphClauseOptimization.should_use_multi_iterator_for_graph?(graph_term, pattern)
    end

    test "returns true for variable graph with 3 variables" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}]}
      graph_term = {:variable, "g"}

      assert GraphClauseOptimization.should_use_multi_iterator_for_graph?(graph_term, pattern)
    end

    test "returns true for static graph with 2+ variables" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:variable, "p"}, {:bound, 35}}]}
      graph_term = {:named_node, "http://example.org/graph"}

      assert GraphClauseOptimization.should_use_multi_iterator_for_graph?(graph_term, pattern)
    end
  end

  describe "apply_optimization_hints/2" do
    test "returns pattern and graph binding when bind_graph is true" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:bound, 42}, {:bound, 35}}]}
      hints = [bind_graph: true, graph_binding: %{g: 123}]

      {optimized_pattern, graph_binding} =
        GraphClauseOptimization.apply_optimization_hints(pattern, hints)

      assert optimized_pattern == pattern
      assert graph_binding == %{g: 123}
    end

    test "returns nil graph binding when bind_graph is false" do
      pattern = {:bgp, [{:triple, {:variable, "s"}, {:bound, 42}, {:bound, 35}}]}
      hints = []

      {optimized_pattern, graph_binding} =
        GraphClauseOptimization.apply_optimization_hints(pattern, hints)

      assert optimized_pattern == pattern
      assert graph_binding == nil
    end
  end
end
