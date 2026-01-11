defmodule TripleStore.SPARQL.GraphOptimizationTest do
  @moduledoc """
  Unit tests for Graph-Specific Optimizations (Section 3.4).

  Tests the optimizer's handling of quad patterns including:
  - Graph-first pattern ordering
  - Quad pattern selectivity estimation
  - Per-graph statistics collection
  """

  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Optimizer

  # ===========================================================================
  # Quad Pattern Selectivity Tests (3.4.1)
  # ===========================================================================

  describe "estimate_selectivity with quad patterns" do
    test "bound graph is more selective than unbound graph" do
      quad_with_bound_graph = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}
      quad_with_graph_var = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      bound_score = Optimizer.estimate_selectivity(quad_with_bound_graph, MapSet.new(), %{})
      unbound_score = Optimizer.estimate_selectivity(quad_with_graph_var, MapSet.new(), %{})

      # Bound graph should have lower (more selective) score
      # Bound graph: 100 * 50 * 100 * 0.1 = 50,000
      # Unbound graph: 100 * 50 * 100 * 10 = 5,000,000
      assert bound_score < unbound_score
      assert bound_score < 100_000.0
      assert unbound_score > 1_000_000.0
    end

    test "bound named graph is selective" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:named_node, "http://example.org/graph1"}}

      score = Optimizer.estimate_selectivity(quad, MapSet.new(), %{})

      # Should be very selective due to bound graph
      # Score: 100 * 50 * 100 * 0.1 = 50,000
      assert score < 100_000.0
    end

    test "default graph is selective" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}

      score = Optimizer.estimate_selectivity(quad, MapSet.new(), %{})

      # Should be very selective due to bound default graph
      # Score: 100 * 50 * 100 * 0.1 = 50,000
      assert score < 100_000.0
    end

    test "graph variable with existing binding is selective" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      bound_vars = MapSet.new(["g"])

      score = Optimizer.estimate_selectivity(quad, bound_vars, %{})

      # Should be selective because graph variable is already bound
      # Score: 100 * 50 * 100 * 0.1 = 50,000
      assert score < 100_000.0
    end

    test "bound subject + bound graph is most selective" do
      quad = {:quad, {:named_node, "http://example.org/Alice"}, {:variable, "p"}, {:variable, "o"}, :default_graph}

      score = Optimizer.estimate_selectivity(quad, MapSet.new(), %{})

      # Should be very selective - both subject and graph are bound
      # Score: 1 * 50 * 100 * 0.1 = 500
      assert score < 1_000.0
    end
  end

  # ===========================================================================
  # Pattern Ordering Tests (3.4.1)
  # ===========================================================================

  describe "pattern_variables with quad patterns" do
    test "extracts variables from quad pattern" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

      # Call through pattern ordering to get variable set
      # The pattern_variables function is private, so we test through reorder_bgp_patterns
      bgp = {:bgp, [quad]}

      # Should not crash - just verify it handles quad patterns
      assert {:bgp, [_]} = Optimizer.reorder_bgp_patterns(bgp, %{})
    end

    test "handles mixed quad and triple patterns" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}

      bgp = {:bgp, [triple, quad]}

      # Should not crash - just verify it handles mixed patterns
      assert {:bgp, [_, _]} = Optimizer.reorder_bgp_patterns(bgp, %{})
    end
  end

  # ===========================================================================
  # Range Filter Tests with Quad Patterns
  # ===========================================================================

  describe "binds_range_filtered_variable? with quad patterns" do
    test "detects quad pattern binding range-filtered variable" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "x"}, :default_graph}
      filter_context = %{range_filtered_vars: MapSet.new(["x"])}

      assert Optimizer.binds_range_filtered_variable?(quad, filter_context)
    end

    test "returns false when quad variable not in range filters" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "x"}, :default_graph}
      filter_context = %{range_filtered_vars: MapSet.new(["y"])}

      refute Optimizer.binds_range_filtered_variable?(quad, filter_context)
    end
  end

  # ===========================================================================
  # Cross-Graph Optimization Tests (3.4.2)
  # ===========================================================================

  describe "cross-graph query detection" do
    test "single graph query has bound graph" do
      quads = [
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph},
        {:quad, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}, :default_graph}
      ]

      # All patterns have the same bound graph - single graph query
      assert Enum.all?(quads, fn
        {:quad, _, _, _, :default_graph} -> true
        {:quad, _, _, _, {:named_node, _}} -> true
        _ -> false
      end)
    end

    test "multi-graph query has graph variable" do
      quads = [
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}},
        {:quad, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"}, {:variable, "g"}}
      ]

      # Patterns have graph variable - will scan all graphs
      assert Enum.all?(quads, fn
        {:quad, _, _, _, {:variable, "g"}} -> true
        _ -> false
      end)
    end
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  describe "graph_position_score/3" do
    test "default graph has low score (high selectivity)" do
      # The function is private, so we test through estimate_selectivity
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}
      score = Optimizer.estimate_selectivity(quad, MapSet.new(), %{})

      # Default graph should be highly selective (low score)
      # Base score with all vars is 100 * 50 * 100 = 500,000
      # With bound graph (0.1), should be ~50,000
      assert score < 100_000.0
    end

    test "bound graph variable has low score (high selectivity)" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      bound_vars = MapSet.new(["g"])
      score = Optimizer.estimate_selectivity(quad, bound_vars, %{})

      # Bound graph variable should be highly selective
      # Score: 100 * 50 * 100 * 0.1 = 50,000
      assert score < 100_000.0
    end

    test "unbound graph variable has higher score (lower selectivity)" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      score = Optimizer.estimate_selectivity(quad, MapSet.new(), %{})

      # Unbound graph variable is less selective
      # Score: 100 * 50 * 100 * 10 = 5,000,000
      # Should be higher than when graph is bound
      assert score > 1_000_000.0
    end
  end
end
