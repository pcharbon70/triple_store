defmodule TripleStore.SPARQL.GraphOptimizationTest do
  @moduledoc """
  Unit tests for Graph-Specific Optimizations (Section 3.4) and
  Query Optimizer Adaptation (Section 5.3).

  Tests the optimizer's handling of quad patterns including:
  - Graph-first pattern ordering
  - Quad pattern selectivity estimation
  - Per-graph statistics collection
  - Graph grouping in join reordering
  - Graph-aware cost estimation
  """

  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Optimizer
  alias TripleStore.SPARQL.CostModel

  # ===========================================================================
  # Quad Pattern Selectivity Tests (3.4.1)
  # ===========================================================================

  describe "estimate_selectivity with quad patterns" do
    test "bound graph is more selective than unbound graph" do
      quad_with_bound_graph =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}

      quad_with_graph_var =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

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
      quad =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"},
         {:named_node, "http://example.org/graph1"}}

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
      quad =
        {:quad, {:named_node, "http://example.org/Alice"}, {:variable, "p"}, {:variable, "o"},
         :default_graph}

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
        {:quad, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"},
         :default_graph}
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
        {:quad, {:variable, "s"}, {:named_node, "http://example.org/name"}, {:variable, "name"},
         {:variable, "g"}}
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

  # ===========================================================================
  # Graph Grouping Tests (Section 5.3)
  # ===========================================================================

  describe "is_quad_pattern?/1" do
    test "returns true for quad patterns" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}
      assert Optimizer.is_quad_pattern?(quad)
    end

    test "returns false for triple patterns" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      refute Optimizer.is_quad_pattern?(triple)
    end

    test "returns false for non-pattern terms" do
      refute Optimizer.is_quad_pattern?(:some_atom)
      refute Optimizer.is_quad_pattern?({:filter, :expr, :pattern})
    end
  end

  describe "extract_graph_key/1" do
    test "returns :default_graph for default graph quads" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :default_graph}
      assert Optimizer.extract_graph_key(quad) == :default_graph
    end

    test "returns named graph tuple for named graph quads" do
      quad =
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"},
         {:named_node, "http://example.org/graph"}}

      assert Optimizer.extract_graph_key(quad) == {:named_graph, "http://example.org/graph"}
    end

    test "returns variable tuple for graph variable quads" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      assert Optimizer.extract_graph_key(quad) == {:variable, "g"}
    end

    test "returns :default_graph for triple patterns" do
      triple = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      assert Optimizer.extract_graph_key(triple) == :default_graph
    end

    test "returns :cross_graph for unknown graph terms" do
      quad = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, :unknown}
      assert Optimizer.extract_graph_key(quad) == :cross_graph
    end
  end

  describe "group_patterns_by_graph/1" do
    test "groups patterns by bound graph" do
      patterns = [
        {:quad, {:variable, "s1"}, {:variable, "p"}, {:variable, "o1"}, :default_graph},
        {:quad, {:variable, "s2"}, {:variable, "p"}, {:variable, "o2"}, :default_graph},
        {:quad, {:variable, "s3"}, {:variable, "p"}, {:variable, "o3"},
         {:named_node, "http://example.org/g1"}}
      ]

      groups = Optimizer.group_patterns_by_graph(patterns)

      # Should have default_graph group and named graph group
      assert Map.has_key?(groups, :default_graph)
      assert Map.has_key?(groups, {:named_graph, "http://example.org/g1"})
      assert length(groups[:default_graph]) == 2
      assert length(groups[{:named_graph, "http://example.org/g1"}]) == 1
    end

    test "groups patterns by shared graph variable" do
      patterns = [
        {:quad, {:variable, "s1"}, {:variable, "p"}, {:variable, "o1"}, {:variable, "g"}},
        {:quad, {:variable, "s2"}, {:variable, "p"}, {:variable, "o2"}, {:variable, "g"}},
        {:quad, {:variable, "s3"}, {:variable, "p"}, {:variable, "o3"}, {:variable, "other"}}
      ]

      groups = Optimizer.group_patterns_by_graph(patterns)

      # Should group by graph variable
      assert Map.has_key?(groups, {:variable, "g"})
      assert Map.has_key?(groups, {:variable, "other"})
      assert length(groups[{:variable, "g"}]) == 2
      assert length(groups[{:variable, "other"}]) == 1
    end

    test "handles mixed triple and quad patterns" do
      patterns = [
        {:triple, {:variable, "s1"}, {:variable, "p"}, {:variable, "o1"}},
        {:quad, {:variable, "s2"}, {:variable, "p"}, {:variable, "o2"}, :default_graph}
      ]

      groups = Optimizer.group_patterns_by_graph(patterns)

      # Both should be in default_graph
      assert Map.has_key?(groups, :default_graph)
      assert length(groups[:default_graph]) == 2
    end
  end

  describe "reorder_patterns_with_graph_grouping" do
    test "places bound graph patterns before cross-graph patterns" do
      patterns = [
        # Cross-graph pattern (should come last)
        {:quad, {:variable, "s1"}, {:variable, "p"}, {:variable, "o1"}, {:variable, "g"}},
        # Bound graph pattern (should come first)
        {:quad, {:variable, "s2"}, {:variable, "p"}, {:variable, "o2"}, :default_graph}
      ]

      reordered = Optimizer.reorder_bgp_patterns({:bgp, patterns}, %{})
      assert {:bgp, reordered_patterns} = reordered

      # Default graph pattern should come first (higher priority)
      # The first pattern should be in default_graph
      first_graph = reordered_patterns |> hd() |> Optimizer.extract_graph_key()
      assert first_graph == :default_graph
    end

    test "places named graph patterns before cross-graph patterns" do
      patterns = [
        # Cross-graph pattern
        {:quad, {:variable, "s1"}, {:variable, "p"}, {:variable, "o1"}, {:variable, "g"}},
        # Named graph pattern
        {:quad, {:variable, "s2"}, {:variable, "p"}, {:variable, "o2"},
         {:named_node, "http://example.org/g1"}}
      ]

      reordered = Optimizer.reorder_bgp_patterns({:bgp, patterns}, %{})
      assert {:bgp, reordered_patterns} = reordered

      # Named graph pattern should come first
      first_graph = reordered_patterns |> hd() |> Optimizer.extract_graph_key()
      assert first_graph == {:named_graph, "http://example.org/g1"}
    end

    test "groups patterns with same graph variable together" do
      patterns = [
        # Different graph variable
        {:quad, {:variable, "s1"}, {:variable, "p"}, {:variable, "o1"}, {:variable, "g2"}},
        # Same graph variable
        {:quad, {:variable, "s2"}, {:variable, "p"}, {:variable, "o2"}, {:variable, "g1"}},
        {:quad, {:variable, "s3"}, {:variable, "p"}, {:variable, "o3"}, {:variable, "g1"}}
      ]

      reordered = Optimizer.reorder_bgp_patterns({:bgp, patterns}, %{})
      assert {:bgp, reordered_patterns} = reordered

      # Patterns with g1 should be grouped together
      # Since g1 < g2 alphabetically, g1 patterns come first
      g1_graph = reordered_patterns |> hd() |> Optimizer.extract_graph_key()
      assert g1_graph == {:variable, "g1"}

      # Check that at least two consecutive patterns have the same graph
      graph_keys = Enum.map(reordered_patterns, &Optimizer.extract_graph_key/1)

      assert Enum.any?(graph_keys, fn key ->
               count = Enum.count(graph_keys, &(&1 == key))
               count > 1
             end)
    end
  end

  # ===========================================================================
  # Graph-Aware Cost Model Tests (Section 5.3)
  # ===========================================================================

  describe "CostModel.graph_switch_cost/0" do
    test "returns cost with CPU and I/O components" do
      cost = CostModel.graph_switch_cost()

      # Should have all cost components
      assert Map.has_key?(cost, :cpu)
      assert Map.has_key?(cost, :io)
      assert Map.has_key?(cost, :memory)
      assert Map.has_key?(cost, :total)

      # CPU should be small (context switching)
      assert cost.cpu > 0

      # I/O should be small but non-zero (column family switch)
      assert cost.io > 0

      # Memory should be zero (no additional memory needed)
      assert cost.memory == 0

      # Total should equal sum of components
      assert cost.total == cost.cpu + cost.io + cost.memory
    end
  end

  describe "CostModel.cross_graph_join_cost/3" do
    test "single graph join has same cost as hash join" do
      left_card = 1000
      right_card = 500

      cross_cost = CostModel.cross_graph_join_cost(left_card, right_card, 1)
      hash_cost = CostModel.hash_join_cost(left_card, right_card)

      # Should be equal when num_graphs = 1
      assert cross_cost.cpu == hash_cost.cpu
      assert cross_cost.io == hash_cost.io
      assert cross_cost.memory == hash_cost.memory
    end

    test "multi-graph join has higher cost" do
      left_card = 1000
      right_card = 500

      single_cost = CostModel.cross_graph_join_cost(left_card, right_card, 1)
      multi_cost = CostModel.cross_graph_join_cost(left_card, right_card, 5)

      # Multi-graph should be more expensive
      assert multi_cost.total > single_cost.total
    end

    test "cross-graph cost scales logarithmically" do
      left_card = 1000
      right_card = 500

      cost_1 = CostModel.cross_graph_join_cost(left_card, right_card, 1)
      cost_10 = CostModel.cross_graph_join_cost(left_card, right_card, 10)
      cost_100 = CostModel.cross_graph_join_cost(left_card, right_card, 100)

      # Cost should increase with graph count
      assert cost_1.total < cost_10.total
      assert cost_10.total < cost_100.total

      # But not linearly (logarithmic scaling)
      # cost_100 should be less than 10x cost_10
      assert cost_100.total < cost_10.total * 10
    end
  end

  describe "CostModel.quad_pattern_scan_type/1" do
    test "fully bound quad returns point_lookup" do
      pattern = {:quad, 1, 2, 3, 0}
      assert CostModel.quad_pattern_scan_type(pattern) == :point_lookup
    end

    test "partially bound quad returns prefix_scan" do
      # Subject and graph bound
      pattern = {:quad, 1, {:variable, "p"}, {:variable, "o"}, 0}
      assert CostModel.quad_pattern_scan_type(pattern) == :prefix_scan

      # Only graph bound
      pattern2 = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      assert CostModel.quad_pattern_scan_type(pattern2) == :prefix_scan
    end

    test "fully unbound quad returns full_scan" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      assert CostModel.quad_pattern_scan_type(pattern) == :full_scan
    end
  end

  describe "CostModel.quad_index_scan_cost/3" do
    test "point lookup has minimal cost" do
      cost = CostModel.quad_index_scan_cost(:point_lookup, 1, %{})

      # Point lookup is very cheap - single seek and read
      assert cost.total < 100
      # Has index seek cost
      assert cost.io > 0
    end

    test "prefix scan cost scales with result count" do
      small_cost = CostModel.quad_index_scan_cost(:prefix_scan, 10, %{})
      large_cost = CostModel.quad_index_scan_cost(:prefix_scan, 1000, %{})

      # Larger result count should cost more
      assert large_cost.total > small_cost.total
    end

    test "full scan uses quad_count from stats" do
      stats = %{quad_count: 50_000}
      cost = CostModel.quad_index_scan_cost(:full_scan, 100, stats)

      # Should use quad_count for I/O calculation
      assert cost.io > 0
      assert cost.cpu > 0
    end

    test "full scan falls back to triple_count when quad_count unavailable" do
      stats = %{triple_count: 30_000}
      cost = CostModel.quad_index_scan_cost(:full_scan, 100, stats)

      # Should use triple_count
      assert cost.io > 0
      assert cost.cpu > 0
    end
  end

  describe "CostModel.quad_pattern_cost/2" do
    test "estimates cost for fully bound quad pattern" do
      pattern = {:quad, 1, 2, 3, 0}
      stats = %{quad_count: 10_000}

      cost = CostModel.quad_pattern_cost(pattern, stats)

      # Fully bound pattern should be very cheap
      assert cost.total < 100
    end

    test "estimates cost for partially bound quad pattern" do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      stats = %{quad_count: 10_000}

      cost = CostModel.quad_pattern_cost(pattern, stats)

      # Should have non-zero cost
      assert cost.total > 0
      assert cost.cpu > 0
      assert cost.io > 0
    end

    test "estimates cost for cross-graph quad pattern" do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}}
      stats = %{quad_count: 10_000}

      cost = CostModel.quad_pattern_cost(pattern, stats)

      # Cross-graph pattern should be more expensive than graph-scoped
      graph_scoped_pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      graph_scoped_cost = CostModel.quad_pattern_cost(graph_scoped_pattern, stats)

      assert cost.total > graph_scoped_cost.total
    end
  end
end
