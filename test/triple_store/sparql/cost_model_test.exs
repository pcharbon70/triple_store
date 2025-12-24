defmodule TripleStore.SPARQL.CostModelTest do
  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.CostModel

  # ===========================================================================
  # Test Fixtures
  # ===========================================================================

  @small_stats %{
    triple_count: 1_000,
    distinct_subjects: 100,
    distinct_predicates: 10,
    distinct_objects: 200,
    predicate_histogram: %{1 => 100, 2 => 200, 3 => 50}
  }

  @medium_stats %{
    triple_count: 100_000,
    distinct_subjects: 10_000,
    distinct_predicates: 100,
    distinct_objects: 20_000,
    predicate_histogram: %{1 => 10_000, 2 => 20_000, 3 => 5_000}
  }

  @large_stats %{
    triple_count: 10_000_000,
    distinct_subjects: 1_000_000,
    distinct_predicates: 500,
    distinct_objects: 2_000_000,
    predicate_histogram: %{1 => 1_000_000, 2 => 2_000_000}
  }

  # ===========================================================================
  # Nested Loop Join Cost Tests
  # ===========================================================================

  describe "nested_loop_cost/2" do
    test "returns cost structure with all components" do
      cost = CostModel.nested_loop_cost(100, 50)

      assert is_map(cost)
      assert Map.has_key?(cost, :cpu)
      assert Map.has_key?(cost, :io)
      assert Map.has_key?(cost, :memory)
      assert Map.has_key?(cost, :total)
    end

    test "cpu cost is O(left * right)" do
      cost1 = CostModel.nested_loop_cost(100, 50)
      cost2 = CostModel.nested_loop_cost(200, 50)
      cost3 = CostModel.nested_loop_cost(100, 100)

      # Doubling left should double CPU cost
      assert_in_delta cost2.cpu / cost1.cpu, 2.0, 0.01

      # Doubling right should double CPU cost
      assert_in_delta cost3.cpu / cost1.cpu, 2.0, 0.01
    end

    test "memory cost is O(right)" do
      cost1 = CostModel.nested_loop_cost(100, 50)
      cost2 = CostModel.nested_loop_cost(100, 100)
      cost3 = CostModel.nested_loop_cost(200, 50)

      # Doubling right should double memory
      assert_in_delta cost2.memory / cost1.memory, 2.0, 0.01

      # Changing left shouldn't affect memory
      assert cost1.memory == cost3.memory
    end

    test "io cost is zero" do
      cost = CostModel.nested_loop_cost(1000, 500)
      assert cost.io == 0.0
    end

    test "total is sum of components" do
      cost = CostModel.nested_loop_cost(100, 50)
      assert_in_delta cost.total, cost.cpu + cost.io + cost.memory, 0.01
    end

    test "handles small inputs" do
      cost = CostModel.nested_loop_cost(1, 1)
      assert cost.cpu > 0
      assert cost.total > 0
    end

    test "handles large inputs" do
      cost = CostModel.nested_loop_cost(100_000, 50_000)
      assert cost.cpu > 0
      assert cost.total > cost.cpu  # Memory adds to total
    end
  end

  # ===========================================================================
  # Hash Join Cost Tests
  # ===========================================================================

  describe "hash_join_cost/2" do
    test "returns cost structure with all components" do
      cost = CostModel.hash_join_cost(100, 50)

      assert is_map(cost)
      assert Map.has_key?(cost, :cpu)
      assert Map.has_key?(cost, :io)
      assert Map.has_key?(cost, :memory)
      assert Map.has_key?(cost, :total)
    end

    test "cpu cost is O(left + right)" do
      cost1 = CostModel.hash_join_cost(100, 100)
      cost2 = CostModel.hash_join_cost(200, 200)

      # Doubling both should approximately double CPU cost
      ratio = cost2.cpu / cost1.cpu
      assert_in_delta ratio, 2.0, 0.1
    end

    test "memory cost is O(left)" do
      cost1 = CostModel.hash_join_cost(100, 50)
      cost2 = CostModel.hash_join_cost(200, 50)
      cost3 = CostModel.hash_join_cost(100, 100)

      # Doubling left should double memory
      assert_in_delta cost2.memory / cost1.memory, 2.0, 0.01

      # Changing right shouldn't affect memory
      assert cost1.memory == cost3.memory
    end

    test "io cost is zero" do
      cost = CostModel.hash_join_cost(1000, 500)
      assert cost.io == 0.0
    end

    test "is cheaper than nested loop for large inputs" do
      left = 10_000
      right = 5_000

      nl_cost = CostModel.nested_loop_cost(left, right)
      hj_cost = CostModel.hash_join_cost(left, right)

      # Hash join should be much cheaper
      assert hj_cost.total < nl_cost.total
      assert hj_cost.total < nl_cost.total / 100  # At least 100x cheaper
    end

    test "similar to nested loop for tiny inputs" do
      left = 5
      right = 3

      nl_cost = CostModel.nested_loop_cost(left, right)
      hj_cost = CostModel.hash_join_cost(left, right)

      # For tiny inputs, costs should be comparable (within 10x)
      ratio = nl_cost.total / hj_cost.total
      assert ratio < 10
    end
  end

  # ===========================================================================
  # Leapfrog Cost Tests
  # ===========================================================================

  describe "leapfrog_cost/3" do
    test "returns cost structure" do
      cost = CostModel.leapfrog_cost([100, 200, 150], ["x", "y"], @small_stats)

      assert is_map(cost)
      assert Map.has_key?(cost, :cpu)
      assert Map.has_key?(cost, :io)
      assert Map.has_key?(cost, :memory)
      assert Map.has_key?(cost, :total)
    end

    test "returns infinity for single pattern" do
      cost = CostModel.leapfrog_cost([100], ["x"], @small_stats)
      assert cost.cpu == Float.max_finite()
    end

    test "memory is O(k) where k is number of patterns" do
      cost2 = CostModel.leapfrog_cost([100, 200], ["x"], @small_stats)
      cost3 = CostModel.leapfrog_cost([100, 200, 150], ["x"], @small_stats)
      cost4 = CostModel.leapfrog_cost([100, 200, 150, 80], ["x", "y"], @small_stats)

      # Memory should scale linearly with number of patterns
      assert cost3.memory > cost2.memory
      assert cost4.memory > cost3.memory
    end

    test "cost scales reasonably with patterns" do
      # With same join variables, more patterns generally increase cost
      # (though this depends on selectivity)
      cost2 = CostModel.leapfrog_cost([1000, 1000], ["x"], @medium_stats)
      cost3 = CostModel.leapfrog_cost([1000, 1000, 1000], ["x"], @medium_stats)
      cost4 = CostModel.leapfrog_cost([1000, 1000, 1000, 1000], ["x"], @medium_stats)

      # All costs should be positive and finite
      assert cost2.total > 0
      assert cost3.total > 0
      assert cost4.total > 0

      # More patterns with same join var should increase memory (for iterators)
      assert cost3.memory > cost2.memory
      assert cost4.memory > cost3.memory
    end

    test "more join variables reduce output estimate" do
      # More join vars = more selective = smaller output
      cost_one_var = CostModel.leapfrog_cost([1000, 1000, 1000], ["x"], @medium_stats)
      cost_two_vars = CostModel.leapfrog_cost([1000, 1000, 1000], ["x", "y"], @medium_stats)

      # With more join variables, selectivity is higher, output is smaller
      # This should result in lower CPU cost
      assert cost_two_vars.cpu <= cost_one_var.cpu
    end

    test "handles empty join variables" do
      # No join vars = Cartesian product
      cost = CostModel.leapfrog_cost([100, 100, 100], [], @small_stats)

      # Should still return a valid cost
      assert cost.total > 0
    end
  end

  # ===========================================================================
  # Index Scan Cost Tests
  # ===========================================================================

  describe "index_scan_cost/3" do
    test "point lookup has fixed low cost" do
      cost = CostModel.index_scan_cost(:point_lookup, 1, @small_stats)

      assert cost.io > 0  # Seek cost
      assert cost.cpu > 0  # Comparison cost
      assert cost.memory > 0  # Minimal memory
    end

    test "prefix scan cost scales with results" do
      cost1 = CostModel.index_scan_cost(:prefix_scan, 100, @small_stats)
      cost2 = CostModel.index_scan_cost(:prefix_scan, 1000, @small_stats)

      # CPU should scale with results
      assert cost2.cpu > cost1.cpu
      assert_in_delta cost2.cpu / cost1.cpu, 10.0, 1.0
    end

    test "full scan is most expensive" do
      point_cost = CostModel.index_scan_cost(:point_lookup, 1, @medium_stats)
      prefix_cost = CostModel.index_scan_cost(:prefix_scan, 1000, @medium_stats)
      full_cost = CostModel.index_scan_cost(:full_scan, 100_000, @medium_stats)

      assert full_cost.total > prefix_cost.total
      assert prefix_cost.total > point_cost.total
    end

    test "full scan uses triple_count from stats" do
      cost_small = CostModel.index_scan_cost(:full_scan, 1000, @small_stats)
      cost_large = CostModel.index_scan_cost(:full_scan, 1000, @large_stats)

      # Full scan should be more expensive with larger database
      assert cost_large.total > cost_small.total
    end
  end

  # ===========================================================================
  # Pattern Scan Type Tests
  # ===========================================================================

  describe "pattern_scan_type/1" do
    test "fully bound pattern is point lookup" do
      pattern = {:triple, 1, 2, 3}
      assert CostModel.pattern_scan_type(pattern) == :point_lookup
    end

    test "partially bound patterns are prefix scan" do
      # S bound
      assert CostModel.pattern_scan_type({:triple, 1, {:variable, "p"}, {:variable, "o"}}) == :prefix_scan

      # P bound
      assert CostModel.pattern_scan_type({:triple, {:variable, "s"}, 2, {:variable, "o"}}) == :prefix_scan

      # O bound
      assert CostModel.pattern_scan_type({:triple, {:variable, "s"}, {:variable, "p"}, 3}) == :prefix_scan

      # SP bound
      assert CostModel.pattern_scan_type({:triple, 1, 2, {:variable, "o"}}) == :prefix_scan

      # PO bound
      assert CostModel.pattern_scan_type({:triple, {:variable, "s"}, 2, 3}) == :prefix_scan

      # SO bound
      assert CostModel.pattern_scan_type({:triple, 1, {:variable, "p"}, 3}) == :prefix_scan
    end

    test "unbound pattern is full scan" do
      pattern = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}
      assert CostModel.pattern_scan_type(pattern) == :full_scan
    end

    test "handles blank nodes as unbound" do
      pattern = {:triple, {:blank_node, "_:b1"}, {:variable, "p"}, {:variable, "o"}}
      assert CostModel.pattern_scan_type(pattern) == :full_scan
    end
  end

  # ===========================================================================
  # Pattern Cost Tests
  # ===========================================================================

  describe "pattern_cost/2" do
    test "fully bound pattern has lowest cost" do
      bound = {:triple, 1, 2, 3}
      partial = {:triple, 1, {:variable, "p"}, {:variable, "o"}}
      unbound = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}

      bound_cost = CostModel.pattern_cost(bound, @medium_stats)
      partial_cost = CostModel.pattern_cost(partial, @medium_stats)
      unbound_cost = CostModel.pattern_cost(unbound, @medium_stats)

      assert bound_cost.total < partial_cost.total
      assert partial_cost.total < unbound_cost.total
    end

    test "S?O pattern has post-filter penalty" do
      # S?O requires reading from OSP and filtering
      so_pattern = {:triple, 1, {:variable, "p"}, 3}
      sp_pattern = {:triple, 1, 2, {:variable, "o"}}

      so_cost = CostModel.pattern_cost(so_pattern, @medium_stats)
      sp_cost = CostModel.pattern_cost(sp_pattern, @medium_stats)

      # S?O should have higher CPU due to post-filtering
      assert so_cost.cpu > sp_cost.cpu
    end
  end

  # ===========================================================================
  # Strategy Selection Tests
  # ===========================================================================

  describe "select_join_strategy/4" do
    test "returns strategy and cost tuple" do
      {strategy, cost} = CostModel.select_join_strategy(100, 50, ["x"], @small_stats)

      assert strategy in [:nested_loop, :hash_join]
      assert is_map(cost)
      assert Map.has_key?(cost, :total)
    end

    test "selects nested loop for small inputs" do
      {strategy, _cost} = CostModel.select_join_strategy(10, 10, ["x"], @small_stats)
      assert strategy == :nested_loop
    end

    test "selects hash join for large inputs" do
      {strategy, _cost} = CostModel.select_join_strategy(10_000, 5_000, ["x"], @medium_stats)
      assert strategy == :hash_join
    end

    test "considers asymmetric inputs" do
      # Very small left, large right - might still prefer nested loop
      {strategy1, _} = CostModel.select_join_strategy(5, 10_000, ["x"], @medium_stats)

      # Large left, very small right
      {strategy2, _} = CostModel.select_join_strategy(10_000, 5, ["x"], @medium_stats)

      # Both should select hash join for efficiency
      assert strategy1 == :hash_join
      assert strategy2 == :hash_join
    end
  end

  # ===========================================================================
  # Leapfrog Selection Tests
  # ===========================================================================

  describe "should_use_leapfrog?/3" do
    test "returns false for fewer than 3 patterns" do
      refute CostModel.should_use_leapfrog?([100], ["x"], @small_stats)
      refute CostModel.should_use_leapfrog?([100, 200], ["x"], @small_stats)
    end

    test "returns boolean for 3+ patterns" do
      result = CostModel.should_use_leapfrog?([100, 200, 150], ["x", "y"], @small_stats)
      assert is_boolean(result)
    end

    test "considers pattern cardinalities" do
      # With high cardinalities, Leapfrog may be better
      result_large = CostModel.should_use_leapfrog?(
        [10_000, 10_000, 10_000, 10_000],
        ["x", "y", "z"],
        @medium_stats
      )

      # The result depends on the actual cost comparison
      assert is_boolean(result_large)
    end
  end

  # ===========================================================================
  # Cost Utility Tests
  # ===========================================================================

  describe "total_plan_cost/1" do
    test "sums multiple costs" do
      cost1 = CostModel.nested_loop_cost(100, 50)
      cost2 = CostModel.hash_join_cost(200, 100)
      cost3 = CostModel.index_scan_cost(:prefix_scan, 500, @small_stats)

      total = CostModel.total_plan_cost([cost1, cost2, cost3])

      expected_total = cost1.total + cost2.total + cost3.total
      assert_in_delta total.total, expected_total, 0.01
    end

    test "handles empty list" do
      total = CostModel.total_plan_cost([])
      assert total.total == 0.0
    end

    test "handles single cost" do
      cost = CostModel.hash_join_cost(100, 50)
      total = CostModel.total_plan_cost([cost])

      assert_in_delta total.total, cost.total, 0.01
    end
  end

  describe "compare_costs/2" do
    test "returns :lt when first is cheaper" do
      cheaper = CostModel.hash_join_cost(100, 50)
      expensive = CostModel.nested_loop_cost(1000, 500)

      assert CostModel.compare_costs(cheaper, expensive) == :lt
    end

    test "returns :gt when first is more expensive" do
      expensive = CostModel.nested_loop_cost(1000, 500)
      cheaper = CostModel.hash_join_cost(100, 50)

      assert CostModel.compare_costs(expensive, cheaper) == :gt
    end

    test "returns :eq for identical costs" do
      cost1 = CostModel.hash_join_cost(100, 50)
      cost2 = CostModel.hash_join_cost(100, 50)

      assert CostModel.compare_costs(cost1, cost2) == :eq
    end
  end

  # ===========================================================================
  # Cost Model Ranking Tests
  # ===========================================================================

  describe "cost model correctly ranks plans" do
    test "point lookup < prefix scan < full scan for same result count" do
      results = 100

      point = CostModel.index_scan_cost(:point_lookup, results, @medium_stats)
      prefix = CostModel.index_scan_cost(:prefix_scan, results, @medium_stats)
      full = CostModel.index_scan_cost(:full_scan, results, @medium_stats)

      assert CostModel.compare_costs(point, prefix) == :lt
      assert CostModel.compare_costs(prefix, full) == :lt
    end

    test "hash join beats nested loop for quadratic inputs" do
      # 1000 x 1000 = 1M comparisons for NL
      nl = CostModel.nested_loop_cost(1000, 1000)
      hj = CostModel.hash_join_cost(1000, 1000)

      assert CostModel.compare_costs(hj, nl) == :lt
    end

    test "nested loop acceptable for very small inputs" do
      # 10 x 10 = 100 comparisons
      nl = CostModel.nested_loop_cost(10, 10)
      hj = CostModel.hash_join_cost(10, 10)

      # For small inputs, difference shouldn't be dramatic
      ratio = nl.total / hj.total
      assert ratio < 10  # Within an order of magnitude
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles zero cardinality" do
      cost = CostModel.nested_loop_cost(0, 100)
      assert cost.cpu == 0.0
    end

    test "handles very large cardinalities" do
      cost = CostModel.hash_join_cost(1_000_000, 500_000)
      assert cost.total > 0
      assert is_float(cost.total)
    end

    test "handles empty stats" do
      cost = CostModel.index_scan_cost(:full_scan, 1000, %{})
      assert cost.total > 0
    end

    test "handles stats with missing keys" do
      partial_stats = %{triple_count: 1000}
      cost = CostModel.leapfrog_cost([100, 200, 150], ["x"], partial_stats)
      assert cost.total > 0
    end

    test "pattern cost handles integer IDs" do
      pattern = {:triple, 12345, 67890, 11111}
      cost = CostModel.pattern_cost(pattern, @medium_stats)
      assert cost.total > 0
    end
  end

  # ===========================================================================
  # Integration Scenario Tests
  # ===========================================================================

  describe "integration scenarios" do
    test "star query optimization" do
      # Star query: ?x :p1 ?y . ?x :p2 ?z . ?x :p3 ?w
      # All patterns share ?x

      # Pattern cardinalities
      p1_card = 5000
      p2_card = 3000
      p3_card = 8000

      # Compare pairwise hash join cascade vs Leapfrog
      hj1 = CostModel.hash_join_cost(p1_card, p2_card)
      intermediate_card = 1500  # Estimated join result
      hj2 = CostModel.hash_join_cost(intermediate_card, p3_card)
      cascade_total = CostModel.total_plan_cost([hj1, hj2])

      lf_cost = CostModel.leapfrog_cost([p1_card, p2_card, p3_card], ["x"], @medium_stats)

      # Both should have reasonable costs
      assert cascade_total.total > 0
      assert lf_cost.total > 0
    end

    test "chain query optimization" do
      # Chain query: ?x :p1 ?y . ?y :p2 ?z
      # Only ?y is shared

      p1_card = 5000
      p2_card = 5000

      # For chain with only one shared variable, hash join should be efficient
      {strategy, cost} = CostModel.select_join_strategy(p1_card, p2_card, ["y"], @medium_stats)

      assert strategy == :hash_join
      assert cost.total > 0
    end

    test "selective pattern first optimization" do
      # Pattern with bound predicate is more selective
      selective = {:triple, {:variable, "s"}, 42, {:variable, "o"}}
      general = {:triple, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}}

      selective_cost = CostModel.pattern_cost(selective, @medium_stats)
      general_cost = CostModel.pattern_cost(general, @medium_stats)

      # Selective pattern should be cheaper to scan
      assert CostModel.compare_costs(selective_cost, general_cost) == :lt
    end
  end
end
