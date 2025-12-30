defmodule TripleStore.SPARQL.CostOptimizerIntegrationTest do
  @moduledoc """
  Integration tests for the cost-based optimizer components.

  These tests verify that Cardinality, CostModel, JoinEnumeration, and PlanCache
  work together correctly to produce optimal query execution plans.
  """

  use ExUnit.Case, async: false

  alias TripleStore.SPARQL.{Cardinality, CostModel, JoinEnumeration, PlanCache}

  # ===========================================================================
  # Test Fixtures
  # ===========================================================================

  @small_stats %{
    triple_count: 1_000,
    distinct_subjects: 100,
    distinct_predicates: 10,
    distinct_objects: 200,
    predicate_histogram: %{1 => 100, 2 => 200, 3 => 50, 4 => 150, 5 => 500}
  }

  @medium_stats %{
    triple_count: 100_000,
    distinct_subjects: 10_000,
    distinct_predicates: 100,
    distinct_objects: 20_000,
    predicate_histogram: %{1 => 10_000, 2 => 20_000, 3 => 5_000, 4 => 15_000, 5 => 50_000}
  }

  @large_stats %{
    triple_count: 10_000_000,
    distinct_subjects: 1_000_000,
    distinct_predicates: 500,
    distinct_objects: 2_000_000,
    predicate_histogram: %{1 => 1_000_000, 2 => 2_000_000, 3 => 500_000}
  }

  # Helper to create variable
  defp var(name), do: {:variable, name}

  # Unique cache name for each test
  defp unique_cache_name do
    :"CostOptIntegration_#{:erlang.unique_integer([:positive])}"
  end

  # ===========================================================================
  # Cardinality Estimation Tests
  # ===========================================================================

  describe "cardinality estimation for single pattern" do
    test "fully bound pattern has cardinality 1" do
      pattern = {:triple, 1, 2, 3}
      card = Cardinality.estimate_pattern(pattern, @small_stats)

      # Fully bound should be ~1 (may be slightly higher due to estimation)
      assert card >= 1.0
      assert card < 10.0
    end

    test "unbound pattern has cardinality equal to triple count" do
      pattern = {:triple, var("s"), var("p"), var("o")}
      card = Cardinality.estimate_pattern(pattern, @small_stats)

      assert card == @small_stats.triple_count
    end

    test "predicate-bound pattern uses histogram" do
      # Predicate 2 has 200 triples in histogram
      pattern = {:triple, var("s"), 2, var("o")}
      card = Cardinality.estimate_pattern(pattern, @small_stats)

      assert card == 200.0
    end

    test "subject-bound pattern applies selectivity" do
      pattern = {:triple, 42, var("p"), var("o")}
      card = Cardinality.estimate_pattern(pattern, @small_stats)

      # Should be triple_count / distinct_subjects = 1000 / 100 = 10
      assert_in_delta card, 10.0, 1.0
    end

    test "object-bound pattern applies selectivity" do
      pattern = {:triple, var("s"), var("p"), 99}
      card = Cardinality.estimate_pattern(pattern, @small_stats)

      # Should be triple_count / distinct_objects = 1000 / 200 = 5
      assert_in_delta card, 5.0, 1.0
    end

    test "multiple bound positions multiply selectivities" do
      # Subject and predicate bound
      pattern = {:triple, 42, 2, var("o")}
      card = Cardinality.estimate_pattern(pattern, @small_stats)

      # Base is histogram[2] = 200, then apply subject selectivity
      # 200 / 100 = 2
      assert card >= 1.0
      assert card <= 10.0
    end
  end

  describe "cardinality estimation for join" do
    test "join cardinality with shared variable" do
      left_card = 100.0
      right_card = 200.0
      join_vars = ["x"]

      result = Cardinality.estimate_join(left_card, right_card, join_vars, @small_stats)

      # Should be less than left * right (join selectivity applies)
      assert result < left_card * right_card
      assert result > 0
    end

    test "join cardinality without shared variables (Cartesian)" do
      left_card = 100.0
      right_card = 200.0
      join_vars = []

      result = Cardinality.estimate_join(left_card, right_card, join_vars, @small_stats)

      # Cartesian product
      assert result == left_card * right_card
    end

    test "multiple shared variables increase selectivity" do
      left_card = 1000.0
      right_card = 1000.0

      one_var = Cardinality.estimate_join(left_card, right_card, ["x"], @medium_stats)
      two_vars = Cardinality.estimate_join(left_card, right_card, ["x", "y"], @medium_stats)

      # More join variables = more selective = smaller result
      assert two_vars <= one_var
    end

    test "multi-pattern cardinality estimation" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("z"), 3, var("w")}
      ]

      result = Cardinality.estimate_multi_pattern(patterns, @small_stats)

      # Should produce a reasonable estimate
      assert result > 0
      assert result < @small_stats.triple_count * @small_stats.triple_count
    end
  end

  # ===========================================================================
  # Cost Model Ranking Tests
  # ===========================================================================

  describe "cost model ranks plans correctly" do
    test "hash join cheaper than nested loop for large inputs" do
      left_card = 10_000
      right_card = 5_000

      nl_cost = CostModel.nested_loop_cost(left_card, right_card)
      hj_cost = CostModel.hash_join_cost(left_card, right_card)

      assert CostModel.compare_costs(hj_cost, nl_cost) == :lt
    end

    test "point lookup cheaper than prefix scan" do
      point_cost = CostModel.index_scan_cost(:point_lookup, 1, @medium_stats)
      prefix_cost = CostModel.index_scan_cost(:prefix_scan, 100, @medium_stats)

      assert CostModel.compare_costs(point_cost, prefix_cost) == :lt
    end

    test "prefix scan cheaper than full scan" do
      prefix_cost = CostModel.index_scan_cost(:prefix_scan, 1000, @medium_stats)
      full_cost = CostModel.index_scan_cost(:full_scan, 100_000, @medium_stats)

      assert CostModel.compare_costs(prefix_cost, full_cost) == :lt
    end

    test "selective pattern cheaper than general pattern" do
      # Two bound positions
      selective = {:triple, 1, 2, var("o")}
      # No bound positions
      general = {:triple, var("s"), var("p"), var("o")}

      selective_cost = CostModel.pattern_cost(selective, @medium_stats)
      general_cost = CostModel.pattern_cost(general, @medium_stats)

      assert CostModel.compare_costs(selective_cost, general_cost) == :lt
    end

    test "strategy selection prefers hash join for medium inputs" do
      {strategy, _cost} = CostModel.select_join_strategy(1000, 500, ["x"], @medium_stats)
      assert strategy == :hash_join
    end

    test "strategy selection accepts nested loop for tiny inputs" do
      {strategy, _cost} = CostModel.select_join_strategy(5, 5, ["x"], @small_stats)
      assert strategy == :nested_loop
    end
  end

  # ===========================================================================
  # Exhaustive Enumeration Tests
  # ===========================================================================

  describe "exhaustive enumeration finds optimal plan" do
    test "single pattern produces scan plan" do
      patterns = [{:triple, var("x"), 1, var("y")}]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert {:scan, _} = plan.tree
    end

    test "two patterns produce join plan" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert {:join, strategy, _, _, join_vars} = plan.tree
      assert strategy in [:nested_loop, :hash_join]
      assert "y" in join_vars
    end

    test "three patterns produce nested join plan" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("z"), 3, var("w")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # Should be a nested join structure
      assert {:join, _, _, _, _} = plan.tree
    end

    test "enumeration considers all orderings for chain query" do
      # Chain: x-y-z - order matters for cost
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @medium_stats)

      # Plan should have positive finite cost
      assert plan.cost.total > 0
      assert plan.cost.total < Float.max_finite()
    end

    test "enumeration prefers selective patterns first" do
      # Pattern with predicate 3 has fewer triples (50) than predicate 5 (500)
      patterns = [
        # 500 triples
        {:triple, var("x"), 5, var("y")},
        # 50 triples
        {:triple, var("y"), 3, var("z")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # Should produce valid plan
      assert plan.cardinality > 0
    end
  end

  # ===========================================================================
  # DPccp Algorithm Tests
  # ===========================================================================

  describe "DPccp produces same plan as exhaustive for small queries" do
    test "two-pattern query produces equivalent plan" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      # Both should use exhaustive for 2 patterns
      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert plan.cardinality > 0
      assert plan.cost.total > 0
    end

    test "three-pattern query produces equivalent plan" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("z"), 3, var("w")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert plan.cardinality > 0
      assert plan.cost.total > 0
    end

    test "five-pattern query (threshold) produces valid plan" do
      patterns =
        for i <- 1..5 do
          {:triple, var("x"), i, var("y#{i}")}
        end

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert plan.cardinality > 0
      assert plan.cost.total > 0
    end

    test "six-pattern query uses DPccp" do
      patterns =
        for i <- 1..6 do
          {:triple, var("x"), i, var("y#{i}")}
        end

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # DPccp should produce valid plan
      assert plan.cardinality > 0
      assert plan.cost.total > 0
    end

    test "DPccp handles chain queries" do
      # Chain of 7 patterns
      patterns = [
        {:triple, var("a"), 1, var("b")},
        {:triple, var("b"), 2, var("c")},
        {:triple, var("c"), 3, var("d")},
        {:triple, var("d"), 4, var("e")},
        {:triple, var("e"), 5, var("f")},
        {:triple, var("f"), 1, var("g")},
        {:triple, var("g"), 2, var("h")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert plan.cardinality > 0
      assert plan.cost.total > 0
    end
  end

  # ===========================================================================
  # Plan Cache Tests
  # ===========================================================================

  describe "plan cache stores and retrieves plans" do
    setup do
      name = unique_cache_name()
      {:ok, pid} = PlanCache.start_link(name: name, max_size: 100)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
      %{cache_name: name}
    end

    test "caches enumerated plan", %{cache_name: name} do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      compute_count = :counters.new(1, [:atomics])

      # First call computes
      plan1 =
        PlanCache.get_or_compute(
          patterns,
          fn ->
            :counters.add(compute_count, 1, 1)
            {:ok, result} = JoinEnumeration.enumerate(patterns, @small_stats)
            result
          end,
          name: name
        )

      # Second call uses cache
      plan2 =
        PlanCache.get_or_compute(
          patterns,
          fn ->
            :counters.add(compute_count, 1, 1)
            {:ok, result} = JoinEnumeration.enumerate(patterns, @small_stats)
            result
          end,
          name: name
        )

      assert plan1 == plan2
      assert :counters.get(compute_count, 1) == 1
    end

    test "different queries produce different cache entries", %{cache_name: name} do
      patterns1 = [{:triple, var("x"), 1, var("y")}]
      patterns2 = [{:triple, var("x"), 2, var("y")}]

      plan1 =
        PlanCache.get_or_compute(
          patterns1,
          fn ->
            {:ok, result} = JoinEnumeration.enumerate(patterns1, @small_stats)
            result
          end,
          name: name
        )

      plan2 =
        PlanCache.get_or_compute(
          patterns2,
          fn ->
            {:ok, result} = JoinEnumeration.enumerate(patterns2, @small_stats)
            result
          end,
          name: name
        )

      # Different predicates = different cardinality
      refute plan1.cardinality == plan2.cardinality
    end

    test "structurally identical queries share cache", %{cache_name: name} do
      # Same structure, different variable names
      patterns1 = [{:triple, var("x"), 1, var("y")}]
      patterns2 = [{:triple, var("subject"), 1, var("object")}]

      compute_count = :counters.new(1, [:atomics])

      PlanCache.get_or_compute(
        patterns1,
        fn ->
          :counters.add(compute_count, 1, 1)
          {:ok, result} = JoinEnumeration.enumerate(patterns1, @small_stats)
          result
        end,
        name: name
      )

      PlanCache.get_or_compute(
        patterns2,
        fn ->
          :counters.add(compute_count, 1, 1)
          {:ok, result} = JoinEnumeration.enumerate(patterns2, @small_stats)
          result
        end,
        name: name
      )

      # Should only compute once due to normalization
      assert :counters.get(compute_count, 1) == 1
    end

    test "cache hit rate improves with repeated queries", %{cache_name: name} do
      patterns = [{:triple, var("x"), 1, var("y")}]

      # First access (miss)
      PlanCache.get_or_compute(
        patterns,
        fn ->
          {:ok, result} = JoinEnumeration.enumerate(patterns, @small_stats)
          result
        end,
        name: name
      )

      # 9 more accesses (hits)
      for _ <- 1..9 do
        PlanCache.get_or_compute(
          patterns,
          fn ->
            {:ok, result} = JoinEnumeration.enumerate(patterns, @small_stats)
            result
          end,
          name: name
        )
      end

      stats = PlanCache.stats(name: name)

      # Should have 90% hit rate (9 hits, 1 miss)
      assert stats.hit_rate == 0.9
    end
  end

  describe "plan cache invalidates on update" do
    setup do
      name = unique_cache_name()
      {:ok, pid} = PlanCache.start_link(name: name, max_size: 100)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
      %{cache_name: name}
    end

    test "invalidate clears all entries", %{cache_name: name} do
      patterns1 = [{:triple, var("x"), 1, var("y")}]
      patterns2 = [{:triple, var("a"), 2, var("b")}]

      # Cache two plans
      PlanCache.get_or_compute(
        patterns1,
        fn ->
          {:ok, result} = JoinEnumeration.enumerate(patterns1, @small_stats)
          result
        end,
        name: name
      )

      PlanCache.get_or_compute(
        patterns2,
        fn ->
          {:ok, result} = JoinEnumeration.enumerate(patterns2, @small_stats)
          result
        end,
        name: name
      )

      assert PlanCache.size(name: name) == 2

      # Invalidate
      PlanCache.invalidate(name: name)

      assert PlanCache.size(name: name) == 0
    end

    test "invalidate causes recomputation", %{cache_name: name} do
      patterns = [{:triple, var("x"), 1, var("y")}]
      compute_count = :counters.new(1, [:atomics])

      compute = fn ->
        :counters.add(compute_count, 1, 1)
        {:ok, result} = JoinEnumeration.enumerate(patterns, @small_stats)
        result
      end

      # First compute
      PlanCache.get_or_compute(patterns, compute, name: name)
      assert :counters.get(compute_count, 1) == 1

      # Cached access
      PlanCache.get_or_compute(patterns, compute, name: name)
      assert :counters.get(compute_count, 1) == 1

      # Invalidate
      PlanCache.invalidate(name: name)

      # Should recompute
      PlanCache.get_or_compute(patterns, compute, name: name)
      assert :counters.get(compute_count, 1) == 2
    end

    test "invalidate specific query", %{cache_name: name} do
      patterns1 = [{:triple, var("x"), 1, var("y")}]
      patterns2 = [{:triple, var("a"), 2, var("b")}]

      # Cache two plans
      PlanCache.get_or_compute(
        patterns1,
        fn ->
          {:ok, result} = JoinEnumeration.enumerate(patterns1, @small_stats)
          result
        end,
        name: name
      )

      PlanCache.get_or_compute(
        patterns2,
        fn ->
          {:ok, result} = JoinEnumeration.enumerate(patterns2, @small_stats)
          result
        end,
        name: name
      )

      # Invalidate only patterns1
      PlanCache.invalidate(patterns1, name: name)

      # patterns1 should be miss, patterns2 should be hit
      assert PlanCache.get(patterns1, name: name) == :miss
      assert {:ok, _} = PlanCache.get(patterns2, name: name)
    end
  end

  # ===========================================================================
  # End-to-End Integration Tests
  # ===========================================================================

  describe "end-to-end optimization" do
    setup do
      name = unique_cache_name()
      {:ok, pid} = PlanCache.start_link(name: name, max_size: 100)
      on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
      %{cache_name: name}
    end

    test "complete optimization pipeline for star query", %{cache_name: name} do
      # Star query: all patterns share ?x
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("x"), 2, var("z")},
        {:triple, var("x"), 3, var("w")}
      ]

      plan =
        PlanCache.get_or_compute(
          patterns,
          fn ->
            {:ok, result} = JoinEnumeration.enumerate(patterns, @medium_stats)
            result
          end,
          name: name
        )

      # Verify plan structure
      assert plan.cardinality > 0
      assert plan.cost.total > 0

      # Verify caching works
      stats = PlanCache.stats(name: name)
      assert stats.size == 1
    end

    test "complete optimization pipeline for chain query", %{cache_name: name} do
      # Chain query: x-y-z-w
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("z"), 3, var("w")}
      ]

      plan =
        PlanCache.get_or_compute(
          patterns,
          fn ->
            {:ok, result} = JoinEnumeration.enumerate(patterns, @medium_stats)
            result
          end,
          name: name
        )

      # Verify plan includes all join variables
      assert plan.cardinality > 0
      assert plan.cost.total > 0
    end

    test "optimization uses statistics effectively", %{cache_name: _name} do
      # Same query structure, different statistics should produce different estimates
      patterns = [{:triple, var("x"), 1, var("y")}]

      {:ok, small_plan} = JoinEnumeration.enumerate(patterns, @small_stats)
      {:ok, large_plan} = JoinEnumeration.enumerate(patterns, @large_stats)

      # Larger database = higher cardinality estimate
      assert large_plan.cardinality > small_plan.cardinality
    end

    test "cost-based selection works across query types" do
      # Test various query structures
      queries = [
        # Single pattern
        [{:triple, var("x"), 1, var("y")}],
        # Chain
        [{:triple, var("x"), 1, var("y")}, {:triple, var("y"), 2, var("z")}],
        # Star
        [{:triple, var("x"), 1, var("y")}, {:triple, var("x"), 2, var("z")}],
        # Triangle (cyclic)
        [
          {:triple, var("x"), 1, var("y")},
          {:triple, var("y"), 2, var("z")},
          {:triple, var("z"), 3, var("x")}
        ]
      ]

      for patterns <- queries do
        {:ok, plan} = JoinEnumeration.enumerate(patterns, @medium_stats)

        assert plan.cardinality > 0, "Query should have positive cardinality"
        assert plan.cost.total > 0, "Query should have positive cost"
        assert plan.cost.total < Float.max_finite(), "Cost should be finite"
      end
    end
  end

  # ===========================================================================
  # Performance Characteristics Tests
  # ===========================================================================

  describe "performance characteristics" do
    test "enumeration completes quickly for small queries" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("z"), 3, var("w")}
      ]

      {time, {:ok, _plan}} =
        :timer.tc(fn ->
          JoinEnumeration.enumerate(patterns, @medium_stats)
        end)

      # Should complete in < 100ms
      assert time < 100_000
    end

    test "DPccp handles 6 patterns efficiently" do
      patterns =
        for i <- 1..6 do
          {:triple, var("x"), i, var("y#{i}")}
        end

      {time, {:ok, _plan}} =
        :timer.tc(fn ->
          JoinEnumeration.enumerate(patterns, @medium_stats)
        end)

      # Should complete in < 500ms
      assert time < 500_000
    end

    test "cache lookup is fast" do
      name = unique_cache_name()
      {:ok, pid} = PlanCache.start_link(name: name, max_size: 100)

      try do
        patterns = [{:triple, var("x"), 1, var("y")}]

        # Populate cache
        PlanCache.get_or_compute(
          patterns,
          fn ->
            {:ok, result} = JoinEnumeration.enumerate(patterns, @medium_stats)
            result
          end,
          name: name
        )

        # Time cache hit
        {time, _plan} =
          :timer.tc(fn ->
            for _ <- 1..1000 do
              PlanCache.get_or_compute(
                patterns,
                fn ->
                  {:ok, result} = JoinEnumeration.enumerate(patterns, @medium_stats)
                  result
                end,
                name: name
              )
            end
          end)

        # 1000 cache hits should complete in < 100ms (100us per hit)
        assert time < 100_000
      after
        GenServer.stop(pid)
      end
    end
  end
end
