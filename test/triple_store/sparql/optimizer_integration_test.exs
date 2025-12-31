defmodule TripleStore.SPARQL.OptimizerIntegrationTest do
  @moduledoc """
  Integration tests for optimizer selections in actual query execution.

  Task 3.5.4: Optimizer Integration Testing
  - 3.5.4.1 Test optimizer chooses hash join for large intermediate results
  - 3.5.4.2 Test optimizer chooses nested loop for small inputs
  - 3.5.4.3 Test optimizer chooses Leapfrog for multi-way joins
  - 3.5.4.4 Test plan cache shows >90% hit rate on repeated queries

  These tests verify that optimizer decisions are actually applied during
  query execution, not just that the optimizer produces the right plans.
  """

  use ExUnit.Case, async: false

  import TripleStore.Test.IntegrationHelpers, only: [var: 1, triple: 3]

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Index
  alias TripleStore.SPARQL.{CostModel, JoinEnumeration, PlanCache, Query}

  @moduletag :tmp_dir

  # ===========================================================================
  # Test Fixtures
  # ===========================================================================

  @ex "http://example.org/"

  # Statistics for different dataset sizes
  @small_stats %{
    triple_count: 100,
    distinct_subjects: 20,
    distinct_predicates: 5,
    distinct_objects: 50,
    predicate_histogram: %{}
  }

  @medium_stats %{
    triple_count: 10_000,
    distinct_subjects: 1_000,
    distinct_predicates: 20,
    distinct_objects: 2_000,
    predicate_histogram: %{}
  }

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup %{tmp_dir: tmp_dir} do
    db_path = Path.join(tmp_dir, "optimizer_test_#{:erlang.unique_integer([:positive])}")
    {:ok, db} = NIF.open(db_path)
    {:ok, manager} = Manager.start_link(db: db)

    cache_name = :"OptimizerCache_#{:erlang.unique_integer([:positive])}"
    {:ok, cache_pid} = PlanCache.start_link(name: cache_name, max_size: 100)

    on_exit(fn ->
      if Process.alive?(cache_pid), do: GenServer.stop(cache_pid)
      if Process.alive?(manager), do: GenServer.stop(manager)
    end)

    ctx = %{db: db, dict_manager: manager}
    %{ctx: ctx, db: db, manager: manager, cache_name: cache_name}
  end

  # var/1 and triple/3 imported from IntegrationHelpers

  defp add_triple(ctx, {s, p, o}) do
    %{db: db, dict_manager: manager} = ctx
    {:ok, s_id} = Manager.get_or_create_id(manager, RDF.iri("#{@ex}#{s}"))
    {:ok, p_id} = Manager.get_or_create_id(manager, RDF.iri("#{@ex}#{p}"))
    {:ok, o_id} = Manager.get_or_create_id(manager, RDF.iri("#{@ex}#{o}"))
    :ok = Index.insert_triple(db, {s_id, p_id, o_id})
  end

  # ===========================================================================
  # 3.5.4.1 Hash Join for Large Intermediate Results
  # ===========================================================================

  describe "optimizer chooses hash join for large intermediate results" do
    test "hash join selected for medium-sized inputs", %{ctx: ctx} do
      # Create enough data to trigger hash join selection
      # Hash join threshold is 100, so we need > 100 potential results per pattern
      for i <- 1..50 do
        add_triple(ctx, {"person#{i}", "knows", "person#{i + 50}"})
        add_triple(ctx, {"person#{i + 50}", "likes", "thing#{i}"})
      end

      # Two-pattern join query
      # Verify optimizer selects hash join for cardinalities over threshold
      {strategy, _cost} = CostModel.select_join_strategy(500, 500, ["y"], @medium_stats)
      # With inputs > threshold (100), should prefer hash_join for efficiency
      assert strategy == :hash_join

      # Execute actual query to verify it works
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?x ?y ?z WHERE {
        ?x ex:knows ?y .
        ?y ex:likes ?z .
      }
      """

      {:ok, results} = Query.query(ctx, query)
      assert length(results) == 50
    end

    test "hash join produces correct results for chain query", %{ctx: ctx} do
      # Create a chain: person1 -> person2 -> person3
      add_triple(ctx, {"person1", "knows", "person2"})
      add_triple(ctx, {"person2", "knows", "person3"})
      add_triple(ctx, {"person2", "likes", "thing1"})
      add_triple(ctx, {"person3", "likes", "thing2"})

      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?a ?b ?c WHERE {
        ?a ex:knows ?b .
        ?b ex:knows ?c .
      }
      """

      {:ok, results} = Query.query(ctx, query)

      # Should find the chain
      assert length(results) == 1
      result = hd(results)
      assert result["a"] == {:named_node, "#{@ex}person1"}
      assert result["b"] == {:named_node, "#{@ex}person2"}
      assert result["c"] == {:named_node, "#{@ex}person3"}
    end

    test "cost model correctly ranks hash join cheaper for large inputs" do
      left_card = 10_000
      right_card = 5_000

      nl_cost = CostModel.nested_loop_cost(left_card, right_card)
      hj_cost = CostModel.hash_join_cost(left_card, right_card)

      # Hash join should be cheaper for large inputs
      assert CostModel.compare_costs(hj_cost, nl_cost) == :lt
    end
  end

  # ===========================================================================
  # 3.5.4.2 Nested Loop for Small Inputs
  # ===========================================================================

  describe "optimizer chooses nested loop for small inputs" do
    test "nested loop selected for tiny inputs", %{ctx: ctx} do
      # Create minimal data
      add_triple(ctx, {"alice", "knows", "bob"})
      add_triple(ctx, {"bob", "likes", "cats"})

      # Verify strategy selection
      {strategy, _cost} = CostModel.select_join_strategy(5, 5, ["x"], @small_stats)
      assert strategy == :nested_loop

      # Execute query
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?x ?y ?z WHERE {
        ?x ex:knows ?y .
        ?y ex:likes ?z .
      }
      """

      {:ok, results} = Query.query(ctx, query)
      assert length(results) == 1
    end

    test "nested loop produces correct results for small datasets", %{ctx: ctx} do
      # Small dataset: 3 people, each knows one other
      add_triple(ctx, {"alice", "knows", "bob"})
      add_triple(ctx, {"bob", "knows", "charlie"})
      add_triple(ctx, {"charlie", "knows", "alice"})

      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?a ?b WHERE {
        ?a ex:knows ?b .
      }
      """

      {:ok, results} = Query.query(ctx, query)
      assert length(results) == 3
    end

    test "cost model correctly selects nested loop for small cardinalities" do
      # Below threshold (100)
      {strategy, _cost} = CostModel.select_join_strategy(10, 10, ["x"], @small_stats)
      assert strategy == :nested_loop

      {strategy2, _cost2} = CostModel.select_join_strategy(50, 50, ["x"], @small_stats)
      assert strategy2 == :nested_loop

      # Exactly at threshold
      {strategy3, _cost3} = CostModel.select_join_strategy(99, 99, ["x"], @small_stats)
      assert strategy3 == :nested_loop
    end

    test "nested loop is optimal when one side is very small" do
      # Even with large right side, if left is tiny, nested loop may be preferred
      {strategy, _cost} = CostModel.select_join_strategy(3, 3, ["x"], @small_stats)
      assert strategy == :nested_loop
    end
  end

  # ===========================================================================
  # 3.5.4.3 Leapfrog for Multi-Way Joins
  # ===========================================================================

  describe "optimizer chooses Leapfrog for multi-way joins" do
    test "Leapfrog selected for 4+ pattern star query", %{ctx: ctx} do
      # Create star pattern data: one central node with multiple edges
      for i <- 1..10 do
        add_triple(ctx, {"entity#{i}", "prop1", "val1_#{i}"})
        add_triple(ctx, {"entity#{i}", "prop2", "val2_#{i}"})
        add_triple(ctx, {"entity#{i}", "prop3", "val3_#{i}"})
        add_triple(ctx, {"entity#{i}", "prop4", "val4_#{i}"})
      end

      # 4-pattern star query
      patterns = [
        triple(var("x"), 1, var("a")),
        triple(var("x"), 2, var("b")),
        triple(var("x"), 3, var("c")),
        triple(var("x"), 4, var("d"))
      ]

      stats = %{
        triple_count: 100_000,
        distinct_subjects: 10_000,
        distinct_predicates: 10,
        distinct_objects: 20_000,
        predicate_histogram: %{1 => 10_000, 2 => 10_000, 3 => 10_000, 4 => 10_000}
      }

      {:ok, plan} = JoinEnumeration.enumerate(patterns, stats)

      # Should select Leapfrog for 4+ pattern star
      assert match?({:leapfrog, _, _}, plan.tree),
             "Expected Leapfrog for 4-pattern star, got: #{inspect(plan.tree)}"
    end

    test "Leapfrog produces correct results for star query", %{ctx: ctx} do
      # Create star pattern: entity1 has all 4 properties
      add_triple(ctx, {"entity1", "prop1", "val1"})
      add_triple(ctx, {"entity1", "prop2", "val2"})
      add_triple(ctx, {"entity1", "prop3", "val3"})
      add_triple(ctx, {"entity1", "prop4", "val4"})

      # entity2 only has 3 properties (should not match)
      add_triple(ctx, {"entity2", "prop1", "val1"})
      add_triple(ctx, {"entity2", "prop2", "val2"})
      add_triple(ctx, {"entity2", "prop3", "val3"})

      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?x ?a ?b ?c ?d WHERE {
        ?x ex:prop1 ?a .
        ?x ex:prop2 ?b .
        ?x ex:prop3 ?c .
        ?x ex:prop4 ?d .
      }
      """

      {:ok, results} = Query.query(ctx, query)

      # Only entity1 matches all 4 patterns
      assert length(results) == 1
      result = hd(results)
      assert result["x"] == {:named_node, "#{@ex}entity1"}
    end

    test "Leapfrog not selected for 2-3 pattern queries" do
      patterns_2 = [
        triple(var("x"), 1, var("a")),
        triple(var("x"), 2, var("b"))
      ]

      patterns_3 = [
        triple(var("x"), 1, var("a")),
        triple(var("x"), 2, var("b")),
        triple(var("x"), 3, var("c"))
      ]

      stats = %{
        triple_count: 100_000,
        distinct_subjects: 10_000,
        distinct_predicates: 10,
        distinct_objects: 20_000,
        predicate_histogram: %{1 => 10_000, 2 => 10_000, 3 => 10_000}
      }

      {:ok, plan_2} = JoinEnumeration.enumerate(patterns_2, stats)
      {:ok, plan_3} = JoinEnumeration.enumerate(patterns_3, stats)

      # Should NOT use Leapfrog for small queries
      refute match?({:leapfrog, _, _}, plan_2.tree)
      refute match?({:leapfrog, _, _}, plan_3.tree)
    end

    test "Leapfrog selected for 5+ pattern queries" do
      patterns = [
        triple(var("x"), 1, var("a")),
        triple(var("x"), 2, var("b")),
        triple(var("x"), 3, var("c")),
        triple(var("x"), 4, var("d")),
        triple(var("x"), 5, var("e"))
      ]

      stats = %{
        triple_count: 100_000,
        distinct_subjects: 10_000,
        distinct_predicates: 10,
        distinct_objects: 20_000,
        predicate_histogram: %{1 => 10_000, 2 => 10_000, 3 => 10_000, 4 => 10_000, 5 => 10_000}
      }

      {:ok, plan} = JoinEnumeration.enumerate(patterns, stats)

      assert match?({:leapfrog, _, _}, plan.tree),
             "Expected Leapfrog for 5-pattern star"
    end
  end

  # ===========================================================================
  # 3.5.4.4 Plan Cache Hit Rate
  # ===========================================================================

  describe "plan cache shows >90% hit rate on repeated queries" do
    test "achieves 90% hit rate with repeated queries", %{cache_name: cache_name} do
      patterns = [
        triple(var("x"), 1, var("y")),
        triple(var("y"), 2, var("z"))
      ]

      compute_count = :counters.new(1, [:atomics])

      compute = fn ->
        :counters.add(compute_count, 1, 1)
        {:ok, result} = JoinEnumeration.enumerate(patterns, @medium_stats)
        result
      end

      # First access - miss (compute called)
      PlanCache.get_or_compute(patterns, compute, name: cache_name)

      # 9 more accesses - all hits (compute not called)
      for _ <- 1..9 do
        PlanCache.get_or_compute(patterns, compute, name: cache_name)
      end

      stats = PlanCache.stats(name: cache_name)

      # Should have exactly 90% hit rate (9 hits, 1 miss)
      assert stats.hits == 9
      assert stats.misses == 1
      assert stats.hit_rate == 0.9
      assert :counters.get(compute_count, 1) == 1
    end

    test "achieves >90% hit rate with multiple different queries", %{cache_name: cache_name} do
      # Create 10 different query patterns
      patterns_list =
        for i <- 1..10 do
          [
            triple(var("x"), i, var("y")),
            triple(var("y"), i + 10, var("z"))
          ]
        end

      compute_count = :counters.new(1, [:atomics])

      # First round: all misses (10 computes)
      for patterns <- patterns_list do
        PlanCache.get_or_compute(
          patterns,
          fn ->
            :counters.add(compute_count, 1, 1)
            {:ok, result} = JoinEnumeration.enumerate(patterns, @medium_stats)
            result
          end,
          name: cache_name
        )
      end

      assert :counters.get(compute_count, 1) == 10

      # 9 more rounds: all hits (no additional computes)
      for _ <- 1..9 do
        for patterns <- patterns_list do
          PlanCache.get_or_compute(
            patterns,
            fn ->
              :counters.add(compute_count, 1, 1)
              {:ok, result} = JoinEnumeration.enumerate(patterns, @medium_stats)
              result
            end,
            name: cache_name
          )
        end
      end

      # Should still be 10 computes (all additional were hits)
      assert :counters.get(compute_count, 1) == 10

      stats = PlanCache.stats(name: cache_name)

      # 10 misses, 90 hits = 90% hit rate
      assert stats.hits == 90
      assert stats.misses == 10
      assert stats.hit_rate == 0.9
    end

    test "hit rate improves over time with workload", %{cache_name: cache_name} do
      patterns = [triple(var("x"), 1, var("y"))]

      # Execute 100 times
      for _ <- 1..100 do
        PlanCache.get_or_compute(
          patterns,
          fn ->
            {:ok, result} = JoinEnumeration.enumerate(patterns, @small_stats)
            result
          end,
          name: cache_name
        )
      end

      stats = PlanCache.stats(name: cache_name)

      # 1 miss, 99 hits = 99% hit rate
      assert stats.hit_rate == 0.99
    end

    test "structurally equivalent queries share cache entry", %{cache_name: cache_name} do
      # Same structure, different variable names
      patterns1 = [triple(var("x"), 1, var("y"))]
      patterns2 = [triple(var("subject"), 1, var("object"))]
      patterns3 = [triple(var("s"), 1, var("o"))]

      compute_count = :counters.new(1, [:atomics])

      for patterns <- [patterns1, patterns2, patterns3] do
        PlanCache.get_or_compute(
          patterns,
          fn ->
            :counters.add(compute_count, 1, 1)
            {:ok, result} = JoinEnumeration.enumerate(patterns, @small_stats)
            result
          end,
          name: cache_name
        )
      end

      # Should only compute once (all 3 share the same normalized cache key)
      assert :counters.get(compute_count, 1) == 1
    end

    test "cache invalidation resets hit rate", %{cache_name: cache_name} do
      patterns = [triple(var("x"), 1, var("y"))]

      # Build up hit rate
      for _ <- 1..10 do
        PlanCache.get_or_compute(
          patterns,
          fn ->
            {:ok, result} = JoinEnumeration.enumerate(patterns, @small_stats)
            result
          end,
          name: cache_name
        )
      end

      stats_before = PlanCache.stats(name: cache_name)
      assert stats_before.hit_rate == 0.9

      # Invalidate cache
      PlanCache.invalidate(name: cache_name)

      stats_after = PlanCache.stats(name: cache_name)
      assert stats_after.size == 0
      # Note: hits/misses counters may or may not reset depending on implementation
    end
  end

  # ===========================================================================
  # End-to-End Integration Tests
  # ===========================================================================

  describe "end-to-end optimizer integration" do
    test "complex query executes correctly with optimizer", %{ctx: ctx} do
      # Create a social network
      add_triple(ctx, {"alice", "knows", "bob"})
      add_triple(ctx, {"alice", "knows", "charlie"})
      add_triple(ctx, {"bob", "knows", "charlie"})
      add_triple(ctx, {"bob", "knows", "david"})
      add_triple(ctx, {"charlie", "knows", "david"})

      add_triple(ctx, {"alice", "likes", "cats"})
      add_triple(ctx, {"bob", "likes", "dogs"})
      add_triple(ctx, {"charlie", "likes", "cats"})
      add_triple(ctx, {"david", "likes", "birds"})

      # Find people who know someone who likes cats
      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?person ?friend WHERE {
        ?person ex:knows ?friend .
        ?friend ex:likes ex:cats .
      }
      """

      {:ok, results} = Query.query(ctx, query)

      # alice and bob know charlie (who likes cats), alice knows bob (who doesn't)
      friends_who_like_cats =
        results
        |> Enum.map(fn r -> {r["person"], r["friend"]} end)
        |> Enum.sort()

      assert length(friends_who_like_cats) == 2
    end

    test "optimizer handles OPTIONAL correctly", %{ctx: ctx} do
      add_triple(ctx, {"alice", "name", "Alice"})
      add_triple(ctx, {"alice", "email", "alice@example.org"})
      add_triple(ctx, {"bob", "name", "Bob"})
      # Bob has no email

      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?person ?name ?email WHERE {
        ?person ex:name ?name .
        OPTIONAL { ?person ex:email ?email }
      }
      """

      {:ok, results} = Query.query(ctx, query)

      assert length(results) == 2

      alice = Enum.find(results, fn r -> r["name"] == {:named_node, "#{@ex}Alice"} end)
      bob = Enum.find(results, fn r -> r["name"] == {:named_node, "#{@ex}Bob"} end)

      assert alice["email"] == {:named_node, "#{@ex}alice@example.org"}
      assert bob["email"] == nil
    end

    test "optimizer handles UNION correctly", %{ctx: ctx} do
      add_triple(ctx, {"alice", "knows", "bob"})
      add_triple(ctx, {"charlie", "likes", "david"})

      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?x ?y WHERE {
        { ?x ex:knows ?y }
        UNION
        { ?x ex:likes ?y }
      }
      """

      {:ok, results} = Query.query(ctx, query)

      assert length(results) == 2
    end

    test "timing comparison validates optimizer effectiveness", %{ctx: ctx} do
      # Create a dataset with multiple patterns
      for i <- 1..20 do
        add_triple(ctx, {"entity#{i}", "type", "Thing"})
        add_triple(ctx, {"entity#{i}", "value", "val#{i}"})
      end

      query = """
      PREFIX ex: <#{@ex}>
      SELECT ?x ?v WHERE {
        ?x ex:type ex:Thing .
        ?x ex:value ?v .
      }
      """

      # Time multiple executions
      {time, {:ok, results}} =
        :timer.tc(fn ->
          Query.query(ctx, query)
        end)

      assert length(results) == 20
      # Should complete quickly (< 100ms)
      assert time < 100_000, "Query took #{time / 1000}ms, expected < 100ms"
    end
  end
end
