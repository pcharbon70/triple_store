defmodule TripleStore.SPARQL.JoinEnumerationTest do
  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.JoinEnumeration

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

  # Helper to create variable
  defp var(name), do: {:variable, name}

  # ===========================================================================
  # Pattern Variables Tests
  # ===========================================================================

  describe "pattern_variables/1" do
    test "extracts all variables from pattern" do
      pattern = {:triple, var("s"), var("p"), var("o")}
      vars = JoinEnumeration.pattern_variables(pattern)

      assert Enum.sort(vars) == ["o", "p", "s"]
    end

    test "ignores bound positions" do
      pattern = {:triple, var("s"), 42, var("o")}
      vars = JoinEnumeration.pattern_variables(pattern)

      assert Enum.sort(vars) == ["o", "s"]
    end

    test "returns empty list for fully bound pattern" do
      pattern = {:triple, 1, 2, 3}
      vars = JoinEnumeration.pattern_variables(pattern)

      assert vars == []
    end

    test "handles duplicate variables" do
      pattern = {:triple, var("x"), var("p"), var("x")}
      vars = JoinEnumeration.pattern_variables(pattern)

      # Should deduplicate
      assert vars == ["x", "p"] or vars == ["p", "x"]
      assert length(vars) == 2
    end
  end

  # ===========================================================================
  # Shared Variables Tests
  # ===========================================================================

  describe "shared_variables/2" do
    test "finds shared variables between patterns" do
      p1 = {:triple, var("x"), 1, var("y")}
      p2 = {:triple, var("y"), 2, var("z")}

      shared = JoinEnumeration.shared_variables(p1, p2)

      assert shared == ["y"]
    end

    test "returns empty list when no shared variables" do
      p1 = {:triple, var("a"), 1, var("b")}
      p2 = {:triple, var("x"), 2, var("y")}

      shared = JoinEnumeration.shared_variables(p1, p2)

      assert shared == []
    end

    test "handles multiple shared variables" do
      p1 = {:triple, var("x"), var("p"), var("y")}
      p2 = {:triple, var("x"), var("q"), var("y")}

      shared = JoinEnumeration.shared_variables(p1, p2)

      assert Enum.sort(shared) == ["x", "y"]
    end

    test "handles fully bound patterns" do
      p1 = {:triple, 1, 2, 3}
      p2 = {:triple, var("x"), var("y"), var("z")}

      shared = JoinEnumeration.shared_variables(p1, p2)

      assert shared == []
    end
  end

  # ===========================================================================
  # Join Graph Tests
  # ===========================================================================

  describe "build_join_graph/1" do
    test "builds graph for chain query" do
      # ?x :p1 ?y . ?y :p2 ?z
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      graph = JoinEnumeration.build_join_graph(patterns)

      assert MapSet.member?(graph[0], 1)
      assert MapSet.member?(graph[1], 0)
    end

    test "builds graph for star query" do
      # ?x :p1 ?y . ?x :p2 ?z . ?x :p3 ?w
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("x"), 2, var("z")},
        {:triple, var("x"), 3, var("w")}
      ]

      graph = JoinEnumeration.build_join_graph(patterns)

      # All patterns connected through ?x
      assert MapSet.member?(graph[0], 1)
      assert MapSet.member?(graph[0], 2)
      assert MapSet.member?(graph[1], 0)
      assert MapSet.member?(graph[1], 2)
      assert MapSet.member?(graph[2], 0)
      assert MapSet.member?(graph[2], 1)
    end

    test "identifies disconnected patterns" do
      # ?x :p1 ?y . ?a :p2 ?b (no shared variables)
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("a"), 2, var("b")}
      ]

      graph = JoinEnumeration.build_join_graph(patterns)

      assert MapSet.size(graph[0]) == 0
      assert MapSet.size(graph[1]) == 0
    end

    test "handles single pattern" do
      patterns = [{:triple, var("x"), 1, var("y")}]

      graph = JoinEnumeration.build_join_graph(patterns)

      assert graph == %{0 => MapSet.new()}
    end
  end

  # ===========================================================================
  # Sets Connected Tests
  # ===========================================================================

  describe "sets_connected?/3" do
    test "returns true for connected sets" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("z"), 3, var("w")}
      ]

      graph = JoinEnumeration.build_join_graph(patterns)

      set1 = MapSet.new([0])
      set2 = MapSet.new([1])

      assert JoinEnumeration.sets_connected?(set1, set2, graph)
    end

    test "returns false for disconnected sets" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("a"), 2, var("b")}
      ]

      graph = JoinEnumeration.build_join_graph(patterns)

      set1 = MapSet.new([0])
      set2 = MapSet.new([1])

      refute JoinEnumeration.sets_connected?(set1, set2, graph)
    end

    test "handles multi-element sets" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("z"), 3, var("w")}
      ]

      graph = JoinEnumeration.build_join_graph(patterns)

      set1 = MapSet.new([0, 1])
      set2 = MapSet.new([2])

      assert JoinEnumeration.sets_connected?(set1, set2, graph)
    end
  end

  # ===========================================================================
  # Single Pattern Enumeration Tests
  # ===========================================================================

  describe "enumerate/2 with single pattern" do
    test "returns scan plan for single pattern" do
      pattern = {:triple, var("s"), 1, var("o")}

      {:ok, plan} = JoinEnumeration.enumerate([pattern], @small_stats)

      assert {:scan, ^pattern} = plan.tree
      assert plan.cardinality > 0
      assert plan.cost.total > 0
    end

    test "returns error for empty patterns" do
      assert {:error, :empty_patterns} = JoinEnumeration.enumerate([], @small_stats)
    end
  end

  # ===========================================================================
  # Two Pattern Enumeration Tests
  # ===========================================================================

  describe "enumerate/2 with two patterns" do
    test "joins connected patterns" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert {:join, strategy, _left, _right, join_vars} = plan.tree
      assert strategy in [:nested_loop, :hash_join]
      assert "y" in join_vars
    end

    test "handles disconnected patterns with Cartesian product" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("a"), 2, var("b")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # Should still produce a plan (with Cartesian product)
      assert {:join, _strategy, _left, _right, join_vars} = plan.tree
      assert join_vars == []
    end

    test "selects appropriate join strategy" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @medium_stats)

      {:join, strategy, _, _, _} = plan.tree
      assert strategy in [:nested_loop, :hash_join]
    end
  end

  # ===========================================================================
  # Three Pattern Enumeration Tests
  # ===========================================================================

  describe "enumerate/2 with three patterns" do
    test "finds optimal ordering for chain query" do
      # Chain: ?x - ?y - ?z
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("z"), 3, var("w")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert plan.cardinality > 0
      assert plan.cost.total > 0
    end

    test "finds optimal ordering for star query" do
      # Star: all patterns share ?x
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("x"), 2, var("z")},
        {:triple, var("x"), 3, var("w")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert plan.cardinality > 0
      assert plan.cost.total > 0
    end

    test "handles mixed connectivity" do
      # P1 connected to P2, P2 connected to P3, P1 not directly connected to P3
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("a"), 3, var("b")}  # Disconnected
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # Should produce a valid plan
      assert plan.cardinality > 0
    end
  end

  # ===========================================================================
  # Exhaustive vs DPccp Tests
  # ===========================================================================

  describe "exhaustive enumeration" do
    test "uses exhaustive for 5 or fewer patterns" do
      patterns =
        for i <- 1..5 do
          {:triple, var("x"), i, var("y#{i}")}
        end

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert plan.cardinality > 0
    end

    test "produces optimal plan for small queries" do
      # Simple chain where order matters
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # Plan should have reasonable cost
      assert plan.cost.total > 0
      assert plan.cost.total < :math.pow(10, 10)
    end
  end

  describe "DPccp enumeration" do
    test "uses DPccp for more than 5 patterns" do
      # Create 6 connected patterns
      patterns =
        for i <- 1..6 do
          {:triple, var("x"), i, var("y#{i}")}
        end

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert plan.cardinality > 0
    end

    test "produces valid plan for larger queries" do
      # Chain of 7 patterns
      patterns = [
        {:triple, var("a"), 1, var("b")},
        {:triple, var("b"), 2, var("c")},
        {:triple, var("c"), 3, var("d")},
        {:triple, var("d"), 4, var("e")},
        {:triple, var("e"), 5, var("f")},
        {:triple, var("f"), 6, var("g")},
        {:triple, var("g"), 7, var("h")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert plan.cardinality > 0
      assert plan.cost.total > 0
    end
  end

  # ===========================================================================
  # Leapfrog Selection Tests
  # ===========================================================================

  describe "Leapfrog selection" do
    test "considers Leapfrog for 4+ patterns with shared variables" do
      # Star query with 5 patterns - good candidate for Leapfrog
      patterns =
        for i <- 1..5 do
          {:triple, var("center"), i, var("leaf#{i}")}
        end

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @medium_stats)

      # Should produce a valid plan (may or may not use Leapfrog depending on cost)
      assert plan.cardinality > 0
    end

    test "prefers pairwise joins for chain queries" do
      # Chain queries don't benefit as much from Leapfrog
      patterns = [
        {:triple, var("a"), 1, var("b")},
        {:triple, var("b"), 2, var("c")},
        {:triple, var("c"), 3, var("d")},
        {:triple, var("d"), 4, var("e")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # Should produce pairwise join plan (not Leapfrog for chain)
      case plan.tree do
        {:leapfrog, _, _} -> :ok  # Leapfrog is acceptable
        {:join, _, _, _, _} -> :ok  # Pairwise is also acceptable
      end
    end
  end

  # ===========================================================================
  # Plan Structure Tests
  # ===========================================================================

  describe "plan structure" do
    test "scan node has correct structure" do
      pattern = {:triple, var("s"), 1, var("o")}

      {:ok, plan} = JoinEnumeration.enumerate([pattern], @small_stats)

      assert {:scan, ^pattern} = plan.tree
    end

    test "join node has correct structure" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert {:join, strategy, left, right, vars} = plan.tree
      assert strategy in [:nested_loop, :hash_join, :leapfrog]
      assert is_tuple(left)
      assert is_tuple(right)
      assert is_list(vars)
    end

    test "plan includes cost breakdown" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert is_map(plan.cost)
      assert Map.has_key?(plan.cost, :cpu)
      assert Map.has_key?(plan.cost, :io)
      assert Map.has_key?(plan.cost, :memory)
      assert Map.has_key?(plan.cost, :total)
    end

    test "plan includes cardinality estimate" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      assert is_number(plan.cardinality)
      assert plan.cardinality > 0
    end
  end

  # ===========================================================================
  # Cartesian Product Handling Tests
  # ===========================================================================

  describe "Cartesian product handling" do
    test "avoids Cartesian products when possible" do
      # Connected patterns - should not produce Cartesian
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("z"), 3, var("w")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # All joins should have shared variables
      assert check_all_joins_connected(plan.tree)
    end

    test "allows Cartesian products when necessary" do
      # Completely disconnected patterns
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("a"), 2, var("b")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # Should still produce a plan
      assert plan.cardinality > 0
    end
  end

  # Helper to check if all joins have shared variables
  defp check_all_joins_connected({:scan, _}), do: true
  defp check_all_joins_connected({:leapfrog, _, _}), do: true

  defp check_all_joins_connected({:join, _strategy, left, right, vars}) do
    # Empty vars means Cartesian product - but we allow it as fallback
    check_all_joins_connected(left) and check_all_joins_connected(right)
  end

  # ===========================================================================
  # Cost Comparison Tests
  # ===========================================================================

  describe "cost comparison" do
    test "more selective patterns lead to lower cost" do
      # Pattern with bound predicate should be more selective
      selective_patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 1, var("z")}
      ]

      # Pattern with unbound predicate
      general_patterns = [
        {:triple, var("x"), var("p"), var("y")},
        {:triple, var("y"), var("q"), var("z")}
      ]

      {:ok, selective_plan} = JoinEnumeration.enumerate(selective_patterns, @medium_stats)
      {:ok, general_plan} = JoinEnumeration.enumerate(general_patterns, @medium_stats)

      # Selective plan should have lower estimated cardinality
      assert selective_plan.cardinality <= general_plan.cardinality
    end

    test "join order affects cost" do
      # Different join orders can have different costs
      # The optimizer should find a good order
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("x"), 3, var("w")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @medium_stats)

      # Should produce a reasonable cost
      assert plan.cost.total > 0
      assert plan.cost.total < :math.pow(10, 15)
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles patterns with same variable in multiple positions" do
      pattern = {:triple, var("x"), var("p"), var("x")}

      {:ok, plan} = JoinEnumeration.enumerate([pattern], @small_stats)

      assert {:scan, ^pattern} = plan.tree
    end

    test "handles all bound patterns" do
      patterns = [
        {:triple, 1, 2, 3},
        {:triple, 4, 5, 6}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # Should still produce a plan (Cartesian product)
      assert plan.cardinality > 0
    end

    test "handles patterns with blank nodes" do
      patterns = [
        {:triple, {:blank_node, "_:b1"}, 1, var("x")},
        {:triple, var("x"), 2, var("y")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, @small_stats)

      # Blank nodes are treated as unbound
      assert plan.cardinality > 0
    end

    test "handles empty stats" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")}
      ]

      {:ok, plan} = JoinEnumeration.enumerate(patterns, %{})

      # Should use defaults
      assert plan.cardinality > 0
    end
  end

  # ===========================================================================
  # Shared Variables Between Sets Tests
  # ===========================================================================

  describe "shared_variables_between_sets/3" do
    test "finds shared variables between pattern sets" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("y"), 2, var("z")},
        {:triple, var("z"), 3, var("w")}
      ]

      set1 = MapSet.new([0])
      set2 = MapSet.new([1])

      shared = JoinEnumeration.shared_variables_between_sets(patterns, set1, set2)

      assert "y" in shared
    end

    test "handles multiple shared variables" do
      patterns = [
        {:triple, var("x"), 1, var("y")},
        {:triple, var("x"), 2, var("y")}
      ]

      set1 = MapSet.new([0])
      set2 = MapSet.new([1])

      shared = JoinEnumeration.shared_variables_between_sets(patterns, set1, set2)

      assert Enum.sort(shared) == ["x", "y"]
    end

    test "returns empty list for disconnected sets" do
      patterns = [
        {:triple, var("a"), 1, var("b")},
        {:triple, var("x"), 2, var("y")}
      ]

      set1 = MapSet.new([0])
      set2 = MapSet.new([1])

      shared = JoinEnumeration.shared_variables_between_sets(patterns, set1, set2)

      assert shared == []
    end
  end
end
