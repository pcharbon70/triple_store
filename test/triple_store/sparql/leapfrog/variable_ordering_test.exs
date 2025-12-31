defmodule TripleStore.SPARQL.Leapfrog.VariableOrderingTest do
  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Leapfrog.VariableOrdering

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp var(name), do: {:variable, name}
  defp iri(uri), do: {:named_node, uri}
  defp literal(value), do: {:literal, value, nil}
  defp triple(s, p, o), do: {:triple, s, p, o}

  # ===========================================================================
  # Basic Ordering Tests
  # ===========================================================================

  describe "compute/2" do
    test "returns empty list for empty patterns" do
      assert {:ok, []} = VariableOrdering.compute([])
    end

    test "returns single variable for single-variable pattern" do
      patterns = [triple(var("x"), iri("knows"), iri("Alice"))]

      {:ok, order} = VariableOrdering.compute(patterns)

      assert order == ["x"]
    end

    test "returns all variables from pattern" do
      patterns = [triple(var("s"), var("p"), var("o"))]

      {:ok, order} = VariableOrdering.compute(patterns)

      assert length(order) == 3
      assert "s" in order
      assert "p" in order
      assert "o" in order
    end

    test "handles multiple patterns" do
      patterns = [
        triple(var("x"), iri("knows"), var("y")),
        triple(var("y"), iri("age"), var("z"))
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      assert length(order) == 3
      assert "x" in order
      assert "y" in order
      assert "z" in order
    end
  end

  # ===========================================================================
  # Selectivity-Based Ordering Tests
  # ===========================================================================

  describe "selectivity ordering" do
    test "prefers variables appearing in multiple patterns" do
      # y appears in both patterns, should be first
      patterns = [
        triple(var("x"), iri("knows"), var("y")),
        triple(var("y"), iri("age"), var("z"))
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      # y should come before x and z because it's more constrained
      assert hd(order) == "y"
    end

    test "prefers variables in predicate position" do
      # p is in predicate position - typically more selective
      patterns = [triple(var("s"), var("p"), var("o"))]

      {:ok, order} = VariableOrdering.compute(patterns)

      # p should come first as predicates have lower cardinality
      assert hd(order) == "p"
    end

    test "prefers variables with more constants in same pattern" do
      # Pattern 1: x with two constants (knows, Alice)
      # Pattern 2: y with one constant (age)
      patterns = [
        triple(var("x"), iri("knows"), iri("Alice")),
        triple(var("y"), iri("age"), var("z"))
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      # x should come before y because its pattern has more constants
      x_pos = Enum.find_index(order, &(&1 == "x"))
      y_pos = Enum.find_index(order, &(&1 == "y"))

      assert x_pos < y_pos
    end

    test "uses statistics when available" do
      patterns = [
        triple(var("x"), iri("http://example.org/rare"), var("y")),
        triple(var("z"), iri("http://example.org/common"), var("w"))
      ]

      stats = %{
        {:predicate_count, "http://example.org/rare"} => 5,
        {:predicate_count, "http://example.org/common"} => 10_000
      }

      {:ok, order} = VariableOrdering.compute(patterns, stats)

      # x should come before z because rare predicate is more selective
      x_pos = Enum.find_index(order, &(&1 == "x"))
      z_pos = Enum.find_index(order, &(&1 == "z"))

      assert x_pos < z_pos
    end
  end

  # ===========================================================================
  # compute_with_info/2 Tests
  # ===========================================================================

  describe "compute_with_info/2" do
    test "returns variable info map" do
      patterns = [
        triple(var("x"), iri("knows"), var("y")),
        triple(var("y"), iri("age"), var("z"))
      ]

      {:ok, _order, var_infos} = VariableOrdering.compute_with_info(patterns)

      assert is_map(var_infos)
      assert Map.has_key?(var_infos, "x")
      assert Map.has_key?(var_infos, "y")
      assert Map.has_key?(var_infos, "z")

      # Check y's info
      y_info = var_infos["y"]
      assert y_info.name == "y"
      assert length(y_info.patterns) == 2
      assert :object in y_info.positions or :subject in y_info.positions
      assert is_float(y_info.selectivity)
      assert is_list(y_info.available_indices)
    end

    test "provides correct positions for variables" do
      patterns = [triple(var("s"), var("p"), var("o"))]

      {:ok, _order, var_infos} = VariableOrdering.compute_with_info(patterns)

      assert :subject in var_infos["s"].positions
      assert :predicate in var_infos["p"].positions
      assert :object in var_infos["o"].positions
    end

    test "provides available indices" do
      patterns = [triple(var("s"), iri("knows"), var("o"))]

      {:ok, _order, var_infos} = VariableOrdering.compute_with_info(patterns)

      # Subject can use SPO or OSP
      s_indices = var_infos["s"].available_indices
      assert :spo in s_indices or :osp in s_indices

      # Object can use OSP or POS
      o_indices = var_infos["o"].available_indices
      assert :osp in o_indices or :pos in o_indices
    end
  end

  # ===========================================================================
  # best_index_for/3 Tests
  # ===========================================================================

  describe "best_index_for/3" do
    test "uses SPO when subject is target and predicate/object are bound" do
      pattern = triple(var("s"), iri("knows"), iri("Alice"))
      bound_vars = MapSet.new()

      {index, _prefix} = VariableOrdering.best_index_for("s", pattern, bound_vars)

      # With P and O bound, POS is best for finding S
      assert index == :pos
    end

    test "uses POS when object is target and predicate is bound" do
      pattern = triple(var("s"), iri("knows"), var("o"))
      bound_vars = MapSet.new()

      {index, _prefix} = VariableOrdering.best_index_for("o", pattern, bound_vars)

      # With P bound, POS gives us P first
      assert index == :pos
    end

    test "uses SPO when object is target and subject is bound" do
      pattern = triple(var("s"), iri("knows"), var("o"))
      bound_vars = MapSet.new(["s"])

      {index, _prefix} = VariableOrdering.best_index_for("o", pattern, bound_vars)

      # With S bound and P constant, SPO lets us use S as prefix
      assert index == :spo
    end

    test "uses OSP when subject is target and object is bound" do
      pattern = triple(var("s"), var("p"), var("o"))
      bound_vars = MapSet.new(["o"])

      {index, _prefix} = VariableOrdering.best_index_for("s", pattern, bound_vars)

      # With O bound, OSP lets us use O as prefix to find S
      assert index == :osp
    end

    test "returns prefix variables for index" do
      pattern = triple(var("s"), iri("knows"), var("o"))
      bound_vars = MapSet.new(["s"])

      {_index, prefix} = VariableOrdering.best_index_for("o", pattern, bound_vars)

      # s is bound and should be in prefix
      assert "s" in prefix
    end
  end

  # ===========================================================================
  # estimate_selectivity/3 Tests
  # ===========================================================================

  describe "estimate_selectivity/3" do
    test "returns lower selectivity for variables in multiple patterns" do
      patterns_one = [triple(var("x"), iri("knows"), var("y"))]

      patterns_two = [
        triple(var("x"), iri("knows"), var("y")),
        triple(var("x"), iri("age"), var("z"))
      ]

      sel_one = VariableOrdering.estimate_selectivity("x", patterns_one)
      sel_two = VariableOrdering.estimate_selectivity("x", patterns_two)

      # x in two patterns should be more selective (lower score)
      assert sel_two < sel_one
    end

    test "returns lower selectivity for predicate position" do
      patterns = [triple(var("s"), var("p"), var("o"))]

      sel_s = VariableOrdering.estimate_selectivity("s", patterns)
      sel_p = VariableOrdering.estimate_selectivity("p", patterns)
      sel_o = VariableOrdering.estimate_selectivity("o", patterns)

      # Predicate should be most selective
      assert sel_p < sel_s
      assert sel_p < sel_o
    end

    test "returns lower selectivity when pattern has constants" do
      pattern_no_const = [triple(var("x"), var("p"), var("o"))]
      pattern_one_const = [triple(var("x"), iri("knows"), var("o"))]
      pattern_two_const = [triple(var("x"), iri("knows"), iri("Alice"))]

      sel_none = VariableOrdering.estimate_selectivity("x", pattern_no_const)
      sel_one = VariableOrdering.estimate_selectivity("x", pattern_one_const)
      sel_two = VariableOrdering.estimate_selectivity("x", pattern_two_const)

      # More constants = more selective
      assert sel_two < sel_one
      assert sel_one < sel_none
    end

    test "uses predicate statistics for selectivity" do
      patterns = [triple(var("x"), iri("http://rare"), var("y"))]

      stats_rare = %{{:predicate_count, "http://rare"} => 5}
      stats_common = %{{:predicate_count, "http://rare"} => 50_000}

      sel_rare = VariableOrdering.estimate_selectivity("x", patterns, stats_rare)
      sel_common = VariableOrdering.estimate_selectivity("x", patterns, stats_common)

      # Rare predicate should make variable more selective
      assert sel_rare < sel_common
    end
  end

  # ===========================================================================
  # Complex Query Patterns Tests
  # ===========================================================================

  describe "complex query patterns" do
    test "star query: variable at center should be first" do
      # Star pattern: ?person knows Alice, works_at ACME, lives_in NYC
      patterns = [
        triple(var("person"), iri("knows"), iri("Alice")),
        triple(var("person"), iri("works_at"), iri("ACME")),
        triple(var("person"), iri("lives_in"), iri("NYC"))
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      # person appears in all 3 patterns - should be first
      assert hd(order) == "person"
    end

    test "chain query: shared variables should come first" do
      # Chain: ?a knows ?b, ?b knows ?c, ?c knows ?d
      patterns = [
        triple(var("a"), iri("knows"), var("b")),
        triple(var("b"), iri("knows"), var("c")),
        triple(var("c"), iri("knows"), var("d"))
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      # b and c appear in 2 patterns each - should come before a and d
      b_pos = Enum.find_index(order, &(&1 == "b"))
      c_pos = Enum.find_index(order, &(&1 == "c"))
      a_pos = Enum.find_index(order, &(&1 == "a"))
      d_pos = Enum.find_index(order, &(&1 == "d"))

      assert b_pos < a_pos or b_pos < d_pos
      assert c_pos < a_pos or c_pos < d_pos
    end

    test "triangle query: all variables appear twice" do
      # Triangle: ?a knows ?b, ?b knows ?c, ?c knows ?a
      patterns = [
        triple(var("a"), iri("knows"), var("b")),
        triple(var("b"), iri("knows"), var("c")),
        triple(var("c"), iri("knows"), var("a"))
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      # All variables appear in exactly 2 patterns - order should be deterministic
      assert length(order) == 3
      assert "a" in order
      assert "b" in order
      assert "c" in order
    end

    test "mixed bound and unbound" do
      # Some patterns have constants, some don't
      patterns = [
        triple(iri("Alice"), iri("knows"), var("x")),
        triple(var("x"), var("p"), var("y")),
        triple(var("y"), iri("age"), literal("25"))
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      # x and y appear in 2 patterns each
      # x's first pattern has 2 constants
      # y's second pattern has 2 constants (predicate + literal)
      assert length(order) == 3
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles patterns with all constants" do
      patterns = [
        triple(iri("Alice"), iri("knows"), iri("Bob")),
        triple(var("x"), iri("knows"), var("y"))
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      # Should only return variables x and y
      assert length(order) == 2
      assert "x" in order
      assert "y" in order
    end

    test "handles blank nodes as constants" do
      patterns = [
        triple(var("x"), iri("knows"), {:blank_node, "b1"})
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      assert order == ["x"]
    end

    test "handles typed literals" do
      patterns = [
        triple(
          var("x"),
          iri("age"),
          {:literal, "25", "http://www.w3.org/2001/XMLSchema#integer", nil}
        )
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      assert order == ["x"]
    end

    test "handles single pattern with one variable" do
      patterns = [triple(iri("Alice"), iri("knows"), var("x"))]

      {:ok, order} = VariableOrdering.compute(patterns)

      assert order == ["x"]
    end

    test "handles many variables" do
      # 10 variables
      patterns = [
        triple(var("a"), var("b"), var("c")),
        triple(var("d"), var("e"), var("f")),
        triple(var("g"), var("h"), var("i")),
        # a appears twice
        triple(var("j"), iri("link"), var("a"))
      ]

      {:ok, order} = VariableOrdering.compute(patterns)

      assert length(order) == 10
      # a should come first as it appears in 2 patterns
      assert hd(order) == "a"
    end
  end
end
