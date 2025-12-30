defmodule TripleStore.SPARQL.CardinalityTest do
  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.Cardinality

  # ===========================================================================
  # Test Helpers
  # ===========================================================================

  defp var(name), do: {:variable, name}
  defp triple(s, p, o), do: {:triple, s, p, o}

  # Standard test stats
  defp default_stats do
    %{
      triple_count: 10_000,
      distinct_subjects: 1_000,
      distinct_predicates: 100,
      distinct_objects: 2_000,
      predicate_histogram: %{
        42 => 500,
        43 => 1_500,
        44 => 100,
        45 => 5_000
      }
    }
  end

  # ===========================================================================
  # Pattern Cardinality - Basic Tests
  # ===========================================================================

  describe "estimate_pattern/2 - basic" do
    test "fully unbound pattern returns total triple count" do
      stats = default_stats()
      pattern = triple(var("s"), var("p"), var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      assert card == 10_000.0
    end

    test "pattern with known predicate uses histogram count" do
      stats = default_stats()
      pattern = triple(var("s"), 42, var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      assert card == 500.0
    end

    test "pattern with unknown predicate uses total count" do
      stats = default_stats()
      pattern = triple(var("s"), 999, var("o"))

      # Unknown predicate, but still bound - applies 1/distinct_predicates
      card = Cardinality.estimate_pattern(pattern, stats)

      # 10000 * (1/100) = 100
      assert card == 100.0
    end

    test "pattern with empty stats uses defaults" do
      stats = %{}
      pattern = triple(var("s"), var("p"), var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      # Default triple count
      assert card == 10_000.0
    end

    test "cardinality is always >= 1.0" do
      stats = %{
        triple_count: 10,
        distinct_subjects: 10_000,
        distinct_predicates: 10_000,
        distinct_objects: 10_000
      }

      # All positions bound - very selective
      pattern = triple(1, 2, 3)

      card = Cardinality.estimate_pattern(pattern, stats)

      assert card >= 1.0
    end
  end

  # ===========================================================================
  # Pattern Cardinality - Bound Subject Tests
  # ===========================================================================

  describe "estimate_pattern/2 - bound subject" do
    test "bound subject reduces cardinality" do
      stats = default_stats()
      # Only subject bound, pattern uses histogram predicate
      pattern = triple(123, 42, var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      # 500 * (1/1000) = 0.5, but min is 1.0
      assert card == 1.0
    end

    test "bound subject with unbound predicate" do
      stats = default_stats()
      pattern = triple(123, var("p"), var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      # 10000 * (1/1000) = 10
      assert card == 10.0
    end

    test "integer ID as subject" do
      stats = default_stats()
      pattern = triple(42, var("p"), var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      # 10000 * (1/1000) = 10
      assert card == 10.0
    end
  end

  # ===========================================================================
  # Pattern Cardinality - Bound Predicate Tests
  # ===========================================================================

  describe "estimate_pattern/2 - bound predicate" do
    test "bound predicate with high frequency" do
      stats = default_stats()
      pattern = triple(var("s"), 45, var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      # Predicate 45 has 5000 triples
      assert card == 5_000.0
    end

    test "bound predicate with low frequency" do
      stats = default_stats()
      pattern = triple(var("s"), 44, var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      # Predicate 44 has 100 triples
      assert card == 100.0
    end

    test "multiple bound predicates in histogram match their counts" do
      stats = default_stats()

      assert Cardinality.estimate_pattern(triple(var("s"), 42, var("o")), stats) == 500.0
      assert Cardinality.estimate_pattern(triple(var("s"), 43, var("o")), stats) == 1_500.0
      assert Cardinality.estimate_pattern(triple(var("s"), 44, var("o")), stats) == 100.0
      assert Cardinality.estimate_pattern(triple(var("s"), 45, var("o")), stats) == 5_000.0
    end
  end

  # ===========================================================================
  # Pattern Cardinality - Bound Object Tests
  # ===========================================================================

  describe "estimate_pattern/2 - bound object" do
    test "bound object reduces cardinality" do
      stats = default_stats()
      pattern = triple(var("s"), var("p"), 999)

      card = Cardinality.estimate_pattern(pattern, stats)

      # 10000 * (1/2000) = 5
      assert card == 5.0
    end

    test "bound object with bound predicate" do
      stats = default_stats()
      pattern = triple(var("s"), 42, 999)

      card = Cardinality.estimate_pattern(pattern, stats)

      # 500 * (1/2000) = 0.25, min is 1.0
      assert card == 1.0
    end
  end

  # ===========================================================================
  # Pattern Cardinality - Multiple Bound Tests
  # ===========================================================================

  describe "estimate_pattern/2 - multiple bound positions" do
    test "subject and object bound" do
      stats = default_stats()
      pattern = triple(123, var("p"), 456)

      card = Cardinality.estimate_pattern(pattern, stats)

      # 10000 * (1/1000) * (1/2000) = 0.005, min is 1.0
      assert card == 1.0
    end

    test "subject and predicate bound" do
      stats = default_stats()
      pattern = triple(123, 42, var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      # 500 * (1/1000) = 0.5, min is 1.0
      assert card == 1.0
    end

    test "all positions bound" do
      stats = default_stats()
      pattern = triple(123, 42, 456)

      card = Cardinality.estimate_pattern(pattern, stats)

      # Very selective, but min is 1.0
      assert card == 1.0
    end
  end

  # ===========================================================================
  # Pattern Cardinality - With Bindings Tests
  # ===========================================================================

  describe "estimate_pattern_with_bindings/3" do
    test "unbound variable in bindings has no effect" do
      stats = default_stats()
      pattern = triple(var("s"), 42, var("o"))
      bindings = %{}

      card = Cardinality.estimate_pattern_with_bindings(pattern, stats, bindings)

      assert card == 500.0
    end

    test "bound variable reduces cardinality" do
      stats = default_stats()
      pattern = triple(var("s"), 42, var("o"))
      # Subject is bound to 100 distinct values (out of 1000)
      bindings = %{"s" => 100}

      card = Cardinality.estimate_pattern_with_bindings(pattern, stats, bindings)

      # 500 * (100/1000) = 50
      assert card == 50.0
    end

    test "multiple bound variables" do
      stats = default_stats()
      pattern = triple(var("s"), var("p"), var("o"))
      bindings = %{"s" => 100, "p" => 10}

      card = Cardinality.estimate_pattern_with_bindings(pattern, stats, bindings)

      # 10000 * (100/1000) * (10/100) = 100
      assert card == 100.0
    end

    test "binding larger than domain is capped" do
      stats = default_stats()
      pattern = triple(var("s"), 42, var("o"))
      # Binding claims more values than exist
      bindings = %{"s" => 5000}

      card = Cardinality.estimate_pattern_with_bindings(pattern, stats, bindings)

      # Should be capped at 1.0 selectivity
      assert card == 500.0
    end
  end

  # ===========================================================================
  # Join Cardinality - Basic Tests
  # ===========================================================================

  describe "estimate_join/4 - basic" do
    test "cartesian product with no join variables" do
      stats = default_stats()

      card = Cardinality.estimate_join(100.0, 50.0, [], stats)

      assert card == 5_000.0
    end

    test "join with single variable reduces cardinality" do
      stats = default_stats()

      card = Cardinality.estimate_join(1000.0, 500.0, ["x"], stats)

      # Should be less than cartesian product
      assert card < 1000.0 * 500.0
      assert card >= 1.0
    end

    test "join with multiple variables is more selective" do
      stats = default_stats()

      single_var_card = Cardinality.estimate_join(1000.0, 500.0, ["x"], stats)
      multi_var_card = Cardinality.estimate_join(1000.0, 500.0, ["x", "y"], stats)

      # More join variables = more selective
      assert multi_var_card < single_var_card
    end

    test "symmetric join produces same result" do
      stats = default_stats()

      card1 = Cardinality.estimate_join(100.0, 200.0, ["x"], stats)
      card2 = Cardinality.estimate_join(200.0, 100.0, ["x"], stats)

      assert_in_delta card1, card2, 0.001
    end

    test "small cardinalities don't produce zero" do
      stats = default_stats()

      card = Cardinality.estimate_join(1.0, 1.0, ["x"], stats)

      assert card >= 1.0
    end
  end

  # ===========================================================================
  # Join Cardinality - Realistic Scenarios
  # ===========================================================================

  describe "estimate_join/4 - scenarios" do
    test "highly selective join" do
      stats = default_stats()

      # Small left, large right
      card = Cardinality.estimate_join(10.0, 10_000.0, ["x"], stats)

      # Should be closer to smaller input than cartesian
      assert card < 100_000.0
      assert card >= 1.0
    end

    test "join of similar-sized inputs" do
      stats = default_stats()

      card = Cardinality.estimate_join(500.0, 500.0, ["x"], stats)

      # With reasonable selectivity
      assert card < 250_000.0
      assert card >= 1.0
    end
  end

  # ===========================================================================
  # Multi-Pattern Cardinality Tests
  # ===========================================================================

  describe "estimate_multi_pattern/2" do
    test "single pattern" do
      stats = default_stats()
      patterns = [triple(var("s"), 42, var("o"))]

      card = Cardinality.estimate_multi_pattern(patterns, stats)

      assert card == 500.0
    end

    test "empty patterns returns minimum" do
      stats = default_stats()

      card = Cardinality.estimate_multi_pattern([], stats)

      assert card == 1.0
    end

    test "two patterns with shared variable" do
      stats = default_stats()

      patterns = [
        # 500 results
        triple(var("s"), 42, var("y")),
        # 1500 results
        triple(var("y"), 43, var("o"))
      ]

      card = Cardinality.estimate_multi_pattern(patterns, stats)

      # Join on y, should be much less than 500 * 1500
      assert card < 750_000.0
      assert card >= 1.0
    end

    test "two patterns without shared variable (cartesian)" do
      stats = default_stats()

      patterns = [
        # 500 results
        triple(var("a"), 42, var("b")),
        # 1500 results
        triple(var("c"), 43, var("d"))
      ]

      card = Cardinality.estimate_multi_pattern(patterns, stats)

      # Cartesian product
      assert card == 500.0 * 1500.0
    end

    test "three pattern chain" do
      stats = default_stats()

      patterns = [
        # 500
        triple(var("a"), 42, var("b")),
        # 1500
        triple(var("b"), 43, var("c")),
        # 100
        triple(var("c"), 44, var("d"))
      ]

      card = Cardinality.estimate_multi_pattern(patterns, stats)

      # Multiple joins, should be much less than product
      assert card < 500.0 * 1500.0 * 100.0
      assert card >= 1.0
    end

    test "star pattern (shared subject)" do
      stats = default_stats()

      patterns = [
        # 500
        triple(var("s"), 42, var("a")),
        # 1500
        triple(var("s"), 43, var("b")),
        # 100
        triple(var("s"), 44, var("c"))
      ]

      card = Cardinality.estimate_multi_pattern(patterns, stats)

      # All join on s, very selective
      assert card < 500.0 * 1500.0 * 100.0
      assert card >= 1.0
    end
  end

  # ===========================================================================
  # Selectivity Tests
  # ===========================================================================

  describe "estimate_selectivity/2" do
    test "fully unbound pattern has selectivity 1.0" do
      stats = default_stats()
      pattern = triple(var("s"), var("p"), var("o"))

      sel = Cardinality.estimate_selectivity(pattern, stats)

      assert sel == 1.0
    end

    test "bound predicate with histogram" do
      stats = default_stats()
      pattern = triple(var("s"), 42, var("o"))

      sel = Cardinality.estimate_selectivity(pattern, stats)

      # 500 / 10000 = 0.05
      assert_in_delta sel, 0.05, 0.001
    end

    test "highly selective pattern" do
      stats = default_stats()
      pattern = triple(1, 42, 100)

      sel = Cardinality.estimate_selectivity(pattern, stats)

      # Very low, but > 0
      assert sel > 0
      assert sel < 0.01
    end

    test "selectivity is capped at 1.0" do
      # Edge case: pattern cardinality > total (shouldn't happen, but handle it)
      stats = %{triple_count: 100}
      pattern = triple(var("s"), var("p"), var("o"))

      sel = Cardinality.estimate_selectivity(pattern, stats)

      assert sel <= 1.0
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "zero triple count uses minimum cardinality" do
      stats = %{triple_count: 0}
      pattern = triple(var("s"), var("p"), var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      assert card >= 1.0
    end

    test "empty predicate histogram falls back to total count" do
      stats = %{triple_count: 5000, predicate_histogram: %{}}
      pattern = triple(var("s"), 42, var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      # Falls back to count-based estimate
      assert card > 0
    end

    test "named_node predicate without ID" do
      stats = default_stats()
      pattern = triple(var("s"), {:named_node, "http://example.org/pred"}, var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      # Can't look up in histogram, uses selectivity-based estimate
      assert card > 0
    end

    test "literal object" do
      stats = default_stats()
      pattern = triple(var("s"), 42, {:literal, "hello", {:named_node, "xsd:string"}})

      card = Cardinality.estimate_pattern(pattern, stats)

      # Literal is treated as constant
      assert card > 0
    end

    test "blank_node subject" do
      stats = default_stats()
      pattern = triple({:blank_node, "b0"}, var("p"), var("o"))

      card = Cardinality.estimate_pattern(pattern, stats)

      # Blank node is treated as constant
      assert card > 0
    end
  end

  # ===========================================================================
  # Integration Tests
  # ===========================================================================

  describe "integration" do
    test "realistic LUBM-like query pattern" do
      # Simulate a LUBM benchmark query pattern
      stats = %{
        triple_count: 100_000,
        distinct_subjects: 10_000,
        distinct_predicates: 20,
        distinct_objects: 15_000,
        predicate_histogram: %{
          # rdf:type
          1 => 10_000,
          # ub:takesCourse
          2 => 5_000,
          # ub:teacherOf
          3 => 1_000,
          # ub:memberOf
          4 => 10_000,
          # ub:headOf
          5 => 500
        }
      }

      # Query: ?x type Student, ?x takesCourse ?y, ?y teacherOf ?z
      patterns = [
        # ?x type Student (100 = Student class ID)
        triple(var("x"), 1, 100),
        # ?x takesCourse ?y
        triple(var("x"), 2, var("y")),
        # ?y teacherOf ?z
        triple(var("y"), 3, var("z"))
      ]

      card = Cardinality.estimate_multi_pattern(patterns, stats)

      # Should produce a reasonable estimate
      assert card >= 1.0
      # Much less than full product
      assert card < 100_000 * 5_000 * 1_000
    end

    test "star query pattern" do
      stats = default_stats()

      # Find entities with multiple properties
      patterns = [
        triple(var("x"), 42, var("a")),
        triple(var("x"), 43, var("b")),
        triple(var("x"), 44, var("c")),
        triple(var("x"), 45, var("d"))
      ]

      card = Cardinality.estimate_multi_pattern(patterns, stats)

      # Very selective due to multiple constraints on x
      assert card >= 1.0
    end
  end
end
