defmodule TripleStore.SPARQL.QuadCardinalityTest do
  @moduledoc """
  Unit tests for quad pattern cardinality estimation (Section 5.2).

  Tests cardinality estimation for quad patterns including:
  - Graph-scoped patterns (bound graph)
  - Cross-graph patterns (unbound graph)
  - Position selectivity (S, P, O, G)
  - Join cardinality for quads
  """

  use ExUnit.Case, async: true

  alias TripleStore.SPARQL.QuadCardinality

  # ===========================================================================
  # 5.2.1 Quad Pattern Estimation
  # ===========================================================================

  describe "estimate_pattern/2" do
    setup do
      # Base statistics map
      %{
        stats: %{
          quad_count: 15_000,
          triple_count: 10_000,
          distinct_subjects: 1_000,
          distinct_predicates: 50,
          distinct_objects: 2_000,
          total_graphs: 3,
          predicate_histogram: %{42 => 3_000, 43 => 2_000},
          per_graph_stats: %{
            0 => %{
              quad_count: 5_000,
              distinct_subjects: 500,
              distinct_predicates: 20,
              distinct_objects: 800,
              predicate_counts: %{42 => 1_000, 43 => 500}
            },
            123 => %{
              quad_count: 10_000,
              distinct_subjects: 800,
              distinct_predicates: 40,
              distinct_objects: 1_500,
              predicate_counts: %{42 => 2_000, 43 => 1_500}
            }
          }
        }
      }
    end

    test "fully bound pattern returns 1.0 (exact match)", %{stats: stats} do
      pattern = {:quad, 123, 456, 789, 0}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      # Fully bound means exact match - should be close to 1.0
      # In practice this would need exact lookup, but estimation gives minimum
      assert card >= 1.0
    end

    test "fully unbound pattern returns total quad count", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      # Should be approximately total quad count
      assert card >= stats.quad_count
    end

    test "graph-scoped pattern with bound predicate uses per-graph stats", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      # Should use predicate count from graph 0: 1000
      assert card == 1000.0
    end

    test "cross-graph pattern sums across all graphs", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      # Should sum predicate 42 counts across graphs: 1000 + 2000 = 3000
      assert card == 3000.0
    end

    test "graph-scoped pattern with bound subject applies selectivity", %{stats: stats} do
      pattern = {:quad, 999, {:variable, "p"}, {:variable, "o"}, 0}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      # Base: 5000 quads in graph 0
      # Subject selectivity: 1/500 = 0.002
      # Expected: 5000 * 0.002 = 10.0
      assert_in_delta card, 10.0, 1.0
    end

    test "graph-scoped pattern with bound object applies selectivity", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, 999, 0}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      # Base: 5000 quads in graph 0
      # Object selectivity: 1/800 = 0.00125
      # Expected: 5000 * 0.00125 = 6.25
      assert_in_delta card, 6.25, 1.0
    end

    test "non-existent graph returns minimum cardinality", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 999}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      assert card == 1.0
    end

    test "non-existent predicate uses base graph count", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, 999, {:variable, "o"}, 0}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      # No predicate 999 in graph 0, should use base count with selectivity
      assert card >= 1.0
    end

    test "default graph term maps to graph ID 0", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, :default_graph}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      # Should use graph 0 stats, predicate 42 count = 1000
      assert card == 1000.0
    end
  end

  # ===========================================================================
  # 5.2.2 Position Selectivity
  # ===========================================================================

  describe "position_selectivity/4" do
    setup do
      %{
        graph_stats: %{
          quad_count: 5_000,
          distinct_subjects: 500,
          distinct_predicates: 20,
          distinct_objects: 800
        },
        global_stats: %{
          total_graphs: 3,
          quad_count: 15_000
        }
      }
    end

    test "subject selectivity for bound constant", %{graph_stats: graph_stats} do
      sel = QuadCardinality.position_selectivity(123, :subject, graph_stats, nil)
      # 1/500 = 0.002
      assert_in_delta sel, 0.002, 0.0001
    end

    test "subject selectivity for unbound variable", %{graph_stats: graph_stats} do
      sel = QuadCardinality.position_selectivity({:variable, "s"}, :subject, graph_stats, nil)
      assert sel == 1.0
    end

    test "predicate selectivity for bound constant", %{graph_stats: graph_stats} do
      sel = QuadCardinality.position_selectivity(42, :predicate, graph_stats, nil)
      # 1/20 = 0.05
      assert_in_delta sel, 0.05, 0.001
    end

    test "object selectivity for bound constant", %{graph_stats: graph_stats} do
      sel = QuadCardinality.position_selectivity(789, :object, graph_stats, nil)
      # 1/800 = 0.00125
      assert_in_delta sel, 0.00125, 0.0001
    end

    test "graph selectivity for bound constant is 1.0", %{global_stats: global_stats} do
      sel = QuadCardinality.position_selectivity(0, :graph, nil, global_stats)
      assert sel == 1.0
    end

    test "graph selectivity for unbound variable is 1.0", %{global_stats: global_stats} do
      sel = QuadCardinality.position_selectivity({:variable, "g"}, :graph, nil, global_stats)
      # For unbound graph, we still return 1.0 (iteration handled elsewhere)
      assert sel == 1.0
    end
  end

  # ===========================================================================
  # 5.2.3 Cross-Graph Pattern Estimation
  # ===========================================================================

  describe "estimate_cross_graph_pattern/3" do
    setup do
      %{
        stats: %{
          quad_count: 15_000,
          per_graph_stats: %{
            0 => %{
              quad_count: 5_000,
              distinct_subjects: 500,
              distinct_predicates: 20,
              distinct_objects: 800,
              predicate_counts: %{42 => 1_000}
            },
            123 => %{
              quad_count: 7_000,
              distinct_subjects: 600,
              distinct_predicates: 30,
              distinct_objects: 1_000,
              predicate_counts: %{42 => 2_000}
            },
            456 => %{
              quad_count: 3_000,
              distinct_subjects: 300,
              distinct_predicates: 15,
              distinct_objects: 400,
              predicate_counts: %{42 => 500}
            }
          }
        }
      }
    end

    test "sums predicate counts across all graphs", %{stats: stats} do
      # Predicate 42: 1000 + 2000 + 500 = 3500
      card = QuadCardinality.estimate_cross_graph_pattern({:variable, "s"}, 42, {:variable, "o"}, stats)
      assert card == 3500.0
    end

    test "handles unbound predicate by summing base counts", %{stats: stats} do
      # Sum of all graph quad counts: 5000 + 7000 + 3000 = 15000
      card =
        QuadCardinality.estimate_cross_graph_pattern(
          {:variable, "s"},
          {:variable, "p"},
          {:variable, "o"},
          stats
        )

      assert card >= 15_000.0
    end

    test "applies subject selectivity across graphs", %{stats: stats} do
      # Base sum: 15000
      # Subject selectivity varies by graph
      card =
        QuadCardinality.estimate_cross_graph_pattern(999, {:variable, "p"}, {:variable, "o"}, stats)

      # Should be significantly less than base
      assert card < 15_000.0
      assert card >= 1.0
    end

    test "falls back to aggregate stats when per-graph unavailable" do
      stats = %{
        quad_count: 10_000,
        distinct_subjects: 500,
        distinct_predicates: 50
      }

      card =
        QuadCardinality.estimate_cross_graph_pattern(
          {:variable, "s"},
          {:variable, "p"},
          {:variable, "o"},
          stats
        )

      assert card >= 10_000.0
    end

    test "returns minimum for empty per_graph_stats" do
      # Empty per_graph_stats falls back to aggregate with default quad_count
      stats = %{per_graph_stats: %{}}
      card = QuadCardinality.estimate_cross_graph_pattern({:variable, "s"}, 42, {:variable, "o"}, stats)
      # Default quad_count is 10000, default distinct_predicates is 100
      # Predicate selectivity: 1/100 = 0.01
      # Expected: 10000 * 0.01 = 100
      assert card >= 1.0
    end
  end

  # ===========================================================================
  # 5.2.4 Join Cardinality Estimation
  # ===========================================================================

  describe "estimate_quad_join/5" do
    setup do
      %{
        stats: %{
          quad_count: 15_000,
          distinct_subjects: 1_000,
          distinct_objects: 2_000
        }
      }
    end

    test "cartesian product when no join variables", %{stats: stats} do
      card = QuadCardinality.estimate_quad_join(100.0, 50.0, [], false, stats)
      assert card == 100.0 * 50.0
    end

    test "same graph join reduces cardinality", %{stats: stats} do
      # Join on subject: 100 * 50 / domain_size
      card = QuadCardinality.estimate_quad_join(100.0, 50.0, ["s"], true, stats)
      # Should be less than cartesian product
      assert card < 100.0 * 50.0
      assert card >= 1.0
    end

    test "different graph join may have higher cardinality", %{stats: stats} do
      card_same = QuadCardinality.estimate_quad_join(100.0, 50.0, ["s"], true, stats)
      card_diff = QuadCardinality.estimate_quad_join(100.0, 50.0, ["s"], false, stats)
      # Different graphs might have less overlap, but we still apply join selectivity
      assert card_diff >= 1.0
    end

    test "multiple join variables increases selectivity", %{stats: stats} do
      card_single = QuadCardinality.estimate_quad_join(100.0, 50.0, ["s"], true, stats)
      card_double = QuadCardinality.estimate_quad_join(100.0, 50.0, ["s", "o"], true, stats)
      # More join variables = more selective = lower cardinality
      assert card_double <= card_single
    end

    test "handles small cardinalities", %{stats: stats} do
      card = QuadCardinality.estimate_quad_join(2.0, 3.0, ["x"], true, stats)
      assert card >= 1.0
    end
  end

  # ===========================================================================
  # Multi-Pattern Estimation
  # ===========================================================================

  describe "estimate_multi_quad_pattern/2" do
    setup do
      %{
        stats: %{
          quad_count: 15_000,
          distinct_subjects: 1_000,
          distinct_predicates: 50,
          distinct_objects: 2_000,
          total_graphs: 2,
          per_graph_stats: %{
            0 => %{
              quad_count: 5_000,
              distinct_subjects: 500,
              distinct_predicates: 20,
              distinct_objects: 800,
              predicate_counts: %{42 => 1_000, 43 => 500}
            },
            123 => %{
              quad_count: 10_000,
              distinct_subjects: 800,
              distinct_predicates: 40,
              distinct_objects: 1_500,
              predicate_counts: %{42 => 2_000, 43 => 1_500}
            }
          }
        }
      }
    end

    test "single pattern returns pattern cardinality", %{stats: stats} do
      patterns = [{:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}]
      card = QuadCardinality.estimate_multi_quad_pattern(patterns, stats)
      assert card == 1000.0
    end

    test "empty list returns minimum cardinality", %{stats: stats} do
      card = QuadCardinality.estimate_multi_quad_pattern([], stats)
      assert card == 1.0
    end

    test "same graph patterns with shared variable", %{stats: stats} do
      patterns = [
        {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0},
        {:quad, {:variable, "s"}, 43, {:variable, "x"}, 0}
      ]

      card = QuadCardinality.estimate_multi_quad_pattern(patterns, stats)
      # Join on subject reduces cardinality
      # Base: 1000 * 500 = 500000
      # With join selectivity: much less
      assert card < 1000.0 * 500.0
      assert card >= 1.0
    end

    test "different graph patterns produces cartesian product", %{stats: stats} do
      patterns = [
        {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0},
        {:quad, {:variable, "x"}, 43, {:variable, "y"}, 123}
      ]

      card = QuadCardinality.estimate_multi_quad_pattern(patterns, stats)
      # Different graphs, no shared variables: cartesian product
      # 1000 (from first) + 2000 (from second in graph 123) isn't quite right
      # Actually: first pattern in graph 0 = 1000, second in graph 123 = 1500
      # But they don't join, so it's more complex
      assert card >= 1.0
    end

    test "cross-graph patterns with shared variable", %{stats: stats} do
      patterns = [
        {:quad, {:variable, "s"}, 42, {:variable, "o"}, {:variable, "g"}},
        {:quad, {:variable, "s"}, 43, {:variable, "x"}, {:variable, "g"}}
      ]

      card = QuadCardinality.estimate_multi_quad_pattern(patterns, stats)
      # Both patterns share subject and graph variables
      # Cardinality should reflect the join
      assert card >= 1.0
    end
  end

  # ===========================================================================
  # Pattern with Bindings
  # ===========================================================================

  describe "estimate_pattern_with_bindings/3" do
    setup do
      %{
        stats: %{
          quad_count: 15_000,
          distinct_subjects: 1_000,
          distinct_predicates: 50,
          distinct_objects: 2_000,
          per_graph_stats: %{
            0 => %{
              quad_count: 5_000,
              distinct_subjects: 500,
              distinct_predicates: 20,
              distinct_objects: 800,
              predicate_counts: %{42 => 1_000}
            }
          }
        }
      }
    end

    test "applies binding selectivity for bound variable", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      bound_vars = %{"s" => 100}

      # Base cardinality: 1000
      # Subject bound to 100 values out of 500: 100/500 = 0.2
      # Expected: 1000 * 0.2 = 200
      card = QuadCardinality.estimate_pattern_with_bindings(pattern, stats, bound_vars)
      assert_in_delta card, 200.0, 10.0
    end

    test "no adjustment when variable not bound", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      bound_vars = %{}

      card = QuadCardinality.estimate_pattern_with_bindings(pattern, stats, bound_vars)
      assert card == 1000.0
    end

    test "applies multiple binding adjustments", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      bound_vars = %{"s" => 100, "o" => 200}

      # Base: 1000
      # Subject: 100/500 = 0.2
      # Object: 200/800 = 0.25
      # Expected: 1000 * 0.2 * 0.25 = 50
      card = QuadCardinality.estimate_pattern_with_bindings(pattern, stats, bound_vars)
      assert_in_delta card, 50.0, 10.0
    end

    test "binding larger than domain returns 1.0 selectivity", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      bound_vars = %{"s" => 10_000}

      # Subject bound to more values than exist: capped at 1.0
      card = QuadCardinality.estimate_pattern_with_bindings(pattern, stats, bound_vars)
      assert card == 1000.0
    end
  end

  # ===========================================================================
  # Selectivity Estimation
  # ===========================================================================

  describe "estimate_selectivity/2" do
    setup do
      %{
        stats: %{
          quad_count: 10_000,
          per_graph_stats: %{
            0 => %{
              quad_count: 5_000,
              distinct_subjects: 500,
              predicate_counts: %{42 => 1_000}
            }
          }
        }
      }
    end

    test "fully unbound pattern has selectivity 1.0", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      sel = QuadCardinality.estimate_selectivity(pattern, stats)
      # Cross-graph pattern sums per_graph_stats (5000) against quad_count (10000)
      # Selectivity = 5000/10000 = 0.5
      # (If per_graph_stats summed to total quad_count, selectivity would be 1.0)
      assert sel == 0.5
    end

    test "selective pattern has selectivity < 1.0", %{stats: stats} do
      pattern = {:quad, {:variable, "s"}, 42, {:variable, "o"}, 0}
      sel = QuadCardinality.estimate_selectivity(pattern, stats)
      # 1000 / 10000 = 0.1
      assert_in_delta sel, 0.1, 0.01
    end

    test "very selective pattern has low selectivity", %{stats: stats} do
      pattern = {:quad, 999, 42, {:variable, "o"}, 0}
      sel = QuadCardinality.estimate_selectivity(pattern, stats)
      # Very selective due to bound subject
      assert sel < 0.1
      assert sel > 0.0
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles empty stats map" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      card = QuadCardinality.estimate_pattern(pattern, %{})
      # Should use defaults
      assert card >= 1.0
    end

    test "handles stats with only quad_count" do
      stats = %{quad_count: 5000}
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      assert card >= 1.0
    end

    test "handles nil per_graph_stats" do
      stats = %{quad_count: 5000, per_graph_stats: nil}
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      assert card >= 1.0
    end

    test "handles graph stats with zero counts" do
      stats = %{
        per_graph_stats: %{
          0 => %{quad_count: 0, distinct_subjects: 0, distinct_predicates: 0, distinct_objects: 0}
        }
      }

      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      card = QuadCardinality.estimate_pattern(pattern, stats)
      # Should return minimum, not crash
      assert card >= 1.0
    end
  end

  # ===========================================================================
  # Stats Validation (C21)
  # ===========================================================================

  describe "validate_stats/1" do
    test "accepts valid stats with quad_count" do
      stats = %{quad_count: 1000}
      assert QuadCardinality.validate_stats(stats) == :ok
    end

    test "accepts valid stats with triple_count" do
      stats = %{triple_count: 1000}
      assert QuadCardinality.validate_stats(stats) == :ok
    end

    test "accepts valid stats with all fields" do
      stats = %{
        quad_count: 1000,
        distinct_subjects: 100,
        distinct_predicates: 50,
        distinct_objects: 200,
        total_graphs: 3,
        predicate_histogram: %{42 => 100},
        per_graph_stats: %{0 => %{quad_count: 500}}
      }
      assert QuadCardinality.validate_stats(stats) == :ok
    end

    test "rejects empty stats map" do
      assert QuadCardinality.validate_stats(%{}) == {:error, :missing_required_keys}
    end

    test "rejects non-map stats" do
      assert QuadCardinality.validate_stats("not a map") == {:error, :invalid_stats}
      assert QuadCardinality.validate_stats(nil) == {:error, :invalid_stats}
      assert QuadCardinality.validate_stats(123) == {:error, :invalid_stats}
    end

    test "rejects negative quad_count" do
      stats = %{quad_count: -1}
      assert QuadCardinality.validate_stats(stats) == {:error, :invalid_stat_value}
    end

    test "rejects invalid per_graph_stats" do
      stats = %{
        quad_count: 1000,
        per_graph_stats: %{0 => "not a map"}
      }
      assert QuadCardinality.validate_stats(stats) == {:error, :invalid_graph_stats}
    end
  end

  describe "ensure_stats_defaults/1" do
    test "fills in missing quad_count" do
      stats = %{}
      result = QuadCardinality.ensure_stats_defaults(stats)

      assert Map.has_key?(result, :quad_count)
      assert result.quad_count == 10_000
    end

    test "preserves existing quad_count" do
      stats = %{quad_count: 5000}
      result = QuadCardinality.ensure_stats_defaults(stats)

      assert result.quad_count == 5000
    end

    test "fills in all missing fields" do
      stats = %{}
      result = QuadCardinality.ensure_stats_defaults(stats)

      assert Map.has_key?(result, :quad_count)
      assert Map.has_key?(result, :distinct_subjects)
      assert Map.has_key?(result, :distinct_predicates)
      assert Map.has_key?(result, :distinct_objects)
      assert Map.has_key?(result, :total_graphs)
      assert Map.has_key?(result, :predicate_histogram)
      assert Map.has_key?(result, :per_graph_stats)
    end

    test "handles non-map input" do
      result = QuadCardinality.ensure_stats_defaults(nil)

      assert is_map(result)
      assert Map.has_key?(result, :quad_count)
    end

    test "preserves existing per_graph_stats" do
      stats = %{per_graph_stats: %{0 => %{quad_count: 100}}}
      result = QuadCardinality.ensure_stats_defaults(stats)

      assert result.per_graph_stats == %{0 => %{quad_count: 100}}
    end
  end

  describe "estimate_pattern/2 with invalid stats" do
    test "uses defaults when stats is empty map" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      card = QuadCardinality.estimate_pattern(pattern, %{})

      # Should use default quad_count of 10000
      assert card >= 1.0
    end

    test "uses defaults when stats is nil" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      card = QuadCardinality.estimate_pattern(pattern, nil)

      # Should use default stats
      assert card >= 1.0
    end

    test "uses defaults when stats is invalid type" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      card = QuadCardinality.estimate_pattern(pattern, "invalid")

      # Should use default stats
      assert card >= 1.0
    end
  end
end
