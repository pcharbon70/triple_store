defmodule TripleStore.SPARQL.QuadCardinalityPropertyTest do
  @moduledoc """
  Property-based tests for QuadCardinality module (S22).

  Uses StreamData to test invariants of cardinality estimation with random inputs.
  """

  use ExUnit.Case
  use ExUnitProperties

  import StreamData

  alias TripleStore.SPARQL.QuadCardinality

  # Default statistics for testing
  @default_stats %{
    quad_count: 10_000,
    triple_count: 10_000,
    distinct_subjects: 1_000,
    distinct_predicates: 100,
    distinct_objects: 2_000,
    total_graphs: 5
  }

  # ===========================================================================
  # Property: Cardinality Non-Negativity
  # ===========================================================================

  property "estimate_pattern always returns non-negative values" do
    check all stats <- stats_generator(), pattern <- quad_pattern_generator() do
      result = QuadCardinality.estimate_pattern(pattern, stats)
      assert result >= 0
    end
  end

  property "fully-bound patterns estimate to 0 or 1" do
    check all stats <- stats_generator(),
              s <- term_id_gen(),
              p <- term_id_gen(),
              o <- term_id_gen(),
              g <- term_id_gen() do
      pattern = {:quad, {:bound, s}, {:bound, p}, {:bound, o}, {:bound, g}}
      result = QuadCardinality.estimate_pattern(pattern, stats)
      # Cardinality can be 0.0 or 1.0 for fully bound patterns
      assert result >= 0 and result <= 1
    end
  end

  # ===========================================================================
  # Property: Selectivity Monotonicity
  # ===========================================================================

  property "more specific patterns have lower or equal cardinality" do
    var_gen = StreamData.constant({:variable, "var"})

    check all stats <- stats_generator(),
              s <- term_id_gen(),
              p <- term_id_gen(),
              o <- term_id_gen(),
              g <- term_id_gen() do
      # All variables
      all_vars = {:quad, var_gen, var_gen, var_gen, var_gen}
      # One bound
      one_bound = {:quad, {:bound, s}, var_gen, var_gen, var_gen}
      # Two bound
      two_bound = {:quad, {:bound, s}, {:bound, p}, var_gen, var_gen}
      # Three bound
      three_bound = {:quad, {:bound, s}, {:bound, p}, {:bound, o}, var_gen}
      # All bound
      all_bound = {:quad, {:bound, s}, {:bound, p}, {:bound, o}, {:bound, g}}

      # Extract actual values from generators for cardinality estimation
      all_vars_pat = {:quad, {:variable, "var1"}, {:variable, "var2"}, {:variable, "var3"}, {:variable, "var4"}}
      one_bound_pat = {:quad, {:bound, s}, {:variable, "var"}, {:variable, "var"}, {:variable, "var"}}
      two_bound_pat = {:quad, {:bound, s}, {:bound, p}, {:variable, "var"}, {:variable, "var"}}
      three_bound_pat = {:quad, {:bound, s}, {:bound, p}, {:bound, o}, {:variable, "var"}}

      all_vars_card = QuadCardinality.estimate_pattern(all_vars_pat, stats)
      one_bound_card = QuadCardinality.estimate_pattern(one_bound_pat, stats)
      two_bound_card = QuadCardinality.estimate_pattern(two_bound_pat, stats)
      three_bound_card = QuadCardinality.estimate_pattern(three_bound_pat, stats)
      all_bound_card = QuadCardinality.estimate_pattern(all_bound, stats)

      # Each additional binding should not increase cardinality
      assert all_vars_card >= one_bound_card
      assert one_bound_card >= two_bound_card
      assert two_bound_card >= three_bound_card
      assert three_bound_card >= all_bound_card
    end
  end

  # ===========================================================================
  # Property: Bounding
  # ===========================================================================

  property "estimated cardinality never exceeds total quad count" do
    check all stats <- stats_generator(), pattern <- quad_pattern_generator() do
      result = QuadCardinality.estimate_pattern(pattern, stats)
      assert result <= stats.quad_count
    end
  end

  # ===========================================================================
  # Property: Idempotency
  # ===========================================================================

  property "estimate_pattern is idempotent for same inputs" do
    check all stats <- stats_generator(), pattern <- quad_pattern_generator() do
      result1 = QuadCardinality.estimate_pattern(pattern, stats)
      result2 = QuadCardinality.estimate_pattern(pattern, stats)
      assert result1 == result2
    end
  end

  # ===========================================================================
  # Property: Range Selectivity
  # ===========================================================================

  @tag :skip
  property "range selectivity is between 0 and 1" do
    check all stats <- stats_generator(),
              predicate <- term_id_gen(),
              min_val <- StreamData.integer(0, 1000),
              max_val <- StreamData.integer(0, 1000) do
      {low, high} = if min_val <= max_val, do: {min_val, max_val}, else: {max_val, min_val}

      selectivity =
        QuadCardinality.range_selectivity(
          stats,
          predicate,
          low / 1.0,
          high / 1.0
        )

      assert selectivity >= 0.0
      assert selectivity <= 1.0
    end
  end

  # ===========================================================================
  # Generators
  # ===========================================================================

  defp stats_generator do
    gen all quad_count <- positive_integer(),
            distinct_subjects <- positive_integer(),
            distinct_predicates <- positive_integer(),
            distinct_objects <- positive_integer(),
            total_graphs <- positive_integer() do
      quad_count = min(quad_count, 1_000_000)
      distinct_subjects = min(distinct_subjects, quad_count)
      distinct_predicates = min(distinct_predicates, quad_count)
      distinct_objects = min(distinct_objects, quad_count)
      total_graphs = min(total_graphs, quad_count)

      %{
        quad_count: quad_count,
        triple_count: quad_count,
        distinct_subjects: distinct_subjects,
        distinct_predicates: distinct_predicates,
        distinct_objects: distinct_objects,
        total_graphs: total_graphs
      }
    end
  end

  defp quad_pattern_generator do
    gen all s <- pattern_term_gen(),
            p <- pattern_term_gen(),
            o <- pattern_term_gen(),
            g <- pattern_term_gen() do
      {:quad, s, p, o, g}
    end
  end

  defp pattern_term_gen do
    frequency([
      {7, StreamData.constant({:variable, "var"})},
      {3, map(term_id_gen(), &{:bound, &1})}
    ])
  end

  defp term_id_gen do
    map(positive_integer(), &min(&1, 1_000_000))
  end
end
