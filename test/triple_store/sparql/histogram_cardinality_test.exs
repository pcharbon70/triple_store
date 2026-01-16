defmodule TripleStore.SPARQL.HistogramCardinalityTest do
  @moduledoc """
  Tests for histogram-based cardinality estimation (S2).
  """

  use ExUnit.Case

  alias TripleStore.SPARQL.QuadCardinality

  @sample_histograms %{
    0 => %{10 => 100, 11 => 100, 12 => 100, 13 => 100, 14 => 100},
    1 => %{10 => 50, 11 => 50, 12 => 50},
    2 => %{20 => 200, 21 => 150}
  }

  @sample_stats %{
    quad_count: 5000,
    distinct_subjects: 500,
    distinct_predicates: 30
  }

  describe "estimate_with_histogram/2" do
    test "estimates graph-scoped pattern with bound predicate" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}
      estimate = QuadCardinality.estimate_with_histogram(pattern, @sample_histograms)
      assert estimate == 100.0
    end

    test "estimates graph-scoped pattern with variable predicate" do
      pattern = {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, 0}
      estimate = QuadCardinality.estimate_with_histogram(pattern, @sample_histograms)
      assert estimate == 500.0
    end

    test "estimates cross-graph pattern with bound predicate" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, {:variable, "g"}}
      estimate = QuadCardinality.estimate_with_histogram(pattern, @sample_histograms)
      assert estimate == 150.0
    end

    test "handles empty histograms" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}
      estimate = QuadCardinality.estimate_with_histogram(pattern, %{})
      assert estimate == 1.0
    end
  end

  describe "estimate_with_hybrid/3" do
    test "uses histogram when data is available" do
      pattern = {:quad, {:variable, "s"}, 10, {:variable, "o"}, 0}
      estimate = QuadCardinality.estimate_with_hybrid(pattern, @sample_histograms, @sample_stats)
      assert estimate == 100.0
    end

    test "falls back to statistics when histogram has no data" do
      pattern = {:quad, {:variable, "s"}, 999, {:variable, "o"}, 0}
      estimate = QuadCardinality.estimate_with_hybrid(pattern, @sample_histograms, @sample_stats)
      assert estimate > 0.01
    end
  end

  describe "get_predicate_counts_for_graph/2" do
    test "returns predicate counts for existing graph" do
      counts = QuadCardinality.get_predicate_counts_for_graph(0, @sample_histograms)
      assert map_size(counts) == 5
      assert counts[10] == 100
    end

    test "returns empty map for non-existent graph" do
      counts = QuadCardinality.get_predicate_counts_for_graph(999, @sample_histograms)
      assert counts == %{}
    end
  end

  describe "get_predicate_count/3" do
    test "returns count for existing predicate" do
      count = QuadCardinality.get_predicate_count(0, 10, @sample_histograms)
      assert count == 100
    end

    test "returns nil for non-existent predicate" do
      count = QuadCardinality.get_predicate_count(0, 999, @sample_histograms)
      assert is_nil(count)
    end
  end
end
