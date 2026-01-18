defmodule TripleStore.ConstantsTest do
  @moduledoc """
  Tests for centralized Constants module (S18).
  """

  use ExUnit.Case

  alias TripleStore.Constants

  describe "default_query_timeout/0" do
    test "returns 30 seconds in milliseconds" do
      assert Constants.default_query_timeout() == 30_000
    end
  end

  describe "default_leapfrog_timeout/0" do
    test "returns 30 seconds in milliseconds" do
      assert Constants.default_leapfrog_timeout() == 30_000
    end
  end

  describe "default_batch_size/0" do
    test "returns 1000" do
      assert Constants.default_batch_size() == 1_000
    end
  end

  describe "default_max_cache_entries/0" do
    test "returns 10_000" do
      assert Constants.default_max_cache_entries() == 10_000
    end
  end

  describe "default_max_iterations/0" do
    test "returns 1_000_000" do
      assert Constants.default_max_iterations() == 1_000_000
    end
  end

  describe "default_parallel_threshold/0" do
    test "returns 1_000" do
      assert Constants.default_parallel_threshold() == 1_000
    end
  end

  describe "default_cache_ttl/0" do
    test "returns 5 minutes in milliseconds" do
      assert Constants.default_cache_ttl() == 300_000
    end
  end

  describe "default_plan_cache_size/0" do
    test "returns 1_000" do
      assert Constants.default_plan_cache_size() == 1_000
    end
  end

  describe "default_graph_id/0" do
    test "returns 0" do
      assert Constants.default_graph_id() == 0
    end
  end

  describe "default_stats/0" do
    test "returns default statistics map" do
      stats = Constants.default_stats()

      assert is_map(stats)
      assert stats.triple_count == 10_000
      assert stats.quad_count == 10_000
      assert stats.distinct_subjects == 1_000
      assert stats.distinct_predicates == 100
      assert stats.distinct_objects == 2_000
      assert stats.total_graphs == 1
    end
  end

  describe "default_cost_weights/0" do
    test "returns default cost weights map" do
      weights = Constants.default_cost_weights()

      assert is_map(weights)
      assert weights.comparison_cost == 1
      assert weights.hash_cost == 50
      assert weights.hash_probe_cost == 5
      assert weights.index_seek_cost == 10
      assert weights.sequential_read_cost == 1
      assert weights.memory_weight == 1
      assert weights.leapfrog_seek_cost == 10
      assert weights.leapfrog_comparison_cost == 1
      assert weights.hash_join_threshold == 100
    end
  end
end
