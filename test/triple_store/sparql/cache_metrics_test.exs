defmodule TripleStore.SPARQL.CacheMetricsTest do
  @moduledoc """
  Tests for CacheMetrics module (S24).
  """

  use ExUnit.Case

  alias TripleStore.SPARQL.CacheMetrics

  # ===========================================================================
  # Test Setup
  # ===========================================================================

  setup do
    # Start the CacheMetrics server for tests
    {:ok, pid} = start_supervised(CacheMetrics)

    # Reset metrics before each test
    CacheMetrics.reset_metrics()

    %{pid: pid}
  end

  # ===========================================================================
  # Basic Metrics Tests
  # ===========================================================================

  describe "record_hit/2" do
    test "increments hit count" do
      assert CacheMetrics.record_hit(:plan_cache) == :ok
      assert CacheMetrics.record_hit(:plan_cache) == :ok

      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      assert metrics.hits == 2
      assert metrics.misses == 0
    end

    test "accepts metadata" do
      assert CacheMetrics.record_hit(:stats_cache, %{query: "test"}) == :ok

      {:ok, metrics} = CacheMetrics.metrics(:stats_cache)

      assert metrics.hits == 1
    end
  end

  describe "record_miss/2" do
    test "increments miss count" do
      assert CacheMetrics.record_miss(:plan_cache) == :ok
      assert CacheMetrics.record_miss(:plan_cache) == :ok

      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      assert metrics.hits == 0
      assert metrics.misses == 2
    end
  end

  describe "record_eviction/2" do
    test "increments eviction count" do
      assert CacheMetrics.record_eviction(:plan_cache) == :ok

      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      assert metrics.evictions == 1
    end
  end

  describe "record_size/2" do
    test "updates cache size" do
      assert CacheMetrics.record_size(:plan_cache, 100) == :ok
      assert CacheMetrics.record_size(:plan_cache, 150) == :ok

      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      assert metrics.size == 150
    end

    test "rejects negative sizes" do
      # The function guard prevents negative sizes from being processed
      # Calling with negative size should effectively be a no-op
      assert CacheMetrics.record_size(:plan_cache, -1) == :ok

      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      # Size should not have changed (negative values ignored)
      assert metrics.size == 0
    end
  end

  # ===========================================================================
  # Metrics Calculation Tests
  # ===========================================================================

  describe "hit rate calculation" do
    test "calculates hit rate correctly" do
      CacheMetrics.record_hit(:plan_cache)
      CacheMetrics.record_hit(:plan_cache)
      CacheMetrics.record_hit(:plan_cache)
      CacheMetrics.record_miss(:plan_cache)

      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      assert metrics.hit_rate == 0.75
    end

    test "handles empty cache" do
      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      assert metrics.hit_rate == 0.0
    end

    test "handles all hits" do
      CacheMetrics.record_hit(:plan_cache)
      CacheMetrics.record_hit(:plan_cache)

      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      assert metrics.hit_rate == 1.0
    end

    test "handles all misses" do
      CacheMetrics.record_miss(:plan_cache)
      CacheMetrics.record_miss(:plan_cache)

      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      assert metrics.hit_rate == 0.0
    end
  end

  describe "memory estimation" do
    test "estimates memory based on size" do
      CacheMetrics.record_size(:plan_cache, 10_000)

      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      # 10,000 entries * 100 bytes / 1MB
      expected_mb = 10_000 * 100 / 1_048_576
      assert_in_delta metrics.memory_mb, expected_mb, 0.01
    end
  end

  # ===========================================================================
  # All Metrics Tests
  # ===========================================================================

  describe "all_metrics/0" do
    test "returns metrics for all cache types" do
      CacheMetrics.record_hit(:plan_cache)
      CacheMetrics.record_miss(:stats_cache)
      CacheMetrics.record_eviction(:cardinality_cache)

      all = CacheMetrics.all_metrics()

      assert Map.has_key?(all, :plan_cache)
      assert Map.has_key?(all, :stats_cache)
      assert Map.has_key?(all, :cardinality_cache)

      assert all.plan_cache.hits == 1
      assert all.stats_cache.misses == 1
      assert all.cardinality_cache.evictions == 1
    end
  end

  # ===========================================================================
  # Reset Tests
  # ===========================================================================

  describe "reset_metrics/0" do
    test "resets all metrics to zero" do
      CacheMetrics.record_hit(:plan_cache)
      CacheMetrics.record_miss(:stats_cache)
      CacheMetrics.record_eviction(:cardinality_cache)

      assert CacheMetrics.reset_metrics() == :ok

      all = CacheMetrics.all_metrics()

      Enum.each(all, fn {_cache_type, metrics} ->
        assert metrics.hits == 0
        assert metrics.misses == 0
        assert metrics.evictions == 0
      end)
    end
  end

  describe "reset_metrics/1" do
    test "resets specific cache type" do
      CacheMetrics.record_hit(:plan_cache)
      CacheMetrics.record_hit(:stats_cache)

      assert CacheMetrics.reset_metrics(:plan_cache) == :ok

      {:ok, plan_metrics} = CacheMetrics.metrics(:plan_cache)
      {:ok, stats_metrics} = CacheMetrics.metrics(:stats_cache)

      assert plan_metrics.hits == 0
      assert stats_metrics.hits == 1
    end

    test "returns error for unknown cache type" do
      assert CacheMetrics.metrics(:unknown_cache) == {:error, :unknown_cache}
    end
  end

  # ===========================================================================
  # Snapshot Tests
  # ===========================================================================

  describe "snapshot/0" do
    test "returns current snapshot" do
      CacheMetrics.record_hit(:plan_cache)
      CacheMetrics.record_miss(:plan_cache)
      CacheMetrics.record_size(:plan_cache, 100)

      snapshot = CacheMetrics.snapshot()

      assert is_map(snapshot)
      assert Map.has_key?(snapshot, :plan_cache)

      plan_metrics = snapshot.plan_cache
      assert plan_metrics.hits == 1
      assert plan_metrics.misses == 1
      assert plan_metrics.size == 100
      assert is_float(plan_metrics.hit_rate)
    end
  end

  # ===========================================================================
  # Formatting Tests
  # ===========================================================================

  describe "format_metrics/1" do
    test "formats single cache metrics" do
      metrics = %{
        hits: 100,
        misses: 25,
        evictions: 5,
        size: 500,
        max_size: 1000,
        hit_rate: 0.8,
        memory_mb: 0.05
      }

      formatted = CacheMetrics.format_metrics(metrics)

      assert String.contains?(formatted, "Hits: 100")
      assert String.contains?(formatted, "Misses: 25")
      assert String.contains?(formatted, "Hit Rate: 80.0%")
    end

    test "formats all cache metrics" do
      all_metrics = %{
        plan_cache: %{
          hits: 10,
          misses: 2,
          evictions: 0,
          size: 5,
          max_size: 100,
          hit_rate: 0.8333,
          memory_mb: 0.0
        },
        stats_cache: %{
          hits: 50,
          misses: 10,
          evictions: 1,
          size: 20,
          max_size: nil,
          hit_rate: 0.8333,
          memory_mb: 0.0
        }
      }

      formatted = CacheMetrics.format_metrics(all_metrics)

      assert String.contains?(formatted, "plan_cache:")
      assert String.contains?(formatted, "stats_cache:")
      assert String.contains?(formatted, "Hits: 10")
      assert String.contains?(formatted, "Hits: 50")
    end
  end

  # ===========================================================================
  # Error Handling Tests
  # ===========================================================================

  describe "error handling" do
    test "metrics/1 returns error for unknown cache" do
      assert CacheMetrics.metrics(:not_a_cache) == {:error, :unknown_cache}
    end

    test "handles concurrent updates" do
      # Spawn multiple tasks updating the same cache
      tasks =
        Enum.map(1..100, fn _ ->
          Task.async(fn ->
            CacheMetrics.record_hit(:plan_cache)
            CacheMetrics.record_miss(:plan_cache)
          end)
        end)

      Task.await_many(tasks, 5000)

      {:ok, metrics} = CacheMetrics.metrics(:plan_cache)

      # Should have 100 hits and 100 misses
      assert metrics.hits == 100
      assert metrics.misses == 100
    end
  end
end
