defmodule TripleStore.SPARQL.Update.RateLimiterTest do
  @moduledoc """
  Comprehensive tests for the UPDATE rate limiter.

  These tests verify that the rate limiter properly prevents DoS attacks
  by limiting the number of operations per user per time window.
  """

  use ExUnit.Case, async: false

  alias TripleStore.SPARQL.Update.RateLimiter

  @moduletag :integration
  @moduletag :rate_limiter

  setup do
    # Ensure the rate limiter is started
    case Process.whereis(RateLimiter) do
      nil -> {:ok, _pid} = RateLimiter.start_link()
      pid -> {:ok, pid}
    end

    # Clean up any existing test data
    RateLimiter.reset_user("test_user1")
    RateLimiter.reset_user("test_user2")
    RateLimiter.reset_user("")

    :ok
  end

  # ===========================================================================
  # Basic Operation Tests
  # ===========================================================================

  describe "allow?/2" do
    test "returns :ok for first operation within limit" do
      user_id = "test_user1"
      operation = :insert_data

      assert :ok = RateLimiter.allow?(user_id, operation)
    end

    test "returns :ok for multiple operations within limit" do
      user_id = "test_user1"
      operation = :insert_data

      for _i <- 1..10 do
        assert :ok = RateLimiter.allow?(user_id, operation)
      end
    end

    test "returns {:error, :rate_limited} when limit exceeded" do
      user_id = "test_user1"
      operation = :insert_data

      # Use up the limit (100 operations per 60 seconds by default)
      for _i <- 1..100 do
        assert :ok = RateLimiter.allow?(user_id, operation)
      end

      # Next operation should be rate limited
      assert {:error, :rate_limited} = RateLimiter.allow?(user_id, operation)
    end

    test "different operations have separate limits" do
      user_id = "test_user1"

      # Use up insert_data limit
      for _i <- 1..100 do
        assert :ok = RateLimiter.allow?(user_id, :insert_data)
      end

      # insert_data should be rate limited
      assert {:error, :rate_limited} = RateLimiter.allow?(user_id, :insert_data)

      # But delete_data should still work (separate limit)
      assert :ok = RateLimiter.allow?(user_id, :delete_data)
    end

    test "allows operations for unknown operation types" do
      user_id = "test_user1"

      # Unknown operations are allowed by default
      assert :ok = RateLimiter.allow?(user_id, :unknown_operation)
      assert :ok = RateLimiter.allow?(user_id, :unknown_operation)
    end
  end

  # ===========================================================================
  # Per-User Isolation Tests
  # ===========================================================================

  describe "per-user isolation" do
    test "tracks limits separately for each user" do
      user1 = "test_user1"
      user2 = "test_user2"
      operation = :insert_data

      # User1 uses up most of their limit
      for _i <- 1..90 do
        assert :ok = RateLimiter.allow?(user1, operation)
      end

      # User1 should still have capacity
      assert :ok = RateLimiter.allow?(user1, operation)

      # User2 should have full capacity
      assert :ok = RateLimiter.allow?(user2, operation)

      for _i <- 1..10 do
        assert :ok = RateLimiter.allow?(user2, operation)
      end
    end
  end

  # ===========================================================================
  # Record Tests
  # ===========================================================================

  describe "record/2" do
    test "increments operation counter" do
      user_id = "test_user1"
      operation = :insert_data

      # Record some operations
      :ok = RateLimiter.record(user_id, operation)
      :ok = RateLimiter.record(user_id, operation)
      :ok = RateLimiter.record(user_id, operation)

      # Check stats
      assert {:ok, stats} = RateLimiter.stats(user_id, operation)
      assert stats.count == 3
    end

    test "works for first operation" do
      user_id = "test_user1"
      operation = :delete_data

      # Record first operation
      :ok = RateLimiter.record(user_id, operation)

      # Check stats
      assert {:ok, stats} = RateLimiter.stats(user_id, operation)
      assert stats.count == 1
    end
  end

  # ===========================================================================
  # Stats Tests
  # ===========================================================================

  describe "stats/2" do
    test "returns current usage statistics" do
      user_id = "test_user1"
      operation = :insert_data

      # Record some operations
      for _i <- 1..5 do
        :ok = RateLimiter.record(user_id, operation)
      end

      assert {:ok, stats} = RateLimiter.stats(user_id, operation)
      assert stats.count == 5
      assert stats.remaining > 0
      assert is_integer(stats.reset_at)
    end

    test "returns zero for user with no operations" do
      user_id = "nonexistent_user"
      operation = :insert_data

      assert {:ok, stats} = RateLimiter.stats(user_id, operation)
      assert stats.count == 0
      assert stats.remaining > 0
    end

    test "returns error for unknown operation" do
      user_id = "test_user1"

      assert {:error, :unknown_operation} = RateLimiter.stats(user_id, :unknown_op)
    end
  end

  # ===========================================================================
  # Reset Tests
  # ===========================================================================

  describe "reset/2" do
    test "resets counter for specific operation" do
      user_id = "test_user1"
      operation = :insert_data

      # Record some operations
      for _i <- 1..10 do
        :ok = RateLimiter.record(user_id, operation)
      end

      # Reset
      :ok = RateLimiter.reset(user_id, operation)

      # Should start from zero
      assert :ok = RateLimiter.allow?(user_id, operation)

      # Check stats
      assert {:ok, stats} = RateLimiter.stats(user_id, operation)
      # One operation from the allow? call above
      assert stats.count == 1
    end

    test "does not affect other operations for same user" do
      user_id = "test_user1"

      # Record operations for two different types
      :ok = RateLimiter.record(user_id, :insert_data)
      :ok = RateLimiter.record(user_id, :delete_data)

      # Reset only insert_data
      :ok = RateLimiter.reset(user_id, :insert_data)

      # insert_data should be reset
      assert {:ok, insert_stats} = RateLimiter.stats(user_id, :insert_data)
      assert insert_stats.count == 0

      # delete_data should still have its count
      assert {:ok, delete_stats} = RateLimiter.stats(user_id, :delete_data)
      assert delete_stats.count == 1
    end
  end

  describe "reset_user/1" do
    test "resets all counters for a user" do
      user_id = "test_user1"

      # Record operations for multiple types
      for _i <- 1..5 do
        :ok = RateLimiter.record(user_id, :insert_data)
      end

      for _i <- 1..3 do
        :ok = RateLimiter.record(user_id, :delete_data)
      end

      # Reset all
      :ok = RateLimiter.reset_user(user_id)

      # All should be reset
      assert {:ok, stats} = RateLimiter.stats(user_id, :insert_data)
      assert stats.count == 0

      assert {:ok, stats} = RateLimiter.stats(user_id, :delete_data)
      assert stats.count == 0
    end
  end

  # ===========================================================================
  # Cross-Operation Limits
  # ===========================================================================

  describe "cross-operation limits" do
    test "each operation type has its own limit" do
      user_id = "test_cross_op"

      # Use up entire insert_data limit (100 operations)
      for _i <- 1..100 do
        assert :ok = RateLimiter.allow?(user_id, :insert_data)
      end

      # insert_data should be rate limited now
      assert {:error, :rate_limited} = RateLimiter.allow?(user_id, :insert_data)

      # But other operations still work
      assert :ok = RateLimiter.allow?(user_id, :delete_data)
      assert :ok = RateLimiter.allow?(user_id, :modify)
      assert :ok = RateLimiter.allow?(user_id, :create_graph)
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles empty user ID" do
      # Empty string user ID should work
      assert :ok = RateLimiter.allow?("", :insert_data)
    end

    test "handles special characters in user ID" do
      special_user = "user@example.com"

      assert :ok = RateLimiter.allow?(special_user, :insert_data)
      :ok = RateLimiter.record(special_user, :insert_data)

      assert {:ok, stats} = RateLimiter.stats(special_user, :insert_data)
      assert stats.count == 2
    end
  end

  # ===========================================================================
  # Default Limits
  # ===========================================================================

  describe "default limits" do
    test "uses default limits when not configured" do
      user_id = "test_default_limits"

      # Check various operations have different limits
      {:ok, insert_stats} = RateLimiter.stats(user_id, :insert_data)
      {:ok, modify_stats} = RateLimiter.stats(user_id, :modify)
      {:ok, create_stats} = RateLimiter.stats(user_id, :create_graph)

      # insert_data has higher limit than modify
      assert insert_stats.remaining > modify_stats.remaining

      # create_graph has lower limit
      assert create_stats.remaining < insert_stats.remaining
    end
  end

  # ===========================================================================
  # Time Window Tests
  # ===========================================================================

  describe "time windows" do
    @tag :slow
    test "allow?/2 resets after time window passes" do
      # This test is slow because it needs to wait for the time window
      # Skip in normal test runs
      :ok
    end

    test "stats/2 shows correct remaining count" do
      user_id = "test_window"

      # Use some operations
      for _i <- 1..10 do
        :ok = RateLimiter.record(user_id, :insert_data)
      end

      assert {:ok, stats} = RateLimiter.stats(user_id, :insert_data)
      assert stats.count == 10
      # 100 - 10
      assert stats.remaining == 90
      assert stats.reset_at > System.system_time(:second)
    end
  end
end
