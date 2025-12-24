defmodule TripleStore.SPARQL.PlanCacheTest do
  use ExUnit.Case, async: false

  alias TripleStore.SPARQL.PlanCache

  # Use unique names to avoid conflicts between tests
  defp unique_name do
    :"PlanCache_#{:erlang.unique_integer([:positive])}"
  end

  setup do
    name = unique_name()
    {:ok, pid} = PlanCache.start_link(name: name, max_size: 10)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    %{name: name, pid: pid}
  end

  # ===========================================================================
  # Basic Operations Tests
  # ===========================================================================

  describe "start_link/1" do
    test "starts with default options" do
      name = unique_name()
      {:ok, pid} = PlanCache.start_link(name: name)
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end

    test "starts with custom max_size" do
      name = unique_name()
      {:ok, pid} = PlanCache.start_link(name: name, max_size: 100)
      assert Process.alive?(pid)
      GenServer.stop(pid)
    end
  end

  describe "get_or_compute/3" do
    test "computes and caches on miss", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      compute_called = :counters.new(1, [:atomics])

      result = PlanCache.get_or_compute(query, fn ->
        :counters.add(compute_called, 1, 1)
        {:ok, :test_plan}
      end, name: name)

      assert result == {:ok, :test_plan}
      assert :counters.get(compute_called, 1) == 1
    end

    test "returns cached value on hit", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      compute_called = :counters.new(1, [:atomics])

      # First call - computes
      PlanCache.get_or_compute(query, fn ->
        :counters.add(compute_called, 1, 1)
        {:ok, :test_plan}
      end, name: name)

      # Second call - should use cache
      result = PlanCache.get_or_compute(query, fn ->
        :counters.add(compute_called, 1, 1)
        {:ok, :different_plan}
      end, name: name)

      assert result == {:ok, :test_plan}
      assert :counters.get(compute_called, 1) == 1
    end

    test "different queries get different cache entries", %{name: name} do
      query1 = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      query2 = {:triple, {:variable, "x"}, 2, {:variable, "y"}}

      PlanCache.get_or_compute(query1, fn -> :plan1 end, name: name)
      PlanCache.get_or_compute(query2, fn -> :plan2 end, name: name)

      assert PlanCache.get(query1, name: name) == {:ok, :plan1}
      assert PlanCache.get(query2, name: name) == {:ok, :plan2}
    end
  end

  describe "get/2" do
    test "returns :miss for uncached query", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      assert PlanCache.get(query, name: name) == :miss
    end

    test "returns {:ok, plan} for cached query", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      PlanCache.put(query, :cached_plan, name: name)

      # Give async put time to complete
      Process.sleep(10)

      assert PlanCache.get(query, name: name) == {:ok, :cached_plan}
    end
  end

  describe "put/3" do
    test "stores plan in cache", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      PlanCache.put(query, :my_plan, name: name)

      Process.sleep(10)

      assert PlanCache.get(query, name: name) == {:ok, :my_plan}
    end

    test "overwrites existing entry", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}

      PlanCache.put(query, :plan_v1, name: name)
      Process.sleep(10)

      PlanCache.put(query, :plan_v2, name: name)
      Process.sleep(10)

      assert PlanCache.get(query, name: name) == {:ok, :plan_v2}
    end
  end

  # ===========================================================================
  # Cache Key Tests
  # ===========================================================================

  describe "compute_key/1" do
    test "same query produces same key" do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}

      key1 = PlanCache.compute_key(query)
      key2 = PlanCache.compute_key(query)

      assert key1 == key2
    end

    test "different queries produce different keys" do
      query1 = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      query2 = {:triple, {:variable, "x"}, 2, {:variable, "y"}}

      key1 = PlanCache.compute_key(query1)
      key2 = PlanCache.compute_key(query2)

      refute key1 == key2
    end

    test "queries with different variable names but same structure share keys" do
      query1 = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      query2 = {:triple, {:variable, "a"}, 1, {:variable, "b"}}

      key1 = PlanCache.compute_key(query1)
      key2 = PlanCache.compute_key(query2)

      assert key1 == key2
    end

    test "variable order within pattern matters" do
      # Same variable in different positions
      query1 = {:triple, {:variable, "x"}, 1, {:variable, "x"}}
      query2 = {:triple, {:variable, "x"}, 1, {:variable, "y"}}

      key1 = PlanCache.compute_key(query1)
      key2 = PlanCache.compute_key(query2)

      refute key1 == key2
    end

    test "normalizes across multiple patterns" do
      patterns1 = [
        {:triple, {:variable, "x"}, 1, {:variable, "y"}},
        {:triple, {:variable, "y"}, 2, {:variable, "z"}}
      ]

      patterns2 = [
        {:triple, {:variable, "a"}, 1, {:variable, "b"}},
        {:triple, {:variable, "b"}, 2, {:variable, "c"}}
      ]

      key1 = PlanCache.compute_key(patterns1)
      key2 = PlanCache.compute_key(patterns2)

      assert key1 == key2
    end

    test "different join orders produce different keys" do
      patterns1 = [
        {:triple, {:variable, "x"}, 1, {:variable, "y"}},
        {:triple, {:variable, "y"}, 2, {:variable, "z"}}
      ]

      patterns2 = [
        {:triple, {:variable, "y"}, 2, {:variable, "z"}},
        {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      ]

      key1 = PlanCache.compute_key(patterns1)
      key2 = PlanCache.compute_key(patterns2)

      # Different order = different structure = different key
      refute key1 == key2
    end
  end

  # ===========================================================================
  # Invalidation Tests
  # ===========================================================================

  describe "invalidate/1" do
    test "clears entire cache", %{name: name} do
      query1 = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      query2 = {:triple, {:variable, "a"}, 2, {:variable, "b"}}

      PlanCache.put(query1, :plan1, name: name)
      PlanCache.put(query2, :plan2, name: name)
      Process.sleep(10)

      assert PlanCache.size(name: name) == 2

      PlanCache.invalidate(name: name)

      assert PlanCache.size(name: name) == 0
      assert PlanCache.get(query1, name: name) == :miss
      assert PlanCache.get(query2, name: name) == :miss
    end
  end

  describe "invalidate/2" do
    test "clears specific entry", %{name: name} do
      query1 = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      query2 = {:triple, {:variable, "a"}, 2, {:variable, "b"}}

      PlanCache.put(query1, :plan1, name: name)
      PlanCache.put(query2, :plan2, name: name)
      Process.sleep(10)

      PlanCache.invalidate(query1, name: name)

      assert PlanCache.get(query1, name: name) == :miss
      assert PlanCache.get(query2, name: name) == {:ok, :plan2}
    end

    test "no error for non-existent key", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      assert :ok = PlanCache.invalidate(query, name: name)
    end
  end

  # ===========================================================================
  # LRU Eviction Tests
  # ===========================================================================

  describe "LRU eviction" do
    test "evicts oldest entry when max size exceeded" do
      name = unique_name()
      {:ok, pid} = PlanCache.start_link(name: name, max_size: 3)

      try do
        # Add 3 entries
        for i <- 1..3 do
          query = {:pattern, i}
          PlanCache.put(query, {:plan, i}, name: name)
          Process.sleep(5)  # Ensure distinct timestamps
        end

        Process.sleep(10)
        assert PlanCache.size(name: name) == 3

        # Add 4th entry - should evict oldest
        PlanCache.put({:pattern, 4}, {:plan, 4}, name: name)
        Process.sleep(10)

        assert PlanCache.size(name: name) == 3

        # First entry should be evicted
        assert PlanCache.get({:pattern, 1}, name: name) == :miss
        # Others should still be there
        assert PlanCache.get({:pattern, 2}, name: name) == {:ok, {:plan, 2}}
        assert PlanCache.get({:pattern, 3}, name: name) == {:ok, {:plan, 3}}
        assert PlanCache.get({:pattern, 4}, name: name) == {:ok, {:plan, 4}}
      after
        GenServer.stop(pid)
      end
    end

    test "access updates LRU order" do
      name = unique_name()
      {:ok, pid} = PlanCache.start_link(name: name, max_size: 3)

      try do
        # Add 3 entries
        for i <- 1..3 do
          PlanCache.put({:pattern, i}, {:plan, i}, name: name)
          Process.sleep(5)
        end

        Process.sleep(10)

        # Access entry 1 to make it recently used
        PlanCache.get({:pattern, 1}, name: name)
        Process.sleep(5)

        # Add 4th entry - should evict entry 2 (oldest non-accessed)
        PlanCache.put({:pattern, 4}, {:plan, 4}, name: name)
        Process.sleep(10)

        # Entry 1 should still be there (was accessed)
        assert PlanCache.get({:pattern, 1}, name: name) == {:ok, {:plan, 1}}
        # Entry 2 should be evicted (oldest)
        assert PlanCache.get({:pattern, 2}, name: name) == :miss
      after
        GenServer.stop(pid)
      end
    end

    test "stats track evictions" do
      name = unique_name()
      {:ok, pid} = PlanCache.start_link(name: name, max_size: 2)

      try do
        for i <- 1..5 do
          PlanCache.put({:pattern, i}, {:plan, i}, name: name)
          Process.sleep(5)
        end

        Process.sleep(10)

        stats = PlanCache.stats(name: name)
        assert stats.evictions >= 3  # At least 3 evictions (5 inserts, max 2)
      after
        GenServer.stop(pid)
      end
    end
  end

  # ===========================================================================
  # Statistics Tests
  # ===========================================================================

  describe "stats/1" do
    test "tracks hits and misses", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}

      # Miss
      PlanCache.get(query, name: name)

      # Put and hit
      PlanCache.put(query, :plan, name: name)
      Process.sleep(10)
      PlanCache.get(query, name: name)
      PlanCache.get(query, name: name)

      stats = PlanCache.stats(name: name)

      assert stats.misses >= 1
      assert stats.hits >= 2
    end

    test "calculates hit rate", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}

      # 1 miss
      PlanCache.get(query, name: name)

      # Put then 3 hits
      PlanCache.put(query, :plan, name: name)
      Process.sleep(10)
      PlanCache.get(query, name: name)
      PlanCache.get(query, name: name)
      PlanCache.get(query, name: name)

      stats = PlanCache.stats(name: name)

      # 3 hits, 1 miss = 75% hit rate
      assert stats.hit_rate == 0.75
    end

    test "reports current size", %{name: name} do
      for i <- 1..5 do
        PlanCache.put({:pattern, i}, {:plan, i}, name: name)
      end

      Process.sleep(10)

      stats = PlanCache.stats(name: name)
      assert stats.size == 5
    end

    test "hit rate is 0.0 with no accesses", %{name: name} do
      stats = PlanCache.stats(name: name)
      assert stats.hit_rate == 0.0
    end
  end

  describe "size/1" do
    test "returns current cache size", %{name: name} do
      assert PlanCache.size(name: name) == 0

      PlanCache.put({:pattern, 1}, :plan1, name: name)
      PlanCache.put({:pattern, 2}, :plan2, name: name)
      Process.sleep(10)

      assert PlanCache.size(name: name) == 2
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles complex nested structures", %{name: name} do
      query = {
        :bgp,
        [
          {:triple, {:variable, "x"}, {:uri, "http://example.org/knows"}, {:variable, "y"}},
          {:triple, {:variable, "y"}, {:uri, "http://example.org/name"}, {:literal, "Alice"}}
        ]
      }

      PlanCache.put(query, :complex_plan, name: name)
      Process.sleep(10)

      assert PlanCache.get(query, name: name) == {:ok, :complex_plan}
    end

    test "handles map values in query", %{name: name} do
      query = %{
        type: :select,
        patterns: [{:triple, {:variable, "x"}, 1, {:variable, "y"}}],
        modifiers: %{limit: 10, offset: 0}
      }

      PlanCache.put(query, :map_plan, name: name)
      Process.sleep(10)

      assert PlanCache.get(query, name: name) == {:ok, :map_plan}
    end

    test "handles empty pattern list", %{name: name} do
      query = []
      PlanCache.put(query, :empty_plan, name: name)
      Process.sleep(10)

      assert PlanCache.get(query, name: name) == {:ok, :empty_plan}
    end

    test "handles atom variable names", %{name: name} do
      query1 = {:triple, {:variable, :x}, 1, {:variable, :y}}
      query2 = {:triple, {:variable, "x"}, 1, {:variable, "y"}}

      # Atom and string variable names should produce same key
      key1 = PlanCache.compute_key(query1)
      key2 = PlanCache.compute_key(query2)

      assert key1 == key2
    end

    test "concurrent access is safe", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}

      # Spawn multiple processes accessing cache concurrently
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            if rem(i, 2) == 0 do
              PlanCache.put({:pattern, i}, {:plan, i}, name: name)
            else
              PlanCache.get({:pattern, i}, name: name)
            end
          end)
        end

      # All tasks should complete without error
      results = Task.await_many(tasks)
      assert length(results) == 100
    end
  end

  # ===========================================================================
  # Integration-like Tests
  # ===========================================================================

  describe "realistic usage patterns" do
    test "repeated query execution benefits from cache", %{name: name} do
      query = [
        {:triple, {:variable, "person"}, 1, {:variable, "name"}},
        {:triple, {:variable, "person"}, 2, {:variable, "age"}}
      ]

      compute_count = :counters.new(1, [:atomics])

      expensive_compute = fn ->
        :counters.add(compute_count, 1, 1)
        # Simulate expensive optimization
        Process.sleep(10)
        {:ok, :optimized_plan}
      end

      # First execution - computes
      result1 = PlanCache.get_or_compute(query, expensive_compute, name: name)

      # Subsequent executions - use cache
      for _ <- 1..10 do
        result = PlanCache.get_or_compute(query, expensive_compute, name: name)
        assert result == result1
      end

      # Should only have computed once
      assert :counters.get(compute_count, 1) == 1
    end

    test "similar queries with different variable names share cache", %{name: name} do
      compute_count = :counters.new(1, [:atomics])

      compute = fn ->
        :counters.add(compute_count, 1, 1)
        :shared_plan
      end

      # Query with one set of variable names
      query1 = {:triple, {:variable, "x"}, 1, {:variable, "y"}}
      PlanCache.get_or_compute(query1, compute, name: name)

      # Same structure, different variable names - should hit cache
      query2 = {:triple, {:variable, "subject"}, 1, {:variable, "object"}}
      result = PlanCache.get_or_compute(query2, compute, name: name)

      assert result == :shared_plan
      assert :counters.get(compute_count, 1) == 1
    end

    test "invalidation after data change", %{name: name} do
      query = {:triple, {:variable, "x"}, 1, {:variable, "y"}}

      # Cache a plan
      PlanCache.get_or_compute(query, fn -> :old_plan end, name: name)
      assert PlanCache.get(query, name: name) == {:ok, :old_plan}

      # Simulate data change - invalidate cache
      PlanCache.invalidate(name: name)

      # Next access should recompute
      result = PlanCache.get_or_compute(query, fn -> :new_plan end, name: name)
      assert result == :new_plan
    end
  end
end
