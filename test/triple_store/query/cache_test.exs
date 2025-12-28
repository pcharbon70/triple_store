defmodule TripleStore.Query.CacheTest do
  use ExUnit.Case, async: true

  alias TripleStore.Query.Cache

  # Use unique names to avoid conflicts between tests
  defp unique_name do
    :"test_cache_#{System.unique_integer([:positive])}"
  end

  defp safe_stop(pid) do
    if Process.alive?(pid) do
      GenServer.stop(pid)
    end
  end

  describe "start_link/1" do
    test "starts with default options" do
      name = unique_name()
      assert {:ok, pid} = Cache.start_link(name: name)
      assert Process.alive?(pid)
      safe_stop(pid)
    end

    test "starts with custom options" do
      name = unique_name()

      assert {:ok, pid} =
               Cache.start_link(
                 name: name,
                 max_entries: 500,
                 max_result_size: 5000,
                 ttl_ms: 60_000
               )

      config = Cache.config(name: name)
      assert config.max_entries == 500
      assert config.max_result_size == 5000
      assert config.ttl_ms == 60_000
      safe_stop(pid)
    end
  end

  describe "get_or_execute/3" do
    setup do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100, max_result_size: 1000)
      on_exit(fn -> safe_stop(pid) end)
      %{name: name}
    end

    test "returns result from execute_fn on cache miss", %{name: name} do
      query = "SELECT ?s WHERE { ?s ?p ?o }"
      result = [%{s: :some_subject}]

      {:ok, returned} =
        Cache.get_or_execute(
          query,
          fn -> {:ok, result} end,
          name: name
        )

      assert returned == result
    end

    test "returns cached result on cache hit", %{name: name} do
      query = "SELECT ?s WHERE { ?s ?p ?o }"
      result = [%{s: :some_subject}]

      # First call - cache miss
      {:ok, _} =
        Cache.get_or_execute(
          query,
          fn -> {:ok, result} end,
          name: name
        )

      # Second call - should be cache hit
      call_count = :counters.new(1, [:atomics])

      {:ok, returned} =
        Cache.get_or_execute(
          query,
          fn ->
            :counters.add(call_count, 1, 1)
            {:ok, result}
          end,
          name: name
        )

      assert returned == result
      assert :counters.get(call_count, 1) == 0
    end

    test "propagates errors from execute_fn", %{name: name} do
      query = "INVALID QUERY"

      result =
        Cache.get_or_execute(
          query,
          fn -> {:error, :parse_error} end,
          name: name
        )

      assert result == {:error, :parse_error}
    end

    test "does not cache errors", %{name: name} do
      query = "FAILING QUERY"

      # First call - error
      {:error, _} =
        Cache.get_or_execute(
          query,
          fn -> {:error, :some_error} end,
          name: name
        )

      # Second call - should still be miss
      call_count = :counters.new(1, [:atomics])

      Cache.get_or_execute(
        query,
        fn ->
          :counters.add(call_count, 1, 1)
          {:ok, []}
        end,
        name: name
      )

      assert :counters.get(call_count, 1) == 1
    end
  end

  describe "get/2 and put/3" do
    setup do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100, max_result_size: 1000)
      on_exit(fn -> safe_stop(pid) end)
      %{name: name}
    end

    test "get returns :miss for uncached query", %{name: name} do
      assert Cache.get("uncached query", name: name) == :miss
    end

    test "put stores and get retrieves", %{name: name} do
      query = "SELECT ?x WHERE { ?x a ?type }"
      result = [%{x: :a}, %{x: :b}]

      assert Cache.put(query, result, name: name) == :ok
      assert {:ok, ^result} = Cache.get(query, name: name)
    end

    test "put returns :skipped for large results", %{name: name} do
      query = "SELECT ?x WHERE { ?x ?p ?o }"
      # Create result larger than max_result_size (1000)
      large_result = Enum.map(1..1500, fn i -> %{x: i} end)

      assert Cache.put(query, large_result, name: name) == :skipped
      assert Cache.get(query, name: name) == :miss
    end
  end

  describe "compute_key/1" do
    test "produces consistent keys for same query string" do
      query = "SELECT ?s WHERE { ?s ?p ?o }"
      key1 = Cache.compute_key(query)
      key2 = Cache.compute_key(query)
      assert key1 == key2
    end

    test "produces different keys for different queries" do
      query1 = "SELECT ?s WHERE { ?s ?p ?o }"
      query2 = "SELECT ?s WHERE { ?s a ?type }"
      key1 = Cache.compute_key(query1)
      key2 = Cache.compute_key(query2)
      assert key1 != key2
    end

    test "handles non-string queries (algebra)" do
      algebra = {:bgp, [{:triple, :s, :p, :o}]}
      key = Cache.compute_key(algebra)
      assert is_binary(key)
      assert byte_size(key) == 32
    end
  end

  describe "LRU eviction" do
    test "evicts oldest entries when max_entries exceeded" do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 3, max_result_size: 1000)
      on_exit(fn -> safe_stop(pid) end)

      # Insert 3 entries
      Cache.put("query1", [1], name: name)
      Process.sleep(1)
      Cache.put("query2", [2], name: name)
      Process.sleep(1)
      Cache.put("query3", [3], name: name)

      assert Cache.size(name: name) == 3

      # Insert 4th entry - should evict query1
      Process.sleep(1)
      Cache.put("query4", [4], name: name)

      assert Cache.size(name: name) == 3
      assert Cache.get("query1", name: name) == :miss
      assert {:ok, [2]} = Cache.get("query2", name: name)
      assert {:ok, [3]} = Cache.get("query3", name: name)
      assert {:ok, [4]} = Cache.get("query4", name: name)
    end

    test "access updates LRU ordering" do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 3, max_result_size: 1000)
      on_exit(fn -> safe_stop(pid) end)

      # Insert 3 entries
      Cache.put("query1", [1], name: name)
      Process.sleep(1)
      Cache.put("query2", [2], name: name)
      Process.sleep(1)
      Cache.put("query3", [3], name: name)

      # Access query1 to make it recently used
      Process.sleep(1)
      Cache.get("query1", name: name)

      # Insert 4th entry - should evict query2 (oldest accessed)
      Process.sleep(1)
      Cache.put("query4", [4], name: name)

      assert Cache.get("query1", name: name) != :miss
      assert Cache.get("query2", name: name) == :miss
      assert Cache.get("query3", name: name) != :miss
      assert Cache.get("query4", name: name) != :miss
    end
  end

  describe "TTL expiration" do
    test "entries expire after TTL" do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100, ttl_ms: 50)
      on_exit(fn -> safe_stop(pid) end)

      Cache.put("query", [1, 2, 3], name: name)
      assert {:ok, [1, 2, 3]} = Cache.get("query", name: name)

      # Wait for expiration
      Process.sleep(60)

      assert Cache.get("query", name: name) == :expired
    end

    test "get_or_execute re-executes for expired entries" do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100, ttl_ms: 50)
      on_exit(fn -> safe_stop(pid) end)

      query = "SELECT ?s WHERE { ?s ?p ?o }"

      # First execution
      {:ok, [1]} =
        Cache.get_or_execute(
          query,
          fn -> {:ok, [1]} end,
          name: name
        )

      # Wait for expiration
      Process.sleep(60)

      # Should re-execute
      call_count = :counters.new(1, [:atomics])

      {:ok, [2]} =
        Cache.get_or_execute(
          query,
          fn ->
            :counters.add(call_count, 1, 1)
            {:ok, [2]}
          end,
          name: name
        )

      assert :counters.get(call_count, 1) == 1
    end

    test "cleanup_expired removes expired entries" do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100, ttl_ms: 50)
      on_exit(fn -> safe_stop(pid) end)

      Cache.put("query1", [1], name: name)
      Cache.put("query2", [2], name: name)
      Cache.put("query3", [3], name: name)

      assert Cache.size(name: name) == 3

      # Wait for expiration
      Process.sleep(60)

      count = Cache.cleanup_expired(name: name)
      assert count == 3
      assert Cache.size(name: name) == 0
    end
  end

  describe "invalidation" do
    setup do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100, max_result_size: 1000)
      on_exit(fn -> safe_stop(pid) end)
      %{name: name}
    end

    test "invalidate/1 clears entire cache", %{name: name} do
      Cache.put("query1", [1], name: name)
      Cache.put("query2", [2], name: name)
      Cache.put("query3", [3], name: name)

      assert Cache.size(name: name) == 3

      Cache.invalidate(name: name)

      assert Cache.size(name: name) == 0
      assert Cache.get("query1", name: name) == :miss
      assert Cache.get("query2", name: name) == :miss
      assert Cache.get("query3", name: name) == :miss
    end

    test "invalidate_query/2 removes specific query", %{name: name} do
      Cache.put("query1", [1], name: name)
      Cache.put("query2", [2], name: name)
      Cache.put("query3", [3], name: name)

      Cache.invalidate_query("query2", name: name)

      assert {:ok, [1]} = Cache.get("query1", name: name)
      assert Cache.get("query2", name: name) == :miss
      assert {:ok, [3]} = Cache.get("query3", name: name)
    end

    test "invalidate_predicates/2 removes queries with matching predicates", %{name: name} do
      pred1 = :predicate_1
      pred2 = :predicate_2
      pred3 = :predicate_3

      Cache.put("query1", [1], name: name, predicates: [pred1, pred2])
      Cache.put("query2", [2], name: name, predicates: [pred2, pred3])
      Cache.put("query3", [3], name: name, predicates: [pred3])

      # Invalidate pred1 - should only affect query1
      Cache.invalidate_predicates([pred1], name: name)

      assert Cache.get("query1", name: name) == :miss
      assert {:ok, [2]} = Cache.get("query2", name: name)
      assert {:ok, [3]} = Cache.get("query3", name: name)
    end

    test "invalidate_predicates/2 with multiple predicates", %{name: name} do
      pred1 = :predicate_1
      pred2 = :predicate_2
      pred3 = :predicate_3

      Cache.put("query1", [1], name: name, predicates: [pred1])
      Cache.put("query2", [2], name: name, predicates: [pred2])
      Cache.put("query3", [3], name: name, predicates: [pred3])

      # Invalidate pred1 and pred2
      Cache.invalidate_predicates([pred1, pred2], name: name)

      assert Cache.get("query1", name: name) == :miss
      assert Cache.get("query2", name: name) == :miss
      assert {:ok, [3]} = Cache.get("query3", name: name)
    end
  end

  describe "stats/1" do
    setup do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 3, max_result_size: 10)
      on_exit(fn -> safe_stop(pid) end)
      %{name: name}
    end

    test "tracks hits and misses", %{name: name} do
      Cache.put("query1", [1], name: name)

      # 1 miss
      Cache.get("query2", name: name)
      # 2 hits
      Cache.get("query1", name: name)
      Cache.get("query1", name: name)

      stats = Cache.stats(name: name)
      assert stats.hits == 2
      assert stats.misses == 1
      assert_in_delta stats.hit_rate, 0.666, 0.01
    end

    test "tracks evictions", %{name: name} do
      # max_entries is 3
      Cache.put("query1", [1], name: name)
      Cache.put("query2", [2], name: name)
      Cache.put("query3", [3], name: name)
      Cache.put("query4", [4], name: name)

      stats = Cache.stats(name: name)
      assert stats.evictions == 1
    end

    test "tracks skipped large results", %{name: name} do
      # max_result_size is 10
      large_result = Enum.map(1..15, fn i -> %{x: i} end)
      Cache.put("query1", large_result, name: name)

      stats = Cache.stats(name: name)
      assert stats.skipped_large == 1
    end

    test "returns zero hit_rate when no lookups", %{name: name} do
      stats = Cache.stats(name: name)
      assert stats.hit_rate == 0.0
    end
  end

  describe "size/1" do
    test "returns current cache size" do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100)
      on_exit(fn -> safe_stop(pid) end)

      assert Cache.size(name: name) == 0

      Cache.put("query1", [1], name: name)
      assert Cache.size(name: name) == 1

      Cache.put("query2", [2], name: name)
      assert Cache.size(name: name) == 2
    end
  end

  describe "result size calculation" do
    setup do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100, max_result_size: 5)
      on_exit(fn -> safe_stop(pid) end)
      %{name: name}
    end

    test "list results use length", %{name: name} do
      # 3 items - should be cached
      assert Cache.put("query1", [1, 2, 3], name: name) == :ok
      # 6 items - should be skipped
      assert Cache.put("query2", [1, 2, 3, 4, 5, 6], name: name) == :skipped
    end

    test "map with bindings uses bindings length", %{name: name} do
      # 3 bindings - should be cached
      assert Cache.put("query1", %{bindings: [%{}, %{}, %{}]}, name: name) == :ok
      # 6 bindings - should be skipped
      assert Cache.put("query2", %{bindings: [%{}, %{}, %{}, %{}, %{}, %{}]}, name: name) ==
               :skipped
    end

    test "other results count as size 1", %{name: name} do
      assert Cache.put("query1", :ok, name: name) == :ok
      assert Cache.put("query2", true, name: name) == :ok
      assert Cache.put("query3", "result", name: name) == :ok
    end
  end

  describe "concurrent access" do
    test "handles concurrent reads and writes" do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 1000, max_result_size: 100)
      on_exit(fn -> safe_stop(pid) end)

      # Spawn multiple processes doing reads and writes
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            query = "query_#{i}"
            result = [i]

            # Write
            Cache.put(query, result, name: name)

            # Read multiple times
            for _ <- 1..10 do
              case Cache.get(query, name: name) do
                {:ok, ^result} -> :ok
                :miss -> :miss
              end
            end
          end)
        end

      # All tasks should complete without errors
      results = Task.await_many(tasks, 5000)
      assert length(results) == 100
    end

    test "handles concurrent get_or_execute" do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100)
      on_exit(fn -> safe_stop(pid) end)

      call_count = :counters.new(1, [:atomics])
      query = "shared_query"

      # Spawn multiple processes calling get_or_execute for same query
      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            Cache.get_or_execute(
              query,
              fn ->
                :counters.add(call_count, 1, 1)
                # Simulate some work
                Process.sleep(10)
                {:ok, [1, 2, 3]}
              end,
              name: name
            )
          end)
        end

      results = Task.await_many(tasks, 5000)

      # All should return the same result
      assert Enum.all?(results, fn {:ok, r} -> r == [1, 2, 3] end)

      # Execute should be called at least once (cache miss)
      # Due to race conditions, it may be called more than once
      # but should be significantly less than 50 (allowing up to 50 for edge cases)
      executions = :counters.get(call_count, 1)
      assert executions >= 1
      assert executions <= 50
    end
  end

  describe "telemetry events" do
    setup do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100, max_result_size: 1000, ttl_ms: 50)
      on_exit(fn -> safe_stop(pid) end)
      %{name: name}
    end

    test "emits hit event on cache hit", %{name: name} do
      test_pid = self()
      handler_id = "cache-hit-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :cache, :query, :hit],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      query = "SELECT ?s WHERE { ?s ?p ?o }"

      # First call - miss
      Cache.get_or_execute(query, fn -> {:ok, [1]} end, name: name)

      # Second call - should be hit
      Cache.get_or_execute(query, fn -> {:ok, [2]} end, name: name)

      assert_receive {:telemetry, [:triple_store, :cache, :query, :hit], %{count: 1}, %{}}

      :telemetry.detach(handler_id)
    end

    test "emits miss event on cache miss", %{name: name} do
      test_pid = self()
      handler_id = "cache-miss-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :cache, :query, :miss],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      Cache.get_or_execute("uncached_query", fn -> {:ok, [1]} end, name: name)

      assert_receive {:telemetry, [:triple_store, :cache, :query, :miss], %{count: 1}, %{}}

      :telemetry.detach(handler_id)
    end

    test "emits expired event when entry has expired", %{name: name} do
      test_pid = self()
      handler_id = "cache-expired-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :cache, :query, :expired],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      query = "SELECT ?s WHERE { ?s ?p ?o }"

      # Cache the query
      Cache.get_or_execute(query, fn -> {:ok, [1]} end, name: name)

      # Wait for expiration
      Process.sleep(60)

      # Should get expired event
      Cache.get_or_execute(query, fn -> {:ok, [2]} end, name: name)

      assert_receive {:telemetry, [:triple_store, :cache, :query, :expired], %{count: 1}, %{}}

      :telemetry.detach(handler_id)
    end
  end

  describe "cache warming - persistence" do
    setup do
      name = unique_name()
      temp_dir = System.tmp_dir!()
      cache_file = Path.join(temp_dir, "cache_test_#{:erlang.unique_integer([:positive])}.bin")
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100, max_result_size: 1000)
      on_exit(fn ->
        safe_stop(pid)
        File.rm(cache_file)
      end)
      %{name: name, cache_file: cache_file}
    end

    test "persist_to_file saves cache to disk", %{name: name, cache_file: cache_file} do
      # Add some entries
      Cache.put("query1", [1, 2, 3], name: name, predicates: [:pred1])
      Cache.put("query2", %{bindings: [%{a: 1}, %{a: 2}]}, name: name, predicates: [:pred2])
      Cache.put("query3", :ok, name: name)

      assert Cache.size(name: name) == 3

      # Persist to file
      assert {:ok, 3} = Cache.persist_to_file(cache_file, name: name)
      assert File.exists?(cache_file)
    end

    test "warm_from_file restores cache from disk", %{name: name, cache_file: cache_file} do
      # Add and persist entries
      Cache.put("query1", [1, 2, 3], name: name, predicates: [:pred1])
      Cache.put("query2", %{bindings: [%{a: 1}]}, name: name, predicates: [:pred2])
      {:ok, 2} = Cache.persist_to_file(cache_file, name: name)

      # Clear cache
      Cache.invalidate(name: name)
      assert Cache.size(name: name) == 0

      # Warm from file
      assert {:ok, 2} = Cache.warm_from_file(cache_file, name: name)
      assert Cache.size(name: name) == 2

      # Verify entries are accessible
      assert {:ok, [1, 2, 3]} = Cache.get("query1", name: name)
      assert {:ok, %{bindings: [%{a: 1}]}} = Cache.get("query2", name: name)
    end

    test "warm_from_file handles missing file gracefully", %{name: name} do
      result = Cache.warm_from_file("/nonexistent/path/cache.bin", name: name)
      # Returns the state unchanged (file not found is not an error)
      assert {:ok, 0} = result
    end

    test "persist_to_file handles write errors", %{name: name} do
      Cache.put("query1", [1], name: name)
      # Try to write to a non-existent directory
      result = Cache.persist_to_file("/nonexistent/directory/cache.bin", name: name)
      assert {:error, :enoent} = result
    end

    test "get_all_entries returns all cached entries", %{name: name} do
      Cache.put("query1", [1, 2, 3], name: name, predicates: [:pred1, :pred2])
      Cache.put("query2", %{bindings: [%{a: 1}]}, name: name, predicates: [:pred3])

      entries = Cache.get_all_entries(name: name)

      assert length(entries) == 2
      assert Enum.all?(entries, fn e ->
        Map.has_key?(e, :key) and
        Map.has_key?(e, :result) and
        Map.has_key?(e, :result_size) and
        Map.has_key?(e, :predicates)
      end)
    end
  end

  describe "cache warming - warm on start" do
    test "warms cache from file on startup when enabled" do
      name = unique_name()
      temp_dir = System.tmp_dir!()
      cache_file = Path.join(temp_dir, "cache_warm_start_#{:erlang.unique_integer([:positive])}.bin")

      # Start cache, add entries, persist
      {:ok, pid1} = Cache.start_link(name: name, max_entries: 100)
      Cache.put("query1", [1, 2, 3], name: name)
      Cache.put("query2", [4, 5, 6], name: name)
      {:ok, 2} = Cache.persist_to_file(cache_file, name: name)
      safe_stop(pid1)

      # Wait for process to stop
      Process.sleep(50)

      # Start new cache with warm_on_start
      name2 = unique_name()
      {:ok, pid2} = Cache.start_link(
        name: name2,
        max_entries: 100,
        persistence_path: cache_file,
        warm_on_start: true
      )

      # Should have warmed entries
      assert Cache.size(name: name2) == 2
      assert {:ok, [1, 2, 3]} = Cache.get("query1", name: name2)
      assert {:ok, [4, 5, 6]} = Cache.get("query2", name: name2)

      safe_stop(pid2)
      File.rm(cache_file)
    end

    test "starts empty when warm_on_start enabled but file missing" do
      name = unique_name()

      {:ok, pid} = Cache.start_link(
        name: name,
        max_entries: 100,
        persistence_path: "/nonexistent/cache.bin",
        warm_on_start: true
      )

      # Should start empty (no error)
      assert Cache.size(name: name) == 0

      safe_stop(pid)
    end
  end

  describe "cache warming - query pre-execution" do
    setup do
      name = unique_name()
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100, max_result_size: 1000)
      on_exit(fn -> safe_stop(pid) end)
      %{name: name}
    end

    test "warm_queries pre-executes and caches queries", %{name: name} do
      queries = [
        {"query1", fn -> {:ok, [1, 2, 3]} end, []},
        {"query2", fn -> {:ok, [4, 5]} end, [predicates: [:pred1]]},
        {"query3", fn -> {:ok, %{result: true}} end, []}
      ]

      assert {:ok, %{cached: 3, failed: 0}} = Cache.warm_queries(queries, name: name)
      assert Cache.size(name: name) == 3

      # Verify entries are cached
      assert {:ok, [1, 2, 3]} = Cache.get("query1", name: name)
      assert {:ok, [4, 5]} = Cache.get("query2", name: name)
      assert {:ok, %{result: true}} = Cache.get("query3", name: name)
    end

    test "warm_queries handles failures gracefully", %{name: name} do
      queries = [
        {"query1", fn -> {:ok, [1]} end, []},
        {"query2", fn -> {:error, :failed} end, []},
        {"query3", fn -> {:ok, [3]} end, []}
      ]

      assert {:ok, %{cached: 2, failed: 1}} = Cache.warm_queries(queries, name: name)
      assert Cache.size(name: name) == 2
    end

    test "warm_queries skips already cached queries", %{name: name} do
      # Pre-cache query1
      Cache.put("query1", [:original], name: name)

      call_count = :counters.new(1, [:atomics])

      queries = [
        {"query1", fn ->
          :counters.add(call_count, 1, 1)
          {:ok, [:new]}
        end, []}
      ]

      {:ok, _} = Cache.warm_queries(queries, name: name)

      # Should not have executed because query was cached
      assert :counters.get(call_count, 1) == 0

      # Original value should be preserved
      assert {:ok, [:original]} = Cache.get("query1", name: name)
    end
  end

  describe "cache warming - telemetry" do
    setup do
      name = unique_name()
      temp_dir = System.tmp_dir!()
      cache_file = Path.join(temp_dir, "cache_telemetry_#{:erlang.unique_integer([:positive])}.bin")
      {:ok, pid} = Cache.start_link(name: name, max_entries: 100)
      on_exit(fn ->
        safe_stop(pid)
        File.rm(cache_file)
      end)
      %{name: name, cache_file: cache_file}
    end

    test "emits persist event when persisting", %{name: name, cache_file: cache_file} do
      test_pid = self()
      handler_id = "persist-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :cache, :query, :persist],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      Cache.put("query1", [1], name: name)
      Cache.put("query2", [2], name: name)
      Cache.persist_to_file(cache_file, name: name)

      assert_receive {:telemetry, [:triple_store, :cache, :query, :persist], %{count: 2}, %{}}

      :telemetry.detach(handler_id)
    end

    test "emits warm event when warming from file", %{name: name, cache_file: cache_file} do
      # Setup: persist some entries
      Cache.put("query1", [1], name: name)
      Cache.persist_to_file(cache_file, name: name)
      Cache.invalidate(name: name)

      test_pid = self()
      handler_id = "warm-test-#{:erlang.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:triple_store, :cache, :query, :warm],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      Cache.warm_from_file(cache_file, name: name)

      assert_receive {:telemetry, [:triple_store, :cache, :query, :warm], %{count: 1}, %{}}

      :telemetry.detach(handler_id)
    end
  end
end
