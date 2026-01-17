defmodule TripleStore.SPARQL.ParallelExecutorTest do
  @moduledoc """
  Tests for parallel pattern execution (S8).
  """

  use ExUnit.Case, async: false

  alias TripleStore.SPARQL.ParallelExecutor

  setup do
    start_supervised!(ParallelExecutor)
    :ok
  end

  describe "execute_parallel/2" do
    test "executes single pattern directly" do
      fun = fn -> [1, 2, 3] end

      assert {:ok, [[1, 2, 3]]} = ParallelExecutor.execute_parallel([fun])
    end

    test "executes multiple patterns in parallel" do
      fun1 = fn -> Process.sleep(10); [1, 2] end
      fun2 = fn -> Process.sleep(10); [3, 4] end
      fun3 = fn -> Process.sleep(10); [5, 6] end

      assert {:ok, results} = ParallelExecutor.execute_parallel([fun1, fun2, fun3])
      assert length(results) == 3
      assert [1, 2] in results
      assert [3, 4] in results
      assert [5, 6] in results
    end

    test "maintains result order" do
      fun1 = fn -> [1] end
      fun2 = fn -> [2] end
      fun3 = fn -> [3] end

      assert {:ok, [[1], [2], [3]]} = ParallelExecutor.execute_parallel([fun1, fun2, fun3])
    end

    test "handles pattern that raises error" do
      fun1 = fn -> [1, 2] end
      fun2 = fn -> raise "error" end

      # Returns empty list for failed pattern
      assert {:ok, results} = ParallelExecutor.execute_parallel([fun1, fun2])
      assert [1, 2] in results
    end

    test "handles pattern that exits" do
      fun1 = fn -> [1, 2] end
      fun2 = fn -> exit(:normal) end

      assert {:ok, results} = ParallelExecutor.execute_parallel([fun1, fun2])
      assert [1, 2] in results
    end

    test "handles pattern that throws" do
      fun1 = fn -> [1, 2] end
      fun2 = fn -> throw(:error) end

      assert {:ok, results} = ParallelExecutor.execute_parallel([fun1, fun2])
      assert [1, 2] in results
    end

    test "respects timeout" do
      fun1 = fn -> [1, 2] end
      fun2 = fn -> Process.sleep(1000); [3, 4] end

      assert {:ok, results} = ParallelExecutor.execute_parallel([fun1, fun2], timeout: 50)
      # fun2 should timeout and return empty
      assert [1, 2] in results
    end

    test "batches when exceeding max_parallel" do
      funs = for i <- 1..10, do: fn -> [i] end

      assert {:ok, results} = ParallelExecutor.execute_parallel(funs, max_parallel: 4)
      assert length(results) == 10
      assert List.flatten(results) == Enum.to_list(1..10)
    end
  end

  describe "execute_union_branches/2" do
    test "combines results from multiple branches" do
      branch1 = fn -> [%{s: 1}] end
      branch2 = fn -> [%{s: 2}] end
      branch3 = fn -> [%{s: 3}] end

      assert {:ok, results} = ParallelExecutor.execute_union_branches([branch1, branch2, branch3])
      assert length(results) == 3
      assert %{s: 1} in results
      assert %{s: 2} in results
      assert %{s: 3} in results
    end

    test "handles empty branches" do
      branch1 = fn -> [] end
      branch2 = fn -> [%{s: 1}] end

      assert {:ok, results} = ParallelExecutor.execute_union_branches([branch1, branch2])
      assert results == [%{s: 1}]
    end

    test "handles failing branch with partial results" do
      branch1 = fn -> [%{s: 1}] end
      branch2 = fn -> raise "error" end
      branch3 = fn -> [%{s: 3}] end

      # Returns partial results
      assert {:ok, results} = ParallelExecutor.execute_union_branches([branch1, branch2, branch3])
      assert %{s: 1} in results
      assert %{s: 3} in results
    end

    test "executes sequentially for single branch" do
      branch = fn -> [%{s: 1}] end

      assert {:ok, results} = ParallelExecutor.execute_union_branches([branch])
      assert results == [%{s: 1}]
    end
  end

  describe "should_parallelize?/2" do
    test "returns false for single pattern" do
      refute ParallelExecutor.should_parallelize?([fn -> [] end])
    end

    test "returns true for multiple patterns" do
      assert ParallelExecutor.should_parallelize?([fn -> [] end, fn -> [] end])
    end

    test "respects threshold" do
      assert ParallelExecutor.should_parallelize?([1, 2], 0)
      assert ParallelExecutor.should_parallelize?([1, 2], 100)
    end
  end

  describe "stats/0" do
    test "returns execution statistics" do
      stats = ParallelExecutor.stats()

      assert is_map(stats)
      assert Map.has_key?(stats, :total_parallel)
      assert Map.has_key?(stats, :total_sequential)
      assert Map.has_key?(stats, :parallel_executions)
      assert Map.has_key?(stats, :uptime_ms)
    end

    test "tracks uptime" do
      stats1 = ParallelExecutor.stats()
      Process.sleep(100)
      stats2 = ParallelExecutor.stats()

      assert stats2.uptime_ms > stats1.uptime_ms
    end
  end

  describe "error isolation" do
    test "one failing pattern doesn't affect others" do
      fun1 = fn -> Process.sleep(10); [1] end
      fun2 = fn -> Process.sleep(10); raise "error" end
      fun3 = fn -> Process.sleep(10); [3] end

      assert {:ok, results} = ParallelExecutor.execute_parallel([fun1, fun2, fun3])
      assert [1] in results
      assert [3] in results
    end

    test "timeout doesn't affect completed tasks" do
      fun1 = fn -> [1] end
      fun2 = fn -> Process.sleep(1000); [2] end
      fun3 = fn -> [3] end

      assert {:ok, results} = ParallelExecutor.execute_parallel([fun1, fun2, fun3], timeout: 50)
      assert [1] in results
      assert [3] in results
    end
  end

  describe "complex scenarios" do
    test "handles nested list results" do
      fun1 = fn -> [[1, 2], [3, 4]] end
      fun2 = fn -> [[5, 6]] end

      assert {:ok, results} = ParallelExecutor.execute_parallel([fun1, fun2])
      assert [[1, 2], [3, 4]] in results
      assert [[5, 6]] in results
    end

    test "handles map results" do
      fun1 = fn -> %{a: 1} end
      fun2 = fn -> %{b: 2} end

      assert {:ok, results} = ParallelExecutor.execute_parallel([fun1, fun2])
      assert %{a: 1} in results
      assert %{b: 2} in results
    end

    test "handles mixed result types" do
      fun1 = fn -> [1, 2, 3] end
      fun2 = fn -> %{count: 3} end
      fun3 = fn -> "result" end

      assert {:ok, results} = ParallelExecutor.execute_parallel([fun1, fun2, fun3])
      assert [1, 2, 3] in results
      assert %{count: 3} in results
      assert "result" in results
    end
  end

  describe "performance characteristics" do
    @tag :benchmark
    test "parallel execution is faster than sequential for slow operations" do
      slow_fun = fn -> Process.sleep(50); [1] end

      # Sequential: 3 * 50ms = 150ms
      {time_seq, _} =
        :timer.tc(fn ->
          Enum.each([slow_fun, slow_fun, slow_fun], fn fun -> fun.() end)
        end)

      # Parallel: ~50ms (limited by slowest)
      {time_par, _} =
        :timer.tc(fn ->
          ParallelExecutor.execute_parallel([slow_fun, slow_fun, slow_fun])
        end)

      # Parallel should be significantly faster
      assert time_par < time_seq
    end
  end
end
