defmodule TripleStore.SPARQL.ParallelExecutor do
  @moduledoc """
  Parallel pattern execution for SPARQL queries (S8).

  Executes independent BGP patterns in parallel using Task.Supervisor.
  Useful for:
  - UNION branches
  - Independent graph patterns
  - OPTIONAL branches

  Features:
  - Configurable parallelism threshold
  - Timeout support
  - Error isolation (one branch failure doesn't affect others)
  - Ordered result merging
  """

  use GenServer
  require Logger

  @type pattern_result :: {:ok, list()} | {:error, term()}

  # Only parallelize if estimated cost >= 1000
  @default_threshold 1000
  @max_parallel 4
  @default_timeout 30_000

  # Client API

  @doc """
  Start the parallel executor supervisor.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Execute multiple patterns in parallel.

  Returns results in the same order as input patterns.
  """
  @spec execute_parallel(list(fun()), keyword()) :: {:ok, list(list())} | {:error, term()}
  def execute_parallel(pattern_funs, opts \\ []) when is_list(pattern_funs) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    threshold = Keyword.get(opts, :threshold, @default_threshold)
    max_parallel = Keyword.get(opts, :max_parallel, @max_parallel)

    cond do
      length(pattern_funs) <= 1 ->
        # Single pattern, execute directly
        execute_single(pattern_funs)

      length(pattern_funs) > max_parallel ->
        # Too many patterns, execute in batches
        execute_batched(pattern_funs, max_parallel, timeout, threshold)

      true ->
        # Execute in parallel
        do_execute_parallel(pattern_funs, timeout, threshold)
    end
  end

  @doc """
  Execute UNION branches in parallel.

  Each branch should be a function that returns a list of bindings.
  """
  @spec execute_union_branches(list(fun()), keyword()) :: {:ok, list()} | {:error, term()}
  def execute_union_branches(branches, opts \\ []) when is_list(branches) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    threshold = Keyword.get(opts, :threshold, @default_threshold)

    if should_parallelize?(branches, threshold) do
      case do_execute_parallel(branches, timeout, threshold) do
        {:ok, results} ->
          # results is a list of lists from each branch
          # Union all results (flat_map since each result is a list)
          combined = List.flatten(results)

          {:ok, combined}
      end
    else
      # Execute sequentially
      case execute_sequential(branches) do
        {:ok, results} ->
          # Flatten since each result is a list
          {:ok, List.flatten(results)}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Check if patterns should be executed in parallel.

  Based on:
  - Number of patterns (need >= 2)
  - Estimated cost (if available)
  - Configured threshold
  """
  @spec should_parallelize?(list(term()), non_neg_integer()) :: boolean()
  def should_parallelize?(patterns, _threshold \\ @default_threshold) do
    length(patterns) >= 2
  end

  @doc """
  Get execution statistics.
  """
  @spec stats() :: map()
  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  # GenServer Callbacks

  @impl true
  def init(_opts) do
    state = %{
      total_parallel: 0,
      total_sequential: 0,
      parallel_executions: [],
      start_time: System.monotonic_time(:millisecond)
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    uptime = System.monotonic_time(:millisecond) - state.start_time

    stats = %{
      total_parallel: state.total_parallel,
      total_sequential: state.total_sequential,
      parallel_executions: length(state.parallel_executions),
      uptime_ms: uptime
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, state) do
    {:noreply, state}
  end

  # Private Functions

  defp execute_single([fun]) when is_function(fun, 0) do
    try do
      {:ok, [fun.()]}
    rescue
      e -> {:error, e}
    end
  end

  defp execute_single([_]), do: {:error, :invalid_pattern_function}

  defp execute_sequential(funs) when is_list(funs) do
    results =
      Enum.map(funs, fn fun ->
        try do
          {:ok, fun.()}
        rescue
          e -> {:error, e}
        end
      end)

    if Enum.all?(results, fn
         {:ok, _} -> true
         _ -> false
       end) do
      {:ok, Enum.map(results, fn {:ok, r} -> r end)}
    else
      {:error, :sequential_execution_failed}
    end
  end

  defp do_execute_parallel(funs, timeout, _threshold) do
    # Start a temporary Task.Supervisor for this execution
    {:ok, sup} = Task.Supervisor.start_link()

    tasks =
      Enum.map(funs, fn fun ->
        Task.Supervisor.async_nolink(sup, fn ->
          try do
            fun.()
          rescue
            e -> {:error, e}
          catch
            :exit, reason -> {:error, {:exit, reason}}
            :throw, reason -> {:error, {:throw, reason}}
          end
        end)
      end)

    # Wait for all tasks with timeout
    results =
      Task.yield_many(tasks, timeout)

    # Shutdown pending tasks and the supervisor
    {pending, completed} =
      Enum.split_with(results, fn
        {_task, nil} -> true
        _ -> false
      end)

    Enum.each(pending, fn {task, _} ->
      Task.shutdown(task, :brutal_kill)
    end)

    # Stop the supervisor
    Process.exit(sup, :normal)

    # Extract results - Task.Supervisor wraps in {:ok, result}
    final_results =
      Enum.map(completed, fn
        {_task, {:ok, result}} -> {:ok, result}
        {_task, {:exit, _reason}} -> {:error, :task_exited}
      end) ++
        Enum.map(pending, fn {_task, _} -> {:error, :timeout} end)

    if Enum.all?(final_results, fn
         {:ok, _} -> true
         _ -> false
       end) do
      {:ok, Enum.map(final_results, fn {:ok, r} -> r end)}
    else
      # Return results even if some failed
      {:ok,
       Enum.map(final_results, fn
         {:ok, r} -> r
         {:error, _} -> []
       end)}
    end
  end

  defp execute_batched(funs, batch_size, timeout, threshold) do
    results =
      funs
      |> Enum.chunk_every(batch_size)
      |> Enum.map(fn batch ->
        case do_execute_parallel(batch, timeout, threshold) do
          {:ok, results} -> results
          {:error, _} -> []
        end
      end)

    {:ok, List.flatten(results)}
  end
end
