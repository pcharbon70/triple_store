defmodule TripleStore.Benchmark.LDBC.SNB.BI.GraphAlgorithms do
  @moduledoc """
  Bounded, deterministic graph traversals for SNB BI extension operations.

  Traversals request neighbours lazily from an engine-owned provider. They do
  not copy the graph into the benchmark driver. Limits, cancellation, and
  deadlines are checked while expanding the frontier. Equal-cost outputs are
  ordered by the stable term encoding of their endpoint IDs.
  """

  @telemetry_prefix [:triple_store, :benchmark, :ldbc, :snb_bi, :traversal]

  @type provider :: (term() -> {:ok, [{term(), number()}]} | {:error, term()})

  @doc "Returns all vertices at a shortest unweighted distance within an inclusive range."
  @spec shortest_path_range(term(), non_neg_integer(), non_neg_integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def shortest_path_range(start, minimum, maximum, opts)
      when is_integer(minimum) and minimum >= 0 and is_integer(maximum) and maximum >= minimum do
    with {:ok, runtime} <- runtime(opts) do
      run(:shortest_path_range, runtime, fn state ->
        bfs([{start, 0}], %{start => 0}, minimum, maximum, state)
      end)
    end
  end

  @doc "Returns every source/target pair tied for globally cheapest weighted path."
  @spec cheapest_pairs([term()], [term()], keyword()) :: {:ok, map()} | {:error, term()}
  def cheapest_pairs(sources, targets, opts) when is_list(sources) and is_list(targets) do
    with {:ok, runtime} <- runtime(opts) do
      run(:cheapest_pairs, runtime, fn state ->
        target_set = MapSet.new(targets)

        sources
        |> stable_sort()
        |> traverse_sources(target_set, state)
        |> case do
          {:ok, [], final} -> {:ok, %{rows: [], state: final}}
          {:ok, pairs, final} -> {:ok, %{rows: globally_cheapest(pairs), state: final}}
          {:error, _} = error -> error
        end
      end)
    end
  end

  defp traverse_sources(sources, target_set, state) do
    Enum.reduce_while(sources, {:ok, [], state}, fn source, {:ok, pairs, current} ->
      traverse_source(source, target_set, pairs, current)
    end)
  end

  defp traverse_source(source, target_set, pairs, state) do
    case dijkstra(source, target_set, state) do
      {:ok, costs, next} ->
        source_pairs = Enum.map(costs, fn {target, cost} -> {source, target, cost} end)
        {:cont, {:ok, pairs ++ source_pairs, next}}

      {:error, _} = error ->
        {:halt, error}
    end
  end

  @doc "Describes the measurable traversal boundary for explain artifacts."
  @spec explain(atom(), keyword()) :: map()
  def explain(algorithm, opts \\ []) do
    %{
      algorithm: algorithm,
      provider: :index_backed,
      frontier: if(algorithm == :cheapest_pairs, do: :priority_queue, else: :fifo),
      max_visited: Keyword.get(opts, :max_visited, 1_000_000),
      timeout_ms: Keyword.get(opts, :timeout, 120_000),
      deterministic_ties: true,
      driver_side_graph_materialization: false
    }
  end

  @doc "Builds an index-backed undirected neighbour provider for one predicate."
  @spec index_provider(pid(), non_neg_integer(), [non_neg_integer()] | :all, keyword()) ::
          provider()
  def index_provider(db, predicate_id, graphs \\ :all, opts \\ [])
      when is_pid(db) and is_integer(predicate_id) and predicate_id >= 0 do
    weight = Keyword.get(opts, :weight, fn _left, _right, _graph -> 1 end)

    fn vertex ->
      outgoing =
        TripleStore.QuadOperations.lookup_quads(
          db,
          {:bound, :bound, :var, :var},
          %{s: vertex, p: predicate_id}
        )

      incoming =
        TripleStore.QuadOperations.lookup_quads(
          db,
          {:var, :bound, :bound, :var},
          %{p: predicate_id, o: vertex}
        )

      neighbours =
        outgoing
        |> Enum.map(fn {_subject, _predicate, object, graph} -> {object, graph} end)
        |> Kernel.++(
          Enum.map(incoming, fn {subject, _predicate, _object, graph} -> {subject, graph} end)
        )
        |> Enum.filter(fn {_neighbour, graph} -> graphs == :all or graph in graphs end)
        |> Enum.map(fn {neighbour, graph} -> {neighbour, weight.(vertex, neighbour, graph)} end)
        |> Enum.uniq()

      {:ok, neighbours}
    end
  end

  defp runtime(opts) do
    provider = Keyword.get(opts, :neighbors)
    timeout = Keyword.get(opts, :timeout, 120_000)
    max_visited = Keyword.get(opts, :max_visited, 1_000_000)
    cancelled? = Keyword.get(opts, :cancelled?, fn -> false end)

    if is_function(provider, 1) and is_integer(timeout) and timeout > 0 and
         is_integer(max_visited) and max_visited > 0 and is_function(cancelled?, 0) do
      {:ok,
       %{
         provider: provider,
         deadline: System.monotonic_time(:millisecond) + timeout,
         max_visited: max_visited,
         cancelled?: cancelled?,
         expanded: 0,
         edges: 0,
         frontier_peak: 1
       }}
    else
      {:error, :invalid_traversal_options}
    end
  end

  defp run(algorithm, runtime, fun) do
    started = System.monotonic_time()
    result = fun.(runtime)
    duration = System.monotonic_time() - started

    metadata = %{algorithm: algorithm, status: status(result)}
    measurements = measurements(result, duration)
    :telemetry.execute(@telemetry_prefix ++ [:stop], measurements, metadata)

    case result do
      {:ok, %{rows: rows, state: state}} ->
        {:ok,
         %{
           rows: rows,
           metrics: Map.take(state, [:expanded, :edges, :frontier_peak]),
           explain: explain(algorithm, max_visited: state.max_visited)
         }}

      {:error, _} = error ->
        error
    end
  end

  defp bfs([], _visited, _minimum, _maximum, state),
    do: {:ok, %{rows: [], state: state}}

  defp bfs(queue, visited, minimum, maximum, state) do
    with :ok <- checkpoint(state, map_size(visited)),
         {{vertex, distance}, rest} <- pop_front(queue) do
      if distance == maximum do
        finish_bfs(rest, visited, minimum, state)
      else
        expand_bfs(vertex, distance, rest, visited, minimum, maximum, state)
      end
    end
  end

  defp expand_bfs(vertex, distance, rest, visited, minimum, maximum, state) do
    case state.provider.(vertex) do
      {:ok, neighbours} ->
        {next_queue, next_visited} = enqueue_unseen(neighbours, rest, visited, distance + 1)
        next_state = bump(state, length(neighbours), length(next_queue))
        bfs(next_queue, next_visited, minimum, maximum, next_state)

      {:error, reason} ->
        {:error, {:neighbour_lookup_failed, vertex, reason}}
    end
  end

  defp enqueue_unseen(neighbours, queue, visited, distance) do
    neighbours
    |> stable_neighbours()
    |> Enum.reduce({queue, visited}, fn {neighbour, _weight}, {pending, seen} ->
      enqueue_neighbour(neighbour, distance, pending, seen)
    end)
  end

  defp enqueue_neighbour(neighbour, distance, pending, seen) do
    if Map.has_key?(seen, neighbour) do
      {pending, seen}
    else
      {pending ++ [{neighbour, distance}], Map.put(seen, neighbour, distance)}
    end
  end

  defp finish_bfs(queue, visited, minimum, state) do
    if Enum.any?(queue, fn {_vertex, distance} -> distance < minimum end) do
      bfs(queue, visited, minimum, minimum, state)
    else
      rows =
        visited
        |> Enum.reject(fn {_vertex, distance} -> distance < minimum end)
        |> Enum.map(fn {vertex, distance} -> %{vertex: vertex, distance: distance} end)
        |> Enum.sort_by(fn row -> {row.distance, stable_key(row.vertex)} end)

      {:ok, %{rows: rows, state: state}}
    end
  end

  defp dijkstra(source, targets, state) do
    dijkstra_loop([{0, stable_key(source), source}], %{source => 0}, targets, %{}, state)
  end

  defp dijkstra_loop([], _distances, _targets, found, state),
    do: {:ok, stable_costs(found), state}

  defp dijkstra_loop(frontier, distances, targets, found, state) do
    [{cost, _key, vertex} | rest] = Enum.sort(frontier)

    with :ok <- checkpoint(state, map_size(distances)) do
      cond do
        cost != Map.fetch!(distances, vertex) ->
          dijkstra_loop(rest, distances, targets, found, state)

        complete_at_lower_cost?(found, cost) ->
          {:ok, stable_costs(found), state}

        true ->
          next_found =
            if MapSet.member?(targets, vertex), do: Map.put(found, vertex, cost), else: found

          case state.provider.(vertex) do
            {:ok, neighbours} ->
              {next_frontier, next_distances} =
                relax(neighbours, cost, rest, distances)

              next_state = bump(state, length(neighbours), length(next_frontier))
              dijkstra_loop(next_frontier, next_distances, targets, next_found, next_state)

            {:error, reason} ->
              {:error, {:neighbour_lookup_failed, vertex, reason}}
          end
      end
    end
  end

  defp relax(neighbours, cost, frontier, distances) do
    Enum.reduce(stable_neighbours(neighbours), {frontier, distances}, fn
      {_vertex, weight}, _acc when not is_number(weight) or weight <= 0 ->
        throw({:invalid_edge_weight, weight})

      {vertex, weight}, {pending, known} ->
        candidate = cost + weight

        if candidate < Map.get(known, vertex, :infinity) do
          {[{candidate, stable_key(vertex), vertex} | pending], Map.put(known, vertex, candidate)}
        else
          {pending, known}
        end
    end)
  catch
    {:invalid_edge_weight, weight} -> throw({:traversal_error, {:invalid_edge_weight, weight}})
  end

  defp checkpoint(state, visited) do
    cond do
      state.cancelled?.() -> {:error, :cancelled}
      System.monotonic_time(:millisecond) >= state.deadline -> {:error, :timeout}
      visited > state.max_visited -> {:error, {:memory_bound_exceeded, state.max_visited}}
      true -> :ok
    end
  end

  defp bump(state, edges, frontier_size) do
    %{
      state
      | expanded: state.expanded + 1,
        edges: state.edges + edges,
        frontier_peak: max(state.frontier_peak, frontier_size)
    }
  end

  defp globally_cheapest(pairs) do
    minimum = pairs |> Enum.map(&elem(&1, 2)) |> Enum.min()

    pairs
    |> Enum.filter(&(elem(&1, 2) == minimum))
    |> Enum.sort_by(fn {left, right, _cost} -> {stable_key(left), stable_key(right)} end)
    |> Enum.map(fn {left, right, cost} -> %{source: left, target: right, cost: cost} end)
  end

  defp complete_at_lower_cost?(found, cost),
    do: found != %{} and found |> Map.values() |> Enum.min() < cost

  defp stable_costs(costs),
    do: Enum.sort_by(costs, fn {vertex, cost} -> {cost, stable_key(vertex)} end)

  defp stable_neighbours(neighbours),
    do: Enum.sort_by(neighbours, fn {vertex, _} -> stable_key(vertex) end)

  defp stable_sort(values), do: Enum.sort_by(values, &stable_key/1)
  defp stable_key(value), do: :erlang.term_to_binary(value, [:deterministic])
  defp pop_front([head | tail]), do: {head, tail}

  defp status({:ok, _value}), do: :ok
  defp status({:error, _reason}), do: :error

  defp measurements({:ok, %{state: state}}, duration),
    do: %{duration: duration, expanded: state.expanded, edges: state.edges}

  defp measurements(_result, duration), do: %{duration: duration}
end
