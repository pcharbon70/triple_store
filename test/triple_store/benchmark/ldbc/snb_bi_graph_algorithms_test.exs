defmodule TripleStore.Benchmark.LDBC.SNBBIGraphAlgorithmsTest do
  use ExUnit.Case, async: true

  alias TripleStore.Benchmark.LDBC.SNB.BI.{Analytics, GraphAlgorithms, Workload}

  test "shortest path range is bounded, deterministic, and reports traversal evidence" do
    graph = %{
      a: [{:c, 1}, {:b, 1}],
      b: [{:d, 1}],
      c: [{:d, 1}],
      d: [{:e, 1}],
      e: []
    }

    assert {:ok, result} =
             GraphAlgorithms.shortest_path_range(:a, 2, 3,
               neighbors: &{:ok, Map.fetch!(graph, &1)},
               timeout: 1_000,
               max_visited: 10
             )

    assert result.rows == [%{distance: 2, vertex: :d}, %{distance: 3, vertex: :e}]
    assert result.metrics.expanded == 4
    assert result.explain.driver_side_graph_materialization == false
  end

  test "weighted traversal returns every globally cheapest endpoint pair with stable ties" do
    graph = %{
      a: [{:x, 2}, {:y, 1}],
      b: [{:x, 1}, {:y, 4}],
      x: [{:z, 2}],
      y: [{:z, 3}],
      z: []
    }

    assert {:ok, result} =
             GraphAlgorithms.cheapest_pairs([:b, :a], [:z],
               neighbors: &{:ok, Map.fetch!(graph, &1)},
               timeout: 1_000
             )

    assert result.rows == [%{source: :b, target: :z, cost: 3}]
  end

  test "weighted traversal supports fractional BI interaction costs" do
    graph = %{
      a: [{:b, 0.5}, {:c, 0.25}],
      b: [{:d, 0.5}],
      c: [{:d, 0.25}],
      d: []
    }

    assert {:ok, result} =
             GraphAlgorithms.cheapest_pairs([:a], [:d], neighbors: &{:ok, Map.fetch!(graph, &1)})

    assert result.rows == [%{source: :a, target: :d, cost: 0.5}]
  end

  test "traversal fails on cancellation and memory bounds" do
    neighbours = fn value -> {:ok, [{value + 1, 1}]} end

    assert {:error, :cancelled} =
             GraphAlgorithms.shortest_path_range(0, 1, 2,
               neighbors: neighbours,
               cancelled?: fn -> true end
             )

    assert {:error, {:memory_bound_exceeded, 2}} =
             GraphAlgorithms.shortest_path_range(0, 1, 10,
               neighbors: neighbours,
               max_visited: 2
             )
  end

  test "weighted traversal rejects malformed and non-positive weights with typed errors" do
    for {edge, expected} <- [
          {{:b, nil}, {:invalid_edge_weight, nil}},
          {{:b, 0}, {:invalid_edge_weight, 0}},
          {{:b, -1}, {:invalid_edge_weight, -1}},
          {:malformed, {:malformed_edge, :malformed}}
        ] do
      assert {:error, {:invalid_traversal_edge, :a, ^expected}} =
               GraphAlgorithms.cheapest_pairs([:a], [:b],
                 neighbors: fn
                   :a -> {:ok, [edge]}
                   :b -> {:ok, []}
                 end
               )
    end
  end

  test "traversal bounds intermediate edges before growing the frontier" do
    assert {:error, {:intermediate_bound_exceeded, 2}} =
             GraphAlgorithms.shortest_path_range(:a, 1, 2,
               neighbors: fn
                 :a -> {:ok, [{:b, 1}, {:c, 1}, {:d, 1}]}
                 _vertex -> {:ok, []}
               end,
               max_intermediate: 2
             )
  end

  test "bounded traversals match independent references for every four-vertex graph" do
    vertices = 0..3 |> Enum.to_list()
    possible_edges = for left <- vertices, right <- vertices, left < right, do: {left, right}

    for mask <- 0..(Bitwise.bsl(1, length(possible_edges)) - 1) do
      graph = graph_for_mask(vertices, possible_edges, mask)
      provider = &{:ok, Map.fetch!(graph, &1)}

      assert {:ok, bfs} =
               GraphAlgorithms.shortest_path_range(0, 1, 3,
                 neighbors: provider,
                 max_visited: 16
               )

      assert bfs.rows == reference_distances(graph, 0)

      assert {:ok, weighted} =
               GraphAlgorithms.cheapest_pairs([0], [3],
                 neighbors: provider,
                 max_visited: 16
               )

      assert weighted.rows == reference_cheapest(graph, 0, 3)
    end
  end

  test "analytics preserve result types, duplicates, ordering, and limits" do
    assert {:ok, definitions} = Workload.load()
    assert {:ok, definition} = Workload.fetch(definitions, 19, "a")

    rows = [
      %{"person1.id" => 2, "person2.id" => 4, "totalWeight" => 7},
      %{"person1.id" => 1, "person2.id" => 3, "totalWeight" => 7},
      %{"person1.id" => 1, "person2.id" => 3, "totalWeight" => 7}
    ]

    handler = fn _context, _parameters, _opts -> {:ok, rows} end
    context = %{operation_handlers: %{{19, "a"} => handler}}

    assert {:ok, result} = Analytics.execute(definition, context, %{})
    assert result.rows == [[1, 3, 7], [1, 3, 7], [2, 4, 7]]
    assert result.duplicate_semantics == :bag
  end

  defp graph_for_mask(vertices, possible_edges, mask) do
    Enum.reduce(Enum.with_index(possible_edges), Map.new(vertices, &{&1, []}), fn
      {{left, right}, bit}, graph ->
        if Bitwise.band(mask, Bitwise.bsl(1, bit)) == 0 do
          graph
        else
          weight = rem(left + right, 3) + 1

          graph
          |> Map.update!(left, &[{right, weight} | &1])
          |> Map.update!(right, &[{left, weight} | &1])
        end
    end)
  end

  defp reference_distances(graph, source) do
    reference_bfs([{source, 0}], graph, %{source => 0})
    |> Enum.reject(fn {vertex, _distance} -> vertex == source end)
    |> Enum.map(fn {vertex, distance} -> %{vertex: vertex, distance: distance} end)
    |> Enum.sort_by(&{&1.distance, &1.vertex})
  end

  defp reference_bfs([], _graph, distances), do: distances

  defp reference_bfs([{vertex, distance} | queue], graph, distances) do
    {queue, distances} =
      Enum.reduce(Map.fetch!(graph, vertex), {queue, distances}, fn {neighbour, _weight}, acc ->
        {pending, known} = acc

        if Map.has_key?(known, neighbour) do
          acc
        else
          {pending ++ [{neighbour, distance + 1}], Map.put(known, neighbour, distance + 1)}
        end
      end)

    reference_bfs(queue, graph, distances)
  end

  defp reference_cheapest(graph, source, target) do
    case reference_dijkstra([{0, source}], graph, %{source => 0}, target) do
      :unreachable -> []
      cost -> [%{source: source, target: target, cost: cost}]
    end
  end

  defp reference_dijkstra([], _graph, _costs, _target), do: :unreachable

  defp reference_dijkstra(frontier, graph, costs, target) do
    [{cost, vertex} | rest] = Enum.sort(frontier)

    cond do
      cost != Map.fetch!(costs, vertex) ->
        reference_dijkstra(rest, graph, costs, target)

      vertex == target ->
        cost

      true ->
        {frontier, costs} = reference_relax(Map.fetch!(graph, vertex), cost, rest, costs)

        reference_dijkstra(frontier, graph, costs, target)
    end
  end

  defp reference_relax(neighbours, cost, frontier, costs) do
    Enum.reduce(neighbours, {frontier, costs}, fn {neighbour, weight}, {pending, known} = acc ->
      candidate = cost + weight

      if candidate < Map.get(known, neighbour, :infinity) do
        {[{candidate, neighbour} | pending], Map.put(known, neighbour, candidate)}
      else
        acc
      end
    end)
  end
end
