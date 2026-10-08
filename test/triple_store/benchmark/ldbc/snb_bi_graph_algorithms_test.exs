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

    assert result.rows ==
             [
               %{source: :a, target: :z, cost: 4},
               %{source: :b, target: :z, cost: 3}
             ]
             |> Enum.filter(&(&1.cost == 3))
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
end
