defmodule TripleStore.SPARQL.Leapfrog.QuadScanReferenceIntegrationTest do
  @moduledoc """
  Compares QuadLeapfrog with the independent QuadOperations lookup path.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Leapfrog.QuadLeapfrog

  @quads [
    {1, 10, 100, 0},
    {1, 10, 101, 0},
    {1, 11, 100, 5},
    {2, 10, 100, 5},
    {2, 10, 100, 9},
    {7, 7, 9, 11}
  ]

  setup do
    path =
      Path.join(System.tmp_dir!(), "quad-scan-reference-#{System.unique_integer([:positive])}")

    {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    :ok = QuadOperations.insert_quads(db, @quads, sync: false)
    :ok = QuadOperations.insert_quad(db, hd(@quads))

    on_exit(fn ->
      ErlangAdapter.close(db)
      File.rm_rf!(path)
    end)

    %{db: db}
  end

  test "matches reference lookup across graph IDs and bound-position shapes", %{db: db} do
    variable = fn name -> {:variable, name} end

    patterns = [
      {:quad, variable.("s"), variable.("p"), variable.("o"), variable.("g")},
      {:quad, variable.("s"), variable.("p"), variable.("o"), 0},
      {:quad, variable.("s"), variable.("p"), variable.("o"), 5},
      {:quad, 1, variable.("p"), variable.("o"), variable.("g")},
      {:quad, variable.("s"), 10, variable.("o"), variable.("g")},
      {:quad, variable.("s"), variable.("p"), 100, variable.("g")},
      {:quad, 1, 10, variable.("o"), 0},
      {:quad, variable.("s"), 10, 100, 5},
      {:quad, 2, variable.("p"), 100, 9},
      {:quad, variable.("same"), variable.("same"), variable.("o"), 11},
      {:quad, 1, 10, 100, 0},
      {:quad, 999, 10, 100, 0}
    ]

    for pattern <- patterns do
      assert leapfrog_quads(db, pattern) == reference_quads(db, pattern),
             "quad scan diverged for #{inspect(pattern)}"
    end
  end

  test "matches reference lookup for an empty dataset" do
    path = Path.join(System.tmp_dir!(), "quad-scan-empty-#{System.unique_integer([:positive])}")
    {:ok, empty_db} = ErlangAdapter.open(path, schema: :quad)

    on_exit(fn ->
      ErlangAdapter.close(empty_db)
      File.rm_rf!(path)
    end)

    pattern =
      {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}

    assert leapfrog_quads(empty_db, pattern) == reference_quads(empty_db, pattern)
  end

  defp leapfrog_quads(db, pattern) do
    results =
      case QuadLeapfrog.from_pattern(db, pattern) do
        {:ok, scan} -> scan |> QuadLeapfrog.stream() |> Enum.to_list()
        {:exhausted, scan} -> close_and_empty(scan)
      end

    results
    |> Enum.map(&quad_from_binding(pattern, &1))
    |> Enum.sort()
  end

  defp reference_quads(db, pattern) do
    db
    |> QuadOperations.lookup_quads({:var, :var, :var, :var}, %{})
    |> Enum.filter(&matches_pattern?(pattern, &1))
    |> Enum.sort()
  end

  defp close_and_empty(scan) do
    :ok = QuadLeapfrog.close(scan)
    []
  end

  defp quad_from_binding({:quad, s, p, o, g}, binding) do
    [s, p, o, g]
    |> Enum.map(fn
      value when is_integer(value) -> value
      {:variable, name} -> Map.fetch!(binding, name)
    end)
    |> List.to_tuple()
  end

  defp matches_pattern?({:quad, s, p, o, g}, quad) do
    [s, p, o, g]
    |> Enum.zip(Tuple.to_list(quad))
    |> Enum.reduce_while(%{}, fn
      {value, value}, variables when is_integer(value) -> {:cont, variables}
      {value, _id}, _variables when is_integer(value) -> {:halt, false}
      {{:variable, name}, id}, variables -> match_variable(variables, name, id)
    end)
    |> is_map()
  end

  defp match_variable(variables, name, id) do
    case Map.fetch(variables, name) do
      :error -> {:cont, Map.put(variables, name, id)}
      {:ok, ^id} -> {:cont, variables}
      {:ok, _other} -> {:halt, false}
    end
  end
end
