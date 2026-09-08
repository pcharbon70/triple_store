defmodule TripleStore.SPARQL.Leapfrog.QuadIteratorCleanupTest do
  use ExUnit.Case, async: false
  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Leapfrog.QuadLeapfrog

  setup do
    path = Path.join(System.tmp_dir!(), "quad-cleanup-#{System.unique_integer([:positive])}")
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)

    for quad <- [{1, 10, 100, 0}, {1, 10, 101, 0}, {1, 10, 102, 0}] do
      :ok = QuadOperations.insert_quad(db, quad)
    end

    on_exit(fn ->
      ErlangAdapter.close(db)
      File.rm_rf!(path)
    end)

    %{db: db}
  end

  test "direct scans close their iterator explicitly and idempotently", %{db: db} do
    qlf = scan!(db)
    pids = iterator_pids(qlf)
    assert pids != []
    assert Enum.all?(pids, &Process.alive?/1)
    assert :ok = QuadLeapfrog.close(qlf)
    assert :ok = QuadLeapfrog.close(qlf)
    assert Enum.all?(pids, &(not Process.alive?(&1)))
  end

  test "full consumption and early halt release every scan iterator", %{db: db} do
    for consume <- [&Enum.to_list/1, &Enum.take(&1, 1)] do
      qlf = scan!(db)
      pids = iterator_pids(qlf)
      assert consume.(QuadLeapfrog.stream(qlf)) != []
      assert Enum.all?(pids, &(not Process.alive?(&1)))
    end
  end

  test "consumer exceptions release scan iterators", %{db: db} do
    qlf = scan!(db)
    pids = iterator_pids(qlf)

    assert_raise RuntimeError, "consumer failed", fn ->
      qlf |> QuadLeapfrog.stream() |> Enum.each(fn _ -> raise "consumer failed" end)
    end

    assert Enum.all?(pids, &(not Process.alive?(&1)))
  end

  test "exhausted scans release their iterator without yielding", %{db: db} do
    {state, qlf} =
      QuadLeapfrog.from_pattern(
        db,
        {:quad, 999, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      )

    assert state in [:ok, :exhausted]
    pids = iterator_pids(qlf)
    assert Enum.to_list(QuadLeapfrog.stream(qlf)) == []
    assert Enum.all?(pids, &(not Process.alive?(&1)))
  end

  test "repeated public queries release exhausted multi-iterator fallback resources", %{db: db} do
    alias TripleStore.Dictionary.Manager
    alias TripleStore.SPARQL.Query
    {:ok, manager} = Manager.start_link(db: db)
    on_exit(fn -> if Process.alive?(manager), do: Manager.stop(manager) end)

    [s, p, o, g] =
      for name <- ~w[s p o g] do
        {:ok, id} = Manager.get_or_create_id(manager, RDF.iri("https://cleanup.test/#{name}"))
        id
      end

    :ok = QuadOperations.insert_quad(db, {s, p, o, g})
    context = %{db: db, dict_manager: manager, permit_all: true}

    query =
      "SELECT ?p ?o WHERE { GRAPH <https://cleanup.test/g> { <https://cleanup.test/s> ?p ?o } } LIMIT 64"

    {:ok, expected} = Query.query(context, query, use_cache: false)
    assert length(expected) == 1
    before = owned_iterators(db)

    for _ <- 1..50 do
      assert {:ok, ^expected} = Query.query(context, query, use_cache: false)
    end

    assert owned_iterators(db) == before
  end

  defp owned_iterators(db) do
    {:links, links} = Process.info(db, :links)

    links
    |> Enum.filter(fn pid ->
      is_pid(pid) and
        match?(
          {TripleStore.Backend.RocksDB.Iterator, _, _},
          :proc_lib.translate_initial_call(pid)
        )
    end)
    |> MapSet.new()
  end

  defp scan!(db) do
    {:ok, qlf} =
      QuadLeapfrog.from_pattern(
        db,
        {:quad, {:variable, "s"}, {:variable, "p"}, {:variable, "o"}, {:variable, "g"}}
      )

    qlf
  end

  defp iterator_pids(qlf), do: Enum.map(qlf.tagged_iterators, & &1.iterator.iter_ref)
end
