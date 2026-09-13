defmodule TripleStore.SPARQL.Phase2CorrectnessTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Query.Cache
  alias TripleStore.SPARQL.UpdateExecutor

  @indices [:gspo, :gpos, :spog, :posg]

  test "failed quad MODIFY leaves every explicit index unchanged and does not invalidate" do
    path = unique_path("failed_modify")
    {:ok, db} = ErlangAdapter.open(path, schema: :quad, mixed_batch_failure: :injected)
    {:ok, manager} = Manager.start_link(db: db)
    cache_name = unique_name(:failed)
    {:ok, cache} = Cache.start_link(name: cache_name)
    {:ok, store_id} = ErlangAdapter.instance_id(db)
    ctx = %{db: db, dict_manager: manager}

    on_exit(fn -> cleanup(path, db, manager, cache) end)

    old = quad("old", "g")
    assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, [old])
    before = index_contents(db)
    assert :ok = Cache.put(:query, [:cached], name: cache_name, store_id: store_id)

    assert {:error, :injected} =
             UpdateExecutor.execute_modify(ctx, [old], [quad("new", "g")], nil)

    assert before == index_contents(db)
    assert {:ok, [:cached]} = Cache.get(:query, name: cache_name, store_id: store_id)
  end

  test "quad MODIFY uses delete-before-insert and retains compatible template counts" do
    path = unique_path("counts")
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)
    ctx = %{db: db, dict_manager: manager}
    on_exit(fn -> cleanup(path, db, manager, nil) end)

    existing = quad("same", "g")
    assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, [existing])

    # Duplicate template applications remain reflected in the count. RocksDB
    # indices retain set semantics, and a missing term in DELETE is a no-op.
    assert {:ok, 4} =
             UpdateExecutor.execute_modify(
               ctx,
               [existing, existing, quad("missing", "g")],
               [existing, existing],
               nil
             )

    assert Enum.all?(@indices, fn index -> length(index_contents(db)[index]) == 1 end)
  end

  test "committed INSERT DATA and MODIFY invalidate all named caches for their store" do
    path = unique_path("invalidation")
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)
    ctx = %{db: db, dict_manager: manager}
    {:ok, store_id} = ErlangAdapter.instance_id(db)
    cache_names = [unique_name(:one), unique_name(:two)]
    caches = Enum.map(cache_names, fn name -> elem(Cache.start_link(name: name), 1) end)
    on_exit(fn -> cleanup(path, db, manager, caches) end)

    warm_all(cache_names, store_id)
    assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, [quad("old", "g")])
    assert_all_missed(cache_names, store_id)

    warm_all(cache_names, store_id)

    assert {:ok, 2} =
             UpdateExecutor.execute_modify(ctx, [quad("old", "g")], [quad("new", "g")], nil)

    assert_all_missed(cache_names, store_id)

    warm_all(cache_names, store_id)

    assert {:ok, 1} =
             UpdateExecutor.execute_clear(ctx, graph: {:named_node, "http://example.org/g"})

    assert_all_missed(cache_names, store_id)
  end

  test "direct triple insert and delete invalidate materialized results" do
    path = unique_path("direct")
    {:ok, store} = TripleStore.open(path)
    {:ok, store_id} = ErlangAdapter.instance_id(store.db)
    cache_name = unique_name(:direct)
    {:ok, cache} = Cache.start_link(name: cache_name)
    triple = {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("o")}

    on_exit(fn ->
      if Process.alive?(cache), do: GenServer.stop(cache)
      TripleStore.close(store)
      File.rm_rf(path)
    end)

    assert :ok = Cache.put(:query, [], name: cache_name, store_id: store_id)
    assert {:ok, 1} = TripleStore.insert(store, triple)
    assert :miss = Cache.get(:query, name: cache_name, store_id: store_id)

    assert :ok = Cache.put(:query, [:row], name: cache_name, store_id: store_id)
    assert {:ok, 1} = TripleStore.delete(store, triple)
    assert :miss = Cache.get(:query, name: cache_name, store_id: store_id)
  end

  test "successful quad MODIFY survives reopen with all four indices consistent" do
    path = unique_path("reopen")
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)
    ctx = %{db: db, dict_manager: manager}

    assert {:ok, 1} = UpdateExecutor.execute_insert_data(ctx, [quad("old", "g")])

    assert {:ok, 2} =
             UpdateExecutor.execute_modify(ctx, [quad("old", "g")], [quad("new", "g")], nil)

    Manager.stop(manager)
    :ok = ErlangAdapter.close(db)

    {:ok, reopened} = ErlangAdapter.open(path, schema: :quad)

    try do
      contents = index_contents(reopened)
      assert Enum.all?(@indices, fn index -> length(contents[index]) == 1 end)
    after
      ErlangAdapter.close(reopened)
      File.rm_rf(path)
    end
  end

  test "invalid graph conversion fails before an explicit-index batch" do
    path = unique_path("conversion")
    {:ok, db} = ErlangAdapter.open(path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)
    ctx = %{db: db, dict_manager: manager}
    before = index_contents(db)
    on_exit(fn -> cleanup(path, db, manager, nil) end)

    invalid =
      {:quad, {:named_node, "http://example.org/s"}, {:named_node, "http://example.org/p"},
       {:literal, :simple, "o"}, {:blank_node, "not-a-graph"}}

    assert {:error, :invalid_quad} = UpdateExecutor.execute_modify(ctx, [], [invalid], nil)
    assert before == index_contents(db)
  end

  defp warm_all(names, store_id) do
    Enum.each(names, fn name ->
      assert :ok = Cache.put(:query, [], name: name, store_id: store_id)
    end)
  end

  defp assert_all_missed(names, store_id) do
    Enum.each(names, fn name ->
      assert :miss = Cache.get(:query, name: name, store_id: store_id)
    end)
  end

  defp index_contents(db) do
    Map.new(@indices, fn index ->
      entries = ErlangAdapter.fold(db, index, <<>>, [], fn entry, acc -> [entry | acc] end)
      {index, Enum.sort(entries)}
    end)
  end

  defp quad(value, graph) do
    {:quad, {:named_node, "http://example.org/#{value}"}, {:named_node, "http://example.org/p"},
     {:literal, :simple, value}, {:named_node, "http://example.org/#{graph}"}}
  end

  defp unique_path(label),
    do: "/tmp/triple_store_phase2_#{label}_#{System.unique_integer([:positive])}"

  defp unique_name(label),
    do: String.to_atom("phase2_#{label}_#{System.unique_integer([:positive])}")

  defp cleanup(path, db, manager, caches) do
    caches
    |> List.wrap()
    |> Enum.each(fn cache -> if Process.alive?(cache), do: GenServer.stop(cache) end)

    if Process.alive?(manager), do: Manager.stop(manager)
    ErlangAdapter.close(db)
    File.rm_rf(path)
  end
end
