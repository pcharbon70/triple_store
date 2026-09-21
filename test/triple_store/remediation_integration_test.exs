defmodule TripleStore.RemediationIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.QuadOperations
  alias TripleStore.Query.Cache
  alias TripleStore.SPARQL.Authorization
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.Query
  alias TripleStore.SPARQL.UpdateExecutor

  @indices [:gspo, :gpos, :spog, :posg]
  @subject "http://example.org/subject"
  @predicate "http://example.org/predicate"

  test "authorized variable-graph MODIFY refreshes only the mutated store across caches" do
    first = open_store("authorized")
    second = open_store("isolated")
    cache_names = [unique_name(:primary), unique_name(:secondary)]
    caches = Enum.map(cache_names, fn name -> elem(Cache.start_link(name: name), 1) end)
    on_exit(fn -> cleanup([first, second], caches) end)

    graph = "http://example.org/authorized"
    editor = %{id: "editor", roles: [:editor]}
    viewer = %{id: "viewer", roles: [:viewer]}
    editor_ctx = Map.put(first.ctx, :user, editor)
    viewer_ctx = Map.put(first.ctx, :user, viewer)

    seed_graph(first, graph, "old")
    seed_graph(second, "http://example.org/isolated", "other")
    :ok = Authorization.grant(first.ctx, graph, editor.id, :read)
    :ok = Authorization.grant(first.ctx, graph, editor.id, :write)

    warm_actor_entries(cache_names, first.store_id, [editor.id, viewer.id])
    warm_actor_entries(cache_names, second.store_id, [editor.id, viewer.id])

    assert {:ok, 2} = execute_variable_modify(editor_ctx)

    for cache_name <- cache_names, actor <- [editor.id, viewer.id] do
      assert :miss =
               Cache.get({:protected_query, first.store_id, actor},
                 name: cache_name,
                 store_id: first.store_id
               )

      assert {:ok, [^actor]} =
               Cache.get({:protected_query, second.store_id, actor},
                 name: cache_name,
                 store_id: second.store_id
               )
    end

    query =
      "SELECT ?o WHERE { GRAPH <#{graph}> { <#{@subject}> <#{@predicate}> ?o } }"

    assert {:ok, [%{"o" => {:literal, :simple, "new"}}]} =
             Query.query(editor_ctx, query, use_cache: true, cache_name: hd(cache_names))

    assert {:error, :unauthorized} =
             Query.query(viewer_ctx, query, use_cache: true, cache_name: hd(cache_names))

    assert_consistent_single_quad(first.db)
  end

  test "authorization denial and atomic write failure preserve indices and caches" do
    store = open_store("failed", mixed_batch_failure: :injected)
    cache_name = unique_name(:failed)
    {:ok, cache} = Cache.start_link(name: cache_name)
    on_exit(fn -> cleanup([store], [cache]) end)

    graph = "http://example.org/failure"
    editor = %{id: "editor", roles: [:editor]}
    viewer = %{id: "viewer", roles: [:viewer]}
    editor_ctx = Map.put(store.ctx, :user, editor)
    viewer_ctx = Map.put(store.ctx, :user, viewer)

    seed_graph(store, graph, "old")
    :ok = Authorization.grant(store.ctx, graph, editor.id, :read)
    :ok = Authorization.grant(store.ctx, graph, editor.id, :write)

    before = index_contents(store.db)

    assert :ok =
             Cache.put(:protected_query, [:current], name: cache_name, store_id: store.store_id)

    assert {:error, :unauthorized} = execute_variable_modify(viewer_ctx)
    assert before == index_contents(store.db)

    assert {:ok, [:current]} =
             Cache.get(:protected_query, name: cache_name, store_id: store.store_id)

    assert {:error, {:storage, :injected}} = execute_variable_modify(editor_ctx)
    assert before == index_contents(store.db)

    assert {:ok, [:current]} =
             Cache.get(:protected_query, name: cache_name, store_id: store.store_id)
  end

  defp execute_variable_modify(ctx) do
    update = """
    DELETE {
      GRAPH ?g { <#{@subject}> <#{@predicate}> "old" . }
    }
    INSERT {
      GRAPH ?g { <#{@subject}> <#{@predicate}> "new" . }
    }
    WHERE {
      GRAPH ?g { <#{@subject}> <#{@predicate}> "old" . }
    }
    """

    {:ok, ast} = Parser.parse_update(update)
    UpdateExecutor.execute(ctx, ast)
  end

  defp seed_graph(store, graph, value) do
    {:ok, subject} = Manager.get_or_create_id(store.manager, RDF.iri(@subject))
    {:ok, predicate} = Manager.get_or_create_id(store.manager, RDF.iri(@predicate))
    {:ok, object} = Manager.get_or_create_id(store.manager, RDF.literal(value))
    {:ok, graph_id} = Manager.get_or_create_id(store.manager, RDF.iri(graph))
    :ok = QuadOperations.insert_quad(store.db, {subject, predicate, object, graph_id})
  end

  defp warm_actor_entries(cache_names, store_id, actors) do
    for cache_name <- cache_names, actor <- actors do
      assert :ok =
               Cache.put({:protected_query, store_id, actor}, [actor],
                 name: cache_name,
                 store_id: store_id
               )
    end
  end

  defp assert_consistent_single_quad(db) do
    contents = index_contents(db)
    assert Enum.all?(@indices, &(length(contents[&1]) == 1))
  end

  defp index_contents(db) do
    Map.new(@indices, fn index ->
      entries = ErlangAdapter.fold(db, index, <<>>, [], fn entry, acc -> [entry | acc] end)
      {index, Enum.sort(entries)}
    end)
  end

  defp open_store(label, opts \\ []) do
    path = "/tmp/triple_store_remediation_#{label}_#{System.unique_integer([:positive])}"
    {:ok, db} = ErlangAdapter.open(path, Keyword.merge([schema: :quad], opts))
    {:ok, manager} = Manager.start_link(db: db)
    {:ok, store_id} = ErlangAdapter.instance_id(db)

    %{
      path: path,
      db: db,
      manager: manager,
      store_id: store_id,
      ctx: %{db: db, dict_manager: manager}
    }
  end

  defp cleanup(stores, caches) do
    Enum.each(caches, fn cache -> if Process.alive?(cache), do: GenServer.stop(cache) end)

    Enum.each(stores, fn store ->
      if Process.alive?(store.manager), do: Manager.stop(store.manager)
      ErlangAdapter.close(store.db)
      File.rm_rf(store.path)
    end)
  end

  defp unique_name(label),
    do: String.to_atom("remediation_#{label}_#{System.unique_integer([:positive])}")
end
