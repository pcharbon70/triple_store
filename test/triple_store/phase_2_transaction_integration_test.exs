defmodule TripleStore.Phase2TransactionIntegrationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager, as: DictManager
  alias TripleStore.Query.Cache
  alias TripleStore.QuadOperations
  alias TripleStore.SPARQL.Parser
  alias TripleStore.SPARQL.PlanCache
  alias TripleStore.SPARQL.UpdateExecutor

  @moduletag :scn_008

  test "a failed final batch preserves explicit indices and the result-cache generation" do
    path = unique_path("failed_commit")

    {:ok, db} = ErlangAdapter.open(path, mixed_batch_failure: :injected)
    {:ok, manager} = DictManager.start_link(db: db)
    {cache, cache_name} = start_cache()
    {:ok, store_id} = ErlangAdapter.instance_id(db)

    on_exit(fn ->
      stop_if_alive(cache)
      stop_if_alive(manager)
      ErlangAdapter.close(db)
      File.rm_rf!(path)
    end)

    assert :ok = Cache.put(:failure_probe, [:cached], name: cache_name, store_id: store_id)
    generations_before = :sys.get_state(cache).store_generations
    attach_commit_stop(db)

    assert {:ok, ast} =
             Parser.parse_update(
               "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }"
             )

    assert {:error, {:storage, :injected}} =
             UpdateExecutor.execute(%{db: db, dict_manager: manager}, ast)

    for index <- [:spo, :pos, :osp] do
      assert [] == keys(db, index)
    end

    assert {:ok, [:cached]} =
             Cache.get(:failure_probe, name: cache_name, store_id: store_id)

    assert generations_before == :sys.get_state(cache).store_generations

    assert_receive {:commit_stopped, %{status: :error}, %{mutation_count: 3}}, 1_000
  end

  test "concurrent public updates use the store coordinator in deterministic order" do
    path = unique_path("public_update_order")
    {:ok, store} = TripleStore.open(path)
    counter = :atomics.new(1, [])
    attach_first_commit_barrier(store.db, counter)

    on_exit(fn ->
      safe_close(store)
      File.rm_rf!(path)
    end)

    insert =
      Task.async(fn ->
        TripleStore.update(
          store,
          "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }"
        )
      end)

    assert_receive {:first_commit_waiting, coordinator}, 2_000
    assert coordinator == store.transaction

    delete =
      Task.async(fn ->
        TripleStore.update(
          store,
          "DELETE DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }"
        )
      end)

    assert wait_for_queued_call(store.transaction)
    send(coordinator, :release_commit)

    assert {:ok, 1} = Task.await(insert, 2_000)
    assert {:ok, 1} = Task.await(delete, 2_000)

    for index <- [:spo, :pos, :osp] do
      assert [] == keys(store.db, index)
    end
  end

  test "a direct read sees no staged state and reopened triple indices stay coherent" do
    path = unique_path("triple_visibility")
    {:ok, store} = TripleStore.open(path)
    {cache, cache_name} = start_cache()
    {:ok, store_id} = ErlangAdapter.instance_id(store.db)
    counter = :atomics.new(1, [])
    attach_first_commit_barrier(store.db, counter)

    on_exit(fn ->
      stop_if_alive(cache)
      safe_close(store)
      File.rm_rf!(path)
    end)

    assert :ok = Cache.put(:triple_probe, [:stale], name: cache_name, store_id: store_id)
    assert :ok = PlanCache.invalidate()
    assert :ok = PlanCache.put(:phase_2_probe, :stale_plan)
    assert 1 == PlanCache.stats().size

    update =
      Task.async(fn ->
        TripleStore.update(store, """
        INSERT DATA { <http://example.org/s1> <http://example.org/p> <http://example.org/o1> } ;
        INSERT DATA { <http://example.org/s2> <http://example.org/p> <http://example.org/o2> }
        """)
      end)

    assert_receive {:first_commit_waiting, coordinator}, 2_000

    assert {:ok, []} =
             TripleStore.query(
               store,
               "SELECT ?s WHERE { ?s <http://example.org/p> ?o } ORDER BY ?s"
             )

    for index <- [:spo, :pos, :osp] do
      assert [] == keys(store.db, index)
    end

    send(coordinator, :release_commit)
    assert {:ok, 2} = Task.await(update, 2_000)
    assert :miss = Cache.get(:triple_probe, name: cache_name, store_id: store_id)
    assert 1 == Map.fetch!(:sys.get_state(cache).store_generations, store_id)
    assert 0 == PlanCache.stats().size

    for index <- [:spo, :pos, :osp] do
      assert 2 == length(keys(store.db, index))
    end

    assert :ok = TripleStore.close(store)
    {:ok, reopened} = TripleStore.open(path)
    on_exit(fn -> safe_close(reopened) end)

    for index <- [:spo, :pos, :osp] do
      assert 2 == length(keys(reopened.db, index))
    end
  end

  test "reopened quad indices and named-graph metadata reflect one committed request" do
    path = unique_path("quad_reopen")
    {:ok, store} = TripleStore.open(path, schema: :quad)
    {cache, cache_name} = start_cache()
    {:ok, store_id} = ErlangAdapter.instance_id(store.db)

    on_exit(fn ->
      stop_if_alive(cache)
      safe_close(store)
      File.rm_rf!(path)
    end)

    assert :ok = Cache.put(:quad_probe, [:stale], name: cache_name, store_id: store_id)

    assert {:ok, 2} =
             TripleStore.update(store, """
             INSERT DATA {
               GRAPH <http://example.org/source> {
                 <http://example.org/s> <http://example.org/p> <http://example.org/o>
               }
             } ;
             COPY GRAPH <http://example.org/source> TO GRAPH <http://example.org/target>
             """)

    assert :miss = Cache.get(:quad_probe, name: cache_name, store_id: store_id)
    assert 1 == Map.fetch!(:sys.get_state(cache).store_generations, store_id)

    assert :ok = TripleStore.close(store)
    {:ok, reopened} = TripleStore.open(path, schema: :quad)
    on_exit(fn -> safe_close(reopened) end)

    for index <- [:gspo, :gpos, :spog, :posg] do
      assert 2 == length(keys(reopened.db, index))
    end

    assert {:ok, graphs} = QuadOperations.list_graphs(reopened.db)

    assert MapSet.new(graphs) ==
             MapSet.new([
               RDF.iri("http://example.org/source"),
               RDF.iri("http://example.org/target")
             ])
  end

  defp unique_path(suffix) do
    Path.join(
      System.tmp_dir!(),
      "phase_2_#{suffix}_#{System.unique_integer([:positive])}"
    )
  end

  defp start_cache do
    name = String.to_atom("phase_2_cache_#{System.unique_integer([:positive])}")
    {:ok, cache} = Cache.start_link(name: name)
    {cache, name}
  end

  defp attach_commit_stop(db) do
    handler_id = "phase-2-commit-stop-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :update, :commit, :stop],
        &__MODULE__.forward_commit_stop/4,
        %{db: db, test_pid: self()}
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp attach_first_commit_barrier(db, counter) do
    handler_id = "phase-2-commit-barrier-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :update, :commit, :start],
        &__MODULE__.hold_first_commit/4,
        %{counter: counter, db: db, test_pid: self()}
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp keys(db, index) do
    ErlangAdapter.fold_keys(db, index, <<>>, [], fn key, acc -> [key | acc] end)
  end

  defp wait_for_queued_call(transaction, attempts \\ 1_000)
  defp wait_for_queued_call(_transaction, 0), do: false

  defp wait_for_queued_call(transaction, attempts) do
    case Process.info(transaction, :message_queue_len) do
      {:message_queue_len, length} when length > 0 ->
        true

      _ ->
        :erlang.yield()
        wait_for_queued_call(transaction, attempts - 1)
    end
  end

  defp safe_close(store) do
    TripleStore.close(store)
  catch
    :exit, _ -> :ok
  end

  defp stop_if_alive(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  @doc false
  def forward_commit_stop(_event, measurements, %{db: event_db} = metadata, config) do
    if event_db == config.db do
      send(config.test_pid, {:commit_stopped, metadata, measurements})
    end
  end

  @doc false
  def hold_first_commit(_event, _measurements, %{db: event_db}, config) do
    if event_db == config.db and :atomics.add_get(config.counter, 1, 1) == 1 do
      send(config.test_pid, {:first_commit_waiting, self()})

      receive do
        :release_commit -> :ok
      end
    end
  end
end
