defmodule TripleStore.TransactionSerializationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager, as: DictManager
  alias TripleStore.Transaction

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "transaction_serialization_#{System.unique_integer([:positive])}"
      )

    {:ok, db} = ErlangAdapter.open(path)
    {:ok, manager} = DictManager.start_link(db: db)
    {:ok, transaction} = Transaction.start_link(db: db, dict_manager: manager)

    on_exit(fn ->
      Transaction.stop(transaction)
      if Process.alive?(manager), do: DictManager.stop(manager)
      ErlangAdapter.close(db)
      File.rm_rf!(path)
    end)

    %{db: db, transaction: transaction}
  end

  test "a query waits behind an in-progress commit and observes the committed request", %{
    db: db,
    transaction: transaction
  } do
    test_pid = self()
    handler_id = "transaction-commit-barrier-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:triple_store, :sparql, :update, :commit, :start],
        &__MODULE__.handle_commit_barrier/4,
        %{db: db, test_pid: test_pid}
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    update_task =
      Task.async(fn ->
        Transaction.update(
          transaction,
          "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }"
        )
      end)

    assert_receive {:commit_waiting, coordinator}, 2_000
    assert coordinator == transaction

    query_task =
      Task.async(fn ->
        send(test_pid, :query_calling)
        Transaction.query(transaction, "SELECT ?s WHERE { ?s <http://example.org/p> ?o }")
      end)

    assert_receive :query_calling, 1_000
    assert wait_for_queued_call(transaction)
    query_ref = query_task.ref
    refute_receive {^query_ref, _result}, 0

    send(coordinator, :release_commit)

    assert {:ok, 1} = Task.await(update_task, 2_000)

    assert {:ok, [%{"s" => {:named_node, "http://example.org/s"}}]} =
             Task.await(query_task, 2_000)
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

  @doc false
  def handle_commit_barrier(_event, _measurements, %{db: event_db}, config) do
    if event_db == config.db do
      send(config.test_pid, {:commit_waiting, self()})

      receive do
        :release_commit -> :ok
      end
    end
  end
end
