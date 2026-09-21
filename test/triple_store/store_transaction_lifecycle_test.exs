defmodule TripleStore.StoreTransactionLifecycleTest do
  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager, as: DictManager
  alias TripleStore.Transaction

  setup do
    path = Path.join(System.tmp_dir!(), "store_transaction_#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(path) end)
    %{path: path}
  end

  test "open starts one coordinator and close stops it before releasing the store", %{path: path} do
    assert {:ok, store} = TripleStore.open(path)
    assert store.transaction_owner == :store
    assert is_pid(store.transaction)
    assert Process.alive?(store.transaction)

    transaction = store.transaction
    assert :ok = TripleStore.close(store)
    refute Process.alive?(transaction)
    assert {:error, :already_closed} = TripleStore.close(store)
  end

  test "public updates reuse the coordinator carried by copied handles", %{path: path} do
    assert {:ok, store} = TripleStore.open(path)
    copied_store = Map.new(store)

    assert {:ok, 1} =
             TripleStore.update(
               copied_store,
               "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }"
             )

    assert copied_store.transaction == store.transaction
    assert Process.alive?(store.transaction)
    assert :ok = TripleStore.close(store)
  end

  test "an explicit external coordinator takes precedence and remains caller-owned", %{path: path} do
    assert {:ok, db} = ErlangAdapter.open(path)
    assert {:ok, manager} = DictManager.start_link(db: db)
    assert {:ok, transaction} = Transaction.start_link(db: db, dict_manager: manager)

    external_path = path <> "_external"

    assert {:ok, store} =
             TripleStore.open(external_path, transaction: {:external, transaction})

    assert store.transaction == transaction
    assert store.transaction_owner == :external
    assert :ok = TripleStore.close(store)
    assert Process.alive?(transaction)

    assert :ok = Transaction.stop(transaction)
    assert :ok = DictManager.stop(manager)
    assert :ok = ErlangAdapter.close(db)
    File.rm_rf!(external_path)
  end

  test "invalid transaction configuration rolls back opened resources", %{path: path} do
    assert {:error, :invalid_transaction_option} = TripleStore.open(path, transaction: :invalid)

    assert {:ok, store} = TripleStore.open(path)
    assert :ok = TripleStore.close(store)
  end

  test "a dead coordinator returns a tagged public update error", %{path: path} do
    assert {:ok, store} = TripleStore.open(path)
    assert :ok = Transaction.stop(store.transaction)

    assert {:error, {:transaction_unavailable, _reason}} =
             TripleStore.update(
               store,
               "INSERT DATA { <http://example.org/s> <http://example.org/p> <http://example.org/o> }"
             )

    assert :ok = TripleStore.close(store)
  end
end
