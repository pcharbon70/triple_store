defmodule TripleStore.Test.DbPool do
  @moduledoc """
  Pool of pre-initialized RocksDB databases for tests.
  """

  use GenServer

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.TestHelpers

  @column_families [:id2str, :str2id, :spo, :pos, :osp, :derived]
  @default_pool_size System.schedulers_online()
  @checkout_timeout 30_000

  # Client API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def checkout(opts \\ []) do
    timeout = Keyword.get(opts, :timeout, @checkout_timeout)
    GenServer.call(__MODULE__, :checkout, timeout)
  end

  def checkin(db_info) do
    GenServer.cast(__MODULE__, {:checkin, db_info})
  end

  # Server callbacks

  @impl true
  def init(opts) do
    pool_size = Keyword.get(opts, :pool_size, pool_size_from_env(@default_pool_size))

    databases =
      for id <- 1..pool_size do
        path = TestHelpers.test_db_path("pool_#{id}")
        {:ok, db} = NIF.open(path)
        %{db: db, path: path, id: id}
      end

    state = %{
      available: databases,
      in_use: %{},
      waiters: :queue.new()
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:checkout, from, state) do
    case state.available do
      [db_info | rest] ->
        new_state = %{
          state
          | available: rest,
            in_use: Map.put(state.in_use, db_info.id, db_info)
        }

        {:reply, db_info, new_state}

      [] ->
        {:noreply, %{state | waiters: :queue.in(from, state.waiters)}}
    end
  end

  @impl true
  def handle_cast({:checkin, db_info}, state) do
    clear_database(db_info.db)
    new_in_use = Map.delete(state.in_use, db_info.id)

    case :queue.out(state.waiters) do
      {{:value, waiter}, new_waiters} ->
        GenServer.reply(waiter, db_info)

        new_state = %{
          state
          | in_use: Map.put(new_in_use, db_info.id, db_info),
            waiters: new_waiters
        }

        {:noreply, new_state}

      {:empty, _} ->
        {:noreply, %{state | available: [db_info | state.available], in_use: new_in_use}}
    end
  end

  @impl true
  def terminate(_reason, state) do
    state.available
    |> Enum.concat(Map.values(state.in_use))
    |> Enum.each(fn db_info ->
      NIF.close(db_info.db)
      File.rm_rf(db_info.path)
    end)

    :ok
  end

  # Internal helpers

  defp pool_size_from_env(default) do
    case System.get_env("TRIPLE_STORE_TEST_POOL_SIZE") do
      nil ->
        default

      value ->
        case Integer.parse(value) do
          {size, _} when size > 0 -> size
          _ -> default
        end
    end
  end

  defp clear_database(db) do
    Enum.each(@column_families, fn cf ->
      clear_column_family(db, cf)
    end)
  end

  defp clear_column_family(db, cf) do
    case NIF.prefix_iterator(db, cf, <<>>) do
      {:ok, iter} ->
        try do
          delete_all_keys(db, cf, iter)
        after
          NIF.iterator_close(iter)
        end

      {:error, _} ->
        :ok
    end
  end

  defp delete_all_keys(db, cf, iter) do
    case NIF.iterator_next(iter) do
      {:ok, key, _value} ->
        _ = NIF.delete(db, cf, key)
        delete_all_keys(db, cf, iter)

      :iterator_end ->
        :ok

      {:error, _} ->
        :ok
    end
  end
end
