defmodule TripleStore.Test.DbPool do
  @moduledoc """
  Pool of pre-initialized RocksDB databases for tests.
  """

  use GenServer
  require Logger

  alias TripleStore.Backend.RocksDB.ErlangAdapter
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

    try do
      databases =
        for id <- 1..pool_size do
          path = TestHelpers.test_db_path("pool_#{id}")
          {:ok, db} = ErlangAdapter.open(path)
          %{db: db, path: path, id: id}
        end

      state = %{
        available: databases,
        in_use: %{},
        waiters: :queue.new()
      }

      {:ok, state}
    rescue
      e ->
        # If ErlangAdapter is not ready, skip the DbPool
        # Some tests will be skipped during migration phases
        {:stop, {:adapter_not_ready, e}}
    end
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
    new_in_use = Map.delete(state.in_use, db_info.id)

    # Check if database is still alive - if not, replace it with a fresh one
    {db_to_return, state} =
      if Process.alive?(db_info.db) do
        clear_database(db_info.db)
        {db_info, state}
      else
        # Database died during test - close it (safely) and create a replacement
        try do
          ErlangAdapter.close(db_info.db)
        rescue
          _ -> :ok
        end

        # Remove old database files (including lock file)
        File.rm_rf(db_info.path)

        # Create new database with retry logic
        case create_database_with_retry(db_info.path) do
          {:ok, new_db} ->
            new_db_info = %{db_info | db: new_db}
            {new_db_info, state}

          {:error, reason} ->
            # If we still can't open after retries, log and remove from pool
            require Logger
            Logger.warning("Failed to recreate database at #{db_info.path}: #{inspect(reason)}")

            # Don't return this database to the pool - shrink the pool
          {nil, %{state | available: []}}
        end
      end

    case :queue.out(state.waiters) do
      {{:value, waiter}, new_waiters} ->
        # If we don't have a valid database, tell the waiter to try again later
        if db_to_return == nil do
          # Re-queue the waiter at the front
          {:noreply, %{state | waiters: :queue.in(waiter, new_waiters)}}
        else
          GenServer.reply(waiter, db_to_return)

          new_state = %{
            state
            | in_use: Map.put(new_in_use, db_to_return.id, db_to_return),
              waiters: new_waiters
          }

          {:noreply, new_state}
        end

      {:empty, _} ->
        if db_to_return == nil do
          # No valid database to return, just update state
          {:noreply, state}
        else
          {:noreply, %{state | available: [db_to_return | state.available], in_use: new_in_use}}
        end
    end
  end

  @impl true
  def terminate(_reason, state) do
    state.available
    |> Enum.concat(Map.values(state.in_use))
    |> Enum.each(fn db_info ->
      ErlangAdapter.close(db_info.db)
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

  defp create_database_with_retry(path, retries \\ 3, delay \\ 100) do
    create_database_with_retry(path, retries, delay, nil)
  end

  defp create_database_with_retry(_path, retries, _delay, last_error) when retries <= 0 do
    {:error, last_error}
  end

  defp create_database_with_retry(path, retries, delay, _last_error) do
    case ErlangAdapter.open(path) do
      {:ok, db} ->
        {:ok, db}

      {:error, reason} = error ->
        # If there's a lock error, wait and retry
        if String.contains?(inspect(reason), "lock") or
           String.contains?(inspect(reason), "LOCK") do
          Process.sleep(delay)
          create_database_with_retry(path, retries - 1, delay, error)
        else
          error
        end
    end
  end

  defp clear_database(db) do
    # Check if database is still alive before attempting to clear
    if Process.alive?(db) do
      Enum.each(@column_families, fn cf ->
        clear_column_family(db, cf)
      end)
    else
      :ok
    end
  end

  defp clear_column_family(db, cf) do
    # Only attempt to clear if database is still alive
    if Process.alive?(db) do
      case ErlangAdapter.prefix_iterator(db, cf, <<>>) do
        {:ok, iter} ->
          try do
            delete_all_keys(db, cf, iter)
          after
            # Only close iterator if the database is still alive
            if Process.alive?(db) do
              ErlangAdapter.iterator_close(iter)
            end
          end

        {:error, _} ->
          :ok
      end
    else
      :ok
    end
  end

  defp delete_all_keys(db, cf, iter) do
    # Stop if database dies during iteration
    if Process.alive?(db) do
      case ErlangAdapter.iterator_next(iter) do
        {:ok, key, _value} ->
          _ = ErlangAdapter.delete(db, cf, key)
          delete_all_keys(db, cf, iter)

        :iterator_end ->
          :ok

        {:error, _} ->
          :ok
      end
    else
      :ok
    end
  end
end
