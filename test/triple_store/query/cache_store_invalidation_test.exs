defmodule TripleStore.Query.CacheStoreInvalidationTest do
  use ExUnit.Case, async: false

  alias TripleStore.Query.Cache

  setup do
    name_a = unique_name(:a)
    name_b = unique_name(:b)
    {:ok, cache_a} = Cache.start_link(name: name_a)
    {:ok, cache_b} = Cache.start_link(name: name_b)

    on_exit(fn ->
      stop_if_alive(cache_a)
      stop_if_alive(cache_b)
    end)

    %{cache_a: cache_a, cache_b: cache_b, name_a: name_a, name_b: name_b}
  end

  test "store invalidation reaches every named cache and preserves other stores", ctx do
    store_a = make_ref()
    store_b = make_ref()

    for name <- [ctx.name_a, ctx.name_b] do
      assert :ok = Cache.put(:a, [:stale], name: name, store_id: store_a)
      assert :ok = Cache.put(:b, [:current], name: name, store_id: store_b)
    end

    assert :ok = Cache.invalidate_store(store_a)

    for name <- [ctx.name_a, ctx.name_b] do
      assert :miss = Cache.get(:a, name: name, store_id: store_a)
      assert {:ok, [:current]} = Cache.get(:b, name: name, store_id: store_b)
    end
  end

  test "an in-flight fill cannot repopulate a store after invalidation", ctx do
    store_id = make_ref()
    parent = self()

    fill =
      Task.async(fn ->
        Cache.get_or_execute(
          :slow,
          fn ->
            send(parent, :fill_started)

            receive do
              :finish_fill -> {:ok, [:stale]}
            end
          end,
          name: ctx.name_a,
          store_id: store_id
        )
      end)

    assert_receive :fill_started
    assert :ok = Cache.invalidate_store(store_id)
    send(fill.pid, :finish_fill)
    assert {:ok, [:stale]} = Task.await(fill)

    # Drain the asynchronous put message before checking the entry.
    assert 0 == Cache.size(name: ctx.name_a)
    assert :miss = Cache.get(:slow, name: ctx.name_a, store_id: store_id)
  end

  test "a concurrently stopped cache does not make store invalidation fail", ctx do
    GenServer.stop(ctx.cache_b)
    assert :ok = Cache.invalidate_store(make_ref())
  end

  defp unique_name(suffix) do
    String.to_atom("store_cache_#{suffix}_#{System.unique_integer([:positive])}")
  end

  defp stop_if_alive(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  end
end
