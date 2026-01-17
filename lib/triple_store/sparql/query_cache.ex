defmodule TripleStore.SPARQL.QueryCache do
  @moduledoc """
  Query result caching for SPARQL queries (S7).

  Caches query results based on:
  - Query hash (MD5 of query string)
  - Database state version (for invalidation)
  - Optional TTL for time-based expiration

  Uses ETS for fast in-memory caching with:
  - Size limits (LRU eviction)
  - Time-based expiration
  - Selective invalidation
  """

  use GenServer
  require Logger

  @type cache_key :: {binary(), non_neg_integer()}
  @type cache_entry :: %{
    result: term(),
    inserted_at: integer(),
    access_count: non_neg_integer(),
    last_accessed: integer(),
    size_bytes: non_neg_integer()
  }

  @table_name :triple_store_query_cache
  @max_cache_size 1000
  @max_cache_bytes 100_000_000  # 100MB
  @default_ttl 300_000  # 5 minutes

  # Client API

  @doc """
  Start the query cache GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Get a cached result for a query.
  """
  @spec get(binary(), non_neg_integer()) :: {:ok, term()} | :miss
  def get(query, db_version \\ 0) do
    key = cache_key(query, db_version)

    case :ets.lookup(@table_name, key) do
      [{^key, entry}] ->
        # Check TTL
        if entry_valid?(entry) do
          # Update access stats
          new_entry = %{entry |
            access_count: entry.access_count + 1,
            last_accessed: System.monotonic_time(:millisecond)
          }
          :ets.insert(@table_name, {key, new_entry})
          {:ok, entry.result}
        else
          # Expired, remove it
          :ets.delete(@table_name, key)
          :miss
        end

      [] ->
        :miss
    end
  end

  @doc """
  Put a result in the cache.
  """
  @spec put(binary(), non_neg_integer(), term(), keyword()) :: :ok
  def put(query, db_version, result, opts \\ []) do
    GenServer.call(__MODULE__, {:put, query, db_version, result, opts})
  end

  @doc """
  Invalidate all cache entries for a specific database version.
  """
  @spec invalidate_version(non_neg_integer()) :: :ok
  def invalidate_version(db_version) do
    GenServer.call(__MODULE__, {:invalidate_version, db_version})
  end

  @doc """
  Invalidate all cache entries.
  """
  @spec invalidate_all() :: :ok
  def invalidate_all do
    GenServer.call(__MODULE__, :invalidate_all)
  end

  @doc """
  Get cache statistics.
  """
  @spec stats() :: map()
  def stats do
    GenServer.call(__MODULE__, :stats)
  end

  @doc """
  Check if caching is enabled for a query type.

  Queries that modify data are never cached.
  """
  @spec cacheable?(String.t()) :: boolean()
  def cacheable?(query) when is_binary(query) do
    # Only cache SELECT and CONSTRUCT queries
    trimmed = String.trim(query)
    upcase = String.upcase(trimmed)

    String.starts_with?(upcase, "SELECT") or
      String.starts_with?(upcase, "CONSTRUCT") or
      String.starts_with?(upcase, "ASK") or
      String.starts_with?(upcase, "DESCRIBE")
  end

  def cacheable?(_), do: false

  # GenServer Callbacks

  @impl true
  def init(opts) do
    max_size = Keyword.get(opts, :max_size, @max_cache_size)
    max_bytes = Keyword.get(opts, :max_bytes, @max_cache_bytes)

    table = :ets.new(@table_name, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    # Start periodic cleanup
    Process.send_after(self(), :cleanup, 60_000)

    {:ok, %{
      table: table,
      max_size: max_size,
      max_bytes: max_bytes,
      hit_count: 0,
      miss_count: 0,
      eviction_count: 0
    }}
  end

  @impl true
  def handle_call({:put, query, db_version, result, opts}, _from, state) do
    ttl = Keyword.get(opts, :ttl, @default_ttl)
    key = cache_key(query, db_version)

    entry = %{
      result: result,
      inserted_at: System.monotonic_time(:millisecond),
      access_count: 0,
      last_accessed: System.monotonic_time(:millisecond),
      size_bytes: estimate_size(result),
      ttl: ttl
    }

    # Check if we need to evict before inserting
    state = maybe_evict(state, entry.size_bytes)

    :ets.insert(@table_name, {key, entry})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:invalidate_version, db_version}, _from, state) do
    # Remove all entries for this version by scanning all entries
    entries = :ets.tab2list(@table_name)

    Enum.each(entries, fn
      {{_hash, ^db_version}, _entry} ->
        :ets.delete(@table_name, {_hash, db_version})

      _ ->
        :ok
    end)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:invalidate_all, _from, state) do
    :ets.delete_all_objects(@table_name)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    entries_count = :ets.info(@table_name, :size)

    total_bytes = :ets.foldl(fn {_key, entry}, acc ->
      acc + entry.size_bytes
    end, 0, @table_name)

    stats = %{
      entries: entries_count,
      total_bytes: total_bytes,
      max_entries: state.max_size,
      max_bytes: state.max_bytes,
      hit_count: state.hit_count,
      miss_count: state.miss_count,
      eviction_count: state.eviction_count,
      hit_rate: calculate_hit_rate(state.hit_count, state.miss_count)
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    # Remove expired entries
    now = System.monotonic_time(:millisecond)

    pattern = {{:"$1", :"$2"}, [], [:"$2"]}
    entries = :ets.select(@table_name, pattern)

    Enum.each(entries, fn entry ->
      if not entry_valid?(entry, now) do
        :ets.select_delete(@table_name, [{{:"$1", :"$2"}, [{:==, :"$2", entry}], [true]}])
      end
    end)

    # Schedule next cleanup
    Process.send_after(self(), :cleanup, 60_000)
    {:noreply, state}
  end

  # Private Helpers

  defp cache_key(query, db_version) do
    hash = :crypto.hash(:md5, query) |> Base.encode16(case: :lower)
    {hash, db_version}
  end

  defp entry_valid?(entry) do
    entry_valid?(entry, System.monotonic_time(:millisecond))
  end

  defp entry_valid?(entry, now) do
    ttl = Map.get(entry, :ttl, @default_ttl)
    now - entry.inserted_at < ttl
  end

  defp maybe_evict(state, new_entry_size) do
    current_size = :ets.info(@table_name, :size)

    cond do
      current_size >= state.max_size ->
        evict_lru(state, new_entry_size)

      true ->
        current_bytes = :ets.foldl(fn {_key, entry}, acc ->
          acc + entry.size_bytes
        end, 0, @table_name)

        if current_bytes + new_entry_size > state.max_bytes do
          evict_lru(state, new_entry_size)
        else
          state
        end
    end
  end

  defp evict_lru(state, _new_entry_size) do
    # Find least recently used entry using tab2list
    entries = :ets.tab2list(@table_name)

    if entries != [] do
      # Find entry with oldest last_accessed
      {key, _entry} = Enum.min_by(entries, fn {_key, entry} ->
        entry.last_accessed
      end)

      :ets.delete(@table_name, key)
      %{state | eviction_count: state.eviction_count + 1}
    else
      state
    end
  end

  defp estimate_size(term) when is_binary(term), do: byte_size(term)
  defp estimate_size(term) when is_integer(term), do: 8
  defp estimate_size(term) when is_float(term), do: 8
  defp estimate_size(term) when is_atom(term), do: 8
  defp estimate_size(term) when is_list(term) do
    Enum.reduce(term, 0, fn item, acc -> acc + estimate_size(item) end) + 16
  end
  defp estimate_size(term) when is_map(term) do
    Enum.reduce(term, 0, fn {k, v}, acc ->
      acc + estimate_size(k) + estimate_size(v)
    end) + 32
  end
  defp estimate_size(_), do: 8

  defp calculate_hit_rate(0, _), do: 0.0
  defp calculate_hit_rate(hits, misses) do
    total = hits + misses
    if total > 0, do: hits / total, else: 0.0
  end
end
