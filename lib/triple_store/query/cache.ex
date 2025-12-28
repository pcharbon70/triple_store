defmodule TripleStore.Query.Cache do
  @moduledoc """
  Cache for SPARQL query results.

  This module implements a GenServer-based cache that stores query results
  for frequently executed queries. The cache uses ETS for fast lookups and
  implements an LRU (Least Recently Used) eviction policy to manage memory.

  ## Purpose

  Query execution can be expensive, especially for complex queries with
  joins and aggregations. The result cache stores previously computed
  results so that repeated queries return immediately without re-execution.

  ## Cache Key Strategy

  Results are keyed by a hash of:
  1. The SPARQL query string (or normalized algebra)
  2. Any bound variables or parameters

  This ensures that identical queries with different parameter values
  get separate cache entries.

  ## Size Limiting

  Large result sets are not cached by default to prevent memory exhaustion:

  - `:max_result_size` - Maximum number of rows in a cacheable result (default: 10,000)
  - Results exceeding this limit are returned but not cached

  ## Invalidation

  The cache supports multiple invalidation strategies:

  1. **Full invalidation**: Clear entire cache after updates
  2. **Predicate-based invalidation**: Clear queries touching specific predicates (Task 5.3.2)
  3. **TTL expiration**: Entries expire after a configurable time

  ## LRU Eviction

  When the cache exceeds its maximum size, the least recently used entries
  are evicted. Access time is tracked on each lookup.

  ## Usage

      # Start the cache (typically via supervision tree)
      {:ok, _pid} = Query.Cache.start_link(max_entries: 1000)

      # Get or execute a query
      result = Query.Cache.get_or_execute(query, fn ->
        TripleStore.SPARQL.Query.execute(db, query)
      end)

      # Invalidate after data change
      Query.Cache.invalidate()

      # Get cache statistics
      stats = Query.Cache.stats()
      # => %{size: 42, hits: 1000, misses: 50, hit_rate: 0.95}

  ## Configuration

  - `:max_entries` - Maximum number of cached results (default: 1000)
  - `:max_result_size` - Maximum rows in a cacheable result (default: 10,000)
  - `:ttl_ms` - Time-to-live in milliseconds (default: nil, no expiration)
  - `:name` - Process name (default: `TripleStore.Query.Cache`)

  """

  use GenServer

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Cache key (query hash)"
  @type cache_key :: binary()

  @typedoc "Cached result entry"
  @type cache_entry :: %{
          result: term(),
          result_size: non_neg_integer(),
          access_time: integer(),
          created_at: integer(),
          predicates: MapSet.t()
        }

  @typedoc "Cache statistics"
  @type cache_stats :: %{
          size: non_neg_integer(),
          hits: non_neg_integer(),
          misses: non_neg_integer(),
          hit_rate: float(),
          evictions: non_neg_integer(),
          skipped_large: non_neg_integer(),
          expired: non_neg_integer()
        }

  @typedoc "Cache options"
  @type cache_opts :: [
          max_entries: pos_integer(),
          max_result_size: pos_integer(),
          ttl_ms: pos_integer() | nil,
          name: atom()
        ]

  # ===========================================================================
  # Constants
  # ===========================================================================

  @default_max_entries 1000
  @default_max_result_size 10_000
  @default_ttl_ms nil
  @default_name __MODULE__

  # ETS table names (atoms derived from process name)
  @results_table_suffix :_results
  @lru_table_suffix :_lru

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the query cache GenServer.

  ## Options

  - `:max_entries` - Maximum number of cached results (default: 1000)
  - `:max_result_size` - Maximum rows in a cacheable result (default: 10,000)
  - `:ttl_ms` - Time-to-live in milliseconds (default: nil, no expiration)
  - `:name` - Process name (default: `TripleStore.Query.Cache`)

  ## Examples

      {:ok, pid} = Query.Cache.start_link(max_entries: 500, max_result_size: 5000)

  """
  @spec start_link(cache_opts()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Gets a cached result or executes the query and caches it.

  If the result for the given query is in the cache (and not expired),
  returns it immediately. Otherwise, calls the execute function,
  caches the result if it's not too large, and returns it.

  ## Arguments

  - `query` - The query to look up (string or algebra)
  - `execute_fn` - Zero-arity function that executes the query if not cached
  - `opts` - Options:
    - `:name` - Cache process name
    - `:predicates` - Set of predicates accessed by this query (for invalidation)

  ## Returns

  - `{:ok, result}` - The cached or computed result
  - `{:error, reason}` - If execution failed

  ## Examples

      {:ok, result} = Query.Cache.get_or_execute(query, fn ->
        TripleStore.SPARQL.Query.execute(db, query)
      end)

  """
  @spec get_or_execute(term(), (-> {:ok, term()} | {:error, term()}), keyword()) ::
          {:ok, term()} | {:error, term()}
  def get_or_execute(query, execute_fn, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    predicates = Keyword.get(opts, :predicates, MapSet.new())
    key = compute_key(query)

    case GenServer.call(name, {:get, key}) do
      {:hit, result} ->
        emit_cache_hit()
        {:ok, result}

      :miss ->
        emit_cache_miss()

        case execute_fn.() do
          {:ok, result} = success ->
            GenServer.cast(name, {:put, key, result, predicates})
            success

          {:error, _} = error ->
            error
        end

      :expired ->
        emit_cache_expired()

        case execute_fn.() do
          {:ok, result} = success ->
            GenServer.cast(name, {:put, key, result, predicates})
            success

          {:error, _} = error ->
            error
        end
    end
  end

  @doc """
  Gets a cached result without executing.

  ## Arguments

  - `query` - The query to look up
  - `opts` - Options including `:name` for the cache process

  ## Returns

  - `{:ok, result}` if cached and not expired
  - `:miss` if not cached
  - `:expired` if cached but expired

  """
  @spec get(term(), keyword()) :: {:ok, term()} | :miss | :expired
  def get(query, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    key = compute_key(query)

    case GenServer.call(name, {:get, key}) do
      {:hit, result} -> {:ok, result}
      :miss -> :miss
      :expired -> :expired
    end
  end

  @doc """
  Stores a result in the cache.

  The result will be cached only if its size is within the configured limit.

  ## Arguments

  - `query` - The query
  - `result` - The query result
  - `opts` - Options:
    - `:name` - Cache process name
    - `:predicates` - Set of predicates accessed by this query

  ## Returns

  - `:ok` - Result was cached
  - `:skipped` - Result was too large to cache

  """
  @spec put(term(), term(), keyword()) :: :ok | :skipped
  def put(query, result, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    predicates = Keyword.get(opts, :predicates, MapSet.new())
    key = compute_key(query)
    GenServer.call(name, {:put_sync, key, result, predicates})
  end

  @doc """
  Invalidates the entire cache.

  Call this after bulk data loads or schema changes.

  ## Examples

      Query.Cache.invalidate()

  """
  @spec invalidate(keyword()) :: :ok
  def invalidate(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :invalidate)
  end

  @doc """
  Invalidates cached queries that access specific predicates.

  This is the primary invalidation mechanism for incremental updates.
  After modifying data with a specific predicate, call this to invalidate
  all cached queries that read from that predicate.

  ## Arguments

  - `predicates` - Set or list of predicates that were modified
  - `opts` - Options including `:name` for the cache process

  ## Examples

      # After updating foaf:name data
      Query.Cache.invalidate_predicates([RDF.iri("http://xmlns.com/foaf/0.1/name")])

  """
  @spec invalidate_predicates(Enumerable.t(), keyword()) :: :ok
  def invalidate_predicates(predicates, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    predicate_set = MapSet.new(predicates)
    GenServer.call(name, {:invalidate_predicates, predicate_set})
  end

  @doc """
  Invalidates a specific cached query.

  ## Arguments

  - `query` - The query to invalidate

  """
  @spec invalidate_query(term(), keyword()) :: :ok
  def invalidate_query(query, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    key = compute_key(query)
    GenServer.call(name, {:invalidate_key, key})
  end

  @doc """
  Returns cache statistics.

  ## Returns

  Map with:
  - `:size` - Current number of cached results
  - `:hits` - Total cache hits
  - `:misses` - Total cache misses
  - `:hit_rate` - Hit rate (0.0 to 1.0)
  - `:evictions` - Total evictions performed
  - `:skipped_large` - Results skipped due to size
  - `:expired` - Results that expired

  """
  @spec stats(keyword()) :: cache_stats()
  def stats(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :stats)
  end

  @doc """
  Returns the current cache size.
  """
  @spec size(keyword()) :: non_neg_integer()
  def size(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :size)
  end

  @doc """
  Returns the cache configuration.
  """
  @spec config(keyword()) :: map()
  def config(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :config)
  end

  @doc """
  Clears expired entries from the cache.

  This is called automatically on reads, but can be called manually
  to proactively clean up expired entries.
  """
  @spec cleanup_expired(keyword()) :: non_neg_integer()
  def cleanup_expired(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :cleanup_expired)
  end

  # ===========================================================================
  # Key Computation
  # ===========================================================================

  @doc """
  Computes a cache key from a query.

  The key is computed by hashing the query structure. This works for
  both SPARQL strings and algebra representations.
  """
  @spec compute_key(term()) :: cache_key()
  def compute_key(query) when is_binary(query) do
    :crypto.hash(:sha256, query)
  end

  def compute_key(query) do
    :crypto.hash(:sha256, :erlang.term_to_binary(query))
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(opts) do
    max_entries = Keyword.get(opts, :max_entries, @default_max_entries)
    max_result_size = Keyword.get(opts, :max_result_size, @default_max_result_size)
    ttl_ms = Keyword.get(opts, :ttl_ms, @default_ttl_ms)
    name = Keyword.get(opts, :name, @default_name)

    # Create ETS tables
    results_table = table_name(name, @results_table_suffix)
    lru_table = table_name(name, @lru_table_suffix)

    :ets.new(results_table, [:set, :named_table, :public, read_concurrency: true])
    :ets.new(lru_table, [:ordered_set, :named_table, :public])

    state = %{
      results_table: results_table,
      lru_table: lru_table,
      max_entries: max_entries,
      max_result_size: max_result_size,
      ttl_ms: ttl_ms,
      hits: 0,
      misses: 0,
      evictions: 0,
      skipped_large: 0,
      expired: 0
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case :ets.lookup(state.results_table, key) do
      [{^key, entry}] ->
        if expired?(entry, state.ttl_ms) do
          # Remove expired entry
          remove_entry(state, key, entry)
          {:reply, :expired, %{state | expired: state.expired + 1}}
        else
          # Update access time
          now = System.monotonic_time(:millisecond)
          old_time = entry.access_time
          new_entry = %{entry | access_time: now}
          :ets.insert(state.results_table, {key, new_entry})

          # Update LRU ordering
          :ets.delete(state.lru_table, {old_time, key})
          :ets.insert(state.lru_table, {{now, key}, true})

          {:reply, {:hit, entry.result}, %{state | hits: state.hits + 1}}
        end

      [] ->
        {:reply, :miss, %{state | misses: state.misses + 1}}
    end
  end

  @impl true
  def handle_call({:put_sync, key, result, predicates}, _from, state) do
    result_size = compute_result_size(result)

    if result_size > state.max_result_size do
      {:reply, :skipped, %{state | skipped_large: state.skipped_large + 1}}
    else
      state = do_put(state, key, result, result_size, predicates)
      {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call(:invalidate, _from, state) do
    :ets.delete_all_objects(state.results_table)
    :ets.delete_all_objects(state.lru_table)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:invalidate_key, key}, _from, state) do
    case :ets.lookup(state.results_table, key) do
      [{^key, entry}] ->
        remove_entry(state, key, entry)

      [] ->
        :ok
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:invalidate_predicates, predicate_set}, _from, state) do
    # Find all entries with overlapping predicates
    to_invalidate =
      :ets.foldl(
        fn {key, entry}, acc ->
          if MapSet.size(MapSet.intersection(entry.predicates, predicate_set)) > 0 do
            [{key, entry} | acc]
          else
            acc
          end
        end,
        [],
        state.results_table
      )

    # Remove matching entries
    Enum.each(to_invalidate, fn {key, entry} ->
      remove_entry(state, key, entry)
    end)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    size = :ets.info(state.results_table, :size)
    total = state.hits + state.misses

    hit_rate =
      if total > 0 do
        state.hits / total
      else
        0.0
      end

    stats = %{
      size: size,
      hits: state.hits,
      misses: state.misses,
      hit_rate: hit_rate,
      evictions: state.evictions,
      skipped_large: state.skipped_large,
      expired: state.expired
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_call(:size, _from, state) do
    size = :ets.info(state.results_table, :size)
    {:reply, size, state}
  end

  @impl true
  def handle_call(:config, _from, state) do
    config = %{
      max_entries: state.max_entries,
      max_result_size: state.max_result_size,
      ttl_ms: state.ttl_ms
    }

    {:reply, config, state}
  end

  @impl true
  def handle_call(:cleanup_expired, _from, state) do
    if state.ttl_ms do
      count = do_cleanup_expired(state)
      {:reply, count, %{state | expired: state.expired + count}}
    else
      {:reply, 0, state}
    end
  end

  @impl true
  def handle_cast({:put, key, result, predicates}, state) do
    result_size = compute_result_size(result)

    if result_size > state.max_result_size do
      {:noreply, %{state | skipped_large: state.skipped_large + 1}}
    else
      state = do_put(state, key, result, result_size, predicates)
      {:noreply, state}
    end
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp table_name(process_name, suffix) when is_atom(process_name) do
    String.to_atom("#{process_name}#{suffix}")
  end

  defp do_put(state, key, result, result_size, predicates) do
    now = System.monotonic_time(:millisecond)

    entry = %{
      result: result,
      result_size: result_size,
      access_time: now,
      created_at: now,
      predicates: MapSet.new(predicates)
    }

    # Check if key already exists
    case :ets.lookup(state.results_table, key) do
      [{^key, old_entry}] ->
        # Update existing entry
        :ets.delete(state.lru_table, {old_entry.access_time, key})

      [] ->
        :ok
    end

    # Insert new entry
    :ets.insert(state.results_table, {key, entry})
    :ets.insert(state.lru_table, {{now, key}, true})

    # Evict if necessary
    maybe_evict(state)
  end

  defp maybe_evict(state) do
    current_size = :ets.info(state.results_table, :size)

    if current_size > state.max_entries do
      evict_oldest(state)
    else
      state
    end
  end

  defp evict_oldest(state) do
    # Get oldest entry from LRU table
    case :ets.first(state.lru_table) do
      :"$end_of_table" ->
        state

      {_time, key} = lru_key ->
        :ets.delete(state.lru_table, lru_key)
        :ets.delete(state.results_table, key)
        %{state | evictions: state.evictions + 1}
    end
  end

  defp remove_entry(state, key, entry) do
    :ets.delete(state.results_table, key)
    :ets.delete(state.lru_table, {entry.access_time, key})
  end

  defp expired?(_entry, nil), do: false

  defp expired?(entry, ttl_ms) do
    now = System.monotonic_time(:millisecond)
    now - entry.created_at > ttl_ms
  end

  defp compute_result_size(result) when is_list(result), do: length(result)
  defp compute_result_size(%{bindings: bindings}) when is_list(bindings), do: length(bindings)
  defp compute_result_size(_), do: 1

  defp do_cleanup_expired(state) do
    now = System.monotonic_time(:millisecond)
    ttl_ms = state.ttl_ms

    expired_entries =
      :ets.foldl(
        fn {key, entry}, acc ->
          if now - entry.created_at > ttl_ms do
            [{key, entry} | acc]
          else
            acc
          end
        end,
        [],
        state.results_table
      )

    Enum.each(expired_entries, fn {key, entry} ->
      remove_entry(state, key, entry)
    end)

    length(expired_entries)
  end

  # ===========================================================================
  # Telemetry
  # ===========================================================================

  defp emit_cache_hit do
    :telemetry.execute(
      [:triple_store, :cache, :query, :hit],
      %{count: 1},
      %{}
    )
  end

  defp emit_cache_miss do
    :telemetry.execute(
      [:triple_store, :cache, :query, :miss],
      %{count: 1},
      %{}
    )
  end

  defp emit_cache_expired do
    :telemetry.execute(
      [:triple_store, :cache, :query, :expired],
      %{count: 1},
      %{}
    )
  end
end
