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
          skipped_memory: non_neg_integer(),
          expired: non_neg_integer(),
          memory_bytes: non_neg_integer()
        }

  @typedoc "Cache options"
  @type cache_opts :: [
          max_entries: pos_integer(),
          max_result_size: pos_integer(),
          max_memory_bytes: pos_integer() | nil,
          ttl_ms: pos_integer() | nil,
          name: atom(),
          persistence_path: String.t() | nil,
          warm_on_start: boolean(),
          allowed_persistence_dir: String.t() | nil
        ]

  @typedoc "Persistable cache entry (for disk storage)"
  @type persistable_entry :: %{
          key: cache_key(),
          result: term(),
          result_size: non_neg_integer(),
          predicates: [term()]
        }

  # ===========================================================================
  # Constants
  # ===========================================================================

  @default_max_entries 1000
  @default_max_result_size 10_000
  @default_max_memory_bytes nil
  @default_ttl_ms nil
  @default_name __MODULE__

  # ETS table names (atoms derived from process name)
  @results_table_suffix :_results
  @lru_table_suffix :_lru
  @predicate_index_suffix :_pred_idx

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
  # Cache Warming API
  # ===========================================================================

  @doc """
  Persists the current cache contents to disk.

  Saves all cached entries to the specified file path. The cache can later
  be restored from this file using `warm_from_file/2` or by starting with
  `warm_on_start: true`.

  ## Arguments

  - `path` - File path to write the cache data
  - `opts` - Options including `:name` for the cache process

  ## Returns

  - `{:ok, count}` - Number of entries persisted
  - `{:error, reason}` - On failure

  ## Examples

      {:ok, 42} = Query.Cache.persist_to_file("/var/cache/query_cache.bin")

  """
  @spec persist_to_file(String.t(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def persist_to_file(path, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, {:persist_to_file, path})
  end

  @doc """
  Warms the cache from a previously persisted file.

  Loads cached entries from disk and populates the cache. This is useful
  for restoring cache state after a restart.

  ## Arguments

  - `path` - File path to read the cache data from
  - `opts` - Options including `:name` for the cache process

  ## Returns

  - `{:ok, count}` - Number of entries loaded
  - `{:error, reason}` - On failure (file not found, corrupt data, etc.)

  ## Examples

      {:ok, 42} = Query.Cache.warm_from_file("/var/cache/query_cache.bin")

  """
  @spec warm_from_file(String.t(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def warm_from_file(path, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, {:warm_from_file, path})
  end

  @doc """
  Pre-executes a list of queries to warm the cache.

  Executes each query using the provided execute function and caches
  the results. This is useful for pre-warming the cache with known
  frequent queries.

  ## Arguments

  - `queries` - List of `{query, execute_fn, opts}` tuples where:
    - `query` - The query to cache
    - `execute_fn` - Function that returns `{:ok, result}` or `{:error, reason}`
    - `opts` - Options for the cache entry (e.g., `:predicates`)
  - `opts` - Options including `:name` for the cache process

  ## Returns

  - `{:ok, %{cached: count, failed: count}}` - Summary of warming results

  ## Examples

      queries = [
        {"SELECT ?s WHERE { ?s a <Person> }", fn -> execute_query(...) end, []},
        {"SELECT ?name WHERE { ?s <name> ?name }", fn -> execute_query(...) end, []}
      ]
      {:ok, %{cached: 2, failed: 0}} = Query.Cache.warm_queries(queries)

  """
  @spec warm_queries([{term(), (-> {:ok, term()} | {:error, term()}), keyword()}], keyword()) ::
          {:ok, %{cached: non_neg_integer(), failed: non_neg_integer()}}
  def warm_queries(queries, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)

    results =
      Enum.map(queries, fn {query, execute_fn, query_opts} ->
        case get_or_execute(query, execute_fn, Keyword.merge(query_opts, name: name)) do
          {:ok, _result} -> :cached
          {:error, _reason} -> :failed
        end
      end)

    cached = Enum.count(results, &(&1 == :cached))
    failed = Enum.count(results, &(&1 == :failed))

    {:ok, %{cached: cached, failed: failed}}
  end

  @doc """
  Returns all cache entries in a format suitable for persistence.

  This is useful for manual cache management or custom persistence strategies.

  ## Returns

  List of persistable entry maps with `:key`, `:result`, `:result_size`, and `:predicates`.
  """
  @spec get_all_entries(keyword()) :: [persistable_entry()]
  def get_all_entries(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :get_all_entries)
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
    max_memory_bytes = Keyword.get(opts, :max_memory_bytes, @default_max_memory_bytes)
    ttl_ms = Keyword.get(opts, :ttl_ms, @default_ttl_ms)
    name = Keyword.get(opts, :name, @default_name)
    persistence_path = Keyword.get(opts, :persistence_path)
    warm_on_start = Keyword.get(opts, :warm_on_start, false)
    allowed_persistence_dir = Keyword.get(opts, :allowed_persistence_dir)

    # Create ETS tables
    results_table = table_name(name, @results_table_suffix)
    lru_table = table_name(name, @lru_table_suffix)
    predicate_index_table = table_name(name, @predicate_index_suffix)

    :ets.new(results_table, [:set, :named_table, :public, read_concurrency: true])
    :ets.new(lru_table, [:ordered_set, :named_table, :public])
    # Predicate index: {predicate, cache_key} - bag allows multiple keys per predicate
    :ets.new(predicate_index_table, [:bag, :named_table, :public])

    state = %{
      results_table: results_table,
      lru_table: lru_table,
      predicate_index_table: predicate_index_table,
      max_entries: max_entries,
      max_result_size: max_result_size,
      max_memory_bytes: max_memory_bytes,
      ttl_ms: ttl_ms,
      persistence_path: persistence_path,
      allowed_persistence_dir: allowed_persistence_dir,
      current_memory_bytes: 0,
      hits: 0,
      misses: 0,
      evictions: 0,
      skipped_large: 0,
      skipped_memory: 0,
      expired: 0
    }

    # Warm cache from disk if enabled and file exists
    state =
      if warm_on_start and persistence_path do
        do_warm_from_file(state, persistence_path)
      else
        state
      end

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
    # Use reverse index for O(1) lookup per predicate instead of O(n) table scan
    keys_to_invalidate =
      predicate_set
      |> Enum.flat_map(fn pred ->
        :ets.lookup(state.predicate_index_table, pred)
        |> Enum.map(fn {_pred, key} -> key end)
      end)
      |> Enum.uniq()

    # Remove matching entries and update memory tracking
    state =
      Enum.reduce(keys_to_invalidate, state, fn key, acc_state ->
        case :ets.lookup(acc_state.results_table, key) do
          [{^key, entry}] ->
            remove_entry(acc_state, key, entry)

          [] ->
            acc_state
        end
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
      skipped_memory: state.skipped_memory,
      expired: state.expired,
      memory_bytes: state.current_memory_bytes
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
      max_memory_bytes: state.max_memory_bytes,
      ttl_ms: state.ttl_ms,
      persistence_path: state.persistence_path,
      allowed_persistence_dir: state.allowed_persistence_dir
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

  @impl true
  def handle_call({:persist_to_file, path}, _from, state) do
    result = do_persist_to_file(state, path)
    {:reply, result, state}
  end

  @impl true
  def handle_call({:warm_from_file, path}, _from, state) do
    case do_warm_from_file(state, path) do
      {:error, _} = error -> {:reply, error, state}
      new_state -> {:reply, {:ok, :ets.info(new_state.results_table, :size)}, new_state}
    end
  end

  @impl true
  def handle_call(:get_all_entries, _from, state) do
    entries =
      :ets.foldl(
        fn {key, entry}, acc ->
          [
            %{
              key: key,
              result: entry.result,
              result_size: entry.result_size,
              predicates: MapSet.to_list(entry.predicates)
            }
            | acc
          ]
        end,
        [],
        state.results_table
      )

    {:reply, entries, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Persist cache to disk on shutdown if persistence is configured
    if state.persistence_path do
      Logger.info("Persisting cache to disk on shutdown...")
      do_persist_to_file(state, state.persistence_path)
    end

    # Clean up ETS tables (they're owned by GenServer and will be deleted anyway,
    # but explicit cleanup is good practice)
    if :ets.whereis(state.results_table) != :undefined do
      :ets.delete(state.results_table)
    end

    if :ets.whereis(state.lru_table) != :undefined do
      :ets.delete(state.lru_table)
    end

    if :ets.whereis(state.predicate_index_table) != :undefined do
      :ets.delete(state.predicate_index_table)
    end

    :ok
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp table_name(process_name, suffix) when is_atom(process_name) do
    String.to_atom("#{process_name}#{suffix}")
  end

  defp do_put(state, key, result, result_size, predicates) do
    now = System.monotonic_time(:millisecond)
    predicate_set = MapSet.new(predicates)

    # Estimate memory size of entry
    entry_memory = estimate_entry_memory(result)

    # Check memory limit before adding
    state =
      if state.max_memory_bytes do
        if state.current_memory_bytes + entry_memory > state.max_memory_bytes do
          # Need to evict until we have room
          evict_until_memory_available(state, entry_memory)
        else
          state
        end
      else
        state
      end

    entry = %{
      result: result,
      result_size: result_size,
      memory_bytes: entry_memory,
      access_time: now,
      created_at: now,
      predicates: predicate_set
    }

    # Check if key already exists and clean up old entry
    state =
      case :ets.lookup(state.results_table, key) do
        [{^key, old_entry}] ->
          # Update existing entry - remove from LRU and predicate index
          :ets.delete(state.lru_table, {old_entry.access_time, key})
          remove_from_predicate_index(state, key, old_entry.predicates)
          %{state | current_memory_bytes: state.current_memory_bytes - old_entry.memory_bytes}

        [] ->
          state
      end

    # Insert new entry
    :ets.insert(state.results_table, {key, entry})
    :ets.insert(state.lru_table, {{now, key}, true})

    # Add to predicate index
    add_to_predicate_index(state, key, predicate_set)

    # Update memory tracking
    state = %{state | current_memory_bytes: state.current_memory_bytes + entry_memory}

    # Evict if entry count exceeded
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
        # Get entry for memory tracking before deletion
        case :ets.lookup(state.results_table, key) do
          [{^key, entry}] ->
            :ets.delete(state.lru_table, lru_key)
            :ets.delete(state.results_table, key)
            remove_from_predicate_index(state, key, entry.predicates)
            entry_memory = Map.get(entry, :memory_bytes, 0)

            %{
              state
              | evictions: state.evictions + 1,
                current_memory_bytes: max(0, state.current_memory_bytes - entry_memory)
            }

          [] ->
            :ets.delete(state.lru_table, lru_key)
            %{state | evictions: state.evictions + 1}
        end
    end
  end

  defp remove_entry(state, key, entry) do
    :ets.delete(state.results_table, key)
    :ets.delete(state.lru_table, {entry.access_time, key})
    remove_from_predicate_index(state, key, entry.predicates)

    # Update memory tracking
    entry_memory = Map.get(entry, :memory_bytes, 0)
    %{state | current_memory_bytes: max(0, state.current_memory_bytes - entry_memory)}
  end

  defp expired?(_entry, nil), do: false

  defp expired?(entry, ttl_ms) do
    now = System.monotonic_time(:millisecond)
    now - entry.created_at > ttl_ms
  end

  defp compute_result_size(result) when is_list(result), do: length(result)
  defp compute_result_size(%{bindings: bindings}) when is_list(bindings), do: length(bindings)
  defp compute_result_size(_), do: 1

  # ===========================================================================
  # Predicate Index Helpers
  # ===========================================================================

  defp add_to_predicate_index(state, key, predicates) do
    Enum.each(predicates, fn pred ->
      :ets.insert(state.predicate_index_table, {pred, key})
    end)
  end

  defp remove_from_predicate_index(state, key, predicates) do
    Enum.each(predicates, fn pred ->
      :ets.delete_object(state.predicate_index_table, {pred, key})
    end)
  end

  # ===========================================================================
  # Memory Management Helpers
  # ===========================================================================

  @doc false
  # Estimates memory usage of a result using term_to_binary size as proxy
  defp estimate_entry_memory(result) do
    # Use :erlang.external_size for a rough estimate without actually serializing
    # This is faster than term_to_binary and gives a reasonable approximation
    try do
      :erlang.external_size(result)
    rescue
      _ -> 0
    end
  end

  defp evict_until_memory_available(state, needed_bytes) do
    if state.current_memory_bytes + needed_bytes <= state.max_memory_bytes do
      state
    else
      case :ets.first(state.lru_table) do
        :"$end_of_table" ->
          # No more entries to evict
          %{state | skipped_memory: state.skipped_memory + 1}

        {_time, key} = lru_key ->
          case :ets.lookup(state.results_table, key) do
            [{^key, entry}] ->
              :ets.delete(state.lru_table, lru_key)
              :ets.delete(state.results_table, key)
              remove_from_predicate_index(state, key, entry.predicates)
              entry_memory = Map.get(entry, :memory_bytes, 0)

              new_state = %{
                state
                | evictions: state.evictions + 1,
                  current_memory_bytes: max(0, state.current_memory_bytes - entry_memory)
              }

              # Recursively evict until we have enough room
              evict_until_memory_available(new_state, needed_bytes)

            [] ->
              :ets.delete(state.lru_table, lru_key)
              evict_until_memory_available(state, needed_bytes)
          end
      end
    end
  end

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
  # Persistence Helpers
  # ===========================================================================

  # File format version for backwards compatibility
  @cache_file_version 1

  defp do_persist_to_file(state, path) do
    case validate_persistence_path(state, path) do
      {:ok, validated_path} ->
        entries =
          :ets.foldl(
            fn {key, entry}, acc ->
              [
                %{
                  key: key,
                  result: entry.result,
                  result_size: entry.result_size,
                  predicates: MapSet.to_list(entry.predicates)
                }
                | acc
              ]
            end,
            [],
            state.results_table
          )

        data = %{
          version: @cache_file_version,
          timestamp: System.os_time(:second),
          entry_count: length(entries),
          entries: entries
        }

        binary = :erlang.term_to_binary(data, [:compressed])

        case File.write(validated_path, binary) do
          :ok ->
            emit_cache_persist(length(entries))
            {:ok, length(entries)}

          {:error, reason} ->
            Logger.warning("Failed to persist cache to #{validated_path}: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_warm_from_file(state, path) do
    case validate_persistence_path(state, path) do
      {:ok, validated_path} ->
        case File.read(validated_path) do
          {:ok, binary} ->
            try do
              # Use :safe to prevent atom creation, but also validate structure
              data = :erlang.binary_to_term(binary, [:safe])
              validated_data = validate_cache_data(data)
              load_entries_from_data(state, validated_data)
            rescue
              e ->
                Logger.warning("Failed to parse cache file #{validated_path}: #{inspect(e)}")
                {:error, :invalid_format}
            end

          {:error, :enoent} ->
            # File doesn't exist - not an error, just no cache to warm
            Logger.debug("Cache file not found at #{validated_path}, starting with empty cache")
            state

          {:error, reason} ->
            Logger.warning("Failed to read cache file #{validated_path}: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, _reason} when path == state.persistence_path ->
        # If it's the configured path but validation fails on startup, just return state
        Logger.debug("Persistence path validation failed, starting with empty cache")
        state

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Validates the persistence path to prevent path traversal attacks
  defp validate_persistence_path(state, path) when is_binary(path) do
    expanded_path = Path.expand(path)

    cond do
      # If allowed_persistence_dir is configured, path must be within it
      state.allowed_persistence_dir != nil ->
        allowed_dir = Path.expand(state.allowed_persistence_dir)

        if String.starts_with?(expanded_path, allowed_dir <> "/") or expanded_path == allowed_dir do
          {:ok, expanded_path}
        else
          Logger.warning(
            "Path traversal attempt blocked: #{path} is not within #{state.allowed_persistence_dir}"
          )

          {:error, :path_not_allowed}
        end

      # If no restriction, allow any valid absolute path (but warn in logs)
      true ->
        Logger.debug(
          "Persistence path used without allowed_persistence_dir restriction: #{expanded_path}"
        )

        {:ok, expanded_path}
    end
  end

  defp validate_persistence_path(_state, _path), do: {:error, :invalid_path}

  # Validates the structure of deserialized cache data to prevent code execution
  # This is a defense-in-depth measure on top of :safe option
  defp validate_cache_data(%{version: version, entries: entries} = data)
       when is_integer(version) and is_list(entries) do
    validated_entries =
      entries
      |> Enum.filter(&valid_cache_entry?/1)
      |> Enum.take(10_000)

    %{data | entries: validated_entries}
  end

  defp validate_cache_data(_data) do
    Logger.warning("Invalid cache data structure")
    %{version: 0, entries: []}
  end

  # Validates individual cache entries have the expected structure
  defp valid_cache_entry?(%{key: key, result: _result, result_size: size, predicates: preds})
       when is_binary(key) and is_integer(size) and size >= 0 and is_list(preds) do
    # Validate predicates are not functions or other dangerous types
    Enum.all?(preds, fn pred ->
      is_atom(pred) or is_binary(pred) or is_tuple(pred) or is_map(pred)
    end)
  end

  defp valid_cache_entry?(_entry), do: false

  defp load_entries_from_data(state, %{version: 1, entries: entries}) when is_list(entries) do
    now = System.monotonic_time(:millisecond)
    loaded_count = length(entries)

    # Load entries into ETS, respecting max_entries limit
    entries_to_load = Enum.take(entries, state.max_entries)

    Enum.each(entries_to_load, fn entry ->
      # Skip entries that exceed max_result_size
      if entry.result_size <= state.max_result_size do
        predicate_set = MapSet.new(entry.predicates)
        entry_memory = estimate_entry_memory(entry.result)

        cache_entry = %{
          result: entry.result,
          result_size: entry.result_size,
          memory_bytes: entry_memory,
          access_time: now,
          created_at: now,
          predicates: predicate_set
        }

        :ets.insert(state.results_table, {entry.key, cache_entry})
        :ets.insert(state.lru_table, {{now, entry.key}, true})
        add_to_predicate_index(state, entry.key, predicate_set)
      end
    end)

    emit_cache_warm(:ets.info(state.results_table, :size))

    Logger.info(
      "Warmed cache from file: #{:ets.info(state.results_table, :size)} entries loaded (#{loaded_count} in file)"
    )

    state
  end

  defp load_entries_from_data(_state, %{version: version}) when is_integer(version) do
    Logger.warning("Unsupported cache file version: #{version}")
    {:error, {:unsupported_version, version}}
  end

  defp load_entries_from_data(_state, _data) do
    {:error, :invalid_format}
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

  defp emit_cache_persist(count) do
    :telemetry.execute(
      [:triple_store, :cache, :query, :persist],
      %{count: count},
      %{}
    )
  end

  defp emit_cache_warm(count) do
    :telemetry.execute(
      [:triple_store, :cache, :query, :warm],
      %{count: count},
      %{}
    )
  end
end
