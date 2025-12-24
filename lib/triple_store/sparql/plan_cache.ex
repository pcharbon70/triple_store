defmodule TripleStore.SPARQL.PlanCache do
  @moduledoc """
  Cache for optimized SPARQL query execution plans.

  This module implements a GenServer-based cache that stores optimized execution
  plans for SPARQL queries. The cache uses ETS for fast lookups and implements
  an LRU (Least Recently Used) eviction policy to manage memory usage.

  ## Purpose

  Query optimization is expensive, especially for complex queries with many
  patterns. The plan cache stores previously computed plans so that repeated
  queries can skip the optimization phase entirely.

  ## Cache Key Strategy

  Plans are keyed by a hash of the query's algebraic representation, not the
  raw SPARQL string. This means:

  - Whitespace differences don't create separate cache entries
  - Parameterized queries with the same structure share plans
  - Different variable names with same structure share plans (normalized)

  ## Invalidation

  The cache is invalidated when:

  1. **Statistics change**: After bulk loads or significant data changes
  2. **Schema modification**: When predicates are added or removed
  3. **Manual invalidation**: Via the `invalidate/0` or `invalidate/1` functions

  ## LRU Eviction

  When the cache exceeds its maximum size, the least recently used entries
  are evicted. Access time is tracked on each lookup and the oldest entries
  are removed first.

  ## Usage

      # Start the cache (typically via supervision tree)
      {:ok, _pid} = PlanCache.start_link(max_size: 1000)

      # Get or compute a plan
      plan = PlanCache.get_or_compute(query_algebra, fn ->
        JoinEnumeration.enumerate(patterns, stats)
      end)

      # Invalidate after data change
      PlanCache.invalidate()

      # Get cache statistics
      stats = PlanCache.stats()
      # => %{size: 42, hits: 1000, misses: 50, hit_rate: 0.95}

  ## Configuration

  - `:max_size` - Maximum number of cached plans (default: 1000)
  - `:name` - Process name (default: `TripleStore.SPARQL.PlanCache`)

  """

  use GenServer

  require Logger

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Cache key (query hash)"
  @type cache_key :: binary()

  @typedoc "Cached plan entry"
  @type cache_entry :: %{
          plan: term(),
          access_time: integer(),
          created_at: integer()
        }

  @typedoc "Cache statistics"
  @type cache_stats :: %{
          size: non_neg_integer(),
          hits: non_neg_integer(),
          misses: non_neg_integer(),
          hit_rate: float(),
          evictions: non_neg_integer()
        }

  # ===========================================================================
  # Constants
  # ===========================================================================

  @default_max_size 1000
  @default_name __MODULE__

  # ETS table names (atoms derived from process name)
  @plans_table_suffix :_plans
  @lru_table_suffix :_lru

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the plan cache GenServer.

  ## Options

  - `:max_size` - Maximum number of cached plans (default: 1000)
  - `:name` - Process name (default: `TripleStore.SPARQL.PlanCache`)

  ## Examples

      {:ok, pid} = PlanCache.start_link(max_size: 500)

  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Gets a cached plan or computes and caches it.

  If the plan for the given query is in the cache, returns it immediately.
  Otherwise, calls the compute function, caches the result, and returns it.

  ## Arguments

  - `query` - The query algebra to look up
  - `compute_fn` - Zero-arity function that computes the plan if not cached
  - `opts` - Options including `:name` for the cache process

  ## Returns

  The cached or computed plan.

  ## Examples

      plan = PlanCache.get_or_compute(query, fn ->
        JoinEnumeration.enumerate(patterns, stats)
      end)

  """
  @spec get_or_compute(term(), (-> term()), keyword()) :: term()
  def get_or_compute(query, compute_fn, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    key = compute_key(query)

    case GenServer.call(name, {:get, key}) do
      {:hit, plan} ->
        plan

      :miss ->
        plan = compute_fn.()
        GenServer.cast(name, {:put, key, plan})
        plan
    end
  end

  @doc """
  Gets a cached plan without computing.

  ## Arguments

  - `query` - The query algebra to look up
  - `opts` - Options including `:name` for the cache process

  ## Returns

  - `{:ok, plan}` if cached
  - `:miss` if not cached

  """
  @spec get(term(), keyword()) :: {:ok, term()} | :miss
  def get(query, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    key = compute_key(query)

    case GenServer.call(name, {:get, key}) do
      {:hit, plan} -> {:ok, plan}
      :miss -> :miss
    end
  end

  @doc """
  Stores a plan in the cache.

  ## Arguments

  - `query` - The query algebra
  - `plan` - The computed plan
  - `opts` - Options including `:name` for the cache process

  """
  @spec put(term(), term(), keyword()) :: :ok
  def put(query, plan, opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    key = compute_key(query)
    GenServer.cast(name, {:put, key, plan})
  end

  @doc """
  Invalidates the entire cache.

  Call this after bulk data loads or schema changes.

  ## Examples

      PlanCache.invalidate()

  """
  @spec invalidate(keyword()) :: :ok
  def invalidate(opts \\ []) do
    name = Keyword.get(opts, :name, @default_name)
    GenServer.call(name, :invalidate)
  end

  @doc """
  Invalidates a specific cached plan.

  ## Arguments

  - `query` - The query algebra to invalidate

  """
  @spec invalidate(term(), keyword()) :: :ok
  def invalidate(query, opts) do
    name = Keyword.get(opts, :name, @default_name)
    key = compute_key(query)
    GenServer.call(name, {:invalidate, key})
  end

  @doc """
  Returns cache statistics.

  ## Returns

  Map with:
  - `:size` - Current number of cached plans
  - `:hits` - Total cache hits
  - `:misses` - Total cache misses
  - `:hit_rate` - Hit rate (0.0 to 1.0)
  - `:evictions` - Total evictions performed

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

  # ===========================================================================
  # Key Computation
  # ===========================================================================

  @doc """
  Computes a cache key from a query algebra.

  The key is computed by:
  1. Normalizing variable names to canonical form
  2. Computing a hash of the normalized structure

  This ensures that queries with the same structure but different variable
  names share cache entries.

  """
  @spec compute_key(term()) :: cache_key()
  def compute_key(query) do
    normalized = normalize_query(query)
    :crypto.hash(:sha256, :erlang.term_to_binary(normalized))
  end

  # Normalize query by replacing variable names with positional indices
  defp normalize_query(query) do
    {normalized, _var_map, _counter} = normalize_term(query, %{}, 0)
    normalized
  end

  defp normalize_term({:variable, name}, var_map, counter) when is_binary(name) do
    case Map.get(var_map, name) do
      nil ->
        new_map = Map.put(var_map, name, counter)
        {{:variable, counter}, new_map, counter + 1}

      idx ->
        {{:variable, idx}, var_map, counter}
    end
  end

  defp normalize_term({:variable, name}, var_map, counter) when is_atom(name) do
    normalize_term({:variable, Atom.to_string(name)}, var_map, counter)
  end

  defp normalize_term(tuple, var_map, counter) when is_tuple(tuple) do
    list = Tuple.to_list(tuple)
    {normalized_list, final_map, final_counter} = normalize_list(list, var_map, counter)
    {List.to_tuple(normalized_list), final_map, final_counter}
  end

  defp normalize_term(list, var_map, counter) when is_list(list) do
    normalize_list(list, var_map, counter)
  end

  defp normalize_term(map, var_map, counter) when is_map(map) do
    {pairs, final_map, final_counter} =
      Enum.reduce(Map.to_list(map), {[], var_map, counter}, fn {k, v}, {acc, vm, c} ->
        {norm_k, vm2, c2} = normalize_term(k, vm, c)
        {norm_v, vm3, c3} = normalize_term(v, vm2, c2)
        {[{norm_k, norm_v} | acc], vm3, c3}
      end)

    {Map.new(pairs), final_map, final_counter}
  end

  defp normalize_term(other, var_map, counter) do
    {other, var_map, counter}
  end

  defp normalize_list(list, var_map, counter) do
    {reversed, final_map, final_counter} =
      Enum.reduce(list, {[], var_map, counter}, fn elem, {acc, vm, c} ->
        {norm, new_vm, new_c} = normalize_term(elem, vm, c)
        {[norm | acc], new_vm, new_c}
      end)

    {Enum.reverse(reversed), final_map, final_counter}
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @impl true
  def init(opts) do
    max_size = Keyword.get(opts, :max_size, @default_max_size)
    name = Keyword.get(opts, :name, @default_name)

    # Create ETS tables
    plans_table = table_name(name, @plans_table_suffix)
    lru_table = table_name(name, @lru_table_suffix)

    :ets.new(plans_table, [:set, :named_table, :public, read_concurrency: true])
    :ets.new(lru_table, [:ordered_set, :named_table, :public])

    state = %{
      plans_table: plans_table,
      lru_table: lru_table,
      max_size: max_size,
      hits: 0,
      misses: 0,
      evictions: 0
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:get, key}, _from, state) do
    case :ets.lookup(state.plans_table, key) do
      [{^key, entry}] ->
        # Update access time
        now = System.monotonic_time(:millisecond)
        old_time = entry.access_time
        new_entry = %{entry | access_time: now}
        :ets.insert(state.plans_table, {key, new_entry})

        # Update LRU ordering
        :ets.delete(state.lru_table, {old_time, key})
        :ets.insert(state.lru_table, {{now, key}, true})

        {:reply, {:hit, entry.plan}, %{state | hits: state.hits + 1}}

      [] ->
        {:reply, :miss, %{state | misses: state.misses + 1}}
    end
  end

  @impl true
  def handle_call(:invalidate, _from, state) do
    :ets.delete_all_objects(state.plans_table)
    :ets.delete_all_objects(state.lru_table)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:invalidate, key}, _from, state) do
    case :ets.lookup(state.plans_table, key) do
      [{^key, entry}] ->
        :ets.delete(state.plans_table, key)
        :ets.delete(state.lru_table, {entry.access_time, key})

      [] ->
        :ok
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_call(:stats, _from, state) do
    size = :ets.info(state.plans_table, :size)
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
      evictions: state.evictions
    }

    {:reply, stats, state}
  end

  @impl true
  def handle_call(:size, _from, state) do
    size = :ets.info(state.plans_table, :size)
    {:reply, size, state}
  end

  @impl true
  def handle_cast({:put, key, plan}, state) do
    now = System.monotonic_time(:millisecond)

    entry = %{
      plan: plan,
      access_time: now,
      created_at: now
    }

    # Check if key already exists
    case :ets.lookup(state.plans_table, key) do
      [{^key, old_entry}] ->
        # Update existing entry
        :ets.delete(state.lru_table, {old_entry.access_time, key})

      [] ->
        :ok
    end

    # Insert new entry
    :ets.insert(state.plans_table, {key, entry})
    :ets.insert(state.lru_table, {{now, key}, true})

    # Evict if necessary
    state = maybe_evict(state)

    {:noreply, state}
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp table_name(process_name, suffix) when is_atom(process_name) do
    String.to_atom("#{process_name}#{suffix}")
  end

  defp maybe_evict(state) do
    current_size = :ets.info(state.plans_table, :size)

    if current_size > state.max_size do
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
        :ets.delete(state.plans_table, key)
        %{state | evictions: state.evictions + 1}
    end
  end
end
