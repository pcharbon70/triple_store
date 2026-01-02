defmodule TripleStore.Index.SubjectCache do
  @moduledoc """
  LRU cache for subject property maps.

  Caches all properties for recently accessed subjects, reducing redundant
  index lookups when multiple patterns reference the same subject. This is
  particularly beneficial for queries like Q6 that retrieve many properties
  from a single entity.

  ## Usage

      # Initialize the cache
      SubjectCache.init()

      # Configure cache size (default: 1000 entries)
      SubjectCache.configure(max_entries: 5000)

      # Get cached properties or fetch from index
      {:ok, properties} = SubjectCache.get_or_fetch(db, subject_id)

      # Invalidate on subject update
      SubjectCache.invalidate(subject_id)

      # Clear entire cache
      SubjectCache.clear()

  ## Performance

  - Cache hits: O(1) ETS lookup
  - Cache misses: O(m) where m = number of properties, plus cache write
  - LRU eviction: O(log n) where n = cache size
  """

  alias TripleStore.Index
  alias TripleStore.Backend.RocksDB.NIF

  require Logger

  # ===========================================================================
  # Constants
  # ===========================================================================

  @cache_table :triple_store_subject_cache
  @lru_table :triple_store_subject_cache_lru
  @config_table :triple_store_subject_cache_config
  @default_max_entries 1000
  # Default max memory: 100MB
  @default_max_memory_bytes 100 * 1024 * 1024
  # Max properties per subject to prevent memory exhaustion
  @max_properties_per_subject 10_000

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Cached property map: predicate_id => [object_id, ...]"
  @type property_map :: %{non_neg_integer() => [non_neg_integer()]}

  # ===========================================================================
  # Initialization
  # ===========================================================================

  @doc """
  Initializes the subject cache ETS tables.

  Creates the cache table for storing property maps and the LRU table
  for tracking access order. Safe to call concurrently - uses atomic table
  creation to avoid race conditions.

  ## Returns

  - `:ok` - Cache initialized successfully
  """
  @spec init() :: :ok
  def init do
    alias TripleStore.ETSHelper

    ETSHelper.ensure_table!(@cache_table, [:set, :public, :named_table, read_concurrency: true])
    ETSHelper.ensure_table!(@lru_table, [:ordered_set, :public, :named_table])

    # Config table needs special handling to insert defaults on creation
    case ETSHelper.ensure_table(@config_table, [:set, :public, :named_table]) do
      :created ->
        :ets.insert(@config_table, {:max_entries, @default_max_entries})
        :ets.insert(@config_table, {:max_memory_bytes, @default_max_memory_bytes})

      :exists ->
        :ok
    end

    :ok
  end

  @doc """
  Configures cache parameters.

  ## Options

  - `:max_entries` - Maximum number of cached subjects (default: #{@default_max_entries})
  - `:max_memory_bytes` - Maximum memory in bytes (default: 100MB)

  ## Returns

  - `:ok` - Configuration applied
  """
  @spec configure(keyword()) :: :ok
  def configure(opts) do
    init()

    if max_entries = Keyword.get(opts, :max_entries) do
      :ets.insert(@config_table, {:max_entries, max_entries})
    end

    if max_memory = Keyword.get(opts, :max_memory_bytes) do
      :ets.insert(@config_table, {:max_memory_bytes, max_memory})
    end

    :ok
  end

  # ===========================================================================
  # Cache Operations
  # ===========================================================================

  @doc """
  Gets cached properties or fetches from index.

  First checks the cache for the subject's property map. On cache miss,
  fetches from the index using `Index.lookup_all_properties/2` and caches
  the result.

  ## Arguments

  - `db` - RocksDB database reference
  - `subject_id` - Subject term ID

  ## Returns

  - `{:ok, property_map}` - Map of predicate_id => [object_id, ...]
  - `{:error, reason}` - On fetch failure
  """
  @spec get_or_fetch(NIF.db_ref(), non_neg_integer()) ::
          {:ok, property_map()} | {:error, term()}
  def get_or_fetch(db, subject_id) do
    init()

    case :ets.lookup(@cache_table, subject_id) do
      [{^subject_id, {_timestamp, properties}}] ->
        # Cache hit (new format with timestamp) - update LRU
        update_lru_on_hit(subject_id)

        :telemetry.execute(
          [:triple_store, :index, :subject_cache, :hit],
          %{count: 1},
          %{subject_id: subject_id}
        )

        {:ok, properties}

      [{^subject_id, properties}] when is_map(properties) ->
        # Cache hit (legacy format without timestamp) - update LRU
        update_lru(subject_id)

        :telemetry.execute(
          [:triple_store, :index, :subject_cache, :hit],
          %{count: 1},
          %{subject_id: subject_id}
        )

        {:ok, properties}

      [] ->
        # Cache miss - fetch and cache
        case Index.lookup_all_properties(db, subject_id) do
          {:ok, properties} ->
            cache_properties(subject_id, properties)

            :telemetry.execute(
              [:triple_store, :index, :subject_cache, :miss],
              %{count: 1},
              %{subject_id: subject_id, property_count: map_size(properties)}
            )

            {:ok, properties}

          {:error, _} = error ->
            error
        end
    end
  end

  @doc """
  Gets cached properties without fetching.

  Returns the cached property map if available, or `:not_found` if not cached.

  ## Arguments

  - `subject_id` - Subject term ID

  ## Returns

  - `{:ok, property_map}` - Cached properties
  - `:not_found` - Subject not in cache
  """
  @spec get(non_neg_integer()) :: {:ok, property_map()} | :not_found
  def get(subject_id) do
    init()

    case :ets.lookup(@cache_table, subject_id) do
      [{^subject_id, {_timestamp, properties}}] ->
        update_lru_on_hit(subject_id)
        {:ok, properties}

      [{^subject_id, properties}] when is_map(properties) ->
        update_lru(subject_id)
        {:ok, properties}

      [] ->
        :not_found
    end
  end

  @doc """
  Explicitly caches properties for a subject.

  ## Arguments

  - `subject_id` - Subject term ID
  - `properties` - Property map to cache

  ## Returns

  - `:ok`
  """
  @spec put(non_neg_integer(), property_map()) :: :ok
  def put(subject_id, properties) do
    init()
    cache_properties(subject_id, properties)
    :ok
  end

  @doc """
  Invalidates cached properties for a subject.

  Call this when a subject is updated to ensure stale data is not returned.

  ## Arguments

  - `subject_id` - Subject term ID to invalidate

  ## Returns

  - `:ok`
  """
  @spec invalidate(non_neg_integer()) :: :ok
  def invalidate(subject_id) do
    init()

    # Remove from cache
    case :ets.lookup(@cache_table, subject_id) do
      [{^subject_id, {timestamp, _properties}}] when is_integer(timestamp) ->
        # New format with timestamp - use O(1) delete
        :ets.delete(@cache_table, subject_id)
        :ets.delete(@lru_table, timestamp)

      [{^subject_id, _properties}] ->
        # Legacy format - use O(n) match_delete
        :ets.delete(@cache_table, subject_id)
        :ets.match_delete(@lru_table, {:_, subject_id})

      [] ->
        :ok
    end

    :ok
  end

  @doc """
  Clears the entire cache.

  ## Returns

  - `:ok`
  """
  @spec clear() :: :ok
  def clear do
    init()
    :ets.delete_all_objects(@cache_table)
    :ets.delete_all_objects(@lru_table)
    :ok
  end

  @doc """
  Returns cache statistics.

  ## Returns

  Map with `:size` (current entries) and `:max_entries` (configured limit).
  """
  @spec stats() :: %{size: non_neg_integer(), max_entries: non_neg_integer()}
  def stats do
    init()

    size = :ets.info(@cache_table, :size) || 0

    max_entries =
      case :ets.lookup(@config_table, :max_entries) do
        [{:max_entries, max}] -> max
        [] -> @default_max_entries
      end

    %{size: size, max_entries: max_entries}
  end

  # ===========================================================================
  # Private Helpers
  # ===========================================================================

  defp cache_properties(subject_id, properties) do
    # Validate property count to prevent memory exhaustion from large subjects
    property_count = map_size(properties)

    if property_count > @max_properties_per_subject do
      # Don't cache excessively large subjects - log and skip
      Logger.warning(
        "SubjectCache: Skipping cache for subject #{subject_id} with #{property_count} properties " <>
          "(exceeds max #{@max_properties_per_subject})"
      )

      :ok
    else
      # Evict if at capacity (entry count or memory)
      maybe_evict()

      # Update LRU and get timestamp for cache entry
      timestamp = update_lru(subject_id)

      # Insert into cache with timestamp for O(1) LRU cleanup
      :ets.insert(@cache_table, {subject_id, {timestamp, properties}})
    end
  end

  # Update LRU on cache hit - more efficient than full update
  # Only updates the timestamp without re-inserting the entire cache entry
  defp update_lru_on_hit(subject_id) do
    case :ets.lookup(@cache_table, subject_id) do
      [{^subject_id, {old_timestamp, properties}}] ->
        # Remove old LRU entry
        :ets.delete(@lru_table, old_timestamp)

        # Insert new LRU entry with updated timestamp
        new_timestamp = System.monotonic_time()
        :ets.insert(@lru_table, {new_timestamp, subject_id})

        # Update cache entry with new timestamp
        :ets.insert(@cache_table, {subject_id, {new_timestamp, properties}})

      _ ->
        # Fallback to full update for legacy format
        :ok
    end
  end

  defp update_lru(subject_id) do
    # Get the old timestamp for this subject from the cache entry metadata
    # The cache stores {subject_id, {timestamp, properties}} to enable O(1) LRU cleanup
    timestamp = System.monotonic_time()

    # First, look up the old entry to get its timestamp
    case :ets.lookup(@cache_table, subject_id) do
      [{^subject_id, {old_timestamp, _properties}}] when is_integer(old_timestamp) ->
        # Remove old LRU entry using the known timestamp (O(1) vs O(n) match_delete)
        :ets.delete(@lru_table, old_timestamp)

      [{^subject_id, _properties}] ->
        # Old format entry without timestamp - use match_delete as fallback
        :ets.match_delete(@lru_table, {:_, subject_id})

      [] ->
        :ok
    end

    # Insert new LRU entry
    :ets.insert(@lru_table, {timestamp, subject_id})

    # Return timestamp for caller to use when storing cache entry
    timestamp
  end

  defp maybe_evict do
    max_entries =
      case :ets.lookup(@config_table, :max_entries) do
        [{:max_entries, max}] -> max
        [] -> @default_max_entries
      end

    max_memory =
      case :ets.lookup(@config_table, :max_memory_bytes) do
        [{:max_memory_bytes, max}] -> max
        [] -> @default_max_memory_bytes
      end

    current_size = :ets.info(@cache_table, :size) || 0
    current_memory = :ets.info(@cache_table, :memory) * :erlang.system_info(:wordsize)

    # Evict if over entry count OR memory limit
    if current_size >= max_entries or current_memory >= max_memory do
      evict_oldest()
    end
  end

  defp evict_oldest do
    # Evict oldest entry (smallest timestamp)
    case :ets.first(@lru_table) do
      :"$end_of_table" ->
        :ok

      oldest_timestamp ->
        case :ets.lookup(@lru_table, oldest_timestamp) do
          [{^oldest_timestamp, subject_id}] ->
            :ets.delete(@cache_table, subject_id)
            :ets.delete(@lru_table, oldest_timestamp)

            :telemetry.execute(
              [:triple_store, :index, :subject_cache, :eviction],
              %{count: 1},
              %{subject_id: subject_id}
            )

          [] ->
            :ok
        end
    end
  end

  @doc """
  Returns estimated memory usage in bytes.
  """
  @spec memory_usage() :: non_neg_integer()
  def memory_usage do
    init()
    (:ets.info(@cache_table, :memory) || 0) * :erlang.system_info(:wordsize)
  end
end
