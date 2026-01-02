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
  for tracking access order. Safe to call multiple times.

  ## Returns

  - `:ok` - Cache initialized successfully
  """
  @spec init() :: :ok
  def init do
    # Create tables if they don't exist
    if :ets.whereis(@cache_table) == :undefined do
      :ets.new(@cache_table, [:set, :public, :named_table, read_concurrency: true])
    end

    if :ets.whereis(@lru_table) == :undefined do
      :ets.new(@lru_table, [:ordered_set, :public, :named_table])
    end

    if :ets.whereis(@config_table) == :undefined do
      :ets.new(@config_table, [:set, :public, :named_table])
      :ets.insert(@config_table, {:max_entries, @default_max_entries})
    end

    :ok
  end

  @doc """
  Configures cache parameters.

  ## Options

  - `:max_entries` - Maximum number of cached subjects (default: #{@default_max_entries})

  ## Returns

  - `:ok` - Configuration applied
  """
  @spec configure(keyword()) :: :ok
  def configure(opts) do
    init()

    if max_entries = Keyword.get(opts, :max_entries) do
      :ets.insert(@config_table, {:max_entries, max_entries})
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
      [{^subject_id, properties}] ->
        # Cache hit - update LRU
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
      [{^subject_id, properties}] ->
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
      [{^subject_id, _properties}] ->
        :ets.delete(@cache_table, subject_id)
        # Remove LRU entry (find by subject_id in value)
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
    # Evict if at capacity
    maybe_evict()

    # Insert into cache
    :ets.insert(@cache_table, {subject_id, properties})

    # Update LRU (use monotonic time as key for ordering)
    update_lru(subject_id)
  end

  defp update_lru(subject_id) do
    # Remove old LRU entry for this subject
    :ets.match_delete(@lru_table, {:_, subject_id})

    # Insert new entry with current timestamp
    timestamp = System.monotonic_time()
    :ets.insert(@lru_table, {timestamp, subject_id})
  end

  defp maybe_evict do
    max_entries =
      case :ets.lookup(@config_table, :max_entries) do
        [{:max_entries, max}] -> max
        [] -> @default_max_entries
      end

    current_size = :ets.info(@cache_table, :size) || 0

    if current_size >= max_entries do
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
  end
end
