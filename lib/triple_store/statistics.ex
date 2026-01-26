defmodule TripleStore.Statistics do
  @moduledoc """
  Statistics collection for cost-based query optimization.

  Provides functions to compute and cache statistics about the stored triples
  and quads, enabling the query optimizer to accurately estimate result sizes
  and select efficient query plans.

  ## Features

  - **Triple counts**: Total count and per-predicate counts
  - **Quad counts**: Per-graph quad counts for named graph support
  - **Distinct counts**: Counts of distinct subjects, predicates, objects
  - **Predicate histogram**: Per-predicate triple counts for selectivity estimation
  - **Numeric histograms**: Equi-width histograms for range selectivity estimation
  - **Per-graph caching**: ETS-based cache for graph-specific statistics
  - **Persistence**: Statistics stored in RocksDB for fast reload
  - **Telemetry**: Collection timing and metrics

  ## Usage

      # Collect all statistics (full scan)
      {:ok, stats} = Statistics.collect(db)

      # Get cached statistics (fast)
      {:ok, stats} = Statistics.get(db)

      # Refresh statistics
      :ok = Statistics.refresh(db)

      # Get graph-specific statistics with caching
      {:ok, stats} = Statistics.graph_statistics(db, 0)

      # Warm cache for a specific graph
      :ok = Statistics.warm_graph_cache(db, 0)

      # Invalidate cache for a specific graph
      :ok = Statistics.invalidate_quad_cache(db, 0)

      # Estimate range selectivity
      selectivity = Statistics.estimate_range_selectivity(stats, pred_id, 10.0, 100.0)

  ## Statistics Structure

      # Triple store statistics
      %{
        triple_count: 10000,
        distinct_subjects: 1000,
        distinct_predicates: 50,
        distinct_objects: 2000,
        predicate_histogram: %{42 => 500, 43 => 1500},
        numeric_histograms: %{
          price_id => %{min: 0.0, max: 1000.0, buckets: [...]}
        },
        collected_at: ~U[2026-01-02 12:00:00Z],
        version: 1
      }

      # Quad store per-graph statistics (cached)
      %{
        graph_id: 0,
        quad_count: 5000,
        distinct_subjects: 500,
        distinct_predicates: 20,
        distinct_objects: 800,
        predicate_counts: %{42 => 1000, 43 => 500}
      }

  ## Stream Performance and Memory Characteristics

  This module uses RocksDB prefix iterators via `ErlangAdapter.prefix_stream/3` to efficiently
  scan large datasets without loading everything into memory. Understanding the
  performance characteristics is important for optimal usage.

  ### Memory Usage

  - **Constant memory per stream**: Each stream maintains a RocksDB iterator that
    holds a single key-value pair in memory at a time (~100 bytes per iterator)
  - **Pipeline memory**: `Stream.transform` operations accumulate results in
    their buffers before emitting (controlled by `:max_items` option)
  - **Map accumulation**: Operations like `Enum.reduce/3` build the result map
    in memory - ensure adequate heap space for large histograms

  ### Performance Estimates

  For a database with N triples:

  - **Full index scan**: ~O(N) time, ~1ms per 1000 entries (SSD)
  - **Predicate histogram**: ~O(N) time, ~N * 8 bytes for result map
  - **Numeric histogram**: ~O(N) time, two full passes (min/max + bucket population)
  - **Per-graph scan**: ~O(G) where G is quads in that graph

  ### Stream Best Practices

  1. **Use Stream operations, not Enum**: Stream operations are lazy and process
     one element at a time. Enum operations load the entire collection into memory.

  ```elixir
  # GOOD - Lazy, constant memory
  stream
  |> Stream.map(fn {k, v} -> process(k, v) end)
  |> Stream.filter(fn result -> keep?(result) end)
  |> Enum.reduce(%{}, fn val, acc -> Map.update(acc, val, 1, &(&1 + 1)) end)

  # BAD - Loads all intermediate results into memory
  stream
  |> Enum.map(fn {k, v} -> process(k, v) end)
  |> Enum.filter(fn result -> keep?(result) end)
  ```

  2. **Close streams promptly**: RocksDB iterators hold resources. Always consume
     or close streams:

  ```elixir
  # Stream is consumed and resources released
  count = stream |> Enum.count()

  # For partial consumption, ensure stream is fully consumed
  stream |> Stream.take(100) |> Enum.into([])
  ```

  3. **Two-pass algorithms**: For operations requiring global bounds (e.g., histograms),
     two separate scans are more memory-efficient than materializing all values:

  ```elixir
  # Pass 1: Find min/max (constant memory)
  {min_val, max_val} = stream |> Enum.reduce({nil, nil}, fn val, {min, max} -> ... end)

  # Pass 2: Populate buckets (constant memory)
  histogram = stream |> build_buckets(min_val, max_val)
  ```

  4. **Batch size for async operations**: When using `Task.async_stream/5`,
     the `max_concurrency` option controls parallelism. Higher values increase
     throughput but also memory usage (one iterator per concurrent task).

  ### Monitoring

  Use telemetry events to monitor stream operations:

  - `[:triple_store, :statistics, :collect]` - Duration and record count
  - `[:triple_store, :statistics, :histogram]` - Histogram build duration
  - `[:triple_store, :statistics, :cache_miss]` - Cache misses requiring recomputation
  """

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary
  alias TripleStore.Index

  require Logger

  # Inline hot path functions for performance
  @compile {:inline, extract_first_id: 1, extract_second_id: 1, is_numeric_id?: 1}

  # Note on GenServer Usage:
  # This module uses GenServer for ETS table lifecycle management and asynchronous
  # cache invalidation, NOT for serializing cache operations. Statistics operations
  # are CPU-intensive (not I/O-bound), so direct ETS access is used for performance.
  # The GenServer handles:
  # - ETS table creation on startup
  # - Periodic cache size checks and eviction
  # - Asynchronous cache invalidation messages
  use GenServer

  # ===========================================================================
  # Client API
  # ===========================================================================

  @doc """
  Starts the statistics cache server.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stops the statistics cache server.
  """
  def stop do
    GenServer.stop(__MODULE__)
  end

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Database reference"
  @type db_ref :: ErlangAdapter.db_ref()

  @typedoc "64-bit term ID"
  @type term_id :: non_neg_integer()

  @typedoc "Histogram for numeric values"
  @type numeric_histogram :: %{
          min: float(),
          max: float(),
          bucket_count: pos_integer(),
          bucket_width: float(),
          buckets: [non_neg_integer()],
          total_count: non_neg_integer()
        }

  @typedoc "Complete statistics map"
  @type stats :: %{
          triple_count: non_neg_integer(),
          distinct_subjects: non_neg_integer(),
          distinct_predicates: non_neg_integer(),
          distinct_objects: non_neg_integer(),
          predicate_histogram: %{term_id() => non_neg_integer()},
          numeric_histograms: %{term_id() => numeric_histogram()},
          collected_at: DateTime.t(),
          version: pos_integer()
        }

  @typedoc "Graph-specific statistics map (cached)"
  @type graph_stats :: %{
          graph_id: term_id(),
          quad_count: non_neg_integer(),
          distinct_subjects: non_neg_integer(),
          distinct_predicates: non_neg_integer(),
          distinct_objects: non_neg_integer() | :not_computed,
          predicate_counts: %{term_id() => non_neg_integer()},
          accuracy: :exact | :approximate
        }

  @typedoc "Lazy statistics wrapper for on-demand collection"
  @type lazy_stats :: %{
          required(:__lazy__) => true,
          required(:__db__) => db_ref(),
          optional(:__cache__) => boolean(),
          optional(:__ttl__) => pos_integer(),
          optional(:__collected_at__) => integer() | nil,
          optional(:__cached_stats__) => map() | nil
        }

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Statistics version for forward compatibility
  # Increment this when the stats structure changes incompatibly
  @stats_version 1

  # Default histogram bucket count for predicate distribution
  @default_bucket_count 100

  # ===========================================================================
  # Cache Key Design and Collision Prevention
  # ===========================================================================
  #
  # The statistics module uses binary prefixes to store metadata in the RocksDB
  # id2str column family. These prefixes are designed to avoid collisions with
  # actual term IDs through careful byte-level design.
  #
  # ## Term ID Encoding
  #
  # Term IDs in the triple store use type tagging in the most significant bits:
  # - 0x00...0x0F: Reserved for metadata
  # - 0x10...0xFF: Actual term identifiers
  #
  # Valid term IDs always have the high bit (0x80) set for 64-bit IDs, with
  # the type tag in bits 4-7. This means all valid term IDs are >= 2^56.
  #
  # ## Key Prefix Structure
  #
  # Statistics keys use 8-byte prefixes with the pattern:
  # - Byte 0: 0x00 (metadata marker - never valid for term IDs)
  # - Bytes 1-6: 0x00 (reserved for future use)
  # - Byte 7: Identifier (1 for global stats, 2 for per-graph stats)
  #
  # This design guarantees that:
  # 1. No valid term ID can start with 0x00 (would have type tag 0x00 which is reserved)
  # 2. Statistics keys are easily identifiable by their prefix
  # 3. Multiple key types can coexist without collision
  # 4. Prefix scans can efficiently find all statistics entries
  #
  # ## Examples
  #
  #   @stats_key_prefix   => <<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01>>
  #   @quad_stats_prefix  => <<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02>>
  #
  # Per-graph stats key construction:
  #   @quad_stats_prefix <> <<graph_id::64-big>>
  #
  # This creates keys like:
  #   <<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05>>
  #   for graph_id=5
  #
  # ===========================================================================

  # Key prefix for persisted global statistics in id2str column family
  # Uses a reserved prefix that can't conflict with term IDs (type tag 0)
  # The 0x01 identifier distinguishes global stats from per-graph stats
  @stats_key_prefix <<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01>>

  # Required statistics keys for validation
  # These keys must be present in a valid statistics map
  @required_stats_keys [
    :triple_count,
    :distinct_subjects,
    :distinct_predicates,
    :distinct_objects,
    :predicate_histogram,
    :numeric_histograms,
    :collected_at,
    :version
  ]

  # Key prefix for persisted per-graph quad statistics
  # Uses the same collision-resistant design as @stats_key_prefix
  # The 0x02 identifier distinguishes per-graph stats from global stats
  # Full key for a graph: @quad_stats_prefix <> <<graph_id::64-big>>
  @quad_stats_prefix <<0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02>>

  # ETS table name for in-memory quad statistics cache
  # Named table allows efficient in-process lookups without RocksDB access
  @quad_cache_table :triple_store_quad_stats_cache

  # Default max concurrency for parallel cache warming
  # Limits simultaneous graph cache warming to prevent overwhelming the database
  @default_max_concurrency 4

  # Maximum size for binary term deserialization (10MB)
  # Prevents memory exhaustion attacks via maliciously large terms
  # Any term exceeding this size during deserialization will be rejected
  @max_term_size 10_000_000

  # Maximum number of entries in quad statistics cache
  # When this limit is exceeded, the entire cache is cleared (simple eviction)
  # This prevents unbounded memory growth from caching many graphs
  @max_cache_entries 10_000

  # Interval for cache size checks (60 seconds)
  # The cache size is checked periodically rather than on every insert
  # to avoid the overhead of size checking during cache operations
  @cache_check_interval 60_000

  # Default counts for when statistics are not available
  @default_triple_count 10_000
  @default_quad_count 10_000

  # Cache warming limits
  # 30 seconds per graph
  @cache_warm_timeout 30_000
  # Maximum graphs to warm in parallel
  @max_graphs_to_warm 100

  # ===========================================================================
  # Column Family Validation
  # ===========================================================================
  #
  # The Statistics module interacts with specific RocksDB column families.
  # Using an invalid column family atom will cause the NIF layer to return
  # an error, but validating at the Elixir level provides better error messages.
  #
  # ## Valid Column Families for Statistics Operations
  #
  # For quad stores:
  # - `:gspo` - Graph-Subject-Predicate-Object index (primary quad index)
  # - `:gpos` - Graph-Predicate-Object-Subject index
  # - `:spog` - Subject-Predicate-Object-Graph index
  # - `:posg` - Predicate-Object-Subject-Graph index
  # - `:id2str` - ID to string dictionary (stores persisted statistics)
  #
  # For triple stores (legacy):
  # - `:spo` - Subject-Predicate-Object index
  # - `:pos` - Predicate-Object-Subject index
  # - `:osp` - Object-Subject-Predicate index
  # - `:id2str` - ID to string dictionary (stores persisted statistics)
  #
  # ## Statistics Storage
  #
  # Statistics are persisted in the `:id2str` column family using the reserved
  # key prefixes defined above (@stats_key_prefix for global stats,
  # @quad_stats_prefix for per-graph stats).
  #
  # ## Statistics Computation
  #
  # Statistics are computed by scanning the `:gspo` index for quad counts,
  # predicate distributions, and other metrics.
  #
  # ===========================================================================

  @valid_quad_cfs [:gspo, :gpos, :spog, :posg, :id2str]
  @valid_triple_cfs [:spo, :pos, :osp, :id2str]

  @doc """
  Validates that a column family atom is valid for statistics operations.

  ## Returns

  - `:ok` if the column family is valid
  - `{:error, :invalid_cf}` if the column family is not recognized

  ## Examples

      iex> Statistics.validate_cf(:gspo)
      :ok

      iex> Statistics.validate_cf(:invalid)
      {:error, :invalid_cf}

  """
  @spec validate_cf(atom()) :: :ok | {:error, :invalid_cf}
  def validate_cf(cf) when cf in @valid_quad_cfs, do: :ok
  def validate_cf(cf) when cf in @valid_triple_cfs, do: :ok
  def validate_cf(_cf), do: {:error, :invalid_cf}

  @doc """
  Validates a column family and raises an error if invalid.

  ## Raises

  - `ArgumentError` if the column family is not valid

  ## Examples

      iex> Statistics.validate_cf!(:gspo)
      :ok

      iex> Statistics.validate_cf!(:invalid)
      ** (ArgumentError) invalid column family: :invalid

  """
  @spec validate_cf!(atom()) :: :ok
  def validate_cf!(cf) do
    case validate_cf(cf) do
      :ok -> :ok
      {:error, :invalid_cf} -> raise ArgumentError, "invalid column family: #{inspect(cf)}"
    end
  end

  # ===========================================================================
  # GenServer Callbacks
  # ===========================================================================

  @doc false
  def init(_opts) do
    # Create ETS table for in-memory caching
    table =
      :ets.new(@quad_cache_table, [
        :set,
        :public,
        :named_table,
        read_concurrency: true,
        write_concurrency: true
      ])

    # Start periodic cache size check
    :timer.send_interval(@cache_check_interval, :check_cache_size)

    {:ok, %{table: table}}
  end

  @doc false
  def handle_continue(:warm_cache_on_startup, state) do
    # Optionally warm cache on startup if configured
    {:noreply, state}
  end

  @doc false
  def handle_info({:invalidate_graph, graph_id}, state) do
    # Invalidate cache for specific graph
    graph_key = quad_cache_key(graph_id)
    :ets.delete(@quad_cache_table, graph_key)

    # Also invalidate all-graphs summary
    all_key = all_graphs_cache_key()
    :ets.delete(@quad_cache_table, all_key)

    {:noreply, state}
  end

  @doc false
  def handle_info(:invalidate_all, state) do
    # Invalidate all quad statistics
    :ets.delete_all_objects(@quad_cache_table)
    {:noreply, state}
  end

  @doc false
  def handle_info(:check_cache_size, state) do
    # Check cache size and evict if over limit
    size = :ets.info(@quad_cache_table, :size)

    if size > @max_cache_entries do
      # Emit telemetry warning
      :telemetry.execute(
        [:triple_store, :statistics, :quad_cache, :overflow],
        %{current_size: size, max_size: @max_cache_entries},
        %{}
      )

      # Clear cache when over limit (simple eviction policy)
      # For production, consider LRU or other smarter eviction
      :ets.delete_all_objects(@quad_cache_table)
    end

    {:noreply, state}
  end

  # ===========================================================================
  # Public API - Quad Cache Management
  # ===========================================================================

  @doc """
  Gets cached graph statistics, computing and caching if necessary.

  First checks the in-memory ETS cache. If not found, computes fresh
  statistics and stores them in the cache.

  ## Arguments

  - `db` - Database reference
  - `graph_id` - The graph ID (0 for default, or named graph ID)

  ## Returns

  - `{:ok, stats}` - Graph statistics map
  - `{:error, reason}` - On failure

  """
  @spec get_cached_graph_stats(db_ref(), term_id()) :: {:ok, graph_stats()} | {:error, term()}
  def get_cached_graph_stats(db, graph_id) when is_integer(graph_id) and graph_id >= 0 do
    cache_key = quad_cache_key(graph_id)

    case :ets.lookup(@quad_cache_table, cache_key) do
      [{^cache_key, stats}] ->
        # Emit telemetry for cache hit
        :telemetry.execute(
          [:triple_store, :statistics, :quad_cache, :hit],
          %{graph_id: graph_id},
          %{}
        )

        {:ok, stats}

      [] ->
        # Cache miss - compute and cache
        case compute_and_cache_graph_stats(db, graph_id) do
          {:ok, _stats} = result ->
            # Emit telemetry for cache miss
            :telemetry.execute(
              [:triple_store, :statistics, :quad_cache, :miss],
              %{graph_id: graph_id},
              %{}
            )

            result

          error ->
            error
        end
    end
  end

  @doc """
  Gets cached all-graphs summary, computing and caching if necessary.

  ## Arguments

  - `db` - Database reference
  - `opts` - Options passed to `all_graphs_summary/2`

  ## Returns

  - `{:ok, summary}` - All graphs summary map
  - `{:error, reason}` - On failure
  """
  @spec get_cached_all_graphs_summary(db_ref(), keyword()) :: {:ok, map()} | {:error, term()}
  def get_cached_all_graphs_summary(db, opts \\ []) do
    cache_key = all_graphs_cache_key()

    case :ets.lookup(@quad_cache_table, cache_key) do
      [{^cache_key, summary}] ->
        # Emit telemetry for cache hit
        :telemetry.execute(
          [:triple_store, :statistics, :quad_cache, :all_graphs_hit],
          %{},
          %{}
        )

        {:ok, summary}

      [] ->
        # Cache miss - compute and cache
        case all_graphs_summary(db, opts) do
          {:ok, summary} = result ->
            # Cache the result using insert_new to prevent race conditions
            # If another process inserted the value while we were computing,
            # we use their value instead (which should be identical)
            case :ets.insert_new(@quad_cache_table, {cache_key, summary}) do
              true ->
                # We successfully inserted our computed value
                result

              false ->
                # Another process beat us to it - their value is in cache
                # Read and return the cached value to ensure consistency
                case :ets.lookup(@quad_cache_table, cache_key) do
                  [{^cache_key, cached_summary}] -> {:ok, cached_summary}
                  # Cache was cleared, return our computed value
                  [] -> result
                end
            end

          error ->
            error
        end
    end
  end

  @doc """
  Invalidates cached statistics for a specific graph.

  Removes the graph's statistics from the in-memory cache.
  Also invalidates the all-graphs summary since it depends on
  individual graph statistics.

  ## Arguments

  - `db` - Database reference (unused, for API consistency)
  - `graph_id` - The graph ID to invalidate

  ## Returns

  - `:ok` - Always succeeds

  """
  @spec invalidate_quad_cache(db_ref(), term_id()) :: :ok
  def invalidate_quad_cache(_db, graph_id) when is_integer(graph_id) and graph_id >= 0 do
    # Only attempt to invalidate if the ETS table exists
    # (i.e., the Statistics server is running)
    if :ets.whereis(@quad_cache_table) != :undefined do
      graph_key = quad_cache_key(graph_id)
      :ets.delete(@quad_cache_table, graph_key)

      # Invalidate all-graphs summary
      all_key = all_graphs_cache_key()
      :ets.delete(@quad_cache_table, all_key)

      # Emit telemetry
      :telemetry.execute(
        [:triple_store, :statistics, :quad_cache, :invalidate],
        %{graph_id: graph_id},
        %{}
      )
    end

    :ok
  end

  @doc """
  Invalidates all cached quad statistics.

  Clears all graph-specific statistics and the all-graphs summary
  from the in-memory cache.

  ## Arguments

  - `db` - Database reference (unused, for API consistency)

  ## Returns

  - `:ok` - Always succeeds

  """
  @spec invalidate_all_quad_cache(db_ref()) :: :ok
  def invalidate_all_quad_cache(_db) do
    # Only attempt to invalidate if the ETS table exists
    # (i.e., the Statistics server is running)
    if :ets.whereis(@quad_cache_table) != :undefined do
      :ets.delete_all_objects(@quad_cache_table)

      # Emit telemetry
      :telemetry.execute(
        [:triple_store, :statistics, :quad_cache, :invalidate_all],
        %{},
        %{}
      )
    end

    :ok
  end

  @doc """
  Warms the cache for a specific graph by computing and storing statistics.

  ## Arguments

  - `db` - Database reference
  - `graph_id` - The graph ID to warm cache for
  - `opts` - Options (none currently)

  ## Returns

  - `:ok` - Cache warmed successfully
  - `{:error, reason}` - On failure

  """
  @spec warm_graph_cache(db_ref(), term_id(), keyword()) :: :ok | {:error, term()}
  def warm_graph_cache(db, graph_id, opts \\ []) when is_integer(graph_id) and graph_id >= 0 do
    start_time = System.monotonic_time()

    case graph_summary(db, graph_id, opts) do
      {:ok, summary} ->
        cache_key = quad_cache_key(graph_id)
        # Use insert_new to avoid overwriting a more recent cache entry
        # Returns true if we inserted, false if key already existed
        :ets.insert_new(@quad_cache_table, {cache_key, summary})

        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:triple_store, :statistics, :quad_cache, :warm],
          %{graph_id: graph_id, duration: duration},
          %{}
        )

        :ok

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Warms the cache for all graphs in the store.

  Computes and caches statistics for all graphs. Can use parallel
  processing for multiple graphs.

  ## Arguments

  - `db` - Database reference
  - `opts` - Options:
    - `:max_concurrency` - Maximum parallel warming tasks (default: 4)
    - `:include_default` - Include default graph (default: true)

  ## Returns

  - `:ok` - All caches warmed successfully
  - `{:error, reason}` - On failure

  """
  @spec warm_all_graphs_cache(db_ref(), keyword()) :: :ok | {:error, term()}
  def warm_all_graphs_cache(db, opts \\ []) do
    start_time = System.monotonic_time()
    max_concurrency = Keyword.get(opts, :max_concurrency, @default_max_concurrency)
    include_default = Keyword.get(opts, :include_default, true)

    # Get all graph IDs
    case build_per_graph_histograms(db, include_default: include_default) do
      {:ok, histograms} ->
        graph_ids = Map.keys(histograms)

        # Limit number of graphs to warm to prevent resource exhaustion
        graph_ids = Enum.take(graph_ids, @max_graphs_to_warm)

        # Warm cache for each graph (with parallel processing and timeout)
        graph_ids
        |> Task.async_stream(
          fn graph_id -> warm_graph_cache(db, graph_id, opts) end,
          max_concurrency: max_concurrency,
          ordered: false,
          timeout: @cache_warm_timeout
        )
        |> Stream.run()

        # Warm all-graphs summary
        cache_key = all_graphs_cache_key()

        case all_graphs_summary(db, opts) do
          {:ok, summary} ->
            # Use insert_new to avoid overwriting a more recent cache entry
            :ets.insert_new(@quad_cache_table, {cache_key, summary})
        end

        duration = System.monotonic_time() - start_time

        :telemetry.execute(
          [:triple_store, :statistics, :quad_cache, :warm_all],
          %{graph_count: length(graph_ids), duration: duration},
          %{}
        )

        :ok
    end
  end

  # ===========================================================================
  # Public API - Collection
  # ===========================================================================

  @doc """
  Collects all statistics from the database.

  Performs a full scan of indices to gather:
  - Total triple count
  - Distinct subject/predicate/object counts
  - Per-predicate cardinalities (predicate histogram)
  - Numeric histograms for inline-encoded predicates

  ## Arguments

  - `db` - Database reference
  - `opts` - Options:
    - `:bucket_count` - Number of histogram buckets (default: 100)
    - `:build_histograms` - Whether to build numeric histograms (default: true)

  ## Returns

  - `{:ok, stats}` - Complete statistics map
  - `{:error, reason}` - On failure

  ## Performance

  This performs multiple index scans. For large datasets:
  - SPO scan for triple count and distinct subjects
  - POS scan for predicate histogram and distinct predicates
  - OSP scan for distinct objects
  - Optional per-predicate scans for numeric histograms
  """
  @spec collect(db_ref(), keyword()) :: {:ok, stats()} | {:error, term()}
  def collect(db, opts \\ []) do
    start_time = System.monotonic_time()
    bucket_count = Keyword.get(opts, :bucket_count, @default_bucket_count)
    build_histograms? = Keyword.get(opts, :build_histograms, true)

    with {:ok, triple_count} <- triple_count(db),
         {:ok, distinct_subjects} <- distinct_subjects(db),
         {:ok, distinct_predicates} <- distinct_predicates(db),
         {:ok, distinct_objects} <- distinct_objects(db),
         {:ok, predicate_histogram} <- build_predicate_histogram(db),
         {:ok, numeric_histograms} <-
           maybe_build_numeric_histograms(
             db,
             predicate_histogram,
             bucket_count,
             build_histograms?
           ) do
      stats = %{
        triple_count: triple_count,
        distinct_subjects: distinct_subjects,
        distinct_predicates: distinct_predicates,
        distinct_objects: distinct_objects,
        predicate_histogram: predicate_histogram,
        numeric_histograms: numeric_histograms,
        collected_at: DateTime.utc_now(),
        version: @stats_version
      }

      duration = System.monotonic_time() - start_time

      :telemetry.execute(
        [:triple_store, :statistics, :collect],
        %{
          duration: duration,
          triple_count: triple_count,
          predicate_count: map_size(predicate_histogram)
        },
        %{}
      )

      {:ok, stats}
    end
  end

  @doc """
  Persists statistics to the database.

  Stores the statistics in the id2str column family using a reserved key.
  Statistics can be reloaded on restart using `load/1`.

  ## Arguments

  - `db` - Database reference
  - `stats` - Statistics map to persist

  ## Returns

  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec save(db_ref(), stats()) :: :ok | {:error, term()}
  def save(db, stats) do
    # Validate stats structure before encoding to prevent encoding errors
    with :ok <- validate_stats_structure(stats),
         # Also check size before encoding to prevent memory issues
         :ok <- validate_stats_size(stats) do
      encoded = :erlang.term_to_binary(stats, [:compressed])

      case ErlangAdapter.put(db, :id2str, @stats_key_prefix, encoded) do
        :ok -> :ok
        {:error, _} = error -> error
      end
    end
  end

  @doc """
  Validates the structure of a statistics map.

  Checks that all required keys are present and have valid types.

  ## Returns

  - `:ok` if stats structure is valid
  - `{:error, reason}` if validation fails

  """
  @spec validate_stats_structure(term()) :: :ok | {:error, term()}
  def validate_stats_structure(stats) when is_map(stats) do
    with :ok <- validate_required_keys(stats),
         :ok <- validate_stat_types(stats) do
      :ok
    end
  end

  def validate_stats_structure(_stats), do: {:error, :not_a_map}

  @doc """
  Validates the stats structure and raises if invalid.

  This is a bang version of `validate_stats_structure/1` that raises
  an ArgumentError instead of returning `{:error, reason}`.

  ## Raises

  - `ArgumentError` if stats structure is invalid

  ## Examples

      iex> Statistics.validate_stats!(%{triple_count: 100, quad_count: 100})
      :ok

      iex> Statistics.validate_stats!(%{})
      ** (ArgumentError) Invalid statistics: missing required keys: [:triple_count, :quad_count]

  """
  @spec validate_stats!(map()) :: :ok | no_return()
  def validate_stats!(stats) when is_map(stats) do
    case validate_stats_structure(stats) do
      :ok ->
        :ok

      {:error, {:missing_keys, keys}} ->
        raise ArgumentError, "Invalid statistics: missing required keys: #{inspect(keys)}"

      {:error, {:invalid_type, key, value}} ->
        raise ArgumentError, "Invalid statistics: #{key} has invalid value: #{inspect(value)}"

      {:error, reason} ->
        raise ArgumentError, "Invalid statistics: #{inspect(reason)}"
    end
  end

  def validate_stats!(stats) do
    raise ArgumentError, "Invalid statistics: expected a map, got: #{inspect(stats)}"
  end

  # Validate that all required keys are present
  defp validate_required_keys(stats) do
    missing_keys =
      @required_stats_keys
      |> Enum.reject(fn key -> Map.has_key?(stats, key) end)

    if Enum.empty?(missing_keys) do
      :ok
    else
      {:error, {:missing_keys, missing_keys}}
    end
  end

  # Validate that stats have the correct types
  defp validate_stat_types(stats) do
    # Validate specific keys have expected types
    validators = [
      {:triple_count, &validate_non_neg_integer/1},
      {:quad_count, &validate_non_neg_integer/1},
      {:distinct_subjects, &validate_non_neg_integer/1},
      {:distinct_predicates, &validate_non_neg_integer/1},
      {:distinct_objects, &validate_non_neg_integer/1},
      {:version, &validate_non_neg_integer/1},
      {:collected_at,
       fn
         %DateTime{} -> :ok
         _ -> {:error, :invalid_datetime}
       end}
    ]

    errors =
      validators
      |> Enum.filter(fn {key, _validator} -> Map.has_key?(stats, key) end)
      |> Enum.map(fn {key, validator} ->
        case validator.(Map.get(stats, key)) do
          :ok -> nil
          error -> {key, error}
        end
      end)
      |> Enum.reject(&is_nil/1)

    if Enum.empty?(errors) do
      :ok
    else
      {:error, {:invalid_types, errors}}
    end
  end

  defp validate_non_neg_integer(value) when is_integer(value) and value >= 0, do: :ok
  defp validate_non_neg_integer(_value), do: {:error, :invalid_type}

  # Validate stats size before encoding to prevent memory exhaustion
  defp validate_stats_size(stats) do
    # Use :erlang.external_size to estimate binary size without creating it
    try do
      size = :erlang.external_size(stats, [:compressed])

      if size > @max_term_size do
        {:error, {:stats_too_large, size, @max_term_size}}
      else
        :ok
      end
    rescue
      _ -> {:error, :cannot_estimate_size}
    end
  end

  # ===========================================================================
  # Lazy Statistics Collection
  # ===========================================================================

  @doc """
  Creates a lazy statistics wrapper that defers collection until needed.

  Instead of collecting all statistics immediately, this creates a wrapper
  that collects statistics on-demand when specific values are accessed.

  ## Options

  - `:cache` - Whether to cache collected results (default: true)
  - `:ttl` - Time-to-live for cached results in milliseconds (default: 300_000)

  ## Examples

      # Create lazy statistics
      lazy_stats = Statistics.lazy(db)

      # Statistics are collected when accessed
      {:ok, triple_count} = Statistics.get_lazy(lazy_stats, :triple_count)

      # Force collection of all statistics
      {:ok, stats} = Statistics materialize(lazy_stats)
  """
  @spec lazy(db_ref(), keyword()) :: lazy_stats()
  def lazy(db, opts \\ []) do
    cache = Keyword.get(opts, :cache, true)
    ttl = Keyword.get(opts, :ttl, 300_000)

    %{
      __lazy__: true,
      __module__: __MODULE__,
      __db__: db,
      __cache__: cache,
      __ttl__: ttl,
      __collected_at__: nil,
      __cached_stats__: nil
    }
  end

  @doc """
  Gets a specific statistic from a lazy statistics wrapper.

  If the statistic hasn't been collected yet, it will be collected now.
  If caching is enabled, subsequent calls for the same statistic will return
  the cached value.

  ## Examples

      lazy_stats = Statistics.lazy(db)
      {:ok, count} = Statistics.get_lazy(lazy_stats, :triple_count)
  """
  @spec get_lazy(lazy_stats(), atom()) :: {:ok, term()} | {:error, term()}
  def get_lazy(%{__lazy__: true} = lazy_stats, key) do
    get_lazy_cached(lazy_stats, key)
  end

  def get_lazy(_stats, _key), do: {:error, :not_lazy}

  # Get from cache or collect
  defp get_lazy_cached(lazy_stats, key) do
    if should_refresh_cache?(lazy_stats) do
      case collect_for_key(lazy_stats.__db__, key) do
        {:ok, value} ->
          if lazy_stats.__cache__ do
            update_lazy_cache(lazy_stats, key, value)
          end

          {:ok, value}

        error ->
          error
      end
    else
      # Return cached value
      {:ok, Map.get(lazy_stats.__cached_stats__, key)}
    end
  end

  # Check if cache should be refreshed
  defp should_refresh_cache?(lazy_stats) do
    not lazy_stats.__cache__ or
      lazy_stats.__cached_stats__ == nil or
      cache_expired?(lazy_stats)
  end

  defp cache_expired?(lazy_stats) do
    if lazy_stats.__collected_at__ do
      elapsed = System.monotonic_time(:millisecond) - lazy_stats.__collected_at__
      elapsed > lazy_stats.__ttl__
    else
      true
    end
  end

  # Collect a specific statistic by key
  defp collect_for_key(db, :triple_count) do
    triple_count(db)
  end

  defp collect_for_key(db, :quad_count) do
    triple_count(db)
  end

  defp collect_for_key(db, :distinct_subjects) do
    distinct_subjects(db)
  end

  defp collect_for_key(db, :distinct_predicates) do
    distinct_predicates(db)
  end

  defp collect_for_key(db, :distinct_objects) do
    distinct_objects(db)
  end

  defp collect_for_key(db, :predicate_histogram) do
    build_predicate_histogram(db)
  end

  defp collect_for_key(db, :all) do
    collect(db)
  end

  defp collect_for_key(_db, key), do: {:error, {:unsupported_key, key}}

  # Update lazy cache with new value
  defp update_lazy_cache(lazy_stats, key, value) do
    current_cache = lazy_stats.__cached_stats__ || %{}
    new_cache = Map.put(current_cache, key, value)

    # Update the struct in place (this works within GenServer state)
    Process.put(:lazy_stats_cache, new_cache)
  end

  @doc """
  Materializes all lazy statistics, collecting them all at once.

  This is useful when you know you'll need multiple statistics and want
  to avoid multiple on-demand collections.

  ## Examples

      lazy_stats = Statistics.lazy(db)
      {:ok, full_stats} = Statistics.materialize(lazy_stats)
  """
  @spec materialize(lazy_stats()) :: {:ok, stats()} | {:error, term()}
  def materialize(%{__lazy__: true, __db__: db}) do
    collect(db)
  end

  def materialize(_stats), do: {:error, :not_lazy}

  @doc """
  Checks if a statistics map is a lazy wrapper.

  ## Examples

      lazy_stats = Statistics.lazy(db)
      Statistics.lazy?(lazy_stats)  # => true

      regular_stats = %{triple_count: 100}
      Statistics.lazy?(regular_stats)  # => false
  """
  @spec lazy?(term()) :: boolean()
  def lazy?(%{__lazy__: true}), do: true
  def lazy?(_), do: false

  @doc """
  Refreshes a lazy statistics wrapper, clearing any cached values.

  ## Examples

      lazy_stats = Statistics.lazy(db)
      {:ok, _} = Statistics.get_lazy(lazy_stats, :triple_count)
      Statistics.refresh_lazy(lazy_stats)
      # Next get_lazy will re-collect
  """
  @spec refresh_lazy(lazy_stats()) :: :ok | {:error, term()}
  def refresh_lazy(%{__lazy__: true}) do
    # Clear the cache by updating process dictionary
    Process.put(:lazy_stats_cache, nil)
    Process.put(:lazy_stats_collected_at, nil)
    :ok
  end

  def refresh_lazy(_stats), do: {:error, :not_lazy}

  @spec load(db_ref()) :: {:ok, stats() | nil} | {:error, term()}
  def load(db) do
    case ErlangAdapter.get(db, :id2str, @stats_key_prefix) do
      {:ok, encoded} when is_binary(encoded) ->
        # Check size before deserialization to prevent memory exhaustion
        if byte_size(encoded) > @max_term_size do
          {:error, {:term_too_large, byte_size(encoded), @max_term_size}}
        else
          try do
            stats = :erlang.binary_to_term(encoded, [:safe])

            case validate_stats_structure(stats) do
              :ok -> {:ok, stats}
              {:error, _} = error -> error
            end
          rescue
            ArgumentError -> {:error, :invalid_stats_format}
            Protocol.UndefinedError -> {:error, :incompatible_stats_version}
          end
        end

      :not_found ->
        {:ok, nil}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Gets statistics, loading from persistence if available.

  First attempts to load persisted statistics. If not available,
  collects fresh statistics and persists them.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, stats}` - Statistics map
  - `{:error, reason}` - On failure
  """
  @spec get(db_ref()) :: {:ok, stats()} | {:error, term()}
  def get(db) do
    case load(db) do
      {:ok, nil} ->
        with {:ok, stats} <- collect(db),
             :ok <- save(db, stats) do
          {:ok, stats}
        end

      {:ok, stats} ->
        {:ok, stats}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Refreshes statistics by collecting fresh data.

  Collects new statistics and persists them, replacing any
  previously stored statistics.

  ## Arguments

  - `db` - Database reference
  - `opts` - Collection options (see `collect/2`)

  ## Returns

  - `{:ok, stats}` - Fresh statistics
  - `{:error, reason}` - On failure
  """
  @spec refresh(db_ref(), keyword()) :: {:ok, stats()} | {:error, term()}
  def refresh(db, opts \\ []) do
    with {:ok, stats} <- collect(db, opts),
         :ok <- save(db, stats) do
      {:ok, stats}
    end
  end

  # ===========================================================================
  # Public API - Triple Counts
  # ===========================================================================

  @doc """
  Returns the total number of triples in the store.

  Counts all entries in the SPO index to get the total triple count.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Total number of triples
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, count} = Statistics.triple_count(db)
      iex> count
      42
  """
  @spec triple_count(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def triple_count(db) do
    Index.count(db, {:var, :var, :var})
  end

  @doc """
  Standardized accessor for getting quad count from statistics.

  Returns the quad_count from the stats map, falling back to triple_count
  for backward compatibility with triple stores.

  ## Arguments

  - `stats` - Statistics map

  ## Returns

  The quad count (or triple count as fallback)

  """
  @spec quad_count_from_stats(map()) :: non_neg_integer()
  def quad_count_from_stats(stats) do
    Map.get(stats, :quad_count) || Map.get(stats, :triple_count, @default_quad_count)
  end

  @doc """
  Standardized accessor for getting triple count from statistics.

  Returns the triple_count from the stats map, with a default fallback.

  ## Arguments

  - `stats` - Statistics map

  ## Returns

  The triple count

  """
  @spec triple_count_from_stats(map()) :: non_neg_integer()
  def triple_count_from_stats(stats) do
    Map.get(stats, :triple_count, @default_triple_count)
  end

  @doc """
  Gets the total count from statistics, working for both triple and quad stores.

  For quad stores, returns :quad_count.
  For triple stores, returns :triple_count.

  ## Arguments

  - `stats` - Statistics map

  ## Returns

  The total count

  """
  @spec total_count_from_stats(map()) :: non_neg_integer()
  def total_count_from_stats(stats) do
    Map.get(stats, :quad_count) || Map.get(stats, :triple_count, @default_triple_count)
  end

  @doc """
  Returns the number of triples with a specific predicate.

  Uses the POS index for efficient predicate-based counting.

  ## Arguments

  - `db` - Database reference
  - `predicate_id` - The term ID of the predicate to count

  ## Returns

  - `{:ok, count}` - Number of triples with this predicate
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, count} = Statistics.predicate_count(db, rdf_type_id)
      iex> count
      1000
  """
  @spec predicate_count(db_ref(), term_id()) :: {:ok, non_neg_integer()} | {:error, term()}
  def predicate_count(db, predicate_id) when is_integer(predicate_id) and predicate_id >= 0 do
    Index.count(db, {:var, {:bound, predicate_id}, :var})
  end

  # ===========================================================================
  # Public API - Distinct Counts
  # ===========================================================================

  @doc """
  Returns the count of distinct subjects.

  Scans the SPO index and counts unique subject IDs.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Number of distinct subjects
  - `{:error, reason}` - On failure
  """
  @spec distinct_subjects(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def distinct_subjects(db) do
    count_distinct_by_position(db, :spo)
  end

  @doc """
  Returns the count of distinct predicates.

  Scans the POS index and counts unique predicate IDs.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Number of distinct predicates
  - `{:error, reason}` - On failure
  """
  @spec distinct_predicates(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def distinct_predicates(db) do
    count_distinct_by_position(db, :pos)
  end

  @doc """
  Returns the count of distinct objects.

  Scans the OSP index and counts unique object IDs.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, count}` - Number of distinct objects
  - `{:error, reason}` - On failure
  """
  @spec distinct_objects(db_ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def distinct_objects(db) do
    count_distinct_by_position(db, :osp)
  end

  # ===========================================================================
  # Public API - Predicate Histogram
  # ===========================================================================

  @doc """
  Builds a histogram of triple counts per predicate.

  Scans the POS index and counts triples for each unique predicate.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, histogram}` - Map of predicate_id => count
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, histogram} = Statistics.build_predicate_histogram(db)
      iex> histogram
      %{42 => 500, 43 => 1500, 44 => 300}
  """
  @spec build_predicate_histogram(db_ref()) :: {:ok, %{term_id() => non_neg_integer()}}
  def build_predicate_histogram(db) do
    # prefix_stream now returns the stream directly (may raise on error)
    stream = ErlangAdapter.prefix_stream(db, :pos, <<>>)

    histogram =
      stream
      |> Stream.map(fn {key, _value} -> extract_first_id(key) end)
      |> Enum.frequencies()

    {:ok, histogram}
  end

  # ===========================================================================
  # Public API - Numeric Histograms
  # ===========================================================================

  @doc """
  Builds a numeric histogram for a specific predicate.

  Scans all values for the predicate and builds an equi-width histogram
  of the numeric values. Only works for predicates with inline-encoded
  numeric values (integers, decimals, datetimes).

  ## Arguments

  - `db` - Database reference
  - `predicate_id` - The predicate to build histogram for
  - `bucket_count` - Number of histogram buckets (default: 100)

  ## Returns

  - `{:ok, histogram}` - Numeric histogram map
  - `{:ok, nil}` - Predicate has no numeric values
  - `{:error, reason}` - On failure

  ## Histogram Structure

      %{
        min: 0.0,
        max: 1000.0,
        bucket_count: 100,
        buckets: [45, 67, 89, ...],  # 100 counts
        total_count: 5000
      }
  """
  @spec build_numeric_histogram(db_ref(), term_id(), pos_integer()) ::
          {:ok, numeric_histogram() | nil}
  def build_numeric_histogram(db, predicate_id, bucket_count \\ @default_bucket_count) do
    prefix = <<predicate_id::64-big>>

    # Two-pass streaming to avoid loading all values into memory (B2 fix)
    # Pass 1: Find min, max, and count by streaming
    # prefix_stream now returns the stream directly (may raise on error)
    stream1 = ErlangAdapter.prefix_stream(db, :pos, prefix)

    {min_val, max_val, count} =
      stream1
      |> Stream.map(fn {key, _value} -> extract_second_id(key) end)
      |> Stream.filter(&is_numeric_id?/1)
      |> Stream.map(&decode_numeric_value/1)
      |> Enum.reduce({nil, nil, 0}, fn value, {min_acc, max_acc, count_acc} ->
        min_val = if min_acc == nil, do: value, else: min(min_acc, value)
        max_val = if max_acc == nil, do: value, else: max(max_acc, value)
        {min_val, max_val, count_acc + 1}
      end)

    if count == 0 do
      {:ok, nil}
    else
      # Pass 2: Stream again to populate buckets
      {:ok, histogram} = build_histogram_from_values(min_val, max_val, count, bucket_count)

      # prefix_stream now returns the stream directly
      stream2 = ErlangAdapter.prefix_stream(db, :pos, prefix)

      value_stream =
        stream2
        |> Stream.map(fn {key, _value} -> extract_second_id(key) end)
        |> Stream.filter(&is_numeric_id?/1)
        |> Stream.map(&decode_numeric_value/1)

      final_histogram = populate_histogram_buckets(histogram, value_stream)
      {:ok, final_histogram}
    end
  end

  @doc """
  Estimates the selectivity of a range query using histogram.

  Returns the estimated fraction of values that fall within the given range.

  ## Arguments

  - `stats` - Statistics map containing numeric_histograms
  - `predicate_id` - The predicate to estimate for
  - `min_value` - Minimum value of range (inclusive)
  - `max_value` - Maximum value of range (inclusive)

  ## Returns

  - Selectivity as a float between 0.0 and 1.0
  - 1.0 if no histogram available for the predicate

  ## Examples

      iex> Statistics.estimate_range_selectivity(stats, price_id, 10.0, 100.0)
      0.35  # Approximately 35% of values fall in this range
  """
  @spec estimate_range_selectivity(stats(), term_id(), number(), number()) :: float()
  def estimate_range_selectivity(stats, predicate_id, min_value, max_value) do
    case get_in(stats, [:numeric_histograms, predicate_id]) do
      nil ->
        # No histogram available, assume uniform distribution
        1.0

      histogram ->
        estimate_range_from_histogram(histogram, min_value, max_value)
    end
  end

  # ===========================================================================
  # Public API - Bulk Statistics
  # ===========================================================================

  @doc """
  Returns all basic statistics in a single call.

  Computes basic counts together for convenience. For full statistics
  including histograms, use `collect/1`.

  ## Arguments

  - `db` - Database reference

  ## Returns

  - `{:ok, stats_map}` - Map with basic statistics
  - `{:error, reason}` - On failure
  """
  @spec all(db_ref()) :: {:ok, map()} | {:error, term()}
  def all(db) do
    with {:ok, tc} <- triple_count(db),
         {:ok, ds} <- distinct_subjects(db),
         {:ok, dp} <- distinct_predicates(db),
         {:ok, do_} <- distinct_objects(db) do
      {:ok,
       %{
         triple_count: tc,
         distinct_subjects: ds,
         distinct_predicates: dp,
         distinct_objects: do_
       }}
    end
  end

  # ===========================================================================
  # Public API - Graph-Specific Statistics (Quad Store)
  # ===========================================================================

  @doc """
  Returns statistics for a specific named graph or the default graph.

  Computes per-graph statistics including:
  - Quad count in the graph
  - Distinct subjects in the graph
  - Per-predicate counts within the graph

  ## Arguments

  - `db` - Database reference
  - `graph_id` - The graph ID (0 for default graph, or encoded named graph ID)

  ## Returns

  - `{:ok, stats_map}` - Map with graph-specific statistics
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, stats} = Statistics.graph_statistics(db, 0)
      iex> stats
      %{
        quad_count: 1000,
        distinct_subjects: 50,
        distinct_predicates: 10,
        predicate_counts: %{42 => 500, 43 => 1500}
      }

      iex> {:ok, stats} = Statistics.graph_statistics(db, named_graph_id)
  """
  @spec graph_statistics(db_ref(), term_id()) :: {:ok, map()} | {:error, term()}
  def graph_statistics(db, graph_id) when is_integer(graph_id) and graph_id >= 0 do
    with {:ok, quad_count} <- graph_quad_count(db, graph_id),
         {:ok, distinct_subjects} <- graph_distinct_subjects(db, graph_id),
         {:ok, predicate_counts} <- graph_predicate_histogram(db, graph_id) do
      {:ok,
       %{
         quad_count: quad_count,
         distinct_subjects: distinct_subjects,
         distinct_predicates: map_size(predicate_counts),
         predicate_counts: predicate_counts
       }}
    end
  end

  @doc """
  Returns the number of quads in a specific graph.

  Uses the GSPO index for efficient graph-scoped counting.

  ## Arguments

  - `db` - Database reference
  - `graph_id` - The graph ID (0 for default graph, or encoded named graph ID)

  ## Returns

  - `{:ok, count}` - Number of quads in this graph
  - `{:error, reason}` - On failure
  """
  @spec graph_quad_count(db_ref(), term_id()) :: {:ok, non_neg_integer()} | {:error, term()}
  def graph_quad_count(db, graph_id) when is_integer(graph_id) and graph_id >= 0 do
    # Count quads with the specified graph ID using GSPO index
    # GSPO key is: graph_id (64-bit) | subject_id (64-bit) | predicate_id (64-bit) | object_id (64-bit)
    prefix = <<graph_id::64-big>>

    # Use prefix_stream and count results
    stream = ErlangAdapter.prefix_stream(db, :gspo, prefix)
    count = Enum.count(stream)
    {:ok, count}
  end

  @doc """
  Returns the count of distinct subjects in a specific graph.

  Scans the GSPO index for the specified graph and counts unique subject IDs.

  ## Arguments

  - `db` - Database reference
  - `graph_id` - The graph ID (0 for default graph, or encoded named graph ID)

  ## Returns

  - `{:ok, count}` - Number of distinct subjects in this graph
  - `{:error, reason}` - On failure
  """
  @spec graph_distinct_subjects(db_ref(), term_id()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def graph_distinct_subjects(db, graph_id) when is_integer(graph_id) and graph_id >= 0 do
    prefix = <<graph_id::64-big>>
    stream = ErlangAdapter.prefix_stream(db, :gspo, prefix)

    count =
      stream
      |> Stream.map(fn {key, _value} ->
        # GSPO key: graph_id | subject_id | predicate_id | object_id
        # Extract subject_id (bytes 9-16, 0-indexed as 8-15)
        <<_graph::64, subject_id::64-big, _rest::binary>> = key
        subject_id
      end)
      |> Stream.dedup()
      |> Enum.count()

    {:ok, count}
  end

  @doc """
  Builds a predicate histogram for a specific graph.

  Scans the GSPO index for the specified graph and counts triples
  for each unique predicate.

  ## Arguments

  - `db` - Database reference
  - `graph_id` - The graph ID (0 for default graph, or encoded named graph ID)

  ## Returns

  - `{:ok, histogram}` - Map of predicate_id => count within the graph
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, histogram} = Statistics.graph_predicate_histogram(db, 0)
      iex> histogram
      %{42 => 500, 43 => 1500}
  """
  @spec graph_predicate_histogram(db_ref(), term_id()) ::
          {:ok, %{term_id() => non_neg_integer()}} | {:error, term()}
  def graph_predicate_histogram(db, graph_id) when is_integer(graph_id) and graph_id >= 0 do
    prefix = <<graph_id::64-big>>
    stream = ErlangAdapter.prefix_stream(db, :gspo, prefix)

    histogram =
      stream
      |> Stream.map(fn {key, _value} ->
        # GSPO key: graph_id | subject_id | predicate_id | object_id
        # Extract predicate_id (bytes 17-24, 0-indexed as 16-23)
        <<_graph::64, _subject::64, predicate_id::64-big, _object::64>> = key
        predicate_id
      end)
      |> Enum.frequencies()

    {:ok, histogram}
  end

  @doc """
  Builds predicate histograms for all graphs in a single pass.

  This is an optimized version that builds histograms for all graphs
  in one scan of the GSPO index, rather than scanning once per graph.

  ## Arguments

  - `db` - Database reference
  - `opts` - Options:
    - `:include_default` - Include default graph (ID 0) in results (default: true)

  ## Returns

  - `{:ok, histograms}` - Map of graph_id => predicate_histogram
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, histograms} = Statistics.build_per_graph_histograms(db)
      iex> histograms
      %{
        0 => %{42 => 500, 43 => 100},
        123 => %{42 => 200, 44 => 50}
      }

  ## Performance

  This function uses a single-pass algorithm that builds all histograms
  in O(total_quads) time instead of O(num_graphs * avg_quads_per_graph).

  ## Sampling

  For large datasets, you can use sampling to build approximate histograms more efficiently:

  - `sample_rate` - Fraction of quads to sample (0.0 to 1.0). E.g., 0.01 samples 1% of quads
  - `seed` - Random seed for reproducible sampling (default: random)

  When sampling, histogram counts are scaled up to estimate the full dataset.

  ## Examples

      # Sample 1% of quads for fast approximate histogram
      {:ok, histogram} = Statistics.build_per_graph_histograms(db, sample_rate: 0.01)

      # Sample with reproducible seed
      {:ok, histogram} = Statistics.build_per_graph_histograms(db, sample_rate: 0.1, seed: 42)

  """
  @spec build_per_graph_histograms(db_ref(), keyword()) ::
          {:ok, %{term_id() => %{term_id() => non_neg_integer()}}} | {:error, term()}
  def build_per_graph_histograms(db, opts \\ []) do
    include_default = Keyword.get(opts, :include_default, true)
    sample_rate = Keyword.get(opts, :sample_rate, 1.0)
    seed = Keyword.get(opts, :seed, :os.system_time(:millisecond))

    use_sampling = sample_rate < 1.0

    # Initialize random generator with seed for reproducibility
    if use_sampling do
      :rand.seed(:exsss, {seed, seed, seed})
    end

    # Stream GSPO index and build histogram
    histogram =
      db
      |> ErlangAdapter.prefix_stream(:gspo, <<>>)
      |> Stream.filter(fn {key, _value} ->
        # GSPO key: graph_id | subject_id | predicate_id | object_id
        <<graph_id::64-big, _rest::binary>> = key

        # Skip default graph if not requested
        if not include_default and graph_id == 0 do
          false
        else
          # Apply sampling if enabled
          not use_sampling or :rand.uniform() <= sample_rate
        end
      end)
      |> Enum.reduce(%{}, fn {key, _value}, acc ->
        <<graph_id::64-big, _subject::64, predicate_id::64-big, _object::64>> = key

        # Increment count for this (graph_id, predicate_id) pair
        Map.update(acc, graph_id, %{predicate_id => 1}, fn graph_hist ->
          Map.update(graph_hist, predicate_id, 1, &(&1 + 1))
        end)
      end)

    # Scale up histogram counts if sampling was used
    final_histogram =
      if use_sampling and sample_rate > 0 do
        scale_factor = 1.0 / sample_rate

        histogram
        |> Enum.map(fn {graph_id, predicates} ->
          scaled_predicates =
            Enum.map(predicates, fn {pred_id, count} ->
              # Scale count and ensure at least 1
              scaled_count = max(trunc(count * scale_factor), 1)
              {pred_id, scaled_count}
            end)
            |> Map.new()

          {graph_id, scaled_predicates}
        end)
        |> Map.new()
      else
        histogram
      end

    {:ok, final_histogram}
  end

  @doc """
  Returns the count of distinct objects in a specific graph.

  Scans the GSPO index for the specified graph and counts unique object IDs.

  ## Arguments

  - `db` - Database reference
  - `graph_id` - The graph ID (0 for default graph, or encoded named graph ID)

  ## Returns

  - `{:ok, count}` - Number of distinct objects in this graph
  - `{:error, reason}` - On failure
  """
  @spec graph_object_count(db_ref(), term_id()) :: {:ok, non_neg_integer()} | {:error, term()}
  def graph_object_count(db, graph_id) when is_integer(graph_id) and graph_id >= 0 do
    prefix = <<graph_id::64-big>>
    stream = ErlangAdapter.prefix_stream(db, :gspo, prefix)

    count =
      stream
      |> Stream.map(fn {key, _value} ->
        # GSPO key: graph_id | subject_id | predicate_id | object_id
        # Extract object_id (bytes 25-32, 0-indexed as 24-31)
        <<_graph::64, _subject::64, _predicate::64, object_id::64-big>> = key
        object_id
      end)
      |> Stream.dedup()
      |> Enum.count()

    {:ok, count}
  end

  @doc """
  Returns a complete summary for a specific graph.

  Aggregates all available statistics for a single graph including
  quad count, distinct values, and predicate distribution.

  ## Arguments

  - `db` - Database reference
  - `graph_id` - The graph ID (0 for default graph, or encoded named graph ID)
  - `opts` - Options:
    - `:sampling_threshold` - Use sampling for graphs above this size (default: 10000)
    - `:include_object_count` - Include distinct object count (default: true)

  ## Returns

  - `{:ok, summary}` - Map with complete graph statistics
  - `{:error, :not_found}` - If graph doesn't exist
  - `{:error, reason}` - On other failures

  ## Examples

      iex> {:ok, summary} = Statistics.graph_summary(db, 0)
      iex> summary
      %{
        graph_id: 0,
        quad_count: 1000,
        distinct_subjects: 50,
        distinct_predicates: 10,
        distinct_objects: 200,
        predicate_counts: %{42 => 500, 43 => 1500},
        accuracy: :exact
      }
  """
  @spec graph_summary(db_ref(), term_id(), keyword()) :: {:ok, map()} | {:error, term()}
  def graph_summary(db, graph_id, opts \\ []) when is_integer(graph_id) and graph_id >= 0 do
    sampling_threshold = Keyword.get(opts, :sampling_threshold, 10_000)
    include_object_count = Keyword.get(opts, :include_object_count, true)

    with {:ok, quad_count} <- graph_quad_count(db, graph_id),
         :ok <- maybe_check_graph_exists(quad_count, graph_id) do
      # Determine if we should use sampling
      accuracy = if quad_count > sampling_threshold, do: :approximate, else: :exact

      {:ok, distinct_subjects} = graph_distinct_subjects(db, graph_id)

      {:ok, predicate_counts} = graph_predicate_histogram(db, graph_id)
      distinct_predicates = map_size(predicate_counts)

      distinct_objects =
        if include_object_count do
          {:ok, count} = graph_object_count(db, graph_id)
          count
        else
          :not_computed
        end

      summary = %{
        graph_id: graph_id,
        quad_count: quad_count,
        distinct_subjects: distinct_subjects,
        distinct_predicates: distinct_predicates,
        distinct_objects: distinct_objects,
        predicate_counts: predicate_counts,
        accuracy: accuracy
      }

      {:ok, summary}
    end
  end

  @doc """
  Returns a summary across all graphs in the store.

  Aggregates statistics across all graphs including total quad count,
  graph count, largest graph, and per-graph breakdown.

  ## Arguments

  - `db` - Database reference
  - `opts` - Options:
    - `:include_default` - Include default graph (ID 0) in results (default: true)
    - `:include_per_graph` - Include per-graph breakdown (default: true)
    - `:sampling_threshold` - Use sampling for graphs above this size (default: 10000)

  ## Returns

  - `{:ok, summary}` - Map with aggregate statistics
  - `{:error, reason}` - On failure

  ## Examples

      iex> {:ok, summary} = Statistics.all_graphs_summary(db)
      iex> summary
      %{
        total_quads: 5000,
        graph_count: 3,
        largest_graph_id: 123,
        largest_graph_count: 3000,
        per_graph: %{
          0 => %{quad_count: 1000, ...},
          123 => %{quad_count: 3000, ...},
          456 => %{quad_count: 1000, ...}
        }
      }
  """
  @spec all_graphs_summary(db_ref(), keyword()) :: {:ok, map()} | {:error, term()}
  def all_graphs_summary(db, opts \\ []) do
    include_default = Keyword.get(opts, :include_default, true)
    include_per_graph = Keyword.get(opts, :include_per_graph, true)
    sampling_threshold = Keyword.get(opts, :sampling_threshold, 10_000)

    # Get all graph IDs
    {:ok, histograms} = build_per_graph_histograms(db, include_default: include_default)
    graph_ids = Map.keys(histograms)

    # Compute statistics for each graph
    graph_stats =
      Enum.reduce(graph_ids, %{}, fn graph_id, acc ->
        case graph_summary(db, graph_id, sampling_threshold: sampling_threshold) do
          {:ok, summary} ->
            Map.put(acc, graph_id, summary)

          {:error, _} ->
            acc
        end
      end)

    # Find largest graph
    {largest_graph_id, largest_stats} =
      if map_size(graph_stats) > 0 do
        Enum.max_by(graph_stats, fn {_id, stats} -> stats.quad_count end)
      else
        {nil, %{quad_count: 0}}
      end

    largest_graph_count = largest_stats.quad_count

    # Compute aggregates
    total_quads = Enum.reduce(graph_stats, 0, fn {_id, stats}, acc -> acc + stats.quad_count end)
    graph_count = map_size(graph_stats)

    summary = %{
      total_quads: total_quads,
      graph_count: graph_count,
      largest_graph_id: largest_graph_id,
      largest_graph_count: largest_graph_count,
      per_graph: if(include_per_graph, do: graph_stats, else: nil)
    }

    {:ok, summary}
  end

  # ===========================================================================
  # Private Helpers - Distinct Counting
  # ===========================================================================

  @spec count_distinct_by_position(db_ref(), :spo | :pos | :osp) ::
          {:ok, non_neg_integer()}
  defp count_distinct_by_position(db, cf) do
    # prefix_stream now returns the stream directly (may raise on error)
    stream = ErlangAdapter.prefix_stream(db, cf, <<>>)

    count =
      stream
      |> Stream.map(fn {key, _value} -> extract_first_id(key) end)
      |> Stream.dedup()
      |> Enum.count()

    {:ok, count}
  end

  # Extract the first 8-byte ID from a 24-byte index key
  @spec extract_first_id(binary()) :: non_neg_integer()
  defp extract_first_id(<<first_id::64-big, _rest::binary>>) do
    first_id
  end

  # Extract the second 8-byte ID from a 24-byte index key
  @spec extract_second_id(binary()) :: non_neg_integer()
  defp extract_second_id(<<_first::64, second_id::64-big, _rest::binary>>) do
    second_id
  end

  # ===========================================================================
  # Private Helpers - Graph Existence
  # ===========================================================================

  # Checks if a graph exists based on quad count and whether it's the default graph.
  # Returns `:ok` if the graph exists or is the default graph (which always exists).
  # Returns `{:error, :not_found}` if a named graph has no quads.
  # For the default graph (ID 0), we allow empty graphs as valid since it
  # represents the default unnamed graph.
  @spec maybe_check_graph_exists(non_neg_integer(), term_id()) :: :ok | {:error, :not_found}
  # Default graph always exists
  defp maybe_check_graph_exists(_count, 0), do: :ok
  defp maybe_check_graph_exists(count, _graph_id) when count > 0, do: :ok
  defp maybe_check_graph_exists(0, _graph_id), do: {:error, :not_found}

  # ===========================================================================
  # Private Helpers - Numeric Histograms
  # ===========================================================================

  @spec maybe_build_numeric_histograms(db_ref(), map(), pos_integer(), boolean()) ::
          {:ok, map()} | {:error, term()}
  defp maybe_build_numeric_histograms(_db, _predicate_histogram, _bucket_count, false) do
    {:ok, %{}}
  end

  defp maybe_build_numeric_histograms(db, predicate_histogram, bucket_count, true) do
    # Build histograms for predicates that have numeric values
    # We sample the first value to check if a predicate has numeric objects
    predicate_histogram
    |> Map.keys()
    |> Enum.reduce({:ok, %{}}, fn predicate_id, {:ok, acc} ->
      case build_numeric_histogram(db, predicate_id, bucket_count) do
        {:ok, nil} ->
          {:ok, acc}

        {:ok, histogram} ->
          {:ok, Map.put(acc, predicate_id, histogram)}
      end
    end)
  end

  @spec is_numeric_id?(non_neg_integer()) :: boolean()
  defp is_numeric_id?(id) do
    Dictionary.inline_encoded?(id)
  end

  @spec decode_numeric_value(non_neg_integer()) :: float()
  defp decode_numeric_value(id) do
    case Dictionary.decode_inline(id) do
      {:ok, %DateTime{} = dt} ->
        # Convert DateTime to float seconds since epoch
        DateTime.to_unix(dt, :millisecond) / 1000.0

      {:ok, %Decimal{} = d} ->
        Decimal.to_float(d)

      {:ok, int} when is_integer(int) ->
        int * 1.0

      {:error, :not_inline_encoded} ->
        0.0
    end
  end

  @spec build_histogram_from_values(float(), float(), non_neg_integer(), pos_integer()) ::
          {:ok, numeric_histogram()}
  defp build_histogram_from_values(min_val, max_val, total_count, bucket_count) do
    # Handle case where all values are the same
    {adjusted_min, adjusted_max, range} =
      if min_val == max_val do
        {min_val - 0.5, max_val + 0.5, 1.0}
      else
        {min_val, max_val, max_val - min_val}
      end

    bucket_width = range / bucket_count

    histogram = %{
      min: adjusted_min,
      max: adjusted_max,
      bucket_count: bucket_count,
      bucket_width: bucket_width,
      buckets: List.duplicate(0, bucket_count),
      total_count: total_count
    }

    {:ok, histogram}
  end

  # Update buckets with values from a stream (second pass)
  @spec populate_histogram_buckets(numeric_histogram(), Enumerable.t()) :: numeric_histogram()
  defp populate_histogram_buckets(histogram, value_stream) do
    %{min: min_val, bucket_width: bucket_width, bucket_count: bucket_count} = histogram

    # Use an ETS table for efficient bucket updates
    table = :ets.new(:histogram_buckets, [:set, :private])

    # Initialize buckets
    for i <- 0..(bucket_count - 1), do: :ets.insert(table, {i, 0})

    # Stream values and update bucket counts
    value_stream
    |> Stream.each(fn value ->
      bucket_idx = trunc((value - min_val) / bucket_width)
      bucket_idx = min(max(bucket_idx, 0), bucket_count - 1)
      :ets.update_counter(table, bucket_idx, 1)
    end)
    |> Stream.run()

    # Extract bucket counts
    buckets =
      0..(bucket_count - 1)
      |> Enum.map(fn i ->
        [{^i, count}] = :ets.lookup(table, i)
        count
      end)

    :ets.delete(table)

    %{histogram | buckets: buckets}
  end

  @spec estimate_range_from_histogram(numeric_histogram(), number(), number()) :: float()
  defp estimate_range_from_histogram(histogram, min_value, max_value) do
    %{
      min: hist_min,
      max: hist_max,
      bucket_count: bucket_count,
      bucket_width: bucket_width,
      buckets: buckets,
      total_count: total
    } = histogram

    if total == 0 do
      1.0
    else
      # Clamp query range to histogram range
      query_min = max(min_value, hist_min)
      query_max = min(max_value, hist_max)

      if query_min >= query_max do
        0.0
      else
        # Calculate bucket indices
        start_bucket = trunc((query_min - hist_min) / bucket_width)
        end_bucket = trunc((query_max - hist_min) / bucket_width)

        start_bucket = min(max(start_bucket, 0), bucket_count - 1)
        end_bucket = min(max(end_bucket, 0), bucket_count - 1)

        # Sum counts in range (with fractional bucket handling)
        count =
          start_bucket..end_bucket
          |> Enum.reduce(0.0, fn bucket_idx, acc ->
            bucket_count_val = Enum.at(buckets, bucket_idx, 0)

            # Calculate fraction of bucket in range
            bucket_start = hist_min + bucket_idx * bucket_width
            bucket_end = bucket_start + bucket_width

            overlap_start = max(query_min, bucket_start)
            overlap_end = min(query_max, bucket_end)
            overlap_fraction = (overlap_end - overlap_start) / bucket_width

            acc + bucket_count_val * overlap_fraction
          end)

        # Return selectivity as fraction of total
        min(count / total, 1.0)
      end
    end
  end

  # ===========================================================================
  # Private Helpers - Version Migration (S13)
  # ===========================================================================

  @doc false
  @spec migrate_stats_if_needed(stats()) :: stats()
  def migrate_stats_if_needed(%{version: @stats_version} = stats), do: stats

  def migrate_stats_if_needed(%{version: old_version} = stats)
      when old_version < @stats_version do
    Logger.info("Migrating statistics from version #{old_version} to #{@stats_version}")

    stats
    |> migrate_to_v1()
    |> Map.put(:version, @stats_version)
  end

  # Fallback for stats without version field (pre-v1)
  def migrate_stats_if_needed(stats) when is_map(stats) do
    stats
    |> Map.put_new(:version, 0)
    |> migrate_stats_if_needed()
  end

  defp migrate_to_v1(stats) do
    # Add bucket_width to numeric histograms if missing
    numeric_histograms =
      Map.get(stats, :numeric_histograms, %{})
      |> Enum.map(fn {pred_id, histogram} ->
        histogram = maybe_add_bucket_width(histogram)
        {pred_id, histogram}
      end)
      |> Map.new()

    %{stats | numeric_histograms: numeric_histograms}
  end

  defp maybe_add_bucket_width(%{bucket_width: _} = histogram), do: histogram

  defp maybe_add_bucket_width(%{min: min_val, max: max_val, bucket_count: count} = histogram) do
    range = max_val - min_val
    bucket_width = if range > 0, do: range / count, else: 1.0
    Map.put(histogram, :bucket_width, bucket_width)
  end

  defp maybe_add_bucket_width(histogram), do: histogram

  # ===========================================================================
  # Private Helpers - Quad Cache Keys
  # ===========================================================================

  @doc """
  Generates a cache key for graph-specific quad statistics.

  Uses the quad stats prefix combined with graph_id to create
  a unique cache key that won't collide with triple statistics.

  ## Examples

      iex> Statistics.quad_cache_key(0)
      {<<0, 0, 0, 0, 0, 0, 0, 2>>, 0}

      iex> Statistics.quad_cache_key(123)
      {<<0, 0, 0, 0, 0, 0, 0, 2>>, 123}
  """
  @spec quad_cache_key(term_id()) :: {binary(), term_id()}
  def quad_cache_key(graph_id) when is_integer(graph_id) and graph_id >= 0 do
    {@quad_stats_prefix, graph_id}
  end

  @doc """
  Generates a cache key for the all-graphs summary.

  Uses a special marker to distinguish from graph-specific keys.

  ## Examples

      iex> Statistics.all_graphs_cache_key()
      {<<0, 0, 0, 0, 0, 0, 0, 2>>, :all_graphs}
  """
  @spec all_graphs_cache_key() :: {binary(), :all_graphs}
  def all_graphs_cache_key do
    {@quad_stats_prefix, :all_graphs}
  end

  @doc """
  Computes and caches graph statistics.

  Computes fresh statistics for the given graph and stores them
  in the ETS cache.

  ## Arguments

  - `db` - Database reference
  - `graph_id` - The graph ID

  ## Returns

  - `{:ok, stats}` - Computed statistics map
  - `{:error, reason}` - On failure
  """
  @spec compute_and_cache_graph_stats(db_ref(), term_id()) ::
          {:ok, graph_stats()} | {:error, term()}
  def compute_and_cache_graph_stats(db, graph_id) when is_integer(graph_id) and graph_id >= 0 do
    case graph_summary(db, graph_id) do
      {:ok, summary} = result ->
        # Cache the result using insert_new to prevent race conditions
        cache_key = quad_cache_key(graph_id)
        :ets.insert_new(@quad_cache_table, {cache_key, summary})
        result

      error ->
        error
    end
  end
end
