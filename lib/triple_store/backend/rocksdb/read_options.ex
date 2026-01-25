defmodule TripleStore.Backend.RocksDB.ReadOptions do
  @moduledoc """
  Read options presets for different query patterns.

  This module provides optimized read option configurations for various
  query patterns used by the TripleStore. Using appropriate read options
  improves performance by tuning RocksDB behavior to the access pattern.

  ## Read Option Presets

  | Preset | Use Case | Description |
  |--------|----------|-------------|
  | `default/0` | General queries | Balanced settings for most queries |
  | `point_lookup/0` | Dictionary lookups | Optimized for single key lookups |
  | `prefix_scan/0` | Index prefix scans | Optimized for prefix iteration |
  | `quad_prefix_scan/0` | Quad graph-scoped queries | Optimized for GSPO/GPOS scans |
  | `cross_graph_scan/0` | Quad cross-graph queries | Optimized for SPOG/POSG scans |
  | `full_scan/0` | Bulk scans | Optimized for full table scans |
  | `cached_scan/0` | Repeated queries | Maximizes cache usage |
  | `uncached_scan/0` | Large one-time scans | Bypasses cache to avoid pollution |

  ## Usage

  ```elixir
  # For dictionary lookups (high cache value)
  opts = ReadOptions.point_lookup()
  {:ok, value} = ErlangAdapter.get(db, :id2str, key, opts)

  # For prefix scans over indices
  opts = ReadOptions.prefix_scan()
  {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spo, prefix, opts)

  # For graph-scoped quad queries (GSPO/GPOS)
  opts = ReadOptions.quad_prefix_scan()
  {:ok, iter} = ErlangAdapter.prefix_iterator(db, :gspo, prefix, opts)

  # For cross-graph quad queries (SPOG/POSG)
  opts = ReadOptions.cross_graph_scan()
  {:ok, iter} = ErlangAdapter.prefix_iterator(db, :spog, prefix, opts)

  # For large bulk scans that shouldn't pollute cache
  opts = ReadOptions.uncached_scan()
  stream = ErlangAdapter.prefix_stream(db, :spo, prefix, opts)
  ```

  ## Option Details

  ### `fill_cache`
  - `true`: Data read is added to block cache (default for most queries)
  - `false`: Bypass cache for large one-time scans

  ### `iterate_upper_bound`
  - Limits iteration to keys < this value
  - Used to bound range scans to specific prefixes

  ### `total_order_seek`
  - `true`: Seek over entire database (slower)
  - `false`: Use prefix-based seeks (faster for prefix scans)

  ### `prefix_same_as_start`
  - `true`: Optimize for keys with same prefix
  - `false`: General iteration (default)

  ### `tailing`
  - `true`: Iterator can read future updates
  - `false`: Iterator sees snapshot only (default)

  ## Quad Store Read Strategy

  Quad stores (schema v2) use 32-byte keys with four indices:

  | Index | Key Ordering | Read Preset | Use Case |
  |-------|--------------|-------------|----------|
  | `gspo` | Graph-Subject-Predicate-Object | `quad_prefix_scan/0` | All quads in specific graph |
  | `gpos` | Graph-Predicate-Object-Subject | `quad_prefix_scan/0` | All predicates in specific graph |
  | `spog` | Subject-Predicate-Object-Graph | `cross_graph_scan/0` | Subject-scoped queries across graphs |
  | `posg` | Predicate-Object-Subject-Graph | `cross_graph_scan/0` | Predicate-scoped queries across graphs |

  The quad-specific presets optimize for the different access patterns:
  - `quad_prefix_scan/0`: For graph-scoped queries (prefix on graph ID)
  - `cross_graph_scan/0`: For cross-graph queries (prefix on subject/predicate)
  """

  @type read_option ::
          {:fill_cache, boolean()}
          | {:iterate_upper_bound, binary()}
          | {:total_order_seek, boolean()}
          | {:prefix_same_as_start, boolean()}
          | {:tailing, boolean()}
          | {:snapshot, reference()}

  @type read_options :: [read_option()]

  # ===========================================================================
  # Public API - Presets
  # ===========================================================================

  @doc """
  Default read options for most queries.

  Balanced settings with cache enabled for good general performance.

  ## Returns

  Read options keyword list.

  ## Examples

      iex> ReadOptions.default() |> Keyword.take([:fill_cache, :total_order_seek, :prefix_same_as_start])
      [fill_cache: true, total_order_seek: false, prefix_same_as_start: false]

  """
  @spec default() :: read_options()
  def default do
    [
      fill_cache: true,
      total_order_seek: false,
      prefix_same_as_start: false
    ]
  end

  @doc """
  Read options optimized for point lookups.

  Used for dictionary lookups where a single key is retrieved.
  Enables caching and uses total order seek for exact key access.

  ## Returns

  Read options keyword list.

  ## Examples

      iex> opts = ReadOptions.point_lookup()
      iex> Keyword.get(opts, :fill_cache)
      true

  """
  @spec point_lookup() :: read_options()
  def point_lookup do
    [
      fill_cache: true,
      total_order_seek: true,
      prefix_same_as_start: false
    ]
  end

  @doc """
  Read options optimized for prefix scans.

  Used for index queries where we scan all keys with a given prefix.
  Disables total order seek for faster prefix-based iteration.

  ## Returns

  Read options keyword list.

  ## Examples

      iex> opts = ReadOptions.prefix_scan()
      iex> Keyword.get(opts, :fill_cache)
      true
      iex> Keyword.get(opts, :total_order_seek)
      false

  """
  @spec prefix_scan() :: read_options()
  def prefix_scan do
    [
      fill_cache: true,
      total_order_seek: false,
      prefix_same_as_start: true
    ]
  end

  @doc """
  Read options optimized for quad graph-scoped queries.

  Used for GSPO and GPOS index queries where we scan all quads within
  a specific graph. The graph ID is the first component of the key,
  making prefix scans very efficient.

  ## Returns

  Read options keyword list.

  ## Examples

      iex> opts = ReadOptions.quad_prefix_scan()
      iex> Keyword.get(opts, :fill_cache)
      true
      iex> Keyword.get(opts, :total_order_seek)
      false
      iex> Keyword.get(opts, :prefix_same_as_start)
      true

  """
  @spec quad_prefix_scan() :: read_options()
  def quad_prefix_scan do
    [
      fill_cache: true,
      total_order_seek: false,
      prefix_same_as_start: true
    ]
  end

  @doc """
  Read options optimized for quad cross-graph queries.

  Used for SPOG and POSG index queries where we scan across multiple graphs
  based on subject or predicate prefix. These queries access data from
  all graphs matching a subject or predicate pattern.

  ## Returns

  Read options keyword list.

  ## Examples

      iex> opts = ReadOptions.cross_graph_scan()
      iex> Keyword.get(opts, :fill_cache)
      true
      iex> Keyword.get(opts, :total_order_seek)
      false
      iex> Keyword.get(opts, :prefix_same_as_start)
      true

  """
  @spec cross_graph_scan() :: read_options()
  def cross_graph_scan do
    [
      fill_cache: true,
      total_order_seek: false,
      prefix_same_as_start: true
    ]
  end

  @doc """
  Read options optimized for full table scans.

  Used when scanning an entire column family. Disables cache to avoid
  polluting the block cache with scan results.

  ## Returns

  Read options keyword list.

  ## Examples

      iex> opts = ReadOptions.full_scan()
      iex> Keyword.get(opts, :fill_cache)
      false

  """
  @spec full_scan() :: read_options()
  def full_scan do
    [
      fill_cache: false,
      total_order_seek: false,
      prefix_same_as_start: false
    ]
  end

  @doc """
  Read options that maximize cache usage.

  Used for frequently accessed data or repeated queries.

  ## Returns

  Read options keyword list.

  ## Examples

      iex> opts = ReadOptions.cached_scan()
      iex> Keyword.get(opts, :fill_cache)
      true

  """
  @spec cached_scan() :: read_options()
  def cached_scan do
    [
      fill_cache: true,
      total_order_seek: false,
      prefix_same_as_start: true
    ]
  end

  @doc """
  Read options that bypass cache to avoid pollution.

  Used for large one-time scans where caching would waste memory.

  ## Returns

  Read options keyword list.

  ## Examples

      iex> opts = ReadOptions.uncached_scan()
      iex> Keyword.get(opts, :fill_cache)
      false

  """
  @spec uncached_scan() :: read_options()
  def uncached_scan do
    [
      fill_cache: false,
      total_order_seek: false,
      prefix_same_as_start: false
    ]
  end

  # ===========================================================================
  # Public API - Builders
  # ===========================================================================

  @doc """
  Creates read options with an upper bound for iteration.

  Useful for limiting prefix scans to a specific range.

  ## Parameters

  - `upper_bound`: Binary key that represents the exclusive upper bound

  ## Returns

  Read options keyword list with iterate_upper_bound set.

  ## Examples

      iex> opts = ReadOptions.with_upper_bound(<<123, 255>>)
      iex> Keyword.get(opts, :iterate_upper_bound)
      <<123, 255>>

  """
  @spec with_upper_bound(binary()) :: read_options()
  def with_upper_bound(upper_bound) when is_binary(upper_bound) do
    [
      iterate_upper_bound: upper_bound,
      fill_cache: true,
      total_order_seek: false,
      prefix_same_as_start: true
    ]
  end

  @doc """
  Creates read options from a snapshot reference.

  ## Parameters

  - `snapshot_ref`: Snapshot reference from `ErlangAdapter.snapshot/1`

  ## Returns

  Read options keyword list with snapshot set.

  ## Examples

      iex> {:ok, snapshot} = ErlangAdapter.snapshot(db)
      iex> opts = ReadOptions.from_snapshot(snapshot)
      iex> Keyword.has_key?(opts, :snapshot)
      true

  """
  @spec from_snapshot(reference()) :: read_options()
  def from_snapshot(snapshot_ref) when is_reference(snapshot_ref) do
    [
      snapshot: snapshot_ref,
      fill_cache: true,
      total_order_seek: false,
      prefix_same_as_start: false
    ]
  end

  @doc """
  Merges custom options into a preset.

  ## Parameters

  - `preset`: Base preset (function from this module)
  - `custom_opts`: Custom options to override/add

  ## Returns

  Combined read options keyword list.

  ## Examples

      iex> opts = ReadOptions.merge(ReadOptions.prefix_scan(), fill_cache: false)
      iex> Keyword.get(opts, :fill_cache)
      false

  """
  @spec merge(read_options(), keyword()) :: read_options()
  def merge(preset, custom_opts) when is_list(preset) and is_list(custom_opts) do
    Keyword.merge(preset, custom_opts)
  end

  # ===========================================================================
  # Public API - Utilities
  # ===========================================================================

  @doc """
  Determines if cache should be filled for a given operation type.

  ## Parameters

  - `operation_type`: Atom indicating the type of operation

  ## Returns

  Boolean indicating if cache should be used.

  ## Examples

      iex> ReadOptions.use_cache?(:point_lookup)
      true

      iex> ReadOptions.use_cache?(:bulk_export)
      false

  """
  @spec use_cache?(atom()) :: boolean()
  def use_cache?(:point_lookup), do: true
  def use_cache?(:prefix_scan), do: true
  def use_cache?(:repeated_query), do: true
  def use_cache?(:bulk_export), do: false
  def use_cache?(:full_scan), do: false
  def use_cache?(:maintenance), do: false
  def use_cache?(_), do: true

  @doc """
  Gets the appropriate read preset for a column family.

  ## Parameters

  - `cf`: Column family atom

  ## Returns

  Read options keyword list optimized for the column family.

  ## Examples

      iex> opts = ReadOptions.for_cf(:id2str)
      iex> Keyword.get(opts, :total_order_seek)
      true

      iex> opts = ReadOptions.for_cf(:spo)
      iex> Keyword.get(opts, :total_order_seek)
      false

      iex> opts = ReadOptions.for_cf(:gspo)
      iex> Keyword.get(opts, :prefix_same_as_start)
      true

      iex> opts = ReadOptions.for_cf(:spog)
      iex> Keyword.get(opts, :prefix_same_as_start)
      true

  """
  @spec for_cf(TripleStore.Backend.RocksDB.ColumnFamilyConfig.column_family()) ::
          read_options()
  def for_cf(:id2str), do: point_lookup()
  def for_cf(:str2id), do: point_lookup()
  # Triple store indices
  def for_cf(:spo), do: prefix_scan()
  def for_cf(:pos), do: prefix_scan()
  def for_cf(:osp), do: prefix_scan()
  # Quad store indices (schema v2)
  def for_cf(:gspo), do: quad_prefix_scan()
  def for_cf(:gpos), do: quad_prefix_scan()
  def for_cf(:spog), do: cross_graph_scan()
  def for_cf(:posg), do: cross_graph_scan()
  # Other column families
  def for_cf(:derived), do: full_scan()
  def for_cf(:numeric_range), do: prefix_scan()
  def for_cf(:default), do: default()
end
