defmodule TripleStore.Backend.RocksDB.ColumnFamilyConfig do
  @moduledoc """
  Column Family configuration for erlang-rocksdb.

  This module defines the column family options that match the current
  Rust NIF tuning settings, ensuring data compatibility and optimal performance.

  ## Column Families

  The TripleStore uses the following column families:

  | CF Name | Purpose | Access Pattern |
  |---------|---------|----------------|
  | `id2str` | ID → Term mapping | Random point lookups |
  | `str2id` | Term → ID mapping | Random point lookups |
  | `spo` | Subject-Predicate-Object index | Prefix scans (subject, subject-predicate) |
  | `pos` | Predicate-Object-Subject index | Prefix scans (predicate, predicate-object) |
  | `osp` | Object-Subject-Predicate index | Prefix scans (object, object-subject) |
  | `derived` | Inferred triples from reasoning | Sequential bulk writes, batch scans |
  | `numeric_range` | Numeric range indices | Range queries |

  ## Configuration Strategy

  ### Dictionary CFs (id2str, str2id)
  - **High bloom filter** (14 bits/key) for effective point lookup filtering
  - **Small block size** (2KB) for better cache utilization
  - **Pinned L0 filter/index** in cache for hot dictionary data
  - **No prefix extractor** (random access pattern)

  ### Index CFs (spo, pos, osp)
  - **Medium bloom filter** (12 bits/key) for prefix scan filtering
  - **Medium block size** (8KB) for sequential scan efficiency
  - **Prefix extractor** (8 bytes) for optimized prefix-based scans
  - **Memtable prefix bloom** for efficient in-memory prefix filtering

  ### Derived CF
  - **No bloom filter** (sequential bulk access)
  - **Large block size** (32KB) for sequential scan efficiency
  - **No cache pinning** (sequential access pattern)

  ## Usage

  ```elixir
  # Get all column family descriptors for opening a database
  cf_descriptors = ColumnFamilyConfig.cf_descriptors()

  # Open database with column families
  {:ok, db, cf_handles} = :rocksdb.open_with_cf(path, db_opts, cf_descriptors)
  ```

  ## Compression

  All column families use:
  - L0: `none` (uncompressed for fast memtable flush)
  - L1-L6: `lz4` (fast compression for lower levels)

  This matches the Rust NIF configuration for optimal performance.
  """

  @type column_family :: :id2str | :str2id | :spo | :pos | :osp | :derived | :numeric_range
  @type cf_descriptor :: {String.t(), [:rocksdb.cf_options()]}
  @type db_options :: [:rocksdb.db_options()]

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Bloom filter bits per key
  @bloom_dict_bits 14  # Dictionary CFs: high precision for point lookups
  @bloom_index_bits 12 # Index CFs: balanced for prefix scans
  @bloom_derived_bits 0 # Derived CF: no bloom filter (sequential access)

  # Block sizes
  @block_size_dict 2 * 1024      # 2KB for dictionary (small blocks, better cache)
  @block_size_index 8 * 1024     # 8KB for indices (balanced)
  @block_size_derived 32 * 1024  # 32KB for derived (large blocks, sequential)

  # Prefix extractor length (for index CFs)
  # First 8 bytes = subject/predicate/object ID (64 bits)
  @prefix_extractor_bytes 8

  # Compression settings
  @compression_l0 :none
  @compression_l1_l6 :lz4

  # Cache settings
  @block_cache_size_mb 512  # Shared block cache size

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Returns all column family descriptors for database opening.

  Each descriptor is a tuple `{cf_name, cf_options}` suitable for
  `:rocksdb.open_with_cf/3`.

  ## Returns

  List of column family descriptors.

  ## Examples

      iex> length(ColumnFamilyConfig.cf_descriptors())
      7

  """
  @spec cf_descriptors() :: [cf_descriptor()]
  def cf_descriptors do
    [
      {"default", default_cf_options()},
      {"id2str", dictionary_cf_options()},
      {"str2id", dictionary_cf_options()},
      {"spo", index_cf_options()},
      {"pos", index_cf_options()},
      {"osp", index_cf_options()},
      {"derived", derived_cf_options()},
      {"numeric_range", numeric_range_cf_options()}
    ]
  end

  @doc """
  Returns database options for opening a new database.

  These options configure global database settings including
  the shared block cache.

  ## Returns

  List of database options for `:rocksdb.open_with_cf/3`.

  """
  @spec db_options() :: db_options()
  def db_options do
    [
      create_if_missing: true,
      # Configure shared block cache
      # Using a single shared cache across all CFs for better memory utilization
      #{create_if_missing: true, capacity: {@block_cache_size_mb * 1024 * 1024, strict_capacity_limit: false}}
    ]
  end

  @doc """
  Returns column family names in the order expected by the database.

  This order matches the handles returned by `:rocksdb.open_with_cf/3`.

  ## Returns

  List of column family names.

  """
  @spec column_family_names() :: [String.t()]
  def column_family_names do
    ["default", "id2str", "str2id", "spo", "pos", "osp", "derived", "numeric_range"]
  end

  @doc """
  Gets the configuration for a specific column family.

  ## Parameters

  - `cf`: Column family atom (`:id2str`, `:spo`, etc.)

  ## Returns

  List of column family options, or `nil` if CF is unknown.

  """
  @spec get_cf_options(column_family()) :: [:rocksdb.cf_options()] | nil
  def get_cf_options(:id2str), do: dictionary_cf_options()
  def get_cf_options(:str2id), do: dictionary_cf_options()
  def get_cf_options(:spo), do: index_cf_options()
  def get_cf_options(:pos), do: index_cf_options()
  def get_cf_options(:osp), do: index_cf_options()
  def get_cf_options(:derived), do: derived_cf_options()
  def get_cf_options(:numeric_range), do: numeric_range_cf_options()
  def get_cf_options(_), do: nil

  # ===========================================================================
  # Column Family Options
  # ===========================================================================

  # Default column family options
  defp default_cf_options do
    dictionary_cf_options()
  end

  # Dictionary CF options (id2str, str2id)
  # Optimized for random point lookups with high cache hit rates
  defp dictionary_cf_options do
    base_options()
    |> Keyword.merge(
      # High bloom filter for effective point lookup filtering
      block_based_table_options: [
        bloom_filter_policy: @bloom_dict_bits,
        block_size: @block_size_dict,
        # Cache index and filter blocks for faster lookups
        cache_index_and_filter_blocks: true,
        # Pin L0 filter/index blocks in cache (hot data)
        pin_l0_filter_and_index_blocks_in_cache: true,
        # Whole key filtering for dictionary (exact match lookups)
        whole_key_filtering: true
      ],
      # No prefix extractor needed (random access pattern)
      # Compression: L0 none, L1-L6 lz4
      compression: @compression_l1_l6,
      bottommost_compression: @compression_l1_l6
    )
  end

  # Index CF options (spo, pos, osp)
  # Optimized for prefix-based scans with efficient in-memory filtering
  defp index_cf_options do
    base_options()
    |> Keyword.merge(
      # Medium bloom filter for prefix scan filtering
      block_based_table_options: [
        bloom_filter_policy: @bloom_index_bits,
        block_size: @block_size_index,
        cache_index_and_filter_blocks: true,
        # Don't pin L0 blocks (sequential scan pattern)
        pin_l0_filter_and_index_blocks_in_cache: false,
        # Use prefix-based bloom filtering
        whole_key_filtering: false
      ],
      # Note: prefix_extractor configuration format is version-dependent
      # The erlang-rocksdb library may accept different formats like:
      # - {:prefix_extractor, "fixed:8"} (string format)
      # - {:prefix_extractor, {:fixed, 8}} (tuple format)
      # For now, we skip this option and will add it during adapter implementation
      # prefix_extractor: {"fixed.prefix", @prefix_extractor_bytes},
      # Memtable prefix bloom for efficient in-memory filtering
      memtable_prefix_bloom_size_ratio: 0.1,
      # Compression
      compression: @compression_l1_l6,
      bottommost_compression: @compression_l1_l6
    )
  end

  # Derived CF options
  # Optimized for sequential bulk writes and scans
  defp derived_cf_options do
    base_options()
    |> Keyword.merge(
      # No bloom filter (sequential access pattern)
      block_based_table_options: [
        bloom_filter_policy: @bloom_derived_bits,
        block_size: @block_size_derived,
        # Don't cache index/filter blocks (sequential access)
        cache_index_and_filter_blocks: false,
        pin_l0_filter_and_index_blocks_in_cache: false
      ],
      # No prefix extractor (full scans)
      # Compression
      compression: @compression_l1_l6,
      bottommost_compression: @compression_l1_l6
    )
  end

  # Numeric range CF options
  # Optimized for range queries on numeric values
  defp numeric_range_cf_options do
    base_options()
    |> Keyword.merge(
      # Medium bloom filter for range queries
      block_based_table_options: [
        bloom_filter_policy: @bloom_index_bits,
        block_size: @block_size_index,
        cache_index_and_filter_blocks: true,
        pin_l0_filter_and_index_blocks_in_cache: false
      ],
      # No prefix extractor (range queries)
      # Compression
      compression: @compression_l1_l6,
      bottommost_compression: @compression_l1_l6
    )
  end

  # Base options shared by all column families
  defp base_options do
    [
      # Disable write-ahead log sync for faster writes (durability handled by WAL fsync)
      # For ACID compliance, use sync: true in write operations
      # Write buffer size (memtable size)
      write_buffer_size: 64 * 1024 * 1024, # 64MB memtable
      # Maximum number of write buffers (memtables)
      max_write_buffer_number: 3,
      # Minimum number of write buffers to flush
      min_write_buffer_number_to_merge: 1,
      # Level 0 file size limit
      target_file_size_base: 64 * 1024 * 1024, # 64MB L0 files
      # Level 0 compaction trigger
      level0_file_num_compaction_trigger: 4,
      # Level 0 slowdown trigger
      level0_slowdown_writes_trigger: 8,
      # Level 0 stop trigger
      level0_stop_writes_trigger: 12,
      # Max bytes for each level
      max_bytes_for_level_base: 256 * 1024 * 1024, # 256MB for L1
      # Multiplier for each level's size
      max_bytes_for_level_multiplier: 10
    ]
  end

  # ===========================================================================
  # Utility Functions
  # ===========================================================================

  @doc """
  Converts a column family atom to its string name.

  ## Examples

      iex> ColumnFamilyConfig.cf_name_to_string(:id2str)
      "id2str"

  """
  @spec cf_name_to_string(column_family()) :: String.t()
  def cf_name_to_string(:id2str), do: "id2str"
  def cf_name_to_string(:str2id), do: "str2id"
  def cf_name_to_string(:spo), do: "spo"
  def cf_name_to_string(:pos), do: "pos"
  def cf_name_to_string(:osp), do: "osp"
  def cf_name_to_string(:derived), do: "derived"
  def cf_name_to_string(:numeric_range), do: "numeric_range"

  @doc """
  Converts a string column family name to its atom equivalent.

  ## Examples

      iex> ColumnFamilyConfig.cf_string_to_name("id2str")
      :id2str

  """
  @spec cf_string_to_name(String.t()) :: column_family() | nil
  def cf_string_to_name("id2str"), do: :id2str
  def cf_string_to_name("str2id"), do: :str2id
  def cf_string_to_name("spo"), do: :spo
  def cf_string_to_name("pos"), do: :pos
  def cf_string_to_name("osp"), do: :osp
  def cf_string_to_name("derived"), do: :derived
  def cf_string_to_name("numeric_range"), do: :numeric_range
  def cf_string_to_name(_), do: nil

  @doc """
  Returns the bloom filter bits per key for a column family.

  ## Examples

      iex> ColumnFamilyConfig.bloom_bits(:id2str)
      14

  """
  @spec bloom_bits(column_family()) :: non_neg_integer()
  def bloom_bits(:id2str), do: @bloom_dict_bits
  def bloom_bits(:str2id), do: @bloom_dict_bits
  def bloom_bits(:spo), do: @bloom_index_bits
  def bloom_bits(:pos), do: @bloom_index_bits
  def bloom_bits(:osp), do: @bloom_index_bits
  def bloom_bits(:derived), do: @bloom_derived_bits
  def bloom_bits(:numeric_range), do: @bloom_index_bits

  @doc """
  Returns the block size for a column family in bytes.

  ## Examples

      iex> ColumnFamilyConfig.block_size(:id2str)
      2048

  """
  @spec block_size(column_family()) :: pos_integer()
  def block_size(:id2str), do: @block_size_dict
  def block_size(:str2id), do: @block_size_dict
  def block_size(:spo), do: @block_size_index
  def block_size(:pos), do: @block_size_index
  def block_size(:osp), do: @block_size_index
  def block_size(:derived), do: @block_size_derived
  def block_size(:numeric_range), do: @block_size_index

  @doc """
  Checks if a column family has a prefix extractor configured.

  Note: Currently returns false for all CFs as prefix_extractor configuration
  is deferred to adapter implementation. The prefix extractor format is
  version-dependent in erlang-rocksdb and will be configured during
  the actual adapter implementation.

  ## Examples

      iex> ColumnFamilyConfig.has_prefix_extractor?(:spo)
      false

      iex> ColumnFamilyConfig.has_prefix_extractor?(:id2str)
      false

  """
  @spec has_prefix_extractor?(column_family()) :: boolean()
  def has_prefix_extractor?(_), do: false
end
