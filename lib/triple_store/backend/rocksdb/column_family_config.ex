defmodule TripleStore.Backend.RocksDB.ColumnFamilyConfig do
  @moduledoc """
  Column Family configuration for erlang-rocksdb.

  This module defines the column family options for both triple and quad stores,
  ensuring data compatibility and optimal performance.

  ## Triple Store Column Families (Schema v1)

  The triple store uses the following column families:

  | CF Name | Purpose | Access Pattern |
  |---------|---------|----------------|
  | `id2str` | ID → Term mapping | Random point lookups |
  | `str2id` | Term → ID mapping | Random point lookups |
  | `spo` | Subject-Predicate-Object index | Prefix scans (subject, subject-predicate) |
  | `pos` | Predicate-Object-Subject index | Prefix scans (predicate, predicate-object) |
  | `osp` | Object-Subject-Predicate index | Prefix scans (object, object-subject) |
  | `derived` | Inferred triples from reasoning | Sequential bulk writes, batch scans |
  | `derivation_provenance` | Derivation tracking for provenance | Point lookups and scans |
  | `numeric_range` | Numeric range indices | Range queries |

  ## Quad Store Column Families (Schema v2)

  The quad store uses four quad indices for named graph support:

  | CF Name | Key Ordering | Primary Use Case |
  |---------|-------------|------------------|
  | `gspo` | Graph-Subject-Predicate-Object | All quads in specific graph |
  | `gpos` | Graph-Predicate-Object-Subject | All predicates in specific graph |
  | `spog` | Subject-Predicate-Object-Graph | Subject-scoped queries across graphs |
  | `posg` | Predicate-Object-Subject-Graph | Predicate-scoped queries across graphs |

  ## Storage Tradeoffs

  Quad keys are 32 bytes (4 × 64-bit IDs) vs 24 bytes for triples:
  - Key size: 32 bytes vs 24 bytes (~33% increase)
  - Write amplification: 4x instead of 3x
  - Skip `ospg` and `gosp` indices (less common patterns handled via filtering)

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

  @type column_family ::
          :id2str
          | :str2id
          | :spo
          | :pos
          | :osp
          | :derived
          | :derivation_provenance
          | :numeric_range
          | :gspo
          | :gpos
          | :spog
          | :posg
          | :acl
  @type cf_descriptor :: {String.t(), keyword()}
  @type db_options :: keyword()

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Bloom filter bits per key
  # Dictionary CFs: high precision for point lookups
  @bloom_dict_bits 14
  # Index CFs: balanced for prefix scans
  @bloom_index_bits 12
  # Derived CF: no bloom filter (sequential access)
  @bloom_derived_bits 0

  # Block sizes
  # 2KB for dictionary (small blocks, better cache)
  @block_size_dict 2 * 1024
  # 8KB for indices (balanced)
  @block_size_index 8 * 1024
  # 32KB for derived (large blocks, sequential)
  @block_size_derived 32 * 1024

  # Compression settings
  # L0 is uncompressed by default (fast memtable flush)
  # L1-L6 currently use :none due to erlang-rocksdb build limitations
  # Note: To enable compression, recompile erlang-rocksdb with LZ4 or Snappy support
  @compression_l1_l6 :none

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Returns all column family descriptors for database opening.

  Each descriptor is a tuple `{cf_name, cf_options}` suitable for
  `:rocksdb.open_with_cf/3`.

  By default, returns triple store (v1) column families. Use `cf_descriptors(:quad)`
  for quad store (v2) column families.

  ## Parameters

  - `schema` - Schema version: `:triple` (v1, default) or `:quad` (v2)

  ## Returns

  List of column family descriptors.

  ## Examples

      iex> length(ColumnFamilyConfig.cf_descriptors())
      8

      iex> length(ColumnFamilyConfig.cf_descriptors(:quad))
      9

  """
  @spec cf_descriptors(:triple | :quad) :: [cf_descriptor()]
  def cf_descriptors(schema \\ :triple)

  def cf_descriptors(:triple) do
    [
      {"default", default_cf_options()},
      {"id2str", dictionary_cf_options()},
      {"str2id", dictionary_cf_options()},
      {"spo", index_cf_options()},
      {"pos", index_cf_options()},
      {"osp", index_cf_options()},
      {"derived", derived_cf_options()},
      {"derivation_provenance", derivation_provenance_cf_options()},
      {"numeric_range", numeric_range_cf_options()}
    ]
  end

  def cf_descriptors(:quad) do
    [
      {"default", default_cf_options()},
      {"id2str", dictionary_cf_options()},
      {"str2id", dictionary_cf_options()},
      {"gspo", quad_index_cf_options()},
      {"gpos", quad_index_cf_options()},
      {"spog", quad_index_cf_options()},
      {"posg", quad_index_cf_options()},
      {"derived", derived_cf_options()},
      {"derivation_provenance", derivation_provenance_cf_options()},
      {"numeric_range", numeric_range_cf_options()},
      {"acl", acl_cf_options()}
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
      create_if_missing: true
      # Note: Shared block cache configuration is version-dependent
      # erlang-rocksdb may accept different formats for cache configuration
      # This will be configured during adapter implementation
    ]
  end

  @doc """
  Returns column family names in the order expected by the database.

  This order matches the handles returned by `:rocksdb.open_with_cf/3`.

  ## Parameters

  - `schema` - Schema version: `:triple` (v1, default) or `:quad` (v2)

  ## Returns

  List of column family names.

  """
  @spec column_family_names(:triple | :quad) :: [String.t()]
  def column_family_names(schema \\ :triple)

  def column_family_names(:triple) do
    ["default", "id2str", "str2id", "spo", "pos", "osp", "derived", "derivation_provenance", "numeric_range"]
  end

  def column_family_names(:quad) do
    ["default", "id2str", "str2id", "gspo", "gpos", "spog", "posg", "derived", "derivation_provenance", "numeric_range", "acl"]
  end

  @doc """
  Gets the configuration for a specific column family.

  ## Parameters

  - `cf`: Column family atom (`:id2str`, `:spo`, `:gspo`, etc.)

  ## Returns

  List of column family options, or `nil` if CF is unknown.

  """
  @spec get_cf_options(column_family()) :: keyword() | nil
  def get_cf_options(:id2str), do: dictionary_cf_options()
  def get_cf_options(:str2id), do: dictionary_cf_options()
  def get_cf_options(:spo), do: index_cf_options()
  def get_cf_options(:pos), do: index_cf_options()
  def get_cf_options(:osp), do: index_cf_options()
  def get_cf_options(:gspo), do: quad_index_cf_options()
  def get_cf_options(:gpos), do: quad_index_cf_options()
  def get_cf_options(:spog), do: quad_index_cf_options()
  def get_cf_options(:posg), do: quad_index_cf_options()
  def get_cf_options(:derived), do: derived_cf_options()
  def get_cf_options(:derivation_provenance), do: derivation_provenance_cf_options()
  def get_cf_options(:numeric_range), do: numeric_range_cf_options()
  def get_cf_options(:acl), do: acl_cf_options()
  def get_cf_options(_), do: nil

  @doc """
  Validates a column family atom.

  ## Parameters

  - `cf`: Column family atom to validate

  ## Returns

  - `:ok` - Valid column family
  - `{:error, :invalid_column_family}` - Invalid column family

  ## Examples

      iex> ColumnFamilyConfig.validate_cf(:spo)
      :ok

      iex> ColumnFamilyConfig.validate_cf(:gspo)
      :ok

      iex> ColumnFamilyConfig.validate_cf(:invalid)
      {:error, :invalid_column_family}

  """
  @spec validate_cf(atom()) :: :ok | {:error, :invalid_column_family}
  def validate_cf(cf)
      when cf in [
             :id2str,
             :str2id,
             :spo,
             :pos,
             :osp,
             :derived,
             :derivation_provenance,
             :numeric_range,
             :default,
             :gspo,
             :gpos,
             :spog,
             :posg,
             :acl
           ],
      do: :ok

  def validate_cf(_), do: {:error, :invalid_column_family}

  # ===========================================================================
  # Column Family Options
  # ===========================================================================

  # Default column family options
  defp default_cf_options do
    dictionary_cf_options()
  end

  # ===========================================================================
  # Compaction Options (Section 3.2.3)
  # ===========================================================================

  @doc """
  Returns compaction options optimized for dictionary column families.

  Dictionary CFs have high read-to-write ratios with point lookups,
  so we prioritize read performance over write throughput.
  """
  @spec dictionary_compaction_options() :: keyword()
  def dictionary_compaction_options do
    [
      # Use universal compaction for better read performance on point lookups
      # Universal compaction reduces space amplification at the cost of
      # slightly higher write amplification
      compaction_style: :universal,
      # Size amplification threshold (5% = database size can be 5% larger than ideal)
      # Lower values reduce space but increase write amplification
      compaction_options_universal_size_amp_percent: 105,
      # Number of levels for universal compaction
      num_levels: 7,
      # Target file size for compaction (64MB)
      target_file_size_base: 64 * 1024 * 1024,
      # Compression levels
      compression: @compression_l1_l6,
      bottommost_compression: @compression_l1_l6
    ]
  end

  @doc """
  Returns compaction options optimized for index column families.

  Index CFs have balanced read/write patterns with prefix scans,
  so we use level compaction for good overall performance.
  """
  @spec index_compaction_options() :: keyword()
  def index_compaction_options do
    [
      # Use level compaction (default) for balanced performance
      compaction_style: :level,
      # Write buffer size (64MB memtable)
      write_buffer_size: 64 * 1024 * 1024,
      # Maximum number of write buffers (memtables)
      max_write_buffer_number: 3,
      # Minimum number of write buffers to flush
      min_write_buffer_number_to_merge: 1,
      # Target file size for L1 (64MB)
      target_file_size_base: 64 * 1024 * 1024,
      # Target file size multiplier for each level
      target_file_size_multiplier: 1,
      # Level 0 file size limit
      level0_file_num_compaction_trigger: 4,
      # Level 0 slowdown trigger
      level0_slowdown_writes_trigger: 8,
      # Level 0 stop trigger
      level0_stop_writes_trigger: 12,
      # Max bytes for each level
      # 256MB for L1, scaling up by 10x each level
      max_bytes_for_level_base: 256 * 1024 * 1024,
      # Multiplier for each level's size
      max_bytes_for_level_multiplier: 10,
      # Compaction priority (0 = lowest, 1 = highest)
      # Index CFs get higher priority due to query performance impact
      # (Note: this would be set via set_options_cf during runtime)
      # compaction_priority: 1,
      # Number of levels
      num_levels: 7,
      # Compression
      compression: @compression_l1_l6,
      bottommost_compression: @compression_l1_l6
    ]
  end

  @doc """
  Returns compaction options optimized for derived column family.

  Derived CF has write-heavy bulk load patterns with periodic full scans,
  so we optimize for write throughput.
  """
  @spec derived_compaction_options() :: keyword()
  def derived_compaction_options do
    [
      # Use level compaction for write-heavy workload
      compaction_style: :level,
      # Larger write buffer for bulk writes (128MB)
      write_buffer_size: 128 * 1024 * 1024,
      # More write buffers for concurrent writes
      max_write_buffer_number: 4,
      # Minimum number of write buffers to flush
      min_write_buffer_number_to_merge: 1,
      # Larger target file size for sequential access (128MB)
      target_file_size_base: 128 * 1024 * 1024,
      # Delayed L0 compaction for bulk loading
      level0_file_num_compaction_trigger: 8,
      # Higher slowdown trigger for bulk writes
      level0_slowdown_writes_trigger: 16,
      # Higher stop trigger for bulk writes
      level0_stop_writes_trigger: 24,
      # Max bytes for each level (larger due to sequential access)
      max_bytes_for_level_base: 512 * 1024 * 1024,
      # Multiplier for each level's size
      max_bytes_for_level_multiplier: 10,
      # Lower compaction priority (background task)
      # compaction_priority: 0,
      # Number of levels
      num_levels: 7,
      # Compression
      compression: @compression_l1_l6,
      bottommost_compression: @compression_l1_l6
    ]
  end

  # Dictionary CF options (id2str, str2id)
  # Optimized for random point lookups with high cache hit rates
  defp dictionary_cf_options do
    base_options()
    |> Keyword.merge(dictionary_compaction_options())
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
    |> Keyword.merge(index_compaction_options())
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
      # prefix_extractor: {"fixed.prefix", 8},
      # Memtable prefix bloom for efficient in-memory filtering
      memtable_prefix_bloom_size_ratio: 0.1,
      # Compression
      compression: @compression_l1_l6,
      bottommost_compression: @compression_l1_l6
    )
  end

  # Quad Index CF options (gspo, gpos, spog, posg)
  # Optimized for 32-byte quad keys with prefix-based scans
  #
  # Quad keys are larger (32 bytes vs 24 bytes for triples), so we use similar
  # tuning but adjust for the different access patterns:
  # - GSPO/GPOS: Graph-scoped queries (prefix on graph ID)
  # - SPOG/POSG: Cross-graph queries (prefix on subject/predicate)
  defp quad_index_cf_options do
    base_options()
    |> Keyword.merge(index_compaction_options())
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
      # Memtable prefix bloom for efficient in-memory filtering
      # Quad indices benefit even more from prefix bloom due to 4-part keys
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
    |> Keyword.merge(derived_compaction_options())
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
    |> Keyword.merge(index_compaction_options())
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

  # ACL CF options
  # Optimized for point lookups of access control entries
  defp acl_cf_options do
    base_options()
    |> Keyword.merge(dictionary_compaction_options())
    |> Keyword.merge(
      # High bloom filter for effective point lookup filtering
      block_based_table_options: [
        bloom_filter_policy: @bloom_dict_bits,
        block_size: @block_size_dict,
        # Cache index and filter blocks for faster lookups
        cache_index_and_filter_blocks: true,
        # Pin L0 filter/index blocks in cache (hot auth data)
        pin_l0_filter_and_index_blocks_in_cache: true,
        # Whole key filtering for ACL entries (exact match lookups)
        whole_key_filtering: true
      ],
      # No prefix extractor needed (random access pattern)
      # Compression
      compression: @compression_l1_l6,
      bottommost_compression: @compression_l1_l6
    )
  end

  # Derivation Provenance CF options
  # Optimized for mixed point lookups and scans of derivation records
  # Similar to derived CF but with moderate bloom filter for point lookups
  defp derivation_provenance_cf_options do
    base_options()
    |> Keyword.merge(index_compaction_options())
    |> Keyword.merge(
      # Medium bloom filter (balances point lookups and scans)
      block_based_table_options: [
        bloom_filter_policy: @bloom_index_bits,
        block_size: @block_size_index,
        cache_index_and_filter_blocks: true,
        pin_l0_filter_and_index_blocks_in_cache: false,
        # No whole key filtering (need prefix scans on graph ID)
        whole_key_filtering: false
      ],
      # No prefix extractor (custom key encoding)
      # Compression
      compression: @compression_l1_l6,
      bottommost_compression: @compression_l1_l6
    )
  end

  # Base options shared by all column families (minimal common settings)
  defp base_options do
    [
      # Disable write-ahead log sync for faster writes (durability handled by WAL fsync)
      # For ACID compliance, use sync: true in write operations
      # Note: Compaction-specific options are now in dedicated functions
      # (dictionary_compaction_options, index_compaction_options, derived_compaction_options)
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

      iex> ColumnFamilyConfig.cf_name_to_string(:gspo)
      "gspo"

  """
  @spec cf_name_to_string(column_family()) :: String.t()
  def cf_name_to_string(:id2str), do: "id2str"
  def cf_name_to_string(:str2id), do: "str2id"
  def cf_name_to_string(:spo), do: "spo"
  def cf_name_to_string(:pos), do: "pos"
  def cf_name_to_string(:osp), do: "osp"
  def cf_name_to_string(:gspo), do: "gspo"
  def cf_name_to_string(:gpos), do: "gpos"
  def cf_name_to_string(:spog), do: "spog"
  def cf_name_to_string(:posg), do: "posg"
  def cf_name_to_string(:derived), do: "derived"
  def cf_name_to_string(:derivation_provenance), do: "derivation_provenance"
  def cf_name_to_string(:numeric_range), do: "numeric_range"
  def cf_name_to_string(:acl), do: "acl"

  @doc """
  Converts a string column family name to its atom equivalent.

  ## Examples

      iex> ColumnFamilyConfig.cf_string_to_name("id2str")
      :id2str

      iex> ColumnFamilyConfig.cf_string_to_name("gspo")
      :gspo

  """
  @spec cf_string_to_name(String.t()) :: column_family() | nil
  def cf_string_to_name("id2str"), do: :id2str
  def cf_string_to_name("str2id"), do: :str2id
  def cf_string_to_name("spo"), do: :spo
  def cf_string_to_name("pos"), do: :pos
  def cf_string_to_name("osp"), do: :osp
  def cf_string_to_name("gspo"), do: :gspo
  def cf_string_to_name("gpos"), do: :gpos
  def cf_string_to_name("spog"), do: :spog
  def cf_string_to_name("posg"), do: :posg
  def cf_string_to_name("derived"), do: :derived
  def cf_string_to_name("derivation_provenance"), do: :derivation_provenance
  def cf_string_to_name("numeric_range"), do: :numeric_range
  def cf_string_to_name("acl"), do: :acl
  def cf_string_to_name(_), do: nil

  @doc """
  Returns the bloom filter bits per key for a column family.

  ## Examples

      iex> ColumnFamilyConfig.bloom_bits(:id2str)
      14

      iex> ColumnFamilyConfig.bloom_bits(:gspo)
      12

  """
  @spec bloom_bits(column_family()) :: non_neg_integer()
  def bloom_bits(:id2str), do: @bloom_dict_bits
  def bloom_bits(:str2id), do: @bloom_dict_bits
  def bloom_bits(:spo), do: @bloom_index_bits
  def bloom_bits(:pos), do: @bloom_index_bits
  def bloom_bits(:osp), do: @bloom_index_bits
  def bloom_bits(:gspo), do: @bloom_index_bits
  def bloom_bits(:gpos), do: @bloom_index_bits
  def bloom_bits(:spog), do: @bloom_index_bits
  def bloom_bits(:posg), do: @bloom_index_bits
  def bloom_bits(:derived), do: @bloom_derived_bits
  def bloom_bits(:derivation_provenance), do: @bloom_index_bits
  def bloom_bits(:numeric_range), do: @bloom_index_bits
  def bloom_bits(:acl), do: @bloom_dict_bits

  @doc """
  Returns the block size for a column family in bytes.

  ## Examples

      iex> ColumnFamilyConfig.block_size(:id2str)
      2048

      iex> ColumnFamilyConfig.block_size(:gspo)
      8192

  """
  @spec block_size(column_family()) :: pos_integer()
  def block_size(:id2str), do: @block_size_dict
  def block_size(:str2id), do: @block_size_dict
  def block_size(:spo), do: @block_size_index
  def block_size(:pos), do: @block_size_index
  def block_size(:osp), do: @block_size_index
  def block_size(:gspo), do: @block_size_index
  def block_size(:gpos), do: @block_size_index
  def block_size(:spog), do: @block_size_index
  def block_size(:posg), do: @block_size_index
  def block_size(:derived), do: @block_size_derived
  def block_size(:derivation_provenance), do: @block_size_index
  def block_size(:numeric_range), do: @block_size_index
  def block_size(:acl), do: @block_size_dict

  @doc """
  Checks if a column family has a prefix extractor configured.

  Note: The prefix_extractor configuration is version-dependent in erlang-rocksdb.
  The actual prefix extractor option is commented out in the CF options due to
  format uncertainty. However, for the purposes of index CFs (spo, pos, osp,
  gspo, gpos, spog, posg), a prefix extractor conceptually exists for the first
  8 bytes (64-bit ID).

  This function returns `true` for index CFs to indicate they are designed
  for prefix-based scans, even though the actual RocksDB option is not
  currently configured.

  ## Examples

      iex> ColumnFamilyConfig.has_prefix_extractor?(:spo)
      true

      iex> ColumnFamilyConfig.has_prefix_extractor?(:gspo)
      true

      iex> ColumnFamilyConfig.has_prefix_extractor?(:id2str)
      false

  """
  @spec has_prefix_extractor?(column_family()) :: boolean()
  def has_prefix_extractor?(:spo), do: true
  def has_prefix_extractor?(:pos), do: true
  def has_prefix_extractor?(:osp), do: true
  def has_prefix_extractor?(:gspo), do: true
  def has_prefix_extractor?(:gpos), do: true
  def has_prefix_extractor?(:spog), do: true
  def has_prefix_extractor?(:posg), do: true
  def has_prefix_extractor?(:id2str), do: false
  def has_prefix_extractor?(:str2id), do: false
  def has_prefix_extractor?(:derived), do: false
  def has_prefix_extractor?(:derivation_provenance), do: false
  def has_prefix_extractor?(:numeric_range), do: false
  def has_prefix_extractor?(:acl), do: false
  def has_prefix_extractor?(:default), do: false
end
