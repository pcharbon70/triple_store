defmodule TripleStore.Config.ColumnFamily do
  @moduledoc """
  Column family tuning configuration for RocksDB.

  See `TripleStore.Config.Helpers` for shared utilities (format_bytes, validation helpers).

  This module provides per-column-family configuration optimized for the
  specific access patterns of each column family in the triple store:

  ## Column Families and Their Access Patterns

  ### Dictionary Column Families (Point Lookups)
  - `id2str` - Integer ID to string lookup (decode operations)
  - `str2id` - String to integer ID lookup (encode operations)

  These benefit from bloom filters since they perform point lookups.
  False positive rate of 1% provides good memory/performance tradeoff.

  ### Index Column Families (Prefix Scans)
  - `spo` - Subject-Predicate-Object index (subject queries)
  - `pos` - Predicate-Object-Subject index (predicate queries)
  - `osp` - Object-Subject-Predicate index (object queries)

  These benefit from prefix extractors since queries often scan by
  the first component (8 bytes = one triple component ID).

  ### Derived Column Family (Bulk Operations)
  - `derived` - Materialized inferred triples

  This handles bulk reads/writes during reasoning, benefiting from
  larger block sizes to reduce I/O operations.

  ## Configuration Options

  Each column family configuration includes:
  - `:bloom_filter` - Bloom filter settings (bits_per_key, block_based)
  - `:prefix_extractor` - Prefix extractor settings (type, length)
  - `:block_size` - Block size in bytes
  - `:cache_index_and_filter_blocks` - Whether to cache index/filter blocks
  - `:pin_l0_filter_and_index_blocks_in_cache` - Pin L0 blocks in cache
  - `:optimize_filters_for_hits` - Optimize for high hit rate workloads

  ## Usage

      # Get configuration for a specific column family
      config = ColumnFamily.for_cf(:spo)

      # Get all column family configurations
      all = ColumnFamily.all()

      # Get configuration with custom overrides
      config = ColumnFamily.for_cf(:spo, block_size: 8192)

  """

  alias TripleStore.Config.Helpers

  # Bloom filter bits per key for index CFs (prefix scans)
  # 12 bits/key gives approximately 0.09% false positive rate
  @index_bloom_bits_per_key 12

  # Bloom filter bits per key for dictionary CFs (point lookups)
  # 14 bits/key gives approximately 0.01% false positive rate
  @dictionary_bloom_bits_per_key 14

  # Legacy alias for backward compatibility
  @default_bloom_bits_per_key @index_bloom_bits_per_key

  # Triple component ID size in bytes (64-bit integer)
  @triple_component_size 8

  # Block size for index CFs (balanced for prefix scans)
  @index_block_size 8 * 1024

  # Block size for derived CF (optimized for sequential reads)
  @bulk_block_size 32 * 1024

  # Block size for dictionary CFs (optimized for point lookups)
  @point_lookup_block_size 2 * 1024

  # Column family types
  @dictionary_cfs [:id2str, :str2id]
  @index_cfs [:spo, :pos, :osp]
  @derived_cfs [:derived]
  @all_cfs @dictionary_cfs ++ @index_cfs ++ @derived_cfs

  @typedoc "Column family name"
  @type cf_name :: :id2str | :str2id | :spo | :pos | :osp | :derived

  @typedoc "Bloom filter configuration"
  @type bloom_filter_config :: %{
          enabled: boolean(),
          bits_per_key: pos_integer(),
          block_based: boolean()
        }

  @typedoc "Prefix extractor configuration"
  @type prefix_extractor_config :: %{
          enabled: boolean(),
          type: :fixed | :capped,
          length: non_neg_integer()
        }

  @typedoc "Column family configuration"
  @type t :: %{
          name: cf_name(),
          type: :dictionary | :index | :derived,
          access_pattern: :point_lookup | :prefix_scan | :bulk,
          bloom_filter: bloom_filter_config(),
          prefix_extractor: prefix_extractor_config(),
          block_size: pos_integer(),
          cache_index_and_filter_blocks: boolean(),
          pin_l0_filter_and_index_blocks_in_cache: boolean(),
          optimize_filters_for_hits: boolean(),
          whole_key_filtering: boolean(),
          format_version: pos_integer()
        }

  @doc """
  Returns configuration for a specific column family.

  ## Options

  - `:block_size` - Override default block size
  - `:bloom_bits_per_key` - Override bloom filter bits per key
  - `:cache_index_and_filter_blocks` - Override cache setting

  ## Examples

      iex> config = ColumnFamily.for_cf(:spo)
      iex> config.type
      :index

      iex> config = ColumnFamily.for_cf(:id2str)
      iex> config.bloom_filter.enabled
      true

  """
  @spec for_cf(cf_name(), keyword()) :: t()
  def for_cf(name, opts \\ []) when name in @all_cfs do
    base_config = base_config_for(name)
    apply_overrides(base_config, opts)
  end

  @doc """
  Returns configurations for all column families.

  ## Examples

      iex> all = ColumnFamily.all()
      iex> length(Map.keys(all))
      6

  """
  @spec all(keyword()) :: %{cf_name() => t()}
  def all(opts \\ []) do
    Map.new(@all_cfs, fn name -> {name, for_cf(name, opts)} end)
  end

  @doc """
  Returns the list of all column family names.
  """
  @spec column_family_names() :: [cf_name()]
  def column_family_names, do: @all_cfs

  @doc """
  Returns the list of dictionary column family names.
  """
  @spec dictionary_cfs() :: [cf_name()]
  def dictionary_cfs, do: @dictionary_cfs

  @doc """
  Returns the list of index column family names.
  """
  @spec index_cfs() :: [cf_name()]
  def index_cfs, do: @index_cfs

  @doc """
  Returns the list of derived column family names.
  """
  @spec derived_cfs() :: [cf_name()]
  def derived_cfs, do: @derived_cfs

  @doc """
  Returns the column family type for a given name.

  ## Examples

      iex> ColumnFamily.cf_type(:id2str)
      :dictionary

      iex> ColumnFamily.cf_type(:spo)
      :index

      iex> ColumnFamily.cf_type(:derived)
      :derived

  """
  @spec cf_type(cf_name()) :: :dictionary | :index | :derived
  def cf_type(name) when name in @dictionary_cfs, do: :dictionary
  def cf_type(name) when name in @index_cfs, do: :index
  def cf_type(name) when name in @derived_cfs, do: :derived

  @doc """
  Returns the access pattern for a given column family.

  ## Examples

      iex> ColumnFamily.access_pattern(:id2str)
      :point_lookup

      iex> ColumnFamily.access_pattern(:spo)
      :prefix_scan

      iex> ColumnFamily.access_pattern(:derived)
      :bulk

  """
  @spec access_pattern(cf_name()) :: :point_lookup | :prefix_scan | :bulk
  def access_pattern(name) when name in @dictionary_cfs, do: :point_lookup
  def access_pattern(name) when name in @index_cfs, do: :prefix_scan
  def access_pattern(name) when name in @derived_cfs, do: :bulk

  @doc """
  Returns bloom filter configuration for a column family.

  Dictionary column families use higher bits per key for more aggressive
  filtering. Index column families use standard settings. Derived column
  family disables bloom filters since it primarily does range scans.

  ## Examples

      iex> bloom = ColumnFamily.bloom_filter_config(:id2str)
      iex> bloom.enabled
      true
      iex> bloom.bits_per_key
      12

      iex> bloom = ColumnFamily.bloom_filter_config(:derived)
      iex> bloom.enabled
      false

  """
  @spec bloom_filter_config(cf_name()) :: bloom_filter_config()
  def bloom_filter_config(name) when name in @dictionary_cfs do
    %{
      enabled: true,
      bits_per_key: @dictionary_bloom_bits_per_key,
      block_based: false
    }
  end

  def bloom_filter_config(name) when name in @index_cfs do
    %{
      enabled: true,
      bits_per_key: @index_bloom_bits_per_key,
      block_based: true
    }
  end

  def bloom_filter_config(name) when name in @derived_cfs do
    %{
      enabled: false,
      bits_per_key: 0,
      block_based: false
    }
  end

  @doc """
  Returns prefix extractor configuration for a column family.

  Index column families use fixed-length prefix extractors matching the
  size of a triple component (8 bytes). Dictionary and derived column
  families don't use prefix extraction.

  ## Examples

      iex> prefix = ColumnFamily.prefix_extractor_config(:spo)
      iex> prefix.enabled
      true
      iex> prefix.length
      8

      iex> prefix = ColumnFamily.prefix_extractor_config(:id2str)
      iex> prefix.enabled
      false

  """
  @spec prefix_extractor_config(cf_name()) :: prefix_extractor_config()
  def prefix_extractor_config(name) when name in @index_cfs do
    %{
      enabled: true,
      type: :fixed,
      length: @triple_component_size
    }
  end

  def prefix_extractor_config(name) when name in @dictionary_cfs do
    %{
      enabled: false,
      type: :fixed,
      length: 0
    }
  end

  def prefix_extractor_config(name) when name in @derived_cfs do
    %{
      enabled: false,
      type: :fixed,
      length: 0
    }
  end

  @doc """
  Returns the recommended block size for a column family.

  - Dictionary CFs: 2KB (optimized for point lookups)
  - Index CFs: 8KB (balanced for prefix scans)
  - Derived CF: 32KB (optimized for bulk operations)

  ## Examples

      iex> ColumnFamily.block_size(:id2str)
      2048

      iex> ColumnFamily.block_size(:derived)
      32768

  """
  @spec block_size(cf_name()) :: pos_integer()
  def block_size(name) when name in @dictionary_cfs, do: @point_lookup_block_size
  def block_size(name) when name in @index_cfs, do: @index_block_size
  def block_size(name) when name in @derived_cfs, do: @bulk_block_size

  @doc """
  Returns the triple component size in bytes.

  Triple components (subject, predicate, object IDs) are 64-bit integers,
  requiring 8 bytes each. This is used for prefix extractor configuration.
  """
  @spec triple_component_size() :: pos_integer()
  def triple_component_size, do: @triple_component_size

  @doc """
  Returns the default bloom filter bits per key.
  """
  @spec default_bloom_bits_per_key() :: pos_integer()
  def default_bloom_bits_per_key, do: @default_bloom_bits_per_key

  @doc """
  Returns the dictionary bloom filter bits per key.
  """
  @spec dictionary_bloom_bits_per_key() :: pos_integer()
  def dictionary_bloom_bits_per_key, do: @dictionary_bloom_bits_per_key

  @doc """
  Returns the index bloom filter bits per key.
  """
  @spec index_bloom_bits_per_key() :: pos_integer()
  def index_bloom_bits_per_key, do: @index_bloom_bits_per_key

  @doc """
  Calculates the estimated false positive rate for a bloom filter.

  Formula: (1 - e^(-k*n/m))^k where:
  - k = number of hash functions ≈ 0.693 * bits_per_key
  - n = number of keys
  - m = total bits = n * bits_per_key

  For a well-tuned bloom filter: FPR ≈ 0.6185^bits_per_key

  ## Examples

      iex> fpr = ColumnFamily.estimated_false_positive_rate(10)
      iex> fpr < 0.02
      true

      iex> fpr = ColumnFamily.estimated_false_positive_rate(12)
      iex> fpr < 0.01
      true

  """
  @spec estimated_false_positive_rate(pos_integer()) :: float()
  def estimated_false_positive_rate(bits_per_key) when bits_per_key > 0 do
    :math.pow(0.6185, bits_per_key)
  end

  @doc """
  Returns the estimated memory overhead per key for bloom filters.

  Memory usage = bits_per_key / 8 bytes per key.

  ## Examples

      iex> ColumnFamily.bloom_memory_per_key(10)
      1.25

      iex> ColumnFamily.bloom_memory_per_key(12)
      1.5

  """
  @spec bloom_memory_per_key(pos_integer()) :: float()
  def bloom_memory_per_key(bits_per_key) when bits_per_key > 0 do
    bits_per_key / 8
  end

  @doc """
  Estimates bloom filter memory usage for a given number of keys.

  ## Examples

      iex> bytes = ColumnFamily.estimate_bloom_memory(:id2str, 1_000_000)
      iex> bytes == 1_500_000
      true

  """
  @spec estimate_bloom_memory(cf_name(), non_neg_integer()) :: non_neg_integer()
  def estimate_bloom_memory(name, num_keys) when name in @all_cfs and num_keys >= 0 do
    config = bloom_filter_config(name)

    if config.enabled do
      trunc(num_keys * bloom_memory_per_key(config.bits_per_key))
    else
      0
    end
  end

  @doc """
  Returns tuning rationale for a column family.

  This provides human-readable explanations of why specific settings
  are recommended for each column family type.

  ## Examples

      iex> rationale = ColumnFamily.tuning_rationale(:spo)
      iex> rationale.bloom_filter_rationale
      "Enabled with 10 bits/key for prefix bloom filtering during seeks"

  """
  @spec tuning_rationale(cf_name()) :: %{
          cf_name: cf_name(),
          cf_type: :dictionary | :index | :derived,
          access_pattern: :point_lookup | :prefix_scan | :bulk,
          bloom_filter_rationale: String.t(),
          prefix_extractor_rationale: String.t(),
          block_size_rationale: String.t(),
          cache_rationale: String.t()
        }
  def tuning_rationale(name) when name in @dictionary_cfs do
    %{
      cf_name: name,
      cf_type: :dictionary,
      access_pattern: :point_lookup,
      bloom_filter_rationale:
        "Enabled with #{@dictionary_bloom_bits_per_key} bits/key (#{format_fpr(@dictionary_bloom_bits_per_key)} FPR) " <>
          "to efficiently filter negative lookups during string encoding/decoding",
      prefix_extractor_rationale:
        "Disabled - dictionary keys are looked up by exact match, not prefix",
      block_size_rationale:
        "#{format_bytes(@point_lookup_block_size)} blocks optimize for random point lookups " <>
          "with minimal read amplification",
      cache_rationale:
        "Index and filter blocks cached and pinned in L0 for fast dictionary access"
    }
  end

  def tuning_rationale(name) when name in @index_cfs do
    index_name =
      case name do
        :spo -> "Subject-Predicate-Object"
        :pos -> "Predicate-Object-Subject"
        :osp -> "Object-Subject-Predicate"
      end

    %{
      cf_name: name,
      cf_type: :index,
      access_pattern: :prefix_scan,
      bloom_filter_rationale:
        "Block-based bloom filter with #{@index_bloom_bits_per_key} bits/key " <>
          "(#{format_fpr(@index_bloom_bits_per_key)} FPR) " <>
          "enables prefix bloom filtering for range queries",
      prefix_extractor_rationale:
        "Fixed #{@triple_component_size}-byte prefix extractor matches triple component ID size, " <>
          "enabling efficient prefix seeks for #{index_name} queries",
      block_size_rationale:
        "#{format_bytes(@index_block_size)} blocks balance between read amplification " <>
          "and cache efficiency for prefix scans",
      cache_rationale:
        "Index blocks cached, filter blocks pinned in L0 for hot path query performance"
    }
  end

  def tuning_rationale(name) when name in @derived_cfs do
    %{
      cf_name: name,
      cf_type: :derived,
      access_pattern: :bulk,
      bloom_filter_rationale:
        "Disabled - derived facts are accessed via bulk scans during reasoning, " <>
          "not point lookups; bloom filters would add memory overhead without benefit",
      prefix_extractor_rationale:
        "Disabled - reasoning processes scan derived facts sequentially " <>
          "rather than by prefix",
      block_size_rationale:
        "#{format_bytes(@bulk_block_size)} larger blocks reduce I/O operations " <>
          "and improve throughput for bulk materialization workloads",
      cache_rationale:
        "Index blocks cached but filters not pinned; derived data is less frequently " <>
          "accessed than indices and dictionaries"
    }
  end

  @doc """
  Returns a formatted summary of all column family configurations.

  ## Examples

      iex> summary = ColumnFamily.format_summary()
      iex> String.contains?(summary, "spo")
      true

  """
  @spec format_summary() :: String.t()
  def format_summary do
    header = """
    Column Family Tuning Summary
    ============================

    """

    cf_summaries =
      Enum.map_join(@all_cfs, "\n", fn name ->
        config = for_cf(name)
        rationale = tuning_rationale(name)

        bloom_status =
          if config.bloom_filter.enabled do
            "#{config.bloom_filter.bits_per_key} bits/key"
          else
            "Disabled"
          end

        prefix_status =
          if config.prefix_extractor.enabled do
            "#{config.prefix_extractor.length} bytes"
          else
            "Disabled"
          end

        """
        #{name} (#{rationale.cf_type})
        #{String.duplicate("-", String.length(to_string(name)) + String.length(to_string(rationale.cf_type)) + 3)}
          Access Pattern: #{rationale.access_pattern}
          Bloom Filter: #{bloom_status}
          Prefix Extractor: #{prefix_status}
          Block Size: #{format_bytes(config.block_size)}
        """
      end)

    header <> cf_summaries
  end

  @doc """
  Validates a column family configuration.

  ## Examples

      iex> config = ColumnFamily.for_cf(:spo)
      iex> ColumnFamily.validate(config)
      :ok

  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate(config) do
    with :ok <- validate_name(config),
         :ok <- validate_bloom_filter(config),
         :ok <- validate_prefix_extractor(config) do
      validate_block_size(config)
    end
  end

  @doc """
  Validates all column family configurations.
  """
  @spec validate_all() :: :ok | {:error, String.t()}
  def validate_all do
    results =
      Enum.map(@all_cfs, fn name ->
        config = for_cf(name)
        {name, validate(config)}
      end)

    case Enum.find(results, fn {_name, result} -> result != :ok end) do
      nil -> :ok
      {name, {:error, reason}} -> {:error, "#{name}: #{reason}"}
    end
  end

  @doc """
  Returns RocksDB options representation for a column family.

  This converts the configuration to a keyword list suitable for
  passing to RocksDB NIF functions.

  ## Examples

      iex> opts = ColumnFamily.to_rocksdb_options(:spo)
      iex> Keyword.has_key?(opts, :block_size)
      true

  """
  @spec to_rocksdb_options(cf_name(), keyword()) :: keyword()
  def to_rocksdb_options(name, opts \\ []) when name in @all_cfs do
    config = for_cf(name, opts)

    base_opts = [
      block_size: config.block_size,
      cache_index_and_filter_blocks: config.cache_index_and_filter_blocks,
      pin_l0_filter_and_index_blocks_in_cache: config.pin_l0_filter_and_index_blocks_in_cache,
      optimize_filters_for_hits: config.optimize_filters_for_hits,
      whole_key_filtering: config.whole_key_filtering,
      format_version: config.format_version
    ]

    bloom_opts =
      if config.bloom_filter.enabled do
        [
          bloom_filter_bits_per_key: config.bloom_filter.bits_per_key,
          bloom_filter_block_based: config.bloom_filter.block_based
        ]
      else
        []
      end

    prefix_opts =
      if config.prefix_extractor.enabled do
        [
          prefix_extractor_type: config.prefix_extractor.type,
          prefix_extractor_length: config.prefix_extractor.length
        ]
      else
        []
      end

    base_opts ++ bloom_opts ++ prefix_opts
  end

  # Private functions

  defp base_config_for(name) do
    %{
      name: name,
      type: cf_type(name),
      access_pattern: access_pattern(name),
      bloom_filter: bloom_filter_config(name),
      prefix_extractor: prefix_extractor_config(name),
      block_size: block_size(name),
      cache_index_and_filter_blocks: true,
      pin_l0_filter_and_index_blocks_in_cache: name in @dictionary_cfs or name in @index_cfs,
      optimize_filters_for_hits: name in @dictionary_cfs,
      whole_key_filtering: name in @dictionary_cfs,
      format_version: 5
    }
  end

  defp apply_overrides(config, opts) do
    Enum.reduce(opts, config, fn
      {:block_size, size}, acc when is_integer(size) and size > 0 ->
        %{acc | block_size: size}

      {:bloom_bits_per_key, bits}, acc when is_integer(bits) and bits > 0 ->
        bloom = %{acc.bloom_filter | bits_per_key: bits, enabled: true}
        %{acc | bloom_filter: bloom}

      {:cache_index_and_filter_blocks, value}, acc when is_boolean(value) ->
        %{acc | cache_index_and_filter_blocks: value}

      {:pin_l0_filter_and_index_blocks_in_cache, value}, acc when is_boolean(value) ->
        %{acc | pin_l0_filter_and_index_blocks_in_cache: value}

      {:optimize_filters_for_hits, value}, acc when is_boolean(value) ->
        %{acc | optimize_filters_for_hits: value}

      _, acc ->
        acc
    end)
  end

  defp validate_name(%{name: name}) when name in @all_cfs, do: :ok
  defp validate_name(%{name: name}), do: {:error, "Unknown column family: #{name}"}

  defp validate_bloom_filter(%{bloom_filter: %{enabled: false}}), do: :ok

  defp validate_bloom_filter(%{bloom_filter: %{enabled: true, bits_per_key: bits}})
       when bits > 0 and bits <= 24,
       do: :ok

  defp validate_bloom_filter(%{bloom_filter: %{bits_per_key: bits}}) do
    {:error, "Bloom filter bits_per_key must be between 1 and 24, got: #{bits}"}
  end

  defp validate_prefix_extractor(%{prefix_extractor: %{enabled: false}}), do: :ok

  defp validate_prefix_extractor(%{prefix_extractor: %{enabled: true, length: len}})
       when len > 0 and len <= 64,
       do: :ok

  defp validate_prefix_extractor(%{prefix_extractor: %{length: len}}) do
    {:error, "Prefix extractor length must be between 1 and 64, got: #{len}"}
  end

  defp validate_block_size(%{block_size: size}) when size >= 1024 and size <= 1_048_576, do: :ok

  defp validate_block_size(%{block_size: size}) do
    {:error, "Block size must be between 1KB and 1MB, got: #{size}"}
  end

  defp format_fpr(bits_per_key) do
    fpr = estimated_false_positive_rate(bits_per_key)
    "~#{Float.round(fpr * 100, 2)}%"
  end

  defp format_bytes(bytes), do: Helpers.format_bytes(bytes)
end
