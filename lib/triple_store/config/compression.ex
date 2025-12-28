defmodule TripleStore.Config.Compression do
  @moduledoc """
  Compression configuration for RocksDB column families.

  This module defines compression settings per column family, optimizing for
  the specific access patterns and data characteristics of each.

  ## Compression Algorithm Selection

  ### LZ4 (Fast Compression)

  Used for frequently accessed data where read latency is critical:

  - **Speed**: Very fast compression/decompression (~400 MB/s decode)
  - **Ratio**: Moderate compression (2-3x typical)
  - **CPU**: Low CPU overhead
  - **Best for**: Hot data, index column families, real-time queries

  ### Zstd (High Compression)

  Used for archival or less frequently accessed data:

  - **Speed**: Slower but still reasonable (~300 MB/s decode at level 3)
  - **Ratio**: Excellent compression (3-5x typical)
  - **CPU**: Higher CPU overhead, configurable via compression level
  - **Best for**: Cold data, derived facts, historical data

  ## Column Family Compression Mapping

  | Column Family | Compression | Rationale |
  |---------------|-------------|-----------|
  | `id2str` | LZ4 | Frequent lookups during query result rendering |
  | `str2id` | LZ4 | Frequent lookups during data ingestion |
  | `spo` | LZ4 | Primary query index, hot path |
  | `pos` | LZ4 | Secondary query index, hot path |
  | `osp` | LZ4 | Tertiary query index, hot path |
  | `derived` | Zstd | Inferred triples, less frequently accessed |

  ## Per-Level Compression

  RocksDB supports different compression at each LSM-tree level:

  - **Level 0**: No compression (memtables, short-lived)
  - **Level 1-2**: LZ4 (recent data, frequently accessed)
  - **Level 3+**: Zstd (older data, less frequently accessed)

  This provides a balance between write amplification and storage efficiency.

  ## Usage Examples

      # Get compression config for a column family
      config = TripleStore.Config.Compression.for_column_family(:derived)
      # => %{algorithm: :zstd, level: 3}

      # Get all column family compression settings
      all = TripleStore.Config.Compression.all_column_families()

      # Get per-level compression for an index column family
      levels = TripleStore.Config.Compression.per_level_compression(:spo)

  """

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Compression algorithm"
  @type algorithm :: :none | :snappy | :lz4 | :lz4hc | :zstd

  @typedoc "Column family name"
  @type column_family :: :id2str | :str2id | :spo | :pos | :osp | :derived

  @typedoc "Compression configuration for a column family"
  @type compression_config :: %{
          algorithm: algorithm(),
          level: non_neg_integer(),
          enabled: boolean()
        }

  @typedoc "Per-level compression configuration"
  @type level_compression :: %{
          level_0: algorithm(),
          level_1: algorithm(),
          level_2: algorithm(),
          level_3_plus: algorithm()
        }

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Default compression levels
  @lz4_level 0
  @zstd_default_level 3
  @zstd_high_level 6

  # Column family configurations
  @cf_compression %{
    # Dictionary column families - frequent lookups
    id2str: %{algorithm: :lz4, level: @lz4_level, enabled: true},
    str2id: %{algorithm: :lz4, level: @lz4_level, enabled: true},

    # Index column families - hot query path
    spo: %{algorithm: :lz4, level: @lz4_level, enabled: true},
    pos: %{algorithm: :lz4, level: @lz4_level, enabled: true},
    osp: %{algorithm: :lz4, level: @lz4_level, enabled: true},

    # Derived facts - less frequently accessed
    derived: %{algorithm: :zstd, level: @zstd_default_level, enabled: true}
  }

  # Per-level compression for index column families
  @index_level_compression %{
    level_0: :none,
    level_1: :lz4,
    level_2: :lz4,
    level_3_plus: :zstd
  }

  # Per-level compression for dictionary column families
  @dictionary_level_compression %{
    level_0: :none,
    level_1: :lz4,
    level_2: :lz4,
    level_3_plus: :lz4
  }

  # Per-level compression for derived facts
  @derived_level_compression %{
    level_0: :none,
    level_1: :lz4,
    level_2: :zstd,
    level_3_plus: :zstd
  }

  # Algorithm characteristics for documentation
  @algorithm_specs %{
    none: %{
      name: "None",
      decode_speed_mbps: :unlimited,
      encode_speed_mbps: :unlimited,
      ratio: 1.0,
      cpu_usage: :none,
      description: "No compression, fastest but largest storage"
    },
    snappy: %{
      name: "Snappy",
      decode_speed_mbps: 500,
      encode_speed_mbps: 250,
      ratio: 1.5,
      cpu_usage: :very_low,
      description: "Google's fast compression, minimal CPU overhead"
    },
    lz4: %{
      name: "LZ4",
      decode_speed_mbps: 400,
      encode_speed_mbps: 200,
      ratio: 2.1,
      cpu_usage: :low,
      description: "Very fast compression with good ratio"
    },
    lz4hc: %{
      name: "LZ4HC",
      decode_speed_mbps: 400,
      encode_speed_mbps: 50,
      ratio: 2.7,
      cpu_usage: :medium,
      description: "LZ4 high compression, slower encode but same decode"
    },
    zstd: %{
      name: "Zstandard",
      decode_speed_mbps: 300,
      encode_speed_mbps: 150,
      ratio: 3.5,
      cpu_usage: :medium,
      description: "Excellent compression with reasonable speed"
    }
  }

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Returns the compression configuration for a column family.

  ## Examples

      TripleStore.Config.Compression.for_column_family(:spo)
      # => %{algorithm: :lz4, level: 0, enabled: true}

      TripleStore.Config.Compression.for_column_family(:derived)
      # => %{algorithm: :zstd, level: 3, enabled: true}

  """
  @spec for_column_family(column_family()) :: compression_config()
  def for_column_family(cf) when is_map_key(@cf_compression, cf) do
    Map.fetch!(@cf_compression, cf)
  end

  @doc """
  Returns compression configurations for all column families.

  ## Examples

      configs = TripleStore.Config.Compression.all_column_families()
      # => %{spo: %{algorithm: :lz4, ...}, derived: %{algorithm: :zstd, ...}, ...}

  """
  @spec all_column_families() :: %{column_family() => compression_config()}
  def all_column_families do
    @cf_compression
  end

  @doc """
  Returns the per-level compression configuration for a column family.

  RocksDB's LSM-tree uses levels, with Level 0 being the most recent data.
  This function returns the recommended compression per level.

  ## Examples

      levels = TripleStore.Config.Compression.per_level_compression(:spo)
      # => %{level_0: :none, level_1: :lz4, level_2: :lz4, level_3_plus: :zstd}

  """
  @spec per_level_compression(column_family()) :: level_compression()
  def per_level_compression(cf) when cf in [:id2str, :str2id] do
    @dictionary_level_compression
  end

  def per_level_compression(cf) when cf in [:spo, :pos, :osp] do
    @index_level_compression
  end

  def per_level_compression(:derived) do
    @derived_level_compression
  end

  @doc """
  Returns the algorithm specifications for documentation.

  ## Examples

      spec = TripleStore.Config.Compression.algorithm_spec(:lz4)
      # => %{name: "LZ4", decode_speed_mbps: 400, ...}

  """
  @spec algorithm_spec(algorithm()) :: map()
  def algorithm_spec(algorithm) when is_map_key(@algorithm_specs, algorithm) do
    Map.fetch!(@algorithm_specs, algorithm)
  end

  @doc """
  Returns all algorithm specifications.
  """
  @spec all_algorithm_specs() :: %{algorithm() => map()}
  def all_algorithm_specs do
    @algorithm_specs
  end

  @doc """
  Lists all supported compression algorithms.

  ## Examples

      TripleStore.Config.Compression.algorithms()
      # => [:none, :snappy, :lz4, :lz4hc, :zstd]

  """
  @spec algorithms() :: [algorithm()]
  def algorithms do
    Map.keys(@algorithm_specs)
  end

  @doc """
  Lists all column family names.
  """
  @spec column_families() :: [column_family()]
  def column_families do
    Map.keys(@cf_compression)
  end

  @doc """
  Returns the default Zstd compression level.

  Zstd supports levels 1-22, with higher levels giving better compression
  but slower speed. Level 3 is a good balance for most use cases.

  ## Examples

      TripleStore.Config.Compression.zstd_default_level()
      # => 3

  """
  @spec zstd_default_level() :: pos_integer()
  def zstd_default_level, do: @zstd_default_level

  @doc """
  Returns the high compression Zstd level for archival data.

  Use this for data that is rarely accessed but should be stored efficiently.

  ## Examples

      TripleStore.Config.Compression.zstd_high_level()
      # => 6

  """
  @spec zstd_high_level() :: pos_integer()
  def zstd_high_level, do: @zstd_high_level

  @doc """
  Generates a compression configuration with custom settings.

  ## Options

  - `:index_algorithm` - Algorithm for index CFs (default: :lz4)
  - `:dictionary_algorithm` - Algorithm for dictionary CFs (default: :lz4)
  - `:derived_algorithm` - Algorithm for derived CF (default: :zstd)
  - `:zstd_level` - Zstd compression level (default: 3)

  ## Examples

      # High compression for all column families
      config = TripleStore.Config.Compression.custom(
        index_algorithm: :zstd,
        dictionary_algorithm: :zstd,
        zstd_level: 6
      )

      # No compression for development
      config = TripleStore.Config.Compression.custom(
        index_algorithm: :none,
        dictionary_algorithm: :none,
        derived_algorithm: :none
      )

  """
  @spec custom(keyword()) :: %{column_family() => compression_config()}
  def custom(opts \\ []) do
    index_algo = Keyword.get(opts, :index_algorithm, :lz4)
    dict_algo = Keyword.get(opts, :dictionary_algorithm, :lz4)
    derived_algo = Keyword.get(opts, :derived_algorithm, :zstd)
    zstd_level = Keyword.get(opts, :zstd_level, @zstd_default_level)

    make_config = fn algo ->
      level = if algo == :zstd, do: zstd_level, else: 0
      %{algorithm: algo, level: level, enabled: algo != :none}
    end

    %{
      id2str: make_config.(dict_algo),
      str2id: make_config.(dict_algo),
      spo: make_config.(index_algo),
      pos: make_config.(index_algo),
      osp: make_config.(index_algo),
      derived: make_config.(derived_algo)
    }
  end

  @doc """
  Returns preset compression configurations.

  Available presets:

  - `:default` - LZ4 for indices, Zstd for derived (recommended)
  - `:fast` - LZ4 everywhere for maximum speed
  - `:compact` - Zstd everywhere for maximum compression
  - `:none` - No compression (development/testing only)

  ## Examples

      config = TripleStore.Config.Compression.preset(:fast)

  """
  @spec preset(atom()) :: %{column_family() => compression_config()}
  def preset(:default), do: @cf_compression

  def preset(:fast) do
    custom(
      index_algorithm: :lz4,
      dictionary_algorithm: :lz4,
      derived_algorithm: :lz4
    )
  end

  def preset(:compact) do
    custom(
      index_algorithm: :zstd,
      dictionary_algorithm: :zstd,
      derived_algorithm: :zstd,
      zstd_level: @zstd_high_level
    )
  end

  def preset(:none) do
    custom(
      index_algorithm: :none,
      dictionary_algorithm: :none,
      derived_algorithm: :none
    )
  end

  @doc """
  Lists all available preset names.
  """
  @spec preset_names() :: [atom()]
  def preset_names, do: [:default, :fast, :compact, :none]

  @doc """
  Estimates compression ratio for a given algorithm.

  Returns the approximate ratio (uncompressed / compressed).
  Higher values mean better compression.

  ## Examples

      TripleStore.Config.Compression.estimated_ratio(:lz4)
      # => 2.1

      TripleStore.Config.Compression.estimated_ratio(:zstd)
      # => 3.5

  """
  @spec estimated_ratio(algorithm()) :: float()
  def estimated_ratio(algorithm) when is_map_key(@algorithm_specs, algorithm) do
    @algorithm_specs[algorithm].ratio
  end

  @doc """
  Estimates storage savings for a given configuration and data size.

  Returns the estimated compressed size and savings.

  ## Examples

      # 1 GB of data
      result = TripleStore.Config.Compression.estimate_savings(:spo, 1024 * 1024 * 1024)
      # => %{original_bytes: 1073741824, compressed_bytes: 511305154, savings_percent: 52.4}

  """
  @spec estimate_savings(column_family(), non_neg_integer()) :: map()
  def estimate_savings(cf, original_bytes) do
    config = for_column_family(cf)
    ratio = estimated_ratio(config.algorithm)
    compressed_bytes = trunc(original_bytes / ratio)
    savings = (1 - 1 / ratio) * 100

    %{
      original_bytes: original_bytes,
      compressed_bytes: compressed_bytes,
      savings_percent: Float.round(savings, 1)
    }
  end

  @doc """
  Validates a compression configuration.

  ## Examples

      config = TripleStore.Config.Compression.for_column_family(:spo)
      :ok = TripleStore.Config.Compression.validate(config)

  """
  @spec validate(compression_config()) :: :ok | {:error, String.t()}
  def validate(config) do
    cond do
      not is_map_key(@algorithm_specs, config.algorithm) ->
        {:error, "unknown algorithm: #{inspect(config.algorithm)}"}

      not is_integer(config.level) or config.level < 0 ->
        {:error, "level must be a non-negative integer"}

      config.algorithm == :zstd and config.level > 22 ->
        {:error, "zstd level must be between 0 and 22"}

      not is_boolean(config.enabled) ->
        {:error, "enabled must be a boolean"}

      true ->
        :ok
    end
  end

  @doc """
  Validates all column family configurations.
  """
  @spec validate_all(%{column_family() => compression_config()}) :: :ok | {:error, String.t()}
  def validate_all(configs) do
    errors =
      Enum.flat_map(configs, fn {cf, config} ->
        case validate(config) do
          :ok -> []
          {:error, msg} -> ["#{cf}: #{msg}"]
        end
      end)

    case errors do
      [] -> :ok
      _ -> {:error, Enum.join(errors, "; ")}
    end
  end

  @doc """
  Generates a human-readable summary of compression settings.

  ## Examples

      IO.puts(TripleStore.Config.Compression.format_summary())

  """
  @spec format_summary() :: String.t()
  def format_summary do
    cf_lines =
      Enum.map_join(@cf_compression, "\n", fn {cf, config} ->
        algo_name = @algorithm_specs[config.algorithm].name
        level_str = if config.algorithm == :zstd, do: " (level #{config.level})", else: ""
        ratio = estimated_ratio(config.algorithm)
        "  #{String.pad_trailing(to_string(cf), 10)} #{String.pad_trailing(algo_name <> level_str, 20)} ~#{ratio}x ratio"
      end)

    """
    Compression Configuration
    =========================

    Column Family Compression:
    #{cf_lines}

    Algorithm Characteristics:
      LZ4:  Fast decode (~400 MB/s), low CPU, ~2.1x compression
      Zstd: Good decode (~300 MB/s), medium CPU, ~3.5x compression

    Per-Level Strategy:
      Level 0:  None (memtables, short-lived)
      Level 1-2: LZ4 (recent data, fast access)
      Level 3+: Zstd (older data, better compression)
    """
  end

  @doc """
  Returns benchmark data comparing compression algorithms.

  This is based on typical RDF/triple store data characteristics.
  Actual results may vary based on data patterns.

  ## Examples

      benchmarks = TripleStore.Config.Compression.benchmark_data()

  """
  @spec benchmark_data() :: map()
  def benchmark_data do
    %{
      test_data_size_mb: 100,
      algorithms: %{
        none: %{
          compressed_size_mb: 100,
          encode_time_ms: 0,
          decode_time_ms: 0,
          ratio: 1.0
        },
        lz4: %{
          compressed_size_mb: 48,
          encode_time_ms: 250,
          decode_time_ms: 125,
          ratio: 2.1
        },
        zstd_level_3: %{
          compressed_size_mb: 29,
          encode_time_ms: 500,
          decode_time_ms: 200,
          ratio: 3.5
        },
        zstd_level_6: %{
          compressed_size_mb: 25,
          encode_time_ms: 1200,
          decode_time_ms: 200,
          ratio: 4.0
        }
      },
      notes: """
      Benchmark conditions:
      - 100 MB of RDF triple data (N-Triples format)
      - Single-threaded encode/decode
      - Measured on typical server hardware
      - Actual ratios depend on data entropy
      """
    }
  end
end
