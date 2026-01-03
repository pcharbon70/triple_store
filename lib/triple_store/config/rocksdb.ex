defmodule TripleStore.Config.RocksDB do
  @moduledoc """
  RocksDB memory configuration for production workloads.

  See `TripleStore.Config.Helpers` for shared utilities.

  This module provides intelligent memory tuning for RocksDB based on
  available system RAM and workload characteristics.

  ## Memory Allocation Guidelines

  RocksDB uses memory in several key areas:

  1. **Block Cache** - Caches uncompressed data blocks for reads
     - Recommended: 40% of available RAM for dedicated triple store
     - Minimum: 64 MB for basic operation
     - Maximum: 32 GB (diminishing returns beyond this)

  2. **Write Buffers** - In-memory buffers for write batching
     - Each memtable uses this much memory
     - Default: 64 MB per buffer, 2-4 buffers per column family
     - For write-heavy loads: increase to 128-256 MB

  3. **Index/Filter Blocks** - Bloom filters and block indices
     - Typically 5-10% of block cache size
     - Stored in block cache by default

  ## Usage Examples

      # Get recommended configuration based on system RAM
      config = TripleStore.Config.RocksDB.recommended()

      # Get configuration for specific memory budget
      config = TripleStore.Config.RocksDB.for_memory_budget(8 * 1024 * 1024 * 1024)  # 8 GB

      # Use preset configurations
      config = TripleStore.Config.RocksDB.preset(:production_high_memory)

  ## Column Family Considerations

  The triple store uses 6 column families (id2str, str2id, spo, pos, osp, derived).
  Memory settings should account for per-CF overhead:

  - Write buffers: 6 CFs × 2 buffers × buffer_size
  - Block cache: Shared across all column families

  """

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "RocksDB memory configuration"
  @type t :: %{
          block_cache_size: non_neg_integer(),
          write_buffer_size: non_neg_integer(),
          max_write_buffer_number: pos_integer(),
          max_open_files: integer(),
          target_file_size_base: non_neg_integer(),
          max_bytes_for_level_base: non_neg_integer()
        }

  @typedoc "Configuration preset name"
  @type preset_name ::
          :development
          | :production_low_memory
          | :production_high_memory
          | :write_heavy
          | :bulk_load

  # ===========================================================================
  # Constants
  # ===========================================================================

  # Import shared helpers
  import TripleStore.Config.Helpers, only: [clamp: 3]
  alias TripleStore.Config.Helpers

  # Number of column families (matching TripleStore.Config.ColumnFamily.column_family_names())
  # Used for memory calculations. This should stay in sync with the ColumnFamily module.
  @num_column_families 6

  # Minimum and maximum block cache sizes
  @min_block_cache_size 64 * 1024 * 1024
  @max_block_cache_size 32 * 1024 * 1024 * 1024

  # Target allocation percentages
  @block_cache_ram_percentage 0.40

  # ===========================================================================
  # Presets
  # ===========================================================================

  @presets %{
    development: %{
      block_cache_size: 128 * 1024 * 1024,
      write_buffer_size: 32 * 1024 * 1024,
      max_write_buffer_number: 2,
      max_open_files: 256,
      target_file_size_base: 32 * 1024 * 1024,
      max_bytes_for_level_base: 256 * 1024 * 1024
    },
    production_low_memory: %{
      block_cache_size: 256 * 1024 * 1024,
      write_buffer_size: 32 * 1024 * 1024,
      max_write_buffer_number: 2,
      max_open_files: 512,
      target_file_size_base: 64 * 1024 * 1024,
      max_bytes_for_level_base: 512 * 1024 * 1024
    },
    production_high_memory: %{
      block_cache_size: 4 * 1024 * 1024 * 1024,
      write_buffer_size: 128 * 1024 * 1024,
      max_write_buffer_number: 4,
      max_open_files: 4096,
      target_file_size_base: 128 * 1024 * 1024,
      max_bytes_for_level_base: 1024 * 1024 * 1024
    },
    write_heavy: %{
      block_cache_size: 1024 * 1024 * 1024,
      write_buffer_size: 256 * 1024 * 1024,
      max_write_buffer_number: 4,
      max_open_files: 2048,
      target_file_size_base: 256 * 1024 * 1024,
      max_bytes_for_level_base: 2 * 1024 * 1024 * 1024
    },
    # Optimized for bulk loading large datasets (>1M triples)
    # Memory usage: ~18 GB (6 CFs × 6 buffers × 512 MB)
    # Use with Loader bulk_mode: true for maximum throughput
    bulk_load: %{
      block_cache_size: 512 * 1024 * 1024,
      write_buffer_size: 512 * 1024 * 1024,
      max_write_buffer_number: 6,
      max_open_files: 8192,
      target_file_size_base: 512 * 1024 * 1024,
      max_bytes_for_level_base: 4 * 1024 * 1024 * 1024
    }
  }

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Returns the recommended RocksDB configuration based on system RAM.

  This function detects available system memory and calculates optimal
  settings following the 40% block cache guideline.

  ## Options

  - `:available_memory` - Override detected system memory (bytes)
  - `:block_cache_percentage` - Override default 40% allocation

  ## Examples

      config = TripleStore.Config.RocksDB.recommended()
      # => %{block_cache_size: 3_221_225_472, ...}  # for 8GB system

      config = TripleStore.Config.RocksDB.recommended(available_memory: 16 * 1024 * 1024 * 1024)
      # => %{block_cache_size: 6_442_450_944, ...}  # for 16GB specified

  """
  @spec recommended(keyword()) :: t()
  def recommended(opts \\ []) do
    available_memory = Keyword.get(opts, :available_memory, detect_system_memory())
    percentage = Keyword.get(opts, :block_cache_percentage, @block_cache_ram_percentage)

    for_memory_budget(available_memory, percentage)
  end

  @doc """
  Returns RocksDB configuration for a specific memory budget.

  Calculates optimal settings given the total memory available for RocksDB,
  distributing memory between block cache and write buffers.

  ## Arguments

  - `available_bytes` - Total memory available for RocksDB (bytes)
  - `block_cache_percentage` - Percentage for block cache (0.0-1.0, default 0.40)

  ## Examples

      # Configure for 4 GB memory budget
      config = TripleStore.Config.RocksDB.for_memory_budget(4 * 1024 * 1024 * 1024)

      # Configure with 50% block cache
      config = TripleStore.Config.RocksDB.for_memory_budget(4 * 1024 * 1024 * 1024, 0.50)

  """
  @spec for_memory_budget(non_neg_integer(), float()) :: t()
  def for_memory_budget(available_bytes, block_cache_percentage \\ @block_cache_ram_percentage) do
    # Calculate block cache size (clamped to min/max)
    raw_block_cache = trunc(available_bytes * block_cache_percentage)
    block_cache_size = clamp(raw_block_cache, @min_block_cache_size, @max_block_cache_size)

    # Calculate write buffer size based on remaining memory
    remaining_memory = available_bytes - block_cache_size
    write_buffer_config = calculate_write_buffer_config(remaining_memory)

    # Calculate max_open_files based on system limits
    max_open_files = calculate_max_open_files()

    # Calculate level configuration based on write buffer size
    target_file_size = write_buffer_config.write_buffer_size
    max_bytes_for_level_base = target_file_size * 10

    %{
      block_cache_size: block_cache_size,
      write_buffer_size: write_buffer_config.write_buffer_size,
      max_write_buffer_number: write_buffer_config.max_write_buffer_number,
      max_open_files: max_open_files,
      target_file_size_base: target_file_size,
      max_bytes_for_level_base: max_bytes_for_level_base
    }
  end

  @doc """
  Returns a preset configuration.

  Available presets:

  - `:development` - Small caches for local development (128 MB block cache)
  - `:production_low_memory` - Conservative settings for <4 GB systems
  - `:production_high_memory` - Aggressive caching for 16+ GB systems
  - `:write_heavy` - Optimized for bulk loading and frequent updates
  - `:bulk_load` - Maximum write throughput for large imports (~18 GB RAM)

  ## Memory Requirements

  The `:bulk_load` preset requires significant memory:

  - Write buffers: 6 CFs × 6 buffers × 512 MB = ~18 GB
  - Block cache: 512 MB
  - Total: ~19 GB RAM minimum

  Use this preset only for one-time bulk imports on systems with 32+ GB RAM.
  For regular write-heavy workloads, use `:write_heavy` instead.

  ## Examples

      config = TripleStore.Config.RocksDB.preset(:production_high_memory)

      # For bulk loading on high-memory systems
      config = TripleStore.Config.RocksDB.preset(:bulk_load)

  """
  @spec preset(preset_name()) :: t()
  def preset(name) when is_map_key(@presets, name) do
    Map.fetch!(@presets, name)
  end

  @doc """
  Lists all available preset names.

  ## Examples

      [:development, :production_low_memory, ...] = TripleStore.Config.RocksDB.preset_names()

  """
  @spec preset_names() :: [preset_name()]
  def preset_names do
    Map.keys(@presets)
  end

  @doc """
  Returns the default configuration.

  Uses system detection to provide reasonable defaults.
  """
  @spec default() :: t()
  def default do
    recommended()
  end

  @doc """
  Calculates block cache size based on available RAM.

  Follows the 40% guideline with min/max bounds.

  ## Arguments

  - `available_bytes` - Available RAM in bytes

  ## Examples

      # For 8 GB system
      size = TripleStore.Config.RocksDB.calculate_block_cache_size(8 * 1024 * 1024 * 1024)
      # => 3_221_225_472 (3 GB)

  """
  @spec calculate_block_cache_size(non_neg_integer()) :: non_neg_integer()
  def calculate_block_cache_size(available_bytes) do
    raw_size = trunc(available_bytes * @block_cache_ram_percentage)
    clamp(raw_size, @min_block_cache_size, @max_block_cache_size)
  end

  @doc """
  Detects available system memory in bytes.

  Attempts to read from:
  1. `:memsup` if available (requires `os_mon` application)
  2. `/proc/meminfo` on Linux
  3. `sysctl` on macOS/BSD
  4. Falls back to 1 GB default

  ## Examples

      bytes = TripleStore.Config.RocksDB.detect_system_memory()
      # => 17179869184  # 16 GB on a typical system

  """
  @spec detect_system_memory() :: non_neg_integer()
  def detect_system_memory do
    cond do
      # Try :memsup from os_mon
      memsup_available?() ->
        get_memsup_memory()

      # Try reading /proc/meminfo on Linux
      File.exists?("/proc/meminfo") ->
        read_linux_memory()

      # Try sysctl on macOS/BSD
      macos_or_bsd?() ->
        read_macos_memory()

      # Default fallback: 1 GB
      true ->
        1024 * 1024 * 1024
    end
  end

  @doc """
  Calculates max_open_files based on system limits.

  Checks the system file descriptor limit and returns a safe value.
  Uses at most 50% of available file descriptors.

  ## Examples

      max_files = TripleStore.Config.RocksDB.calculate_max_open_files()
      # => 4096 (on a system with 8192 fd limit)

  """
  @spec calculate_max_open_files() :: integer()
  def calculate_max_open_files do
    case get_system_file_limit() do
      {:ok, limit} ->
        # Use at most 50% of available file descriptors
        # RocksDB uses -1 for unlimited, but we set reasonable bounds
        clamped = clamp(div(limit, 2), 256, 65_536)
        clamped

      :error ->
        # Conservative default
        1024
    end
  end

  @doc """
  Estimates total memory usage for a given configuration.

  This includes:
  - Block cache
  - Write buffers (per column family)
  - Overhead for indices and bloom filters

  ## Examples

      config = TripleStore.Config.RocksDB.preset(:production_high_memory)
      bytes = TripleStore.Config.RocksDB.estimate_memory_usage(config)
      # => 7_516_192_768 (about 7 GB)

  """
  @spec estimate_memory_usage(t()) :: non_neg_integer()
  def estimate_memory_usage(config) do
    block_cache = config.block_cache_size

    # Write buffers: per CF × max buffers × buffer size
    write_buffer_total =
      @num_column_families * config.max_write_buffer_number * config.write_buffer_size

    # Overhead estimate: ~10% of block cache for indices/filters
    overhead = div(block_cache, 10)

    block_cache + write_buffer_total + overhead
  end

  @doc """
  Formats a byte count as a human-readable string.

  ## Examples

      TripleStore.Config.RocksDB.format_bytes(1024 * 1024 * 1024)
      # => "1.00 GB"

      TripleStore.Config.RocksDB.format_bytes(64 * 1024 * 1024)
      # => "64.00 MB"

  """
  @spec format_bytes(non_neg_integer()) :: String.t()
  defdelegate format_bytes(bytes), to: Helpers

  @doc """
  Generates a human-readable summary of a configuration.

  ## Examples

      config = TripleStore.Config.RocksDB.recommended()
      IO.puts(TripleStore.Config.RocksDB.format_summary(config))

  """
  @spec format_summary(t()) :: String.t()
  def format_summary(config) do
    estimated_usage = estimate_memory_usage(config)

    """
    RocksDB Memory Configuration
    ============================

    Block Cache:       #{format_bytes(config.block_cache_size)}
    Write Buffer:      #{format_bytes(config.write_buffer_size)} × #{config.max_write_buffer_number} buffers
    Max Open Files:    #{config.max_open_files}
    Target File Size:  #{format_bytes(config.target_file_size_base)}
    L0 Max Size:       #{format_bytes(config.max_bytes_for_level_base)}

    Estimated Total Memory Usage: #{format_bytes(estimated_usage)}
    (6 column families × #{config.max_write_buffer_number} write buffers each)
    """
  end

  @doc """
  Validates a configuration map.

  Returns `:ok` if valid, or `{:error, reason}` if invalid.

  ## Examples

      config = TripleStore.Config.RocksDB.recommended()
      :ok = TripleStore.Config.RocksDB.validate(config)

  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate(config) do
    with :ok <- Helpers.validate_non_negative(config.block_cache_size, "block_cache_size"),
         :ok <- Helpers.validate_positive(config.write_buffer_size, "write_buffer_size"),
         :ok <-
           Helpers.validate_min(config.max_write_buffer_number, 1, "max_write_buffer_number"),
         :ok <- validate_integer(config.max_open_files, "max_open_files"),
         :ok <- Helpers.validate_positive(config.target_file_size_base, "target_file_size_base") do
      Helpers.validate_positive(config.max_bytes_for_level_base, "max_bytes_for_level_base")
    end
  end

  @doc """
  Returns the number of column families used for memory calculations.

  This value should match `length(ColumnFamily.column_family_names())`.
  """
  @spec num_column_families() :: pos_integer()
  def num_column_families, do: @num_column_families

  defp validate_integer(value, _name) when is_integer(value), do: :ok
  defp validate_integer(_, name), do: {:error, "#{name} must be an integer"}

  # ===========================================================================
  # Private Functions
  # ===========================================================================

  defp calculate_write_buffer_config(remaining_memory) do
    # Reserve memory for write buffers across all column families
    # Each CF has max_write_buffer_number buffers
    # Total: 6 CFs × 2-4 buffers × buffer_size

    # Default to 2 buffers per CF, upgrade to 4 if we have enough memory
    {buffer_count, target_per_cf} =
      cond do
        # > 2 GB remaining: use 4 buffers per CF with larger buffers
        remaining_memory > 2 * 1024 * 1024 * 1024 ->
          {4, div(remaining_memory, @num_column_families * 4)}

        # > 512 MB remaining: use 2 buffers per CF
        remaining_memory > 512 * 1024 * 1024 ->
          {2, div(remaining_memory, @num_column_families * 2)}

        # Limited memory: use minimum viable configuration
        true ->
          {2, 32 * 1024 * 1024}
      end

    # Clamp write buffer size to reasonable bounds
    clamped_buffer = clamp(target_per_cf, 16 * 1024 * 1024, 512 * 1024 * 1024)

    %{
      write_buffer_size: clamped_buffer,
      max_write_buffer_number: buffer_count
    }
  end

  defp memsup_available? do
    case :code.which(:memsup) do
      :non_existing -> false
      _ -> Process.whereis(:memsup) != nil
    end
  end

  defp get_memsup_memory do
    # Use apply to avoid compile-time warning when :memsup is not available
    # credo:disable-for-next-line Credo.Check.Refactor.Apply
    case apply(:memsup, :get_system_memory_data, []) do
      data when is_list(data) ->
        # Try total_memory first, fall back to system_total_memory
        Keyword.get(data, :total_memory) ||
          Keyword.get(data, :system_total_memory, 1024 * 1024 * 1024)

      _ ->
        1024 * 1024 * 1024
    end
  catch
    _, _ -> 1024 * 1024 * 1024
  end

  defp read_linux_memory do
    case File.read("/proc/meminfo") do
      {:ok, content} ->
        case Regex.run(~r/MemTotal:\s+(\d+)\s+kB/, content) do
          [_, kb_str] ->
            {kb, _} = Integer.parse(kb_str)
            kb * 1024

          _ ->
            1024 * 1024 * 1024
        end

      _ ->
        1024 * 1024 * 1024
    end
  end

  defp macos_or_bsd? do
    case :os.type() do
      {:unix, os} when os in [:darwin, :freebsd, :openbsd, :netbsd] -> true
      _ -> false
    end
  end

  defp read_macos_memory do
    case System.cmd("sysctl", ["-n", "hw.memsize"], stderr_to_stdout: true) do
      {output, 0} ->
        output
        |> String.trim()
        |> Integer.parse()
        |> case do
          {bytes, _} -> bytes
          :error -> 1024 * 1024 * 1024
        end

      _ ->
        1024 * 1024 * 1024
    end
  rescue
    _ -> 1024 * 1024 * 1024
  end

  defp get_system_file_limit do
    case System.cmd("ulimit", ["-n"], stderr_to_stdout: true) do
      {output, 0} ->
        output
        |> String.trim()
        |> Integer.parse()
        |> case do
          {limit, _} -> {:ok, limit}
          :error -> :error
        end

      _ ->
        # Try reading from /proc on Linux
        read_linux_file_limit()
    end
  rescue
    _ -> :error
  end

  defp read_linux_file_limit do
    case File.read("/proc/sys/fs/file-max") do
      {:ok, content} ->
        content
        |> String.trim()
        |> Integer.parse()
        |> case do
          {limit, _} -> {:ok, limit}
          :error -> :error
        end

      _ ->
        :error
    end
  end
end
