defmodule TripleStore.Config.Compaction do
  @moduledoc """
  Compaction configuration for RocksDB.

  See `TripleStore.Config.Helpers` for shared utilities (format_bytes, validation helpers).

  This module provides configuration for RocksDB's LSM-tree compaction process,
  which is critical for maintaining read performance and managing disk space.

  ## LSM-Tree Background

  RocksDB uses a Log-Structured Merge-tree (LSM) with multiple levels:

  - **Level 0 (L0)**: Memtables flushed directly, may have overlapping keys
  - **Level 1-N**: Sorted runs with non-overlapping keys within each level
  - **Compaction**: Merges levels to maintain sorted structure and reclaim space

  ## Compaction Styles

  ### Level Compaction (Recommended)

  The default and most common style:

  - Each level is ~10x larger than the previous
  - Data moves from L0 → L1 → L2 → ... → Lmax
  - Good balance of read/write amplification
  - Best for general workloads

  ### Universal Compaction

  Alternative for write-heavy workloads:

  - All SSTables in a single sorted run
  - Lower write amplification
  - Higher space amplification
  - Best for write-heavy, read-light workloads

  ## Rate Limiting

  Compaction can consume significant I/O bandwidth. Rate limiting prevents
  compaction from starving foreground operations:

  - **Bytes per second limit**: Caps compaction I/O throughput
  - **Refill period**: How often the rate limiter refills its quota
  - **Fairness**: Balance between compaction and flush operations

  ## Configuration Examples

      # Get default compaction configuration
      config = TripleStore.Config.Compaction.default()

      # Get preset for write-heavy workload
      config = TripleStore.Config.Compaction.preset(:write_heavy)

      # Custom configuration
      config = TripleStore.Config.Compaction.custom(
        level_compaction_dynamic_level_bytes: true,
        max_bytes_for_level_base: 256 * 1024 * 1024,
        rate_limit_bytes_per_sec: 100 * 1024 * 1024
      )

  """

  # ===========================================================================
  # Types
  # ===========================================================================

  @typedoc "Compaction style"
  @type compaction_style :: :level | :universal | :fifo

  @typedoc "Compaction configuration"
  @type t :: %{
          # Compaction style
          style: compaction_style(),

          # Level compaction settings
          level_compaction_dynamic_level_bytes: boolean(),
          max_bytes_for_level_base: non_neg_integer(),
          max_bytes_for_level_multiplier: float(),
          num_levels: pos_integer(),

          # L0 settings
          level0_file_num_compaction_trigger: pos_integer(),
          level0_slowdown_writes_trigger: pos_integer(),
          level0_stop_writes_trigger: pos_integer(),

          # Rate limiting
          rate_limit_bytes_per_sec: non_neg_integer(),
          rate_limit_refill_period_us: pos_integer(),
          rate_limit_fairness: pos_integer(),

          # Background jobs
          max_background_compactions: pos_integer(),
          max_background_flushes: pos_integer(),

          # File settings
          target_file_size_base: non_neg_integer(),
          target_file_size_multiplier: pos_integer()
        }

  @typedoc "Preset name"
  @type preset_name :: :default | :write_heavy | :read_heavy | :balanced | :low_latency

  # ===========================================================================
  # Constants
  # ===========================================================================

  alias TripleStore.Config.Helpers

  # Default level multiplier (each level is 10x larger)
  @default_level_multiplier 10.0

  # Default number of levels (0-6 = 7 levels)
  @default_num_levels 7

  # Default L0 triggers
  @default_l0_compaction_trigger 4
  @default_l0_slowdown_trigger 20
  @default_l0_stop_trigger 36

  # Default rate limit (0 = unlimited)
  @default_rate_limit_bytes_per_sec 0

  # Rate limit refill period (100ms)
  @default_refill_period_us 100_000

  # Default fairness (higher = more fair to compaction)
  @default_fairness 10

  # ===========================================================================
  # Presets
  # ===========================================================================

  @presets %{
    default: %{
      style: :level,
      level_compaction_dynamic_level_bytes: true,
      max_bytes_for_level_base: 256 * 1024 * 1024,
      max_bytes_for_level_multiplier: @default_level_multiplier,
      num_levels: @default_num_levels,
      level0_file_num_compaction_trigger: @default_l0_compaction_trigger,
      level0_slowdown_writes_trigger: @default_l0_slowdown_trigger,
      level0_stop_writes_trigger: @default_l0_stop_trigger,
      rate_limit_bytes_per_sec: @default_rate_limit_bytes_per_sec,
      rate_limit_refill_period_us: @default_refill_period_us,
      rate_limit_fairness: @default_fairness,
      max_background_compactions: 4,
      max_background_flushes: 2,
      target_file_size_base: 64 * 1024 * 1024,
      target_file_size_multiplier: 1
    },
    write_heavy: %{
      style: :level,
      level_compaction_dynamic_level_bytes: true,
      max_bytes_for_level_base: 512 * 1024 * 1024,
      max_bytes_for_level_multiplier: @default_level_multiplier,
      num_levels: @default_num_levels,
      # Higher L0 triggers to reduce compaction during writes
      level0_file_num_compaction_trigger: 8,
      level0_slowdown_writes_trigger: 32,
      level0_stop_writes_trigger: 48,
      # Rate limit to prevent I/O saturation
      rate_limit_bytes_per_sec: 200 * 1024 * 1024,
      rate_limit_refill_period_us: @default_refill_period_us,
      rate_limit_fairness: 5,
      # More background jobs for faster compaction
      max_background_compactions: 8,
      max_background_flushes: 4,
      target_file_size_base: 128 * 1024 * 1024,
      target_file_size_multiplier: 1
    },
    read_heavy: %{
      style: :level,
      level_compaction_dynamic_level_bytes: true,
      max_bytes_for_level_base: 128 * 1024 * 1024,
      max_bytes_for_level_multiplier: @default_level_multiplier,
      num_levels: @default_num_levels,
      # Lower L0 triggers for faster compaction (better read perf)
      level0_file_num_compaction_trigger: 2,
      level0_slowdown_writes_trigger: 12,
      level0_stop_writes_trigger: 24,
      rate_limit_bytes_per_sec: @default_rate_limit_bytes_per_sec,
      rate_limit_refill_period_us: @default_refill_period_us,
      rate_limit_fairness: @default_fairness,
      max_background_compactions: 4,
      max_background_flushes: 2,
      # Smaller files for faster point lookups
      target_file_size_base: 32 * 1024 * 1024,
      target_file_size_multiplier: 1
    },
    balanced: %{
      style: :level,
      level_compaction_dynamic_level_bytes: true,
      max_bytes_for_level_base: 256 * 1024 * 1024,
      max_bytes_for_level_multiplier: @default_level_multiplier,
      num_levels: @default_num_levels,
      level0_file_num_compaction_trigger: 4,
      level0_slowdown_writes_trigger: 16,
      level0_stop_writes_trigger: 32,
      # Moderate rate limiting
      rate_limit_bytes_per_sec: 100 * 1024 * 1024,
      rate_limit_refill_period_us: @default_refill_period_us,
      rate_limit_fairness: @default_fairness,
      max_background_compactions: 4,
      max_background_flushes: 2,
      target_file_size_base: 64 * 1024 * 1024,
      target_file_size_multiplier: 1
    },
    low_latency: %{
      style: :level,
      level_compaction_dynamic_level_bytes: true,
      max_bytes_for_level_base: 64 * 1024 * 1024,
      max_bytes_for_level_multiplier: @default_level_multiplier,
      num_levels: @default_num_levels,
      # Very aggressive L0 compaction
      level0_file_num_compaction_trigger: 1,
      level0_slowdown_writes_trigger: 8,
      level0_stop_writes_trigger: 16,
      # Strong rate limiting to prevent latency spikes
      rate_limit_bytes_per_sec: 50 * 1024 * 1024,
      rate_limit_refill_period_us: 50_000,
      rate_limit_fairness: 20,
      max_background_compactions: 2,
      max_background_flushes: 1,
      # Small files for predictable latency
      target_file_size_base: 16 * 1024 * 1024,
      target_file_size_multiplier: 1
    }
  }

  # ===========================================================================
  # Public API
  # ===========================================================================

  @doc """
  Returns the default compaction configuration.

  Uses level compaction with dynamic level sizing, which automatically
  adjusts level sizes based on the total data size.

  ## Examples

      config = TripleStore.Config.Compaction.default()

  """
  @spec default() :: t()
  def default do
    Map.fetch!(@presets, :default)
  end

  @doc """
  Returns a preset compaction configuration.

  Available presets:

  - `:default` - Balanced defaults suitable for most workloads
  - `:write_heavy` - Optimized for bulk loading and frequent updates
  - `:read_heavy` - Aggressive compaction for best read performance
  - `:balanced` - Middle ground with moderate rate limiting
  - `:low_latency` - Minimizes latency spikes from compaction

  ## Examples

      config = TripleStore.Config.Compaction.preset(:write_heavy)

  """
  @spec preset(preset_name()) :: t()
  def preset(name) when is_map_key(@presets, name) do
    Map.fetch!(@presets, name)
  end

  @doc """
  Lists all available preset names.
  """
  @spec preset_names() :: [preset_name()]
  def preset_names do
    Map.keys(@presets)
  end

  @doc """
  Creates a custom compaction configuration.

  Starts with the default configuration and applies the provided overrides.

  ## Options

  - `:style` - Compaction style (`:level`, `:universal`, `:fifo`)
  - `:level_compaction_dynamic_level_bytes` - Enable dynamic level sizing
  - `:max_bytes_for_level_base` - Size of L1 in bytes
  - `:max_bytes_for_level_multiplier` - Level size multiplier
  - `:num_levels` - Number of levels (default: 7)
  - `:level0_file_num_compaction_trigger` - Files in L0 to trigger compaction
  - `:level0_slowdown_writes_trigger` - Files in L0 to slow down writes
  - `:level0_stop_writes_trigger` - Files in L0 to stop writes
  - `:rate_limit_bytes_per_sec` - Compaction I/O rate limit (0 = unlimited)
  - `:max_background_compactions` - Max concurrent compaction jobs
  - `:max_background_flushes` - Max concurrent flush jobs

  ## Examples

      config = TripleStore.Config.Compaction.custom(
        rate_limit_bytes_per_sec: 50 * 1024 * 1024,
        max_background_compactions: 2
      )

  """
  @spec custom(keyword()) :: t()
  def custom(opts \\ []) do
    default_config = default()

    Enum.reduce(opts, default_config, fn {key, value}, acc ->
      if Map.has_key?(acc, key) do
        Map.put(acc, key, value)
      else
        acc
      end
    end)
  end

  @doc """
  Creates a custom compaction configuration with validation.

  Same as `custom/1` but validates the configuration and raises on error.

  ## Examples

      # Valid configuration
      config = TripleStore.Config.Compaction.custom!(rate_limit_bytes_per_sec: 50 * 1024 * 1024)

      # Invalid configuration raises
      TripleStore.Config.Compaction.custom!(max_background_compactions: -1)
      # => ** (ArgumentError) max_background_compactions must be a positive integer

  """
  @spec custom!(keyword()) :: t()
  def custom!(opts \\ []) do
    config = custom(opts)

    case validate(config) do
      :ok -> config
      {:error, reason} -> raise ArgumentError, reason
    end
  end

  @doc """
  Calculates the size of each level based on configuration.

  Returns a map with the target size for each level.

  ## Examples

      config = TripleStore.Config.Compaction.default()
      sizes = TripleStore.Config.Compaction.level_sizes(config)
      # => %{0 => :variable, 1 => 268435456, 2 => 2684354560, ...}

  """
  @spec level_sizes(t()) :: %{non_neg_integer() => non_neg_integer() | :variable}
  def level_sizes(config) do
    base = config.max_bytes_for_level_base
    multiplier = config.max_bytes_for_level_multiplier

    Enum.reduce(0..(config.num_levels - 1), %{}, fn level, acc ->
      size =
        case level do
          0 -> :variable
          1 -> base
          n -> trunc(base * :math.pow(multiplier, n - 1))
        end

      Map.put(acc, level, size)
    end)
  end

  @doc """
  Calculates the total capacity of the LSM-tree.

  This is the theoretical maximum data size before the tree is "full"
  and needs more levels or larger level sizes.

  ## Examples

      config = TripleStore.Config.Compaction.default()
      capacity = TripleStore.Config.Compaction.total_capacity(config)
      # => 2_863_311_530_598 (about 2.6 TB)

  """
  @spec total_capacity(t()) :: non_neg_integer()
  def total_capacity(config) do
    sizes = level_sizes(config)

    sizes
    |> Enum.filter(fn {_level, size} -> is_integer(size) end)
    |> Enum.map(fn {_level, size} -> size end)
    |> Enum.sum()
  end

  @doc """
  Returns rate limiting configuration.

  ## Examples

      config = TripleStore.Config.Compaction.preset(:balanced)
      rate = TripleStore.Config.Compaction.rate_limit_config(config)
      # => %{bytes_per_sec: 104857600, refill_period_us: 100000, fairness: 10}

  """
  @spec rate_limit_config(t()) :: map()
  def rate_limit_config(config) do
    %{
      bytes_per_sec: config.rate_limit_bytes_per_sec,
      refill_period_us: config.rate_limit_refill_period_us,
      fairness: config.rate_limit_fairness,
      enabled: config.rate_limit_bytes_per_sec > 0
    }
  end

  @doc """
  Returns background job configuration.

  ## Examples

      config = TripleStore.Config.Compaction.default()
      jobs = TripleStore.Config.Compaction.background_jobs(config)
      # => %{compactions: 4, flushes: 2, total: 6}

  """
  @spec background_jobs(t()) :: map()
  def background_jobs(config) do
    %{
      compactions: config.max_background_compactions,
      flushes: config.max_background_flushes,
      total: config.max_background_compactions + config.max_background_flushes
    }
  end

  @doc """
  Returns L0 trigger configuration.

  These thresholds control when L0 compaction is triggered and when
  writes are throttled or stopped.

  ## Examples

      config = TripleStore.Config.Compaction.default()
      triggers = TripleStore.Config.Compaction.l0_triggers(config)
      # => %{compaction: 4, slowdown: 20, stop: 36}

  """
  @spec l0_triggers(t()) :: map()
  def l0_triggers(config) do
    %{
      compaction: config.level0_file_num_compaction_trigger,
      slowdown: config.level0_slowdown_writes_trigger,
      stop: config.level0_stop_writes_trigger
    }
  end

  @doc """
  Validates a compaction configuration.

  ## Examples

      config = TripleStore.Config.Compaction.default()
      :ok = TripleStore.Config.Compaction.validate(config)

  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate(config) do
    with :ok <- validate_style(config.style),
         :ok <-
           Helpers.validate_positive(config.max_bytes_for_level_base, "max_bytes_for_level_base"),
         :ok <- Helpers.validate_positive(config.num_levels, "num_levels"),
         :ok <-
           Helpers.validate_positive(
             config.level0_file_num_compaction_trigger,
             "level0_file_num_compaction_trigger"
           ),
         :ok <- validate_l0_triggers(config),
         :ok <-
           Helpers.validate_non_negative(
             config.rate_limit_bytes_per_sec,
             "rate_limit_bytes_per_sec"
           ),
         :ok <-
           Helpers.validate_positive(
             config.max_background_compactions,
             "max_background_compactions"
           ),
         :ok <- Helpers.validate_positive(config.max_background_flushes, "max_background_flushes") do
      Helpers.validate_positive(config.target_file_size_base, "target_file_size_base")
    end
  end

  defp validate_style(style) when style in [:level, :universal, :fifo], do: :ok
  defp validate_style(style), do: {:error, "invalid compaction style: #{inspect(style)}"}

  defp validate_l0_triggers(config) do
    cond do
      config.level0_slowdown_writes_trigger <= config.level0_file_num_compaction_trigger ->
        {:error, "level0_slowdown_writes_trigger must be > level0_file_num_compaction_trigger"}

      config.level0_stop_writes_trigger <= config.level0_slowdown_writes_trigger ->
        {:error, "level0_stop_writes_trigger must be > level0_slowdown_writes_trigger"}

      true ->
        :ok
    end
  end

  @doc """
  Estimates compaction write amplification.

  Write amplification is the ratio of bytes written to storage vs bytes
  written by the application. Lower is better.

  For level compaction: ~10-30x typical
  For universal compaction: ~5-10x typical

  ## Examples

      config = TripleStore.Config.Compaction.default()
      wa = TripleStore.Config.Compaction.estimate_write_amplification(config)
      # => %{min: 10, typical: 15, max: 30}

  """
  @spec estimate_write_amplification(t()) :: map()
  def estimate_write_amplification(config) do
    case config.style do
      :level ->
        # Level compaction: roughly (num_levels - 1) * level_multiplier / 2
        multiplier = config.max_bytes_for_level_multiplier
        levels = config.num_levels

        %{
          min: trunc(levels),
          typical: trunc((levels - 1) * multiplier / 2),
          max: trunc((levels - 1) * multiplier)
        }

      :universal ->
        %{min: 5, typical: 8, max: 15}

      :fifo ->
        %{min: 1, typical: 1, max: 1}
    end
  end

  @doc """
  Estimates compaction read amplification.

  Read amplification is the number of disk reads needed per application read.
  Lower is better.

  For level compaction: ~num_levels typical (with bloom filters: ~1-2)

  ## Examples

      config = TripleStore.Config.Compaction.default()
      ra = TripleStore.Config.Compaction.estimate_read_amplification(config)
      # => %{without_bloom: 7, with_bloom: 1.5}

  """
  @spec estimate_read_amplification(t()) :: map()
  def estimate_read_amplification(config) do
    case config.style do
      :level ->
        %{
          without_bloom: config.num_levels,
          with_bloom: 1.5
        }

      :universal ->
        # Universal can have more runs to check
        %{
          without_bloom: config.num_levels * 2,
          with_bloom: 2.0
        }

      :fifo ->
        %{without_bloom: 1, with_bloom: 1}
    end
  end

  @doc """
  Returns compaction monitoring metrics to track.

  These metrics should be monitored via telemetry to detect compaction issues.

  ## Examples

      metrics = TripleStore.Config.Compaction.monitoring_metrics()

  """
  @spec monitoring_metrics() :: [map()]
  def monitoring_metrics do
    [
      %{
        name: :compaction_pending_bytes,
        description: "Bytes pending compaction",
        unit: :bytes,
        warning_threshold: 1024 * 1024 * 1024,
        critical_threshold: 10 * 1024 * 1024 * 1024
      },
      %{
        name: :compaction_pending_files,
        description: "Files pending compaction",
        unit: :count,
        warning_threshold: 50,
        critical_threshold: 100
      },
      %{
        name: :level0_file_count,
        description: "Number of files in L0",
        unit: :count,
        warning_threshold: 10,
        critical_threshold: 20
      },
      %{
        name: :compaction_cpu_seconds,
        description: "CPU time spent on compaction",
        unit: :seconds,
        warning_threshold: nil,
        critical_threshold: nil
      },
      %{
        name: :compaction_bytes_written,
        description: "Bytes written during compaction",
        unit: :bytes,
        warning_threshold: nil,
        critical_threshold: nil
      },
      %{
        name: :compaction_bytes_read,
        description: "Bytes read during compaction",
        unit: :bytes,
        warning_threshold: nil,
        critical_threshold: nil
      },
      %{
        name: :write_stall_duration,
        description: "Time spent stalled waiting for compaction",
        unit: :microseconds,
        warning_threshold: 1_000_000,
        critical_threshold: 10_000_000
      }
    ]
  end

  @doc """
  Returns a list of metrics that indicate compaction lag.

  Compaction lag occurs when compaction can't keep up with writes,
  leading to increased L0 file count and potential write stalls.

  ## Examples

      lag_indicators = TripleStore.Config.Compaction.lag_indicators()

  """
  @spec lag_indicators() :: [map()]
  def lag_indicators do
    [
      %{
        metric: :level0_file_count,
        threshold_warning: 10,
        threshold_critical: 20,
        action: "Increase max_background_compactions or reduce write rate"
      },
      %{
        metric: :compaction_pending_bytes,
        threshold_warning: 1024 * 1024 * 1024,
        threshold_critical: 10 * 1024 * 1024 * 1024,
        action: "Increase rate_limit_bytes_per_sec or reduce write rate"
      },
      %{
        metric: :write_stall_count,
        threshold_warning: 1,
        threshold_critical: 10,
        action: "Increase L0 triggers or add more background compaction threads"
      }
    ]
  end

  @doc """
  Generates a human-readable summary of the compaction configuration.

  ## Examples

      config = TripleStore.Config.Compaction.preset(:balanced)
      IO.puts(TripleStore.Config.Compaction.format_summary(config))

  """
  @spec format_summary(t()) :: String.t()
  def format_summary(config) do
    rate_limit =
      if config.rate_limit_bytes_per_sec > 0 do
        format_bytes(config.rate_limit_bytes_per_sec) <> "/s"
      else
        "Unlimited"
      end

    wa = estimate_write_amplification(config)
    capacity = total_capacity(config)

    """
    Compaction Configuration
    ========================

    Style: #{config.style}
    Dynamic Level Bytes: #{config.level_compaction_dynamic_level_bytes}

    Level Configuration:
      Levels: #{config.num_levels}
      L1 Size: #{format_bytes(config.max_bytes_for_level_base)}
      Multiplier: #{config.max_bytes_for_level_multiplier}x
      Total Capacity: #{format_bytes(capacity)}

    L0 Triggers:
      Compaction: #{config.level0_file_num_compaction_trigger} files
      Slowdown: #{config.level0_slowdown_writes_trigger} files
      Stop: #{config.level0_stop_writes_trigger} files

    Rate Limiting:
      Limit: #{rate_limit}
      Fairness: #{config.rate_limit_fairness}

    Background Jobs:
      Compactions: #{config.max_background_compactions}
      Flushes: #{config.max_background_flushes}

    Estimated Write Amplification: #{wa.min}x - #{wa.max}x (typical: #{wa.typical}x)
    """
  end

  # ===========================================================================
  # Shared Helpers
  # ===========================================================================

  @doc """
  Formats a byte count as a human-readable string.

  Delegated to `TripleStore.Config.Helpers.format_bytes/1`.
  """
  @spec format_bytes(non_neg_integer()) :: String.t()
  defdelegate format_bytes(bytes), to: Helpers
end
