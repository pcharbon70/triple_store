defmodule TripleStore.Config.CompactionTest do
  use ExUnit.Case, async: true

  alias TripleStore.Config.Compaction

  describe "default/0" do
    test "returns level compaction style" do
      config = Compaction.default()

      assert config.style == :level
    end

    test "enables dynamic level bytes" do
      config = Compaction.default()

      assert config.level_compaction_dynamic_level_bytes == true
    end

    test "has 7 levels" do
      config = Compaction.default()

      assert config.num_levels == 7
    end

    test "has reasonable L0 triggers" do
      config = Compaction.default()

      assert config.level0_file_num_compaction_trigger == 4
      assert config.level0_slowdown_writes_trigger > config.level0_file_num_compaction_trigger
      assert config.level0_stop_writes_trigger > config.level0_slowdown_writes_trigger
    end

    test "has no rate limit by default" do
      config = Compaction.default()

      assert config.rate_limit_bytes_per_sec == 0
    end
  end

  describe "preset/1" do
    test "returns default preset" do
      config = Compaction.preset(:default)

      assert config.style == :level
      assert config.max_background_compactions == 4
    end

    test "returns write_heavy preset with higher L0 triggers" do
      config = Compaction.preset(:write_heavy)

      assert config.level0_file_num_compaction_trigger > 4
      assert config.max_background_compactions > 4
      assert config.rate_limit_bytes_per_sec > 0
    end

    test "returns read_heavy preset with lower L0 triggers" do
      config = Compaction.preset(:read_heavy)

      assert config.level0_file_num_compaction_trigger < 4
      assert config.target_file_size_base < 64 * 1024 * 1024
    end

    test "returns balanced preset with rate limiting" do
      config = Compaction.preset(:balanced)

      assert config.rate_limit_bytes_per_sec > 0
    end

    test "returns low_latency preset with aggressive settings" do
      config = Compaction.preset(:low_latency)

      assert config.level0_file_num_compaction_trigger == 1
      assert config.rate_limit_bytes_per_sec > 0
      assert config.target_file_size_base == 16 * 1024 * 1024
    end

    test "returns bulk_load preset with very high L0 triggers" do
      config = Compaction.preset(:bulk_load)

      # Very high L0 triggers to minimize compaction during bulk load
      assert config.level0_file_num_compaction_trigger == 16
      assert config.level0_slowdown_writes_trigger == 64
      assert config.level0_stop_writes_trigger == 128
      # No rate limiting
      assert config.rate_limit_bytes_per_sec == 0
      # Maximum background jobs
      assert config.max_background_compactions == 16
      assert config.max_background_flushes == 8
      # Large target file size
      assert config.target_file_size_base == 256 * 1024 * 1024
    end

    test "bulk_load preset has higher L0 triggers than write_heavy" do
      bulk_load = Compaction.preset(:bulk_load)
      write_heavy = Compaction.preset(:write_heavy)

      assert bulk_load.level0_file_num_compaction_trigger > write_heavy.level0_file_num_compaction_trigger
      assert bulk_load.level0_slowdown_writes_trigger > write_heavy.level0_slowdown_writes_trigger
      assert bulk_load.level0_stop_writes_trigger > write_heavy.level0_stop_writes_trigger
    end

    test "raises for unknown preset" do
      assert_raise FunctionClauseError, fn ->
        Compaction.preset(:unknown)
      end
    end
  end

  describe "preset_names/0" do
    test "returns all preset names" do
      names = Compaction.preset_names()

      assert :default in names
      assert :write_heavy in names
      assert :read_heavy in names
      assert :balanced in names
      assert :low_latency in names
      assert :bulk_load in names
    end
  end

  describe "custom/1" do
    test "uses defaults when no options provided" do
      config = Compaction.custom()

      assert config == Compaction.default()
    end

    test "overrides specific settings" do
      config = Compaction.custom(max_background_compactions: 8)

      assert config.max_background_compactions == 8
      assert config.style == :level
    end

    test "allows multiple overrides" do
      config =
        Compaction.custom(
          rate_limit_bytes_per_sec: 50 * 1024 * 1024,
          max_background_compactions: 2,
          level0_file_num_compaction_trigger: 2
        )

      assert config.rate_limit_bytes_per_sec == 50 * 1024 * 1024
      assert config.max_background_compactions == 2
      assert config.level0_file_num_compaction_trigger == 2
    end

    test "ignores unknown options" do
      config = Compaction.custom(unknown_option: 42)

      assert config == Compaction.default()
    end
  end

  describe "level_sizes/1" do
    test "returns variable for L0" do
      config = Compaction.default()
      sizes = Compaction.level_sizes(config)

      assert sizes[0] == :variable
    end

    test "returns base size for L1" do
      config = Compaction.default()
      sizes = Compaction.level_sizes(config)

      assert sizes[1] == config.max_bytes_for_level_base
    end

    test "levels grow by multiplier" do
      config = Compaction.default()
      sizes = Compaction.level_sizes(config)

      assert is_integer(sizes[2])
      assert sizes[2] > sizes[1]
      assert_in_delta sizes[2] / sizes[1], config.max_bytes_for_level_multiplier, 0.1
    end

    test "returns all levels" do
      config = Compaction.default()
      sizes = Compaction.level_sizes(config)

      assert map_size(sizes) == config.num_levels
    end
  end

  describe "total_capacity/1" do
    test "returns positive capacity" do
      config = Compaction.default()
      capacity = Compaction.total_capacity(config)

      assert capacity > 0
    end

    test "capacity increases with more levels" do
      config1 = Compaction.custom(num_levels: 5)
      config2 = Compaction.custom(num_levels: 7)

      assert Compaction.total_capacity(config2) > Compaction.total_capacity(config1)
    end

    test "capacity increases with larger base" do
      config1 = Compaction.custom(max_bytes_for_level_base: 128 * 1024 * 1024)
      config2 = Compaction.custom(max_bytes_for_level_base: 512 * 1024 * 1024)

      assert Compaction.total_capacity(config2) > Compaction.total_capacity(config1)
    end
  end

  describe "rate_limit_config/1" do
    test "returns rate limit details" do
      config = Compaction.preset(:balanced)
      rate = Compaction.rate_limit_config(config)

      assert rate.bytes_per_sec > 0
      assert rate.enabled == true
      assert is_integer(rate.refill_period_us)
      assert is_integer(rate.fairness)
    end

    test "reports disabled when limit is 0" do
      config = Compaction.default()
      rate = Compaction.rate_limit_config(config)

      assert rate.bytes_per_sec == 0
      assert rate.enabled == false
    end
  end

  describe "background_jobs/1" do
    test "returns job counts" do
      config = Compaction.default()
      jobs = Compaction.background_jobs(config)

      assert jobs.compactions == 4
      assert jobs.flushes == 2
      assert jobs.total == 6
    end

    test "total is sum of compactions and flushes" do
      config = Compaction.preset(:write_heavy)
      jobs = Compaction.background_jobs(config)

      assert jobs.total == jobs.compactions + jobs.flushes
    end
  end

  describe "l0_triggers/1" do
    test "returns trigger thresholds" do
      config = Compaction.default()
      triggers = Compaction.l0_triggers(config)

      assert triggers.compaction == 4
      assert triggers.slowdown == 20
      assert triggers.stop == 36
    end

    test "triggers are properly ordered" do
      for name <- Compaction.preset_names() do
        config = Compaction.preset(name)
        triggers = Compaction.l0_triggers(config)

        assert triggers.compaction < triggers.slowdown
        assert triggers.slowdown < triggers.stop
      end
    end
  end

  describe "validate/1" do
    test "validates default configuration" do
      config = Compaction.default()
      assert Compaction.validate(config) == :ok
    end

    test "validates all presets" do
      for name <- Compaction.preset_names() do
        config = Compaction.preset(name)
        assert Compaction.validate(config) == :ok
      end
    end

    test "rejects invalid style" do
      config = %{Compaction.default() | style: :invalid}
      assert {:error, _} = Compaction.validate(config)
    end

    test "rejects negative rate limit" do
      config = %{Compaction.default() | rate_limit_bytes_per_sec: -1}
      assert {:error, _} = Compaction.validate(config)
    end

    test "rejects zero background compactions" do
      config = %{Compaction.default() | max_background_compactions: 0}
      assert {:error, _} = Compaction.validate(config)
    end

    test "rejects invalid L0 trigger order" do
      config = %{
        Compaction.default()
        | level0_file_num_compaction_trigger: 10,
          level0_slowdown_writes_trigger: 5
      }

      assert {:error, _} = Compaction.validate(config)
    end
  end

  describe "estimate_write_amplification/1" do
    test "returns min, typical, and max for level compaction" do
      config = Compaction.default()
      wa = Compaction.estimate_write_amplification(config)

      assert wa.min > 0
      assert wa.typical > wa.min
      assert wa.max >= wa.typical
    end

    test "level compaction has higher WA than universal" do
      level_config = Compaction.custom(style: :level)
      universal_config = Compaction.custom(style: :universal)

      level_wa = Compaction.estimate_write_amplification(level_config)
      universal_wa = Compaction.estimate_write_amplification(universal_config)

      assert level_wa.typical > universal_wa.typical
    end

    test "FIFO has minimal write amplification" do
      config = Compaction.custom(style: :fifo)
      wa = Compaction.estimate_write_amplification(config)

      assert wa.min == 1
      assert wa.max == 1
    end
  end

  describe "estimate_read_amplification/1" do
    test "returns with and without bloom filter estimates" do
      config = Compaction.default()
      ra = Compaction.estimate_read_amplification(config)

      assert is_number(ra.without_bloom)
      assert is_number(ra.with_bloom)
      assert ra.with_bloom < ra.without_bloom
    end

    test "bloom filters significantly reduce read amplification" do
      config = Compaction.default()
      ra = Compaction.estimate_read_amplification(config)

      assert ra.with_bloom < ra.without_bloom / 2
    end
  end

  describe "monitoring_metrics/0" do
    test "returns list of metrics" do
      metrics = Compaction.monitoring_metrics()

      assert [_ | _] = metrics
    end

    test "each metric has required fields" do
      metrics = Compaction.monitoring_metrics()

      for metric <- metrics do
        assert Map.has_key?(metric, :name)
        assert Map.has_key?(metric, :description)
        assert Map.has_key?(metric, :unit)
      end
    end

    test "includes compaction_pending_bytes" do
      metrics = Compaction.monitoring_metrics()
      names = Enum.map(metrics, & &1.name)

      assert :compaction_pending_bytes in names
    end

    test "includes level0_file_count" do
      metrics = Compaction.monitoring_metrics()
      names = Enum.map(metrics, & &1.name)

      assert :level0_file_count in names
    end

    test "includes write_stall_duration" do
      metrics = Compaction.monitoring_metrics()
      names = Enum.map(metrics, & &1.name)

      assert :write_stall_duration in names
    end
  end

  describe "lag_indicators/0" do
    test "returns list of indicators" do
      indicators = Compaction.lag_indicators()

      assert [_ | _] = indicators
    end

    test "each indicator has thresholds and action" do
      indicators = Compaction.lag_indicators()

      for indicator <- indicators do
        assert Map.has_key?(indicator, :metric)
        assert Map.has_key?(indicator, :threshold_warning)
        assert Map.has_key?(indicator, :threshold_critical)
        assert Map.has_key?(indicator, :action)
      end
    end

    test "critical threshold > warning threshold" do
      indicators = Compaction.lag_indicators()

      for indicator <- indicators do
        assert indicator.threshold_critical > indicator.threshold_warning
      end
    end
  end

  describe "format_summary/1" do
    test "returns string summary" do
      config = Compaction.default()
      summary = Compaction.format_summary(config)

      assert is_binary(summary)
      assert String.contains?(summary, "Compaction")
      assert String.contains?(summary, "Level")
    end

    test "shows rate limit when enabled" do
      config = Compaction.preset(:balanced)
      summary = Compaction.format_summary(config)

      assert String.contains?(summary, "MB")
      refute String.contains?(summary, "Unlimited")
    end

    test "shows unlimited when no rate limit" do
      config = Compaction.default()
      summary = Compaction.format_summary(config)

      assert String.contains?(summary, "Unlimited")
    end
  end

  describe "compaction completes without errors" do
    test "all configurations pass validation" do
      for name <- Compaction.preset_names() do
        config = Compaction.preset(name)
        assert Compaction.validate(config) == :ok
      end
    end

    test "custom configurations with various options pass validation" do
      configs = [
        Compaction.custom(style: :level),
        Compaction.custom(style: :universal),
        Compaction.custom(rate_limit_bytes_per_sec: 100 * 1024 * 1024),
        Compaction.custom(max_background_compactions: 16),
        Compaction.custom(num_levels: 5)
      ]

      for config <- configs do
        assert Compaction.validate(config) == :ok
      end
    end
  end
end
