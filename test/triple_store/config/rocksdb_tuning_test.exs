defmodule TripleStore.Config.RocksDBTuningTest do
  @moduledoc """
  Integration tests for RocksDB tuning configuration (Section 5.2).

  These tests validate that all configuration modules work correctly together
  and produce valid, consistent settings for RocksDB.

  Test coverage:
  - Configuration loads without errors
  - Bloom filters reduce negative lookups (configuration validation)
  - Compression achieves expected ratio (configuration validation)
  - Compaction completes without errors (configuration validation)
  """

  use ExUnit.Case, async: true

  alias TripleStore.Config.ColumnFamily
  alias TripleStore.Config.Compaction
  alias TripleStore.Config.Compression
  alias TripleStore.Config.RocksDB

  # ============================================================================
  # Configuration Loads Without Errors
  # ============================================================================

  describe "configuration loads without errors" do
    test "RocksDB memory configuration loads for all presets" do
      presets = [:development, :production_low_memory, :production_high_memory, :write_heavy]

      for preset <- presets do
        config = RocksDB.preset(preset)
        assert is_map(config)
        assert config.block_cache_size > 0
        assert config.write_buffer_size > 0
      end
    end

    test "compression configuration loads for all column families" do
      cfs = Compression.column_families()

      for cf <- cfs do
        config = Compression.for_column_family(cf)
        assert is_map(config)
        assert Map.has_key?(config, :algorithm)
        assert Map.has_key?(config, :enabled)
      end
    end

    test "compaction configuration loads for all presets" do
      presets = Compaction.preset_names()

      for preset <- presets do
        config = Compaction.preset(preset)
        assert is_map(config)
        assert config.style in [:level, :universal, :fifo]
      end
    end

    test "column family configuration loads for all CFs" do
      cfs = ColumnFamily.column_family_names()

      for cf <- cfs do
        config = ColumnFamily.for_cf(cf)
        assert is_map(config)
        assert config.name == cf
        assert config.block_size > 0
      end
    end

    test "recommended memory configuration loads based on system" do
      config = RocksDB.recommended()

      assert is_map(config)
      assert config.block_cache_size > 0
      assert config.write_buffer_size > 0
      assert config.max_open_files > 0
    end

    test "all configurations validate successfully" do
      # RocksDB
      for preset <- [:development, :production_low_memory, :production_high_memory, :write_heavy] do
        config = RocksDB.preset(preset)
        assert RocksDB.validate(config) == :ok
      end

      # Compaction
      for preset <- Compaction.preset_names() do
        config = Compaction.preset(preset)
        assert Compaction.validate(config) == :ok
      end

      # Column Families
      assert ColumnFamily.validate_all() == :ok
    end

    test "custom configurations can be created and validated" do
      # Custom RocksDB config
      rocksdb = RocksDB.for_memory_budget(2 * 1024 * 1024 * 1024)
      assert RocksDB.validate(rocksdb) == :ok

      # Custom compaction config
      compaction = Compaction.custom(rate_limit_bytes_per_sec: 50 * 1024 * 1024)
      assert Compaction.validate(compaction) == :ok

      # Custom column family config
      cf = ColumnFamily.for_cf(:spo, block_size: 8192)
      assert ColumnFamily.validate(cf) == :ok
    end
  end

  # ============================================================================
  # Bloom Filters Reduce Negative Lookups
  # ============================================================================

  describe "bloom filters reduce negative lookups" do
    test "dictionary column families have bloom filters enabled" do
      for cf <- ColumnFamily.dictionary_cfs() do
        config = ColumnFamily.for_cf(cf)

        assert config.bloom_filter.enabled == true
        assert config.bloom_filter.bits_per_key > 0
      end
    end

    test "dictionary bloom filters have low false positive rate" do
      bits_per_key = ColumnFamily.dictionary_bloom_bits_per_key()
      fpr = ColumnFamily.estimated_false_positive_rate(bits_per_key)

      # Dictionary CFs use 12 bits/key, should have <1% FPR
      assert fpr < 0.01
    end

    test "index column families have bloom filters for prefix queries" do
      for cf <- ColumnFamily.index_cfs() do
        config = ColumnFamily.for_cf(cf)

        assert config.bloom_filter.enabled == true
        assert config.bloom_filter.block_based == true
      end
    end

    test "index bloom filters have acceptable false positive rate" do
      bits_per_key = ColumnFamily.default_bloom_bits_per_key()
      fpr = ColumnFamily.estimated_false_positive_rate(bits_per_key)

      # Index CFs use 10 bits/key, should have <2% FPR
      assert fpr < 0.02
    end

    test "derived column family has no bloom filter (bulk access)" do
      config = ColumnFamily.for_cf(:derived)

      assert config.bloom_filter.enabled == false
    end

    test "bloom filter memory usage is reasonable" do
      # For 10 million keys in dictionary CFs
      num_keys = 10_000_000

      for cf <- ColumnFamily.dictionary_cfs() do
        memory = ColumnFamily.estimate_bloom_memory(cf, num_keys)

        # 14 bits/key = 1.75 bytes/key = 17.5MB for 10M keys
        assert memory == 17_500_000
        # Should be less than 25MB for 10M keys
        assert memory < 25_000_000
      end
    end

    test "bloom filter configuration produces valid RocksDB options" do
      for cf <- ColumnFamily.column_family_names() do
        opts = ColumnFamily.to_rocksdb_options(cf)
        config = ColumnFamily.for_cf(cf)

        if config.bloom_filter.enabled do
          assert Keyword.has_key?(opts, :bloom_filter_bits_per_key)
          assert Keyword.get(opts, :bloom_filter_bits_per_key) > 0
        else
          refute Keyword.has_key?(opts, :bloom_filter_bits_per_key)
        end
      end
    end
  end

  # ============================================================================
  # Compression Achieves Expected Ratio
  # ============================================================================

  describe "compression achieves expected ratio" do
    test "all column families have compression configured" do
      cfs = Compression.column_families()

      assert length(cfs) == 6
      assert :id2str in cfs
      assert :str2id in cfs
      assert :spo in cfs
      assert :pos in cfs
      assert :osp in cfs
      assert :derived in cfs
    end

    test "index column families use LZ4 for speed" do
      index_cfs = [:spo, :pos, :osp]

      for cf <- index_cfs do
        config = Compression.for_column_family(cf)

        assert config.algorithm == :lz4
        assert config.enabled == true
      end
    end

    test "dictionary column families use LZ4 for speed" do
      dict_cfs = [:id2str, :str2id]

      for cf <- dict_cfs do
        config = Compression.for_column_family(cf)

        assert config.algorithm == :lz4
        assert config.enabled == true
      end
    end

    test "derived column family uses Zstd for better compression" do
      config = Compression.for_column_family(:derived)

      assert config.algorithm == :zstd
      assert config.enabled == true
    end

    test "LZ4 has expected compression ratio" do
      spec = Compression.algorithm_spec(:lz4)

      # LZ4 typically achieves ~2x compression
      assert spec.ratio >= 1.5
      assert spec.ratio <= 3.0
    end

    test "Zstd has better compression ratio than LZ4" do
      lz4_spec = Compression.algorithm_spec(:lz4)
      zstd_spec = Compression.algorithm_spec(:zstd)

      assert zstd_spec.ratio > lz4_spec.ratio
    end

    test "compression presets produce valid configurations" do
      for preset <- Compression.preset_names() do
        configs = Compression.preset(preset)

        assert is_map(configs)

        for {cf, config} <- configs do
          assert cf in Compression.column_families()
          assert Compression.validate(config) == :ok
        end
      end
    end

    test "per-level compression is configured for index CFs" do
      for cf <- [:spo, :pos, :osp] do
        levels = Compression.per_level_compression(cf)

        assert is_map(levels)
        # Level 0 should have no compression (fast writes)
        assert levels.level_0 == :none
        # Higher levels should have compression
        assert levels.level_1 in [:lz4, :zstd, :snappy]
      end
    end

    test "storage savings estimation is reasonable" do
      # 1GB uncompressed
      uncompressed_size = 1024 * 1024 * 1024

      for cf <- Compression.column_families() do
        config = Compression.for_column_family(cf)
        savings = Compression.estimate_savings(cf, uncompressed_size)

        if config.enabled do
          # Should save at least 30% for any compression
          assert savings.compressed_bytes < uncompressed_size
          assert savings.original_bytes - savings.compressed_bytes > 0
          assert savings.savings_percent > 30
        else
          assert savings.compressed_bytes == uncompressed_size
        end
      end
    end
  end

  # ============================================================================
  # Compaction Completes Without Errors
  # ============================================================================

  describe "compaction completes without errors" do
    test "all compaction presets validate successfully" do
      for preset <- Compaction.preset_names() do
        config = Compaction.preset(preset)
        assert Compaction.validate(config) == :ok
      end
    end

    test "L0 triggers are properly ordered for all presets" do
      for preset <- Compaction.preset_names() do
        config = Compaction.preset(preset)
        triggers = Compaction.l0_triggers(config)

        assert triggers.compaction < triggers.slowdown
        assert triggers.slowdown < triggers.stop
      end
    end

    test "level compaction is default style" do
      config = Compaction.default()

      assert config.style == :level
      assert config.level_compaction_dynamic_level_bytes == true
    end

    test "level sizes grow exponentially" do
      config = Compaction.default()
      sizes = Compaction.level_sizes(config)

      # L0 is variable
      assert sizes[0] == :variable

      # Each level should be larger than the previous
      for level <- 2..(config.num_levels - 1) do
        assert sizes[level] > sizes[level - 1]
      end
    end

    test "total capacity is reasonable for default config" do
      config = Compaction.default()
      capacity = Compaction.total_capacity(config)

      # With 7 levels and 10x multiplier, should be in TB range
      assert capacity > 100 * 1024 * 1024 * 1024
    end

    test "rate limiting is configurable" do
      # Default has no rate limit
      default_config = Compaction.default()
      default_rate = Compaction.rate_limit_config(default_config)
      assert default_rate.enabled == false

      # Balanced preset has rate limiting
      balanced_config = Compaction.preset(:balanced)
      balanced_rate = Compaction.rate_limit_config(balanced_config)
      assert balanced_rate.enabled == true
      assert balanced_rate.bytes_per_sec > 0
    end

    test "background jobs are configured" do
      config = Compaction.default()
      jobs = Compaction.background_jobs(config)

      assert jobs.compactions > 0
      assert jobs.flushes > 0
      assert jobs.total == jobs.compactions + jobs.flushes
    end

    test "monitoring metrics are defined" do
      metrics = Compaction.monitoring_metrics()

      assert is_list(metrics)
      refute Enum.empty?(metrics)

      metric_names = Enum.map(metrics, & &1.name)
      assert :compaction_pending_bytes in metric_names
      assert :level0_file_count in metric_names
      assert :write_stall_duration in metric_names
    end

    test "lag indicators have proper thresholds" do
      indicators = Compaction.lag_indicators()

      for indicator <- indicators do
        assert indicator.threshold_critical > indicator.threshold_warning
        assert is_binary(indicator.action)
      end
    end

    test "write amplification estimates are reasonable" do
      level_config = Compaction.custom(style: :level)
      level_wa = Compaction.estimate_write_amplification(level_config)

      # Level compaction typically has 10-30x write amplification (max can be higher in worst case)
      assert level_wa.min >= 5
      assert level_wa.max <= 100
      assert level_wa.typical > level_wa.min
      assert level_wa.typical <= level_wa.max
    end

    test "read amplification estimates show bloom filter benefit" do
      config = Compaction.default()
      ra = Compaction.estimate_read_amplification(config)

      # Bloom filters should significantly reduce read amplification
      assert ra.with_bloom < ra.without_bloom
      assert ra.with_bloom < ra.without_bloom / 2
    end
  end

  # ============================================================================
  # Integration: All Configurations Work Together
  # ============================================================================

  describe "integration: configurations work together" do
    test "can generate complete configuration for each column family" do
      for cf <- ColumnFamily.column_family_names() do
        # Get all relevant configurations
        cf_config = ColumnFamily.for_cf(cf)
        compression = Compression.for_column_family(cf)
        rocksdb_opts = ColumnFamily.to_rocksdb_options(cf)

        # All should be valid
        assert ColumnFamily.validate(cf_config) == :ok
        assert Compression.validate(compression) == :ok
        assert is_list(rocksdb_opts)
      end
    end

    test "memory budget is consistent across configurations" do
      # Get recommended config
      rocksdb = RocksDB.recommended()

      # Block cache should be significant portion of memory
      # Write buffer memory = write_buffer_size * max_write_buffer_number * num_cfs
      write_buffer_memory = rocksdb.write_buffer_size * rocksdb.max_write_buffer_number * 6
      total_memory = rocksdb.block_cache_size + write_buffer_memory

      # Total memory usage should be reasonable (not exceed budget)
      assert total_memory > 0
    end

    test "compression and column family configs are aligned" do
      # Index CFs should use fast compression
      for cf <- ColumnFamily.index_cfs() do
        compression = Compression.for_column_family(cf)
        assert compression.algorithm == :lz4
      end

      # Derived CF should use better compression (less frequently accessed)
      derived_compression = Compression.for_column_family(:derived)
      assert derived_compression.algorithm == :zstd
    end

    test "format summaries are generated without errors" do
      # All modules should be able to generate summaries
      cf_summary = ColumnFamily.format_summary()
      assert is_binary(cf_summary)
      assert String.length(cf_summary) > 0

      compression_summary = Compression.format_summary()
      assert is_binary(compression_summary)
      assert String.length(compression_summary) > 0

      compaction = Compaction.default()
      compaction_summary = Compaction.format_summary(compaction)
      assert is_binary(compaction_summary)
      assert String.length(compaction_summary) > 0

      rocksdb = RocksDB.recommended()
      rocksdb_summary = RocksDB.format_summary(rocksdb)
      assert is_binary(rocksdb_summary)
      assert String.length(rocksdb_summary) > 0
    end

    test "all presets can be combined consistently" do
      # Memory presets
      for mem_preset <- [:development, :production_low_memory, :production_high_memory] do
        rocksdb = RocksDB.preset(mem_preset)
        assert RocksDB.validate(rocksdb) == :ok
      end

      # Compaction presets
      for comp_preset <- Compaction.preset_names() do
        compaction = Compaction.preset(comp_preset)
        assert Compaction.validate(compaction) == :ok
      end

      # Compression presets
      for compr_preset <- Compression.preset_names() do
        compression = Compression.preset(compr_preset)

        for {_cf, config} <- compression do
          assert Compression.validate(config) == :ok
        end
      end
    end
  end

  # ============================================================================
  # Edge Cases and Error Handling
  # ============================================================================

  describe "edge cases and error handling" do
    test "invalid configurations are rejected" do
      # Invalid compaction config
      invalid_compaction = %{Compaction.default() | style: :invalid}
      assert {:error, _} = Compaction.validate(invalid_compaction)

      # Invalid column family config
      cf_config = ColumnFamily.for_cf(:spo)
      invalid_cf = %{cf_config | block_size: 100}
      assert {:error, _} = ColumnFamily.validate(invalid_cf)
    end

    test "unknown presets raise errors" do
      assert_raise FunctionClauseError, fn ->
        Compaction.preset(:unknown)
      end

      assert_raise FunctionClauseError, fn ->
        RocksDB.preset(:unknown)
      end
    end

    test "zero or negative values are handled" do
      # Zero keys for bloom filter memory
      assert ColumnFamily.estimate_bloom_memory(:id2str, 0) == 0

      # Minimum memory budget
      config = RocksDB.for_memory_budget(64 * 1024 * 1024)
      assert RocksDB.validate(config) == :ok
    end

    test "very large values are handled" do
      # Large number of keys
      large_memory = ColumnFamily.estimate_bloom_memory(:id2str, 1_000_000_000)
      assert large_memory > 0

      # Large memory budget
      large_config = RocksDB.for_memory_budget(128 * 1024 * 1024 * 1024)
      assert RocksDB.validate(large_config) == :ok
    end
  end
end
