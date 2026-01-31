defmodule TripleStore.Backend.RocksDB.Phase3ConfigurationTest do
  @moduledoc """
  Unit tests for Phase 3.2: Configuration Tuning.

  These tests verify that the new ReadOptions, WriteOptions, and enhanced
  ColumnFamilyConfig modules provide correct configurations for different
  use cases.
  """

  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB.ColumnFamilyConfig
  alias TripleStore.Backend.RocksDB.ReadOptions
  alias TripleStore.Backend.RocksDB.WriteOptions

  @moduletag :phase3_config
  @moduletag :rocksdb

  # ============================================================================
  # Section 3.2.1: Read Options Tests
  # ============================================================================

  describe "3.2.1 Read Options Optimization" do
    test "default/0 returns balanced options" do
      opts = ReadOptions.default()

      assert Keyword.has_key?(opts, :fill_cache)
      assert Keyword.has_key?(opts, :total_order_seek)
      assert Keyword.has_key?(opts, :prefix_same_as_start)

      # Default should enable cache
      assert Keyword.get(opts, :fill_cache) == true
    end

    test "point_lookup/0 optimized for single key lookups" do
      opts = ReadOptions.point_lookup()

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == true
      assert Keyword.get(opts, :prefix_same_as_start) == false
    end

    test "prefix_scan/0 optimized for prefix iteration" do
      opts = ReadOptions.prefix_scan()

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == false
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end

    test "full_scan/0 bypasses cache to avoid pollution" do
      opts = ReadOptions.full_scan()

      assert Keyword.get(opts, :fill_cache) == false
    end

    test "cached_scan/0 maximizes cache usage" do
      opts = ReadOptions.cached_scan()

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end

    test "uncached_scan/0 bypasses cache" do
      opts = ReadOptions.uncached_scan()

      assert Keyword.get(opts, :fill_cache) == false
    end

    test "with_upper_bound/1 creates bounded iteration options" do
      bound = <<123, 255>>
      opts = ReadOptions.with_upper_bound(bound)

      assert Keyword.get(opts, :iterate_upper_bound) == bound
      assert Keyword.get(opts, :fill_cache) == true
    end

    test "from_snapshot/1 creates snapshot read options" do
      # Create a mock snapshot reference
      snapshot_ref = make_ref()
      opts = ReadOptions.from_snapshot(snapshot_ref)

      assert Keyword.get(opts, :snapshot) == snapshot_ref
      assert Keyword.get(opts, :fill_cache) == true
    end

    test "merge/2 combines preset with custom options" do
      preset = ReadOptions.prefix_scan()
      custom_opts = [fill_cache: false, custom_option: :value]
      merged = ReadOptions.merge(preset, custom_opts)

      # Custom options override preset
      assert Keyword.get(merged, :fill_cache) == false
      assert Keyword.get(merged, :custom_option) == :value
      # Preset options are preserved when not overridden
      assert Keyword.get(merged, :total_order_seek) == false
      assert Keyword.get(merged, :prefix_same_as_start) == true
    end

    test "use_cache?/1 returns correct cache preference" do
      assert ReadOptions.use_cache?(:point_lookup) == true
      assert ReadOptions.use_cache?(:prefix_scan) == true
      assert ReadOptions.use_cache?(:repeated_query) == true
      assert ReadOptions.use_cache?(:bulk_export) == false
      assert ReadOptions.use_cache?(:full_scan) == false
    end

    test "for_cf/1 returns appropriate options for each column family" do
      # Dictionary CFs should use point lookup (total_order_seek: true)
      id2str_opts = ReadOptions.for_cf(:id2str)
      assert Keyword.get(id2str_opts, :total_order_seek) == true

      str2id_opts = ReadOptions.for_cf(:str2id)
      assert Keyword.get(str2id_opts, :total_order_seek) == true

      # Index CFs should use prefix scan
      spo_opts = ReadOptions.for_cf(:spo)
      assert Keyword.get(spo_opts, :total_order_seek) == false

      pos_opts = ReadOptions.for_cf(:pos)
      assert Keyword.get(pos_opts, :total_order_seek) == false

      osp_opts = ReadOptions.for_cf(:osp)
      assert Keyword.get(osp_opts, :total_order_seek) == false

      # Derived CF should use full scan (cache disabled)
      derived_opts = ReadOptions.for_cf(:derived)
      # Note: full_scan returns a different set, so we check for cache disabled
      assert Keyword.get(derived_opts, :fill_cache) == false
    end
  end

  # ============================================================================
  # Section 3.2.2: Write Options Tests
  # ============================================================================

  describe "3.2.2 Write Options Optimization" do
    test "default/0 returns balanced options" do
      opts = WriteOptions.default()

      assert Keyword.has_key?(opts, :sync)
      assert Keyword.has_key?(opts, :disable_wal)

      # Default should not sync every write
      assert Keyword.get(opts, :sync) == false
      # WAL should be enabled
      assert Keyword.get(opts, :disable_wal) == false
    end

    test "sync/0 enables WAL sync for maximum durability" do
      opts = WriteOptions.sync()

      assert Keyword.get(opts, :sync) == true
      assert Keyword.get(opts, :disable_wal) == false
    end

    test "async/0 optimizes for throughput" do
      opts = WriteOptions.async()

      assert Keyword.get(opts, :sync) == false
      assert Keyword.get(opts, :disable_wal) == false
    end

    test "disable_wal/0 disables write-ahead log" do
      opts = WriteOptions.disable_wal()

      assert Keyword.get(opts, :sync) == false
      assert Keyword.get(opts, :disable_wal) == true
    end

    test "bulk_load/0 optimized for high-volume writes" do
      opts = WriteOptions.bulk_load()

      assert Keyword.get(opts, :sync) == false
      assert Keyword.get(opts, :disable_wal) == false
      assert Keyword.has_key?(opts, :no_slowdown)
      assert Keyword.has_key?(opts, :low_pri)
    end

    test "with_sync/1 creates options with custom sync setting" do
      opts_sync = WriteOptions.with_sync(true)
      assert Keyword.get(opts_sync, :sync) == true

      opts_async = WriteOptions.with_sync(false)
      assert Keyword.get(opts_async, :sync) == false
    end

    test "with_timeout/1 creates options with timeout" do
      opts = WriteOptions.with_timeout(5000)

      assert Keyword.get(opts, :timeout) == 5000
      assert Keyword.get(opts, :sync) == false
    end

    test "merge/2 combines preset with custom options" do
      preset = WriteOptions.sync()
      custom_opts = [low_pri: true]
      merged = WriteOptions.merge(preset, custom_opts)

      # Sync from preset is preserved
      assert Keyword.get(merged, :sync) == true
      # Custom option is added
      assert Keyword.get(merged, :low_pri) == true
    end

    test "use_sync?/1 returns correct sync preference" do
      assert WriteOptions.use_sync?(:transaction_commit) == true
      assert WriteOptions.use_sync?(:critical_write) == true
      assert WriteOptions.use_sync?(:user_data) == true
      assert WriteOptions.use_sync?(:bulk_import) == false
      assert WriteOptions.use_sync?(:derived_data) == false
      assert WriteOptions.use_sync?(:temp_data) == false
    end

    test "disable_wal?/1 returns correct WAL preference" do
      assert WriteOptions.disable_wal?(:rebuildable_cache) == true
      assert WriteOptions.disable_wal?(:temp_derived) == true
      assert WriteOptions.disable_wal?(:benchmark) == true
      assert WriteOptions.disable_wal?(:user_data) == false
      assert WriteOptions.disable_wal?(:critical) == false
    end

    test "for_cf/1 returns appropriate options for each column family" do
      # Dictionary CFs should use sync for durability
      id2str_opts = WriteOptions.for_cf(:id2str)
      assert Keyword.get(id2str_opts, :disable_wal) == false

      str2id_opts = WriteOptions.for_cf(:str2id)
      assert Keyword.get(str2id_opts, :disable_wal) == false

      # Index CFs should use default
      spo_opts = WriteOptions.for_cf(:spo)
      assert Keyword.get(spo_opts, :sync) == false

      # Derived CF should use async
      derived_opts = WriteOptions.for_cf(:derived)
      assert Keyword.get(derived_opts, :sync) == false
    end

    test "for_transaction/1 returns sync options for commit" do
      commit_opts = WriteOptions.for_transaction(true)
      assert Keyword.get(commit_opts, :sync) == true

      non_commit_opts = WriteOptions.for_transaction(false)
      assert Keyword.get(non_commit_opts, :sync) == false
    end
  end

  # ============================================================================
  # Section 3.2.3: Compaction Tuning Tests
  # ============================================================================

  describe "3.2.3 Compaction Tuning" do
    test "dictionary_compaction_options/0 returns options for point lookups" do
      opts = ColumnFamilyConfig.dictionary_compaction_options()

      # Should use universal compaction for better read performance
      assert Keyword.get(opts, :compaction_style) == :universal

      # Should have size amplification threshold
      assert Keyword.has_key?(opts, :compaction_options_universal_size_amp_percent)
      size_amp = Keyword.get(opts, :compaction_options_universal_size_amp_percent)
      # Should be close to ideal size
      assert size_amp >= 100

      # Should have target file size
      assert Keyword.has_key?(opts, :target_file_size_base)
      file_size = Keyword.get(opts, :target_file_size_base)
      assert file_size > 0
    end

    test "index_compaction_options/0 returns options for balanced performance" do
      opts = ColumnFamilyConfig.index_compaction_options()

      # Should use level compaction
      assert Keyword.get(opts, :compaction_style) == :level

      # Should have write buffer settings
      assert Keyword.has_key?(opts, :write_buffer_size)
      assert Keyword.get(opts, :write_buffer_size) > 0

      # Should have L0 compaction trigger
      assert Keyword.has_key?(opts, :level0_file_num_compaction_trigger)
      trigger = Keyword.get(opts, :level0_file_num_compaction_trigger)
      assert trigger > 0

      # Should have max bytes for level base
      assert Keyword.has_key?(opts, :max_bytes_for_level_base)
      assert Keyword.get(opts, :max_bytes_for_level_base) > 0
    end

    test "derived_compaction_options/0 returns options for write throughput" do
      opts = ColumnFamilyConfig.derived_compaction_options()

      # Should use level compaction
      assert Keyword.get(opts, :compaction_style) == :level

      # Should have larger write buffer for bulk writes
      write_buffer_size = Keyword.get(opts, :write_buffer_size)

      # Compare with index options - derived should have larger buffer
      index_opts = ColumnFamilyConfig.index_compaction_options()
      index_buffer_size = Keyword.get(index_opts, :write_buffer_size)

      assert write_buffer_size >= index_buffer_size

      # Should have higher L0 trigger for delayed compaction
      l0_trigger = Keyword.get(opts, :level0_file_num_compaction_trigger)

      index_l0_trigger = Keyword.get(index_opts, :level0_file_num_compaction_trigger)

      assert l0_trigger >= index_l0_trigger
    end

    test "compaction options have different settings for different CFs" do
      dict_opts = ColumnFamilyConfig.dictionary_compaction_options()
      index_opts = ColumnFamilyConfig.index_compaction_options()
      derived_opts = ColumnFamilyConfig.derived_compaction_options()

      # Dictionary uses universal compaction
      assert Keyword.get(dict_opts, :compaction_style) == :universal

      # Index uses level compaction
      assert Keyword.get(index_opts, :compaction_style) == :level

      # Derived uses level compaction with different settings
      assert Keyword.get(derived_opts, :compaction_style) == :level

      # Derived should have larger write buffer than index
      derived_buffer = Keyword.get(derived_opts, :write_buffer_size)
      index_buffer = Keyword.get(index_opts, :write_buffer_size)
      assert derived_buffer > index_buffer
    end
  end

  # ============================================================================
  # Section 3.2.4: Integration Tests
  # ============================================================================

  describe "3.2.4 Configuration Integration" do
    test "ColumnFamilyConfig.cf_descriptors returns all 7 CFs" do
      descriptors = ColumnFamilyConfig.cf_descriptors()

      # 7 CFs + default
      assert length(descriptors) == 8

      cf_names = Enum.map(descriptors, fn {name, _opts} -> name end)
      assert "default" in cf_names
      assert "id2str" in cf_names
      assert "str2id" in cf_names
      assert "spo" in cf_names
      assert "pos" in cf_names
      assert "osp" in cf_names
      assert "derived" in cf_names
      assert "numeric_range" in cf_names
    end

    test "get_cf_options/1 returns correct options for each CF" do
      # Test a few CFs
      id2str_opts = ColumnFamilyConfig.get_cf_options(:id2str)
      assert is_list(id2str_opts)
      refute Enum.empty?(id2str_opts)

      spo_opts = ColumnFamilyConfig.get_cf_options(:spo)
      assert is_list(spo_opts)
      refute Enum.empty?(spo_opts)

      derived_opts = ColumnFamilyConfig.get_cf_options(:derived)
      assert is_list(derived_opts)
      refute Enum.empty?(derived_opts)
    end

    test "get_cf_options/1 returns nil for invalid CF" do
      invalid_opts = ColumnFamilyConfig.get_cf_options(:invalid_cf)
      assert is_nil(invalid_opts)
    end

    test "validate_cf/1 validates correct CF atoms" do
      assert ColumnFamilyConfig.validate_cf(:id2str) == :ok
      assert ColumnFamilyConfig.validate_cf(:spo) == :ok
      assert ColumnFamilyConfig.validate_cf(:derived) == :ok
      assert ColumnFamilyConfig.validate_cf(:default) == :ok
    end

    test "validate_cf/1 rejects invalid CF atoms" do
      assert ColumnFamilyConfig.validate_cf(:invalid) == {:error, :invalid_column_family}
      assert ColumnFamilyConfig.validate_cf(:foo) == {:error, :invalid_column_family}
    end

    test "bloom_bits/1 returns correct bits per key" do
      assert ColumnFamilyConfig.bloom_bits(:id2str) == 14
      assert ColumnFamilyConfig.bloom_bits(:str2id) == 14
      assert ColumnFamilyConfig.bloom_bits(:spo) == 12
      assert ColumnFamilyConfig.bloom_bits(:pos) == 12
      assert ColumnFamilyConfig.bloom_bits(:osp) == 12
      assert ColumnFamilyConfig.bloom_bits(:derived) == 0
      assert ColumnFamilyConfig.bloom_bits(:numeric_range) == 12
    end

    test "block_size/1 returns correct block sizes" do
      assert ColumnFamilyConfig.block_size(:id2str) == 2 * 1024
      assert ColumnFamilyConfig.block_size(:str2id) == 2 * 1024
      assert ColumnFamilyConfig.block_size(:spo) == 8 * 1024
      assert ColumnFamilyConfig.block_size(:pos) == 8 * 1024
      assert ColumnFamilyConfig.block_size(:osp) == 8 * 1024
      assert ColumnFamilyConfig.block_size(:derived) == 32 * 1024
      assert ColumnFamilyConfig.block_size(:numeric_range) == 8 * 1024
    end

    test "has_prefix_extractor?/1 correctly identifies index CFs" do
      assert ColumnFamilyConfig.has_prefix_extractor?(:spo) == true
      assert ColumnFamilyConfig.has_prefix_extractor?(:pos) == true
      assert ColumnFamilyConfig.has_prefix_extractor?(:osp) == true
      assert ColumnFamilyConfig.has_prefix_extractor?(:id2str) == false
      assert ColumnFamilyConfig.has_prefix_extractor?(:str2id) == false
      assert ColumnFamilyConfig.has_prefix_extractor?(:derived) == false
      assert ColumnFamilyConfig.has_prefix_extractor?(:numeric_range) == false
    end

    test "cf_name_to_string/1 converts atoms to strings" do
      assert ColumnFamilyConfig.cf_name_to_string(:id2str) == "id2str"
      assert ColumnFamilyConfig.cf_name_to_string(:spo) == "spo"
      assert ColumnFamilyConfig.cf_name_to_string(:derived) == "derived"
    end

    test "cf_string_to_name/1 converts strings to atoms" do
      assert ColumnFamilyConfig.cf_string_to_name("id2str") == :id2str
      assert ColumnFamilyConfig.cf_string_to_name("spo") == :spo
      assert ColumnFamilyConfig.cf_string_to_name("derived") == :derived
      assert ColumnFamilyConfig.cf_string_to_name("invalid") == nil
    end
  end

  # ============================================================================
  # Section 3.2.5: Options Compatibility Tests
  # ============================================================================

  describe "3.2.5 Options Compatibility" do
    test "read options are compatible with NIF operations" do
      # All read options should be keyword lists
      opts = ReadOptions.prefix_scan()
      assert is_list(opts)

      # Should contain valid atoms that erlang-rocksdb accepts
      valid_keys = [:fill_cache, :iterate_upper_bound, :total_order_seek, :prefix_same_as_start]
      opts_keys = Keyword.keys(opts)

      # All keys should be valid
      for key <- opts_keys do
        assert key in valid_keys
      end
    end

    test "write options are compatible with NIF operations" do
      # All write options should be keyword lists
      opts = WriteOptions.bulk_load()
      assert is_list(opts)

      # Should contain valid atoms that erlang-rocksdb accepts
      valid_keys = [:sync, :disable_wal, :no_slowdown, :low_pri, :timeout]
      opts_keys = Keyword.keys(opts)

      # All keys should be valid
      for key <- opts_keys do
        assert key in valid_keys
      end
    end

    test "column family options are compatible with erlang-rocksdb" do
      descriptors = ColumnFamilyConfig.cf_descriptors()

      for {_cf_name, cf_opts} <- descriptors do
        # Should be a keyword list
        assert is_list(cf_opts)

        # Should have valid option keys
        opts_keys = Keyword.keys(cf_opts)
        refute Enum.empty?(opts_keys)
      end
    end
  end
end
