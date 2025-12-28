defmodule TripleStore.Config.ColumnFamilyTest do
  use ExUnit.Case, async: true

  alias TripleStore.Config.ColumnFamily

  describe "for_cf/1" do
    test "returns configuration for dictionary column families" do
      for name <- [:id2str, :str2id] do
        config = ColumnFamily.for_cf(name)

        assert config.name == name
        assert config.type == :dictionary
        assert config.access_pattern == :point_lookup
      end
    end

    test "returns configuration for index column families" do
      for name <- [:spo, :pos, :osp] do
        config = ColumnFamily.for_cf(name)

        assert config.name == name
        assert config.type == :index
        assert config.access_pattern == :prefix_scan
      end
    end

    test "returns configuration for derived column family" do
      config = ColumnFamily.for_cf(:derived)

      assert config.name == :derived
      assert config.type == :derived
      assert config.access_pattern == :bulk
    end

    test "raises for unknown column family" do
      assert_raise FunctionClauseError, fn ->
        ColumnFamily.for_cf(:unknown)
      end
    end
  end

  describe "for_cf/2 with overrides" do
    test "overrides block size" do
      config = ColumnFamily.for_cf(:spo, block_size: 8192)

      assert config.block_size == 8192
    end

    test "overrides bloom bits per key" do
      config = ColumnFamily.for_cf(:spo, bloom_bits_per_key: 15)

      assert config.bloom_filter.bits_per_key == 15
      assert config.bloom_filter.enabled == true
    end

    test "overrides cache settings" do
      config = ColumnFamily.for_cf(:spo, cache_index_and_filter_blocks: false)

      assert config.cache_index_and_filter_blocks == false
    end

    test "ignores unknown options" do
      config = ColumnFamily.for_cf(:spo, unknown_option: 42)
      default = ColumnFamily.for_cf(:spo)

      assert config == default
    end
  end

  describe "all/0" do
    test "returns all column family configurations" do
      all = ColumnFamily.all()

      assert map_size(all) == 6
      assert Map.has_key?(all, :id2str)
      assert Map.has_key?(all, :str2id)
      assert Map.has_key?(all, :spo)
      assert Map.has_key?(all, :pos)
      assert Map.has_key?(all, :osp)
      assert Map.has_key?(all, :derived)
    end

    test "applies overrides to all configurations" do
      all = ColumnFamily.all(block_size: 8192)

      for {_name, config} <- all do
        assert config.block_size == 8192
      end
    end
  end

  describe "column_family_names/0" do
    test "returns all column family names" do
      names = ColumnFamily.column_family_names()

      assert length(names) == 6
      assert :id2str in names
      assert :str2id in names
      assert :spo in names
      assert :pos in names
      assert :osp in names
      assert :derived in names
    end
  end

  describe "dictionary_cfs/0" do
    test "returns dictionary column families" do
      names = ColumnFamily.dictionary_cfs()

      assert length(names) == 2
      assert :id2str in names
      assert :str2id in names
    end
  end

  describe "index_cfs/0" do
    test "returns index column families" do
      names = ColumnFamily.index_cfs()

      assert length(names) == 3
      assert :spo in names
      assert :pos in names
      assert :osp in names
    end
  end

  describe "derived_cfs/0" do
    test "returns derived column families" do
      names = ColumnFamily.derived_cfs()

      assert length(names) == 1
      assert :derived in names
    end
  end

  describe "cf_type/1" do
    test "returns dictionary for id2str and str2id" do
      assert ColumnFamily.cf_type(:id2str) == :dictionary
      assert ColumnFamily.cf_type(:str2id) == :dictionary
    end

    test "returns index for spo, pos, osp" do
      assert ColumnFamily.cf_type(:spo) == :index
      assert ColumnFamily.cf_type(:pos) == :index
      assert ColumnFamily.cf_type(:osp) == :index
    end

    test "returns derived for derived" do
      assert ColumnFamily.cf_type(:derived) == :derived
    end
  end

  describe "access_pattern/1" do
    test "returns point_lookup for dictionary CFs" do
      assert ColumnFamily.access_pattern(:id2str) == :point_lookup
      assert ColumnFamily.access_pattern(:str2id) == :point_lookup
    end

    test "returns prefix_scan for index CFs" do
      assert ColumnFamily.access_pattern(:spo) == :prefix_scan
      assert ColumnFamily.access_pattern(:pos) == :prefix_scan
      assert ColumnFamily.access_pattern(:osp) == :prefix_scan
    end

    test "returns bulk for derived CF" do
      assert ColumnFamily.access_pattern(:derived) == :bulk
    end
  end

  describe "bloom_filter_config/1" do
    test "enables bloom filter for dictionary CFs with higher bits" do
      for name <- [:id2str, :str2id] do
        config = ColumnFamily.bloom_filter_config(name)

        assert config.enabled == true
        assert config.bits_per_key == 12
        assert config.block_based == false
      end
    end

    test "enables bloom filter for index CFs with standard bits" do
      for name <- [:spo, :pos, :osp] do
        config = ColumnFamily.bloom_filter_config(name)

        assert config.enabled == true
        assert config.bits_per_key == 10
        assert config.block_based == true
      end
    end

    test "disables bloom filter for derived CF" do
      config = ColumnFamily.bloom_filter_config(:derived)

      assert config.enabled == false
      assert config.bits_per_key == 0
    end
  end

  describe "prefix_extractor_config/1" do
    test "enables prefix extractor for index CFs" do
      for name <- [:spo, :pos, :osp] do
        config = ColumnFamily.prefix_extractor_config(name)

        assert config.enabled == true
        assert config.type == :fixed
        assert config.length == 8
      end
    end

    test "disables prefix extractor for dictionary CFs" do
      for name <- [:id2str, :str2id] do
        config = ColumnFamily.prefix_extractor_config(name)

        assert config.enabled == false
        assert config.length == 0
      end
    end

    test "disables prefix extractor for derived CF" do
      config = ColumnFamily.prefix_extractor_config(:derived)

      assert config.enabled == false
      assert config.length == 0
    end
  end

  describe "block_size/1" do
    test "returns 4KB for dictionary CFs" do
      for name <- [:id2str, :str2id] do
        assert ColumnFamily.block_size(name) == 4 * 1024
      end
    end

    test "returns 4KB for index CFs" do
      for name <- [:spo, :pos, :osp] do
        assert ColumnFamily.block_size(name) == 4 * 1024
      end
    end

    test "returns 16KB for derived CF" do
      assert ColumnFamily.block_size(:derived) == 16 * 1024
    end
  end

  describe "triple_component_size/0" do
    test "returns 8 bytes" do
      assert ColumnFamily.triple_component_size() == 8
    end
  end

  describe "default_bloom_bits_per_key/0" do
    test "returns 10" do
      assert ColumnFamily.default_bloom_bits_per_key() == 10
    end
  end

  describe "dictionary_bloom_bits_per_key/0" do
    test "returns 12" do
      assert ColumnFamily.dictionary_bloom_bits_per_key() == 12
    end
  end

  describe "estimated_false_positive_rate/1" do
    test "returns lower FPR for more bits" do
      fpr_10 = ColumnFamily.estimated_false_positive_rate(10)
      fpr_12 = ColumnFamily.estimated_false_positive_rate(12)

      assert fpr_12 < fpr_10
    end

    test "10 bits gives approximately 1% FPR" do
      fpr = ColumnFamily.estimated_false_positive_rate(10)

      assert fpr < 0.02
      assert fpr > 0.005
    end

    test "12 bits gives less than 1% FPR" do
      fpr = ColumnFamily.estimated_false_positive_rate(12)

      assert fpr < 0.01
    end
  end

  describe "bloom_memory_per_key/1" do
    test "returns bytes per key" do
      assert ColumnFamily.bloom_memory_per_key(8) == 1.0
      assert ColumnFamily.bloom_memory_per_key(10) == 1.25
      assert ColumnFamily.bloom_memory_per_key(12) == 1.5
    end
  end

  describe "estimate_bloom_memory/2" do
    test "estimates memory for dictionary CFs" do
      # 12 bits = 1.5 bytes per key
      bytes = ColumnFamily.estimate_bloom_memory(:id2str, 1_000_000)

      assert bytes == 1_500_000
    end

    test "estimates memory for index CFs" do
      # 10 bits = 1.25 bytes per key
      bytes = ColumnFamily.estimate_bloom_memory(:spo, 1_000_000)

      assert bytes == 1_250_000
    end

    test "returns 0 for derived CF (no bloom filter)" do
      bytes = ColumnFamily.estimate_bloom_memory(:derived, 1_000_000)

      assert bytes == 0
    end

    test "returns 0 for zero keys" do
      bytes = ColumnFamily.estimate_bloom_memory(:id2str, 0)

      assert bytes == 0
    end
  end

  describe "tuning_rationale/1" do
    test "returns rationale for dictionary CFs" do
      rationale = ColumnFamily.tuning_rationale(:id2str)

      assert rationale.cf_name == :id2str
      assert rationale.cf_type == :dictionary
      assert rationale.access_pattern == :point_lookup
      assert String.contains?(rationale.bloom_filter_rationale, "12 bits/key")
      assert String.contains?(rationale.prefix_extractor_rationale, "Disabled")
      assert String.contains?(rationale.block_size_rationale, "4 KB")
    end

    test "returns rationale for index CFs" do
      rationale = ColumnFamily.tuning_rationale(:spo)

      assert rationale.cf_name == :spo
      assert rationale.cf_type == :index
      assert rationale.access_pattern == :prefix_scan
      assert String.contains?(rationale.bloom_filter_rationale, "10 bits/key")
      assert String.contains?(rationale.prefix_extractor_rationale, "8-byte prefix")
      assert String.contains?(rationale.block_size_rationale, "4 KB")
    end

    test "returns rationale for derived CF" do
      rationale = ColumnFamily.tuning_rationale(:derived)

      assert rationale.cf_name == :derived
      assert rationale.cf_type == :derived
      assert rationale.access_pattern == :bulk
      assert String.contains?(rationale.bloom_filter_rationale, "Disabled")
      assert String.contains?(rationale.prefix_extractor_rationale, "Disabled")
      assert String.contains?(rationale.block_size_rationale, "16 KB")
    end

    test "index rationale includes index name" do
      spo_rationale = ColumnFamily.tuning_rationale(:spo)
      pos_rationale = ColumnFamily.tuning_rationale(:pos)
      osp_rationale = ColumnFamily.tuning_rationale(:osp)

      assert String.contains?(spo_rationale.prefix_extractor_rationale, "Subject-Predicate-Object")
      assert String.contains?(pos_rationale.prefix_extractor_rationale, "Predicate-Object-Subject")
      assert String.contains?(osp_rationale.prefix_extractor_rationale, "Object-Subject-Predicate")
    end
  end

  describe "format_summary/0" do
    test "returns formatted string" do
      summary = ColumnFamily.format_summary()

      assert is_binary(summary)
      assert String.contains?(summary, "Column Family Tuning Summary")
    end

    test "includes all column families" do
      summary = ColumnFamily.format_summary()

      assert String.contains?(summary, "id2str")
      assert String.contains?(summary, "str2id")
      assert String.contains?(summary, "spo")
      assert String.contains?(summary, "pos")
      assert String.contains?(summary, "osp")
      assert String.contains?(summary, "derived")
    end

    test "shows bloom filter status" do
      summary = ColumnFamily.format_summary()

      assert String.contains?(summary, "bits/key")
      assert String.contains?(summary, "Disabled")
    end

    test "shows prefix extractor status" do
      summary = ColumnFamily.format_summary()

      assert String.contains?(summary, "8 bytes")
    end
  end

  describe "validate/1" do
    test "validates all default configurations" do
      for name <- ColumnFamily.column_family_names() do
        config = ColumnFamily.for_cf(name)
        assert ColumnFamily.validate(config) == :ok
      end
    end

    test "rejects invalid bloom filter bits" do
      config = ColumnFamily.for_cf(:spo)
      invalid = %{config | bloom_filter: %{enabled: true, bits_per_key: 50, block_based: true}}

      assert {:error, reason} = ColumnFamily.validate(invalid)
      assert String.contains?(reason, "bits_per_key")
    end

    test "rejects invalid prefix extractor length" do
      config = ColumnFamily.for_cf(:spo)
      invalid = %{config | prefix_extractor: %{enabled: true, type: :fixed, length: 100}}

      assert {:error, reason} = ColumnFamily.validate(invalid)
      assert String.contains?(reason, "length")
    end

    test "rejects invalid block size" do
      config = ColumnFamily.for_cf(:spo)
      invalid = %{config | block_size: 100}

      assert {:error, reason} = ColumnFamily.validate(invalid)
      assert String.contains?(reason, "Block size")
    end

    test "accepts disabled bloom filter" do
      config = ColumnFamily.for_cf(:derived)
      assert ColumnFamily.validate(config) == :ok
    end

    test "accepts disabled prefix extractor" do
      config = ColumnFamily.for_cf(:id2str)
      assert ColumnFamily.validate(config) == :ok
    end
  end

  describe "validate_all/0" do
    test "validates all default configurations" do
      assert ColumnFamily.validate_all() == :ok
    end
  end

  describe "to_rocksdb_options/1" do
    test "includes base options" do
      opts = ColumnFamily.to_rocksdb_options(:spo)

      assert Keyword.has_key?(opts, :block_size)
      assert Keyword.has_key?(opts, :cache_index_and_filter_blocks)
      assert Keyword.has_key?(opts, :pin_l0_filter_and_index_blocks_in_cache)
      assert Keyword.has_key?(opts, :format_version)
    end

    test "includes bloom filter options when enabled" do
      opts = ColumnFamily.to_rocksdb_options(:spo)

      assert Keyword.has_key?(opts, :bloom_filter_bits_per_key)
      assert Keyword.has_key?(opts, :bloom_filter_block_based)
    end

    test "excludes bloom filter options when disabled" do
      opts = ColumnFamily.to_rocksdb_options(:derived)

      refute Keyword.has_key?(opts, :bloom_filter_bits_per_key)
      refute Keyword.has_key?(opts, :bloom_filter_block_based)
    end

    test "includes prefix extractor options when enabled" do
      opts = ColumnFamily.to_rocksdb_options(:spo)

      assert Keyword.has_key?(opts, :prefix_extractor_type)
      assert Keyword.has_key?(opts, :prefix_extractor_length)
    end

    test "excludes prefix extractor options when disabled" do
      opts = ColumnFamily.to_rocksdb_options(:id2str)

      refute Keyword.has_key?(opts, :prefix_extractor_type)
      refute Keyword.has_key?(opts, :prefix_extractor_length)
    end

    test "applies overrides" do
      opts = ColumnFamily.to_rocksdb_options(:spo, block_size: 8192)

      assert Keyword.get(opts, :block_size) == 8192
    end
  end

  describe "configuration consistency" do
    test "all CFs have valid configurations" do
      for name <- ColumnFamily.column_family_names() do
        config = ColumnFamily.for_cf(name)

        assert config.name == name
        assert config.type in [:dictionary, :index, :derived]
        assert config.access_pattern in [:point_lookup, :prefix_scan, :bulk]
        assert is_map(config.bloom_filter)
        assert is_map(config.prefix_extractor)
        assert config.block_size > 0
      end
    end

    test "dictionary CFs have bloom filters enabled" do
      for name <- ColumnFamily.dictionary_cfs() do
        config = ColumnFamily.for_cf(name)
        assert config.bloom_filter.enabled == true
      end
    end

    test "index CFs have prefix extractors enabled" do
      for name <- ColumnFamily.index_cfs() do
        config = ColumnFamily.for_cf(name)
        assert config.prefix_extractor.enabled == true
      end
    end

    test "derived CF has larger block size" do
      derived_size = ColumnFamily.block_size(:derived)
      index_size = ColumnFamily.block_size(:spo)

      assert derived_size > index_size
    end
  end

  describe "cache settings" do
    test "dictionary CFs have optimized cache settings" do
      for name <- ColumnFamily.dictionary_cfs() do
        config = ColumnFamily.for_cf(name)

        assert config.cache_index_and_filter_blocks == true
        assert config.pin_l0_filter_and_index_blocks_in_cache == true
        assert config.optimize_filters_for_hits == true
        assert config.whole_key_filtering == true
      end
    end

    test "index CFs have appropriate cache settings" do
      for name <- ColumnFamily.index_cfs() do
        config = ColumnFamily.for_cf(name)

        assert config.cache_index_and_filter_blocks == true
        assert config.pin_l0_filter_and_index_blocks_in_cache == true
        assert config.optimize_filters_for_hits == false
        assert config.whole_key_filtering == false
      end
    end

    test "derived CF has relaxed cache settings" do
      config = ColumnFamily.for_cf(:derived)

      assert config.cache_index_and_filter_blocks == true
      assert config.pin_l0_filter_and_index_blocks_in_cache == false
      assert config.optimize_filters_for_hits == false
    end
  end
end
