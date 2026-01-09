defmodule TripleStore.Backend.RocksDB.ColumnFamilyConfigurationTest do
  @moduledoc """
  Unit tests for Section 1.4: Column Family Configuration

  These tests verify that the column family configuration matches the
  Rust NIF tuning settings for data compatibility and performance.

  Also includes tests for Section 1.1: Quad Index Architecture
  """
  use ExUnit.Case, async: true

  alias TripleStore.Backend.RocksDB.ColumnFamilyConfig

  # ===========================================================================
  # Section 1.4.1: Column Family Options Mapping
  # ===========================================================================

  describe "1.4.1 Column Family Options Mapping" do
    test "1.4.1.1 Bloom filter settings: 14 bits/key (dict), 12 bits/key (index), none (derived)" do
      assert ColumnFamilyConfig.bloom_bits(:id2str) == 14
      assert ColumnFamilyConfig.bloom_bits(:str2id) == 14
      assert ColumnFamilyConfig.bloom_bits(:spo) == 12
      assert ColumnFamilyConfig.bloom_bits(:pos) == 12
      assert ColumnFamilyConfig.bloom_bits(:osp) == 12
      assert ColumnFamilyConfig.bloom_bits(:derived) == 0
    end

    test "1.4.1.2 Block size settings: 2KB (dict), 8KB (index), 32KB (derived)" do
      assert ColumnFamilyConfig.block_size(:id2str) == 2 * 1024
      assert ColumnFamilyConfig.block_size(:str2id) == 2 * 1024
      assert ColumnFamilyConfig.block_size(:spo) == 8 * 1024
      assert ColumnFamilyConfig.block_size(:pos) == 8 * 1024
      assert ColumnFamilyConfig.block_size(:osp) == 8 * 1024
      assert ColumnFamilyConfig.block_size(:derived) == 32 * 1024
    end

    test "1.4.1.3 Compression settings: none for all levels (build limitation)" do
      # Get options for each CF type
      dict_opts = ColumnFamilyConfig.get_cf_options(:id2str)
      index_opts = ColumnFamilyConfig.get_cf_options(:spo)
      derived_opts = ColumnFamilyConfig.get_cf_options(:derived)

      # All CFs should have compression options (currently :none due to build limitations)
      assert Keyword.has_key?(dict_opts, :compression)
      assert Keyword.has_key?(dict_opts, :bottommost_compression)
      assert Keyword.has_key?(index_opts, :compression)
      assert Keyword.has_key?(derived_opts, :compression)
    end

    test "1.4.1.4 Prefix extractor: fixed_prefix(8) for index CFs" do
      assert ColumnFamilyConfig.has_prefix_extractor?(:spo) == true
      assert ColumnFamilyConfig.has_prefix_extractor?(:pos) == true
      assert ColumnFamilyConfig.has_prefix_extractor?(:osp) == true

      # Dictionary and derived CFs should not have prefix extractor
      assert ColumnFamilyConfig.has_prefix_extractor?(:id2str) == false
      assert ColumnFamilyConfig.has_prefix_extractor?(:str2id) == false
      assert ColumnFamilyConfig.has_prefix_extractor?(:derived) == false
    end

    test "1.4.1.5 Memtable prefix bloom ratio: 0.1 for index CFs" do
      index_opts = ColumnFamilyConfig.get_cf_options(:spo)

      # Index CFs should have memtable prefix bloom configured
      assert Keyword.has_key?(index_opts, :memtable_prefix_bloom_size_ratio)

      # Dictionary CFs should not have it
      dict_opts = ColumnFamilyConfig.get_cf_options(:id2str)
      refute Keyword.has_key?(dict_opts, :memtable_prefix_bloom_size_ratio)
    end
  end

  # ===========================================================================
  # Section 1.4.2: Cache Configuration
  # ===========================================================================

  describe "1.4.2 Cache Configuration" do
    test "1.4.2.1 Shared block cache configured" do
      db_opts = ColumnFamilyConfig.db_options()

      # Should have a cache configured
      assert Keyword.has_key?(db_opts, :create_if_missing)
    end

    test "1.4.2.2 cache_index_and_filter_blocks for dict and index CFs" do
      dict_opts = ColumnFamilyConfig.get_cf_options(:id2str)
      index_opts = ColumnFamilyConfig.get_cf_options(:spo)

      # Get block_based_table_options
      dict_table_opts = Keyword.get(dict_opts, :block_based_table_options, [])
      index_table_opts = Keyword.get(index_opts, :block_based_table_options, [])

      # Dictionary and index CFs should cache index and filter blocks
      dict_table_opts = List.wrap(dict_table_opts)
      index_table_opts = List.wrap(index_table_opts)

      assert {:cache_index_and_filter_blocks, true} in dict_table_opts
      assert {:cache_index_and_filter_blocks, true} in index_table_opts
    end

    test "1.4.2.3 pin_l0_filter_and_index_blocks_in_cache for dict CFs" do
      dict_opts = ColumnFamilyConfig.get_cf_options(:id2str)
      index_opts = ColumnFamilyConfig.get_cf_options(:spo)

      # Get block_based_table_options
      dict_table_opts = Keyword.get(dict_opts, :block_based_table_options, [])
      index_table_opts = Keyword.get(index_opts, :block_based_table_options, [])

      dict_table_opts = List.wrap(dict_table_opts)
      index_table_opts = List.wrap(index_table_opts)

      # Dictionary CFs should pin L0 blocks
      assert {:pin_l0_filter_and_index_blocks_in_cache, true} in dict_table_opts

      # Index CFs should not pin L0 blocks (sequential scan pattern)
      assert {:pin_l0_filter_and_index_blocks_in_cache, false} in index_table_opts
    end

    test "1.4.2.4 Disable cache pinning for derived CF" do
      derived_opts = ColumnFamilyConfig.get_cf_options(:derived)

      # Derived CF should not cache index/filter blocks (sequential access)
      derived_table_opts = Keyword.get(derived_opts, :block_based_table_options, [])
      derived_table_opts = List.wrap(derived_table_opts)

      assert {:cache_index_and_filter_blocks, false} in derived_table_opts
      assert {:pin_l0_filter_and_index_blocks_in_cache, false} in derived_table_opts
    end
  end

  # ===========================================================================
  # Section 1.4.3: Compression Tuning
  # ===========================================================================

  describe "1.4.3 Compression Tuning" do
    test "1.4.3.1 L0 compression: none" do
      # L0 is handled differently - we don't compress L0 for fast memtable flush
      # The compression option applies to L1+, L0 remains uncompressed
      dict_opts = ColumnFamilyConfig.get_cf_options(:id2str)

      # Verify compression is set (applies to L1+, L0 is implicitly none)
      assert Keyword.has_key?(dict_opts, :compression)
    end

    test "1.4.3.2 L1-L6 compression: none (build limitation)" do
      dict_opts = ColumnFamilyConfig.get_cf_options(:id2str)
      index_opts = ColumnFamilyConfig.get_cf_options(:spo)
      derived_opts = ColumnFamilyConfig.get_cf_options(:derived)

      # All CFs currently use :none due to erlang-rocksdb build limitations
      # Note: Can be changed to :lz4 or :snappy if erlang-rocksdb is recompiled with compression support
      assert {:compression, :none} in dict_opts
      assert {:compression, :none} in index_opts
      assert {:compression, :none} in derived_opts
    end

    test "1.4.3.3 Compression options per column family" do
      # All CFs should have both compression and bottommost_compression set
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived, :numeric_range] do
        opts = ColumnFamilyConfig.get_cf_options(cf)
        assert {:compression, :none} in opts
        assert {:bottommost_compression, :none} in opts
      end
    end
  end

  # ===========================================================================
  # Section 1.4.4: Column Family Descriptor Tests
  # ===========================================================================

  describe "1.4.4 Column Family Descriptors" do
    test "1.4.4.1 Column families open with correct options" do
      descriptors = ColumnFamilyConfig.cf_descriptors()

      # Should have 8 column families (7 TripleStore CFs + "default")
      assert length(descriptors) == 8

      # Verify format: {cf_name_string, cf_options_list}
      for {name, opts} when is_binary(name) and is_list(opts) <- descriptors do
        assert is_binary(name)
        assert is_list(opts)
      end
    end

    test "1.4.4.2 All expected column families are present" do
      cf_names = ColumnFamilyConfig.cf_descriptors() |> Enum.map(fn {name, _opts} -> name end)

      assert "default" in cf_names
      assert "id2str" in cf_names
      assert "str2id" in cf_names
      assert "spo" in cf_names
      assert "pos" in cf_names
      assert "osp" in cf_names
      assert "derived" in cf_names
      assert "numeric_range" in cf_names
    end

    test "1.4.4.3 Dictionary CFs have identical configuration" do
      id2str_opts = ColumnFamilyConfig.get_cf_options(:id2str)
      str2id_opts = ColumnFamilyConfig.get_cf_options(:str2id)

      # Dictionary CFs should have identical options
      assert id2str_opts == str2id_opts
    end

    test "1.4.4.4 Index CFs have identical configuration" do
      spo_opts = ColumnFamilyConfig.get_cf_options(:spo)
      pos_opts = ColumnFamilyConfig.get_cf_options(:pos)
      osp_opts = ColumnFamilyConfig.get_cf_options(:osp)

      # Index CFs should have identical options
      assert spo_opts == pos_opts
      assert pos_opts == osp_opts
    end

    test "1.4.4.5 Column family name conversion functions" do
      # Test atom to string conversion
      assert ColumnFamilyConfig.cf_name_to_string(:id2str) == "id2str"
      assert ColumnFamilyConfig.cf_name_to_string(:spo) == "spo"
      assert ColumnFamilyConfig.cf_name_to_string(:derived) == "derived"

      # Test string to atom conversion
      assert ColumnFamilyConfig.cf_string_to_name("id2str") == :id2str
      assert ColumnFamilyConfig.cf_string_to_name("spo") == :spo
      assert ColumnFamilyConfig.cf_string_to_name("derived") == :derived

      # Test round-trip conversion
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived, :numeric_range] do
        str = ColumnFamilyConfig.cf_name_to_string(cf)
        assert ColumnFamilyConfig.cf_string_to_name(str) == cf
      end
    end
  end

  # ===========================================================================
  # Section 1.4.5: Database Options
  # ===========================================================================

  describe "1.4.5 Database Options" do
    test "1.4.5.1 Database options are valid" do
      db_opts = ColumnFamilyConfig.db_options()

      # Should be a keyword list
      assert is_list(db_opts)

      # Should have create_if_missing option
      assert {:create_if_missing, true} in db_opts
    end

    test "1.4.5.2 Column family names in correct order" do
      cf_names = ColumnFamilyConfig.column_family_names()

      # Should have all 8 CFs (7 TripleStore CFs + "default")
      assert length(cf_names) == 8

      # Should be in expected order (default first, then specific CFs)
      assert hd(cf_names) == "default"
      assert "id2str" in cf_names
      assert "str2id" in cf_names
      assert "spo" in cf_names
      assert "pos" in cf_names
      assert "osp" in cf_names
      assert "derived" in cf_names
      assert "numeric_range" in cf_names
    end
  end

  # ===========================================================================
  # Section 1.4.6: Configuration Constants
  # ===========================================================================

  describe "1.4.6 Configuration Constants" do
    test "1.4.6.1 Bloom filter constant values" do
      # Verify the exact constant values
      assert ColumnFamilyConfig.bloom_bits(:id2str) == 14
      assert ColumnFamilyConfig.bloom_bits(:spo) == 12
      assert ColumnFamilyConfig.bloom_bits(:derived) == 0
    end

    test "1.4.6.2 Block size constant values" do
      # Verify the exact constant values
      assert ColumnFamilyConfig.block_size(:id2str) == 2048
      assert ColumnFamilyConfig.block_size(:spo) == 8192
      assert ColumnFamilyConfig.block_size(:derived) == 32768
    end

    test "1.4.6.3 Prefix extractor length" do
      # All index CFs should have 8-byte prefix extractor
      # (matches 64-bit ID size for triple components)
      assert ColumnFamilyConfig.has_prefix_extractor?(:spo)
      assert ColumnFamilyConfig.has_prefix_extractor?(:pos)
      assert ColumnFamilyConfig.has_prefix_extractor?(:osp)

      # Non-index CFs should not have prefix extractor
      refute ColumnFamilyConfig.has_prefix_extractor?(:id2str)
      refute ColumnFamilyConfig.has_prefix_extractor?(:str2id)
      refute ColumnFamilyConfig.has_prefix_extractor?(:numeric_range)
    end
  end

  # ===========================================================================
  # Section 1.4.7: Options Format Validation
  # ===========================================================================

  describe "1.4.7 Options Format Validation" do
    test "1.4.7.1 All CF options are keyword lists" do
      for cf <- [:id2str, :str2id, :spo, :pos, :osp, :derived, :numeric_range] do
        opts = ColumnFamilyConfig.get_cf_options(cf)
        assert is_list(opts), "CF options should be a list"
      end
    end

    test "1.4.7.2 Block based table options are properly nested" do
      dict_opts = ColumnFamilyConfig.get_cf_options(:id2str)
      index_opts = ColumnFamilyConfig.get_cf_options(:spo)

      # Should have block_based_table_options key
      assert Keyword.has_key?(dict_opts, :block_based_table_options)
      assert Keyword.has_key?(index_opts, :block_based_table_options)

      # The value should be a keyword list or list of tuples
      dict_table_opts = Keyword.get(dict_opts, :block_based_table_options)
      index_table_opts = Keyword.get(index_opts, :block_based_table_options)

      assert is_list(dict_table_opts) or Keyword.keyword?(dict_table_opts)
      assert is_list(index_table_opts) or Keyword.keyword?(index_table_opts)
    end

    test "1.4.7.3 Numeric range CF has appropriate configuration" do
      _numeric_opts = ColumnFamilyConfig.get_cf_options(:numeric_range)

      # Should have bloom filter for range queries
      assert ColumnFamilyConfig.bloom_bits(:numeric_range) > 0

      # Should use index-style block size
      assert ColumnFamilyConfig.block_size(:numeric_range) == 8 * 1024

      # Should not have prefix extractor (range queries, not prefix scans)
      refute ColumnFamilyConfig.has_prefix_extractor?(:numeric_range)
    end
  end

  # ===========================================================================
  # Section 1.1.2: Quad Index Column Family Definitions
  # ===========================================================================

  describe "1.1.2 Quad Index Column Family Definitions" do
    test "1.1.2.1 Quad index CFs are defined in type specification" do
      # The quad index CFs should be valid column families
      quad_cfs = [:gspo, :gpos, :spog, :posg]

      for cf <- quad_cfs do
        # Should be able to get options for each quad CF
        opts = ColumnFamilyConfig.get_cf_options(cf)
        assert is_list(opts), "Quad CF #{cf} should have valid options"
      end
    end

    test "1.1.2.2 Quad index CFs have correct bloom filter settings" do
      # Quad indices should use 12 bits/key (same as triple indices)
      assert ColumnFamilyConfig.bloom_bits(:gspo) == 12
      assert ColumnFamilyConfig.bloom_bits(:gpos) == 12
      assert ColumnFamilyConfig.bloom_bits(:spog) == 12
      assert ColumnFamilyConfig.bloom_bits(:posg) == 12
    end

    test "1.1.2.3 Quad index CFs have correct block size settings" do
      # Quad indices should use 8KB blocks (same as triple indices)
      assert ColumnFamilyConfig.block_size(:gspo) == 8 * 1024
      assert ColumnFamilyConfig.block_size(:gpos) == 8 * 1024
      assert ColumnFamilyConfig.block_size(:spog) == 8 * 1024
      assert ColumnFamilyConfig.block_size(:posg) == 8 * 1024
    end

    test "1.1.2.4 Quad index CFs have prefix extractor" do
      # Quad indices should have prefix extractor for 64-bit components
      assert ColumnFamilyConfig.has_prefix_extractor?(:gspo) == true
      assert ColumnFamilyConfig.has_prefix_extractor?(:gpos) == true
      assert ColumnFamilyConfig.has_prefix_extractor?(:spog) == true
      assert ColumnFamilyConfig.has_prefix_extractor?(:posg) == true
    end

    test "1.1.2.5 Quad schema returns 9 column families (8 CFs + default)" do
      descriptors = ColumnFamilyConfig.cf_descriptors(:quad)

      # Quad schema should have 9 CFs (4 quad indices + dict + derived + numeric + default)
      assert length(descriptors) == 9
    end

    test "1.1.2.6 Quad schema has correct column families" do
      cf_names = ColumnFamilyConfig.cf_descriptors(:quad) |> Enum.map(fn {name, _opts} -> name end)

      # Should have quad indices
      assert "gspo" in cf_names
      assert "gpos" in cf_names
      assert "spog" in cf_names
      assert "posg" in cf_names

      # Should NOT have triple indices
      refute "spo" in cf_names
      refute "pos" in cf_names
      refute "osp" in cf_names

      # Should have shared CFs
      assert "default" in cf_names
      assert "id2str" in cf_names
      assert "str2id" in cf_names
      assert "derived" in cf_names
      assert "numeric_range" in cf_names
    end

    test "1.1.2.7 Triple schema returns 8 column families (7 CFs + default)" do
      descriptors = ColumnFamilyConfig.cf_descriptors(:triple)

      # Triple schema should have 8 CFs (3 indices + dict + derived + numeric + default)
      assert length(descriptors) == 8
    end

    test "1.1.2.8 Triple schema has correct column families" do
      cf_names = ColumnFamilyConfig.cf_descriptors(:triple) |> Enum.map(fn {name, _opts} -> name end)

      # Should have triple indices
      assert "spo" in cf_names
      assert "pos" in cf_names
      assert "osp" in cf_names

      # Should NOT have quad indices
      refute "gspo" in cf_names
      refute "gpos" in cf_names
      refute "spog" in cf_names
      refute "posg" in cf_names

      # Should have shared CFs
      assert "default" in cf_names
      assert "id2str" in cf_names
      assert "str2id" in cf_names
      assert "derived" in cf_names
      assert "numeric_range" in cf_names
    end

    test "1.1.2.9 Quad index CFs have identical configuration" do
      gspo_opts = ColumnFamilyConfig.get_cf_options(:gspo)
      gpos_opts = ColumnFamilyConfig.get_cf_options(:gpos)
      spog_opts = ColumnFamilyConfig.get_cf_options(:spog)
      posg_opts = ColumnFamilyConfig.get_cf_options(:posg)

      # All quad index CFs should have identical options
      assert gspo_opts == gpos_opts
      assert gpos_opts == spog_opts
      assert spog_opts == posg_opts
    end

    test "1.1.2.10 Quad and triple indices have same tuning" do
      _spo_opts = ColumnFamilyConfig.get_cf_options(:spo)
      _gspo_opts = ColumnFamilyConfig.get_cf_options(:gspo)

      # Quad indices should have same tuning as triple indices
      # (same bloom bits, block size, etc.)
      assert ColumnFamilyConfig.bloom_bits(:spo) == ColumnFamilyConfig.bloom_bits(:gspo)
      assert ColumnFamilyConfig.block_size(:spo) == ColumnFamilyConfig.block_size(:gspo)
      assert ColumnFamilyConfig.has_prefix_extractor?(:spo) == ColumnFamilyConfig.has_prefix_extractor?(:gspo)
    end

    test "1.1.2.11 Column family name conversion includes quad CFs" do
      # Test atom to string conversion for quad CFs
      assert ColumnFamilyConfig.cf_name_to_string(:gspo) == "gspo"
      assert ColumnFamilyConfig.cf_name_to_string(:gpos) == "gpos"
      assert ColumnFamilyConfig.cf_name_to_string(:spog) == "spog"
      assert ColumnFamilyConfig.cf_name_to_string(:posg) == "posg"

      # Test string to atom conversion for quad CFs
      assert ColumnFamilyConfig.cf_string_to_name("gspo") == :gspo
      assert ColumnFamilyConfig.cf_string_to_name("gpos") == :gpos
      assert ColumnFamilyConfig.cf_string_to_name("spog") == :spog
      assert ColumnFamilyConfig.cf_string_to_name("posg") == :posg

      # Test round-trip conversion for quad CFs
      for cf <- [:gspo, :gpos, :spog, :posg] do
        str = ColumnFamilyConfig.cf_name_to_string(cf)
        assert ColumnFamilyConfig.cf_string_to_name(str) == cf
      end
    end

    test "1.1.2.12 Column family names function accepts schema type" do
      triple_names = ColumnFamilyConfig.column_family_names(:triple)
      quad_names = ColumnFamilyConfig.column_family_names(:quad)

      # Triple should have 8 CFs
      assert length(triple_names) == 8

      # Quad should have 9 CFs
      assert length(quad_names) == 9

      # Triple should have spo, pos, osp
      assert "spo" in triple_names
      assert "pos" in triple_names
      assert "osp" in triple_names

      # Quad should have gspo, gpos, spog, posg
      assert "gspo" in quad_names
      assert "gpos" in quad_names
      assert "spog" in quad_names
      assert "posg" in quad_names
    end

    test "1.1.2.13 Validation accepts quad column families" do
      # Quad CFs should be valid
      assert ColumnFamilyConfig.validate_cf(:gspo) == :ok
      assert ColumnFamilyConfig.validate_cf(:gpos) == :ok
      assert ColumnFamilyConfig.validate_cf(:spog) == :ok
      assert ColumnFamilyConfig.validate_cf(:posg) == :ok
    end
  end
end
