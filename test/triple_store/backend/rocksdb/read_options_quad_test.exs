defmodule TripleStore.Backend.RocksDB.ReadOptionsQuadTest do
  @moduledoc """
  Unit tests for quad store read options.

  Tests the new quad-specific read presets and for_cf handling for quad column families.
  """

  use ExUnit.Case, async: true
  alias TripleStore.Backend.RocksDB.ReadOptions

  # ===========================================================================
  # Section 1.7.3: Read Options for Quads
  # ===========================================================================

  describe "Section 1.7.3: Quad Read Option Presets" do
    test "quad_prefix_scan/0 returns correct options" do
      opts = ReadOptions.quad_prefix_scan()

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == false
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end

    test "cross_graph_scan/0 returns correct options" do
      opts = ReadOptions.cross_graph_scan()

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == false
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end
  end

  # ===========================================================================
  # Section 1.7.1 & 1.7.3: Column Family Read Options
  # ===========================================================================

  describe "for_cf/1 with quad column families" do
    test "for_cf(:gspo) returns quad_prefix_scan options" do
      opts = ReadOptions.for_cf(:gspo)

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == false
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end

    test "for_cf(:gpos) returns quad_prefix_scan options" do
      opts = ReadOptions.for_cf(:gpos)

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == false
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end

    test "for_cf(:spog) returns cross_graph_scan options" do
      opts = ReadOptions.for_cf(:spog)

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == false
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end

    test "for_cf(:posg) returns cross_graph_scan options" do
      opts = ReadOptions.for_cf(:posg)

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == false
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end
  end

  # ===========================================================================
  # Backward Compatibility - Triple Store CFs
  # ===========================================================================

  describe "for_cf/1 with triple store column families" do
    test "for_cf(:spo) still returns prefix_scan options" do
      opts = ReadOptions.for_cf(:spo)

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == false
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end

    test "for_cf(:pos) still returns prefix_scan options" do
      opts = ReadOptions.for_cf(:pos)

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == false
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end

    test "for_cf(:osp) still returns prefix_scan options" do
      opts = ReadOptions.for_cf(:osp)

      assert Keyword.get(opts, :fill_cache) == true
      assert Keyword.get(opts, :total_order_seek) == false
      assert Keyword.get(opts, :prefix_same_as_start) == true
    end
  end

  # ===========================================================================
  # Read Option Strategy Documentation
  # ===========================================================================

  describe "Read Strategy per Index Type" do
    test "graph-scoped indices (gspo, gpos) use same options as quad_prefix_scan" do
      gspo_opts = ReadOptions.for_cf(:gspo)
      gpos_opts = ReadOptions.for_cf(:gpos)
      quad_scan = ReadOptions.quad_prefix_scan()

      # Both should use the same preset
      assert gspo_opts == quad_scan
      assert gpos_opts == quad_scan
    end

    test "cross-graph indices (spog, posg) use same options as cross_graph_scan" do
      spog_opts = ReadOptions.for_cf(:spog)
      posg_opts = ReadOptions.for_cf(:posg)
      cross_scan = ReadOptions.cross_graph_scan()

      # Both should use the same preset
      assert spog_opts == cross_scan
      assert posg_opts == cross_scan
    end

    test "triple indices (spo, pos, osp) still work with prefix_scan" do
      spo_opts = ReadOptions.for_cf(:spo)
      pos_opts = ReadOptions.for_cf(:pos)
      osp_opts = ReadOptions.for_cf(:osp)
      prefix_scan = ReadOptions.prefix_scan()

      # All should use prefix_scan
      assert spo_opts == prefix_scan
      assert pos_opts == prefix_scan
      assert osp_opts == prefix_scan
    end
  end

  # ===========================================================================
  # Cache Behavior
  # ===========================================================================

  describe "Cache behavior for quad indices" do
    test "quad indices enable cache for query performance" do
      for cf <- [:gspo, :gpos, :spog, :posg] do
        opts = ReadOptions.for_cf(cf)
        assert Keyword.get(opts, :fill_cache) == true,
               "Column family #{cf} should have cache enabled"
      end
    end
  end
end
