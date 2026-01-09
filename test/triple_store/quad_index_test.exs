defmodule TripleStore.QuadIndexTest do
  @moduledoc """
  Unit tests for Section 1.2: Quad Key Encoding

  These tests verify quad key encoding, decoding, prefix construction,
  and utility functions for all four quad indices (GSPO, GPOS, SPOG, POSG).
  """
  use ExUnit.Case, async: true

  alias TripleStore.QuadIndex

  import Bitwise, only: [<<<: 2]

  # ===========================================================================
  # Section 1.2.1: Key Encoding Functions
  # ===========================================================================

  describe "1.2.1 Key Encoding Functions" do
    test "1.2.1.1 gspo_key encodes graph, subject, predicate, object" do
      key = QuadIndex.gspo_key(0, 1, 2, 3)
      assert byte_size(key) == 32
      assert key == <<0::64-big, 1::64-big, 2::64-big, 3::64-big>>
    end

    test "1.2.1.2 gpos_key encodes graph, predicate, object, subject" do
      key = QuadIndex.gpos_key(0, 2, 3, 1)
      assert byte_size(key) == 32
      assert key == <<0::64-big, 2::64-big, 3::64-big, 1::64-big>>
    end

    test "1.2.1.3 spog_key encodes subject, predicate, object, graph" do
      key = QuadIndex.spog_key(1, 2, 3, 0)
      assert byte_size(key) == 32
      assert key == <<1::64-big, 2::64-big, 3::64-big, 0::64-big>>
    end

    test "1.2.1.4 posg_key encodes predicate, object, subject, graph" do
      key = QuadIndex.posg_key(2, 3, 1, 0)
      assert byte_size(key) == 32
      assert key == <<2::64-big, 3::64-big, 1::64-big, 0::64-big>>
    end

    test "encoding preserves big-endian ordering" do
      # Small IDs should produce small keys
      small_key = QuadIndex.gspo_key(1, 1, 1, 1)
      large_key = QuadIndex.gspo_key(2, 2, 2, 2)
      assert small_key < large_key
    end
  end

  # ===========================================================================
  # Section 1.2.2: Key Decoding Functions
  # ===========================================================================

  describe "1.2.2 Key Decoding Functions" do
    test "1.2.2.1 decode_gspo_key extracts {g, s, p, o}" do
      key = QuadIndex.gspo_key(10, 20, 30, 40)
      assert QuadIndex.decode_gspo_key(key) == {10, 20, 30, 40}
    end

    test "1.2.2.2 decode_gpos_key extracts {g, p, o, s}" do
      key = QuadIndex.gpos_key(10, 20, 30, 40)
      assert QuadIndex.decode_gpos_key(key) == {10, 20, 30, 40}
    end

    test "1.2.2.3 decode_spog_key extracts {s, p, o, g}" do
      key = QuadIndex.spog_key(20, 30, 40, 10)
      assert QuadIndex.decode_spog_key(key) == {20, 30, 40, 10}
    end

    test "1.2.2.4 decode_posg_key extracts {p, o, s, g}" do
      key = QuadIndex.posg_key(30, 40, 20, 10)
      assert QuadIndex.decode_posg_key(key) == {30, 40, 20, 10}
    end
  end

  # ===========================================================================
  # Section 1.2.3: Quad Prefix Functions
  # ===========================================================================

  describe "1.2.3 Quad Prefix Functions" do
    test "1.2.3.1 gspo_prefix(g) returns 8-byte prefix" do
      prefix = QuadIndex.gspo_prefix(0)
      assert byte_size(prefix) == 8
      assert prefix == <<0::64-big>>
    end

    test "1.2.3.2 gspo_prefix(g, s) returns 16-byte prefix" do
      prefix = QuadIndex.gspo_prefix(0, 1)
      assert byte_size(prefix) == 16
      assert prefix == <<0::64-big, 1::64-big>>
    end

    test "1.2.3.3 gspo_prefix(g, s, p) returns 24-byte prefix" do
      prefix = QuadIndex.gspo_prefix(0, 1, 2)
      assert byte_size(prefix) == 24
      assert prefix == <<0::64-big, 1::64-big, 2::64-big>>
    end

    test "1.2.3.4 spog_prefix(s) returns 8-byte prefix" do
      prefix = QuadIndex.spog_prefix(1)
      assert byte_size(prefix) == 8
      assert prefix == <<1::64-big>>
    end

    test "1.2.3.5 spog_prefix(s, p) returns 16-byte prefix" do
      prefix = QuadIndex.spog_prefix(1, 2)
      assert byte_size(prefix) == 16
      assert prefix == <<1::64-big, 2::64-big>>
    end

    test "1.2.3.6 posg_prefix(p) returns 8-byte prefix" do
      prefix = QuadIndex.posg_prefix(2)
      assert byte_size(prefix) == 8
      assert prefix == <<2::64-big>>
    end

    test "1.2.3.7 gpos_prefix(g) returns 8-byte prefix" do
      prefix = QuadIndex.gpos_prefix(0)
      assert byte_size(prefix) == 8
      assert prefix == <<0::64-big>>
    end

    test "1.2.3.8 gpos_prefix(g, p) returns 16-byte prefix" do
      prefix = QuadIndex.gpos_prefix(0, 2)
      assert byte_size(prefix) == 16
      assert prefix == <<0::64-big, 2::64-big>>
    end

    test "1.2.3.9 gpos_prefix(g, p, o) returns 24-byte prefix" do
      prefix = QuadIndex.gpos_prefix(0, 2, 3)
      assert byte_size(prefix) == 24
      assert prefix == <<0::64-big, 2::64-big, 3::64-big>>
    end

    test "1.2.3.10 spog_prefix(s, p, o) returns 24-byte prefix" do
      prefix = QuadIndex.spog_prefix(1, 2, 3)
      assert byte_size(prefix) == 24
      assert prefix == <<1::64-big, 2::64-big, 3::64-big>>
    end

    test "1.2.3.11 posg_prefix(p, o) returns 16-byte prefix" do
      prefix = QuadIndex.posg_prefix(2, 3)
      assert byte_size(prefix) == 16
      assert prefix == <<2::64-big, 3::64-big>>
    end

    test "1.2.3.12 posg_prefix(p, o, s) returns 24-byte prefix" do
      prefix = QuadIndex.posg_prefix(2, 3, 1)
      assert byte_size(prefix) == 24
      assert prefix == <<2::64-big, 3::64-big, 1::64-big>>
    end

    test "prefix correctly matches full key" do
      # A key should start with its prefix
      key = QuadIndex.gspo_key(5, 10, 15, 20)
      prefix = QuadIndex.gspo_prefix(5, 10)
      assert binary_part(key, 0, byte_size(prefix)) == prefix
    end
  end

  # ===========================================================================
  # Section 1.2.4: Quad Key Utilities
  # ===========================================================================

  describe "1.2.4 Quad Key Utilities" do
    test "1.2.4.1 encode_quad_keys returns all four index keys" do
      keys = QuadIndex.encode_quad_keys(1, 2, 3, 0)

      assert map_size(keys) == 4
      assert Map.has_key?(keys, :gspo)
      assert Map.has_key?(keys, :gpos)
      assert Map.has_key?(keys, :spog)
      assert Map.has_key?(keys, :posg)

      # All keys should be 32 bytes
      assert byte_size(keys.gspo) == 32
      assert byte_size(keys.gpos) == 32
      assert byte_size(keys.spog) == 32
      assert byte_size(keys.posg) == 32
    end

    test "1.2.4.2 key_to_quad converts GSPO key to canonical {s, p, o, g}" do
      key = QuadIndex.gspo_key(10, 1, 2, 3)
      assert QuadIndex.key_to_quad(:gspo, key) == {1, 2, 3, 10}
    end

    test "1.2.4.3 key_to_quad converts GPOS key to canonical {s, p, o, g}" do
      key = QuadIndex.gpos_key(10, 2, 3, 1)
      assert QuadIndex.key_to_quad(:gpos, key) == {1, 2, 3, 10}
    end

    test "1.2.4.4 key_to_quad converts SPOG key to canonical {s, p, o, g}" do
      key = QuadIndex.spog_key(1, 2, 3, 10)
      assert QuadIndex.key_to_quad(:spog, key) == {1, 2, 3, 10}
    end

    test "1.2.4.5 key_to_quad converts POSG key to canonical {s, p, o, g}" do
      key = QuadIndex.posg_key(2, 3, 1, 10)
      assert QuadIndex.key_to_quad(:posg, key) == {1, 2, 3, 10}
    end

    test "1.2.4.6 quad_to_triple extracts {s, p, o} from quad" do
      quad = {1, 2, 3, 0}
      assert QuadIndex.quad_to_triple(quad) == {1, 2, 3}
    end

    test "1.2.4.7 quad_to_triple with named graph" do
      quad = {1, 2, 3, 10}
      assert QuadIndex.quad_to_triple(quad) == {1, 2, 3}
    end
  end

  # ===========================================================================
  # Roundtrip Tests
  # ===========================================================================

  describe "Roundtrip Encoding/Decoding" do
    test "GSPO encode/decode roundtrip" do
      {g, s, p, o} = {0, 100, 200, 300}
      key = QuadIndex.gspo_key(g, s, p, o)
      assert QuadIndex.decode_gspo_key(key) == {g, s, p, o}
    end

    test "GPOS encode/decode roundtrip" do
      {g, p, o, s} = {0, 200, 300, 100}
      key = QuadIndex.gpos_key(g, p, o, s)
      assert QuadIndex.decode_gpos_key(key) == {g, p, o, s}
    end

    test "SPOG encode/decode roundtrip" do
      {s, p, o, g} = {100, 200, 300, 0}
      key = QuadIndex.spog_key(s, p, o, g)
      assert QuadIndex.decode_spog_key(key) == {s, p, o, g}
    end

    test "POSG encode/decode roundtrip" do
      {p, o, s, g} = {200, 300, 100, 0}
      key = QuadIndex.posg_key(p, o, s, g)
      assert QuadIndex.decode_posg_key(key) == {p, o, s, g}
    end

    test "all four indices encode same quad consistently" do
      quad = {1, 2, 3, 0}
      {s, p, o, g} = quad

      gspo_key = QuadIndex.gspo_key(g, s, p, o)
      gpos_key = QuadIndex.gpos_key(g, p, o, s)
      spog_key = QuadIndex.spog_key(s, p, o, g)
      posg_key = QuadIndex.posg_key(p, o, s, g)

      gspo = QuadIndex.key_to_quad(:gspo, gspo_key)
      gpos = QuadIndex.key_to_quad(:gpos, gpos_key)
      spog = QuadIndex.key_to_quad(:spog, spog_key)
      posg = QuadIndex.key_to_quad(:posg, posg_key)

      assert gspo == quad
      assert gpos == quad
      assert spog == quad
      assert posg == quad
    end
  end

  # ===========================================================================
  # Default Graph Tests
  # ===========================================================================

  describe "Default Graph" do
    test "default_graph_id returns 0" do
      assert QuadIndex.default_graph_id() == 0
    end

    test "is_default_graph? returns true for 0" do
      assert QuadIndex.is_default_graph?(0) == true
    end

    test "is_default_graph? returns false for positive IDs" do
      assert QuadIndex.is_default_graph?(1) == false
      assert QuadIndex.is_default_graph?(100) == false
    end

    test "encoding with default graph ID" do
      key = QuadIndex.gspo_key(0, 1, 2, 3)
      assert byte_size(key) == 32
      {g, s, p, o} = QuadIndex.decode_gspo_key(key)
      assert g == 0
      assert s == 1
      assert p == 2
      assert o == 3
    end
  end

  # ===========================================================================
  # Edge Cases and Boundary Conditions
  # ===========================================================================

  describe "Edge Cases" do
    test "handles maximum term ID" do
      max_id = (1 <<< 64) - 1
      key = QuadIndex.gspo_key(max_id, max_id, max_id, max_id)
      assert byte_size(key) == 32
    end

    test "handles ID 0 for all positions" do
      key = QuadIndex.gspo_key(0, 0, 0, 0)
      assert byte_size(key) == 32
      assert QuadIndex.decode_gspo_key(key) == {0, 0, 0, 0}
    end

    test "handles mixed zero and non-zero IDs" do
      key = QuadIndex.gspo_key(0, 100, 0, 200)
      assert byte_size(key) == 32
      assert QuadIndex.decode_gspo_key(key) == {0, 100, 0, 200}
    end

    test "guards reject invalid IDs" do
      assert_raise FunctionClauseError, fn ->
        QuadIndex.gspo_key(-1, 1, 2, 3)
      end

      assert_raise FunctionClauseError, fn ->
        QuadIndex.gspo_key(1, -1, 2, 3)
      end
    end
  end

  # ===========================================================================
  # Lexicographic Ordering Tests
  # ===========================================================================

  describe "Lexicographic Ordering" do
    test "GSPO keys order correctly by graph" do
      key1 = QuadIndex.gspo_key(0, 1, 2, 3)
      key2 = QuadIndex.gspo_key(1, 1, 2, 3)
      assert key1 < key2
    end

    test "GSPO keys order correctly by subject (same graph)" do
      key1 = QuadIndex.gspo_key(0, 1, 2, 3)
      key2 = QuadIndex.gspo_key(0, 2, 2, 3)
      assert key1 < key2
    end

    test "GSPO keys order correctly by predicate (same graph, subject)" do
      key1 = QuadIndex.gspo_key(0, 1, 2, 3)
      key2 = QuadIndex.gspo_key(0, 1, 3, 3)
      assert key1 < key2
    end

    test "GSPO keys order correctly by object (same graph, subject, predicate)" do
      key1 = QuadIndex.gspo_key(0, 1, 2, 3)
      key2 = QuadIndex.gspo_key(0, 1, 2, 4)
      assert key1 < key2
    end

    test "SPOG keys order correctly by subject" do
      key1 = QuadIndex.spog_key(1, 2, 3, 0)
      key2 = QuadIndex.spog_key(2, 2, 3, 0)
      assert key1 < key2
    end

    test "SPOG keys order correctly by graph (same subject, predicate, object)" do
      key1 = QuadIndex.spog_key(1, 2, 3, 0)
      key2 = QuadIndex.spog_key(1, 2, 3, 1)
      assert key1 < key2
    end
  end

  # ===========================================================================
  # Prefix Boundary Tests
  # ===========================================================================

  describe "Prefix Boundaries" do
    test "gspo_prefix matches all quads with same graph" do
      prefix = QuadIndex.gspo_prefix(5)

      key1 = QuadIndex.gspo_key(5, 1, 2, 3)
      key2 = QuadIndex.gspo_key(5, 100, 200, 300)
      key3 = QuadIndex.gspo_key(6, 1, 2, 3)

      assert binary_part(key1, 0, 8) == prefix
      assert binary_part(key2, 0, 8) == prefix
      refute binary_part(key3, 0, 8) == prefix
    end

    test "gspo_prefix matches all quads with same graph and subject" do
      prefix = QuadIndex.gspo_prefix(5, 10)

      key1 = QuadIndex.gspo_key(5, 10, 2, 3)
      key2 = QuadIndex.gspo_key(5, 10, 200, 300)
      key3 = QuadIndex.gspo_key(5, 11, 2, 3)

      assert binary_part(key1, 0, 16) == prefix
      assert binary_part(key2, 0, 16) == prefix
      refute binary_part(key3, 0, 16) == prefix
    end

    test "spog_prefix matches all quads with same subject across graphs" do
      prefix = QuadIndex.spog_prefix(5)

      key1 = QuadIndex.spog_key(5, 2, 3, 0)
      key2 = QuadIndex.spog_key(5, 200, 300, 100)
      key3 = QuadIndex.spog_key(6, 2, 3, 0)

      assert binary_part(key1, 0, 8) == prefix
      assert binary_part(key2, 0, 8) == prefix
      refute binary_part(key3, 0, 8) == prefix
    end

    test "posg_prefix matches all quads with same predicate across graphs" do
      prefix = QuadIndex.posg_prefix(5)

      key1 = QuadIndex.posg_key(5, 3, 1, 0)
      key2 = QuadIndex.posg_key(5, 300, 100, 200)
      key3 = QuadIndex.posg_key(6, 3, 1, 0)

      assert binary_part(key1, 0, 8) == prefix
      assert binary_part(key2, 0, 8) == prefix
      refute binary_part(key3, 0, 8) == prefix
    end
  end

  # ===========================================================================
  # Named Graph Tests
  # ===========================================================================

  describe "Named Graphs" do
    test "encodes named graph with positive ID" do
      graph_id = 100
      key = QuadIndex.gspo_key(graph_id, 1, 2, 3)
      {g, _s, _p, _o} = QuadIndex.decode_gspo_key(key)
      assert g == graph_id
    end

    test "distinguishes default graph from named graph" do
      default_key = QuadIndex.gspo_key(0, 1, 2, 3)
      named_key = QuadIndex.gspo_key(100, 1, 2, 3)

      # Keys should be different
      refute default_key == named_key

      # Default graph key should be lexicographically smaller
      assert default_key < named_key
    end

    test "same quad in different graphs produces different keys" do
      s = 1
      p = 2
      o = 3

      key_default = QuadIndex.gspo_key(0, s, p, o)
      key_named = QuadIndex.gspo_key(100, s, p, o)

      refute key_default == key_named
    end
  end

  # ===========================================================================
  # Section 1.3: Graph ID Representation
  # ===========================================================================

  describe "1.3.1 Default Graph Identifier" do
    test "is_default_graph? returns true for ID 0" do
      assert QuadIndex.is_default_graph?(0) == true
    end

    test "is_default_graph? returns false for positive IDs" do
      refute QuadIndex.is_default_graph?(1)
      refute QuadIndex.is_default_graph?(100)
      refute QuadIndex.is_default_graph?(0xFFFFFFFFFFFFFFFF)
    end

    test "default_graph_id returns 0" do
      assert QuadIndex.default_graph_id() == 0
    end
  end

  describe "1.3.3 Graph ID Resolution" do
    test "resolve_graph_id :default returns 0" do
      assert {:ok, 0} == QuadIndex.resolve_graph_id(:default, nil)
    end

    test "resolve_graph_id invalid reference returns error" do
      assert {:error, {:invalid_graph_reference, _}} =
        QuadIndex.resolve_graph_id("invalid", nil)
    end

    test "id_to_graph_term for default graph ID returns :not_found" do
      assert :not_found == QuadIndex.id_to_graph_term(0, nil)
    end

    test "id_to_graph_term for positive ID raises without valid db" do
      # Without a valid database reference, the lookup will fail
      assert_raise FunctionClauseError, fn ->
        QuadIndex.id_to_graph_term(12345, nil)
      end
    end
  end
end
