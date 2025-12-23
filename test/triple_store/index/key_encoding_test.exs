defmodule TripleStore.Index.KeyEncodingTest do
  @moduledoc """
  Tests for Task 1.4.1: Key Encoding for Triple Index Layer.

  Verifies that:
  - SPO, POS, OSP keys are encoded correctly
  - Keys decode back to original values (roundtrip)
  - Keys are 24 bytes (3 x 64-bit IDs)
  - Big-endian encoding produces correct lexicographic ordering
  - Prefix functions generate correct length prefixes
  - Utility functions work correctly
  """

  use ExUnit.Case, async: true
  import Bitwise

  alias TripleStore.Index

  # ===========================================================================
  # SPO Key Encoding/Decoding
  # ===========================================================================

  describe "spo_key/3" do
    test "encodes to 24 bytes" do
      key = Index.spo_key(1, 2, 3)
      assert byte_size(key) == 24
    end

    test "encodes small values correctly" do
      key = Index.spo_key(1, 2, 3)
      assert key == <<0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3>>
    end

    test "encodes large values correctly" do
      # Use large 64-bit values
      s = 0x123456789ABCDEF0
      p = 0xFEDCBA9876543210
      o = 0x0102030405060708

      key = Index.spo_key(s, p, o)

      assert key ==
               <<0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0, 0xFE, 0xDC, 0xBA, 0x98, 0x76,
                 0x54, 0x32, 0x10, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08>>
    end

    test "encodes zero values" do
      key = Index.spo_key(0, 0, 0)
      assert key == <<0::192>>
    end

    test "encodes max 64-bit values" do
      max = (1 <<< 64) - 1
      key = Index.spo_key(max, max, max)

      assert key ==
               <<255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
                 255, 255, 255, 255, 255, 255, 255, 255>>
    end
  end

  describe "decode_spo_key/1" do
    test "decodes small values correctly" do
      key = Index.spo_key(1, 2, 3)
      assert Index.decode_spo_key(key) == {1, 2, 3}
    end

    test "decodes large values correctly" do
      s = 0x123456789ABCDEF0
      p = 0xFEDCBA9876543210
      o = 0x0102030405060708

      key = Index.spo_key(s, p, o)
      assert Index.decode_spo_key(key) == {s, p, o}
    end

    test "decodes zero values" do
      key = Index.spo_key(0, 0, 0)
      assert Index.decode_spo_key(key) == {0, 0, 0}
    end

    test "decodes max values" do
      max = (1 <<< 64) - 1
      key = Index.spo_key(max, max, max)
      assert Index.decode_spo_key(key) == {max, max, max}
    end

    test "roundtrip preserves all values" do
      test_cases = [
        {1, 2, 3},
        {100, 200, 300},
        {1_000_000, 2_000_000, 3_000_000},
        {0, 0, 0},
        {(1 <<< 64) - 1, (1 <<< 64) - 1, (1 <<< 64) - 1},
        {1, 0, 0},
        {0, 1, 0},
        {0, 0, 1}
      ]

      for {s, p, o} <- test_cases do
        key = Index.spo_key(s, p, o)
        assert Index.decode_spo_key(key) == {s, p, o}, "Failed for {#{s}, #{p}, #{o}}"
      end
    end
  end

  describe "spo_prefix/1" do
    test "creates 8-byte prefix for subject" do
      prefix = Index.spo_prefix(42)
      assert byte_size(prefix) == 8
    end

    test "prefix matches keys with same subject" do
      subject = 12_345
      prefix = Index.spo_prefix(subject)

      key1 = Index.spo_key(subject, 100, 200)
      key2 = Index.spo_key(subject, 999, 888)

      assert String.starts_with?(key1, prefix)
      assert String.starts_with?(key2, prefix)
    end

    test "prefix does not match keys with different subject" do
      prefix = Index.spo_prefix(100)
      key = Index.spo_key(200, 100, 50)

      refute String.starts_with?(key, prefix)
    end
  end

  describe "spo_prefix/2" do
    test "creates 16-byte prefix for subject and predicate" do
      prefix = Index.spo_prefix(42, 100)
      assert byte_size(prefix) == 16
    end

    test "prefix matches keys with same subject and predicate" do
      subject = 12_345
      predicate = 67_890
      prefix = Index.spo_prefix(subject, predicate)

      key1 = Index.spo_key(subject, predicate, 111)
      key2 = Index.spo_key(subject, predicate, 222)

      assert String.starts_with?(key1, prefix)
      assert String.starts_with?(key2, prefix)
    end

    test "prefix does not match keys with different predicate" do
      prefix = Index.spo_prefix(100, 200)
      key = Index.spo_key(100, 300, 400)

      refute String.starts_with?(key, prefix)
    end
  end

  # ===========================================================================
  # POS Key Encoding/Decoding
  # ===========================================================================

  describe "pos_key/3" do
    test "encodes to 24 bytes" do
      key = Index.pos_key(2, 3, 1)
      assert byte_size(key) == 24
    end

    test "encodes with predicate first, then object, then subject" do
      # Verify the ordering is P-O-S not S-P-O
      key = Index.pos_key(2, 3, 1)

      # Predicate (2) should be in first 8 bytes
      <<first::64-big, second::64-big, third::64-big>> = key
      assert first == 2
      assert second == 3
      assert third == 1
    end

    test "encodes large values correctly" do
      p = 0x123456789ABCDEF0
      o = 0xFEDCBA9876543210
      s = 0x0102030405060708

      key = Index.pos_key(p, o, s)
      {decoded_p, decoded_o, decoded_s} = Index.decode_pos_key(key)

      assert decoded_p == p
      assert decoded_o == o
      assert decoded_s == s
    end
  end

  describe "decode_pos_key/1" do
    test "roundtrip preserves all values" do
      test_cases = [
        {2, 3, 1},
        {200, 300, 100},
        {0, 0, 0},
        {(1 <<< 64) - 1, (1 <<< 64) - 1, (1 <<< 64) - 1}
      ]

      for {p, o, s} <- test_cases do
        key = Index.pos_key(p, o, s)
        assert Index.decode_pos_key(key) == {p, o, s}, "Failed for {#{p}, #{o}, #{s}}"
      end
    end
  end

  describe "pos_prefix/1" do
    test "creates 8-byte prefix for predicate" do
      prefix = Index.pos_prefix(42)
      assert byte_size(prefix) == 8
    end

    test "prefix matches keys with same predicate" do
      predicate = 12_345
      prefix = Index.pos_prefix(predicate)

      key1 = Index.pos_key(predicate, 100, 200)
      key2 = Index.pos_key(predicate, 999, 888)

      assert String.starts_with?(key1, prefix)
      assert String.starts_with?(key2, prefix)
    end
  end

  describe "pos_prefix/2" do
    test "creates 16-byte prefix for predicate and object" do
      prefix = Index.pos_prefix(42, 100)
      assert byte_size(prefix) == 16
    end

    test "prefix matches keys with same predicate and object" do
      predicate = 12_345
      object = 67_890
      prefix = Index.pos_prefix(predicate, object)

      key1 = Index.pos_key(predicate, object, 111)
      key2 = Index.pos_key(predicate, object, 222)

      assert String.starts_with?(key1, prefix)
      assert String.starts_with?(key2, prefix)
    end
  end

  # ===========================================================================
  # OSP Key Encoding/Decoding
  # ===========================================================================

  describe "osp_key/3" do
    test "encodes to 24 bytes" do
      key = Index.osp_key(3, 1, 2)
      assert byte_size(key) == 24
    end

    test "encodes with object first, then subject, then predicate" do
      key = Index.osp_key(3, 1, 2)

      <<first::64-big, second::64-big, third::64-big>> = key
      assert first == 3
      assert second == 1
      assert third == 2
    end

    test "encodes large values correctly" do
      o = 0x123456789ABCDEF0
      s = 0xFEDCBA9876543210
      p = 0x0102030405060708

      key = Index.osp_key(o, s, p)
      {decoded_o, decoded_s, decoded_p} = Index.decode_osp_key(key)

      assert decoded_o == o
      assert decoded_s == s
      assert decoded_p == p
    end
  end

  describe "decode_osp_key/1" do
    test "roundtrip preserves all values" do
      test_cases = [
        {3, 1, 2},
        {300, 100, 200},
        {0, 0, 0},
        {(1 <<< 64) - 1, (1 <<< 64) - 1, (1 <<< 64) - 1}
      ]

      for {o, s, p} <- test_cases do
        key = Index.osp_key(o, s, p)
        assert Index.decode_osp_key(key) == {o, s, p}, "Failed for {#{o}, #{s}, #{p}}"
      end
    end
  end

  describe "osp_prefix/1" do
    test "creates 8-byte prefix for object" do
      prefix = Index.osp_prefix(42)
      assert byte_size(prefix) == 8
    end

    test "prefix matches keys with same object" do
      object = 12_345
      prefix = Index.osp_prefix(object)

      key1 = Index.osp_key(object, 100, 200)
      key2 = Index.osp_key(object, 999, 888)

      assert String.starts_with?(key1, prefix)
      assert String.starts_with?(key2, prefix)
    end
  end

  describe "osp_prefix/2" do
    test "creates 16-byte prefix for object and subject" do
      prefix = Index.osp_prefix(42, 100)
      assert byte_size(prefix) == 16
    end

    test "prefix matches keys with same object and subject" do
      object = 12_345
      subject = 67_890
      prefix = Index.osp_prefix(object, subject)

      key1 = Index.osp_key(object, subject, 111)
      key2 = Index.osp_key(object, subject, 222)

      assert String.starts_with?(key1, prefix)
      assert String.starts_with?(key2, prefix)
    end
  end

  # ===========================================================================
  # Lexicographic Ordering
  # ===========================================================================

  describe "lexicographic ordering" do
    test "SPO keys order by subject first" do
      key1 = Index.spo_key(1, 999, 999)
      key2 = Index.spo_key(2, 1, 1)

      assert key1 < key2
    end

    test "SPO keys with same subject order by predicate" do
      key1 = Index.spo_key(100, 1, 999)
      key2 = Index.spo_key(100, 2, 1)

      assert key1 < key2
    end

    test "SPO keys with same subject and predicate order by object" do
      key1 = Index.spo_key(100, 200, 1)
      key2 = Index.spo_key(100, 200, 2)

      assert key1 < key2
    end

    test "POS keys order by predicate first" do
      key1 = Index.pos_key(1, 999, 999)
      key2 = Index.pos_key(2, 1, 1)

      assert key1 < key2
    end

    test "OSP keys order by object first" do
      key1 = Index.osp_key(1, 999, 999)
      key2 = Index.osp_key(2, 1, 1)

      assert key1 < key2
    end
  end

  # ===========================================================================
  # Utility Functions
  # ===========================================================================

  describe "encode_triple_keys/3" do
    test "returns three keys" do
      keys = Index.encode_triple_keys(1, 2, 3)
      assert length(keys) == 3
    end

    test "returns keys for all three indices" do
      keys = Index.encode_triple_keys(1, 2, 3)
      indices = Enum.map(keys, fn {index, _key} -> index end)

      assert :spo in indices
      assert :pos in indices
      assert :osp in indices
    end

    test "all keys are 24 bytes" do
      keys = Index.encode_triple_keys(100, 200, 300)

      for {_index, key} <- keys do
        assert byte_size(key) == 24
      end
    end

    test "keys decode back to correct triple" do
      s = 100
      p = 200
      o = 300

      keys = Index.encode_triple_keys(s, p, o)

      for {index, key} <- keys do
        assert Index.key_to_triple(index, key) == {s, p, o}
      end
    end
  end

  describe "key_to_triple/2" do
    test "converts SPO key to {s, p, o}" do
      key = Index.spo_key(1, 2, 3)
      assert Index.key_to_triple(:spo, key) == {1, 2, 3}
    end

    test "converts POS key to {s, p, o}" do
      # POS key stores as (p, o, s) but should return (s, p, o)
      key = Index.pos_key(2, 3, 1)
      assert Index.key_to_triple(:pos, key) == {1, 2, 3}
    end

    test "converts OSP key to {s, p, o}" do
      # OSP key stores as (o, s, p) but should return (s, p, o)
      key = Index.osp_key(3, 1, 2)
      assert Index.key_to_triple(:osp, key) == {1, 2, 3}
    end

    test "all indices produce same canonical triple" do
      s = 100
      p = 200
      o = 300

      spo_key = Index.spo_key(s, p, o)
      pos_key = Index.pos_key(p, o, s)
      osp_key = Index.osp_key(o, s, p)

      canonical = {s, p, o}

      assert Index.key_to_triple(:spo, spo_key) == canonical
      assert Index.key_to_triple(:pos, pos_key) == canonical
      assert Index.key_to_triple(:osp, osp_key) == canonical
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles boundary between 8-byte segments" do
      # Values that are exactly powers of 256
      s = 256
      p = 256 * 256
      o = 256 * 256 * 256

      key = Index.spo_key(s, p, o)
      assert Index.decode_spo_key(key) == {s, p, o}
    end

    test "handles values with high bits set" do
      # Values with the high bit of each byte set
      s = 0x8080808080808080
      p = 0xFF00FF00FF00FF00
      o = 0x00FF00FF00FF00FF

      key = Index.spo_key(s, p, o)
      assert Index.decode_spo_key(key) == {s, p, o}
    end

    test "handles mixed zero and non-zero values" do
      for {s, p, o} <- [{0, 1, 2}, {1, 0, 2}, {1, 2, 0}, {0, 0, 1}, {0, 1, 0}, {1, 0, 0}] do
        key = Index.spo_key(s, p, o)
        assert Index.decode_spo_key(key) == {s, p, o}
      end
    end
  end
end
