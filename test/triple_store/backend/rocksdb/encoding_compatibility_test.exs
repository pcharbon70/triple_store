defmodule TripleStore.Backend.RocksDB.EncodingCompatibilityTest do
  @moduledoc """
  Unit tests for Section 1.3: Binary Encoding Compatibility

  These tests verify that binary encoding formats match between the current
  Elixir implementation and erlang-rocksdb for data continuity.

  Since erlang-rocksdb stores and retrieves binary keys/values transparently,
  the encoding is done entirely in Elixir. These tests verify:
  1. Triple key encoding (24-byte big-endian format)
  2. Dictionary ID encoding (64-bit with type tags)
  3. Inline numeric encoding (integer, decimal, datetime)

  All encoding is pure Elixir using binary pattern matching, so it will
  work correctly with erlang-rocksdb as long as the byte sequences are correct.
  """
  use ExUnit.Case, async: true

  alias TripleStore.Index
  alias TripleStore.Dictionary

  import Bitwise, only: [<<<: 2, >>>: 2, &&&: 2]

  # ===========================================================================
  # Section 1.3.1: Triple Key Encoding Verification
  # ===========================================================================

  describe "1.3.1 Triple Key Encoding Verification" do
    test "1.3.1.1 Document current key format: 24-byte big-endian" do
      # Verify SPO key is exactly 24 bytes
      key = Index.spo_key(1, 2, 3)
      assert byte_size(key) == 24, "SPO key must be 24 bytes"

      # Verify POS key is exactly 24 bytes
      key = Index.pos_key(2, 3, 1)
      assert byte_size(key) == 24, "POS key must be 24 bytes"

      # Verify OSP key is exactly 24 bytes
      key = Index.osp_key(3, 1, 2)
      assert byte_size(key) == 24, "OSP key must be 24 bytes"
    end

    test "1.3.1.2 Test Elixir binary encoding produces correct byte sequence" do
      # Test with known values to verify big-endian encoding
      # Value 1 in big-endian 64-bit: <<0, 0, 0, 0, 0, 0, 0, 1>>
      # Value 2 in big-endian 64-bit: <<0, 0, 0, 0, 0, 0, 0, 2>>
      # Value 3 in big-endian 64-bit: <<0, 0, 0, 0, 0, 0, 0, 3>>

      key = Index.spo_key(1, 2, 3)

      # Verify each 8-byte segment is correct
      <<s::64-big, p::64-big, o::64-big>> = key
      assert s == 1
      assert p == 2
      assert o == 3

      # Verify raw bytes for big-endian encoding
      assert key == <<1::64-big, 2::64-big, 3::64-big>>
    end

    test "1.3.1.3 Verify SPO index encoding" do
      # SPO: Subject-Predicate-Object
      key = Index.spo_key(100, 200, 300)

      # Decode and verify order
      assert <<100::64-big, 200::64-big, 300::64-big>> == key

      # Verify round-trip decoding
      assert {100, 200, 300} == Index.decode_spo_key(key)
    end

    test "1.3.1.4 Verify POS index encoding" do
      # POS: Predicate-Object-Subject
      key = Index.pos_key(100, 200, 300)

      # Decode and verify order (predicate, object, subject)
      assert <<100::64-big, 200::64-big, 300::64-big>> == key

      # Verify round-trip decoding
      assert {100, 200, 300} == Index.decode_pos_key(key)
    end

    test "1.3.1.5 Verify OSP index encoding" do
      # OSP: Object-Subject-Predicate
      key = Index.osp_key(100, 200, 300)

      # Decode and verify order (object, subject, predicate)
      assert <<100::64-big, 200::64-big, 300::64-big>> == key

      # Verify round-trip decoding
      assert {100, 200, 300} == Index.decode_osp_key(key)
    end

    test "1.3.1.6 Verify lexicographic ordering matches numeric ordering" do
      # Big-endian encoding ensures lexicographic ordering matches numeric ordering
      key1 = Index.spo_key(1, 2, 3)
      key2 = Index.spo_key(1, 2, 4)
      key3 = Index.spo_key(2, 1, 1)

      # key1 < key2 because object 3 < object 4
      assert key1 < key2

      # key1 < key3 because subject 1 < subject 2 (most significant component)
      assert key1 < key3

      # Verify prefix ordering for subject-based scans
      prefix_1 = Index.spo_prefix(1)
      prefix_2 = Index.spo_prefix(2)

      assert prefix_1 < prefix_2
      assert byte_size(prefix_1) == 8
      assert byte_size(prefix_2) == 8
    end

    test "1.3.1.7 Verify prefix bytes for range scans" do
      # Subject prefix: first 8 bytes
      prefix_s = Index.spo_prefix(42)
      assert <<42::64-big>> == prefix_s

      # Subject-Predicate prefix: first 16 bytes
      prefix_sp = Index.spo_prefix(42, 100)
      assert <<42::64-big, 100::64-big>> == prefix_sp

      # Verify prefix is actually a prefix of full key
      full_key = Index.spo_key(42, 100, 200)
      assert :binary.match(full_key, prefix_sp) == {0, 16}
    end
  end

  # ===========================================================================
  # Section 1.3.2: Dictionary Encoding Verification
  # ===========================================================================

  describe "1.3.2 Dictionary Encoding Verification" do
    test "1.3.2.1 Document current ID format: 64-bit with type tag in high 4 bits" do
      # Type tag in high 4 bits, value in low 60 bits
      # ID = (type_tag << 60) | value

      id = Dictionary.encode_id(1, 42)

      # Extract type tag (high 4 bits)
      type_tag = id >>> 60
      assert type_tag == 1

      # Extract value (low 60 bits)
      value = id &&& 0x0FFFFFFFFFFFFFFF
      assert value == 42

      # Verify full encoding
      assert id == (1 <<< 60) + 42
    end

    test "1.3.2.2 Verify URI type tag encoding" do
      # URI has type tag 1
      uri_id = Dictionary.encode_id(Dictionary.type_uri(), 12345)

      # Type tag is in high bits
      assert Dictionary.term_type(uri_id) == :uri

      # Value is preserved
      assert {:uri, 12345} == Dictionary.decode_id(uri_id)
    end

    test "1.3.2.3 Verify Blank Node type tag encoding" do
      # BNode has type tag 2
      bnode_id = Dictionary.encode_id(Dictionary.type_bnode(), 67890)

      # Type tag is in high bits
      assert Dictionary.term_type(bnode_id) == :bnode

      # Value is preserved
      assert {:bnode, 67890} == Dictionary.decode_id(bnode_id)
    end

    test "1.3.2.4 Verify Literal type tag encoding" do
      # Literal has type tag 3
      literal_id = Dictionary.encode_id(Dictionary.type_literal(), 99999)

      # Type tag is in high bits
      assert Dictionary.term_type(literal_id) == :literal

      # Value is preserved
      assert {:literal, 99999} == Dictionary.decode_id(literal_id)
    end

    test "1.3.2.5 Verify inline integer type tag encoding" do
      # Integer has type tag 4
      {:ok, int_id} = Dictionary.encode_integer(42)

      # Type tag is in high bits
      assert Dictionary.term_type(int_id) == :integer

      # Value is preserved
      assert {:ok, 42} == Dictionary.decode_integer(int_id)

      # Verify dictionary_allocated? returns false for inline types
      refute Dictionary.dictionary_allocated?(int_id)
      assert Dictionary.inline_encoded?(int_id)
    end

    test "1.3.2.6 Verify inline decimal type tag encoding" do
      # Decimal has type tag 5
      decimal = Decimal.new("3.14")
      {:ok, dec_id} = Dictionary.encode_decimal(decimal)

      # Type tag is in high bits
      assert Dictionary.term_type(dec_id) == :decimal

      # Value decodes correctly
      assert {:ok, decoded} = Dictionary.decode_decimal(dec_id)
      assert Decimal.eq?(decoded, decimal)

      # Verify inline_encoded? returns true
      assert Dictionary.inline_encoded?(dec_id)
    end

    test "1.3.2.7 Verify inline datetime type tag encoding" do
      # DateTime has type tag 6
      {:ok, dt} = DateTime.new(~D[2024-01-15], ~T[10:30:00])
      {:ok, dt_id} = Dictionary.encode_datetime(dt)

      # Type tag is in high bits
      assert Dictionary.term_type(dt_id) == :datetime

      # Value decodes correctly
      assert {:ok, decoded} = Dictionary.decode_datetime(dt_id)
      assert DateTime.to_unix(decoded, :millisecond) == DateTime.to_unix(dt, :millisecond)

      # Verify inline_encoded? returns true
      assert Dictionary.inline_encoded?(dt_id)
    end

    test "1.3.2.8 Verify type tag space separation prevents collisions" do
      # Each type gets its own 2^60 ID space
      uri_id = Dictionary.encode_id(1, 100)
      bnode_id = Dictionary.encode_id(2, 100)
      literal_id = Dictionary.encode_id(3, 100)
      int_id = Dictionary.encode_id(4, 100)

      # All IDs are different despite having same value
      assert uri_id != bnode_id
      assert bnode_id != literal_id
      assert literal_id != int_id

      # Verify each ID is in its correct type range
      # Type 1: 0x1000_0000_0000_0000 to 0x1FFF_FFFF_FFFF_FFFF
      assert uri_id >= 0x1000_0000_0000_0000
      assert uri_id < 0x2000_0000_0000_0000

      # Type 2: 0x2000_0000_0000_0000 to 0x2FFF_FFFF_FFFF_FFFF
      assert bnode_id >= 0x2000_0000_0000_0000
      assert bnode_id < 0x3000_0000_0000_0000

      # Type 3: 0x3000_0000_0000_0000 to 0x3FFF_FFFF_FFFF_FFFF
      assert literal_id >= 0x3000_0000_0000_0000
      assert literal_id < 0x4000_0000_0000_0000

      # Type 4: 0x4000_0000_0000_0000 to 0x4FFF_FFFF_FFFF_FFFF
      assert int_id >= 0x4000_0000_0000_0000
      assert int_id < 0x5000_0000_0000_0000
    end
  end

  # ===========================================================================
  # Section 1.3.3: Inline Numeric Encoding Tests
  # ===========================================================================

  describe "1.3.3 Inline Numeric Encoding" do
    test "1.3.3.1 Verify inline integer encoding preserves negative values" do
      # Test positive integer
      {:ok, pos_id} = Dictionary.encode_integer(42)
      assert {:ok, 42} == Dictionary.decode_integer(pos_id)

      # Test negative integer (two's complement)
      {:ok, neg_id} = Dictionary.encode_integer(-100)
      assert {:ok, -100} == Dictionary.decode_integer(neg_id)

      # Test zero
      {:ok, zero_id} = Dictionary.encode_integer(0)
      assert {:ok, 0} == Dictionary.decode_integer(zero_id)

      # All should have same type tag but different values
      assert Dictionary.term_type(pos_id) == :integer
      assert Dictionary.term_type(neg_id) == :integer
      assert Dictionary.term_type(zero_id) == :integer
    end

    test "1.3.3.2 Verify inline integer range limits" do
      # Test maximum value
      max_val = Dictionary.max_inline_integer()
      {:ok, max_id} = Dictionary.encode_integer(max_val)
      assert {:ok, max_val} == Dictionary.decode_integer(max_id)

      # Test minimum value
      min_val = Dictionary.min_inline_integer()
      {:ok, min_id} = Dictionary.encode_integer(min_val)
      assert {:ok, min_val} == Dictionary.decode_integer(min_id)

      # Test out of range values
      assert {:error, :out_of_range} = Dictionary.encode_integer(max_val + 1)
      assert {:error, :out_of_range} = Dictionary.encode_integer(min_val - 1)
    end

    test "1.3.3.3 Verify inline decimal encoding precision" do
      # Test simple decimal
      d1 = Decimal.new("3.14")
      {:ok, d1_id} = Dictionary.encode_decimal(d1)
      assert {:ok, d1_decoded} = Dictionary.decode_decimal(d1_id)
      assert Decimal.eq?(d1_decoded, d1)

      # Test negative decimal
      d2 = Decimal.new("-2.5")
      {:ok, d2_id} = Dictionary.encode_decimal(d2)
      assert {:ok, d2_decoded} = Dictionary.decode_decimal(d2_id)
      assert Decimal.eq?(d2_decoded, d2)

      # Test zero
      d3 = Decimal.new("0")
      {:ok, d3_id} = Dictionary.encode_decimal(d3)
      assert {:ok, d3_decoded} = Dictionary.decode_decimal(d3_id)
      assert Decimal.eq?(d3_decoded, d3)
    end

    test "1.3.3.4 Verify inline datetime encoding" do
      # Test specific datetime
      {:ok, dt} = DateTime.new(~D[2024-01-15], ~T[10:30:00], "Etc/UTC")
      {:ok, dt_id} = Dictionary.encode_datetime(dt)
      assert {:ok, dt_decoded} = Dictionary.decode_datetime(dt_id)
      assert DateTime.compare(dt, dt_decoded) == :eq

      # Test Unix epoch
      epoch = DateTime.from_unix!(0, :millisecond)
      {:ok, epoch_id} = Dictionary.encode_datetime(epoch)
      assert {:ok, epoch_decoded} = Dictionary.decode_datetime(epoch_id)
      assert DateTime.to_unix(epoch_decoded, :millisecond) == 0
    end

    test "1.3.3.5 Verify decode_inline dispatches correctly" do
      # Integer
      {:ok, int_id} = Dictionary.encode_integer(42)
      assert {:ok, 42} == Dictionary.decode_inline(int_id)

      # Decimal
      {:ok, dec_id} = Dictionary.encode_decimal(Decimal.new("3.14"))
      assert {:ok, dec} = Dictionary.decode_inline(dec_id)
      assert Decimal.eq?(dec, Decimal.new("3.14"))

      # DateTime
      {:ok, dt} = DateTime.new(~D[2024-01-15], ~T[10:30:00])
      {:ok, dt_id} = Dictionary.encode_datetime(dt)
      assert {:ok, _dt_decoded} = Dictionary.decode_inline(dt_id)

      # Non-inline type (URI) returns error
      uri_id = Dictionary.encode_id(1, 100)
      assert {:error, :not_inline_encoded} = Dictionary.decode_inline(uri_id)
    end
  end

  # ===========================================================================
  # Section 1.3.4: Encoding Format Documentation Tests
  # ===========================================================================

  describe "1.3.4 Encoding Format Documentation" do
    test "1.3.4.1 Triple key format constants are accessible" do
      # These tests verify the encoding format is well-documented
      # and constants are accessible for reference

      # Triple key size
      key = Index.spo_key(0, 0, 0)
      assert byte_size(key) == 24

      # Each component is 64 bits (8 bytes)
      assert 24 == 8 * 3
    end

    test "1.3.4.2 Type tag constants are accessible" do
      # Type tags should be well-defined constants
      assert Dictionary.type_uri() == 0b0001
      assert Dictionary.type_bnode() == 0b0010
      assert Dictionary.type_literal() == 0b0011
      assert Dictionary.type_integer() == 0b0100
      assert Dictionary.type_decimal() == 0b0101
      assert Dictionary.type_datetime() == 0b0110
    end

    test "1.3.4.3 ID space ranges are documented" do
      # Verify the ID space separation is documented through constants
      max_sequence = Dictionary.max_sequence()

      # Max sequence is 2^59 - 1
      assert max_sequence == (1 <<< 59) - 1

      # Each type can have this many IDs
      uri_max_id = Dictionary.encode_id(Dictionary.type_uri(), max_sequence)
      assert uri_max_id < 0x2000_0000_0000_0000
    end
  end
end
