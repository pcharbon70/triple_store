defmodule TripleStore.DictionaryQuadCompatibilityTest do
  @moduledoc """
  Unit tests for quad store compatibility in the Dictionary module.

  Tests that ID 0 is reserved for the default graph and that named graphs
  are encoded correctly as regular RDF terms.
  """

  use ExUnit.Case, async: true
  import Bitwise
  alias TripleStore.Dictionary

  # ===========================================================================
  # Section 1.6.1: Dictionary Validation
  # ===========================================================================

  describe "Section 1.6.1: Dictionary Validation" do
    test "ID 0 is never allocated by encode_id" do
      # The smallest allocated ID has a type tag in the high 4 bits
      # Type tag 1 (URI) shifted left by 60 bits
      uri_id = Dictionary.encode_id(1, 0)
      assert uri_id == 0x1000_0000_0000_0000
      refute uri_id == 0
    end

    test "is_default_graph?/1 returns true only for ID 0" do
      assert Dictionary.is_default_graph?(0) == true
      refute Dictionary.is_default_graph?(1)
      refute Dictionary.is_default_graph?(100)
      refute Dictionary.is_default_graph?(Dictionary.encode_id(1, 42))
    end

    test "is_named_graph?/1 returns false for ID 0" do
      refute Dictionary.is_named_graph?(0)
    end

    test "is_named_graph?/1 returns true for dictionary-allocated terms" do
      uri_id = Dictionary.encode_id(Dictionary.type_uri(), 42)
      assert Dictionary.is_named_graph?(uri_id)

      bnode_id = Dictionary.encode_id(Dictionary.type_bnode(), 123)
      assert Dictionary.is_named_graph?(bnode_id)

      literal_id = Dictionary.encode_id(Dictionary.type_literal(), 456)
      assert Dictionary.is_named_graph?(literal_id)
    end

    test "is_named_graph?/1 returns false for inline-encoded terms" do
      int_id = Dictionary.encode_id(Dictionary.type_integer(), 42)
      refute Dictionary.is_named_graph?(int_id)

      decimal_id = Dictionary.encode_id(Dictionary.type_decimal(), 123)
      refute Dictionary.is_named_graph?(decimal_id)

      datetime_id = Dictionary.encode_id(Dictionary.type_datetime(), 789)
      refute Dictionary.is_named_graph?(datetime_id)
    end
  end

  # ===========================================================================
  # Section 1.6.2: Term ID Bounds Validation
  # ===========================================================================

  describe "Section 1.6.2: Term ID Bounds Validation" do
    test "valid_graph_id?/1 returns false for ID 0 (reserved for default graph)" do
      refute Dictionary.valid_graph_id?(0)
    end

    test "valid_graph_id?/1 returns true for positive integers" do
      assert Dictionary.valid_graph_id?(1)
      assert Dictionary.valid_graph_id?(100)
      assert Dictionary.valid_graph_id?(999_999)
    end

    test "valid_graph_id?/1 returns true for dictionary-allocated term IDs" do
      uri_id = Dictionary.encode_id(Dictionary.type_uri(), 42)
      assert Dictionary.valid_graph_id?(uri_id)

      bnode_id = Dictionary.encode_id(Dictionary.type_bnode(), 123)
      assert Dictionary.valid_graph_id?(bnode_id)
    end

    test "valid_graph_id?/1 returns true for inline-encoded term IDs" do
      # While inline-encoded IDs aren't typically used as graphs,
      # they are still positive integers and technically valid
      int_id = Dictionary.encode_id(Dictionary.type_integer(), 42)
      assert Dictionary.valid_graph_id?(int_id)
    end

    test "encode_id ensures ID 0 is never allocated" do
      # All term IDs have type tags, so they're always >= 2^60
      assert Dictionary.encode_id(1, 0) == 0x1000_0000_0000_0000
      assert Dictionary.encode_id(1, 1) == 0x1000_0000_0000_0001
      assert Dictionary.encode_id(2, 0) == 0x2000_0000_0000_0000
      assert Dictionary.encode_id(3, 0) == 0x3000_0000_0000_0000
    end

    test "sequence counter skips ID 0 by design" do
      # The sequence counter starts at 0, but encode_id adds the type tag
      # So the actual ID stored is 0x1000_0000_0000_0000, not 0
      sequence_0 = Dictionary.encode_id(1, 0)
      assert sequence_0 > 0
      assert Dictionary.decode_id(sequence_0) == {:uri, 0}
    end
  end

  # ===========================================================================
  # Graph ID Constants
  # ===========================================================================

  describe "Graph ID Constants" do
    test "default graph ID is 0" do
      assert Dictionary.is_default_graph?(0)
    end

    test "ID 0 is excluded from named graph validation" do
      refute Dictionary.valid_graph_id?(0)
      refute Dictionary.is_named_graph?(0)
    end

    test "type tags ensure no term gets ID 0" do
      # All types have non-zero type tags
      assert Dictionary.type_uri() > 0
      assert Dictionary.type_bnode() > 0
      assert Dictionary.type_literal() > 0
      assert Dictionary.type_integer() > 0
      assert Dictionary.type_decimal() > 0
      assert Dictionary.type_datetime() > 0
    end
  end

  # ===========================================================================
  # ID Space Verification
  # ===========================================================================

  describe "ID Space Verification" do
    test "all term types are in separate ID ranges" do
      # URI type: 0x1xxx...
      uri_id = Dictionary.encode_id(1, 0)
      assert (uri_id >>> 60) == 1

      # BNode type: 0x2xxx...
      bnode_id = Dictionary.encode_id(2, 0)
      assert (bnode_id >>> 60) == 2

      # Literal type: 0x3xxx...
      literal_id = Dictionary.encode_id(3, 0)
      assert (literal_id >>> 60) == 3

      # Integer type: 0x4xxx...
      int_id = Dictionary.encode_id(4, 0)
      assert (int_id >>> 60) == 4

      # Decimal type: 0x5xxx...
      decimal_id = Dictionary.encode_id(5, 0)
      assert (decimal_id >>> 60) == 5

      # DateTime type: 0x6xxx...
      datetime_id = Dictionary.encode_id(6, 0)
      assert (datetime_id >>> 60) == 6
    end

    test "no term type can produce ID 0" do
      for type <- 1..6 do
        id = Dictionary.encode_id(type, 0)
        assert id > 0, "Type #{type} produced ID 0 for sequence 0"
      end
    end

    test "ID 0 has no type tag" do
      # ID 0 decodes to unknown type, not a valid term type
      assert Dictionary.decode_id(0) == {:unknown, 0}
      assert Dictionary.term_type(0) == :unknown
    end
  end
end
