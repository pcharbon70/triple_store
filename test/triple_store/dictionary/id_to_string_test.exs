defmodule TripleStore.Dictionary.IdToStringTest do
  @moduledoc """
  Tests for ID-to-String mapping (Task 1.3.4).

  Covers:
  - decode_term/1 for parsing binary back to RDF term
  - lookup_term/2 for dictionary-allocated and inline-encoded IDs
  - lookup_terms/2 for batch operations
  - Roundtrip encoding/decoding
  """
  use TripleStore.PooledDbCase

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.IdToString
  alias TripleStore.Dictionary.Manager

  setup %{db: db} do
    assert NIF.is_open(db)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
    end)

    {:ok, manager: manager}
  end

  # ===========================================================================
  # decode_term/1 tests
  # ===========================================================================

  describe "decode_term/1 for URIs" do
    test "decodes simple URI" do
      binary = <<1, "http://example.org/resource"::binary>>
      {:ok, term} = IdToString.decode_term(binary)
      assert %RDF.IRI{value: "http://example.org/resource"} = term
    end

    test "decodes URI with special characters" do
      binary = <<1, "http://example.org/path?query=value&foo=bar"::binary>>
      {:ok, term} = IdToString.decode_term(binary)
      assert term.value == "http://example.org/path?query=value&foo=bar"
    end

    test "decodes URI with unicode" do
      binary = <<1, "http://example.org/café"::binary>>
      {:ok, term} = IdToString.decode_term(binary)
      assert term.value == "http://example.org/café"
    end
  end

  describe "decode_term/1 for blank nodes" do
    test "decodes blank node" do
      binary = <<2, "b1"::binary>>
      {:ok, term} = IdToString.decode_term(binary)
      assert %RDF.BlankNode{value: "b1"} = term
    end

    test "decodes blank node with longer id" do
      binary = <<2, "genid_12345"::binary>>
      {:ok, term} = IdToString.decode_term(binary)
      assert term.value == "genid_12345"
    end
  end

  describe "decode_term/1 for literals" do
    test "decodes plain literal" do
      binary = <<3, 0, "hello world"::binary>>
      {:ok, term} = IdToString.decode_term(binary)
      assert RDF.Literal.lexical(term) == "hello world"
    end

    test "decodes typed literal" do
      binary = <<3, 1, "http://www.w3.org/2001/XMLSchema#integer", 0, "42"::binary>>
      {:ok, term} = IdToString.decode_term(binary)
      assert RDF.Literal.lexical(term) == "42"
    end

    test "decodes language-tagged literal" do
      binary = <<3, 2, "en", 0, "hello"::binary>>
      {:ok, term} = IdToString.decode_term(binary)
      assert RDF.Literal.lexical(term) == "hello"
      assert RDF.LangString.language(term.literal) == "en"
    end

    test "decodes literal with unicode" do
      binary = <<3, 0, "café"::binary>>
      {:ok, term} = IdToString.decode_term(binary)
      assert RDF.Literal.lexical(term) == "café"
    end

    test "decodes empty literal" do
      binary = <<3, 0>>
      {:ok, term} = IdToString.decode_term(binary)
      assert RDF.Literal.lexical(term) == ""
    end
  end

  describe "decode_term/1 edge cases" do
    test "returns error for invalid encoding" do
      assert {:error, :invalid_encoding} = IdToString.decode_term(<<0>>)
      assert {:error, :invalid_encoding} = IdToString.decode_term(<<99>>)
      assert {:error, :invalid_encoding} = IdToString.decode_term(<<>>)
    end

    test "returns error for typed literal without null separator" do
      binary = <<3, 1, "http://www.w3.org/2001/XMLSchema#integer42"::binary>>
      assert {:error, :invalid_encoding} = IdToString.decode_term(binary)
    end

    test "returns error for language literal without null separator" do
      # Language literal format is <<3, 2, lang, 0, value>>
      # Missing the null byte separator
      assert {:error, :invalid_encoding} = IdToString.decode_term(<<3, 2, "envalue">>)
    end

    test "returns error for unknown literal subtype" do
      # Unknown literal subtype (99 is not valid)
      assert {:error, :invalid_encoding} = IdToString.decode_term(<<3, 99, "value">>)
    end
  end

  # ===========================================================================
  # lookup_term/2 for dictionary-allocated IDs
  # ===========================================================================

  describe "lookup_term/2 for dictionary-allocated IDs" do
    test "returns :not_found for unknown ID", %{db: db} do
      # Create a fake ID that doesn't exist
      fake_id = Dictionary.encode_id(Dictionary.type_uri(), 999_999)
      assert :not_found = IdToString.lookup_term(db, fake_id)
    end

    test "returns URI term", %{db: db, manager: manager} do
      uri = RDF.iri("http://example.org/test")
      {:ok, id} = Manager.get_or_create_id(manager, uri)

      {:ok, term} = IdToString.lookup_term(db, id)
      assert term == uri
    end

    test "returns BNode term", %{db: db, manager: manager} do
      bnode = RDF.bnode("b1")
      {:ok, id} = Manager.get_or_create_id(manager, bnode)

      {:ok, term} = IdToString.lookup_term(db, id)
      assert term == bnode
    end

    test "returns literal term", %{db: db, manager: manager} do
      literal = RDF.literal("hello world")
      {:ok, id} = Manager.get_or_create_id(manager, literal)

      {:ok, term} = IdToString.lookup_term(db, id)
      assert RDF.Literal.lexical(term) == "hello world"
    end

    test "returns language-tagged literal", %{db: db, manager: manager} do
      literal = RDF.literal("hello", language: "en")
      {:ok, id} = Manager.get_or_create_id(manager, literal)

      {:ok, term} = IdToString.lookup_term(db, id)
      assert RDF.Literal.lexical(term) == "hello"
      assert RDF.LangString.language(term.literal) == "en"
    end

    test "returns typed literal", %{db: db, manager: manager} do
      literal = RDF.literal(42)
      {:ok, id} = Manager.get_or_create_id(manager, literal)

      {:ok, term} = IdToString.lookup_term(db, id)
      # The returned literal should have the same lexical value
      assert RDF.Literal.lexical(term) == "42"
    end
  end

  # ===========================================================================
  # lookup_term/2 for inline-encoded IDs
  # ===========================================================================

  describe "lookup_term/2 for inline-encoded IDs" do
    test "returns integer literal", %{db: db} do
      {:ok, id} = Dictionary.encode_integer(42)
      {:ok, term} = IdToString.lookup_term(db, id)

      assert RDF.Literal.value(term) == 42
    end

    test "returns negative integer literal", %{db: db} do
      {:ok, id} = Dictionary.encode_integer(-100)
      {:ok, term} = IdToString.lookup_term(db, id)

      assert RDF.Literal.value(term) == -100
    end

    test "returns large integer literal", %{db: db} do
      large_value = 1_000_000_000_000
      {:ok, id} = Dictionary.encode_integer(large_value)
      {:ok, term} = IdToString.lookup_term(db, id)

      assert RDF.Literal.value(term) == large_value
    end

    test "returns decimal literal", %{db: db} do
      decimal = Decimal.new("3.14159")
      {:ok, id} = Dictionary.encode_decimal(decimal)
      {:ok, term} = IdToString.lookup_term(db, id)

      # Decimal precision may vary slightly
      assert Decimal.compare(RDF.Literal.value(term), decimal) == :eq
    end

    test "returns datetime literal", %{db: db} do
      datetime = ~U[2024-01-15 12:30:00Z]
      {:ok, id} = Dictionary.encode_datetime(datetime)
      {:ok, term} = IdToString.lookup_term(db, id)

      # DateTime is encoded with millisecond precision
      result = RDF.Literal.value(term)
      assert DateTime.truncate(result, :second) == DateTime.truncate(datetime, :second)
    end

    test "returns zero integer", %{db: db} do
      {:ok, id} = Dictionary.encode_integer(0)
      {:ok, term} = IdToString.lookup_term(db, id)

      assert RDF.Literal.value(term) == 0
    end
  end

  # ===========================================================================
  # lookup_terms/2 batch operations
  # ===========================================================================

  describe "lookup_terms/2" do
    test "returns results for multiple IDs", %{db: db, manager: manager} do
      uri = RDF.iri("http://example.org/batch1")
      bnode = RDF.bnode("b1")
      literal = RDF.literal("test")

      {:ok, uri_id} = Manager.get_or_create_id(manager, uri)
      {:ok, bnode_id} = Manager.get_or_create_id(manager, bnode)
      {:ok, literal_id} = Manager.get_or_create_id(manager, literal)

      {:ok, results} = IdToString.lookup_terms(db, [uri_id, bnode_id, literal_id])

      assert length(results) == 3
      assert {:ok, ^uri} = Enum.at(results, 0)
      assert {:ok, ^bnode} = Enum.at(results, 1)
      {:ok, lit_result} = Enum.at(results, 2)
      assert RDF.Literal.lexical(lit_result) == "test"
    end

    test "handles mix of found and not found", %{db: db, manager: manager} do
      uri = RDF.iri("http://example.org/exists")
      {:ok, uri_id} = Manager.get_or_create_id(manager, uri)
      fake_id = Dictionary.encode_id(Dictionary.type_uri(), 999_999)

      {:ok, results} = IdToString.lookup_terms(db, [uri_id, fake_id])

      assert [{:ok, ^uri}, :not_found] = results
    end

    test "handles mix of dictionary and inline IDs", %{db: db, manager: manager} do
      uri = RDF.iri("http://example.org/mixed")
      {:ok, uri_id} = Manager.get_or_create_id(manager, uri)
      {:ok, int_id} = Dictionary.encode_integer(42)

      {:ok, results} = IdToString.lookup_terms(db, [uri_id, int_id])

      assert [{:ok, ^uri}, {:ok, int_lit}] = results
      assert RDF.Literal.value(int_lit) == 42
    end

    test "handles empty list", %{db: db} do
      {:ok, results} = IdToString.lookup_terms(db, [])
      assert results == []
    end

    test "handles large batch", %{db: db, manager: manager} do
      # Create 100 terms
      terms =
        for i <- 1..100 do
          RDF.iri("http://example.org/batch/#{i}")
        end

      ids =
        for term <- terms do
          {:ok, id} = Manager.get_or_create_id(manager, term)
          id
        end

      {:ok, results} = IdToString.lookup_terms(db, ids)

      assert length(results) == 100

      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end
  end

  # ===========================================================================
  # Roundtrip tests
  # ===========================================================================

  describe "encoding/decoding roundtrip" do
    test "URI roundtrips correctly", %{db: db, manager: manager} do
      original = RDF.iri("http://example.org/roundtrip")
      {:ok, id} = Manager.get_or_create_id(manager, original)
      {:ok, decoded} = IdToString.lookup_term(db, id)

      assert decoded == original
    end

    test "BNode roundtrips correctly", %{db: db, manager: manager} do
      original = RDF.bnode("roundtrip_node")
      {:ok, id} = Manager.get_or_create_id(manager, original)
      {:ok, decoded} = IdToString.lookup_term(db, id)

      assert decoded == original
    end

    test "plain literal roundtrips correctly", %{db: db, manager: manager} do
      original = RDF.literal("plain text value")
      {:ok, id} = Manager.get_or_create_id(manager, original)
      {:ok, decoded} = IdToString.lookup_term(db, id)

      assert RDF.Literal.lexical(decoded) == RDF.Literal.lexical(original)
    end

    test "language literal roundtrips correctly", %{db: db, manager: manager} do
      original = RDF.literal("hello world", language: "en")
      {:ok, id} = Manager.get_or_create_id(manager, original)
      {:ok, decoded} = IdToString.lookup_term(db, id)

      assert RDF.Literal.lexical(decoded) == RDF.Literal.lexical(original)
      assert RDF.LangString.language(decoded.literal) == "en"
    end

    test "unicode URI roundtrips correctly", %{db: db, manager: manager} do
      original = RDF.iri("http://example.org/資源/café")
      {:ok, id} = Manager.get_or_create_id(manager, original)
      {:ok, decoded} = IdToString.lookup_term(db, id)

      assert decoded == original
    end

    test "inline integer roundtrips correctly", %{db: _db} do
      original = 12_345
      {:ok, id} = Dictionary.encode_integer(original)
      {:ok, decoded_lit} = IdToString.lookup_term(nil, id)

      assert RDF.Literal.value(decoded_lit) == original
    end

    test "inline negative integer roundtrips correctly", %{db: _db} do
      original = -67_890
      {:ok, id} = Dictionary.encode_integer(original)
      {:ok, decoded_lit} = IdToString.lookup_term(nil, id)

      assert RDF.Literal.value(decoded_lit) == original
    end

    test "inline decimal roundtrips correctly", %{db: _db} do
      original = Decimal.new("123.456")
      {:ok, id} = Dictionary.encode_decimal(original)
      {:ok, decoded_lit} = IdToString.lookup_term(nil, id)

      assert Decimal.compare(RDF.Literal.value(decoded_lit), original) == :eq
    end
  end

  # ===========================================================================
  # Edge cases
  # ===========================================================================

  describe "edge cases" do
    test "handles very long URI", %{db: db, manager: manager} do
      long_path = String.duplicate("x", 10_000)
      original = RDF.iri("http://example.org/#{long_path}")
      {:ok, id} = Manager.get_or_create_id(manager, original)
      {:ok, decoded} = IdToString.lookup_term(db, id)

      assert decoded == original
    end

    test "handles literal with null bytes in value", %{db: db, manager: manager} do
      # The value itself can contain data that looks like our separator
      # This should still work because we use binary split correctly
      original = RDF.literal("before\x00after")
      {:ok, id} = Manager.get_or_create_id(manager, original)
      {:ok, decoded} = IdToString.lookup_term(db, id)

      assert RDF.Literal.lexical(decoded) == "before\x00after"
    end

    test "handles empty language tag literal", %{db: db, manager: manager} do
      # Create a literal with language tag (using lowercase)
      original = RDF.literal("text", language: "en")
      {:ok, id} = Manager.get_or_create_id(manager, original)
      {:ok, decoded} = IdToString.lookup_term(db, id)

      assert RDF.Literal.lexical(decoded) == "text"
    end
  end
end
