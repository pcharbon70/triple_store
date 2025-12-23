defmodule TripleStore.Adapter.TermConversionTest do
  @moduledoc """
  Tests for Task 1.5.1: Term Conversion.

  Verifies that:
  - RDF.IRI conversion to/from term IDs works correctly
  - RDF.BlankNode conversion to/from term IDs works correctly
  - RDF.Literal conversion handles both dictionary and inline encoding
  - Batch conversions work correctly
  - Type validation in reverse lookups is enforced
  """

  use TripleStore.PooledDbCase

  alias TripleStore.Adapter
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.Manager

  setup %{db: db} do
    assert NIF.is_open(db)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager) do
        Manager.stop(manager)
      end
    end)

    {:ok, manager: manager}
  end

  # ===========================================================================
  # IRI Conversion Tests
  # ===========================================================================

  describe "from_rdf_iri/2" do
    test "converts IRI to term ID", %{manager: manager} do
      iri = RDF.iri("http://example.org/subject")

      {:ok, id} = Adapter.from_rdf_iri(manager, iri)

      assert is_integer(id)
      assert id > 0
    end

    test "returns same ID for same IRI", %{manager: manager} do
      iri = RDF.iri("http://example.org/subject")

      {:ok, id1} = Adapter.from_rdf_iri(manager, iri)
      {:ok, id2} = Adapter.from_rdf_iri(manager, iri)

      assert id1 == id2
    end

    test "returns different IDs for different IRIs", %{manager: manager} do
      iri1 = RDF.iri("http://example.org/s1")
      iri2 = RDF.iri("http://example.org/s2")

      {:ok, id1} = Adapter.from_rdf_iri(manager, iri1)
      {:ok, id2} = Adapter.from_rdf_iri(manager, iri2)

      assert id1 != id2
    end
  end

  describe "to_rdf_iri/2" do
    test "converts term ID back to IRI", %{db: db, manager: manager} do
      iri = RDF.iri("http://example.org/subject")
      {:ok, id} = Adapter.from_rdf_iri(manager, iri)

      {:ok, result} = Adapter.to_rdf_iri(db, id)

      assert result == iri
    end

    test "returns :not_found for unknown ID", %{db: db} do
      # Create a URI-type ID that doesn't exist
      unknown_id = Dictionary.encode_id(Dictionary.type_uri(), 999_999)

      result = Adapter.to_rdf_iri(db, unknown_id)

      assert result == :not_found
    end

    test "returns type_mismatch for non-URI ID", %{db: db, manager: manager} do
      bnode = RDF.bnode("b1")
      {:ok, bnode_id} = Adapter.from_rdf_bnode(manager, bnode)

      result = Adapter.to_rdf_iri(db, bnode_id)

      assert result == {:error, :type_mismatch}
    end
  end

  # ===========================================================================
  # BlankNode Conversion Tests
  # ===========================================================================

  describe "from_rdf_bnode/2" do
    test "converts blank node to term ID", %{manager: manager} do
      bnode = RDF.bnode("b1")

      {:ok, id} = Adapter.from_rdf_bnode(manager, bnode)

      assert is_integer(id)
      assert id > 0
    end

    test "returns same ID for same blank node", %{manager: manager} do
      bnode = RDF.bnode("b1")

      {:ok, id1} = Adapter.from_rdf_bnode(manager, bnode)
      {:ok, id2} = Adapter.from_rdf_bnode(manager, bnode)

      assert id1 == id2
    end

    test "returns different IDs for different blank nodes", %{manager: manager} do
      bnode1 = RDF.bnode("b1")
      bnode2 = RDF.bnode("b2")

      {:ok, id1} = Adapter.from_rdf_bnode(manager, bnode1)
      {:ok, id2} = Adapter.from_rdf_bnode(manager, bnode2)

      assert id1 != id2
    end
  end

  describe "to_rdf_bnode/2" do
    test "converts term ID back to blank node", %{db: db, manager: manager} do
      bnode = RDF.bnode("test_bnode")
      {:ok, id} = Adapter.from_rdf_bnode(manager, bnode)

      {:ok, result} = Adapter.to_rdf_bnode(db, id)

      assert result == bnode
    end

    test "returns :not_found for unknown ID", %{db: db} do
      unknown_id = Dictionary.encode_id(Dictionary.type_bnode(), 999_999)

      result = Adapter.to_rdf_bnode(db, unknown_id)

      assert result == :not_found
    end

    test "returns type_mismatch for non-bnode ID", %{db: db, manager: manager} do
      iri = RDF.iri("http://example.org")
      {:ok, iri_id} = Adapter.from_rdf_iri(manager, iri)

      result = Adapter.to_rdf_bnode(db, iri_id)

      assert result == {:error, :type_mismatch}
    end
  end

  # ===========================================================================
  # Literal Conversion Tests - Dictionary Allocated
  # ===========================================================================

  describe "from_rdf_literal/2 - dictionary allocated" do
    test "converts plain string literal", %{manager: manager} do
      literal = RDF.literal("hello world")

      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      assert is_integer(id)
      assert not Dictionary.inline_encoded?(id)
    end

    test "converts typed literal", %{manager: manager} do
      literal = RDF.literal("test", datatype: RDF.iri("http://example.org/custom"))

      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      assert is_integer(id)
      assert not Dictionary.inline_encoded?(id)
    end

    test "converts language-tagged literal", %{manager: manager} do
      literal = RDF.literal("bonjour", language: "fr")

      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      assert is_integer(id)
      assert not Dictionary.inline_encoded?(id)
    end

    test "returns same ID for same literal", %{manager: manager} do
      literal = RDF.literal("hello")

      {:ok, id1} = Adapter.from_rdf_literal(manager, literal)
      {:ok, id2} = Adapter.from_rdf_literal(manager, literal)

      assert id1 == id2
    end
  end

  # ===========================================================================
  # Literal Conversion Tests - Inline Encoded
  # ===========================================================================

  describe "from_rdf_literal/2 - inline encoded" do
    test "encodes integer literal inline", %{manager: manager} do
      literal = RDF.literal(42)

      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      assert is_integer(id)
      assert Dictionary.inline_encoded?(id)
    end

    test "encodes negative integer inline", %{manager: manager} do
      literal = RDF.literal(-100)

      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      assert Dictionary.inline_encoded?(id)
    end

    test "encodes zero inline", %{manager: manager} do
      literal = RDF.literal(0)

      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      assert Dictionary.inline_encoded?(id)
    end

    test "encodes decimal literal inline", %{manager: manager} do
      literal = RDF.literal(Decimal.new("3.14159"))

      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      assert Dictionary.inline_encoded?(id)
    end

    test "encodes datetime literal inline", %{manager: manager} do
      {:ok, dt, _} = DateTime.from_iso8601("2024-01-15T12:30:00Z")
      literal = RDF.literal(dt)

      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      assert Dictionary.inline_encoded?(id)
    end
  end

  describe "to_rdf_literal/2 - dictionary allocated" do
    test "converts dictionary literal ID back to literal", %{db: db, manager: manager} do
      literal = RDF.literal("hello world")
      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      {:ok, result} = Adapter.to_rdf_literal(db, id)

      assert result == literal
    end

    test "converts language-tagged literal roundtrip", %{db: db, manager: manager} do
      literal = RDF.literal("bonjour", language: "fr")
      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      {:ok, result} = Adapter.to_rdf_literal(db, id)

      assert result == literal
    end
  end

  describe "to_rdf_literal/2 - inline encoded" do
    test "decodes inline integer", %{db: db, manager: manager} do
      literal = RDF.literal(42)
      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      {:ok, result} = Adapter.to_rdf_literal(db, id)

      assert RDF.Literal.value(result) == 42
    end

    test "decodes inline negative integer", %{db: db, manager: manager} do
      literal = RDF.literal(-999)
      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      {:ok, result} = Adapter.to_rdf_literal(db, id)

      assert RDF.Literal.value(result) == -999
    end

    test "decodes inline decimal", %{db: db, manager: manager} do
      original = Decimal.new("2.71828")
      literal = RDF.literal(original)
      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      {:ok, result} = Adapter.to_rdf_literal(db, id)

      # Decimal comparison - values should be equal
      assert Decimal.equal?(RDF.Literal.value(result), original)
    end

    test "decodes inline datetime", %{db: db, manager: manager} do
      {:ok, dt, _} = DateTime.from_iso8601("2024-06-15T10:00:00Z")
      literal = RDF.literal(dt)
      {:ok, id} = Adapter.from_rdf_literal(manager, literal)

      {:ok, result} = Adapter.to_rdf_literal(db, id)

      # DateTime comparison - compare as Unix timestamps (millisecond precision)
      result_dt = RDF.Literal.value(result)
      assert DateTime.to_unix(result_dt, :millisecond) == DateTime.to_unix(dt, :millisecond)
    end

    test "returns type_mismatch for non-literal ID", %{db: db, manager: manager} do
      iri = RDF.iri("http://example.org")
      {:ok, iri_id} = Adapter.from_rdf_iri(manager, iri)

      result = Adapter.to_rdf_literal(db, iri_id)

      assert result == {:error, :type_mismatch}
    end
  end

  # ===========================================================================
  # Generic Term Conversion Tests
  # ===========================================================================

  describe "term_to_id/2" do
    test "dispatches IRI correctly", %{manager: manager} do
      iri = RDF.iri("http://example.org")
      {:ok, id} = Adapter.term_to_id(manager, iri)

      {type, _} = Dictionary.decode_id(id)
      assert type == :uri
    end

    test "dispatches blank node correctly", %{manager: manager} do
      bnode = RDF.bnode("b1")
      {:ok, id} = Adapter.term_to_id(manager, bnode)

      {type, _} = Dictionary.decode_id(id)
      assert type == :bnode
    end

    test "dispatches literal correctly", %{manager: manager} do
      literal = RDF.literal("test")
      {:ok, id} = Adapter.term_to_id(manager, literal)

      {type, _} = Dictionary.decode_id(id)
      assert type == :literal
    end

    test "returns error for unsupported term", %{manager: manager} do
      result = Adapter.term_to_id(manager, "not an RDF term")
      assert result == {:error, :unsupported_term}
    end
  end

  describe "id_to_term/2" do
    test "returns IRI for URI ID", %{db: db, manager: manager} do
      iri = RDF.iri("http://example.org")
      {:ok, id} = Adapter.term_to_id(manager, iri)

      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == iri
    end

    test "returns blank node for bnode ID", %{db: db, manager: manager} do
      bnode = RDF.bnode("test")
      {:ok, id} = Adapter.term_to_id(manager, bnode)

      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == bnode
    end

    test "returns literal for literal ID", %{db: db, manager: manager} do
      literal = RDF.literal("test")
      {:ok, id} = Adapter.term_to_id(manager, literal)

      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == literal
    end

    test "returns literal for inline integer ID", %{db: db, manager: manager} do
      literal = RDF.literal(123)
      {:ok, id} = Adapter.term_to_id(manager, literal)

      {:ok, result} = Adapter.id_to_term(db, id)

      assert RDF.Literal.value(result) == 123
    end
  end

  # ===========================================================================
  # Batch Conversion Tests
  # ===========================================================================

  describe "terms_to_ids/2" do
    test "converts empty list", %{manager: manager} do
      {:ok, ids} = Adapter.terms_to_ids(manager, [])
      assert ids == []
    end

    test "converts list of terms", %{manager: manager} do
      terms = [
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("o")
      ]

      {:ok, ids} = Adapter.terms_to_ids(manager, terms)

      assert length(ids) == 3
      assert Enum.all?(ids, &is_integer/1)
    end

    test "maintains order", %{db: db, manager: manager} do
      terms = [
        RDF.iri("http://example.org/a"),
        RDF.iri("http://example.org/b"),
        RDF.iri("http://example.org/c")
      ]

      {:ok, ids} = Adapter.terms_to_ids(manager, terms)
      {:ok, results} = Adapter.ids_to_terms(db, ids)

      # Extract successful results
      decoded_terms = Enum.map(results, fn {:ok, term} -> term end)
      assert decoded_terms == terms
    end
  end

  describe "ids_to_terms/2" do
    test "converts empty list", %{db: db} do
      {:ok, terms} = Adapter.ids_to_terms(db, [])
      assert terms == []
    end

    test "converts list of IDs", %{db: db, manager: manager} do
      terms = [
        RDF.iri("http://example.org/s"),
        RDF.bnode("b1"),
        RDF.literal("test")
      ]

      {:ok, ids} = Adapter.terms_to_ids(manager, terms)
      {:ok, results} = Adapter.ids_to_terms(db, ids)

      assert length(results) == 3

      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end

    test "includes :not_found for missing IDs", %{db: db, manager: manager} do
      # Create one known term
      {:ok, known_id} = Adapter.term_to_id(manager, RDF.iri("http://example.org"))

      # Unknown ID
      unknown_id = Dictionary.encode_id(Dictionary.type_uri(), 999_999)

      {:ok, results} = Adapter.ids_to_terms(db, [known_id, unknown_id])

      assert length(results) == 2
      assert match?({:ok, %RDF.IRI{}}, Enum.at(results, 0))
      assert Enum.at(results, 1) == :not_found
    end
  end

  # ===========================================================================
  # Roundtrip Tests
  # ===========================================================================

  describe "roundtrip conversions" do
    test "IRI roundtrip preserves value", %{db: db, manager: manager} do
      original = RDF.iri("http://example.org/test/path?query=1#fragment")
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == original
    end

    test "blank node roundtrip preserves value", %{db: db, manager: manager} do
      original = RDF.bnode("unique_blank_node_123")
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == original
    end

    test "plain literal roundtrip preserves value", %{db: db, manager: manager} do
      original = RDF.literal("Hello, World!")
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == original
    end

    test "typed literal roundtrip preserves value", %{db: db, manager: manager} do
      original =
        RDF.literal("2024-01-15", datatype: RDF.iri("http://www.w3.org/2001/XMLSchema#date"))

      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == original
    end

    test "language-tagged literal roundtrip preserves value", %{db: db, manager: manager} do
      original = RDF.literal("Guten Tag", language: "de")
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == original
    end

    test "inline integer roundtrip preserves value", %{db: db, manager: manager} do
      original = RDF.literal(42)
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert RDF.Literal.value(result) == RDF.Literal.value(original)
    end

    test "large integer roundtrip", %{db: db, manager: manager} do
      large_int = 123_456_789_012_345
      original = RDF.literal(large_int)
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert RDF.Literal.value(result) == large_int
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles empty string literal", %{db: db, manager: manager} do
      original = RDF.literal("")
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert RDF.Literal.value(result) == ""
    end

    test "handles unicode in IRI", %{db: db, manager: manager} do
      original = RDF.iri("http://example.org/unicode/\u00E9\u00E8\u00EA")
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == original
    end

    test "handles unicode in literal", %{db: db, manager: manager} do
      original = RDF.literal("\u4E2D\u6587\u5B57\u7B26")
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == original
    end

    test "handles very long IRI", %{db: db, manager: manager} do
      long_path = String.duplicate("a", 1000)
      original = RDF.iri("http://example.org/#{long_path}")
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == original
    end

    test "handles blank node with special characters", %{db: db, manager: manager} do
      original = RDF.bnode("node-123_abc")
      {:ok, id} = Adapter.term_to_id(manager, original)
      {:ok, result} = Adapter.id_to_term(db, id)

      assert result == original
    end
  end
end
