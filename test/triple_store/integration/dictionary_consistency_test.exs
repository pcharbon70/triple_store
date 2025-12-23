defmodule TripleStore.Integration.DictionaryConsistencyTest do
  @moduledoc """
  Integration tests for Task 1.7.2: Dictionary Consistency Testing.

  Tests dictionary encoding maintains consistency across operations,
  including:
  - Same term always gets same ID
  - ID-to-term and term-to-ID are inverse operations
  - Inline-encoded values compare correctly
  - Dictionary handles Unicode terms correctly
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Dictionary.StringToId
  alias TripleStore.Dictionary.IdToString

  @test_db_base "/tmp/triple_store_dict_consistency_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      NIF.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # 1.7.2.1: Same Term Always Gets Same ID
  # ===========================================================================

  describe "same term always gets same ID" do
    test "URI returns same ID on repeated lookups", %{manager: manager} do
      uri = RDF.iri("http://example.org/resource/123")

      {:ok, id1} = Manager.get_or_create_id(manager, uri)
      {:ok, id2} = Manager.get_or_create_id(manager, uri)
      {:ok, id3} = Manager.get_or_create_id(manager, uri)

      assert id1 == id2
      assert id2 == id3
    end

    test "blank node returns same ID on repeated lookups", %{manager: manager} do
      bnode = RDF.bnode("node123")

      {:ok, id1} = Manager.get_or_create_id(manager, bnode)
      {:ok, id2} = Manager.get_or_create_id(manager, bnode)

      assert id1 == id2
    end

    test "plain literal returns same ID on repeated lookups", %{manager: manager} do
      literal = RDF.literal("hello world")

      {:ok, id1} = Manager.get_or_create_id(manager, literal)
      {:ok, id2} = Manager.get_or_create_id(manager, literal)

      assert id1 == id2
    end

    test "typed literal returns same ID on repeated lookups", %{manager: manager} do
      literal = RDF.XSD.Date.new!("2023-12-22")

      {:ok, id1} = Manager.get_or_create_id(manager, literal)
      {:ok, id2} = Manager.get_or_create_id(manager, literal)

      assert id1 == id2
    end

    test "language-tagged literal returns same ID", %{manager: manager} do
      literal = RDF.literal("hello", language: "en")

      {:ok, id1} = Manager.get_or_create_id(manager, literal)
      {:ok, id2} = Manager.get_or_create_id(manager, literal)

      assert id1 == id2
    end

    test "different URIs get different IDs", %{manager: manager} do
      uri1 = RDF.iri("http://example.org/resource/1")
      uri2 = RDF.iri("http://example.org/resource/2")

      {:ok, id1} = Manager.get_or_create_id(manager, uri1)
      {:ok, id2} = Manager.get_or_create_id(manager, uri2)

      assert id1 != id2
    end

    test "batch lookups are consistent", %{manager: manager} do
      terms = [
        RDF.iri("http://example.org/a"),
        RDF.bnode("b1"),
        RDF.literal("test")
      ]

      {:ok, ids1} = Manager.get_or_create_ids(manager, terms)
      {:ok, ids2} = Manager.get_or_create_ids(manager, terms)

      assert ids1 == ids2
    end
  end

  # ===========================================================================
  # 1.7.2.2: ID-to-Term and Term-to-ID Are Inverse Operations
  # ===========================================================================

  describe "ID-to-term and term-to-ID are inverse operations" do
    test "URI roundtrip preserves value", %{db: db, manager: manager} do
      uri = RDF.iri("http://example.org/resource/test")

      {:ok, id} = Manager.get_or_create_id(manager, uri)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == uri
    end

    test "blank node roundtrip preserves value", %{db: db, manager: manager} do
      bnode = RDF.bnode("test_node")

      {:ok, id} = Manager.get_or_create_id(manager, bnode)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == bnode
    end

    test "plain literal roundtrip preserves value", %{db: db, manager: manager} do
      literal = RDF.literal("Hello, World!")

      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == literal
    end

    test "typed literal roundtrip preserves datatype", %{db: db, manager: manager} do
      literal = RDF.XSD.Boolean.new!(true)

      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == literal
      assert RDF.Literal.datatype_id(recovered) == RDF.iri("http://www.w3.org/2001/XMLSchema#boolean")
    end

    test "language-tagged literal roundtrip preserves language", %{db: db, manager: manager} do
      literal = RDF.literal("bonjour", language: "fr")

      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == literal
      assert RDF.Literal.language(recovered) == "fr"
    end

    test "batch roundtrip preserves all values", %{db: db, manager: manager} do
      terms = [
        RDF.iri("http://example.org/subject"),
        RDF.bnode("b1"),
        RDF.literal("value"),
        RDF.literal("hello", language: "en"),
        RDF.XSD.Integer.new!(2023)
      ]

      {:ok, ids} = Manager.get_or_create_ids(manager, terms)
      {:ok, results} = IdToString.lookup_terms(db, ids)

      recovered = Enum.map(results, fn {:ok, term} -> term end)
      assert recovered == terms
    end

    test "forward and reverse lookups are consistent", %{db: db, manager: manager} do
      uri = RDF.iri("http://example.org/forward-reverse")

      {:ok, id} = Manager.get_or_create_id(manager, uri)
      {:ok, recovered_term} = IdToString.lookup_term(db, id)
      {:ok, recovered_id} = StringToId.lookup_id(db, recovered_term)

      assert recovered_term == uri
      assert recovered_id == id
    end
  end

  # ===========================================================================
  # 1.7.2.3: Inline-Encoded Values Compare Correctly
  # ===========================================================================

  describe "inline-encoded values compare correctly" do
    test "positive inline integers maintain order" do
      # Positive values maintain order within their type tag
      values = [0, 1, 100, 1000, 100_000]

      encoded =
        Enum.map(values, fn v ->
          {:ok, id} = Dictionary.encode_integer(v)
          id
        end)

      # Encoded IDs should maintain the same order for positive integers
      assert encoded == Enum.sort(encoded)
    end

    test "negative inline integers maintain order" do
      # Negative values maintain order within their type tag
      values = [-100_000, -1000, -100, -1]

      encoded =
        Enum.map(values, fn v ->
          {:ok, id} = Dictionary.encode_integer(v)
          id
        end)

      # Encoded IDs should maintain the same order for negative integers
      assert encoded == Enum.sort(encoded)
    end

    test "positive inline decimals maintain order" do
      values = [
        Decimal.new("0.0"),
        Decimal.new("0.5"),
        Decimal.new("1.5"),
        Decimal.new("100.0")
      ]

      encoded =
        Enum.map(values, fn v ->
          {:ok, id} = Dictionary.encode_decimal(v)
          id
        end)

      # Encoded IDs should maintain the same order for positive decimals
      assert encoded == Enum.sort(encoded)
    end

    test "inline datetimes maintain order" do
      datetimes = [
        ~U[2020-01-01 00:00:00Z],
        ~U[2021-06-15 12:30:00Z],
        ~U[2022-12-31 23:59:59Z]
      ]

      encoded =
        Enum.map(datetimes, fn dt ->
          {:ok, id} = Dictionary.encode_datetime(dt)
          id
        end)

      # Encoded IDs should maintain the same order
      assert encoded == Enum.sort(encoded)
    end

    test "inline integer roundtrip preserves value" do
      for value <- [-1_000_000, -1, 0, 1, 1_000_000] do
        {:ok, id} = Dictionary.encode_integer(value)
        {:ok, decoded} = Dictionary.decode_inline(id)

        assert decoded == value
      end
    end

    test "inline decimal roundtrip preserves value" do
      values = [
        Decimal.new("-1.5"),
        Decimal.new("0"),
        Decimal.new("1.5"),
        Decimal.new("100.25")
      ]

      for value <- values do
        {:ok, id} = Dictionary.encode_decimal(value)
        {:ok, decoded} = Dictionary.decode_inline(id)

        # Decoded value should equal original
        assert Decimal.eq?(decoded, value)
      end
    end

    test "inline datetime roundtrip preserves value" do
      datetimes = [
        ~U[2020-01-01 00:00:00Z],
        ~U[2023-06-15 12:30:45Z],
        ~U[2099-12-31 23:59:59Z]
      ]

      for dt <- datetimes do
        {:ok, id} = Dictionary.encode_datetime(dt)
        {:ok, decoded} = Dictionary.decode_inline(id)

        assert DateTime.compare(decoded, DateTime.truncate(dt, :second)) == :eq
      end
    end

    test "large integers fallback to dictionary encoding", %{db: db, manager: manager} do
      # Value larger than inline range - use XSD.Integer
      large_literal = RDF.XSD.Integer.new!(10_000_000_000_000_000_000)

      {:ok, id} = Manager.get_or_create_id(manager, large_literal)

      # Should be stored in dictionary
      {:ok, recovered} = IdToString.lookup_term(db, id)
      assert RDF.Literal.value(recovered) == 10_000_000_000_000_000_000
    end
  end

  # ===========================================================================
  # 1.7.2.4: Dictionary Handles Unicode Terms Correctly
  # ===========================================================================

  describe "dictionary handles Unicode terms correctly" do
    test "URI with Unicode path", %{db: db, manager: manager} do
      uri = RDF.iri("http://example.org/资源/测试")

      {:ok, id} = Manager.get_or_create_id(manager, uri)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == uri
    end

    test "literal with emoji", %{db: db, manager: manager} do
      literal = RDF.literal("Hello 👋 World 🌍")

      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == literal
    end

    test "literal with Chinese characters", %{db: db, manager: manager} do
      literal = RDF.literal("你好世界")

      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == literal
    end

    test "literal with Arabic script", %{db: db, manager: manager} do
      literal = RDF.literal("مرحبا بالعالم")

      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == literal
    end

    test "literal with Cyrillic script", %{db: db, manager: manager} do
      literal = RDF.literal("Привет мир")

      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == literal
    end

    test "literal with Japanese (mixed scripts)", %{db: db, manager: manager} do
      literal = RDF.literal("こんにちは世界 Hello")

      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == literal
    end

    test "language-tagged literal with script", %{db: db, manager: manager} do
      literal = RDF.literal("שלום עולם", language: "he")

      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == literal
      assert RDF.Literal.language(recovered) == "he"
    end

    test "blank node with Unicode identifier", %{db: db, manager: manager} do
      bnode = RDF.bnode("узел_测试")

      {:ok, id} = Manager.get_or_create_id(manager, bnode)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == bnode
    end

    test "special Unicode characters preserved", %{db: db, manager: manager} do
      # Test various special characters
      special_chars = [
        "line\nbreak",
        "tab\there",
        "quote\"inside",
        "backslash\\here",
        "null\x00byte"
      ]

      for value <- special_chars do
        literal = RDF.literal(value)
        {:ok, id} = Manager.get_or_create_id(manager, literal)
        {:ok, recovered} = IdToString.lookup_term(db, id)

        assert RDF.Literal.value(recovered) == value
      end
    end

    test "very long Unicode string", %{db: db, manager: manager} do
      long_value = String.duplicate("你好", 1000)
      literal = RDF.literal(long_value)

      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, recovered} = IdToString.lookup_term(db, id)

      assert recovered == literal
      assert String.length(RDF.Literal.value(recovered)) == 2000
    end
  end
end
