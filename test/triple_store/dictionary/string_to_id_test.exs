defmodule TripleStore.Dictionary.StringToIdTest do
  @moduledoc """
  Tests for String-to-ID mapping (Task 1.3.3).

  Covers:
  - Term encoding for URIs, BNodes, and Literals
  - lookup_id/2 for existing and missing terms
  - get_or_create_id/1 with atomic create-if-missing via Manager
  - Batch operations
  - Concurrent access
  """
  use TripleStore.PooledDbCase

  alias RDF.NS.XSD

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Dictionary.StringToId

  setup %{db: db} do
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
    end)

    {:ok, manager: manager}
  end

  # ===========================================================================
  # encode_term/1 tests
  # ===========================================================================

  describe "encode_term/1 for URIs" do
    test "encodes simple URI" do
      uri = RDF.iri("http://example.org/resource")
      {:ok, key} = StringToId.encode_term(uri)
      assert <<1, "http://example.org/resource"::binary>> = key
    end

    test "encodes URI with angle brackets stripped" do
      # The IRI struct doesn't include angle brackets, but test just in case
      uri = %RDF.IRI{value: "http://example.org/resource"}
      {:ok, key} = StringToId.encode_term(uri)
      assert <<1, "http://example.org/resource"::binary>> = key
    end

    test "encodes URI with special characters" do
      uri = RDF.iri("http://example.org/path?query=value&foo=bar")
      {:ok, key} = StringToId.encode_term(uri)
      assert <<1, "http://example.org/path?query=value&foo=bar"::binary>> = key
    end

    test "encodes URI with unicode characters" do
      uri = RDF.iri("http://example.org/café")
      {:ok, key} = StringToId.encode_term(uri)
      assert <<1, uri_part::binary>> = key
      assert uri_part == "http://example.org/café"
    end

    test "rejects URI with null byte" do
      uri = %RDF.IRI{value: "http://example.org/\x00bad"}
      assert {:error, :null_byte_in_uri} = StringToId.encode_term(uri)
    end
  end

  describe "encode_term/1 for blank nodes" do
    test "encodes blank node" do
      bnode = RDF.bnode("b1")
      {:ok, key} = StringToId.encode_term(bnode)
      assert <<2, "b1"::binary>> = key
    end

    test "encodes blank node with generated id" do
      bnode = RDF.bnode()
      {:ok, key} = StringToId.encode_term(bnode)
      assert <<2, _id::binary>> = key
    end
  end

  describe "encode_term/1 for literals" do
    test "encodes string literal" do
      # RDF.literal/1 with string creates xsd:string typed literal
      literal = RDF.literal("hello world")
      {:ok, key} = StringToId.encode_term(literal)
      # All typed literals use subtype 1 with datatype URI
      assert <<3, 1, rest::binary>> = key
      assert String.contains?(rest, "XMLSchema#string")
      assert String.contains?(rest, "hello world")
    end

    test "encodes typed literal with explicit xsd:string" do
      literal = RDF.literal("hello", datatype: XSD.string())
      {:ok, key} = StringToId.encode_term(literal)
      # Typed literals use subtype 1 with datatype URI
      assert <<3, 1, rest::binary>> = key
      assert String.contains?(rest, "XMLSchema#string")
    end

    test "encodes typed literal with xsd:integer" do
      literal = RDF.literal(42)
      {:ok, key} = StringToId.encode_term(literal)
      assert <<3, 1, rest::binary>> = key
      # Contains datatype and null separator and value
      assert String.contains?(rest, "integer")
    end

    test "encodes language-tagged literal" do
      literal = RDF.literal("hello", language: "en")
      {:ok, key} = StringToId.encode_term(literal)
      # Language literals use subtype 2
      assert <<3, 2, "en", 0, "hello"::binary>> = key
    end

    test "normalizes language tag to lowercase" do
      literal1 = RDF.literal("hello", language: "EN")
      literal2 = RDF.literal("hello", language: "en")
      {:ok, key1} = StringToId.encode_term(literal1)
      {:ok, key2} = StringToId.encode_term(literal2)
      assert key1 == key2
    end

    test "encodes literal with unicode" do
      literal = RDF.literal("café")
      {:ok, key} = StringToId.encode_term(literal)
      # xsd:string typed literal
      assert <<3, 1, rest::binary>> = key
      assert String.contains?(rest, "café")
    end

    test "encodes boolean literal" do
      literal = RDF.literal(true)
      {:ok, key} = StringToId.encode_term(literal)
      assert <<3, 1, rest::binary>> = key
      assert String.contains?(rest, "boolean")
    end

    test "encodes decimal literal" do
      literal = RDF.literal(Decimal.new("3.14"))
      {:ok, key} = StringToId.encode_term(literal)
      assert <<3, 1, rest::binary>> = key
      assert String.contains?(rest, "decimal")
    end

    test "encodes date literal" do
      literal = RDF.literal(~D[2024-01-15])
      {:ok, key} = StringToId.encode_term(literal)
      assert <<3, 1, rest::binary>> = key
      assert String.contains?(rest, "date")
    end
  end

  describe "encode_term/1 edge cases" do
    test "returns error for unsupported term" do
      assert {:error, :unsupported_term} = StringToId.encode_term("not an RDF term")
      assert {:error, :unsupported_term} = StringToId.encode_term(42)
      assert {:error, :unsupported_term} = StringToId.encode_term(%{})
    end

    test "encodes empty string literal" do
      literal = RDF.literal("")
      {:ok, key} = StringToId.encode_term(literal)
      # Empty string is still xsd:string typed
      assert <<3, 1, rest::binary>> = key
      assert String.contains?(rest, "XMLSchema#string")
    end
  end

  # ===========================================================================
  # lookup_id/2 tests
  # ===========================================================================

  describe "lookup_id/2" do
    test "returns :not_found for unknown term", %{db: db} do
      uri = RDF.iri("http://unknown.org/resource")
      assert :not_found = StringToId.lookup_id(db, uri)
    end

    test "returns ID after term is created", %{db: db, manager: manager} do
      uri = RDF.iri("http://example.org/test")

      # First lookup - not found
      assert :not_found = StringToId.lookup_id(db, uri)

      # Create the term
      {:ok, id1} = Manager.get_or_create_id(manager, uri)

      # Now lookup should succeed
      {:ok, id2} = StringToId.lookup_id(db, uri)
      assert id1 == id2
    end

    test "returns correct ID for URI", %{db: db, manager: manager} do
      uri = RDF.iri("http://example.org/uri")
      {:ok, id} = Manager.get_or_create_id(manager, uri)
      {:ok, found_id} = StringToId.lookup_id(db, uri)
      assert id == found_id
      assert Dictionary.term_type(id) == :uri
    end

    test "returns correct ID for BNode", %{db: db, manager: manager} do
      bnode = RDF.bnode("b1")
      {:ok, id} = Manager.get_or_create_id(manager, bnode)
      {:ok, found_id} = StringToId.lookup_id(db, bnode)
      assert id == found_id
      assert Dictionary.term_type(id) == :bnode
    end

    test "returns correct ID for literal", %{db: db, manager: manager} do
      literal = RDF.literal("test value")
      {:ok, id} = Manager.get_or_create_id(manager, literal)
      {:ok, found_id} = StringToId.lookup_id(db, literal)
      assert id == found_id
      assert Dictionary.term_type(id) == :literal
    end
  end

  # ===========================================================================
  # get_or_create_id/3 tests
  # ===========================================================================

  describe "get_or_create_id/3" do
    test "creates new ID for unknown URI", %{db: _db, manager: manager} do
      uri = RDF.iri("http://example.org/new")
      {:ok, id} = Manager.get_or_create_id(manager, uri)
      assert is_integer(id)
      assert Dictionary.term_type(id) == :uri
    end

    test "creates new ID for unknown BNode", %{db: _db, manager: manager} do
      bnode = RDF.bnode("new_bnode")
      {:ok, id} = Manager.get_or_create_id(manager, bnode)
      assert is_integer(id)
      assert Dictionary.term_type(id) == :bnode
    end

    test "creates new ID for unknown literal", %{db: _db, manager: manager} do
      literal = RDF.literal("new value")
      {:ok, id} = Manager.get_or_create_id(manager, literal)
      assert is_integer(id)
      assert Dictionary.term_type(id) == :literal
    end

    test "returns same ID for same term", %{db: _db, manager: manager} do
      uri = RDF.iri("http://example.org/same")
      {:ok, id1} = Manager.get_or_create_id(manager, uri)
      {:ok, id2} = Manager.get_or_create_id(manager, uri)
      assert id1 == id2
    end

    test "returns different IDs for different terms", %{db: _db, manager: manager} do
      uri1 = RDF.iri("http://example.org/one")
      uri2 = RDF.iri("http://example.org/two")
      {:ok, id1} = Manager.get_or_create_id(manager, uri1)
      {:ok, id2} = Manager.get_or_create_id(manager, uri2)
      assert id1 != id2
    end

    test "different term types get different IDs", %{db: _db, manager: manager} do
      # Same string value but different term types
      uri = RDF.iri("http://example.org/test")
      literal = RDF.literal("http://example.org/test")

      {:ok, uri_id} = Manager.get_or_create_id(manager, uri)
      {:ok, lit_id} = Manager.get_or_create_id(manager, literal)

      assert uri_id != lit_id
      assert Dictionary.term_type(uri_id) == :uri
      assert Dictionary.term_type(lit_id) == :literal
    end

    test "stores reverse mapping in id2str", %{db: db, manager: manager} do
      uri = RDF.iri("http://example.org/reverse")
      {:ok, id} = Manager.get_or_create_id(manager, uri)

      # Check id2str has the reverse mapping
      id_binary = <<id::64-big>>
      {:ok, stored_key} = NIF.get(db, :id2str, id_binary)

      # The stored key should be the encoded term
      {:ok, expected_key} = StringToId.encode_term(uri)
      assert stored_key == expected_key
    end
  end

  # ===========================================================================
  # Batch operations tests
  # ===========================================================================

  describe "lookup_ids/2" do
    test "returns results for multiple terms", %{db: db, manager: manager} do
      uri1 = RDF.iri("http://example.org/batch1")
      uri2 = RDF.iri("http://example.org/batch2")
      uri3 = RDF.iri("http://example.org/unknown")

      {:ok, id1} = Manager.get_or_create_id(manager, uri1)
      {:ok, id2} = Manager.get_or_create_id(manager, uri2)

      {:ok, results} = StringToId.lookup_ids(db, [uri1, uri2, uri3])

      assert [{:ok, ^id1}, {:ok, ^id2}, :not_found] = results
    end

    test "handles empty list", %{db: db} do
      {:ok, results} = StringToId.lookup_ids(db, [])
      assert results == []
    end
  end

  describe "get_or_create_ids/3" do
    test "creates IDs for multiple terms", %{db: _db, manager: manager} do
      terms = [
        RDF.iri("http://example.org/multi1"),
        RDF.iri("http://example.org/multi2"),
        RDF.bnode("mb1"),
        RDF.literal("multi value")
      ]

      {:ok, ids} = Manager.get_or_create_ids(manager, terms)

      assert length(ids) == 4
      assert Enum.all?(ids, &is_integer/1)
      assert length(Enum.uniq(ids)) == 4
    end

    test "returns same IDs on repeated call", %{db: _db, manager: manager} do
      terms = [
        RDF.iri("http://example.org/repeat1"),
        RDF.iri("http://example.org/repeat2")
      ]

      {:ok, ids1} = Manager.get_or_create_ids(manager, terms)
      {:ok, ids2} = Manager.get_or_create_ids(manager, terms)

      assert ids1 == ids2
    end

    test "handles empty list", %{db: _db, manager: manager} do
      {:ok, ids} = Manager.get_or_create_ids(manager, [])
      assert ids == []
    end
  end

  # ===========================================================================
  # Concurrent access tests
  # ===========================================================================

  describe "concurrent access" do
    test "handles concurrent get_or_create for same term", %{db: _db, manager: manager} do
      uri = RDF.iri("http://example.org/concurrent")

      # Spawn many tasks trying to create the same term
      tasks =
        for _ <- 1..50 do
          Task.async(fn ->
            Manager.get_or_create_id(manager, uri)
          end)
        end

      results = Task.await_many(tasks)

      # All should succeed
      ids = for {:ok, id} <- results, do: id
      assert length(ids) == 50

      # All should return the same ID
      assert length(Enum.uniq(ids)) == 1
    end

    test "handles concurrent get_or_create for different terms", %{db: _db, manager: manager} do
      # Spawn tasks for different terms
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            uri = RDF.iri("http://example.org/concurrent/#{i}")
            Manager.get_or_create_id(manager, uri)
          end)
        end

      results = Task.await_many(tasks)

      # All should succeed
      ids = for {:ok, id} <- results, do: id
      assert length(ids) == 100

      # All IDs should be unique
      assert length(Enum.uniq(ids)) == 100
    end

    test "mixed read and write operations", %{db: db, manager: manager} do
      # Pre-create some terms
      pre_created =
        for i <- 1..10 do
          uri = RDF.iri("http://example.org/pre/#{i}")
          {:ok, id} = Manager.get_or_create_id(manager, uri)
          {uri, id}
        end

      # Concurrent mix of lookups and creates
      tasks =
        for i <- 1..50 do
          Task.async(fn ->
            if rem(i, 2) == 0 do
              # Lookup existing
              {uri, expected_id} = Enum.random(pre_created)
              {:ok, found_id} = StringToId.lookup_id(db, uri)
              assert found_id == expected_id
              :lookup_ok
            else
              # Create new
              uri = RDF.iri("http://example.org/new/#{i}")
              {:ok, _id} = Manager.get_or_create_id(manager, uri)
              :create_ok
            end
          end)
        end

      results = Task.await_many(tasks)
      assert Enum.all?(results, &(&1 in [:lookup_ok, :create_ok]))
    end
  end

  # ===========================================================================
  # Edge cases and error handling
  # ===========================================================================

  describe "edge cases" do
    test "handles very long URI", %{db: db, manager: manager} do
      # Create a URI near the max size (16KB)
      long_path = String.duplicate("x", 15_000)
      uri = RDF.iri("http://example.org/#{long_path}")

      {:ok, id} = Manager.get_or_create_id(manager, uri)
      {:ok, found_id} = StringToId.lookup_id(db, uri)
      assert id == found_id
    end

    test "rejects term exceeding max size", %{db: _db} do
      # Create a URI exceeding max size (16KB)
      long_path = String.duplicate("x", 20_000)
      uri = %RDF.IRI{value: "http://example.org/#{long_path}"}

      assert {:error, :term_too_large} = StringToId.encode_term(uri)
    end

    test "handles unicode normalization", %{db: _db, manager: manager} do
      # café in NFC vs NFD (composed vs decomposed)
      # The dictionary should normalize both to the same form
      uri1 = RDF.iri("http://example.org/caf\u00E9")
      uri2 = RDF.iri("http://example.org/cafe\u0301")

      {:ok, id1} = Manager.get_or_create_id(manager, uri1)
      {:ok, id2} = Manager.get_or_create_id(manager, uri2)

      # Both should map to the same ID after normalization
      assert id1 == id2
    end

    test "language tags with different cases map to same ID", %{db: _db, manager: manager} do
      lit1 = RDF.literal("hello", language: "EN")
      lit2 = RDF.literal("hello", language: "en")

      {:ok, id1} = Manager.get_or_create_id(manager, lit1)
      {:ok, id2} = Manager.get_or_create_id(manager, lit2)

      assert id1 == id2
    end
  end

  describe "Manager initialization" do
    test "fails gracefully when database reference is invalid" do
      # Create a fake/invalid reference
      invalid_db = make_ref()

      # The Manager traps the exit and returns an error tuple
      # Use Process.flag to trap exits so the test doesn't crash
      Process.flag(:trap_exit, true)

      # Start Manager with invalid db - should fail during init
      result = Manager.start_link(db: invalid_db)

      # The Manager should fail to start because SequenceCounter can't load
      # It either returns an error tuple or crashes (which we've trapped)
      case result do
        {:error, _reason} ->
          # Expected graceful failure
          :ok

        {:ok, pid} ->
          # If it started, it should crash shortly - receive the exit signal
          receive do
            {:EXIT, ^pid, _reason} -> :ok
          after
            1000 -> flunk("Manager did not crash as expected")
          end
      end
    end
  end
end
