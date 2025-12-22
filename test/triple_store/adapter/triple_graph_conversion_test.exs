defmodule TripleStore.Adapter.TripleGraphConversionTest do
  @moduledoc """
  Tests for triple and graph conversion in the RDF.ex Adapter.
  """

  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager

  @test_db_base "/tmp/triple_store_adapter_triple_graph_test"

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
  # from_rdf_triple/2 Tests
  # ===========================================================================

  describe "from_rdf_triple/2" do
    test "converts triple with IRI subject, predicate, and literal object", %{manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")

      triple = {subject, predicate, object}
      {:ok, {s_id, p_id, o_id}} = Adapter.from_rdf_triple(manager, triple)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
    end

    test "converts triple with blank node subject", %{manager: manager} do
      subject = RDF.bnode("b1")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")

      triple = {subject, predicate, object}
      {:ok, {s_id, p_id, o_id}} = Adapter.from_rdf_triple(manager, triple)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
    end

    test "converts triple with IRI object", %{manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.iri("http://example.org/object")

      triple = {subject, predicate, object}
      {:ok, {s_id, p_id, o_id}} = Adapter.from_rdf_triple(manager, triple)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
    end

    test "converts triple with blank node object", %{manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.bnode("b2")

      triple = {subject, predicate, object}
      {:ok, {s_id, p_id, o_id}} = Adapter.from_rdf_triple(manager, triple)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
    end

    test "converts triple with inline-encoded integer object", %{manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal(42)

      triple = {subject, predicate, object}
      {:ok, {s_id, p_id, o_id}} = Adapter.from_rdf_triple(manager, triple)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
    end

    test "returns same IDs for same triple converted twice", %{manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")

      triple = {subject, predicate, object}
      {:ok, {s1, p1, o1}} = Adapter.from_rdf_triple(manager, triple)
      {:ok, {s2, p2, o2}} = Adapter.from_rdf_triple(manager, triple)

      assert s1 == s2
      assert p1 == p2
      assert o1 == o2
    end
  end

  # ===========================================================================
  # to_rdf_triple/2 Tests
  # ===========================================================================

  describe "to_rdf_triple/2" do
    test "converts internal triple back to RDF triple", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal("value")

      triple = {subject, predicate, object}
      {:ok, internal_triple} = Adapter.from_rdf_triple(manager, triple)

      {:ok, {s, p, o}} = Adapter.to_rdf_triple(db, internal_triple)

      assert s == subject
      assert p == predicate
      assert RDF.Literal.value(o) == "value"
    end

    test "converts triple with blank nodes roundtrip", %{db: db, manager: manager} do
      subject = RDF.bnode("b1")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.bnode("b2")

      triple = {subject, predicate, object}
      {:ok, internal_triple} = Adapter.from_rdf_triple(manager, triple)

      {:ok, {s, p, o}} = Adapter.to_rdf_triple(db, internal_triple)

      assert s == subject
      assert p == predicate
      assert o == object
    end

    test "converts triple with inline integer roundtrip", %{db: db, manager: manager} do
      subject = RDF.iri("http://example.org/subject")
      predicate = RDF.iri("http://example.org/predicate")
      object = RDF.literal(12345)

      triple = {subject, predicate, object}
      {:ok, internal_triple} = Adapter.from_rdf_triple(manager, triple)

      {:ok, {s, p, o}} = Adapter.to_rdf_triple(db, internal_triple)

      assert s == subject
      assert p == predicate
      assert RDF.Literal.value(o) == 12345
    end

    test "returns :not_found for unknown ID", %{db: db} do
      # Use an ID that doesn't exist
      result = Adapter.to_rdf_triple(db, {999_999_999, 999_999_998, 999_999_997})
      assert result == :not_found
    end
  end

  # ===========================================================================
  # from_rdf_triples/2 Tests
  # ===========================================================================

  describe "from_rdf_triples/2" do
    test "converts empty list", %{manager: manager} do
      {:ok, result} = Adapter.from_rdf_triples(manager, [])
      assert result == []
    end

    test "converts single triple", %{manager: manager} do
      triple = {
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("o")
      }

      {:ok, [internal_triple]} = Adapter.from_rdf_triples(manager, [triple])

      assert is_tuple(internal_triple)
      assert tuple_size(internal_triple) == 3
    end

    test "converts multiple triples", %{manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p1"), RDF.literal("o1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p2"), RDF.literal("o2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p3"), RDF.literal("o3")}
      ]

      {:ok, internal_triples} = Adapter.from_rdf_triples(manager, triples)

      assert length(internal_triples) == 3
      Enum.each(internal_triples, fn {s, p, o} ->
        assert is_integer(s)
        assert is_integer(p)
        assert is_integer(o)
      end)
    end

    test "maintains order", %{manager: manager} do
      triples = [
        {RDF.iri("http://example.org/a"), RDF.iri("http://example.org/p"), RDF.literal("1")},
        {RDF.iri("http://example.org/b"), RDF.iri("http://example.org/p"), RDF.literal("2")},
        {RDF.iri("http://example.org/c"), RDF.iri("http://example.org/p"), RDF.literal("3")}
      ]

      {:ok, internal_triples} = Adapter.from_rdf_triples(manager, triples)

      # The subject IDs should be in increasing order (allocated sequentially)
      [{s1, _, _}, {s2, _, _}, {s3, _, _}] = internal_triples
      assert s1 < s2
      assert s2 < s3
    end
  end

  # ===========================================================================
  # to_rdf_triples/2 Tests
  # ===========================================================================

  describe "to_rdf_triples/2" do
    test "converts empty list", %{db: db} do
      {:ok, result} = Adapter.to_rdf_triples(db, [])
      assert result == []
    end

    test "converts single internal triple", %{db: db, manager: manager} do
      triple = {
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("o")
      }

      {:ok, [internal_triple]} = Adapter.from_rdf_triples(manager, [triple])
      {:ok, [rdf_triple]} = Adapter.to_rdf_triples(db, [internal_triple])

      {s, p, o} = rdf_triple
      assert s == RDF.iri("http://example.org/s")
      assert p == RDF.iri("http://example.org/p")
      assert RDF.Literal.value(o) == "o"
    end

    test "converts multiple internal triples", %{db: db, manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p1"), RDF.literal("o1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p2"), RDF.literal("o2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p3"), RDF.literal("o3")}
      ]

      {:ok, internal_triples} = Adapter.from_rdf_triples(manager, triples)
      {:ok, rdf_triples} = Adapter.to_rdf_triples(db, internal_triples)

      assert length(rdf_triples) == 3
      Enum.each(rdf_triples, fn {s, p, o} ->
        assert %RDF.IRI{} = s
        assert %RDF.IRI{} = p
        assert %RDF.Literal{} = o
      end)
    end

    test "returns :not_found for missing IDs", %{db: db, manager: manager} do
      # Create a valid triple first
      triple = {
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("o")
      }

      {:ok, [valid_internal]} = Adapter.from_rdf_triples(manager, [triple])

      # Mix with an invalid triple
      invalid_internal = {999_999_999, 999_999_998, 999_999_997}

      {:ok, results} = Adapter.to_rdf_triples(db, [valid_internal, invalid_internal])

      assert length(results) == 2
      assert is_tuple(Enum.at(results, 0))
      assert Enum.at(results, 1) == :not_found
    end
  end

  # ===========================================================================
  # from_rdf_graph/2 Tests
  # ===========================================================================

  describe "from_rdf_graph/2" do
    test "converts empty graph", %{manager: manager} do
      graph = RDF.Graph.new()
      {:ok, result} = Adapter.from_rdf_graph(manager, graph)
      assert result == []
    end

    test "converts graph with single triple", %{manager: manager} do
      triple = {
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("o")
      }
      graph = RDF.Graph.new([triple])

      {:ok, [internal_triple]} = Adapter.from_rdf_graph(manager, graph)

      assert is_tuple(internal_triple)
      assert tuple_size(internal_triple) == 3
    end

    test "converts graph with multiple triples", %{manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p1"), RDF.literal("o1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p2"), RDF.literal("o2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p3"), RDF.literal("o3")}
      ]
      graph = RDF.Graph.new(triples)

      {:ok, internal_triples} = Adapter.from_rdf_graph(manager, graph)

      assert length(internal_triples) == 3
    end

    test "handles graph with various term types", %{manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), RDF.literal("string")},
        {RDF.bnode("b1"), RDF.iri("http://example.org/p"), RDF.literal(42)},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.bnode("b2")}
      ]
      graph = RDF.Graph.new(triples)

      {:ok, internal_triples} = Adapter.from_rdf_graph(manager, graph)

      assert length(internal_triples) == 3
    end
  end

  # ===========================================================================
  # to_rdf_graph/3 Tests
  # ===========================================================================

  describe "to_rdf_graph/3" do
    test "converts empty list to empty graph", %{db: db} do
      {:ok, graph} = Adapter.to_rdf_graph(db, [])
      assert RDF.Graph.triple_count(graph) == 0
    end

    test "converts internal triples to graph", %{db: db, manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p1"), RDF.literal("o1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p2"), RDF.literal("o2")}
      ]

      {:ok, internal_triples} = Adapter.from_rdf_triples(manager, triples)
      {:ok, graph} = Adapter.to_rdf_graph(db, internal_triples)

      assert RDF.Graph.triple_count(graph) == 2
    end

    test "accepts graph options", %{db: db, manager: manager} do
      triple = {
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("o")
      }

      {:ok, [internal_triple]} = Adapter.from_rdf_triples(manager, [triple])
      graph_name = RDF.iri("http://example.org/graph")

      {:ok, graph} = Adapter.to_rdf_graph(db, [internal_triple], name: graph_name)

      assert RDF.Graph.name(graph) == graph_name
    end

    test "skips triples with missing IDs", %{db: db, manager: manager} do
      # Create a valid triple
      triple = {
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("o")
      }

      {:ok, [valid_internal]} = Adapter.from_rdf_triples(manager, [triple])

      # Create an invalid triple
      invalid_internal = {999_999_999, 999_999_998, 999_999_997}

      {:ok, graph} = Adapter.to_rdf_graph(db, [valid_internal, invalid_internal])

      # Only the valid triple should be in the graph
      assert RDF.Graph.triple_count(graph) == 1
    end
  end

  # ===========================================================================
  # stream_from_rdf_graph/2 Tests
  # ===========================================================================

  describe "stream_from_rdf_graph/2" do
    test "returns empty stream for empty graph", %{manager: manager} do
      graph = RDF.Graph.new()
      stream = Adapter.stream_from_rdf_graph(manager, graph)

      assert Enum.to_list(stream) == []
    end

    test "streams triples lazily", %{manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), RDF.literal("o1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.literal("o2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"), RDF.literal("o3")}
      ]
      graph = RDF.Graph.new(triples)

      stream = Adapter.stream_from_rdf_graph(manager, graph)

      # Take first 2 items
      results = Enum.take(stream, 2)

      assert length(results) == 2
      Enum.each(results, fn result ->
        assert {:ok, {s_id, p_id, o_id}} = result
        assert is_integer(s_id)
        assert is_integer(p_id)
        assert is_integer(o_id)
      end)
    end

    test "processes all triples when collected", %{manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), RDF.literal("o1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.literal("o2")},
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"), RDF.literal("o3")}
      ]
      graph = RDF.Graph.new(triples)

      stream = Adapter.stream_from_rdf_graph(manager, graph)
      results = Enum.to_list(stream)

      assert length(results) == 3
      assert Enum.all?(results, fn {:ok, _} -> true; _ -> false end)
    end
  end

  # ===========================================================================
  # Roundtrip Tests
  # ===========================================================================

  describe "roundtrip conversions" do
    test "triple roundtrip preserves values", %{db: db, manager: manager} do
      original = {
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("hello world", language: "en")
      }

      {:ok, internal} = Adapter.from_rdf_triple(manager, original)
      {:ok, {s, p, o}} = Adapter.to_rdf_triple(db, internal)

      {orig_s, orig_p, orig_o} = original
      assert s == orig_s
      assert p == orig_p
      assert RDF.Literal.value(o) == RDF.Literal.value(orig_o)
      assert RDF.Literal.language(o) == RDF.Literal.language(orig_o)
    end

    test "graph roundtrip preserves structure", %{db: db, manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p1"), RDF.literal("o1")},
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p2"), RDF.literal("o2")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p1"), RDF.iri("http://example.org/o3")}
      ]
      original_graph = RDF.Graph.new(triples)

      {:ok, internal_triples} = Adapter.from_rdf_graph(manager, original_graph)
      {:ok, result_graph} = Adapter.to_rdf_graph(db, internal_triples)

      # Compare triple counts
      assert RDF.Graph.triple_count(result_graph) == RDF.Graph.triple_count(original_graph)

      # Verify all original triples are in the result
      original_triples = RDF.Graph.triples(original_graph)
      result_triples = RDF.Graph.triples(result_graph)

      Enum.each(original_triples, fn orig_triple ->
        assert Enum.member?(result_triples, orig_triple)
      end)
    end

    test "batch triple conversion roundtrip", %{db: db, manager: manager} do
      triples = for i <- 1..10 do
        {
          RDF.iri("http://example.org/s#{i}"),
          RDF.iri("http://example.org/p#{i}"),
          RDF.literal("o#{i}")
        }
      end

      {:ok, internal_triples} = Adapter.from_rdf_triples(manager, triples)
      {:ok, result_triples} = Adapter.to_rdf_triples(db, internal_triples)

      assert length(result_triples) == 10

      Enum.zip(triples, result_triples)
      |> Enum.each(fn {{orig_s, orig_p, orig_o}, {res_s, res_p, res_o}} ->
        assert orig_s == res_s
        assert orig_p == res_p
        assert RDF.Literal.value(orig_o) == RDF.Literal.value(res_o)
      end)
    end
  end

  # ===========================================================================
  # Edge Cases
  # ===========================================================================

  describe "edge cases" do
    test "handles unicode in triple values", %{db: db, manager: manager} do
      triple = {
        RDF.iri("http://example.org/日本語"),
        RDF.iri("http://example.org/predicate"),
        RDF.literal("Привет мир 🌍")
      }

      {:ok, internal} = Adapter.from_rdf_triple(manager, triple)
      {:ok, {s, p, o}} = Adapter.to_rdf_triple(db, internal)

      {orig_s, orig_p, orig_o} = triple
      assert s == orig_s
      assert p == orig_p
      assert RDF.Literal.value(o) == RDF.Literal.value(orig_o)
    end

    test "handles large graphs", %{db: db, manager: manager} do
      # Create 100 triples
      triples = for i <- 1..100 do
        {
          RDF.iri("http://example.org/subject/#{i}"),
          RDF.iri("http://example.org/predicate/#{rem(i, 5)}"),
          RDF.literal("value-#{i}")
        }
      end

      graph = RDF.Graph.new(triples)

      {:ok, internal_triples} = Adapter.from_rdf_graph(manager, graph)
      {:ok, result_graph} = Adapter.to_rdf_graph(db, internal_triples)

      assert RDF.Graph.triple_count(result_graph) == 100
    end

    test "handles mixed inline and dictionary-encoded literals", %{db: db, manager: manager} do
      triples = [
        # Dictionary-encoded string
        {RDF.iri("http://ex.org/s1"), RDF.iri("http://ex.org/p"), RDF.literal("string")},
        # Inline-encoded integer
        {RDF.iri("http://ex.org/s2"), RDF.iri("http://ex.org/p"), RDF.literal(42)},
        # Inline-encoded decimal
        {RDF.iri("http://ex.org/s3"), RDF.iri("http://ex.org/p"), RDF.literal(Decimal.new("3.14"))},
        # Language-tagged (dictionary)
        {RDF.iri("http://ex.org/s4"), RDF.iri("http://ex.org/p"), RDF.literal("hello", language: "en")}
      ]

      {:ok, internal_triples} = Adapter.from_rdf_triples(manager, triples)
      {:ok, result_triples} = Adapter.to_rdf_triples(db, internal_triples)

      assert length(result_triples) == 4

      # Verify specific values
      {_, _, o1} = Enum.at(result_triples, 0)
      assert RDF.Literal.value(o1) == "string"

      {_, _, o2} = Enum.at(result_triples, 1)
      assert RDF.Literal.value(o2) == 42

      {_, _, o3} = Enum.at(result_triples, 2)
      assert Decimal.equal?(RDF.Literal.value(o3), Decimal.new("3.14"))

      {_, _, o4} = Enum.at(result_triples, 3)
      assert RDF.Literal.value(o4) == "hello"
      assert RDF.Literal.language(o4) == "en"
    end
  end
end
