defmodule TripleStore.ExporterTest do
  @moduledoc """
  Tests for Task 1.5.4: Export Functions.

  Verifies:
  - export_graph/2 exports all triples as RDF.Graph
  - export_graph/3 exports filtered triples
  - export_file/4 writes to various formats
  - export_string/3 serializes to string
  - stream_triples/2 provides lazy triple stream
  """

  use ExUnit.Case, async: false

  alias TripleStore.Adapter
  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Exporter
  alias TripleStore.Loader

  @test_db_base "/tmp/triple_store_exporter_test"

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

  # Helper to load test data
  defp load_test_triples(db, manager, triples) do
    graph = RDF.Graph.new(triples)
    {:ok, _} = Loader.load_graph(db, manager, graph)
    :ok
  end

  # ===========================================================================
  # export_graph/2 Tests
  # ===========================================================================

  describe "export_graph/2" do
    test "exports empty store", %{db: db} do
      {:ok, graph} = Exporter.export_graph(db)
      assert RDF.Graph.triple_count(graph) == 0
    end

    test "exports single triple", %{db: db, manager: manager} do
      triple = {
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("value")
      }

      :ok = load_test_triples(db, manager, [triple])

      {:ok, graph} = Exporter.export_graph(db)
      assert RDF.Graph.triple_count(graph) == 1

      [exported] = RDF.Graph.triples(graph)
      {s, p, o} = exported
      assert s == RDF.iri("http://example.org/s")
      assert p == RDF.iri("http://example.org/p")
      assert RDF.Literal.value(o) == "value"
    end

    test "exports multiple triples", %{db: db, manager: manager} do
      triples =
        for i <- 1..10 do
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p"),
            RDF.literal("value #{i}")
          }
        end

      :ok = load_test_triples(db, manager, triples)

      {:ok, graph} = Exporter.export_graph(db)
      assert RDF.Graph.triple_count(graph) == 10
    end

    test "preserves various term types", %{db: db, manager: manager} do
      triples = [
        # IRI object
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"),
         RDF.iri("http://example.org/o")},
        # Blank node subject
        {RDF.bnode("b1"), RDF.iri("http://example.org/p"), RDF.literal("value")},
        # Integer literal
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.literal(42)},
        # Language-tagged literal
        {RDF.iri("http://example.org/s3"), RDF.iri("http://example.org/p"),
         RDF.literal("hello", language: "en")}
      ]

      :ok = load_test_triples(db, manager, triples)

      {:ok, graph} = Exporter.export_graph(db)
      assert RDF.Graph.triple_count(graph) == 4
    end

    test "accepts graph options", %{db: db, manager: manager} do
      triple = {
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("value")
      }

      :ok = load_test_triples(db, manager, [triple])

      graph_name = RDF.iri("http://example.org/graph")
      {:ok, graph} = Exporter.export_graph(db, name: graph_name)

      assert RDF.Graph.name(graph) == graph_name
    end
  end

  # ===========================================================================
  # export_graph/3 with Pattern Tests
  # ===========================================================================

  describe "export_graph/3 with pattern" do
    test "filters by subject", %{db: db, manager: manager} do
      s1 = RDF.iri("http://example.org/s1")
      s2 = RDF.iri("http://example.org/s2")
      p = RDF.iri("http://example.org/p")

      triples = [
        {s1, p, RDF.literal("v1")},
        {s1, p, RDF.literal("v2")},
        {s2, p, RDF.literal("v3")}
      ]

      :ok = load_test_triples(db, manager, triples)

      # Get the subject ID
      {:ok, s1_id} = Adapter.lookup_term_id(db, s1)

      # Export only s1's triples
      pattern = {{:bound, s1_id}, :var, :var}
      {:ok, graph} = Exporter.export_graph(db, pattern, [])

      assert RDF.Graph.triple_count(graph) == 2
    end

    test "filters by predicate", %{db: db, manager: manager} do
      s = RDF.iri("http://example.org/s")
      p1 = RDF.iri("http://example.org/p1")
      p2 = RDF.iri("http://example.org/p2")

      triples = [
        {s, p1, RDF.literal("v1")},
        {s, p2, RDF.literal("v2")},
        {s, p1, RDF.literal("v3")}
      ]

      :ok = load_test_triples(db, manager, triples)

      # Get the predicate ID
      {:ok, p1_id} = Adapter.lookup_term_id(db, p1)

      # Export only p1's triples
      pattern = {:var, {:bound, p1_id}, :var}
      {:ok, graph} = Exporter.export_graph(db, pattern, [])

      assert RDF.Graph.triple_count(graph) == 2
    end

    test "filters by object", %{db: db, manager: manager} do
      s = RDF.iri("http://example.org/s")
      p = RDF.iri("http://example.org/p")
      o1 = RDF.iri("http://example.org/o1")
      o2 = RDF.iri("http://example.org/o2")

      triples = [
        {s, p, o1},
        {s, p, o2},
        {s, p, o1}
      ]

      :ok = load_test_triples(db, manager, triples)

      # Get the object ID
      {:ok, o1_id} = Adapter.lookup_term_id(db, o1)

      # Export only triples with o1
      # Note: RDF.Graph deduplicates, so we only have 2 unique triples
      pattern = {:var, :var, {:bound, o1_id}}
      {:ok, graph} = Exporter.export_graph(db, pattern, [])

      assert RDF.Graph.triple_count(graph) == 1
    end
  end

  # ===========================================================================
  # export_file/4 Tests
  # ===========================================================================

  describe "export_file/4" do
    setup %{path: test_path} do
      files_dir = Path.join(test_path, "files")
      File.mkdir_p!(files_dir)
      {:ok, files_dir: files_dir}
    end

    test "exports to Turtle file", %{db: db, manager: manager, files_dir: files_dir} do
      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), RDF.literal("v1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), RDF.literal("v2")}
      ]

      :ok = load_test_triples(db, manager, triples)

      output_path = Path.join(files_dir, "output.ttl")
      {:ok, count} = Exporter.export_file(db, output_path, :turtle)

      assert count == 2
      assert File.exists?(output_path)

      content = File.read!(output_path)
      assert String.contains?(content, "http://example.org/s1")
      assert String.contains?(content, "http://example.org/s2")
    end

    test "exports to N-Triples file", %{db: db, manager: manager, files_dir: files_dir} do
      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]

      :ok = load_test_triples(db, manager, triples)

      output_path = Path.join(files_dir, "output.nt")
      {:ok, count} = Exporter.export_file(db, output_path, :ntriples)

      assert count == 1
      assert File.exists?(output_path)

      content = File.read!(output_path)
      assert String.contains?(content, "<http://example.org/s>")
      assert String.contains?(content, "<http://example.org/p>")
    end

    test "exports to N-Quads file", %{db: db, manager: manager, files_dir: files_dir} do
      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]

      :ok = load_test_triples(db, manager, triples)

      output_path = Path.join(files_dir, "output.nq")
      {:ok, count} = Exporter.export_file(db, output_path, :nquads)

      assert count == 1
      assert File.exists?(output_path)
    end

    test "exports filtered triples", %{db: db, manager: manager, files_dir: files_dir} do
      s1 = RDF.iri("http://example.org/s1")
      s2 = RDF.iri("http://example.org/s2")
      p = RDF.iri("http://example.org/p")

      triples = [
        {s1, p, RDF.literal("v1")},
        {s2, p, RDF.literal("v2")}
      ]

      :ok = load_test_triples(db, manager, triples)

      {:ok, s1_id} = Adapter.lookup_term_id(db, s1)

      output_path = Path.join(files_dir, "filtered.ttl")
      pattern = {{:bound, s1_id}, :var, :var}

      {:ok, count} = Exporter.export_file(db, output_path, :turtle, pattern: pattern)

      assert count == 1

      content = File.read!(output_path)
      assert String.contains?(content, "http://example.org/s1")
      refute String.contains?(content, "http://example.org/s2")
    end
  end

  # ===========================================================================
  # export_string/3 Tests
  # ===========================================================================

  describe "export_string/3" do
    test "serializes to Turtle", %{db: db, manager: manager} do
      triple = {
        RDF.iri("http://example.org/subject"),
        RDF.iri("http://example.org/predicate"),
        RDF.literal("object")
      }

      :ok = load_test_triples(db, manager, [triple])

      {:ok, content} = Exporter.export_string(db, :turtle)

      assert String.contains?(content, "http://example.org/subject")
      assert String.contains?(content, "http://example.org/predicate")
    end

    test "serializes to N-Triples", %{db: db, manager: manager} do
      triple = {
        RDF.iri("http://example.org/s"),
        RDF.iri("http://example.org/p"),
        RDF.literal("value")
      }

      :ok = load_test_triples(db, manager, [triple])

      {:ok, content} = Exporter.export_string(db, :ntriples)

      # N-Triples format should have angle brackets around IRIs
      assert String.contains?(content, "<http://example.org/s>")
      assert String.contains?(content, "<http://example.org/p>")
    end

    test "returns error for unsupported format", %{db: db} do
      result = Exporter.export_string(db, :unsupported_format)
      assert {:error, {:unsupported_format, :unsupported_format}} = result
    end
  end

  # ===========================================================================
  # stream_triples/2 Tests
  # ===========================================================================

  describe "stream_triples/2" do
    test "streams empty store", %{db: db} do
      {:ok, stream} = Exporter.stream_triples(db)
      assert Enum.to_list(stream) == []
    end

    test "streams all triples", %{db: db, manager: manager} do
      triples =
        for i <- 1..5 do
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p"),
            RDF.literal("value #{i}")
          }
        end

      :ok = load_test_triples(db, manager, triples)

      {:ok, stream} = Exporter.stream_triples(db)
      result = Enum.to_list(stream)

      assert length(result) == 5
    end

    test "streams are lazy", %{db: db, manager: manager} do
      triples =
        for i <- 1..100 do
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p"),
            RDF.literal("value #{i}")
          }
        end

      :ok = load_test_triples(db, manager, triples)

      {:ok, stream} = Exporter.stream_triples(db)

      # Take only first 10 - should not process all 100
      result = Enum.take(stream, 10)
      assert length(result) == 10
    end

    test "streams with pattern filter", %{db: db, manager: manager} do
      s1 = RDF.iri("http://example.org/s1")
      s2 = RDF.iri("http://example.org/s2")
      p = RDF.iri("http://example.org/p")

      triples = [
        {s1, p, RDF.literal("v1")},
        {s1, p, RDF.literal("v2")},
        {s2, p, RDF.literal("v3")}
      ]

      :ok = load_test_triples(db, manager, triples)

      {:ok, s1_id} = Adapter.lookup_term_id(db, s1)
      pattern = {{:bound, s1_id}, :var, :var}

      {:ok, stream} = Exporter.stream_triples(db, pattern: pattern)
      result = Enum.to_list(stream)

      assert length(result) == 2
    end
  end

  # ===========================================================================
  # stream_internal_triples/2 Tests
  # ===========================================================================

  describe "stream_internal_triples/2" do
    test "streams internal IDs", %{db: db, manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), RDF.literal("value")}
      ]

      :ok = load_test_triples(db, manager, triples)

      {:ok, stream} = Exporter.stream_internal_triples(db)
      [{s_id, p_id, o_id}] = Enum.to_list(stream)

      assert is_integer(s_id)
      assert is_integer(p_id)
      assert is_integer(o_id)
    end
  end

  # ===========================================================================
  # count/2 Tests
  # ===========================================================================

  describe "count/2" do
    test "counts empty store", %{db: db} do
      {:ok, count} = Exporter.count(db)
      assert count == 0
    end

    test "counts all triples", %{db: db, manager: manager} do
      triples =
        for i <- 1..15 do
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p"),
            RDF.literal("value #{i}")
          }
        end

      :ok = load_test_triples(db, manager, triples)

      {:ok, count} = Exporter.count(db)
      assert count == 15
    end

    test "counts with pattern", %{db: db, manager: manager} do
      s1 = RDF.iri("http://example.org/s1")
      s2 = RDF.iri("http://example.org/s2")
      p = RDF.iri("http://example.org/p")

      triples = [
        {s1, p, RDF.literal("v1")},
        {s1, p, RDF.literal("v2")},
        {s2, p, RDF.literal("v3")}
      ]

      :ok = load_test_triples(db, manager, triples)

      {:ok, s1_id} = Adapter.lookup_term_id(db, s1)
      pattern = {{:bound, s1_id}, :var, :var}

      {:ok, count} = Exporter.count(db, pattern)
      assert count == 2
    end
  end

  # ===========================================================================
  # format_extension/1 Tests
  # ===========================================================================

  describe "format_extension/1" do
    test "returns correct extensions" do
      assert Exporter.format_extension(:turtle) == ".ttl"
      assert Exporter.format_extension(:ntriples) == ".nt"
      assert Exporter.format_extension(:nquads) == ".nq"
      assert Exporter.format_extension(:trig) == ".trig"
      assert Exporter.format_extension(:rdfxml) == ".rdf"
      assert Exporter.format_extension(:jsonld) == ".jsonld"
    end

    test "returns default for unknown format" do
      assert Exporter.format_extension(:unknown) == ".rdf"
    end
  end

  # ===========================================================================
  # Roundtrip Tests
  # ===========================================================================

  describe "roundtrip" do
    test "load and export preserves triples", %{db: db, manager: manager} do
      original_triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p1"),
         RDF.literal("value1")},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p2"), RDF.literal(42)},
        {RDF.bnode("b1"), RDF.iri("http://example.org/p3"), RDF.literal("hello", language: "en")}
      ]

      original_graph = RDF.Graph.new(original_triples)
      {:ok, _} = Loader.load_graph(db, manager, original_graph)

      {:ok, exported_graph} = Exporter.export_graph(db)

      assert RDF.Graph.triple_count(exported_graph) == 3

      # Check each triple exists
      exported_triples = RDF.Graph.triples(exported_graph)

      Enum.each(original_triples, fn {s, p, o} ->
        matching =
          Enum.find(exported_triples, fn {es, ep, eo} ->
            s == es and p == ep and RDF.Literal.value(o) == RDF.Literal.value(eo)
          end)

        assert matching != nil, "Triple not found: #{inspect({s, p, o})}"
      end)
    end
  end
end
