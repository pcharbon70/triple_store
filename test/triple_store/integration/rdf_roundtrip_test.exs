defmodule TripleStore.Integration.RDFRoundtripTest do
  @moduledoc """
  Integration tests for Task 1.7.5: RDF.ex Roundtrip Testing.

  Tests complete roundtrip from RDF.ex through storage and back,
  including:
  - Load RDF.Graph, export, compare equality
  - Load Turtle file, export to N-Triples, verify content
  - Complex literals (language tags, datatypes) roundtrip
  - Blank node identity preservation within graph
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.Exporter

  @test_db_base "/tmp/triple_store_rdf_roundtrip_test"

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
  # 1.7.5.1: Load RDF.Graph, Export, Compare Equality
  # ===========================================================================

  describe "load RDF.Graph, export, compare equality" do
    test "simple graph roundtrip preserves triples", %{db: db, manager: manager} do
      # Create original graph
      original =
        RDF.graph([
          {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p1"),
           RDF.iri("http://example.org/o1")},
          {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p2"),
           RDF.literal("hello")},
          {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p1"),
           RDF.literal(42)}
        ])

      # Load into store
      {:ok, count} = Loader.load_graph(db, manager, original)
      assert count == 3

      # Export back (Exporter only needs db)
      {:ok, exported} = Exporter.export_graph(db)

      # Compare - graphs should be equal
      assert RDF.Graph.triple_count(exported) == RDF.Graph.triple_count(original)

      # Every triple in original should be in exported
      for triple <- RDF.Graph.triples(original) do
        assert RDF.Graph.include?(exported, triple),
               "Missing triple: #{inspect(triple)}"
      end
    end

    test "graph with many triples roundtrips correctly", %{db: db, manager: manager} do
      # Build larger graph
      triples =
        for i <- 1..100 do
          {
            RDF.iri("http://example.org/s#{i}"),
            RDF.iri("http://example.org/p#{rem(i, 5)}"),
            RDF.literal("value #{i}")
          }
        end

      original = RDF.graph(triples)

      {:ok, count} = Loader.load_graph(db, manager, original)
      assert count == 100

      {:ok, exported} = Exporter.export_graph(db)

      assert RDF.Graph.triple_count(exported) == 100

      # Spot check
      assert RDF.Graph.include?(
               exported,
               {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p1"),
                RDF.literal("value 1")}
             )
    end

    test "empty graph roundtrip", %{db: db, manager: manager} do
      original = RDF.graph()

      {:ok, count} = Loader.load_graph(db, manager, original)
      assert count == 0

      {:ok, exported} = Exporter.export_graph(db)
      assert RDF.Graph.triple_count(exported) == 0
    end
  end

  # ===========================================================================
  # 1.7.5.2: File Format Roundtrip
  # ===========================================================================

  describe "file format roundtrip" do
    test "Turtle to storage and back", %{db: db, manager: manager, path: test_path} do
      # Create Turtle content
      turtle = """
      @prefix ex: <http://example.org/> .

      ex:subject1 ex:predicate1 ex:object1 ;
                  ex:predicate2 "hello" .

      ex:subject2 ex:predicate1 "world"@en ;
                  ex:predicate3 42 .
      """

      # Write to temp file
      turtle_path = "#{test_path}_input.ttl"
      File.write!(turtle_path, turtle)

      # Load from file
      {:ok, count} = Loader.load_file(db, manager, turtle_path)
      assert count == 4

      # Export to N-Triples (Exporter only needs db)
      ntriples_path = "#{test_path}_output.nt"
      {:ok, ^count} = Exporter.export_file(db, ntriples_path, :ntriples)

      # Read exported file
      assert File.exists?(ntriples_path)
      content = File.read!(ntriples_path)

      # Verify content contains expected patterns
      assert content =~ "http://example.org/subject1"
      assert content =~ "http://example.org/predicate1"
      assert content =~ "hello"
      assert content =~ "world"

      # Cleanup
      File.rm(turtle_path)
      File.rm(ntriples_path)
    end

    test "N-Triples roundtrip", %{db: db, manager: manager, path: test_path} do
      ntriples = """
      <http://example.org/s1> <http://example.org/p1> <http://example.org/o1> .
      <http://example.org/s1> <http://example.org/p2> "literal value" .
      <http://example.org/s2> <http://example.org/p1> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
      """

      input_path = "#{test_path}_input.nt"
      File.write!(input_path, ntriples)

      {:ok, count} = Loader.load_file(db, manager, input_path)
      assert count == 3

      output_path = "#{test_path}_output.nt"
      {:ok, ^count} = Exporter.export_file(db, output_path, :ntriples)

      # Both files should parse to equivalent graphs
      {:ok, original} = RDF.NTriples.read_file(input_path)
      {:ok, exported} = RDF.NTriples.read_file(output_path)

      assert RDF.Graph.triple_count(original) == RDF.Graph.triple_count(exported)

      File.rm(input_path)
      File.rm(output_path)
    end
  end

  # ===========================================================================
  # 1.7.5.3: Complex Literals Roundtrip
  # ===========================================================================

  describe "complex literals roundtrip" do
    test "language-tagged literals preserve language", %{db: db, manager: manager} do
      original =
        RDF.graph([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/label"),
           RDF.literal("Hello", language: "en")},
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/label"),
           RDF.literal("Bonjour", language: "fr")},
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/label"),
           RDF.literal("Hola", language: "es")}
        ])

      {:ok, _} = Loader.load_graph(db, manager, original)
      {:ok, exported} = Exporter.export_graph(db)

      # Find each language-tagged literal
      labels =
        exported
        |> RDF.Graph.triples()
        |> Enum.map(fn {_s, _p, o} -> o end)
        |> Enum.filter(&RDF.Literal.has_language?/1)

      assert length(labels) == 3

      languages = Enum.map(labels, &RDF.Literal.language/1) |> Enum.sort()
      assert languages == ["en", "es", "fr"]
    end

    test "typed literals preserve datatype", %{db: db, manager: manager} do
      original =
        RDF.graph([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/int"),
           RDF.XSD.Integer.new!(42)},
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/dec"),
           RDF.XSD.Decimal.new!(Decimal.new("3.14"))},
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/bool"),
           RDF.XSD.Boolean.new!(true)},
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/date"),
           RDF.XSD.Date.new!("2023-12-22")}
        ])

      {:ok, _} = Loader.load_graph(db, manager, original)
      {:ok, exported} = Exporter.export_graph(db)

      # Verify each datatype is preserved
      for triple <- RDF.Graph.triples(original) do
        assert RDF.Graph.include?(exported, triple),
               "Missing triple with datatype: #{inspect(triple)}"
      end
    end

    test "string literals with special characters", %{db: db, manager: manager} do
      special_strings = [
        "line\nbreak",
        "tab\there",
        "quote\"inside",
        "backslash\\here",
        "unicode: 你好 🌍 مرحبا"
      ]

      triples =
        for {str, i} <- Enum.with_index(special_strings) do
          {
            RDF.iri("http://example.org/s"),
            RDF.iri("http://example.org/p#{i}"),
            RDF.literal(str)
          }
        end

      original = RDF.graph(triples)

      {:ok, _} = Loader.load_graph(db, manager, original)
      {:ok, exported} = Exporter.export_graph(db)

      for triple <- RDF.Graph.triples(original) do
        {_s, _p, original_literal} = triple

        matching =
          exported
          |> RDF.Graph.triples()
          |> Enum.find(fn {s, p, _o} ->
            s == elem(triple, 0) and p == elem(triple, 1)
          end)

        assert matching != nil, "Missing triple for #{inspect(original_literal)}"
        {_s, _p, exported_literal} = matching
        assert RDF.Literal.value(original_literal) == RDF.Literal.value(exported_literal)
      end
    end

    test "very long literals", %{db: db, manager: manager} do
      long_value = String.duplicate("a", 10_000)

      original =
        RDF.graph([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/long"),
           RDF.literal(long_value)}
        ])

      {:ok, _} = Loader.load_graph(db, manager, original)
      {:ok, exported} = Exporter.export_graph(db)

      [{_s, _p, literal}] = RDF.Graph.triples(exported) |> Enum.to_list()
      assert String.length(RDF.Literal.value(literal)) == 10_000
    end
  end

  # ===========================================================================
  # 1.7.5.4: Blank Node Identity Preservation
  # ===========================================================================

  describe "blank node identity preservation within graph" do
    test "blank nodes maintain identity across triples", %{db: db, manager: manager} do
      # Create graph where same blank node is subject in multiple triples
      bnode = RDF.bnode("person1")

      original =
        RDF.graph([
          {bnode, RDF.type(), RDF.iri("http://xmlns.com/foaf/0.1/Person")},
          {bnode, RDF.iri("http://xmlns.com/foaf/0.1/name"), RDF.literal("Alice")},
          {bnode, RDF.iri("http://xmlns.com/foaf/0.1/age"), RDF.XSD.Integer.new!(30)}
        ])

      {:ok, _} = Loader.load_graph(db, manager, original)
      {:ok, exported} = Exporter.export_graph(db)

      # All triples should have the same subject (blank node)
      subjects =
        exported
        |> RDF.Graph.triples()
        |> Enum.map(fn {s, _p, _o} -> s end)
        |> Enum.uniq()

      assert length(subjects) == 1
      subject = hd(subjects)
      assert match?(%RDF.BlankNode{}, subject)

      # Should have 3 triples
      assert RDF.Graph.triple_count(exported) == 3
    end

    test "blank node as object refers to same node", %{db: db, manager: manager} do
      # Graph where blank node is both subject and object
      bnode = RDF.bnode("address")

      original =
        RDF.graph([
          {RDF.iri("http://example.org/person"), RDF.iri("http://example.org/address"), bnode},
          {bnode, RDF.iri("http://example.org/city"), RDF.literal("New York")},
          {bnode, RDF.iri("http://example.org/zip"), RDF.literal("10001")}
        ])

      {:ok, _} = Loader.load_graph(db, manager, original)
      {:ok, exported} = Exporter.export_graph(db)

      # Find the bnode that is object of :address predicate
      address_triple =
        exported
        |> RDF.Graph.triples()
        |> Enum.find(fn {_s, p, _o} ->
          p == RDF.iri("http://example.org/address")
        end)

      {_s, _p, address_bnode} = address_triple
      assert match?(%RDF.BlankNode{}, address_bnode)

      # That same bnode should be subject of city and zip triples
      city_triple =
        exported
        |> RDF.Graph.triples()
        |> Enum.find(fn {s, p, _o} ->
          s == address_bnode and p == RDF.iri("http://example.org/city")
        end)

      assert city_triple != nil
    end

    test "different blank nodes remain distinct", %{db: db, manager: manager} do
      bnode1 = RDF.bnode("person1")
      bnode2 = RDF.bnode("person2")

      original =
        RDF.graph([
          {bnode1, RDF.iri("http://example.org/name"), RDF.literal("Alice")},
          {bnode2, RDF.iri("http://example.org/name"), RDF.literal("Bob")},
          {bnode1, RDF.iri("http://example.org/knows"), bnode2}
        ])

      {:ok, _} = Loader.load_graph(db, manager, original)
      {:ok, exported} = Exporter.export_graph(db)

      # Should have 3 triples
      assert RDF.Graph.triple_count(exported) == 3

      # Find both name triples
      name_triples =
        exported
        |> RDF.Graph.triples()
        |> Enum.filter(fn {_s, p, _o} ->
          p == RDF.iri("http://example.org/name")
        end)

      assert length(name_triples) == 2

      # Subjects should be different
      subjects = Enum.map(name_triples, fn {s, _p, _o} -> s end) |> Enum.uniq()
      assert length(subjects) == 2
    end
  end
end
