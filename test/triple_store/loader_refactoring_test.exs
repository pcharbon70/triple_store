defmodule TripleStore.LoaderRefactoringTest do
  @moduledoc """
  Tests for Section 2.6: Loader Module Refactoring.

  Verifies that the unified API supports both triples and quads:
  - load_file/4 accepts :graph option
  - load_string/5 accepts :graph option
  - load_stream/4 accepts :graph option
  - load_graph/4 accepts RDF.Dataset and :graph option
  - Backward compatibility is maintained
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/triple_store_loader_refactoring_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = NIF.open(test_path, schema: :quad)
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
  # Helper Functions
  # ===========================================================================

  defp create_turtle_file(name, content \\ nil) do
    default_content = """
    @prefix ex: <http://example.org/> .

    ex:s1 ex:p "o1" .
    ex:s2 ex:p "o2" .
    ex:s3 ex:p "o3" .
    """

    path = Path.join([System.tmp_dir!(), "loader_refactor_#{name}_#{:erlang.unique_integer()}.ttl"])
    File.write!(path, content || default_content)
    path
  end

  # ===========================================================================
  # load_file with :graph option Tests
  # ===========================================================================

  describe "load_file/4 with :graph option" do
    test "loads Turtle file to named graph", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/target_graph")
      path = create_turtle_file("graph_option")

      {:ok, count} = Loader.load_file(db, manager, path, graph: graph)

      assert count == 3

      # Verify quads are in the named graph
      {:ok, graph_id} = TripleStore.Adapter.term_to_id(manager, graph)
      quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})

      assert length(quads) == 3

      File.rm!(path)
    end

    test "preserves default graph when :graph is :default", %{db: db, manager: manager} do
      path = create_turtle_file("default_graph")

      {:ok, count} = Loader.load_file(db, manager, path, graph: :default)

      assert count == 3

      # Verify quads are in default graph
      default_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})
      assert length(default_quads) == 3

      File.rm!(path)
    end

    test "preserves default graph when :graph is not specified", %{db: db, manager: manager} do
      path = create_turtle_file("no_graph_option")

      {:ok, count} = Loader.load_file(db, manager, path)

      assert count == 3

      # Verify quads are in default graph
      default_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})
      assert length(default_quads) == 3

      File.rm!(path)
    end

    test "accepts RDF.BlankNode as graph term", %{db: db, manager: manager} do
      graph = RDF.bnode("temp_graph")
      path = create_turtle_file("bnode_graph")

      {:ok, count} = Loader.load_file(db, manager, path, graph: graph)

      assert count == 3

      File.rm!(path)
    end

    test "backward compatibility - loads without graph option", %{db: db, manager: manager} do
      path = create_turtle_file("backward_compat")

      # Old API - no graph option
      {:ok, count} = Loader.load_file(db, manager, path)

      assert count == 3

      File.rm!(path)
    end
  end

  # ===========================================================================
  # load_string with :graph option Tests
  # ===========================================================================

  describe "load_string/5 with :graph option" do
    test "loads Turtle string to named graph", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/string_graph")
      content = """
      @prefix ex: <http://example.org/> .
      ex:s ex:p "o" .
      """

      {:ok, count} = Loader.load_string(db, manager, content, :turtle, graph: graph)

      assert count == 1

      # Verify quad is in the named graph
      {:ok, graph_id} = TripleStore.Adapter.term_to_id(manager, graph)
      quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})

      assert length(quads) == 1
    end

    test "loads to default graph when :graph is :default", %{db: db, manager: manager} do
      content = """
      @prefix ex: <http://example.org/> .
      ex:s ex:p "o" .
      """

      {:ok, count} = Loader.load_string(db, manager, content, :turtle, graph: :default)

      assert count == 1

      # Verify quad is in default graph
      default_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})
      assert length(default_quads) == 1
    end

    test "backward compatibility - loads without graph option", %{db: db, manager: manager} do
      content = """
      @prefix ex: <http://example.org/> .
      ex:s ex:p "o" .
      """

      {:ok, count} = Loader.load_string(db, manager, content, :turtle)

      assert count == 1
    end
  end

  # ===========================================================================
  # load_stream with :graph option Tests
  # ===========================================================================

  describe "load_stream/4 with :graph option" do
    test "loads stream of triples to named graph", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/stream_graph")

      triples = [
        {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), "o1"},
        {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), "o2"}
      ]

      {:ok, count} = Loader.load_stream(db, manager, triples, graph: graph)

      assert count == 2

      # Verify quads are in the named graph
      {:ok, graph_id} = TripleStore.Adapter.term_to_id(manager, graph)
      quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})

      assert length(quads) == 2
    end

    test "loads to default graph when :graph is :default", %{db: db, manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o"}
      ]

      {:ok, count} = Loader.load_stream(db, manager, triples, graph: :default)

      assert count == 1

      # Verify quad is in default graph
      default_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})
      assert length(default_quads) == 1
    end

    test "backward compatibility - loads without graph option", %{db: db, manager: manager} do
      triples = [
        {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o"}
      ]

      {:ok, count} = Loader.load_stream(db, manager, triples)

      assert count == 1
    end
  end

  # ===========================================================================
  # load_graph with RDF.Dataset and :graph option Tests
  # ===========================================================================

  describe "load_graph/4 with RDF.Dataset" do
    test "loads RDF.Dataset preserving named graphs", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")

      dataset =
        RDF.Dataset.new([
          {RDF.iri("http://example.org/s1"), RDF.iri("http://example.org/p"), "o1", graph1},
          {RDF.iri("http://example.org/s2"), RDF.iri("http://example.org/p"), "o2", graph2}
        ])

      {:ok, count} = Loader.load_graph(db, manager, dataset)

      assert count == 2

      # Verify both graphs have their quads
      assert QuadOperations.graph_exists?(db, manager, graph1)
      assert QuadOperations.graph_exists?(db, manager, graph2)
    end

    test "loads RDF.Dataset with default graph", %{db: db, manager: manager} do
      dataset =
        RDF.Dataset.new([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o"}
        ])

      {:ok, count} = Loader.load_graph(db, manager, dataset)

      assert count == 1

      # Verify quad is in default graph
      default_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})
      assert length(default_quads) == 1
    end
  end

  describe "load_graph/4 with RDF.Graph and :graph option" do
    test "loads RDF.Graph to specified named graph", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/target")

      rdf_graph =
        RDF.Graph.new([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o"}
        ])

      {:ok, count} = Loader.load_graph(db, manager, rdf_graph, graph: graph)

      assert count == 1

      # Verify quad is in the named graph
      {:ok, graph_id} = TripleStore.Adapter.term_to_id(manager, graph)
      quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})

      assert length(quads) == 1
    end

    test "loads RDF.Graph to default graph when :graph is :default", %{db: db, manager: manager} do
      rdf_graph =
        RDF.Graph.new([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o"}
        ])

      {:ok, count} = Loader.load_graph(db, manager, rdf_graph, graph: :default)

      assert count == 1

      # Verify quad is in default graph
      default_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})
      assert length(default_quads) == 1
    end

    test "backward compatibility - loads RDF.Graph without graph option", %{db: db, manager: manager} do
      rdf_graph =
        RDF.Graph.new([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o"}
        ])

      {:ok, count} = Loader.load_graph(db, manager, rdf_graph)

      assert count == 1

      # Verify quad is in default graph
      default_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})
      assert length(default_quads) == 1
    end
  end

  # ===========================================================================
  # Integration Tests
  # ===========================================================================

  describe "integration" do
    test "complete unified API workflow", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")
      graph3 = RDF.iri("http://example.org/g3")

      # Load from file
      path1 = create_turtle_file("file_test")
      {:ok, count1} = Loader.load_file(db, manager, path1, graph: graph1)
      assert count1 == 3
      File.rm!(path1)

      # Load from string
      content = "@prefix ex: <http://example.org/> . ex:s ex:p \"o\" ."
      {:ok, count2} = Loader.load_string(db, manager, content, :turtle, graph: graph2)
      assert count2 == 1

      # Load from stream
      triples = [{RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o"}]
      {:ok, count3} = Loader.load_stream(db, manager, triples, graph: graph3)
      assert count3 == 1

      # Load from RDF.Graph
      rdf_graph = RDF.Graph.new([{RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o"}])
      {:ok, count4} = Loader.load_graph(db, manager, rdf_graph, graph: graph1)
      assert count4 == 1

      # Load from RDF.Dataset
      dataset =
        RDF.Dataset.new([
          {RDF.iri("http://example.org/s"), RDF.iri("http://example.org/p"), "o", graph2}
        ])

      {:ok, count5} = Loader.load_graph(db, manager, dataset)
      assert count5 == 1

      # Verify all graphs exist
      assert QuadOperations.graph_exists?(db, manager, graph1)
      assert QuadOperations.graph_exists?(db, manager, graph2)
      assert QuadOperations.graph_exists?(db, manager, graph3)
    end
  end
end
