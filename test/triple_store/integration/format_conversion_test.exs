defmodule TripleStore.Integration.FormatConversionTest do
  @moduledoc """
  Integration tests for Section 6.2.4: Format Conversion.

  Tests conversion between different RDF formats:
  - Loading Turtle to named graph
  - Exporting single graph as Turtle
  - Exporting default graph as N-Triples
  - Converting N-Quads to Turtle (per graph)
  - Converting TriG to N-Quads
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.Exporter
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/format_conversion_test"
  @ex "http://example.org/"

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  defp unique_path do
    time_component = System.system_time(:microsecond)
    rand_component = :rand.uniform(1_000_000)
    "#{@test_db_base}_#{time_component}_#{rand_component}"
  end

  defp cleanup_path(path) do
    File.rm_rf(path)
  end

  defp count_all_quads(db) do
    all_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :var}, %{})
    length(all_quads)
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    db_path = unique_path()

    {:ok, db} = NIF.open(db_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      NIF.close(db)
      cleanup_path(db_path)
    end)

    %{db: db, manager: manager, db_path: db_path}
  end

  # ===========================================================================
  # 6.2.4.1: Test load Turtle to named graph
  # ===========================================================================

  describe "6.2.4.1 load Turtle to named graph" do
    test "loads Turtle file content to specified named graph", %{db: db, manager: manager, db_path: db_path} do
      turtle_file = Path.join(db_path, "data.ttl")

      content = """
      @prefix ex: <#{@ex}> .

      ex:s1 ex:p1 "o1" .
      ex:s2 ex:p2 "o2" .
      ex:s3 ex:p1 "o3" .
      """

      File.write!(turtle_file, content)

      # Load to named graph using load_to_graph
      target_graph = RDF.iri("#{@ex}myGraph")

      assert {:ok, 3} = Loader.load_to_graph(db, manager, turtle_file, target_graph)

      # Verify quads are in the named graph
      assert QuadOperations.graph_exists?(db, manager, target_graph)

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, target_graph)
      assert count == 3

      # Verify default graph is empty
      refute QuadOperations.default_graph_exists?(db)
    end

    test "loads same Turtle file to different graphs", %{db: db, manager: manager, db_path: db_path} do
      turtle_file = Path.join(db_path, "shared.ttl")

      content = """
      @prefix ex: <#{@ex}> .
      ex:s ex:p "o" .
      """

      File.write!(turtle_file, content)

      graph1 = RDF.iri("#{@ex}g1")
      graph2 = RDF.iri("#{@ex}g2")

      # Load to graph1
      assert {:ok, 1} = Loader.load_to_graph(db, manager, turtle_file, graph1)

      # Load to graph2
      assert {:ok, 1} = Loader.load_to_graph(db, manager, turtle_file, graph2)

      # Both graphs should have 1 quad each
      {:ok, count1} = QuadOperations.graph_quad_count(db, manager, graph1)
      {:ok, count2} = QuadOperations.graph_quad_count(db, manager, graph2)

      assert count1 == 1
      assert count2 == 1

      total = count_all_quads(db)
      assert total == 2
    end

    test "supports clear_graph option when loading", %{db: db, manager: manager, db_path: db_path} do
      turtle_file = Path.join(db_path, "clear.ttl")

      graph = RDF.iri("#{@ex}test")

      # Load initial data
      File.write!(turtle_file, "@prefix ex: <#{@ex}> . ex:s1 ex:p \"o1\" .")
      assert {:ok, 1} = Loader.load_to_graph(db, manager, turtle_file, graph)

      # Load more data with clear_graph option
      File.write!(turtle_file, "@prefix ex: <#{@ex}> . ex:s2 ex:p \"o2\" .")
      assert {:ok, 1} = Loader.load_to_graph(db, manager, turtle_file, graph, clear_graph: true)

      # Graph should only have the second set of data
      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 1
    end
  end

  # ===========================================================================
  # 6.2.4.2: Test export single graph as Turtle
  # ===========================================================================

  describe "6.2.4.2 export single graph as Turtle" do
    test "exports named graph as RDF.Graph structure", %{db: db, manager: manager} do
      # First, load some data into a named graph
      graph = RDF.iri("#{@ex}myGraph")

      nquads_content = """
      <#{@ex}s1> <#{@ex}p> "o1" <#{@ex}myGraph> .
      <#{@ex}s2> <#{@ex}p> "o2" <#{@ex}myGraph> .
      <#{@ex}s3> <#{@ex}p> "o3" <#{@ex}myGraph> .
      """

      assert {:ok, 3} = Loader.load_nquads_string(db, manager, nquads_content)

      # Export the single graph
      assert {:ok, rdf_graph} = Exporter.export_single_graph(db, manager, graph)

      # Verify the graph structure
      assert RDF.Graph.triple_count(rdf_graph) == 3
      assert rdf_graph.name == graph
    end

    test "exports default graph as RDF.Graph", %{db: db, manager: manager} do
      # Load data to default graph
      trig_content = """
      @prefix ex: <#{@ex}> .
      ex:s1 ex:p "o1" .
      ex:s2 ex:p "o2" .
      """

      assert {:ok, 2} = Loader.load_trig_string(db, manager, trig_content)

      # Export default graph
      assert {:ok, rdf_graph} = Exporter.export_default_graph(db)

      # Verify the graph structure
      assert RDF.Graph.triple_count(rdf_graph) == 2
    end

    test "returns error for non-existent graph", %{db: db, manager: manager} do
      non_existent = RDF.iri("#{@ex}doesNotExist")

      assert {:error, :graph_not_found} = Exporter.export_single_graph(db, manager, non_existent)
    end
  end

  # ===========================================================================
  # 6.2.4.3: Test export default graph as N-Triples
  # ===========================================================================

  describe "6.2.4.3 export default graph as N-Triples" do
    test "exports default graph quads correctly", %{db: db, manager: manager} do
      # Load data to default graph
      trig_content = """
      @prefix ex: <#{@ex}> .
      ex:s1 ex:p1 "o1" .
      ex:s2 ex:p2 "o2" .
      ex:s3 ex:p3 "o3" .
      """

      assert {:ok, 3} = Loader.load_trig_string(db, manager, trig_content)

      # Export default graph
      assert {:ok, rdf_graph} = Exporter.export_default_graph(db)

      # Verify triple count
      assert RDF.Graph.triple_count(rdf_graph) == 3
    end

    test "default graph export does not include named graph quads", %{db: db, manager: manager} do
      # Load to both default and named graphs
      trig_content = """
      @prefix ex: <#{@ex}> .
      ex:default ex:p "d" .
      GRAPH ex:named { ex:named ex:p "n" }
      """

      assert {:ok, 2} = Loader.load_trig_string(db, manager, trig_content)

      # Export default graph
      assert {:ok, rdf_graph} = Exporter.export_default_graph(db)

      # Should only have 1 triple from default graph
      assert RDF.Graph.triple_count(rdf_graph) == 1
    end
  end

  # ===========================================================================
  # 6.2.4.4: Test convert N-Quads to Turtle (per graph)
  # ===========================================================================

  describe "6.2.4.4 convert N-Quads to Turtle (per graph)" do
    test "converts N-Quads dataset to RDF.Graph for specific graph", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "input.nq")

      # Create N-Quads with multiple graphs
      File.write!(input_file, """
      <#{@ex}s1> <#{@ex}p> "o1" <#{@ex}g1> .
      <#{@ex}s2> <#{@ex}p> "o2" <#{@ex}g1> .
      <#{@ex}s3> <#{@ex}p> "o3" <#{@ex}g2> .
      """)

      assert {:ok, 3} = Loader.load_nquads_file(db, manager, input_file)

      # Export specific graph
      graph1 = RDF.iri("#{@ex}g1")

      assert {:ok, rdf_graph} = Exporter.export_single_graph(db, manager, graph1)

      # Verify triple count
      assert RDF.Graph.triple_count(rdf_graph) == 2
      assert rdf_graph.name == graph1
    end

    test "can export multiple graphs separately", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "multi.nq")

      File.write!(input_file, """
      <#{@ex}s1> <#{@ex}p> "o1" <#{@ex}g1> .
      <#{@ex}s2> <#{@ex}p> "o2" <#{@ex}g2> .
      """)

      assert {:ok, 2} = Loader.load_nquads_file(db, manager, input_file)

      # Export each graph
      assert {:ok, g1_graph} = Exporter.export_single_graph(db, manager, RDF.iri("#{@ex}g1"))
      assert {:ok, g2_graph} = Exporter.export_single_graph(db, manager, RDF.iri("#{@ex}g2"))

      assert RDF.Graph.triple_count(g1_graph) == 1
      assert RDF.Graph.triple_count(g2_graph) == 1
    end
  end

  # ===========================================================================
  # 6.2.4.5: Test convert TriG to N-Quads
  # ===========================================================================

  describe "6.2.4.5 convert TriG to N-Quads" do
    test "loads TriG and exports as N-Quads", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "input.trig")
      output_file = Path.join(db_path, "output.nq")

      # Create TriG input
      File.write!(input_file, """
      @prefix ex: <#{@ex}> .

      GRAPH ex:g1 {
        ex:s1 ex:p1 "o1" .
        ex:s2 ex:p2 "o2" .
      }

      ex:s3 ex:p3 "o3" .
      """)

      # Load TriG
      assert {:ok, 3} = Loader.load_trig_file(db, manager, input_file)

      # Export as N-Quads
      assert {:ok, 3} = Exporter.export_nquads_file(db, output_file)

      # Verify N-Quads output
      assert File.exists?(output_file)

      {:ok, content} = File.read(output_file)
      # N-Quads should contain graph IRIs
      assert String.contains?(content, "#{@ex}g1")
    end

    test "conversion preserves all graphs from TriG", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "complex.trig")
      output_file = Path.join(db_path, "output.nq")

      File.write!(input_file, """
      @prefix ex: <#{@ex}> .

      ex:s1 ex:p "default1" .
      ex:s2 ex:p "default2" .

      GRAPH ex:g1 {
        ex:s3 ex:p "o1" .
        ex:s4 ex:p "o2" .
      }

      GRAPH ex:g2 {
        ex:s5 ex:p "o3" .
      }
      """)

      assert {:ok, 5} = Loader.load_trig_file(db, manager, input_file)
      assert {:ok, 5} = Exporter.export_nquads_file(db, output_file)

      # Verify all quads were converted
      total = count_all_quads(db)
      assert total == 5

      # Verify graph structure
      {:ok, default_count} = QuadOperations.graph_quad_count(db, manager, :default)
      {:ok, g1_count} = QuadOperations.graph_quad_count(db, manager, RDF.iri("#{@ex}g1"))
      {:ok, g2_count} = QuadOperations.graph_quad_count(db, manager, RDF.iri("#{@ex}g2"))

      assert default_count == 2
      assert g1_count == 2
      assert g2_count == 1
    end

    test "handles TriG with only named graphs", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "named_only.trig")
      output_file = Path.join(db_path, "output.nq")

      File.write!(input_file, """
      @prefix ex: <#{@ex}> .

      GRAPH ex:g1 { ex:s1 ex:p "o1" . }
      GRAPH ex:g2 { ex:s2 ex:p "o2" . }
      GRAPH ex:g3 { ex:s3 ex:p "o3" . }
      """)

      assert {:ok, 3} = Loader.load_trig_file(db, manager, input_file)
      assert {:ok, 3} = Exporter.export_nquads_file(db, output_file)

      # Default graph should be empty
      refute QuadOperations.default_graph_exists?(db)

      # All 3 named graphs should exist
      Enum.each(1..3, fn i ->
        graph = RDF.iri("#{@ex}g#{i}")
        assert QuadOperations.graph_exists?(db, manager, graph)
      end)
    end
  end
end
