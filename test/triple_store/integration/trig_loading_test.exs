defmodule TripleStore.Integration.TriGLoadingTest do
  @moduledoc """
  Integration tests for Section 6.2.2: TriG Loading.

  Tests loading TriG files into the quad store, including:
  - Single named graph loading
  - Multiple named graphs
  - Default graph block handling
  - Nested/multiple graphs in one file
  - Large file performance
  - String loading
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/trig_loading_test"
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

  defp create_test_trig_file(path, content) do
    File.write!(path, content)
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
  # 6.2.2.1: Test load TriG file with single named graph
  # ===========================================================================

  describe "6.2.2.1 load TriG file with single named graph" do
    test "loads quads into a single named graph", %{db: db, manager: manager, db_path: db_path} do
      trig_file = Path.join(db_path, "single_graph.trig")

      content = """
      @prefix ex: <#{@ex}> .

      GRAPH <#{@ex}graph1> {
        ex:s1 ex:p1 "object1" .
        ex:s2 ex:p2 "object2" .
        ex:s3 ex:p1 "object3" .
      }
      """

      create_test_trig_file(trig_file, content)

      assert {:ok, 3} = Loader.load_trig_file(db, manager, trig_file)

      # Verify all quads were loaded into the named graph
      graph = RDF.iri("#{@ex}graph1")

      assert QuadOperations.graph_exists?(db, manager, graph)

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 3
    end

    test "handles TriG with prefixes", %{db: db, manager: manager, db_path: db_path} do
      trig_file = Path.join(db_path, "prefixes.trig")

      content = """
      @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
      @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
      @prefix ex: <#{@ex}> .

      GRAPH ex:testGraph {
        ex:subject a rdf:Resource ;
                  rdfs:label "Test Resource" .
      }
      """

      create_test_trig_file(trig_file, content)

      assert {:ok, 2} = Loader.load_trig_file(db, manager, trig_file)

      graph = RDF.iri("#{@ex}testGraph")

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 2
    end
  end

  # ===========================================================================
  # 6.2.2.2: Test load TriG file with multiple graphs
  # ===========================================================================

  describe "6.2.2.2 load TriG file with multiple graphs" do
    test "loads quads into multiple named graphs", %{db: db, manager: manager, db_path: db_path} do
      trig_file = Path.join(db_path, "multi_graph.trig")

      content = """
      @prefix ex: <#{@ex}> .

      GRAPH ex:graph1 {
        ex:s1 ex:p1 "o1" .
        ex:s2 ex:p2 "o2" .
      }

      GRAPH ex:graph2 {
        ex:s3 ex:p1 "o3" .
        ex:s4 ex:p2 "o4" .
      }

      GRAPH ex:graph3 {
        ex:s5 ex:p3 "o5" .
      }
      """

      create_test_trig_file(trig_file, content)

      assert {:ok, 5} = Loader.load_trig_file(db, manager, trig_file)

      # Verify each graph has the correct number of quads
      graph1 = RDF.iri("#{@ex}graph1")
      graph2 = RDF.iri("#{@ex}graph2")
      graph3 = RDF.iri("#{@ex}graph3")

      {:ok, count1} = QuadOperations.graph_quad_count(db, manager, graph1)
      {:ok, count2} = QuadOperations.graph_quad_count(db, manager, graph2)
      {:ok, count3} = QuadOperations.graph_quad_count(db, manager, graph3)

      assert count1 == 2
      assert count2 == 2
      assert count3 == 1
    end

    test "handles graphs with interleaved declarations", %{db: db, manager: manager, db_path: db_path} do
      trig_file = Path.join(db_path, "interleaved.trig")

      content = """
      @prefix ex: <#{@ex}> .

      GRAPH ex:g1 { ex:s1 ex:p "o1" . }

      GRAPH ex:g2 { ex:s2 ex:p "o2" . }

      GRAPH ex:g1 { ex:s3 ex:p "o3" . }

      GRAPH ex:g2 { ex:s4 ex:p "o4" . }
      """

      create_test_trig_file(trig_file, content)

      assert {:ok, 4} = Loader.load_trig_file(db, manager, trig_file)

      g1 = RDF.iri("#{@ex}g1")
      g2 = RDF.iri("#{@ex}g2")

      {:ok, count1} = QuadOperations.graph_quad_count(db, manager, g1)
      {:ok, count2} = QuadOperations.graph_quad_count(db, manager, g2)

      assert count1 == 2
      assert count2 == 2
    end
  end

  # ===========================================================================
  # 6.2.2.3: Test load TriG with default graph block
  # ===========================================================================

  describe "6.2.2.3 load TriG with default graph block" do
    test "loads default graph triples (outside GRAPH blocks)", %{db: db, manager: manager, db_path: db_path} do
      trig_file = Path.join(db_path, "default_graph.trig")

      content = """
      @prefix ex: <#{@ex}> .

      # These go to the default graph
      ex:s1 ex:p1 "o1" .
      ex:s2 ex:p2 "o2" .

      # Named graph
      GRAPH ex:named {
        ex:s3 ex:p1 "o3" .
      }
      """

      create_test_trig_file(trig_file, content)

      assert {:ok, 3} = Loader.load_trig_file(db, manager, trig_file)

      # Verify default graph
      assert QuadOperations.default_graph_exists?(db)

      {:ok, default_count} = QuadOperations.graph_quad_count(db, manager, :default)
      assert default_count == 2

      # Verify named graph
      named = RDF.iri("#{@ex}named")
      {:ok, named_count} = QuadOperations.graph_quad_count(db, manager, named)
      assert named_count == 1
    end

    test "handles file with only default graph content", %{db: db, manager: manager, db_path: db_path} do
      trig_file = Path.join(db_path, "only_default.trig")

      content = """
      @prefix ex: <#{@ex}> .

      ex:s1 ex:p1 "o1" .
      ex:s2 ex:p2 "o2" .
      ex:s3 ex:p3 "o3" .
      """

      create_test_trig_file(trig_file, content)

      assert {:ok, 3} = Loader.load_trig_file(db, manager, trig_file)

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, :default)
      assert count == 3
    end
  end

  # ===========================================================================
  # 6.2.2.4: Test load TriG with nested graphs (multiple graphs in one file)
  # ===========================================================================

  describe "6.2.2.4 load TriG with multiple graphs in one file" do
    test "handles multiple GRAPH blocks for the same graph", %{db: db, manager: manager, db_path: db_path} do
      trig_file = Path.join(db_path, "same_graph_multiple.trig")

      content = """
      @prefix ex: <#{@ex}> .

      GRAPH ex:shared {
        ex:s1 ex:p1 "o1" .
      }

      GRAPH ex:shared {
        ex:s2 ex:p2 "o2" .
      }

      GRAPH ex:shared {
        ex:s3 ex:p3 "o3" .
      }
      """

      create_test_trig_file(trig_file, content)

      assert {:ok, 3} = Loader.load_trig_file(db, manager, trig_file)

      graph = RDF.iri("#{@ex}shared")

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 3
    end

    test "handles complex multi-graph TriG file", %{db: db, manager: manager, db_path: db_path} do
      trig_file = Path.join(db_path, "complex_multi.trig")

      content = """
      @prefix ex: <#{@ex}> .
      @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

      # Default graph
      ex:defaultThing rdf:type ex:Thing .

      # First named graph
      GRAPH ex:graph1 {
        ex:s1 ex:p "o1" .
        ex:s2 ex:p "o2" .
      }

      # Second named graph
      GRAPH ex:graph2 {
        ex:s3 ex:p "o3" .
        ex:s4 ex:p "o4" .
      }

      # Add more to first graph
      GRAPH ex:graph1 {
        ex:s5 ex:p "o5" .
      }

      # Add more to default
      ex:anotherDefault rdf:type ex:Thing .
      """

      create_test_trig_file(trig_file, content)

      assert {:ok, 7} = Loader.load_trig_file(db, manager, trig_file)

      {:ok, default_count} = QuadOperations.graph_quad_count(db, manager, :default)
      {:ok, g1_count} = QuadOperations.graph_quad_count(db, manager, RDF.iri("#{@ex}graph1"))
      {:ok, g2_count} = QuadOperations.graph_quad_count(db, manager, RDF.iri("#{@ex}graph2"))

      assert default_count == 2
      assert g1_count == 3
      assert g2_count == 2
    end
  end

  # ===========================================================================
  # 6.2.2.5: Test load large TriG file
  # ===========================================================================

  describe "6.2.2.5 load large TriG file" do
    test "handles large files efficiently (10k+ quads)", %{db: db, manager: manager, db_path: db_path} do
      trig_file = Path.join(db_path, "large.trig")

      # Generate 10,000 quads across 10 graphs
      num_quads = 10_000
      num_graphs = 10

      # Write header
      File.open(trig_file, [:write], fn file ->
        IO.write(file, "@prefix ex: <#{@ex}> .\n\n")

        # Generate graphs
        Enum.each(1..num_quads, fn i ->
          graph_num = rem(i, num_graphs) + 1

          if rem(i, 1000) == 1 do
            IO.write(file, "GRAPH ex:g#{graph_num} {\n")
          end

          quad = "  ex:s#{i} ex:p \"o#{i}\" .\n"

          if rem(i, 1000) == 0 or i == num_quads do
            IO.write(file, quad <> "}\n\n")
          else
            IO.write(file, quad)
          end
        end)
      end)

      # Load and measure
      {load_time_us, result} = :timer.tc(fn -> Loader.load_trig_file(db, manager, trig_file) end)

      assert {:ok, ^num_quads} = result

      # Loading should complete in reasonable time
      assert load_time_us < 30_000_000,
             "Loading #{num_quads} quads took #{load_time_us / 1_000_000}ms"

      # Verify total count
      total = count_all_quads(db)
      assert total == num_quads
    end

    test "handles large single graph TriG file", %{db: db, manager: manager, db_path: db_path} do
      trig_file = Path.join(db_path, "large_single.trig")

      # 5,000 quads in a single graph
      num_quads = 5_000

      File.open(trig_file, [:write], fn file ->
        IO.write(file, "@prefix ex: <#{@ex}> .\n\n")
        IO.write(file, "GRAPH ex:large {\n")

        Enum.each(1..num_quads, fn i ->
          IO.write(file, "  ex:s#{i} ex:p \"o#{i}\" .\n")
        end)

        IO.write(file, "}\n")
      end)

      assert {:ok, ^num_quads} = Loader.load_trig_file(db, manager, trig_file)

      graph = RDF.iri("#{@ex}large")

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == num_quads
    end
  end

  # ===========================================================================
  # 6.2.2.6: Test load TriG from string
  # ===========================================================================

  describe "6.2.2.6 load TriG from string" do
    test "loads quads from TriG string", %{db: db, manager: manager} do
      content = """
      @prefix ex: <#{@ex}> .

      GRAPH ex:graph1 {
        ex:s1 ex:p1 "o1" .
        ex:s2 ex:p2 "o2" .
      }

      GRAPH ex:graph2 {
        ex:s3 ex:p1 "o3" .
      }
      """

      assert {:ok, 3} = Loader.load_trig_string(db, manager, content)

      # Verify graphs were created
      graph1 = RDF.iri("#{@ex}graph1")
      graph2 = RDF.iri("#{@ex}graph2")

      assert QuadOperations.graph_exists?(db, manager, graph1)
      assert QuadOperations.graph_exists?(db, manager, graph2)

      {:ok, count1} = QuadOperations.graph_quad_count(db, manager, graph1)
      {:ok, count2} = QuadOperations.graph_quad_count(db, manager, graph2)

      assert count1 == 2
      assert count2 == 1
    end

    test "handles string with default graph", %{db: db, manager: manager} do
      content = """
      @prefix ex: <#{@ex}> .

      ex:s1 ex:p "default1" .
      ex:s2 ex:p "default2" .

      GRAPH ex:named {
        ex:s3 ex:p "named" .
      }
      """

      assert {:ok, 3} = Loader.load_trig_string(db, manager, content)

      # Verify default graph
      {:ok, default_count} = QuadOperations.graph_quad_count(db, manager, :default)
      assert default_count == 2

      # Verify named graph
      named = RDF.iri("#{@ex}named")
      {:ok, named_count} = QuadOperations.graph_quad_count(db, manager, named)
      assert named_count == 1
    end

    test "handles string with blank node subjects", %{db: db, manager: manager} do
      content = """
      @prefix ex: <#{@ex}> .

      GRAPH ex:g1 {
        _:b1 ex:p1 "o1" .
        _:b1 ex:p2 "o2" .
      }

      GRAPH ex:g2 {
        _:b2 ex:p1 "o3" .
      }
      """

      assert {:ok, 3} = Loader.load_trig_string(db, manager, content)

      total = count_all_quads(db)
      assert total == 3
    end

    test "handles empty string", %{db: db, manager: manager} do
      assert {:ok, 0} = Loader.load_trig_string(db, manager, "")

      total = count_all_quads(db)
      assert total == 0
    end

    test "handles string with only whitespace and comments", %{db: db, manager: manager} do
      content = """
      # This is a comment
      @prefix ex: <#{@ex}> .

      # Another comment
      """

      assert {:ok, 0} = Loader.load_trig_string(db, manager, content)

      total = count_all_quads(db)
      assert total == 0
    end
  end
end
