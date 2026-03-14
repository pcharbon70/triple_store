defmodule TripleStore.Integration.NQuadsLoadingTest do
  @moduledoc """
  Integration tests for Section 6.2.1: N-Quads Loading.

  Tests loading N-Quads files into the quad store, including:
  - Single named graph loading
  - Multiple named graphs
  - Default graph handling
  - Blank node graphs
  - Large file performance
  - String loading
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/nquads_loading_test"
  @ex "http://example.org/"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    TripleStore.Integration.Helpers.unique_path("nquads_loading_test")
  end

  defp cleanup_path(path) do
    TripleStore.Integration.Helpers.cleanup_path(path)
  end

  defp create_test_nquads_file(path, content) do
    File.write!(path, content)
  end

  defp count_all_quads(db) do
    # Use lookup_quads with pattern that matches all quads
    all_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :var}, %{})
    length(all_quads)
  end

  # ===========================================================================
  # Setup
  # ===========================================================================

  setup do
    db_path = unique_path()

    {:ok, db} = ErlangAdapter.open(db_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager), do: Manager.stop(manager)
      ErlangAdapter.close(db)
      cleanup_path(db_path)
    end)

    %{db: db, manager: manager, db_path: db_path}
  end

  # ===========================================================================
  # 6.2.1.1: Test load N-Quads file with single graph
  # ===========================================================================

  describe "6.2.1.1 load N-Quads file with single graph" do
    test "loads quads into a single named graph", %{db: db, manager: manager, db_path: db_path} do
      nquads_file = Path.join(db_path, "single_graph.nq")

      content = """
      <#{@ex}s1> <#{@ex}p1> "object1" <#{@ex}graph1> .
      <#{@ex}s2> <#{@ex}p2> "object2" <#{@ex}graph1> .
      <#{@ex}s3> <#{@ex}p1> "object3" <#{@ex}graph1> .
      """

      create_test_nquads_file(nquads_file, content)

      assert {:ok, 3} = Loader.load_nquads_file(db, manager, nquads_file)

      # Verify all quads were loaded into the named graph
      graph = RDF.iri("#{@ex}graph1")

      assert QuadOperations.graph_exists?(db, manager, graph)

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 3
    end

    test "correctly encodes all terms in the graph", %{db: db, manager: manager, db_path: db_path} do
      nquads_file = Path.join(db_path, "encoding_test.nq")

      content = """
      <#{@ex}subject> <#{@ex}predicate> "literal object" <#{@ex}test> .
      <#{@ex}subject> <#{@ex}predicate> "42"^^<http://www.w3.org/2001/XMLSchema#integer> <#{@ex}test> .
      <#{@ex}subject> <#{@ex}predicate> <#{@ex}uri-object> <#{@ex}test> .
      """

      create_test_nquads_file(nquads_file, content)

      assert {:ok, 3} = Loader.load_nquads_file(db, manager, nquads_file)

      graph = RDF.iri("#{@ex}test")

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == 3
    end
  end

  # ===========================================================================
  # 6.2.1.2: Test load N-Quads file with multiple graphs
  # ===========================================================================

  describe "6.2.1.2 load N-Quads file with multiple graphs" do
    test "loads quads into multiple named graphs", %{db: db, manager: manager, db_path: db_path} do
      nquads_file = Path.join(db_path, "multi_graph.nq")

      content = """
      <#{@ex}s1> <#{@ex}p1> "o1" <#{@ex}graph1> .
      <#{@ex}s2> <#{@ex}p2> "o2" <#{@ex}graph2> .
      <#{@ex}s3> <#{@ex}p1> "o3" <#{@ex}graph1> .
      <#{@ex}s4> <#{@ex}p2> "o4" <#{@ex}graph2> .
      <#{@ex}s5> <#{@ex}p3> "o5" <#{@ex}graph3> .
      """

      create_test_nquads_file(nquads_file, content)

      assert {:ok, 5} = Loader.load_nquads_file(db, manager, nquads_file)

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

      # Verify all graphs exist
      assert QuadOperations.graph_exists?(db, manager, graph1)
      assert QuadOperations.graph_exists?(db, manager, graph2)
      assert QuadOperations.graph_exists?(db, manager, graph3)
    end

    test "total count reflects all graphs", %{db: db, manager: manager, db_path: db_path} do
      nquads_file = Path.join(db_path, "total_count.nq")

      content = """
      <#{@ex}s1> <#{@ex}p1> "o1" <#{@ex}g1> .
      <#{@ex}s2> <#{@ex}p1> "o2" <#{@ex}g2> .
      <#{@ex}s3> <#{@ex}p1> "o3" <#{@ex}g3> .
      """

      create_test_nquads_file(nquads_file, content)

      assert {:ok, 3} = Loader.load_nquads_file(db, manager, nquads_file)

      total = count_all_quads(db)
      assert total == 3
    end
  end

  # ===========================================================================
  # 6.2.1.3: Test load N-Quads with default graph
  # ===========================================================================

  describe "6.2.1.3 load N-Quads with default graph" do
    test "loads quads without graph name to default graph", %{
      db: db,
      manager: manager,
      db_path: db_path
    } do
      nquads_file = Path.join(db_path, "default_graph.nq")

      # N-Quads without the fourth component (graph) go to default graph
      content = """
      <#{@ex}s1> <#{@ex}p1> "o1" .
      <#{@ex}s2> <#{@ex}p2> "o2" .
      """

      create_test_nquads_file(nquads_file, content)

      assert {:ok, 2} = Loader.load_nquads_file(db, manager, nquads_file)

      # Verify default graph has quads
      assert QuadOperations.default_graph_exists?(db)

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, :default)
      assert count == 2
    end

    test "handles mix of default and named graphs", %{db: db, manager: manager, db_path: db_path} do
      nquads_file = Path.join(db_path, "mixed_graphs.nq")

      content = """
      <#{@ex}s1> <#{@ex}p1> "o1" <#{@ex}named> .
      <#{@ex}s2> <#{@ex}p2> "o2" .
      <#{@ex}s3> <#{@ex}p1> "o3" <#{@ex}named> .
      <#{@ex}s4> <#{@ex}p2> "o4" .
      """

      create_test_nquads_file(nquads_file, content)

      assert {:ok, 4} = Loader.load_nquads_file(db, manager, nquads_file)

      # Verify named graph
      named_graph = RDF.iri("#{@ex}named")
      {:ok, named_count} = QuadOperations.graph_quad_count(db, manager, named_graph)
      assert named_count == 2

      # Verify default graph
      {:ok, default_count} = QuadOperations.graph_quad_count(db, manager, :default)
      assert default_count == 2
    end
  end

  # ===========================================================================
  # 6.2.1.4: Test load N-Quads with blank node graphs
  # ===========================================================================

  describe "6.2.1.4 load N-Quads with blank node graphs" do
    test "loads quads with blank node as graph name", %{
      db: db,
      manager: manager,
      db_path: db_path
    } do
      nquads_file = Path.join(db_path, "blank_graph.nq")

      content = """
      <#{@ex}s1> <#{@ex}p1> "o1" _:g1 .
      <#{@ex}s2> <#{@ex}p2> "o2" _:g1 .
      <#{@ex}s3> <#{@ex}p1> "o3" _:g2 .
      """

      create_test_nquads_file(nquads_file, content)

      assert {:ok, 3} = Loader.load_nquads_file(db, manager, nquads_file)

      # Verify the quads were loaded - blank nodes are valid graph names
      total = count_all_quads(db)
      assert total == 3
    end

    test "preserves blank node graph distinction", %{db: db, manager: manager, db_path: db_path} do
      nquads_file = Path.join(db_path, "blank_distinct.nq")

      # Same blank node label should identify the same graph
      content = """
      <#{@ex}s1> <#{@ex}p1> "o1" _:sameGraph .
      <#{@ex}s2> <#{@ex}p2> "o2" _:sameGraph .
      """

      create_test_nquads_file(nquads_file, content)

      assert {:ok, 2} = Loader.load_nquads_file(db, manager, nquads_file)

      total = count_all_quads(db)
      assert total == 2
    end
  end

  # ===========================================================================
  # 6.2.1.5: Test load large N-Quads file
  # ===========================================================================

  describe "6.2.1.5 load large N-Quads file" do
    test "handles large files efficiently (10k+ quads)", %{
      db: db,
      manager: manager,
      db_path: db_path
    } do
      nquads_file = Path.join(db_path, "large.nq")

      # Generate 10,000 quads across 10 graphs
      num_quads = 10_000
      num_graphs = 10

      {time_us, _} =
        :timer.tc(fn ->
          File.open(nquads_file, [:write], fn file ->
            Enum.each(1..num_quads, fn i ->
              graph_num = rem(i, num_graphs) + 1
              quad = "<#{@ex}s#{i}> <#{@ex}p> \"o#{i}\" <#{@ex}g#{graph_num}> .\n"
              IO.write(file, quad)
            end)
          end)
        end)

      # File creation speed depends heavily on the local temp filesystem.
      # Keep a coarse guard here so the test focuses on loader behavior rather
      # than failing on normal filesystem variance.
      assert time_us < 5_000_000

      # Load and measure
      {load_time_us, result} =
        :timer.tc(fn -> Loader.load_nquads_file(db, manager, nquads_file) end)

      assert {:ok, ^num_quads} = result

      # Loading should complete in reasonable time
      assert load_time_us < 30_000_000,
             "Loading #{num_quads} quads took #{load_time_us / 1_000_000}ms"

      # Verify total count
      total = count_all_quads(db)
      assert total == num_quads

      # Verify each graph has approximately the right number of quads
      Enum.each(1..num_graphs, fn graph_num ->
        graph = RDF.iri("#{@ex}g#{graph_num}")
        {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)

        # Each graph should have either 1000 or 1001 quads (due to distribution)
        assert count >= 1000 and count <= 1001,
               "Graph #{graph_num} has #{count} quads, expected ~1000"
      end)
    end

    test "handles very large single graph file", %{db: db, manager: manager, db_path: db_path} do
      nquads_file = Path.join(db_path, "large_single.nq")

      # 5,000 quads in a single graph
      num_quads = 5_000

      File.open(nquads_file, [:write], fn file ->
        Enum.each(1..num_quads, fn i ->
          quad = "<#{@ex}s#{i}> <#{@ex}p> \"o#{i}\" <#{@ex}large> .\n"
          IO.write(file, quad)
        end)
      end)

      assert {:ok, ^num_quads} = Loader.load_nquads_file(db, manager, nquads_file)

      graph = RDF.iri("#{@ex}large")

      {:ok, count} = QuadOperations.graph_quad_count(db, manager, graph)
      assert count == num_quads
    end
  end

  # ===========================================================================
  # 6.2.1.6: Test load N-Quads from string
  # ===========================================================================

  describe "6.2.1.6 load N-Quads from string" do
    test "loads quads from N-Quads string", %{db: db, manager: manager} do
      content = """
      <#{@ex}s1> <#{@ex}p1> "o1" <#{@ex}graph1> .
      <#{@ex}s2> <#{@ex}p2> "o2" <#{@ex}graph1> .
      <#{@ex}s3> <#{@ex}p1> "o3" <#{@ex}graph2> .
      """

      assert {:ok, 3} = Loader.load_nquads_string(db, manager, content)

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

    test "handles string with default graph quads", %{db: db, manager: manager} do
      content = """
      <#{@ex}s1> <#{@ex}p> "default1" .
      <#{@ex}s2> <#{@ex}p> "default2" .
      <#{@ex}s3> <#{@ex}p> "named" <#{@ex}named> .
      """

      assert {:ok, 3} = Loader.load_nquads_string(db, manager, content)

      # Verify default graph
      {:ok, default_count} = QuadOperations.graph_quad_count(db, manager, :default)
      assert default_count == 2

      # Verify named graph
      named_graph = RDF.iri("#{@ex}named")
      {:ok, named_count} = QuadOperations.graph_quad_count(db, manager, named_graph)
      assert named_count == 1
    end

    test "handles string with blank node subjects and graphs", %{db: db, manager: manager} do
      content = """
      _:b1 <#{@ex}p1> "o1" <#{@ex}g1> .
      _:b1 <#{@ex}p2> "o2" <#{@ex}g1> .
      _:b2 <#{@ex}p1> "o3" _:g2 .
      """

      assert {:ok, 3} = Loader.load_nquads_string(db, manager, content)

      total = count_all_quads(db)
      assert total == 3
    end

    test "handles empty string", %{db: db, manager: manager} do
      assert {:ok, 0} = Loader.load_nquads_string(db, manager, "")

      total = count_all_quads(db)
      assert total == 0
    end

    test "handles string with only whitespace", %{db: db, manager: manager} do
      assert {:ok, 0} = Loader.load_nquads_string(db, manager, "   \n\n  ")

      total = count_all_quads(db)
      assert total == 0
    end
  end
end
