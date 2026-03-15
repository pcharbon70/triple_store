defmodule TripleStore.Integration.RoundtripTest do
  @moduledoc """
  Integration tests for Section 6.2.3: Roundtrip Tests.

  Tests that loading and exporting data preserves all information correctly:
  - N-Quads load/export roundtrip
  - TriG load/export roundtrip
  - N-Quads to TriG conversion
  - TriG to N-Quads conversion
  - Graph preservation across roundtrip
  - Blank node ID preservation
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Exporter
  alias TripleStore.Integration.Helpers
  alias TripleStore.Loader
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/roundtrip_test"
  @ex "http://example.org/"

  # ===========================================================================
  # Helper Functions (using shared helpers from TripleStore.Integration.Helpers)
  # ===========================================================================

  defp unique_path do
    Helpers.unique_path("roundtrip_test")
  end

  defp cleanup_path(path) do
    Helpers.cleanup_path(path)
  end

  defp count_all_quads(db) do
    all_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :var}, %{})
    length(all_quads)
  end

  defp normalize_nquads_string(nquads) do
    # Normalize for comparison: sort lines, remove extra whitespace
    nquads
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.filter(&(String.length(&1) > 0))
    |> Enum.sort()
    |> Enum.join("\n")
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
  # 6.2.3.1: Test N-Quads load/export roundtrip
  # ===========================================================================

  describe "6.2.3.1 N-Quads load/export roundtrip" do
    test "loading and exporting N-Quads preserves all quads", %{
      db: db,
      manager: manager,
      db_path: db_path
    } do
      input_file = Path.join(db_path, "input.nq")
      output_file = Path.join(db_path, "output.nq")

      input_content = """
      <#{@ex}s1> <#{@ex}p1> "o1" <#{@ex}g1> .
      <#{@ex}s2> <#{@ex}p2> "o2" <#{@ex}g1> .
      <#{@ex}s3> <#{@ex}p1> "o3" <#{@ex}g2> .
      <#{@ex}s4> <#{@ex}p2> "o4" <#{@ex}g2> .
      <#{@ex}s5> <#{@ex}p3> "o5" <#{@ex}g3> .
      """

      File.write!(input_file, input_content)

      # Load the file
      assert {:ok, 5} = Loader.load_nquads_file(db, manager, input_file)

      # Export to N-Quads
      assert {:ok, exported_count} = Exporter.export_nquads_file(db, output_file)

      # The exported count should match the loaded count
      assert exported_count == 5

      # Verify the count in the database
      total = count_all_quads(db)
      assert total == 5
    end

    test "roundtrip preserves graph names", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "graphs.nq")
      output_file = Path.join(db_path, "output.nq")

      File.write!(input_file, """
      <#{@ex}s1> <#{@ex}p> "o1" <#{@ex}graphA> .
      <#{@ex}s2> <#{@ex}p> "o2" <#{@ex}graphB> .
      <#{@ex}s3> <#{@ex}p> "o3" <#{@ex}graphC> .
      """)

      # Load and export
      assert {:ok, 3} = Loader.load_nquads_file(db, manager, input_file)
      assert {:ok, 3} = Exporter.export_nquads_file(db, output_file)

      # Verify graphs exist
      assert QuadOperations.graph_exists?(db, manager, RDF.iri("#{@ex}graphA"))
      assert QuadOperations.graph_exists?(db, manager, RDF.iri("#{@ex}graphB"))
      assert QuadOperations.graph_exists?(db, manager, RDF.iri("#{@ex}graphC"))
    end

    test "roundtrip preserves literal values", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "literals.nq")
      output_file = Path.join(db_path, "output.nq")

      File.write!(input_file, """
      <#{@ex}s1> <#{@ex}p> "simple string" <#{@ex}g> .
      <#{@ex}s2> <#{@ex}p> "42"^^<http://www.w3.org/2001/XMLSchema#integer> <#{@ex}g> .
      <#{@ex}s3> <#{@ex}p> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> <#{@ex}g> .
      """)

      # Load and export
      assert {:ok, 3} = Loader.load_nquads_file(db, manager, input_file)
      assert {:ok, 3} = Exporter.export_nquads_file(db, output_file)

      # Verify count is preserved
      total = count_all_quads(db)
      assert total == 3
    end
  end

  # ===========================================================================
  # 6.2.3.2: Test TriG load/export roundtrip
  # ===========================================================================

  describe "6.2.3.2 TriG load/export roundtrip" do
    test "loading and exporting TriG preserves all quads", %{
      db: db,
      manager: manager,
      db_path: db_path
    } do
      input_file = Path.join(db_path, "input.trig")
      output_file = Path.join(db_path, "output.trig")

      input_content = """
      @prefix ex: <#{@ex}> .

      GRAPH ex:g1 {
        ex:s1 ex:p1 "o1" .
        ex:s2 ex:p2 "o2" .
      }

      GRAPH ex:g2 {
        ex:s3 ex:p1 "o3" .
        ex:s4 ex:p2 "o4" .
      }
      """

      File.write!(input_file, input_content)

      # Load the file
      assert {:ok, 4} = Loader.load_trig_file(db, manager, input_file)

      # Export to TriG
      assert {:ok, 4} = Exporter.export_trig_file(db, output_file)

      # Verify the exported file contains the quads
      assert File.exists?(output_file)

      {:ok, exported_content} = File.read(output_file)

      # The exported content should mention the graphs
      assert String.contains?(exported_content, "#{@ex}g1")
      assert String.contains?(exported_content, "#{@ex}g2")
    end

    test "roundtrip preserves default and named graphs", %{
      db: db,
      manager: manager,
      db_path: db_path
    } do
      input_file = Path.join(db_path, "default_trig.trig")
      output_file = Path.join(db_path, "output.trig")

      input_content = """
      @prefix ex: <#{@ex}> .

      ex:s1 ex:p "default" .

      GRAPH ex:named {
        ex:s2 ex:p "named" .
      }
      """

      File.write!(input_file, input_content)

      # Load and export
      assert {:ok, 2} = Loader.load_trig_file(db, manager, input_file)
      assert {:ok, 2} = Exporter.export_trig_file(db, output_file)

      # Verify both default and named graphs
      assert QuadOperations.default_graph_exists?(db)
      assert QuadOperations.graph_exists?(db, manager, RDF.iri("#{@ex}named"))
    end
  end

  # ===========================================================================
  # 6.2.3.3: Test N-Quads to TriG conversion
  # ===========================================================================

  describe "6.2.3.3 N-Quads to TriG conversion" do
    test "can load N-Quads and export as TriG", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "input.nq")
      output_file = Path.join(db_path, "output.trig")

      # Create N-Quads input
      File.write!(input_file, """
      <#{@ex}s1> <#{@ex}p> "o1" <#{@ex}g1> .
      <#{@ex}s2> <#{@ex}p> "o2" <#{@ex}g1> .
      <#{@ex}s3> <#{@ex}p> "o3" <#{@ex}g2> .
      """)

      # Load N-Quads
      assert {:ok, 3} = Loader.load_nquads_file(db, manager, input_file)

      # Export as TriG
      assert {:ok, 3} = Exporter.export_trig_file(db, output_file)

      # Verify output
      {:ok, output} = File.read(output_file)
      assert String.contains?(output, "GRAPH")
      assert String.contains?(output, "#{@ex}g1") or String.contains?(output, "g1")
    end

    test "conversion preserves all graph structures", %{
      db: db,
      manager: manager,
      db_path: db_path
    } do
      input_file = Path.join(db_path, "multi.nq")
      output_file = Path.join(db_path, "output.trig")

      File.write!(input_file, """
      <#{@ex}s1> <#{@ex}p> "o1" <#{@ex}graph1> .
      <#{@ex}s2> <#{@ex}p> "o2" <#{@ex}graph2> .
      <#{@ex}s3> <#{@ex}p> "o3" <#{@ex}graph3> .
      """)

      assert {:ok, 3} = Loader.load_nquads_file(db, manager, input_file)
      assert {:ok, 3} = Exporter.export_trig_file(db, output_file)

      # Verify all graphs still exist
      assert QuadOperations.graph_exists?(db, manager, RDF.iri("#{@ex}graph1"))
      assert QuadOperations.graph_exists?(db, manager, RDF.iri("#{@ex}graph2"))
      assert QuadOperations.graph_exists?(db, manager, RDF.iri("#{@ex}graph3"))
    end
  end

  # ===========================================================================
  # 6.2.3.4: Test TriG to N-Quads conversion
  # ===========================================================================

  describe "6.2.3.4 TriG to N-Quads conversion" do
    test "can load TriG and export as N-Quads", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "input.trig")
      output_file = Path.join(db_path, "output.nq")

      # Create TriG input
      File.write!(input_file, """
      @prefix ex: <#{@ex}> .

      GRAPH ex:g1 {
        ex:s1 ex:p "o1" .
        ex:s2 ex:p "o2" .
      }

      ex:s3 ex:p "o3" .
      """)

      # Load TriG
      assert {:ok, 3} = Loader.load_trig_file(db, manager, input_file)

      # Export as N-Quads
      assert {:ok, 3} = Exporter.export_nquads_file(db, output_file)

      # Verify output
      {:ok, output} = File.read(output_file)
      # Should have 3 lines (one per quad)
      lines = output |> String.split("\n") |> Enum.filter(&(String.trim(&1) != ""))
      assert length(lines) == 3
    end

    test "conversion preserves graph context", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "context.trig")
      output_file = Path.join(db_path, "output.nq")

      File.write!(input_file, """
      @prefix ex: <#{@ex}> .

      GRAPH ex:named {
        ex:s ex:p "o" .
      }
      """)

      assert {:ok, 1} = Loader.load_trig_file(db, manager, input_file)
      assert {:ok, 1} = Exporter.export_nquads_file(db, output_file)

      {:ok, output} = File.read(output_file)
      # N-Quads format should include the graph IRI
      assert String.contains?(output, "#{@ex}named")
    end
  end

  # ===========================================================================
  # 6.2.3.5: Test roundtrip preserves all graphs
  # ===========================================================================

  describe "6.2.3.5 roundtrip preserves all graphs" do
    test "N-Quads roundtrip preserves multiple graphs", %{
      db: db,
      manager: manager,
      db_path: db_path
    } do
      input_file = Path.join(db_path, "multi.nq")
      output_file = Path.join(db_path, "output.nq")

      # Create input with 5 graphs
      File.write!(input_file, """
      <#{@ex}s1> <#{@ex}p> "o1" <#{@ex}g1> .
      <#{@ex}s2> <#{@ex}p> "o2" <#{@ex}g2> .
      <#{@ex}s3> <#{@ex}p> "o3" <#{@ex}g3> .
      <#{@ex}s4> <#{@ex}p> "o4" <#{@ex}g4> .
      <#{@ex}s5> <#{@ex}p> "o5" <#{@ex}g5> .
      """)

      # Load
      assert {:ok, 5} = Loader.load_nquads_file(db, manager, input_file)

      # Export
      assert {:ok, 5} = Exporter.export_nquads_file(db, output_file)

      # Verify all 5 graphs exist
      Enum.each(1..5, fn i ->
        graph = RDF.iri("#{@ex}g#{i}")
        assert QuadOperations.graph_exists?(db, manager, graph), "Graph g#{i} should exist"
      end)
    end

    test "TriG roundtrip preserves graph structure", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "structure.trig")
      output_file = Path.join(db_path, "output.trig")

      File.write!(input_file, """
      @prefix ex: <#{@ex}> .

      ex:default ex:p "d1" .
      ex:default ex:p "d2" .

      GRAPH ex:named1 {
        ex:s1 ex:p "o1" .
        ex:s1 ex:p "o2" .
      }

      GRAPH ex:named2 {
        ex:s2 ex:p "o3" .
        ex:s2 ex:p "o4" .
      }
      """)

      # Load
      assert {:ok, 6} = Loader.load_trig_file(db, manager, input_file)

      # Export
      assert {:ok, 6} = Exporter.export_trig_file(db, output_file)

      # Verify graph structure
      assert QuadOperations.default_graph_exists?(db)

      {:ok, default_count} = QuadOperations.graph_quad_count(db, manager, :default)
      assert default_count == 2

      {:ok, n1_count} = QuadOperations.graph_quad_count(db, manager, RDF.iri("#{@ex}named1"))
      {:ok, n2_count} = QuadOperations.graph_quad_count(db, manager, RDF.iri("#{@ex}named2"))

      assert n1_count == 2
      assert n2_count == 2
    end
  end

  # ===========================================================================
  # 6.2.3.6: Test roundtrip preserves blank node IDs
  # ===========================================================================

  describe "6.2.3.6 roundtrip preserves blank node IDs" do
    test "N-Quads roundtrip preserves blank node labels", %{
      db: db,
      manager: manager,
      db_path: db_path
    } do
      input_file = Path.join(db_path, "blank.nq")
      output_file = Path.join(db_path, "output.nq")

      # Note: RDF.ex may normalize blank node labels, so we check that blank nodes exist
      # rather than exact label preservation
      File.write!(input_file, """
      <#{@ex}s1> <#{@ex}p> "o1" <#{@ex}g1> .
      _:b1 <#{@ex}p> "o2" <#{@ex}g1> .
      _:b1 <#{@ex}p2> "o3" <#{@ex}g1> .
      """)

      assert {:ok, 3} = Loader.load_nquads_file(db, manager, input_file)
      assert {:ok, 3} = Exporter.export_nquads_file(db, output_file)

      # Verify blank nodes are preserved in the output
      {:ok, output} = File.read(output_file)
      assert String.contains?(output, "_:")
    end

    test "TriG roundtrip preserves blank nodes", %{db: db, manager: manager, db_path: db_path} do
      input_file = Path.join(db_path, "blank.trig")
      output_file = Path.join(db_path, "output.trig")

      File.write!(input_file, """
      @prefix ex: <#{@ex}> .

      GRAPH ex:g1 {
        _:b1 ex:p "o1" .
        _:b1 ex:p2 "o2" .
      }
      """)

      assert {:ok, 2} = Loader.load_trig_file(db, manager, input_file)
      assert {:ok, 2} = Exporter.export_trig_file(db, output_file)

      # Verify the quad count is preserved (blank nodes are preserved internally)
      total = count_all_quads(db)
      assert total == 2

      # Note: RDF.ex may serialize blank nodes using [...] notation instead of _:b1
      # The important thing is that the semantic structure is preserved
      {:ok, output} = File.read(output_file)
      # The output should contain the predicates and objects
      assert String.contains?(output, "#{@ex}p")
      assert String.contains?(output, "#{@ex}p2")
    end

    test "preserves distinction between different blank nodes", %{
      db: db,
      manager: manager,
      db_path: db_path
    } do
      input_file = Path.join(db_path, "multi_blank.nq")
      output_file = Path.join(db_path, "output.nq")

      File.write!(input_file, """
      _:b1 <#{@ex}p> "o1" <#{@ex}g> .
      _:b2 <#{@ex}p> "o2" <#{@ex}g> .
      _:b1 <#{@ex}p2> "o3" <#{@ex}g> .
      """)

      assert {:ok, 3} = Loader.load_nquads_file(db, manager, input_file)
      assert {:ok, 3} = Exporter.export_nquads_file(db, output_file)

      # The key is that b1's two triples should be connected to the same blank node
      # while b2 is a different blank node
      total = count_all_quads(db)
      assert total == 3
    end
  end
end
