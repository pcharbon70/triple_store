defmodule TripleStore.ExporterRefactoringTest do
  @moduledoc """
  Tests for Section 2.7: Exporter Module Refactoring.

  Verifies that the unified API supports both triples and quads:
  - export_dataset/2 exports all quads as RDF.Dataset
  - export_graphs/4 exports specific graphs as RDF.Dataset
  - export_default_graph/1 exports only default graph as RDF.Graph
  - export_single_graph/4 exports single named graph as RDF.Graph
  - export_multiple_graphs/4 exports multiple graphs (alias)
  - Backward compatibility is maintained
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.NIF
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Exporter
  alias TripleStore.Loader

  @test_db_base "/tmp/triple_store_exporter_refactoring_test"

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

  defp temp_file(suffix) do
    "#{@test_db_base}_#{suffix}_#{:erlang.unique_integer()}"
  end

  # ===========================================================================
  # export_dataset/2 Tests
  # ===========================================================================

  describe "export_dataset/2" do
    test "exports all quads as RDF.Dataset with all graphs", %{db: db, manager: manager} do
      # Add quads to multiple graphs
      g1 = RDF.iri("http://example.org/graph1")
      g2 = RDF.iri("http://example.org/graph2")

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s> <http://example.org/p> \"o1\" .", :turtle,
          graph: g1
        )

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s2> <http://example.org/p2> \"o2\" .", :turtle,
          graph: g2
        )

      # Export as dataset
      assert {:ok, dataset} = Exporter.export_dataset(db)

      # Verify dataset contains all graphs
      assert RDF.Dataset.graph_count(dataset) >= 2

      # Verify we can find quads from both graphs
      quads = RDF.Dataset.quads(dataset)
      assert Enum.any?(quads, fn quad -> elem(quad, 3) == g1 end)
      assert Enum.any?(quads, fn quad -> elem(quad, 3) == g2 end)
    end

    test "exports empty store as empty dataset", %{db: db} do
      assert {:ok, dataset} = Exporter.export_dataset(db)
      assert RDF.Dataset.graph_count(dataset) == 0
    end
  end

  # ===========================================================================
  # export_graphs/4 Tests
  # ===========================================================================

  describe "export_graphs/4" do
    test "exports specific named graphs as RDF.Dataset", %{db: db, manager: manager} do
      # Add quads to multiple graphs
      g1 = RDF.iri("http://example.org/graph1")
      g2 = RDF.iri("http://example.org/graph2")
      g3 = RDF.iri("http://example.org/graph3")

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s> <http://example.org/p> \"o1\" .", :turtle,
          graph: g1
        )

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s2> <http://example.org/p2> \"o2\" .", :turtle,
          graph: g2
        )

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s3> <http://example.org/p3> \"o3\" .", :turtle,
          graph: g3
        )

      # Export only g1 and g2
      assert {:ok, dataset} = Exporter.export_graphs(db, manager, [g1, g2])

      # Verify dataset contains only the specified graphs
      graphs = RDF.Dataset.graphs(dataset)
      graph_names = Enum.map(graphs, fn graph -> graph.name end)

      # The graphs should be in the dataset
      assert g1 in graph_names
      assert g2 in graph_names

      # Count quads - should have quads from g1 and g2 only
      quad_count = dataset |> RDF.Dataset.quads() |> length()
      assert quad_count == 2
    end

    test "includes default graph when :include_default is true", %{db: db, manager: manager} do
      g1 = RDF.iri("http://example.org/graph1")

      # Add to named graph
      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s> <http://example.org/p> \"o1\" .", :turtle,
          graph: g1
        )

      # Add to default graph
      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s2> <http://example.org/p2> \"o2\" .", :turtle,
          graph: :default
        )

      # Export with include_default
      assert {:ok, dataset} =
               Exporter.export_graphs(db, manager, [g1], include_default: true)

      # Should have both graphs
      quads = RDF.Dataset.quads(dataset)
      assert length(quads) == 2

      # One should be from default graph (nil in RDF.ex)
      assert Enum.any?(quads, fn quad -> elem(quad, 3) == nil end)
      assert Enum.any?(quads, fn quad -> elem(quad, 3) == g1 end)
    end

    test "handles empty graph list", %{db: db, manager: manager} do
      g1 = RDF.iri("http://example.org/graph1")

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s> <http://example.org/p> \"o1\" .", :turtle,
          graph: g1
        )

      # Export empty list
      assert {:ok, dataset} = Exporter.export_graphs(db, manager, [])

      # Should have no quads
      assert RDF.Dataset.quads(dataset) |> length() == 0
    end
  end

  # ===========================================================================
  # export_default_graph/1 Tests
  # ===========================================================================

  describe "export_default_graph/1" do
    test "exports only default graph as RDF.Graph", %{db: db, manager: manager} do
      # Add to default graph
      {:ok, _} =
        Loader.load_string(
          db,
          manager,
          "<http://example.org/s> <http://example.org/p> \"o1\" . <http://example.org/s2> <http://example.org/p2> \"o2\" .",
          :turtle,
          graph: :default
        )

      # Add to named graph (should not be included)
      g1 = RDF.iri("http://example.org/graph1")

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s3> <http://example.org/p3> \"o3\" .", :turtle,
          graph: g1
        )

      # Export default graph
      assert {:ok, graph} = Exporter.export_default_graph(db)

      # Should have only 2 triples (from default graph)
      assert RDF.Graph.triple_count(graph) == 2

      # Verify graph has no name (default graph)
      assert graph.name == nil
    end

    test "returns empty graph when default graph has no triples", %{db: db} do
      assert {:ok, graph} = Exporter.export_default_graph(db)
      assert RDF.Graph.triple_count(graph) == 0
    end

    test "supports graph options like :name", %{db: db, manager: manager} do
      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s> <http://example.org/p> \"o\" .", :turtle,
          graph: :default
        )

      name = RDF.iri("http://example.org/named")
      assert {:ok, graph} = Exporter.export_default_graph(db, name: name)

      assert graph.name == name
      assert RDF.Graph.triple_count(graph) == 1
    end
  end

  # ===========================================================================
  # export_single_graph/4 Tests
  # ===========================================================================

  describe "export_single_graph/4" do
    test "exports single named graph as RDF.Graph", %{db: db, manager: manager} do
      g1 = RDF.iri("http://example.org/graph1")

      {:ok, _} =
        Loader.load_string(
          db,
          manager,
          "<http://example.org/s> <http://example.org/p> \"o1\" . <http://example.org/s2> <http://example.org/p2> \"o2\" .",
          :turtle,
          graph: g1
        )

      # Add to different graph
      g2 = RDF.iri("http://example.org/graph2")

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s3> <http://example.org/p3> \"o3\" .", :turtle,
          graph: g2
        )

      # Export single graph
      assert {:ok, graph} = Exporter.export_single_graph(db, manager, g1)

      # Should have only 2 triples from g1
      assert RDF.Graph.triple_count(graph) == 2

      # Graph name should be g1
      assert graph.name == g1
    end

    test "returns error when graph does not exist", %{db: db, manager: manager} do
      non_existent = RDF.iri("http://example.org/nonexistent")

      assert {:error, :graph_not_found} =
               Exporter.export_single_graph(db, manager, non_existent)
    end

    test "supports :name option to override graph name", %{db: db, manager: manager} do
      g1 = RDF.iri("http://example.org/graph1")

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s> <http://example.org/p> \"o\" .", :turtle,
          graph: g1
        )

      custom_name = RDF.iri("http://example.org/custom")

      assert {:ok, graph} =
               Exporter.export_single_graph(db, manager, g1, name: custom_name)

      assert graph.name == custom_name
    end
  end

  # ===========================================================================
  # export_multiple_graphs/4 Tests
  # ===========================================================================

  describe "export_multiple_graphs/4" do
    test "alias for export_graphs - exports multiple graphs", %{db: db, manager: manager} do
      g1 = RDF.iri("http://example.org/graph1")
      g2 = RDF.iri("http://example.org/graph2")

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s> <http://example.org/p> \"o1\" .", :turtle,
          graph: g1
        )

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s2> <http://example.org/p2> \"o2\" .", :turtle,
          graph: g2
        )

      # Use export_multiple_graphs (alias)
      assert {:ok, dataset} = Exporter.export_multiple_graphs(db, manager, [g1, g2])

      # Should have both graphs
      assert RDF.Dataset.graph_count(dataset) == 2
      assert RDF.Dataset.quads(dataset) |> length() == 2
    end
  end

  # ===========================================================================
  # Backward Compatibility Tests
  # ===========================================================================

  describe "backward compatibility" do
    test "existing export functions still work", %{db: db, manager: manager} do
      # Load some test data
      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s> <http://example.org/p> \"o\" .", :turtle,
          graph: :default
        )

      # Test export_nquads_file (quad store uses N-Quads export)
      file = temp_file("nq")
      assert {:ok, _} = Exporter.export_nquads_file(db, file)
      assert File.exists?(file)

      content = File.read!(file)
      assert String.contains?(content, "http://example.org/s")

      # Test export_nquads_string
      assert {:ok, string} = Exporter.export_nquads_string(db)
      assert String.contains?(string, "http://example.org/s")

      File.rm_rf(file)
    end

    test "existing N-Quads export still works", %{db: db, manager: manager} do
      g1 = RDF.iri("http://example.org/graph1")

      {:ok, _} =
        Loader.load_string(db, manager, "<http://example.org/s> <http://example.org/p> \"o\" .", :turtle,
          graph: g1
        )

      # Test export_nquads_string
      assert {:ok, string} = Exporter.export_nquads_string(db)
      assert String.contains?(string, "<http://example.org/graph1>")

      # Test export_nquads_file
      file = temp_file("nq")
      assert {:ok, _} = Exporter.export_nquads_file(db, file)
      assert File.exists?(file)

      content = File.read!(file)
      assert String.contains?(content, "<http://example.org/graph1>")

      File.rm_rf(file)
    end
  end
end
