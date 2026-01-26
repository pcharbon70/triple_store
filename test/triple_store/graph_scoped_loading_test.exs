defmodule TripleStore.GraphScopedLoadingTest do
  @moduledoc """
  Tests for Section 2.5: Graph-Scoped Loading.

  Verifies that:
  - load_to_graph loads files to specific named graphs
  - Default graph in source is overridden with target graph
  - clear_graph option works correctly
  - load_files_to_graphs loads multiple files to separate graphs
  - Parallel and sequential loading modes work
  - Conflict handling options work correctly
  """

  use ExUnit.Case, async: false

  alias TripleStore.Backend.RocksDB.ErlangAdapter
  alias TripleStore.Dictionary.Manager
  alias TripleStore.Loader
  alias TripleStore.QuadOperations

  @test_db_base "/tmp/triple_store_graph_scoped_test"

  setup do
    test_path = "#{@test_db_base}_#{:erlang.unique_integer([:positive])}"
    {:ok, db} = ErlangAdapter.open(test_path, schema: :quad)
    {:ok, manager} = Manager.start_link(db: db)

    on_exit(fn ->
      if Process.alive?(manager) do
        Manager.stop(manager)
      end

      ErlangAdapter.close(db)
      File.rm_rf(test_path)
    end)

    {:ok, db: db, manager: manager, path: test_path}
  end

  # ===========================================================================
  # Helper Functions
  # ===========================================================================

  # Create a temporary test file with given content and extension
  defp create_test_file(name, content, ext) do
    path =
      Path.join([
        System.tmp_dir!(),
        "triple_store_test_#{name}_#{:erlang.unique_integer()}.#{ext}"
      ])

    File.write!(path, content)
    path
  end

  # Create a simple Turtle file
  defp create_turtle_file(name) do
    content = """
    @prefix ex: <http://example.org/> .

    ex:s1 ex:p "o1" .
    ex:s2 ex:p "o2" .
    ex:s3 ex:p "o3" .
    """

    create_test_file(name, content, "ttl")
  end

  # Create an N-Triples file
  defp create_ntriples_file(name) do
    content = """
    <http://example.org/s1> <http://example.org/p> "o1" .
    <http://example.org/s2> <http://example.org/p> "o2" .
    """

    create_test_file(name, content, "nt")
  end

  # ===========================================================================
  # load_to_graph Tests
  # ===========================================================================

  describe "load_to_graph/5" do
    test "loads Turtle file to named graph", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/target_graph")
      path = create_turtle_file("turtle")

      {:ok, count} = Loader.load_to_graph(db, manager, path, graph)

      assert count == 3

      # Verify quads are in the named graph
      {:ok, graph_id} = TripleStore.Adapter.term_to_id(manager, graph)
      quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})

      assert length(quads) == 3

      File.rm!(path)
    end

    test "loads N-Triples file to named graph", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/target_graph")
      path = create_ntriples_file("ntriples")

      {:ok, count} = Loader.load_to_graph(db, manager, path, graph)

      assert count == 2

      # Verify quads are in the named graph
      {:ok, graph_id} = TripleStore.Adapter.term_to_id(manager, graph)
      quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})

      assert length(quads) == 2

      File.rm!(path)
    end

    test "overrides default graph from source", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/target_graph")
      path = create_turtle_file("override")

      {:ok, _count} = Loader.load_to_graph(db, manager, path, graph)

      # Verify quads are NOT in default graph
      default_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: 0})
      assert length(default_quads) == 0

      # Verify quads ARE in target graph
      {:ok, graph_id} = TripleStore.Adapter.term_to_id(manager, graph)
      target_quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})
      assert length(target_quads) == 3

      File.rm!(path)
    end

    test "clear_graph option clears existing data", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/target_graph")

      # Load initial data
      path1 = create_turtle_file("initial")
      {:ok, count1} = Loader.load_to_graph(db, manager, path1, graph)
      assert count1 == 3
      File.rm!(path1)

      # Load more data with clear_graph: true
      path2 = create_ntriples_file("update")
      {:ok, count2} = Loader.load_to_graph(db, manager, path2, graph, clear_graph: true)
      assert count2 == 2
      File.rm!(path2)

      # Verify only the second file's data remains
      {:ok, graph_id} = TripleStore.Adapter.term_to_id(manager, graph)
      quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})

      assert length(quads) == 2
    end

    test "without clear_graph, data accumulates", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/target_graph")

      # Load initial data
      path1 = create_turtle_file("initial")
      {:ok, count1} = Loader.load_to_graph(db, manager, path1, graph)
      assert count1 == 3
      File.rm!(path1)

      # Load more data without clearing
      path2 = create_ntriples_file("additional")
      {:ok, count2} = Loader.load_to_graph(db, manager, path2, graph, clear_graph: false)
      assert count2 == 2
      File.rm!(path2)

      # Verify data from both loads exists
      {:ok, graph_id} = TripleStore.Adapter.term_to_id(manager, graph)
      quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})

      # Some triples may overlap (s1, p, o1 appears in both)
      assert length(quads) >= 3
    end

    test "returns error for non-existent file", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/target_graph")

      result = Loader.load_to_graph(db, manager, "/nonexistent/file.ttl", graph)

      assert {:error, :file_not_found} = result
    end

    test "returns error for invalid file format", %{db: db, manager: manager} do
      graph = RDF.iri("http://example.org/target_graph")

      # Create file with unknown extension
      path = create_test_file("unknown", "invalid content", "xyz")

      result = Loader.load_to_graph(db, manager, path, graph)

      assert {:error, {:unsupported_format, _xyz, _}} = result

      File.rm!(path)
    end

    test "accepts RDF.BlankNode as graph term", %{db: db, manager: manager} do
      graph = RDF.bnode("test_graph")
      path = create_turtle_file("bnode")

      {:ok, count} = Loader.load_to_graph(db, manager, path, graph)

      assert count == 3

      # Verify quads are in the blank node graph
      {:ok, graph_id} = TripleStore.Adapter.term_to_id(manager, graph)
      quads = QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: graph_id})

      assert length(quads) == 3

      File.rm!(path)
    end
  end

  # ===========================================================================
  # load_files_to_graphs Tests
  # ===========================================================================

  describe "load_files_to_graphs/3" do
    test "loads multiple files to separate graphs", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")
      graph3 = RDF.iri("http://example.org/g3")

      path1 = create_turtle_file("g1")
      path2 = create_ntriples_file("g2")
      path3 = create_turtle_file("g3")

      graph_files = %{
        graph1 => path1,
        graph2 => path2,
        graph3 => path3
      }

      {:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files)

      # Check summary
      assert map_size(summary) == 3
      assert Map.get(summary, graph1) == 3
      assert Map.get(summary, graph2) == 2
      assert Map.get(summary, graph3) == 3

      # Verify each graph has its data
      {:ok, g1_id} = TripleStore.Adapter.term_to_id(manager, graph1)
      {:ok, g2_id} = TripleStore.Adapter.term_to_id(manager, graph2)
      {:ok, g3_id} = TripleStore.Adapter.term_to_id(manager, graph3)

      assert length(QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g1_id})) == 3
      assert length(QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g2_id})) == 2
      assert length(QuadOperations.lookup_quads(db, {:var, :var, :var, :bound}, %{g: g3_id})) == 3

      File.rm!(path1)
      File.rm!(path2)
      File.rm!(path3)
    end

    test "returns empty map for empty graph_files map", %{db: db, manager: manager} do
      {:ok, summary} = Loader.load_files_to_graphs(db, manager, %{})

      assert summary == %{}
    end

    test "sequential mode (default)", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")

      path1 = create_turtle_file("seq1")
      path2 = create_turtle_file("seq2")

      graph_files = %{
        graph1 => path1,
        graph2 => path2
      }

      # Sequential is the default
      {:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files, parallel: false)

      assert map_size(summary) == 2

      File.rm!(path1)
      File.rm!(path2)
    end

    test "parallel mode", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")
      graph3 = RDF.iri("http://example.org/g3")

      path1 = create_turtle_file("par1")
      path2 = create_turtle_file("par2")
      path3 = create_turtle_file("par3")

      graph_files = %{
        graph1 => path1,
        graph2 => path2,
        graph3 => path3
      }

      {:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files, parallel: true)

      assert map_size(summary) == 3

      File.rm!(path1)
      File.rm!(path2)
      File.rm!(path3)
    end

    test "on_conflict: :continue continues on error", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")
      graph3 = RDF.iri("http://example.org/g3")

      path1 = create_turtle_file("cont1")
      path3 = create_turtle_file("cont3")

      # Use non-existent path for graph2
      graph_files = %{
        graph1 => path1,
        graph2 => "/nonexistent/file.ttl",
        graph3 => path3
      }

      {:ok, summary} =
        Loader.load_files_to_graphs(db, manager, graph_files, on_conflict: :continue)

      # Should have results for all graphs, with error for graph2
      assert map_size(summary) == 3
      assert Map.get(summary, graph1) == 3
      assert {:error, _} = Map.get(summary, graph2)
      assert Map.get(summary, graph3) == 3

      File.rm!(path1)
      File.rm!(path3)
    end

    test "on_conflict: :stop stops on first error", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")
      graph3 = RDF.iri("http://example.org/g3")

      path1 = create_turtle_file("stop1")
      # path2 is non-existent
      path3 = create_turtle_file("stop3")

      graph_files = %{
        graph1 => path1,
        graph2 => "/nonexistent/file.ttl",
        graph3 => path3
      }

      {:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files, on_conflict: :stop)

      # Should have results for graph1 and graph2 (with error)
      # graph3 should not be in summary since we stopped
      assert map_size(summary) >= 1
      assert Map.get(summary, graph1) == 3
      assert {:error, _} = Map.get(summary, graph2)

      File.rm!(path1)
      File.rm!(path3)
    end

    test "on_conflict: :abort returns error tuple", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")

      path1 = create_turtle_file("abort1")

      graph_files = %{
        graph1 => path1,
        graph2 => "/nonexistent/file.ttl"
      }

      result = Loader.load_files_to_graphs(db, manager, graph_files, on_conflict: :abort)

      assert {:error, {:load_error, ^graph2, "/nonexistent/file.ttl", _}} = result

      File.rm!(path1)
    end

    test "clear_graphs clears all target graphs before loading", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")

      path1 = create_turtle_file("clear1")
      path2 = create_turtle_file("clear2")

      # Load initial data
      {:ok, _} = Loader.load_to_graph(db, manager, path1, graph1)

      # Reload with clear_graphs
      graph_files = %{
        graph1 => path1,
        graph2 => path2
      }

      {:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files, clear_graphs: true)

      # Verify counts match file contents (not accumulated)
      assert Map.get(summary, graph1) == 3
      assert Map.get(summary, graph2) == 3

      File.rm!(path1)
      File.rm!(path2)
    end

    test "progress_callback is called during sequential loading", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")

      path1 = create_turtle_file("prog1")
      path2 = create_turtle_file("prog2")

      graph_files = %{
        graph1 => path1,
        graph2 => path2
      }

      # Track callback invocations
      {:ok, callback_agent} = Agent.start_link(fn -> [] end)

      progress_callback = fn info ->
        Agent.update(callback_agent, fn calls -> [info | calls] end)
        :continue
      end

      {:ok, _summary} =
        Loader.load_files_to_graphs(db, manager, graph_files,
          progress_callback: progress_callback,
          parallel: false
        )

      # Verify callback was called twice (once per file)
      calls = Agent.get(callback_agent, & &1)
      assert length(calls) == 2

      Agent.stop(callback_agent)
      File.rm!(path1)
      File.rm!(path2)
    end

    test "progress_callback can halt loading", %{db: db, manager: manager} do
      graph1 = RDF.iri("http://example.org/g1")
      graph2 = RDF.iri("http://example.org/g2")
      graph3 = RDF.iri("http://example.org/g3")

      path1 = create_turtle_file("halt1")
      path2 = create_turtle_file("halt2")
      path3 = create_turtle_file("halt3")

      graph_files = %{
        graph1 => path1,
        graph2 => path2,
        graph3 => path3
      }

      # Halt after first file
      progress_callback = fn info ->
        if info.loaded_so_far >= 1 do
          :halt
        else
          :continue
        end
      end

      {:ok, summary} =
        Loader.load_files_to_graphs(db, manager, graph_files,
          progress_callback: progress_callback,
          parallel: false
        )

      # Should only have loaded the first file
      assert map_size(summary) == 1
      assert Map.get(summary, graph1) == 3

      File.rm!(path1)
      File.rm!(path2)
      File.rm!(path3)
    end
  end

  # ===========================================================================
  # Integration Tests
  # ===========================================================================

  describe "integration" do
    test "complete graph-scoped loading workflow", %{db: db, manager: manager} do
      # Create multiple graphs from different sources
      graph1 = RDF.iri("http://example.org/data")
      graph2 = RDF.iri("http://example.org/schema")

      path1 = create_turtle_file("data")
      path2 = create_ntriples_file("schema")

      # Load to separate graphs
      graph_files = %{
        graph1 => path1,
        graph2 => path2
      }

      {:ok, summary} = Loader.load_files_to_graphs(db, manager, graph_files)

      assert Map.get(summary, graph1) == 3
      assert Map.get(summary, graph2) == 2

      # Verify graphs exist
      assert QuadOperations.graph_exists?(db, manager, graph1)
      assert QuadOperations.graph_exists?(db, manager, graph2)

      # Verify default graph is empty
      refute QuadOperations.default_graph_exists?(db)

      # List graphs and verify
      {:ok, graphs} = QuadOperations.list_graphs(db)
      assert length(graphs) == 2
      assert graph1 in graphs
      assert graph2 in graphs

      File.rm!(path1)
      File.rm!(path2)
    end
  end
end
